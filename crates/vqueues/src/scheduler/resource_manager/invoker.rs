// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::Poll;

use slotmap::SecondaryMap;

use restate_futures_util::concurrency::{Concurrency, Permit};
use restate_types::ServiceName;
use restate_worker_api::invoker::slot_shares::{GroupKey, OwnedShareLane, ShareLane, SlotLease, SlotShares};

use super::grouped_waiters::GroupedWaiters;
use crate::scheduler::VQueueHandle;
use crate::scheduler::eligible::{LaneWeightResolver, SchedulingGroup, WeightResolver};

/// Invoker slot limiter.
///
/// With weighted slot shares enabled, waiters are split in two classes:
/// - **in-flight** work of chains that already run (children, resumes): served
///   first, so a chain holding slots while it awaits its children always gets
///   them run (no hold-and-wait on the pool);
/// - **new starts** (a first run with no parent): served only when no in-flight
///   work waits, except that after `priority_burst` consecutive in-flight
///   grants one freed slot goes to a new start, so a steady stream of in-flight
///   work can never starve new starts. Their weighted share is enforced
///   *before* they get here, at admission (see `ShareGate`), and again at
///   claim time by the resource manager.
///
/// With shares disabled every waiter is in the single WRR list, as before.
pub struct InvokerConcurrencyLimiter {
    limiter: Concurrency,
    // Weighted round-robin over service groups instead of a flat FIFO, so no
    // service can monopolize freed permits regardless of arrival order.
    waiters: GroupedWaiters,
    /// In-flight chain work (shares enabled only), served before `waiters`.
    priority: GroupedWaiters,
    cached_permit: Permit,
    /// Queues `poll_head` woke for a cached permit that have not claimed one
    /// yet. A queue that loses the wake-then-steal race re-parks at the front
    /// of its own lane with its stride refunded. Cleared on successful claim
    /// and on external removal, so a stale flag can never grant a perpetual
    /// front position.
    /// The value says whether the woken queue is in-flight chain work.
    woken: SecondaryMap<VQueueHandle, bool>,
    /// Node-wide weighted slot shares (no-op when disabled).
    shares: SlotShares,
    weight_resolver: WeightResolver,
    lane_weight_resolver: LaneWeightResolver,
    /// Parked queues and the share lane each one is counted as waiting in.
    parked: SecondaryMap<VQueueHandle, Parked>,
    /// After this many consecutive in-flight grants while new starts wait, one
    /// freed slot goes to a new start.
    priority_burst: u32,
    /// Consecutive in-flight grants since the last new-start grant.
    priority_grants: u32,
}

#[derive(Clone)]
struct Parked {
    lane: OwnedShareLane,
    /// In-flight chain work (a child or a resume): never counts as its lane
    /// waiting for a share, and is served from the priority list.
    exempt: bool,
}

/// Share lane of a (group, service): the group (scope, or the service itself
/// when unscoped) with its weight, and the service lane with its lane weight.
pub(super) fn lane_of<'a>(
    group: &'a SchedulingGroup,
    service: &'a ServiceName,
    weight_resolver: &WeightResolver,
    lane_weight_resolver: &LaneWeightResolver,
) -> ShareLane<'a> {
    let key = match group {
        SchedulingGroup::Scope(scope) => GroupKey::Scope(scope.as_str()),
        SchedulingGroup::Service(name) => GroupKey::Service(name.as_ref()),
    };
    ShareLane {
        group: key,
        group_weight: weight_resolver(group).get(),
        lane: service.as_ref(),
        lane_weight: lane_weight_resolver(group, service).get(),
    }
}

impl InvokerConcurrencyLimiter {
    pub fn new(
        limiter: Concurrency,
        weight_resolver: WeightResolver,
        lane_weight_resolver: LaneWeightResolver,
        shares: SlotShares,
        priority_burst: u32,
    ) -> Self {
        Self {
            priority_burst: priority_burst.max(1),
            priority_grants: 0,
            limiter,
            waiters: GroupedWaiters::new(weight_resolver.clone(), lane_weight_resolver.clone()),
            priority: GroupedWaiters::new(weight_resolver.clone(), lane_weight_resolver.clone()),
            cached_permit: Permit::new_empty(),
            woken: SecondaryMap::new(),
            shares,
            weight_resolver,
            lane_weight_resolver,
            parked: SecondaryMap::new(),
        }
    }

    pub(super) fn lane_of<'a>(
        &self,
        group: &'a SchedulingGroup,
        service: &'a ServiceName,
    ) -> ShareLane<'a> {
        lane_of(group, service, &self.weight_resolver, &self.lane_weight_resolver)
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.woken.remove(vqueue);
        self.waiters.remove(vqueue);
        self.priority.remove(vqueue);
        self.unpark(vqueue);
    }

    fn park(&mut self, vqueue: VQueueHandle, parked: Parked) {
        if self.parked.get(vqueue).is_some_and(|p| p.exempt == parked.exempt) {
            return;
        }
        // the head entry changed between new start and in-flight: re-register
        self.unpark(vqueue);
        if !parked.exempt {
            self.shares.set_waiting(parked.lane.as_lane(), 1);
        }
        self.parked.insert(vqueue, parked);
    }

    fn unpark(&mut self, vqueue: VQueueHandle) {
        if let Some(p) = self.parked.remove(vqueue)
            && !p.exempt
        {
            self.shares.set_waiting(p.lane.as_lane(), -1);
        }
    }

    fn claimed(&mut self, cx: &mut std::task::Context<'_>, vqueue: VQueueHandle) {
        self.woken.remove(vqueue);
        self.waiters.remove(vqueue);
        self.priority.remove(vqueue);
        self.unpark(vqueue);
        if !self.waiters.is_empty() || !self.priority.is_empty() {
            cx.waker().wake_by_ref();
        }
    }

    /// Attempts to claim a permit for a queue the scheduler decided to dispatch.
    ///
    /// This does not gate the claim on the caller being the waiter-list head:
    /// with a rotating WRR waiter head, a head-only gate livelocks (the woken
    /// queue arrives after the head has rotated past it). The waiter list's
    /// job is reduced to picking the wake-up order (see `poll_head`).
    ///
    /// `new_start`: a root's first run. With shares enabled a new start never
    /// takes a slot while in-flight chain work is waiting for one.
    pub(super) fn poll_acquire(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
        group: &SchedulingGroup,
        service: &ServiceName,
        new_start: bool,
    ) -> Option<(Permit, SlotLease)> {
        let prioritised = self.shares.is_enabled();
        let exempt = prioritised && !new_start;
        // a new start yields while in-flight work waits for a slot or holds
        // the turn for a permit set aside for it — unless poll_head chose this
        // very start (burst guard), in which case the permit is its own
        let chosen = self.woken.get(vqueue) == Some(&false);
        let yields = prioritised
            && new_start
            && !chosen
            && (!self.priority.is_empty() || self.woken.values().any(|in_flight| *in_flight));
        if !yields {
            // cached permit exists (set aside by poll_head when it woke a waiter)
            let permit = match self.cached_permit.split(1) {
                Some(permit) => Some(permit),
                None => match self.limiter.poll_acquire(cx) {
                    Poll::Ready(permit) => Some(permit),
                    Poll::Pending => None,
                },
            };
            if let Some(permit) = permit {
                self.claimed(cx, vqueue);
                let lease = if prioritised {
                    self.shares.take(self.lane_of(group, service))
                } else {
                    SlotLease::empty()
                };
                return Some((permit, lease));
            }
        }

        // No permit available (or in-flight work goes first): park this queue.
        // A queue that lost the wake-then-steal race keeps its turn.
        // a queue whose head switched class leaves the other list
        let list = if exempt {
            self.waiters.remove(vqueue);
            &mut self.priority
        } else {
            self.priority.remove(vqueue);
            &mut self.waiters
        };
        if self.woken.remove(vqueue).is_some() {
            list.push_front(vqueue, group, service);
        } else {
            list.push_back(vqueue, group, service);
        }
        if prioritised {
            let lane = OwnedShareLane::from_lane(self.lane_of(group, service));
            self.park(vqueue, Parked { lane, exempt });
        }
        None
    }

    pub fn poll_head(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<VQueueHandle>> {
        if self.waiters.is_empty() && self.priority.is_empty() {
            return Poll::Ready(None);
        }

        tracing::trace!(
            "Polling invoker concurrency permits: {} + {} priority waiters. Cached permit: {:?}",
            self.waiters.len(),
            self.priority.len(),
            self.cached_permit
        );

        match self.limiter.poll_acquire(cx) {
            Poll::Ready(permit) => {
                self.cached_permit.merge(permit);
                tracing::trace!("MERGED NEW PERMIT, CURRENT: {:?}", self.cached_permit);
            }
            Poll::Pending => {}
        }

        if !self.cached_permit.is_empty() {
            // store this permit for the next poller: in-flight chain work
            // first, then new starts in WRR order
            // burst guard: after `priority_burst` in-flight grants in a row
            // while new starts wait, the next freed slot is theirs
            let starve_guard =
                !self.waiters.is_empty() && self.priority_grants >= self.priority_burst;
            let (vqueue, in_flight) = if !starve_guard
                && let Some(v) = self.priority.pop_front()
            {
                (v, true)
            } else {
                match self.waiters.pop_front() {
                    Some(v) => (v, false),
                    None => (self.priority.pop_front().expect("some waiter"), true),
                }
            };
            if in_flight {
                self.priority_grants += 1;
            } else {
                self.priority_grants = 0;
            }
            // remember the chosen waiter: if it loses the claim race it keeps
            // its turn (front of its lane, stride refunded) instead of
            // re-parking at the back
            self.woken.insert(vqueue, in_flight);
            if !self.waiters.is_empty() || !self.priority.is_empty() {
                // make sure to take the waker again for the next poll
                cx.waker().wake_by_ref();
            }
            return Poll::Ready(Some(vqueue));
        }

        Poll::Pending
    }
}

impl Drop for InvokerConcurrencyLimiter {
    /// The shares are node-wide and outlive this partition's leadership: give
    /// back every waiting registration of a parked new start.
    fn drop(&mut self) {
        for (_, p) in self.parked.drain() {
            if !p.exempt {
                self.shares.set_waiting(p.lane.as_lane(), -1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;

    use slotmap::SlotMap;

    use restate_types::ServiceName;

    use super::*;
    use crate::scheduler::eligible::WeightResolver;

    fn resolver() -> WeightResolver {
        Arc::new(|_: &SchedulingGroup| NonZeroU32::MIN)
    }

    fn lane_resolver() -> LaneWeightResolver {
        Arc::new(|_: &SchedulingGroup, _: &ServiceName| NonZeroU32::MIN)
    }

    fn group(name: &str) -> SchedulingGroup {
        SchedulingGroup::Service(ServiceName::new(name))
    }

    fn svc(name: &str) -> ServiceName {
        ServiceName::new(name)
    }

    fn limiter_with_one_permit() -> InvokerConcurrencyLimiter {
        InvokerConcurrencyLimiter::new(
            Concurrency::new(Some(NonZeroUsize::new(1).unwrap())),
            resolver(),
            lane_resolver(),
            SlotShares::disabled(),
            8,
        )
    }

    /// The wake-then-steal race is intentional and livelock-free: `poll_head`
    /// sets a permit aside and wakes waiter A, but whichever queue the
    /// scheduler dispatches first may claim it. The loser keeps its turn
    /// (front of its own lane, stride refunded), so no permit is ever
    /// stranded and the loser is never demoted to the back of the group.
    #[test]
    fn woken_permit_can_be_claimed_by_another_queue_without_stranding() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let vq_a = handles.insert(());
        let vq_a2 = handles.insert(());
        let vq_b = handles.insert(());
        let group_a = group("a");
        let group_b = group("b");
        let svc_a = svc("a");
        let svc_b = svc("b");

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut limiter = limiter_with_one_permit();

        // holder takes the only permit; A, A2 (same lane) and B park
        let permit = limiter
            .poll_acquire(&mut cx, holder, &group("holder"), &svc("holder"), true)
            .expect("permit");
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a, true)
                .is_none()
        );
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a2, &group_a, &svc_a, true)
                .is_none()
        );
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_b, &group_b, &svc_b, true)
                .is_none()
        );

        // release; poll_head caches the freed permit and wakes A (WRR head)
        drop(permit);
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));

        // B "wins the race" to dispatch first and steals the cached permit
        let stolen = limiter
            .poll_acquire(&mut cx, vq_b, &group_b, &svc_b, true)
            .expect("B claims the cached permit");

        // A loses and re-parks — at the FRONT of its lane, ahead of A2
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a, true)
                .is_none()
        );
        assert_eq!(
            limiter.waiters.front(),
            Some(vq_a),
            "steal loser keeps its turn at the front of its lane"
        );

        // the next released permit reaches A, not A2 and not B's lane again
        drop(stolen);
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a, true)
                .is_some(),
            "A claims the permit on its wake"
        );
    }

    /// A stale `woken` flag must not grant a perpetual front position: after
    /// external removal (dormancy path), a later re-park is a plain push_back.
    #[test]
    fn stale_woken_flag_cleared_on_removal() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let vq_a = handles.insert(());
        let vq_a2 = handles.insert(());
        let group_a = group("a");
        let svc_a = svc("a");

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut limiter = limiter_with_one_permit();

        let permit = limiter
            .poll_acquire(&mut cx, holder, &group("holder"), &svc("holder"), true)
            .expect("permit");
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a, true)
                .is_none()
        );
        drop(permit);
        // A is woken (flag set)…
        let woken = limiter.poll_head(&mut cx);
        assert!(matches!(woken, Poll::Ready(Some(h)) if h == vq_a));
        // …but goes dormant instead of claiming: the flag must be cleared
        limiter.remove_from_waiters(vq_a);
        // consume the cached permit so the re-parks below actually park
        let p = limiter
            .poll_acquire(&mut cx, holder, &group("holder"), &svc("holder"), true)
            .expect("cached permit");

        // A2 parks first, then A re-parks: A must land BEHIND A2 (plain
        // push_back — no front privilege from the stale flag)
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a2, &group_a, &svc_a, true)
                .is_none()
        );
        assert!(
            limiter
                .poll_acquire(&mut cx, vq_a, &group_a, &svc_a, true)
                .is_none()
        );
        assert_eq!(
            limiter.waiters.front(),
            Some(vq_a2),
            "stale woken flag must not jump the queue"
        );
        drop(p);
    }

    /// With shares enabled, in-flight chain work (a child or a resume) is
    /// served before new chain starts: a freed slot wakes the waiting child
    /// even though the new start parked first, and the new start may not
    /// steal the set-aside permit while the child waits.
    #[test]
    fn in_flight_work_is_served_before_new_starts() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let root = handles.insert(());
        let child = handles.insert(());
        let (g_rail, s_root, s_child) = (group("rail"), svc("root"), svc("child"));

        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let one = NonZeroUsize::new(1).unwrap();
        let mut limiter = InvokerConcurrencyLimiter::new(
            Concurrency::new(Some(one)),
            resolver(),
            lane_resolver(),
            SlotShares::new(Some(one), true),
            8,
        );

        let held = limiter
            .poll_acquire(&mut cx, holder, &g_rail, &s_root, false)
            .expect("slot");
        // a new start parks first, then a child of a running chain
        assert!(limiter.poll_acquire(&mut cx, root, &g_rail, &s_root, true).is_none());
        assert!(limiter.poll_acquire(&mut cx, child, &g_rail, &s_child, false).is_none());

        drop(held);
        let woken = limiter.poll_head(&mut cx);
        assert!(
            matches!(woken, Poll::Ready(Some(h)) if h == child),
            "the freed slot goes to in-flight work first"
        );
        assert!(
            limiter.poll_acquire(&mut cx, root, &g_rail, &s_root, true).is_none(),
            "a new start must not steal the permit set aside for in-flight work"
        );
        assert!(limiter.poll_acquire(&mut cx, child, &g_rail, &s_child, false).is_some());
    }

    /// Burst guard: with in-flight work always waiting, a parked new start
    /// still gets a slot after `priority_burst` in-flight grants, so a steady
    /// stream of children cannot starve new starts.
    #[test]
    fn new_start_gets_a_slot_after_the_priority_burst() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let holder = handles.insert(());
        let start = handles.insert(());
        let (g, s_root, s_child) = (group("rail"), svc("root"), svc("child"));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let one = NonZeroUsize::new(1).unwrap();
        let burst: u32 = 2;
        let mut limiter = InvokerConcurrencyLimiter::new(
            Concurrency::new(Some(one)),
            resolver(),
            lane_resolver(),
            SlotShares::new(Some(one), true),
            burst,
        );
        let mut held = limiter
            .poll_acquire(&mut cx, holder, &g, &s_root, false)
            .expect("slot");
        assert!(limiter.poll_acquire(&mut cx, start, &g, &s_root, true).is_none());

        let mut granted_to_start = None;
        for round in 0..(burst as usize + 1) {
            // a fresh child is always waiting when the slot frees
            let child = handles.insert(());
            assert!(limiter.poll_acquire(&mut cx, child, &g, &s_child, false).is_none());
            drop(held);
            let woken = match limiter.poll_head(&mut cx) {
                Poll::Ready(Some(h)) => h,
                other => panic!("expected a wake, got {other:?}"),
            };
            if woken == start {
                granted_to_start = Some(round);
                assert!(
                    limiter.poll_acquire(&mut cx, start, &g, &s_root, true).is_some(),
                    "the chosen new start may claim its permit despite waiting children"
                );
                break;
            }
            assert_eq!(woken, child, "in-flight work first");
            held = limiter
                .poll_acquire(&mut cx, child, &g, &s_child, false)
                .expect("child claims");
        }
        assert_eq!(
            granted_to_start,
            Some(burst as usize),
            "after {burst} in-flight grants the next freed slot is the new start's"
        );
    }
}
