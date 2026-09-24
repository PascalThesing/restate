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
use restate_worker_api::invoker::slot_shares::{SlotLease, SlotShares};

use super::grouped_waiters::GroupedWaiters;
use crate::scheduler::VQueueHandle;
use crate::scheduler::eligible::{LaneWeightResolver, SchedulingGroup, WeightResolver};

/// Invoker slot limiter.
///
/// With weighted slot shares enabled, waiters are split in two classes:
/// - **in-flight** work of chains that already run (children, resumes): served
///   first, so a chain holding slots while it awaits its children always gets
///   them run (no hold-and-wait on the pool);
/// - **new chain starts** (a root's first run): served only when no in-flight
///   work waits. Their weighted share is enforced *before* they get here, at
///   admission (see `ShareGate`), so this limiter never scans or skips capped
///   waiters.
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
}

#[derive(Clone)]
struct Parked {
    key: String,
    /// In-flight chain work (a child or a resume): never counts as its lane
    /// waiting for a share, and is served from the priority list.
    exempt: bool,
    weight: u32,
}

/// Share lane of a (group, service): node-wide key plus its weight.
pub(super) fn lane_of(
    group: &SchedulingGroup,
    service: &ServiceName,
    weight_resolver: &WeightResolver,
    lane_weight_resolver: &LaneWeightResolver,
) -> (String, u32) {
    let key = match group {
        SchedulingGroup::Scope(scope) => format!("{scope}/{service}"),
        SchedulingGroup::Service(_) => format!("/{service}"),
    };
    let weight = weight_resolver(group)
        .get()
        .saturating_mul(lane_weight_resolver(group, service).get());
    (key, weight)
}

impl InvokerConcurrencyLimiter {
    pub fn new(
        limiter: Concurrency,
        weight_resolver: WeightResolver,
        lane_weight_resolver: LaneWeightResolver,
        shares: SlotShares,
    ) -> Self {
        Self {
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

    pub(super) fn lane_of(&self, group: &SchedulingGroup, service: &ServiceName) -> (String, u32) {
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
            self.shares.set_waiting(&parked.key, parked.weight, 1);
        }
        self.parked.insert(vqueue, parked);
    }

    fn unpark(&mut self, vqueue: VQueueHandle) {
        if let Some(p) = self.parked.remove(vqueue)
            && !p.exempt
        {
            self.shares.set_waiting(&p.key, p.weight, -1);
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
        // the turn for a permit set aside for it
        let yields = prioritised
            && new_start
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
                    let lane = self.lane_of(group, service);
                    self.shares.take(&lane.0, lane.1)
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
            let lane = self.lane_of(group, service);
            self.park(
                vqueue,
                Parked {
                    key: lane.0,
                    exempt,
                    weight: lane.1,
                },
            );
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
            let (vqueue, in_flight) = match self.priority.pop_front() {
                Some(v) => (v, true),
                None => (self.waiters.pop_front().unwrap(), false),
            };
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
                self.shares.set_waiting(&p.key, p.weight, -1);
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
}
