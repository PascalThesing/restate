// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Weighted slot shares, enforced at admission.
//!
//! A new start (a first run with no parent) whose lane already holds its
//! weighted share while other lanes wait is blocked *here*, before it asks the
//! invoker for a slot. It sleeps until the node's shares change (a slot is
//! returned or the waiting set changes) and is then woken only if its lane has
//! headroom. Parking it in the invoker queue instead made the scheduler
//! re-poll every capped start on every freed slot.
//!
//! The weight is a distribution, not a count: each waiting group gets
//! `capacity × weight / Σ waiting weights`, and a group that is not waiting (no
//! demand, or held back by its chain admission limit) keeps only what it
//! holds, leaving the rest of its share to the others.

use std::collections::{HashMap, VecDeque};

use slotmap::SecondaryMap;

use restate_worker_api::invoker::slot_shares::{OwnedShareLane, ShareLane, ShareSubscription, SlotShares};

use crate::scheduler::VQueueHandle;

/// A lane without its weights: the key of a blocked-start queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LaneKey {
    scope: bool,
    group: String,
    lane: String,
}

impl LaneKey {
    fn of(lane: &OwnedShareLane) -> Self {
        Self {
            scope: lane.scope,
            group: lane.group.clone(),
            lane: lane.lane.clone(),
        }
    }
}

pub(super) struct ShareGate {
    shares: SlotShares,
    sub: ShareSubscription,
    /// Blocked starts and the lane each one is counted as waiting in.
    blocked: SecondaryMap<VQueueHandle, OwnedShareLane>,
    /// Per lane: the latest weights and the blocked starts in arrival order.
    /// A woken start leaves the queue but stays in `blocked` (still waiting)
    /// until it re-polls.
    lanes: HashMap<LaneKey, (OwnedShareLane, VecDeque<VQueueHandle>)>,
}

impl ShareGate {
    pub(super) fn new(shares: SlotShares) -> Self {
        let sub = shares.subscribe();
        Self {
            shares,
            sub,
            blocked: SecondaryMap::new(),
            lanes: HashMap::new(),
        }
    }

    pub(super) fn is_enabled(&self) -> bool {
        self.shares.is_enabled()
    }

    /// May this new start of `lane` proceed? If not it is registered as
    /// blocked (and as its lane waiting).
    pub(super) fn admit(&mut self, vqueue: VQueueHandle, lane: ShareLane<'_>) -> bool {
        if self.shares.may_take(lane) {
            self.remove(vqueue);
            return true;
        }
        match self.blocked.get(vqueue) {
            Some(b) if b.same_lane(lane) && b.as_lane() == lane => {
                // woken but beaten to the slot: keep its turn
                let entry = self.lanes.entry(LaneKey::of(b)).or_insert_with(|| (b.clone(), VecDeque::new()));
                if !entry.1.contains(&vqueue) {
                    entry.1.push_front(vqueue);
                }
            }
            _ => {
                self.remove(vqueue);
                self.shares.set_waiting(lane, 1);
                let owned = OwnedShareLane::from_lane(lane);
                let entry = self
                    .lanes
                    .entry(LaneKey::of(&owned))
                    .or_insert_with(|| (owned.clone(), VecDeque::new()));
                entry.0 = owned.clone();
                entry.1.push_back(vqueue);
                self.blocked.insert(vqueue, owned);
            }
        }
        false
    }

    /// Forget `vqueue` (admitted, removed, or its head changed).
    pub(super) fn remove(&mut self, vqueue: VQueueHandle) {
        if let Some(lane) = self.blocked.remove(vqueue) {
            self.shares.set_waiting(lane.as_lane(), -1);
            let key = LaneKey::of(&lane);
            if let Some((_, q)) = self.lanes.get_mut(&key) {
                if let Some(pos) = q.iter().position(|h| *h == vqueue) {
                    q.remove(pos);
                }
                if q.is_empty() {
                    self.lanes.remove(&key);
                }
            }
        }
    }

    /// Wakes blocked starts whose lane has headroom after a share change.
    pub(super) fn poll_wake(&mut self, cx: &std::task::Context<'_>, woken: &mut Vec<VQueueHandle>) {
        // always keep the waker registered: a start blocked later in this
        // tick must still be woken by the next change
        let changed = self.shares.poll_changed(&mut self.sub, cx.waker());
        if self.blocked.is_empty() || !changed {
            return;
        }
        for (lane, q) in self.lanes.values_mut() {
            let room = self.shares.headroom(lane.as_lane()) as usize;
            for _ in 0..room.min(q.len()) {
                woken.extend(q.pop_front());
            }
        }
    }
}

impl Drop for ShareGate {
    /// The shares are node-wide and outlive this partition's leadership: give
    /// back every waiting registration, or the lane would look waiting forever.
    fn drop(&mut self) {
        for (_, lane) in self.blocked.drain() {
            self.shares.set_waiting(lane.as_lane(), -1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use slotmap::SlotMap;

    use restate_worker_api::invoker::slot_shares::GroupKey;

    use super::*;

    fn lane<'a>(scope: &'a str, svc: &'a str) -> ShareLane<'a> {
        ShareLane {
            group: GroupKey::Scope(scope),
            group_weight: 1,
            lane: svc,
            lane_weight: 1,
        }
    }

    /// A start over its share blocks without taking a slot and is woken, once,
    /// when a slot of the pool is returned — not before.
    #[test]
    fn blocked_start_wakes_on_returned_slot() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let hog2 = handles.insert(());
        let shares = SlotShares::new(NonZeroUsize::new(2), true);
        let mut gate = ShareGate::new(shares.clone());
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        let (hog, rail) = (lane("hog", "Emit"), lane("rail", "Wf"));

        let hog_slot = shares.take(hog);
        shares.set_waiting(rail, 1); // the rail waits: shares apply
        let _rail_slot = shares.take(rail);
        assert!(!gate.admit(hog2, hog), "hog holds its share (1 of 2)");
        let mut woken = Vec::new();
        gate.poll_wake(&cx, &mut woken);
        assert!(woken.is_empty(), "no share change yet");

        drop(hog_slot);
        gate.poll_wake(&cx, &mut woken);
        assert_eq!(woken, vec![hog2], "the returned slot wakes the blocked start");
        woken.clear();
        gate.poll_wake(&cx, &mut woken);
        assert!(woken.is_empty(), "woken once per change");
        assert!(gate.admit(hog2, hog), "now within its share");
    }

    /// Removing a blocked start stops counting its lane as waiting, so the
    /// other lane's share grows back to the whole pool.
    #[test]
    fn removal_stops_waiting() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let hog2 = handles.insert(());
        let shares = SlotShares::new(NonZeroUsize::new(4), true);
        let mut gate = ShareGate::new(shares.clone());
        let (hog, rail) = (lane("hog", "Emit"), lane("rail", "Wf"));
        let _h: Vec<_> = (0..2).map(|_| shares.take(hog)).collect();
        let _r: Vec<_> = (0..2).map(|_| shares.take(rail)).collect();
        shares.set_waiting(rail, 1);
        assert!(!gate.admit(hog2, hog));
        assert_eq!(shares.headroom(rail), 0, "split 2/2, pool full");
        gate.remove(hog2);
        assert!(shares.may_take(rail), "hog no longer waits: rail uncapped");
    }

    /// Losing leadership drops the gate; its blocked starts must stop
    /// counting as waiting in the node-wide shares.
    #[test]
    fn drop_returns_waiting_registrations() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let hog2 = handles.insert(());
        let shares = SlotShares::new(NonZeroUsize::new(2), true);
        let (hog, rail) = (lane("hog", "Emit"), lane("rail", "Wf"));
        let _h = shares.take(hog);
        let _r = shares.take(rail);
        shares.set_waiting(rail, 1);
        let mut gate = ShareGate::new(shares.clone());
        assert!(!gate.admit(hog2, hog));
        shares.set_waiting(rail, -1);
        assert!(!shares.may_take(rail), "hog waits: rail capped at its share");
        drop(gate);
        assert!(shares.may_take(rail), "no ghost waiter after the gate is gone");
    }
}
