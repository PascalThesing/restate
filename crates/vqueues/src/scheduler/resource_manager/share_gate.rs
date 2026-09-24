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
//! A new chain start (a root's first run) whose lane already holds its
//! weighted share while other lanes wait is blocked *here*, before it asks the
//! invoker for a slot. It sleeps until the node's shares change (a slot is
//! returned or the waiting set changes) and is then woken only if its lane has
//! headroom. Parking it in the invoker queue instead made the scheduler
//! re-poll every capped start on every freed slot.
//!
//! The weight is a distribution, not a count: each waiting lane gets
//! `capacity × weight / Σ waiting weights`, and a lane that is not waiting (no
//! demand, or held back by its chain admission limit) keeps only what it
//! holds, leaving the rest of its share to the others.

use std::collections::{HashMap, VecDeque};

use slotmap::SecondaryMap;

use restate_worker_api::invoker::slot_shares::{ShareSubscription, SlotShares};

use crate::scheduler::VQueueHandle;

pub(super) struct ShareGate {
    shares: SlotShares,
    sub: ShareSubscription,
    /// Blocked starts and the lane each one is counted as waiting in.
    blocked: SecondaryMap<VQueueHandle, (String, u32)>,
    /// Per lane: weight and the blocked starts in arrival order. A woken start
    /// leaves the queue but stays in `blocked` (still waiting) until it
    /// re-polls.
    lanes: HashMap<String, (u32, VecDeque<VQueueHandle>)>,
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

    /// May this new chain start of `lane` proceed to the invoker? If not it
    /// is registered as blocked (and as its lane waiting).
    pub(super) fn admit(&mut self, vqueue: VQueueHandle, lane: (String, u32)) -> bool {
        if self.shares.may_take(&lane.0, lane.1) {
            self.remove(vqueue);
            return true;
        }
        match self.blocked.get(vqueue) {
            Some((key, weight)) if *key == lane.0 && *weight == lane.1 => {
                // woken but beaten to the slot: keep its turn
                let entry = self.lanes.entry(lane.0).or_default();
                entry.0 = lane.1;
                if !entry.1.contains(&vqueue) {
                    entry.1.push_front(vqueue);
                }
            }
            _ => {
                self.remove(vqueue);
                self.shares.set_waiting(&lane.0, lane.1, 1);
                let entry = self.lanes.entry(lane.0.clone()).or_default();
                entry.0 = lane.1;
                entry.1.push_back(vqueue);
                self.blocked.insert(vqueue, lane);
            }
        }
        false
    }

    /// Forget `vqueue` (admitted, removed, or its head changed).
    pub(super) fn remove(&mut self, vqueue: VQueueHandle) {
        if let Some((key, weight)) = self.blocked.remove(vqueue) {
            self.shares.set_waiting(&key, weight, -1);
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
        for (key, (weight, q)) in self.lanes.iter_mut() {
            let room = self.shares.headroom(key, *weight) as usize;
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
        for (_, (key, weight)) in self.blocked.drain() {
            self.shares.set_waiting(&key, weight, -1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use slotmap::SlotMap;

    use super::*;

    /// A start over its share blocks without taking a slot and is woken, once,
    /// when a slot of the pool is returned — not before.
    #[test]
    fn blocked_start_wakes_on_returned_slot() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (hog2, rail) = (handles.insert(()), handles.insert(()));
        let shares = SlotShares::new(NonZeroUsize::new(2), true);
        let mut gate = ShareGate::new(shares.clone());
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        let hog = ("s/hog".to_owned(), 1);

        let hog_slot = shares.take(&hog.0, hog.1);
        shares.set_waiting("s/rail", 1, 1); // the rail waits: shares apply
        let _rail_slot = shares.take("s/rail", 1);
        assert!(!gate.admit(hog2, hog.clone()), "hog holds its share (1 of 2)");
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
        let _ = rail;
    }

    /// Removing a blocked start stops counting its lane as waiting, so the
    /// other lane's share grows back to the whole pool.
    #[test]
    fn removal_stops_waiting() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let hog2 = handles.insert(());
        let shares = SlotShares::new(NonZeroUsize::new(4), true);
        let mut gate = ShareGate::new(shares.clone());
        let _h: Vec<_> = (0..2).map(|_| shares.take("s/hog", 1)).collect();
        let _r: Vec<_> = (0..2).map(|_| shares.take("s/rail", 1)).collect();
        shares.set_waiting("s/rail", 1, 1);
        assert!(!gate.admit(hog2, ("s/hog".to_owned(), 1)));
        assert_eq!(shares.headroom("s/rail", 1), 0, "split 2/2, pool full");
        gate.remove(hog2);
        assert!(shares.may_take("s/rail", 1), "hog no longer waits: rail uncapped");
    }

    /// Losing leadership drops the gate; its blocked starts must stop
    /// counting as waiting in the node-wide shares.
    #[test]
    fn drop_returns_waiting_registrations() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let hog2 = handles.insert(());
        let shares = SlotShares::new(NonZeroUsize::new(2), true);
        let _h = shares.take("s/hog", 1);
        let _r = shares.take("s/rail", 1);
        shares.set_waiting("s/rail", 1, 1);
        let mut gate = ShareGate::new(shares.clone());
        assert!(!gate.admit(hog2, ("s/hog".to_owned(), 1)));
        shares.set_waiting("s/rail", 1, -1);
        assert!(!shares.may_take("s/rail", 1), "hog waits: rail capped at its share");
        drop(gate);
        assert!(shares.may_take("s/rail", 1), "no ghost waiter after the gate is gone");
    }
}
