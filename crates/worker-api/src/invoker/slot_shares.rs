// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Weighted shares of the node's invoker slots.
//!
//! Scheduler weights order *grants*; a lane whose invocations hold a slot
//! longer still ends up with more *slot-time*. When slots are contended (some
//! other lane is waiting), a lane may hold at most its weighted share. Shares
//! are water-filled: lanes that are not waiting keep what they hold, and the
//! remaining capacity is split by weight among the waiting lanes, so no slot
//! sits idle while someone waits. Shared by every partition on the node.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};
use std::task::Waker;

use futures::task::AtomicWaker;

use metrics::{counter, gauge};

const SLOT_SHARE_HELD: &str = "restate.invoker.slot_share.held";
const SLOT_SHARE_CAPPED_TOTAL: &str = "restate.invoker.slot_share.capped.total";

#[derive(Debug, Default)]
struct Lane {
    held: u32,
    waiting: u32,
    weight: u32,
}

#[derive(Debug)]
struct State {
    capacity: u32,
    lanes: HashMap<String, Lane>,
    /// Bumped whenever a share can have grown (a slot returned, a lane's
    /// waiting set changed); subscribers re-check their blocked starts.
    generation: u64,
    subscribers: Vec<Weak<AtomicWaker>>,
}

impl State {
    /// Records a change; returns the subscribers to wake once the lock is
    /// released (never wake while holding it).
    #[must_use]
    fn changed(&mut self) -> Vec<Arc<AtomicWaker>> {
        self.generation = self.generation.wrapping_add(1);
        let mut live = Vec::with_capacity(self.subscribers.len());
        self.subscribers.retain(|w| match w.upgrade() {
            Some(waker) => {
                live.push(waker);
                true
            }
            None => false,
        });
        live
    }

    fn gc(&mut self, key: &str) {
        if self
            .lanes
            .get(key)
            .is_some_and(|l| l.held == 0 && l.waiting == 0)
        {
            self.lanes.remove(key);
        }
    }
}

/// Node-wide slot-share accounting. Cheap to clone; disabled = no-op.
#[derive(Debug, Clone, Default)]
pub struct SlotShares(Option<Arc<Mutex<State>>>);

impl SlotShares {
    /// `capacity` is the node's invoker slot limit; `None` (unlimited) or
    /// `enabled = false` disables shares.
    pub fn new(capacity: Option<NonZeroUsize>, enabled: bool) -> Self {
        match (capacity, enabled) {
            (Some(c), true) => Self(Some(Arc::new(Mutex::new(State {
                capacity: c.get().min(u32::MAX as usize) as u32,
                lanes: HashMap::new(),
                generation: 0,
                subscribers: Vec::new(),
            })))),
            _ => Self(None),
        }
    }

    pub const fn disabled() -> Self {
        Self(None)
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// May `lane` take one more slot right now?
    pub fn may_take(&self, lane: &str, weight: u32) -> bool {
        let Some(state) = &self.0 else {
            return true;
        };
        let st = state.lock().expect("slot shares lock poisoned");
        let others_waiting = st
            .lanes
            .iter()
            .any(|(k, l)| k.as_str() != lane && l.waiting > 0);
        if !others_waiting {
            return true;
        }
        let held = st.lanes.get(lane).map(|l| l.held).unwrap_or(0);
        // water-fill: lanes that are not waiting keep what they hold
        let mut reserved: u64 = 0;
        let mut waiting_weight: u64 = weight.max(1) as u64;
        for (k, l) in &st.lanes {
            if k.as_str() == lane {
                continue;
            }
            if l.waiting > 0 {
                waiting_weight += l.weight.max(1) as u64;
            } else {
                reserved += l.held as u64;
            }
        }
        let remaining = (st.capacity as u64).saturating_sub(reserved);
        let share = (remaining * weight.max(1) as u64).div_ceil(waiting_weight).max(1);
        let ok = (held as u64) < share;
        if !ok {
            counter!(SLOT_SHARE_CAPPED_TOTAL, "lane" => lane.to_owned()).increment(1);
        }
        ok
    }

    /// How many more slots `lane` may take right now: its weighted share minus
    /// what it holds, bounded by the free capacity. Unbounded (u32::MAX) when
    /// shares are disabled.
    pub fn headroom(&self, lane: &str, weight: u32) -> u32 {
        let Some(state) = &self.0 else {
            return u32::MAX;
        };
        let st = state.lock().expect("slot shares lock poisoned");
        let total_held: u64 = st.lanes.values().map(|l| l.held as u64).sum();
        let free = (st.capacity as u64).saturating_sub(total_held);
        let held = st.lanes.get(lane).map(|l| l.held).unwrap_or(0) as u64;
        let others_waiting = st
            .lanes
            .iter()
            .any(|(k, l)| k.as_str() != lane && l.waiting > 0);
        if !others_waiting {
            return free.min(u32::MAX as u64) as u32;
        }
        let mut reserved: u64 = 0;
        let mut waiting_weight: u64 = weight.max(1) as u64;
        for (k, l) in &st.lanes {
            if k.as_str() == lane {
                continue;
            }
            if l.waiting > 0 {
                waiting_weight += l.weight.max(1) as u64;
            } else {
                reserved += l.held as u64;
            }
        }
        let remaining = (st.capacity as u64).saturating_sub(reserved);
        let share = (remaining * weight.max(1) as u64).div_ceil(waiting_weight).max(1);
        share.saturating_sub(held).min(free).min(u32::MAX as u64) as u32
    }

    /// Subscribes a partition scheduler to share changes.
    pub fn subscribe(&self) -> ShareSubscription {
        let waker = Arc::new(AtomicWaker::new());
        let seen = match &self.0 {
            Some(state) => {
                let mut st = state.lock().expect("slot shares lock poisoned");
                st.subscribers.push(Arc::downgrade(&waker));
                st.generation
            }
            None => 0,
        };
        ShareSubscription { waker, seen }
    }

    /// True (once per change) if a share may have grown since the last call;
    /// registers `waker` to be woken on the next change.
    pub fn poll_changed(&self, sub: &mut ShareSubscription, waker: &Waker) -> bool {
        let Some(state) = &self.0 else {
            return false;
        };
        sub.waker.register(waker);
        let generation = state.lock().expect("slot shares lock poisoned").generation;
        if generation != sub.seen {
            sub.seen = generation;
            true
        } else {
            false
        }
    }

    /// Records one slot taken by `lane`; the returned lease gives it back.
    pub fn take(&self, lane: &str, weight: u32) -> SlotLease {
        let Some(state) = &self.0 else {
            return SlotLease(None);
        };
        let mut st = state.lock().expect("slot shares lock poisoned");
        let l = st.lanes.entry(lane.to_owned()).or_default();
        l.held += 1;
        l.weight = weight.max(1);
        gauge!(SLOT_SHARE_HELD, "lane" => lane.to_owned()).set(l.held as f64);
        SlotLease(Some((state.clone(), lane.to_owned())))
    }

    /// A queue of `lane` started (`+1`) or stopped (`-1`) waiting for a slot.
    pub fn set_waiting(&self, lane: &str, weight: u32, delta: i32) {
        let Some(state) = &self.0 else {
            return;
        };
        let wake = {
            let mut st = state.lock().expect("slot shares lock poisoned");
            let l = st.lanes.entry(lane.to_owned()).or_default();
            l.weight = weight.max(1);
            l.waiting = l.waiting.saturating_add_signed(delta);
            st.gc(lane);
            st.changed()
        };
        wake.iter().for_each(|w| w.wake());
    }
}

/// A partition scheduler's subscription to share changes (see
/// [`SlotShares::poll_changed`]).
#[derive(Debug)]
pub struct ShareSubscription {
    waker: Arc<AtomicWaker>,
    seen: u64,
}

/// One slot held by a lane; dropping it returns the slot to the lane's share.
#[derive(Debug, Default)]
#[must_use]
pub struct SlotLease(Option<(Arc<Mutex<State>>, String)>);

impl SlotLease {
    pub fn empty() -> Self {
        Self(None)
    }

    pub fn take(&mut self) -> SlotLease {
        SlotLease(self.0.take())
    }
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        if let Some((state, lane)) = self.0.take() {
            let wake = match state.lock() {
                Ok(mut st) => {
                    if let Some(l) = st.lanes.get_mut(&lane) {
                        l.held = l.held.saturating_sub(1);
                        gauge!(SLOT_SHARE_HELD, "lane" => lane.clone()).set(l.held as f64);
                    }
                    st.gc(&lane);
                    st.changed()
                }
                Err(_) => Vec::new(),
            };
            wake.iter().for_each(|w| w.wake());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares(cap: usize) -> SlotShares {
        SlotShares::new(NonZeroUsize::new(cap), true)
    }

    /// Without contention a lane takes everything (work-conserving).
    #[test]
    fn uncontended_lane_takes_all() {
        let s = shares(10);
        let leases: Vec<_> = (0..10)
            .map(|_| {
                assert!(s.may_take("hog", 1));
                s.take("hog", 1)
            })
            .collect();
        assert_eq!(leases.len(), 10);
    }

    /// Under contention a lane is capped at its weighted share of what the
    /// waiting lanes can use.
    #[test]
    fn contended_lanes_split_by_weight() {
        let s = shares(12);
        s.set_waiting("hog", 1, 1);
        s.set_waiting("rail", 2, 1);
        let mut hog = Vec::new();
        while s.may_take("hog", 1) {
            hog.push(s.take("hog", 1));
        }
        assert_eq!(hog.len(), 4, "weight 1 of 3 over 12 slots");
        let mut rail = Vec::new();
        while s.may_take("rail", 2) {
            rail.push(s.take("rail", 2));
        }
        assert_eq!(rail.len(), 8, "weight 2 of 3 over 12 slots");
        // releasing a hog slot lets the hog (and only the hog) back in
        drop(hog.pop());
        assert!(s.may_take("hog", 1));
    }

    /// A lane that stops waiting keeps its slots out of the split, so the
    /// waiting lanes share only what is left — no idle capacity.
    #[test]
    fn non_waiting_holders_are_reserved_not_shared() {
        let s = shares(10);
        let _idle: Vec<_> = (0..6).map(|_| s.take("idle", 1)).collect();
        s.set_waiting("a", 1, 1);
        s.set_waiting("b", 1, 1);
        let mut a = Vec::new();
        while s.may_take("a", 1) {
            a.push(s.take("a", 1));
        }
        assert_eq!(a.len(), 2, "half of the remaining 4");
    }

    /// Disabled shares never cap.
    #[test]
    fn disabled_is_transparent() {
        let s = SlotShares::disabled();
        s.set_waiting("rail", 1, 1);
        assert!(s.may_take("hog", 1));
        let _l = s.take("hog", 1);
    }

    /// Headroom is the share minus what the lane holds, never more than the
    /// free capacity; a returned slot is announced to subscribers.
    #[test]
    fn headroom_and_change_notification() {
        let s = shares(12);
        let mut sub = s.subscribe();
        let w = std::task::Waker::noop();
        assert!(!s.poll_changed(&mut sub, w), "nothing changed yet");
        s.set_waiting("hog", 1, 1);
        s.set_waiting("rail", 2, 1);
        assert!(s.poll_changed(&mut sub, w), "waiting set changed");
        assert_eq!(s.headroom("hog", 1), 4, "share 4 of 12, holds 0");
        let hog: Vec<_> = (0..4).map(|_| s.take("hog", 1)).collect();
        assert_eq!(s.headroom("hog", 1), 0);
        assert_eq!(s.headroom("rail", 2), 8);
        let _rail: Vec<_> = (0..8).map(|_| s.take("rail", 2)).collect();
        assert_eq!(s.headroom("rail", 2), 0, "no free capacity left");
        assert!(!s.poll_changed(&mut sub, w), "takes do not grow a share");
        drop(hog);
        assert!(s.poll_changed(&mut sub, w), "returned slots are announced");
        assert_eq!(s.headroom("hog", 1), 4);
    }
}
