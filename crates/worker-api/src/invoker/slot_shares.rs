// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Weighted shares of the node's invoker slots, in two levels.
//!
//! Scheduler weights order *grants*; a lane whose invocations hold a slot
//! longer still ends up with more *slot-time*. When slots are contended (some
//! other group is waiting), a group may hold at most its weighted share of
//! the pool, and inside a group a lane at most its weighted share of the
//! group's share. Both levels are water-filled: whoever is not waiting keeps
//! what it holds, and the rest is split by weight among the waiting ones, so
//! no slot sits idle while someone waits. Shared by every partition on the
//! node.
//!
//! A **group** is a scope (or an unscoped service, which is its own group),
//! mirroring the scheduler's group ring; a **lane** is a service inside it.
//! Children of a chain run in their parent's scope, so their slots count
//! against the scope's share: a scope's fan-out uses up the scope's share,
//! never another scope's.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::task::Waker;

use metrics::{Counter, Gauge, counter, gauge};

use super::notify::{ChangeNotifier, ChangeSubscription, wake_all};

const SLOT_SHARE_HELD: &str = "restate.invoker.slot_share.held";
const SLOT_SHARE_GROUP_HELD: &str = "restate.invoker.slot_share.group_held";
const SLOT_SHARE_CAPPED_TOTAL: &str = "restate.invoker.slot_share.capped.total";

/// The group a lane belongs to: its scope, or, unscoped, the service itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupKey<'a> {
    Scope(&'a str),
    Service(&'a str),
}

impl GroupKey<'_> {
    fn name(&self) -> &str {
        match self {
            GroupKey::Scope(s) | GroupKey::Service(s) => s,
        }
    }

    fn is_scope(&self) -> bool {
        matches!(self, GroupKey::Scope(_))
    }

    /// Metric label: a scope by name, an unscoped service as `/service`.
    fn label(&self) -> String {
        match self {
            GroupKey::Scope(s) => (*s).to_owned(),
            GroupKey::Service(s) => format!("/{s}"),
        }
    }
}

/// A lane and its weights, as the shares see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareLane<'a> {
    pub group: GroupKey<'a>,
    pub group_weight: u32,
    pub lane: &'a str,
    pub lane_weight: u32,
}

/// Owned form of [`ShareLane`], for bookkeeping off the hot path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnedShareLane {
    pub scope: bool,
    pub group: String,
    pub group_weight: u32,
    pub lane: String,
    pub lane_weight: u32,
}

impl OwnedShareLane {
    pub fn from_lane(lane: ShareLane<'_>) -> Self {
        Self {
            scope: lane.group.is_scope(),
            group: lane.group.name().to_owned(),
            group_weight: lane.group_weight,
            lane: lane.lane.to_owned(),
            lane_weight: lane.lane_weight,
        }
    }

    pub fn as_lane(&self) -> ShareLane<'_> {
        ShareLane {
            group: if self.scope {
                GroupKey::Scope(&self.group)
            } else {
                GroupKey::Service(&self.group)
            },
            group_weight: self.group_weight,
            lane: &self.lane,
            lane_weight: self.lane_weight,
        }
    }

    /// Same lane, ignoring the weights.
    pub fn same_lane(&self, other: ShareLane<'_>) -> bool {
        self.scope == other.group.is_scope()
            && self.group == other.group.name()
            && self.lane == other.lane
    }
}

#[derive(Debug)]
struct Lane {
    held: u32,
    waiting: u32,
    weight: u32,
    m_held: Gauge,
    m_capped: Counter,
}

#[derive(Debug)]
struct Group {
    name: Arc<str>,
    label: String,
    held: u32,
    waiting: u32,
    weight: u32,
    lanes: HashMap<Arc<str>, Lane>,
    m_held: Gauge,
}

impl Group {
    /// The lane's interned name, creating the lane on first sight.
    fn ensure_lane(&mut self, lane: &str, weight: u32) -> Arc<str> {
        let key: Arc<str> = match self.lanes.get_key_value(lane) {
            Some((k, _)) => k.clone(),
            None => {
                let key: Arc<str> = Arc::from(lane);
                let labels = [
                    ("scope", self.label.clone()),
                    ("lane", format!("{}/{lane}", self.label)),
                ];
                self.lanes.insert(
                    key.clone(),
                    Lane {
                        held: 0,
                        waiting: 0,
                        weight: weight.max(1),
                        m_held: gauge!(SLOT_SHARE_HELD, &labels),
                        m_capped: counter!(SLOT_SHARE_CAPPED_TOTAL, &labels),
                    },
                );
                key
            }
        };
        self.lanes.get_mut(&*key).expect("present").weight = weight.max(1);
        key
    }
}

#[derive(Debug)]
struct State {
    capacity: u32,
    scopes: HashMap<Arc<str>, Group>,
    services: HashMap<Arc<str>, Group>,
    /// Bumped whenever a share can have grown (a slot returned, a lane's
    /// waiting set changed); subscribers re-check their blocked starts.
    notify: ChangeNotifier,
}

/// The share a lane may hold right now, and how much its group may hold.
struct Shares {
    lane_share: u64,
    group_share: u64,
    lane_held: u64,
    group_held: u64,
    free: u64,
}

impl State {
    fn groups(&self) -> impl Iterator<Item = (bool, &Group)> {
        self.scopes
            .values()
            .map(|g| (true, g))
            .chain(self.services.values().map(|g| (false, g)))
    }

    fn map_mut(&mut self, scope: bool) -> &mut HashMap<Arc<str>, Group> {
        if scope {
            &mut self.scopes
        } else {
            &mut self.services
        }
    }

    fn group(&self, key: GroupKey<'_>) -> Option<&Group> {
        if key.is_scope() {
            self.scopes.get(key.name())
        } else {
            self.services.get(key.name())
        }
    }

    fn group_mut(&mut self, key: GroupKey<'_>, weight: u32) -> &mut Group {
        let map = self.map_mut(key.is_scope());
        if !map.contains_key(key.name()) {
            let name: Arc<str> = Arc::from(key.name());
            let label = key.label();
            map.insert(
                name.clone(),
                Group {
                    name,
                    m_held: gauge!(SLOT_SHARE_GROUP_HELD, "scope" => label.clone()),
                    label,
                    held: 0,
                    waiting: 0,
                    weight: weight.max(1),
                    lanes: HashMap::new(),
                },
            );
        }
        let g = map.get_mut(key.name()).expect("inserted above");
        g.weight = weight.max(1);
        g
    }

    fn gc(&mut self, scope: bool, group: &str, lane: &str) {
        let map = self.map_mut(scope);
        if let Some(g) = map.get_mut(group) {
            if g
                .lanes
                .get(lane)
                .is_some_and(|l| l.held == 0 && l.waiting == 0)
            {
                g.lanes.remove(lane);
            }
            if g.held == 0 && g.waiting == 0 && g.lanes.is_empty() {
                map.remove(group);
            }
        }
    }

    /// Two-level water-fill for `lane`. Level 1 splits the pool across the
    /// waiting groups by group weight; groups that are not waiting keep what
    /// they hold. Level 2 splits the group's share across its waiting lanes
    /// by lane weight; lanes that are not waiting keep what they hold.
    fn shares(&self, lane: ShareLane<'_>) -> Shares {
        let total_held: u64 = self.groups().map(|(_, g)| g.held as u64).sum();
        let free = (self.capacity as u64).saturating_sub(total_held);
        let me = self.group(lane.group);
        let group_held = me.map(|g| g.held as u64).unwrap_or(0);
        let lane_held = me
            .and_then(|g| g.lanes.get(lane.lane))
            .map(|l| l.held as u64)
            .unwrap_or(0);
        let is_me =
            |scope: bool, g: &Group| scope == lane.group.is_scope() && &*g.name == lane.group.name();

        // level 1: groups
        let mut others_waiting = false;
        let mut reserved: u64 = 0;
        let mut waiting_weight: u64 = lane.group_weight.max(1) as u64;
        for (scope, g) in self.groups() {
            if is_me(scope, g) {
                continue;
            }
            if g.waiting > 0 {
                others_waiting = true;
                waiting_weight += g.weight.max(1) as u64;
            } else {
                reserved += g.held as u64;
            }
        }
        let group_share = if others_waiting {
            let remaining = (self.capacity as u64).saturating_sub(reserved);
            (remaining * lane.group_weight.max(1) as u64)
                .div_ceil(waiting_weight)
                .max(1)
        } else {
            // no contention: the pool's own limit is the only bound
            u64::MAX
        };

        // level 2: lanes within the group
        let mut lane_others_waiting = false;
        let mut lane_reserved: u64 = 0;
        let mut lane_waiting_weight: u64 = lane.lane_weight.max(1) as u64;
        if let Some(g) = me {
            for (name, l) in &g.lanes {
                if &**name == lane.lane {
                    continue;
                }
                if l.waiting > 0 {
                    lane_others_waiting = true;
                    lane_waiting_weight += l.weight.max(1) as u64;
                } else {
                    lane_reserved += l.held as u64;
                }
            }
        }
        let lane_share = if lane_others_waiting {
            let remaining = group_share.saturating_sub(lane_reserved);
            (remaining * lane.lane_weight.max(1) as u64)
                .div_ceil(lane_waiting_weight)
                .max(1)
        } else {
            // no other lane of the group waits: the group check is the bound
            group_share
        };

        Shares {
            lane_share,
            group_share,
            lane_held,
            group_held,
            free,
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
                scopes: HashMap::new(),
                services: HashMap::new(),
                notify: ChangeNotifier::default(),
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

    /// May `lane` take one more slot right now? Both its own share and its
    /// group's share must have room.
    pub fn may_take(&self, lane: ShareLane<'_>) -> bool {
        let Some(state) = &self.0 else {
            return true;
        };
        let st = state.lock().expect("slot shares lock poisoned");
        let s = st.shares(lane);
        let ok = s.lane_held < s.lane_share && s.group_held < s.group_share;
        if !ok && let Some(l) = st.group(lane.group).and_then(|g| g.lanes.get(lane.lane)) {
            l.m_capped.increment(1);
        }
        ok
    }

    /// How many more slots `lane` may take right now: the smaller of its own
    /// and its group's remaining share, bounded by the free capacity.
    /// Unbounded (u32::MAX) when shares are disabled.
    pub fn headroom(&self, lane: ShareLane<'_>) -> u32 {
        let Some(state) = &self.0 else {
            return u32::MAX;
        };
        let st = state.lock().expect("slot shares lock poisoned");
        let s = st.shares(lane);
        s.lane_share
            .saturating_sub(s.lane_held)
            .min(s.group_share.saturating_sub(s.group_held))
            .min(s.free)
            .min(u32::MAX as u64) as u32
    }

    /// Subscribes a partition scheduler to share changes.
    pub fn subscribe(&self) -> ShareSubscription {
        match &self.0 {
            Some(state) => state
                .lock()
                .expect("slot shares lock poisoned")
                .notify
                .subscribe(),
            None => ChangeSubscription::detached(),
        }
    }

    /// True (once per change) if a share may have grown since the last call;
    /// registers `waker` to be woken on the next change.
    pub fn poll_changed(&self, sub: &mut ShareSubscription, waker: &Waker) -> bool {
        match &self.0 {
            Some(state) => state
                .lock()
                .expect("slot shares lock poisoned")
                .notify
                .poll_changed(sub, waker),
            None => false,
        }
    }

    /// Records one slot taken by `lane`; the returned lease gives it back.
    /// Never blocks: children of running chains take without a check.
    pub fn take(&self, lane: ShareLane<'_>) -> SlotLease {
        let Some(state) = &self.0 else {
            return SlotLease(None);
        };
        let mut st = state.lock().expect("slot shares lock poisoned");
        let scope = lane.group.is_scope();
        let g = st.group_mut(lane.group, lane.group_weight);
        g.held += 1;
        g.m_held.set(g.held as f64);
        let group_name = g.name.clone();
        let lane_name = g.ensure_lane(lane.lane, lane.lane_weight);
        let l = g.lanes.get_mut(&*lane_name).expect("ensured");
        l.held += 1;
        l.m_held.set(l.held as f64);
        SlotLease(Some(Held {
            state: state.clone(),
            scope,
            group: group_name,
            lane: lane_name,
        }))
    }

    /// A queue of `lane` started (`+1`) or stopped (`-1`) waiting for a slot.
    pub fn set_waiting(&self, lane: ShareLane<'_>, delta: i32) {
        let Some(state) = &self.0 else {
            return;
        };
        let wake = {
            let mut st = state.lock().expect("slot shares lock poisoned");
            let scope = lane.group.is_scope();
            let g = st.group_mut(lane.group, lane.group_weight);
            g.waiting = g.waiting.saturating_add_signed(delta);
            let lane_name = g.ensure_lane(lane.lane, lane.lane_weight);
            let l = g.lanes.get_mut(&*lane_name).expect("ensured");
            l.waiting = l.waiting.saturating_add_signed(delta);
            st.gc(scope, lane.group.name(), lane.lane);
            st.notify.changed()
        };
        wake_all(wake);
    }
}

/// A partition scheduler's subscription to share changes (see
/// [`SlotShares::poll_changed`]).
pub type ShareSubscription = ChangeSubscription;

#[derive(Debug)]
struct Held {
    state: Arc<Mutex<State>>,
    scope: bool,
    group: Arc<str>,
    lane: Arc<str>,
}

/// One slot held by a lane; dropping it returns the slot to the lane's and
/// its group's share.
#[derive(Debug, Default)]
#[must_use]
pub struct SlotLease(Option<Held>);

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
        if let Some(h) = self.0.take() {
            let wake = match h.state.lock() {
                Ok(mut st) => {
                    if let Some(g) = st.map_mut(h.scope).get_mut(&*h.group) {
                        g.held = g.held.saturating_sub(1);
                        g.m_held.set(g.held as f64);
                        if let Some(l) = g.lanes.get_mut(&*h.lane) {
                            l.held = l.held.saturating_sub(1);
                            l.m_held.set(l.held as f64);
                        }
                    }
                    st.gc(h.scope, &h.group, &h.lane);
                    st.notify.changed()
                }
                Err(_) => Vec::new(),
            };
            wake_all(wake);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shares(cap: usize) -> SlotShares {
        SlotShares::new(NonZeroUsize::new(cap), true)
    }

    fn lane<'a>(scope: &'a str, gw: u32, svc: &'a str, lw: u32) -> ShareLane<'a> {
        ShareLane {
            group: GroupKey::Scope(scope),
            group_weight: gw,
            lane: svc,
            lane_weight: lw,
        }
    }

    /// Takes until the lane is capped (bounded: an uncontended lane is never
    /// capped by the shares, only by the pool's own semaphore).
    fn fill(s: &SlotShares, l: ShareLane<'_>) -> Vec<SlotLease> {
        let mut v = Vec::new();
        while v.len() < 1000 && s.may_take(l) {
            v.push(s.take(l));
        }
        v
    }

    /// Without contention a lane is never capped by the shares: the pool's
    /// own semaphore is the only bound (work-conserving).
    #[test]
    fn uncontended_lane_takes_all() {
        let s = shares(10);
        let hog = lane("hog", 1, "Emit", 1);
        let leases: Vec<_> = (0..10)
            .map(|_| {
                assert!(s.may_take(hog));
                s.take(hog)
            })
            .collect();
        assert_eq!(leases.len(), 10);
        assert!(s.may_take(hog), "no one waits: nothing to share");
        assert_eq!(s.headroom(hog), 0, "but the pool is full");
    }

    /// Under contention two scopes split the pool by scope weight.
    #[test]
    fn contended_scopes_split_by_weight() {
        let s = shares(12);
        let hog = lane("hog", 1, "Emit", 1);
        let rail = lane("payment", 2, "Workflow", 1);
        s.set_waiting(hog, 1);
        s.set_waiting(rail, 1);
        let mut hogs = fill(&s, hog);
        assert_eq!(hogs.len(), 4, "weight 1 of 3 over 12 slots");
        let rails = fill(&s, rail);
        assert_eq!(rails.len(), 8, "weight 2 of 3 over 12 slots");
        // releasing a hog slot lets the hog (and only the hog) back in
        drop(hogs.pop());
        assert!(s.may_take(hog));
        assert!(!s.may_take(rail));
    }

    /// Two roots in one scope split that scope's weight instead of doubling
    /// it: payment (10) against indexing (5) stays 2:1 with two payment roots.
    #[test]
    fn two_roots_share_their_scope_weight() {
        let s = shares(30);
        let pay_a = lane("payment", 10, "WorkflowA", 1);
        let pay_b = lane("payment", 10, "WorkflowB", 1);
        let idx = lane("indexing", 5, "Indexer", 1);
        s.set_waiting(pay_a, 1);
        s.set_waiting(pay_b, 1);
        s.set_waiting(idx, 1);
        let a = fill(&s, pay_a);
        let b = fill(&s, pay_b);
        let i = fill(&s, idx);
        assert_eq!(a.len() + b.len(), 20, "payment gets 2/3 of the pool");
        assert_eq!(a.len(), 10, "split evenly between two roots of lane weight 1");
        assert_eq!(b.len(), 10);
        assert_eq!(i.len(), 10, "indexing gets 1/3");
    }

    /// Children run in their parent's scope and are charged to it: a scope
    /// busy with children leaves less for its own new starts, not for the
    /// other scopes.
    #[test]
    fn children_are_charged_to_their_scope() {
        let s = shares(12);
        let root = lane("payment", 1, "Workflow", 1);
        let child = lane("payment", 1, "Sepa", 1);
        let idx = lane("indexing", 1, "Indexer", 1);
        s.set_waiting(root, 1);
        s.set_waiting(idx, 1);
        // children never check; they take in their own lane of the scope
        let kids: Vec<_> = (0..5).map(|_| s.take(child)).collect();
        let roots = fill(&s, root);
        assert_eq!(roots.len(), 1, "payment's share is 6, children hold 5");
        let others = fill(&s, idx);
        assert_eq!(others.len(), 6, "indexing keeps its full half");
        drop((kids, roots, others));
    }

    /// A scope that stops waiting keeps its slots out of the split, so the
    /// waiting scopes share only what is left — no idle capacity.
    #[test]
    fn non_waiting_holders_are_reserved_not_shared() {
        let s = shares(10);
        let idle = lane("idle", 1, "Svc", 1);
        let a = lane("a", 1, "A", 1);
        let b = lane("b", 1, "B", 1);
        let _idle: Vec<_> = (0..6).map(|_| s.take(idle)).collect();
        s.set_waiting(a, 1);
        s.set_waiting(b, 1);
        let got = fill(&s, a);
        assert_eq!(got.len(), 2, "half of the remaining 4");
    }

    /// An unscoped service is its own group, and never collides with a scope
    /// of the same name.
    #[test]
    fn unscoped_service_is_its_own_group() {
        let s = shares(8);
        let scoped = lane("payment", 1, "Workflow", 1);
        let unscoped = ShareLane {
            group: GroupKey::Service("payment"),
            group_weight: 1,
            lane: "payment",
            lane_weight: 1,
        };
        s.set_waiting(scoped, 1);
        s.set_waiting(unscoped, 1);
        let a = fill(&s, scoped);
        let b = fill(&s, unscoped);
        assert_eq!((a.len(), b.len()), (4, 4), "two groups, not one");
    }

    /// Disabled shares never cap.
    #[test]
    fn disabled_is_transparent() {
        let s = SlotShares::disabled();
        let rail = lane("rail", 1, "R", 1);
        let hog = lane("hog", 1, "H", 1);
        s.set_waiting(rail, 1);
        assert!(s.may_take(hog));
        let _l = s.take(hog);
    }

    /// Headroom is the share minus what the lane holds, never more than the
    /// free capacity; a returned slot is announced to subscribers.
    #[test]
    fn headroom_and_change_notification() {
        let s = shares(12);
        let hog = lane("hog", 1, "Emit", 1);
        let rail = lane("payment", 2, "Workflow", 1);
        let mut sub = s.subscribe();
        let w = std::task::Waker::noop();
        assert!(!s.poll_changed(&mut sub, w), "nothing changed yet");
        s.set_waiting(hog, 1);
        s.set_waiting(rail, 1);
        assert!(s.poll_changed(&mut sub, w), "waiting set changed");
        assert_eq!(s.headroom(hog), 4, "share 4 of 12, holds 0");
        let hogs: Vec<_> = (0..4).map(|_| s.take(hog)).collect();
        assert_eq!(s.headroom(hog), 0);
        assert_eq!(s.headroom(rail), 8);
        let _rails: Vec<_> = (0..8).map(|_| s.take(rail)).collect();
        assert_eq!(s.headroom(rail), 0, "no free capacity left");
        assert!(!s.poll_changed(&mut sub, w), "takes do not grow a share");
        drop(hogs);
        assert!(s.poll_changed(&mut sub, w), "returned slots are announced");
        assert_eq!(s.headroom(hog), 4);
    }
}
