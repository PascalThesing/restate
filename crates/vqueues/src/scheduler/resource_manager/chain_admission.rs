// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Automatic admission control per *chain*, the partition-local half.
//!
//! A chain is a root invocation (nothing called it) plus everything it calls.
//! Each (scope, root service) has one node-wide Gradient2 controller (see
//! [`ChainNode`]) whose sample is the chain's active end-to-end time and whose
//! limit caps chains in progress on the node. A chain permit is held for the
//! chain's lifetime — never re-acquired on resume — so a parent can never wait
//! behind its own cap. While the root waits on an external future (or is
//! paused) the permit is released and the clock paused.
//!
//! This side keeps what is partition-local: each held permit's clock and
//! whether the chain took its invoker slot, and the queues waiting for a
//! permit. Roots are recognised from the durable `EntryMetadata::chain_root`
//! marker, so a new leader gates the backlog it inherits. Chains that were
//! already under way are admitted unconditionally when they next run (and
//! counted), so new starts wait until they drain.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use metrics::{Gauge, gauge};
use tokio::time::Instant;

use restate_types::vqueues::EntryId;
use restate_types::{Scope, ServiceName};
use restate_worker_api::invoker::ChangeSubscription;
use restate_worker_api::invoker::chain_metrics::CHAIN_WAITERS;
use restate_worker_api::invoker::chain_node::{ChainKey, ChainNode};
use restate_worker_api::invoker::gradient2::UpdateOutcome;
use restate_worker_api::resources::{ChainSignal, ChainSignalKind};

use crate::scheduler::VQueueHandle;

pub(super) enum Admit {
    /// Not a gated entry (child, running chain, or admission disabled).
    NotGated,
    /// Admitted; the caller records the entry in its permit for revert.
    Admitted,
    /// The chain's limit is used up; the queue was parked.
    Blocked(ChainKey),
}

struct Held {
    key: ChainKey,
    /// `None` while paused (external wait or paused invocation).
    segment_started: Option<Instant>,
    active: Duration,
    /// The chain took its invoker slot in this segment (counts as running).
    started: bool,
}

struct Waiters {
    queue: VecDeque<VQueueHandle>,
    m_waiters: Gauge,
}

impl Waiters {
    fn report(&self) {
        self.m_waiters.set(self.queue.len() as f64);
    }
}

pub(super) struct ChainAdmission {
    node: ChainNode,
    sub: ChangeSubscription,
    partition_label: String,
    held: HashMap<EntryId, Held>,
    waiters: HashMap<ChainKey, Waiters>,
}

impl ChainAdmission {
    pub(super) fn new(node: ChainNode, partition_label: String) -> Self {
        let sub = node.subscribe();
        Self {
            node,
            sub,
            partition_label,
            held: HashMap::new(),
            waiters: HashMap::new(),
        }
    }

    fn waiters(&mut self, key: &ChainKey) -> &mut Waiters {
        if !self.waiters.contains_key(key) {
            let m_waiters = gauge!(CHAIN_WAITERS, "scope" => key.scope_label(), "root" => key.root.to_string(), "partition_id" => self.partition_label.clone());
            self.waiters.insert(
                key.clone(),
                Waiters {
                    queue: VecDeque::new(),
                    m_waiters,
                },
            );
        }
        self.waiters.get_mut(key).expect("inserted above")
    }

    /// The chain key of a root entry, if admission applies to it.
    pub(super) fn key_of(&self, chain_root: Option<&str>, scope: Option<&Scope>) -> Option<ChainKey> {
        if !self.node.is_enabled() {
            return None;
        }
        chain_root.map(|root| ChainKey {
            scope: scope.cloned(),
            root: ServiceName::new(root),
        })
    }

    /// Admission check for the inbox head of `vqueue`. `first_run` is false
    /// for resumes.
    pub(super) fn poll_admit(
        &mut self,
        vqueue: VQueueHandle,
        entry_id: EntryId,
        key: Option<ChainKey>,
        first_run: bool,
        now: Instant,
    ) -> Admit {
        let Some(key) = key else {
            return Admit::NotGated;
        };
        // A chain that holds its permit (resume after a child call, retry of a
        // running attempt) is never gated again.
        if self
            .held
            .get(&entry_id)
            .is_some_and(|h| h.segment_started.is_some())
        {
            return Admit::NotGated;
        }
        // A chain already under way that this leader has never seen (leader
        // change, restart) is admitted unconditionally: it may hold children
        // downstream, and gating it would make it wait behind its own cap.
        // The overshoot is bounded by the chains in flight and drains as they end.
        let top_up = !first_run && !self.held.contains_key(&entry_id);
        if self.node.try_admit(&key, top_up, now) {
            self.remove_waiter(vqueue, &key);
            match self.held.get_mut(&entry_id) {
                // back from an external wait: the clock restarts
                Some(held) => {
                    held.segment_started = Some(now);
                    held.started = false;
                }
                None => {
                    self.held.insert(
                        entry_id,
                        Held {
                            key,
                            segment_started: Some(now),
                            active: Duration::ZERO,
                            started: false,
                        },
                    );
                }
            }
            return Admit::Admitted;
        }
        let w = self.waiters(&key);
        if !w.queue.contains(&vqueue) {
            // a chain already under way goes ahead of new starts
            if first_run {
                w.queue.push_back(vqueue);
            } else {
                w.queue.push_front(vqueue);
            }
            w.report();
        }
        Admit::Blocked(key)
    }

    pub(super) fn remove_waiter(&mut self, vqueue: VQueueHandle, key: &ChainKey) {
        if let Some(w) = self.waiters.get_mut(key) {
            if let Some(pos) = w.queue.iter().position(|h| *h == vqueue) {
                w.queue.remove(pos);
                w.report();
            }
            if w.queue.is_empty() {
                self.waiters.remove(key);
            }
        }
    }

    /// An admitted chain took its invoker slot: it counts as running from
    /// here until it pauses or ends.
    pub(super) fn mark_started(&mut self, entry_id: EntryId, now: Instant) {
        if let Some(held) = self.held.get_mut(&entry_id)
            && held.segment_started.is_some()
            && !held.started
        {
            held.started = true;
            self.node.mark_started(&held.key, now);
        }
    }

    /// An admitted entry never started (its assignment was reverted): give the
    /// permit back; the entry is admitted again on its next attempt.
    pub(super) fn release_unstarted(
        &mut self,
        entry_id: EntryId,
        now: Instant,
        woken: &mut Vec<VQueueHandle>,
    ) {
        let Some(held) = self.held.get_mut(&entry_id) else {
            return;
        };
        let key = held.key.clone();
        let was_running = held.segment_started.take().is_some();
        if std::mem::take(&mut held.started) {
            self.node.mark_stopped(&key, now);
        }
        if held.active == Duration::ZERO {
            self.held.remove(&entry_id);
        }
        if was_running {
            self.node.release(&key, now);
        }
        self.wake_local(&key, now, woken);
    }

    /// Wakes local waiters of `key` up to the node-wide headroom, at least
    /// one: a stale waiter must never leave a free permit idle.
    fn wake_local(&mut self, key: &ChainKey, now: Instant, woken: &mut Vec<VQueueHandle>) {
        let headroom = self.node.headroom(key, now).max(1) as usize;
        if let Some(w) = self.waiters.get_mut(key) {
            for _ in 0..headroom {
                match w.queue.pop_front() {
                    Some(h) => woken.push(h),
                    None => break,
                }
            }
            w.report();
            if w.queue.is_empty() {
                self.waiters.remove(key);
            }
        }
    }

    /// Wakes local waiters after another partition (or this one) freed a
    /// permit or a limit grew.
    pub(super) fn poll_wake(&mut self, cx: &std::task::Context<'_>, now: Instant, woken: &mut Vec<VQueueHandle>) {
        let changed = self.node.poll_changed(&mut self.sub, cx.waker());
        if self.waiters.is_empty() || !changed {
            return;
        }
        let keys: Vec<ChainKey> = self.waiters.keys().cloned().collect();
        for key in keys {
            let headroom = self.node.headroom(&key, now) as usize;
            if headroom == 0 {
                continue;
            }
            if let Some(w) = self.waiters.get_mut(&key) {
                for _ in 0..headroom.min(w.queue.len()) {
                    woken.extend(w.queue.pop_front());
                }
                w.report();
                if w.queue.is_empty() {
                    self.waiters.remove(&key);
                }
            }
        }
    }

    pub(super) fn on_signal(&mut self, signal: ChainSignal, now: Instant) -> Vec<VQueueHandle> {
        let mut woken = Vec::new();
        if !self.node.is_enabled() {
            return woken;
        }
        let entry_id = signal.entry_id;
        match signal.kind {
            ChainSignalKind::Pause => {
                if let Some(held) = self.held.get_mut(&entry_id)
                    && let Some(started_at) = held.segment_started.take()
                {
                    held.active += now.saturating_duration_since(started_at);
                    let key = held.key.clone();
                    if std::mem::take(&mut held.started) {
                        self.node.mark_stopped(&key, now);
                    }
                    self.node.release(&key, now);
                    self.wake_local(&key, now, &mut woken);
                }
            }
            ChainSignalKind::End { completed } => {
                let Some(held) = self.held.remove(&entry_id) else {
                    return woken;
                };
                let key = held.key;
                let was_running = held.segment_started.is_some();
                let active = held.active
                    + held
                        .segment_started
                        .map(|s| now.saturating_duration_since(s))
                        .unwrap_or_default();
                if held.started {
                    self.node.mark_stopped(&key, now);
                }
                if was_running {
                    self.node.release(&key, now);
                    self.wake_local(&key, now, &mut woken);
                }
                if completed && active <= self.node.sample_max() {
                    let outcome = self.node.sample(&key, active, now);
                    if outcome == UpdateOutcome::Increase {
                        self.wake_local(&key, now, &mut woken);
                    }
                }
            }
        }
        woken
    }

    #[cfg(test)]
    pub(super) fn node(&self) -> &ChainNode {
        &self.node
    }
}

impl Drop for ChainAdmission {
    /// The node-wide counts outlive this partition's leadership: give back
    /// every permit and running count this partition holds.
    fn drop(&mut self) {
        let now = Instant::now();
        for (_, held) in self.held.drain() {
            if held.started {
                self.node.mark_stopped(&held.key, now);
            }
            if held.segment_started.is_some() {
                self.node.release(&held.key, now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use restate_worker_api::invoker::chain_node::ChainAdmissionConfig;
    use restate_worker_api::invoker::gradient2::ControllerParams;
    use slotmap::SlotMap;

    const ROOT: &str = "Root";

    fn cfg(min: u32, max: u32) -> ChainAdmissionConfig {
        ChainAdmissionConfig {
            enabled: true,
            params: ControllerParams {
                min,
                max,
                tolerance_permille: 1500,
                smoothing_permille: 200,
                initial: Some(min),
            },
            sample_max: Duration::from_secs(60),
        }
    }

    fn entry(n: u128) -> EntryId {
        EntryId::from(&restate_types::identifiers::InvocationId::from_parts(
            0,
            restate_types::identifiers::InvocationUuid::from_u128(n),
        ))
    }

    fn key(adm: &ChainAdmission, scope: Option<&str>) -> Option<ChainKey> {
        let scope = scope.map(|s| Scope::try_non_interned(s).unwrap());
        adm.key_of(Some(ROOT), scope.as_ref())
    }

    fn signal(e: EntryId, kind: ChainSignalKind) -> ChainSignal {
        ChainSignal { entry_id: e, kind }
    }

    fn admission(node: &ChainNode, label: &str) -> ChainAdmission {
        ChainAdmission::new(node.clone(), label.to_owned())
    }

    /// Admitted once, never re-gated on resume, freed on End; children are
    /// not gated at all.
    #[tokio::test(start_paused = true)]
    async fn permit_spans_the_chain_and_frees_on_end() {
        let node = ChainNode::new(cfg(1, 10));
        let mut adm = admission(&node, "0");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q1, q2) = (handles.insert(()), handles.insert(()));
        let (e1, e2) = (entry(1), entry(2));
        let now = Instant::now();
        let k = key(&adm, None);
        assert!(matches!(adm.poll_admit(q1, e1, k.clone(), true, now), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q2, e2, k.clone(), true, now), Admit::Blocked(_)));
        adm.mark_started(e1, now);
        assert!(matches!(adm.poll_admit(q1, e1, k.clone(), false, now), Admit::NotGated));
        assert!(matches!(adm.poll_admit(q2, entry(99), None, true, now), Admit::NotGated));
        assert_eq!(node.snapshot(k.as_ref().unwrap()).unwrap(), (1, 1, 1));
        let woken = adm.on_signal(signal(e1, ChainSignalKind::End { completed: true }), now);
        assert_eq!(woken, vec![q2], "End frees the permit and wakes the waiter");
        assert_eq!(node.snapshot(k.as_ref().unwrap()).unwrap().1, 0);
        assert!(matches!(adm.poll_admit(q2, e2, k, true, now), Admit::Admitted));
    }

    /// The limit is node-wide: partition B's waiter is woken when partition
    /// A frees a permit, through the change notification.
    #[tokio::test(start_paused = true)]
    async fn permit_freed_on_one_partition_wakes_the_other() {
        let node = ChainNode::new(cfg(1, 10));
        let mut a = admission(&node, "A");
        let mut b = admission(&node, "B");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (qa, qb) = (handles.insert(()), handles.insert(()));
        let now = Instant::now();
        let cx = std::task::Context::from_waker(std::task::Waker::noop());
        let k = key(&a, Some("payment"));
        assert!(matches!(a.poll_admit(qa, entry(1), k.clone(), true, now), Admit::Admitted));
        assert!(matches!(b.poll_admit(qb, entry(2), k.clone(), true, now), Admit::Blocked(_)));
        let mut woken = Vec::new();
        b.poll_wake(&cx, now, &mut woken);
        assert!(woken.is_empty(), "nothing freed yet");
        a.on_signal(signal(entry(1), ChainSignalKind::End { completed: true }), now);
        b.poll_wake(&cx, now, &mut woken);
        assert_eq!(woken, vec![qb], "B's waiter is woken by A's release");
        assert!(matches!(b.poll_admit(qb, entry(2), k, true, now), Admit::Admitted));
    }

    /// The same root in two scopes has two independent limits.
    #[tokio::test(start_paused = true)]
    async fn same_root_in_two_scopes_is_two_chains() {
        let node = ChainNode::new(cfg(1, 10));
        let mut adm = admission(&node, "0");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q1, q2, q3) = (handles.insert(()), handles.insert(()), handles.insert(()));
        let now = Instant::now();
        let pay = key(&adm, Some("payment"));
        let idx = key(&adm, Some("indexing"));
        assert!(matches!(adm.poll_admit(q1, entry(1), pay.clone(), true, now), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q2, entry(2), pay, true, now), Admit::Blocked(_)));
        assert!(matches!(adm.poll_admit(q3, entry(3), idx, true, now), Admit::Admitted));
    }

    /// External wait: the permit is reopened for others; the resuming chain
    /// is served before new starts; the clock excludes the wait.
    #[tokio::test(start_paused = true)]
    async fn external_wait_reopens_slot_and_resume_goes_first() {
        let node = ChainNode::new(cfg(1, 10));
        let mut adm = admission(&node, "0");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q1, q2, q3) = (handles.insert(()), handles.insert(()), handles.insert(()));
        let (e1, e2, e3) = (entry(1), entry(2), entry(3));
        let t0 = Instant::now();
        let k = key(&adm, None);
        assert!(matches!(adm.poll_admit(q1, e1, k.clone(), true, t0), Admit::Admitted));
        adm.mark_started(e1, t0);
        assert!(matches!(adm.poll_admit(q2, e2, k.clone(), true, t0), Admit::Blocked(_)));
        let woken = adm.on_signal(signal(e1, ChainSignalKind::Pause), t0 + Duration::from_millis(100));
        assert_eq!(woken, vec![q2]);
        assert_eq!(node.snapshot(k.as_ref().unwrap()).unwrap(), (1, 0, 0), "permit and running given back");
        assert!(matches!(adm.poll_admit(q2, e2, k.clone(), true, t0), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q3, e3, k.clone(), true, t0), Admit::Blocked(_)));
        // the paused chain wants back in: it queues ahead of the new start
        assert!(matches!(
            adm.poll_admit(q1, e1, k.clone(), false, t0 + Duration::from_secs(30)),
            Admit::Blocked(_)
        ));
        let woken = adm.on_signal(signal(e2, ChainSignalKind::End { completed: true }), t0 + Duration::from_secs(30));
        assert_eq!(woken, vec![q1], "the resuming chain is served before the new start");
        assert!(matches!(
            adm.poll_admit(q1, e1, k, false, t0 + Duration::from_secs(31)),
            Admit::Admitted
        ));
        // active time: 100 ms before the pause + the segment after it
        let held = &adm.held[&e1];
        assert_eq!(held.active, Duration::from_millis(100));
    }

    /// After a leader change, chains already under way are admitted and
    /// counted; new starts wait until they drain.
    #[tokio::test(start_paused = true)]
    async fn inherited_backlog_is_gated_after_leader_change() {
        let node = ChainNode::new(cfg(1, 1));
        let mut adm = admission(&node, "0");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q_new, q_mid, q_new2) = (handles.insert(()), handles.insert(()), handles.insert(()));
        let now = Instant::now();
        let k = key(&adm, None);
        assert!(matches!(adm.poll_admit(q_new, entry(1), k.clone(), true, now), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q_mid, entry(3), k.clone(), false, now), Admit::Admitted));
        assert_eq!(node.snapshot(k.as_ref().unwrap()).unwrap().1, 2, "the top-up is counted");
        assert!(matches!(adm.poll_admit(q_new2, entry(2), k.clone(), true, now), Admit::Blocked(_)));
        adm.on_signal(signal(entry(1), ChainSignalKind::End { completed: true }), now);
        assert!(
            matches!(adm.poll_admit(q_new2, entry(2), k.clone(), true, now), Admit::Blocked(_)),
            "limit 2 with the inherited chain still in progress"
        );
        let woken = adm.on_signal(signal(entry(3), ChainSignalKind::End { completed: true }), now);
        assert_eq!(woken, vec![q_new2]);
    }

    /// A reverted assignment returns the permit; the entry is admitted anew.
    #[tokio::test(start_paused = true)]
    async fn revert_returns_the_permit() {
        let node = ChainNode::new(cfg(1, 10));
        let mut adm = admission(&node, "0");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let e1 = entry(1);
        let now = Instant::now();
        let k = key(&adm, None);
        assert!(matches!(adm.poll_admit(q1, e1, k.clone(), true, now), Admit::Admitted));
        let mut woken = Vec::new();
        adm.release_unstarted(e1, now, &mut woken);
        assert_eq!(node.snapshot(k.as_ref().unwrap()).unwrap().1, 0);
        assert!(matches!(adm.poll_admit(q1, e1, k, true, now), Admit::Admitted));
    }

    /// Losing leadership drops the partition's admission: its permits and
    /// running counts go back to the node, so the other partitions can use
    /// them.
    #[tokio::test(start_paused = true)]
    async fn drop_returns_permits_to_the_node() {
        let node = ChainNode::new(cfg(1, 10));
        let mut a = admission(&node, "A");
        let mut b = admission(&node, "B");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (qa, qb) = (handles.insert(()), handles.insert(()));
        let now = Instant::now();
        let k = key(&a, None);
        assert!(matches!(a.poll_admit(qa, entry(1), k.clone(), true, now), Admit::Admitted));
        a.mark_started(entry(1), now);
        assert!(matches!(b.poll_admit(qb, entry(2), k.clone(), true, now), Admit::Blocked(_)));
        drop(a);
        assert_eq!(node.snapshot(k.as_ref().unwrap()).unwrap(), (1, 0, 0));
        assert!(matches!(b.poll_admit(qb, entry(2), k, true, now), Admit::Admitted));
    }

    /// Disabled: nothing is gated and no controller exists.
    #[tokio::test(start_paused = true)]
    async fn disabled_is_transparent() {
        let node = ChainNode::disabled();
        let mut adm = admission(&node, "0");
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let now = Instant::now();
        assert!(adm.key_of(Some(ROOT), None).is_none());
        assert!(matches!(adm.poll_admit(q1, entry(1), None, true, now), Admit::NotGated));
        assert!(adm.node().snapshot(&ChainKey { scope: None, root: ServiceName::new(ROOT) }).is_none());
    }
}
