// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Automatic admission control per *chain*.
//!
//! A chain is a root invocation (nothing called it) plus everything it calls.
//! Each root service gets one Gradient2 controller whose sample is the chain's
//! active end-to-end time and whose limit caps chains in progress. A chain
//! permit is held for the chain's lifetime — never re-acquired on resume —
//! so a parent can never wait behind its own cap. While the root waits on an
//! external future (or is paused) the permit is released and the clock paused.
//!
//! Roots are recognised from the durable `EntryMetadata::chain_root` marker,
//! so a new leader gates the backlog it inherits. Chains that were already
//! under way are admitted unconditionally when they next run (and counted), so
//! new starts wait until they drain. Roots live in their own vqueues, so a
//! blocked root never sits in front of a child call to the same service.
//!
//! No sojourn backstop here: the root queue's sojourn is backlog age, not a
//! congestion signal; downstream queueing is already inside the chain's
//! end-to-end time.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use metrics::{Gauge, gauge};
use tokio::time::Instant;

use restate_types::ServiceName;
use restate_types::vqueues::EntryId;
use restate_worker_api::resources::{ChainSignal, ChainSignalKind};

use super::gradient2::{ControllerFamily, ControllerParams, Gradient2Controller, UpdateOutcome};
use crate::metric_definitions::CHAIN_WAITERS;
use crate::scheduler::VQueueHandle;

/// Runtime configuration, derived from `worker.invoker.chain-admission`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChainAdmissionConfig {
    pub enabled: bool,
    pub params: ControllerParams,
    /// Chains whose active time exceeds this are released but not sampled.
    pub sample_max: Duration,
}

impl Default for ChainAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            params: ControllerParams {
                min: 4,
                max: 1000,
                tolerance_permille: 1500,
                smoothing_permille: 200,
                initial: None,
            },
            sample_max: Duration::from_secs(60),
        }
    }
}

pub(super) enum Admit {
    /// Not a gated entry (child, running chain, or admission disabled).
    NotGated,
    /// Admitted; the caller records the entry in its permit for revert.
    Admitted,
    /// The root's limit is used up; the queue was parked.
    Blocked(ServiceName),
}

struct Held {
    root: ServiceName,
    /// `None` while paused (external wait or paused invocation).
    segment_started: Option<Instant>,
    active: Duration,
}

#[derive(Default)]
struct RootState {
    in_progress: u32,
    waiters: VecDeque<VQueueHandle>,
    m_waiters: Option<Gauge>,
}

pub(super) struct ChainAdmission {
    cfg: ChainAdmissionConfig,
    partition_label: String,
    controllers: HashMap<ServiceName, Gradient2Controller>,
    roots: HashMap<ServiceName, RootState>,
    held: HashMap<EntryId, Held>,
}

impl ChainAdmission {
    pub(super) fn new(cfg: ChainAdmissionConfig, partition_label: String) -> Self {
        Self {
            cfg,
            partition_label,
            controllers: HashMap::new(),
            roots: HashMap::new(),
            held: HashMap::new(),
        }
    }

    fn controller(&mut self, root: &ServiceName, now: Instant) -> &mut Gradient2Controller {
        if !self.controllers.contains_key(root) {
            self.controllers.insert(
                root.clone(),
                Gradient2Controller::new(
                    self.cfg.params,
                    ControllerFamily::Chain,
                    root.to_string(),
                    self.partition_label.clone(),
                    now,
                ),
            );
        }
        self.controllers.get_mut(root).expect("inserted above")
    }

    fn root_state(&mut self, root: &ServiceName) -> &mut RootState {
        if !self.roots.contains_key(root) {
            let m_waiters = gauge!(CHAIN_WAITERS, "root" => root.to_string(), "partition_id" => self.partition_label.clone());
            self.roots.insert(
                root.clone(),
                RootState {
                    in_progress: 0,
                    waiters: VecDeque::new(),
                    m_waiters: Some(m_waiters),
                },
            );
        }
        self.roots.get_mut(root).expect("inserted above")
    }

    /// Admission check for the inbox head of `vqueue`. `chain_root` is the
    /// entry's durable root marker; `first_run` is false for resumes.
    pub(super) fn poll_admit(
        &mut self,
        vqueue: VQueueHandle,
        entry_id: EntryId,
        chain_root: Option<&str>,
        first_run: bool,
        now: Instant,
    ) -> Admit {
        if !self.cfg.enabled {
            return Admit::NotGated;
        }
        let Some(root_name) = chain_root else {
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
        let root = ServiceName::new(root_name);
        let limit = self.controller(&root, now).current_limit().get();
        // A chain already under way that this leader has never seen (leader
        // change, restart) is admitted unconditionally: it may hold children
        // downstream, and gating it would make it wait behind its own cap.
        // The overshoot is bounded by the chains in flight and drains as they end.
        let top_up = !first_run && !self.held.contains_key(&entry_id);
        let state = self.root_state(&root);
        if top_up || state.in_progress < limit {
            state.in_progress += 1;
            if let Some(pos) = state.waiters.iter().position(|h| *h == vqueue) {
                state.waiters.remove(pos);
            }
            state.report_waiters();
            match self.held.get_mut(&entry_id) {
                // back from an external wait: the clock restarts
                Some(held) => held.segment_started = Some(now),
                None => {
                    self.held.insert(
                        entry_id,
                        Held {
                            root,
                            segment_started: Some(now),
                            active: Duration::ZERO,
                        },
                    );
                }
            }
            return Admit::Admitted;
        }
        if !state.waiters.contains(&vqueue) {
            // a chain already under way goes ahead of new starts
            if first_run {
                state.waiters.push_back(vqueue);
            } else {
                state.waiters.push_front(vqueue);
            }
            state.report_waiters();
        }
        Admit::Blocked(root)
    }

    pub(super) fn remove_waiter(&mut self, vqueue: VQueueHandle, root: &ServiceName) {
        if let Some(state) = self.roots.get_mut(root)
            && let Some(pos) = state.waiters.iter().position(|h| *h == vqueue)
        {
            state.waiters.remove(pos);
            state.report_waiters();
        }
    }

    /// An admitted entry never started (its assignment was reverted): give the
    /// permit back; the entry is admitted again on its next attempt.
    pub(super) fn release_unstarted(&mut self, entry_id: EntryId, woken: &mut Vec<VQueueHandle>) {
        let Some(held) = self.held.get_mut(&entry_id) else {
            return;
        };
        let root = held.root.clone();
        held.segment_started = None;
        if held.active == Duration::ZERO {
            self.held.remove(&entry_id);
        }
        self.free_slot(&root, woken);
    }

    fn free_slot(&mut self, root: &ServiceName, woken: &mut Vec<VQueueHandle>) {
        let limit = self
            .controllers
            .get(root)
            .map(|c| c.current_limit().get())
            .unwrap_or(1);
        let state = self.root_state(root);
        state.in_progress = state.in_progress.saturating_sub(1);
        // wake up to the headroom, not just one: a stale waiter must never
        // leave a free slot idle
        let headroom = limit.saturating_sub(state.in_progress).max(1) as usize;
        for _ in 0..headroom {
            match state.waiters.pop_front() {
                Some(h) => woken.push(h),
                None => break,
            }
        }
        state.report_waiters();
    }

    pub(super) fn on_signal(&mut self, signal: ChainSignal, now: Instant) -> Vec<VQueueHandle> {
        let mut woken = Vec::new();
        if !self.cfg.enabled {
            return woken;
        }
        let entry_id = signal.entry_id;
        match signal.kind {
            ChainSignalKind::Pause => {
                if let Some(held) = self.held.get_mut(&entry_id)
                    && let Some(started) = held.segment_started.take()
                {
                    held.active += now.saturating_duration_since(started);
                    let root = held.root.clone();
                    self.free_slot(&root, &mut woken);
                }
            }
            ChainSignalKind::End { completed } => {
                let Some(held) = self.held.remove(&entry_id) else {
                    return woken;
                };
                let root = held.root;
                let was_running = held.segment_started.is_some();
                let active = held.active
                    + held
                        .segment_started
                        .map(|s| now.saturating_duration_since(s))
                        .unwrap_or_default();
                // in-flight as the controller sees it: chains in progress
                // including this one, at completion.
                let in_flight = self.root_state(&root).in_progress;
                if was_running {
                    self.free_slot(&root, &mut woken);
                }
                if completed && active <= self.cfg.sample_max {
                    let outcome = self.controller(&root, now).on_sample(active, in_flight, now);
                    if outcome == UpdateOutcome::Increase {
                        let limit = self.controllers[&root].current_limit().get();
                        let state = self.root_state(&root);
                        let headroom = limit.saturating_sub(state.in_progress) as usize;
                        for _ in 0..headroom {
                            match state.waiters.pop_front() {
                                Some(h) => woken.push(h),
                                None => break,
                            }
                        }
                        state.report_waiters();
                    }
                }
            }
        }
        woken
    }

    #[cfg(test)]
    pub(super) fn in_progress(&self, root: &ServiceName) -> u32 {
        self.roots.get(root).map(|s| s.in_progress).unwrap_or(0)
    }

    #[cfg(test)]
    pub(super) fn current_limit(&self, root: &ServiceName) -> Option<u32> {
        self.controllers.get(root).map(|c| c.current_limit().get())
    }
}

impl RootState {
    fn report_waiters(&self) {
        if let Some(g) = &self.m_waiters {
            g.set(self.waiters.len() as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn root() -> ServiceName {
        ServiceName::new(ROOT)
    }

    fn signal(e: EntryId, kind: ChainSignalKind) -> ChainSignal {
        ChainSignal { entry_id: e, kind }
    }

    /// The permit spans the chain: a second root is blocked while the first is
    /// in progress, the first's resume after a child call is never gated, and
    /// End frees the slot. Children (no root marker) are never gated.
    #[test]
    fn permit_spans_the_chain_and_frees_on_end() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q1, q2) = (handles.insert(()), handles.insert(()));
        let now = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        let (e1, e2) = (entry(1), entry(2));

        assert!(matches!(adm.poll_admit(q1, e1, Some(ROOT), true, now), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q2, e2, Some(ROOT), true, now), Admit::Blocked(_)));
        // e1 suspends awaiting a child and comes back: not re-gated
        assert!(matches!(adm.poll_admit(q1, e1, Some(ROOT), false, now), Admit::NotGated));
        // a child is never gated
        assert!(matches!(adm.poll_admit(q2, entry(99), None, true, now), Admit::NotGated));
        assert_eq!(adm.in_progress(&root()), 1);

        let woken = adm.on_signal(
            signal(e1, ChainSignalKind::End { completed: true }),
            now + Duration::from_millis(500),
        );
        assert_eq!(woken, vec![q2], "End frees the slot and wakes the waiter");
        assert!(matches!(adm.poll_admit(q2, e2, Some(ROOT), true, now), Admit::Admitted));
    }

    /// An external wait reopens the slot and stops the clock; the resume needs
    /// a permit again but goes ahead of new starts.
    #[test]
    fn external_wait_reopens_slot_and_resume_goes_first() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q1, q2, q3) = (handles.insert(()), handles.insert(()), handles.insert(()));
        let t0 = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        let (e1, e2, e3) = (entry(1), entry(2), entry(3));

        assert!(matches!(adm.poll_admit(q1, e1, Some(ROOT), true, t0), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q2, e2, Some(ROOT), true, t0), Admit::Blocked(_)));

        let woken = adm.on_signal(signal(e1, ChainSignalKind::Pause), t0 + Duration::from_millis(100));
        assert_eq!(woken, vec![q2]);
        assert_eq!(adm.in_progress(&root()), 0);
        assert!(matches!(adm.poll_admit(q2, e2, Some(ROOT), true, t0), Admit::Admitted));

        // a new start queues, then e1's awakeable resolves: e1 queues AHEAD
        assert!(matches!(adm.poll_admit(q3, e3, Some(ROOT), true, t0), Admit::Blocked(_)));
        assert!(matches!(
            adm.poll_admit(q1, e1, Some(ROOT), false, t0 + Duration::from_secs(30)),
            Admit::Blocked(_)
        ), "a paused chain this leader knows re-queues (no top-up)");
        let woken = adm.on_signal(
            signal(e2, ChainSignalKind::End { completed: true }),
            t0 + Duration::from_secs(31),
        );
        assert_eq!(woken, vec![q1], "the resuming chain is served before the new start");
        assert!(matches!(
            adm.poll_admit(q1, e1, Some(ROOT), false, t0 + Duration::from_secs(31)),
            Admit::Admitted
        ));
        assert_eq!(
            adm.held.get(&e1).map(|h| h.active),
            Some(Duration::from_millis(100)),
            "the 30s external wait is not active time"
        );
    }

    /// After a leader change the scheduler holds no state: the inherited
    /// backlog is still gated (durable root marker), while chains already under
    /// way are admitted unconditionally (they must never wait behind their own
    /// cap) and counted, so new starts wait until they drain.
    #[test]
    fn inherited_backlog_is_gated_after_leader_change() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let (q_new, q_mid, q_new2) = (handles.insert(()), handles.insert(()), handles.insert(()));
        let now = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        assert!(matches!(adm.poll_admit(q_new, entry(1), Some(ROOT), true, now), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q_mid, entry(3), Some(ROOT), false, now), Admit::Admitted));
        assert_eq!(adm.in_progress(&root()), 2, "the top-up is counted");
        assert!(matches!(adm.poll_admit(q_new2, entry(2), Some(ROOT), true, now), Admit::Blocked(_)));
        adm.on_signal(signal(entry(1), ChainSignalKind::End { completed: true }), now);
        assert!(matches!(adm.poll_admit(q_new2, entry(2), Some(ROOT), true, now), Admit::Blocked(_)),
            "still over the limit until the inherited chain drains");
        let woken = adm.on_signal(signal(entry(3), ChainSignalKind::End { completed: true }), now);
        assert_eq!(woken, vec![q_new2]);
    }

    /// A reverted (never started) admission is given back and re-admittable.
    #[test]
    fn revert_returns_the_permit() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let now = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        let e1 = entry(1);
        assert!(matches!(adm.poll_admit(q1, e1, Some(ROOT), true, now), Admit::Admitted));
        let mut woken = Vec::new();
        adm.release_unstarted(e1, &mut woken);
        assert_eq!(adm.in_progress(&root()), 0);
        assert!(matches!(adm.poll_admit(q1, e1, Some(ROOT), true, now), Admit::Admitted));
    }

    /// Disabled admission gates nothing.
    #[test]
    fn disabled_is_transparent() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let now = Instant::now();
        let mut adm = ChainAdmission::new(ChainAdmissionConfig::default(), "p".into());
        assert!(matches!(adm.poll_admit(q1, entry(1), Some(ROOT), true, now), Admit::NotGated));
        assert!(adm.current_limit(&root()).is_none());
    }
}
