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
//! external future the permit is released and the clock paused.

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
    /// Not a gated entry (child, unknown, or admission disabled).
    NotGated,
    /// Admitted; the caller records the entry in its permit for revert.
    Admitted,
    /// The root's limit is used up; the queue was parked.
    Blocked(ServiceName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
    NewStart,
    ResumeExternal,
}

struct Pending {
    root: ServiceName,
    mark: Mark,
}

struct Held {
    root: ServiceName,
    /// `None` while paused on an external wait.
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
    pending: HashMap<EntryId, Pending>,
    held: HashMap<EntryId, Held>,
}

impl ChainAdmission {
    pub(super) fn new(cfg: ChainAdmissionConfig, partition_label: String) -> Self {
        Self {
            cfg,
            partition_label,
            controllers: HashMap::new(),
            roots: HashMap::new(),
            pending: HashMap::new(),
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

    /// Admission check for the inbox head of `vqueue`.
    pub(super) fn poll_admit(
        &mut self,
        vqueue: VQueueHandle,
        entry_id: EntryId,
        sojourn: Duration,
        now: Instant,
    ) -> Admit {
        if !self.cfg.enabled {
            return Admit::NotGated;
        }
        let Some(pending) = self.pending.get(&entry_id) else {
            return Admit::NotGated;
        };
        let root = pending.root.clone();
        let mark = pending.mark;

        let limit = {
            let controller = self.controller(&root, now);
            controller.on_sojourn(sojourn, now);
            controller.current_limit().get()
        };
        let state = self.root_state(&root);
        if state.in_progress < limit {
            state.in_progress += 1;
            // A woken waiter that made it here is no longer waiting.
            if let Some(pos) = state.waiters.iter().position(|h| *h == vqueue) {
                state.waiters.remove(pos);
            }
            state.report_waiters();
            self.pending.remove(&entry_id);
            match self.held.get_mut(&entry_id) {
                // resuming after an external wait: the clock restarts
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
            match mark {
                // an external resume goes ahead of new starts
                Mark::ResumeExternal => state.waiters.push_front(vqueue),
                Mark::NewStart => state.waiters.push_back(vqueue),
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
    /// permit back and let the entry be admitted again later.
    pub(super) fn release_unstarted(&mut self, entry_id: EntryId, woken: &mut Vec<VQueueHandle>) {
        let Some(held) = self.held.get_mut(&entry_id) else {
            return;
        };
        let root = held.root.clone();
        let resumed = held.active > Duration::ZERO;
        held.segment_started = None;
        if !resumed {
            self.held.remove(&entry_id);
        }
        self.pending.insert(
            entry_id,
            Pending {
                root: root.clone(),
                mark: if resumed {
                    Mark::ResumeExternal
                } else {
                    Mark::NewStart
                },
            },
        );
        self.free_slot(&root, 1, woken);
    }

    fn free_slot(&mut self, root: &ServiceName, n: usize, woken: &mut Vec<VQueueHandle>) {
        let state = self.root_state(root);
        state.in_progress = state.in_progress.saturating_sub(1);
        for _ in 0..n {
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
            ChainSignalKind::NewStart { root } => {
                if !self.held.contains_key(&entry_id) {
                    self.pending.insert(
                        entry_id,
                        Pending {
                            root,
                            mark: Mark::NewStart,
                        },
                    );
                }
            }
            ChainSignalKind::Pause => {
                if let Some(held) = self.held.get_mut(&entry_id)
                    && let Some(started) = held.segment_started.take()
                {
                    held.active += now.saturating_duration_since(started);
                    let root = held.root.clone();
                    self.free_slot(&root, 1, &mut woken);
                }
            }
            ChainSignalKind::ResumeExternal => {
                if let Some(held) = self.held.get(&entry_id)
                    && held.segment_started.is_none()
                {
                    self.pending.insert(
                        entry_id,
                        Pending {
                            root: held.root.clone(),
                            mark: Mark::ResumeExternal,
                        },
                    );
                }
            }
            ChainSignalKind::End { completed } => {
                self.pending.remove(&entry_id);
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
                // in-flight as seen by the controller: chains in progress
                // including this one, at completion.
                let in_flight = self.root_state(&root).in_progress;
                if was_running {
                    self.free_slot(&root, 1, &mut woken);
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
        ServiceName::new("Root")
    }

    fn signal(e: EntryId, kind: ChainSignalKind) -> ChainSignal {
        ChainSignal { entry_id: e, kind }
    }

    /// The permit is held across the whole chain: a second start is blocked
    /// while the first is in progress (limit 1), and freed by End — not by
    /// any attempt boundary in between.
    #[test]
    fn permit_spans_the_chain_and_frees_on_end() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let q2 = handles.insert(());
        let now = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        let (e1, e2) = (entry(1), entry(2));

        adm.on_signal(signal(e1, ChainSignalKind::NewStart { root: root() }), now);
        adm.on_signal(signal(e2, ChainSignalKind::NewStart { root: root() }), now);
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, now), Admit::Admitted));
        assert!(matches!(adm.poll_admit(q2, e2, Duration::ZERO, now), Admit::Blocked(_)));
        assert_eq!(adm.in_progress(&root()), 1);

        // a child of e1 is never gated
        assert!(matches!(adm.poll_admit(q2, entry(99), Duration::ZERO, now), Admit::NotGated));

        let woken = adm.on_signal(signal(e1, ChainSignalKind::End { completed: true }), now + Duration::from_millis(500));
        assert_eq!(woken, vec![q2], "End frees the slot and wakes the waiter");
        assert!(matches!(adm.poll_admit(q2, e2, Duration::ZERO, now), Admit::Admitted));
    }

    /// An external wait reopens the slot and stops the clock; the resume needs
    /// a permit again but goes ahead of new starts.
    #[test]
    fn external_wait_reopens_slot_and_pauses_clock() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let q2 = handles.insert(());
        let q3 = handles.insert(());
        let t0 = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        let (e1, e2, e3) = (entry(1), entry(2), entry(3));

        adm.on_signal(signal(e1, ChainSignalKind::NewStart { root: root() }), t0);
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, t0), Admit::Admitted));
        adm.on_signal(signal(e2, ChainSignalKind::NewStart { root: root() }), t0);
        assert!(matches!(adm.poll_admit(q2, e2, Duration::ZERO, t0), Admit::Blocked(_)));

        // e1 waits on an awakeable: slot reopened, e2 woken
        let woken = adm.on_signal(signal(e1, ChainSignalKind::Pause), t0 + Duration::from_millis(100));
        assert_eq!(woken, vec![q2]);
        assert_eq!(adm.in_progress(&root()), 0);
        assert!(matches!(adm.poll_admit(q2, e2, Duration::ZERO, t0), Admit::Admitted));

        // the awakeable resolves: e1 must re-acquire, and it queues AHEAD of e3
        adm.on_signal(signal(e3, ChainSignalKind::NewStart { root: root() }), t0);
        assert!(matches!(adm.poll_admit(q3, e3, Duration::ZERO, t0), Admit::Blocked(_)));
        adm.on_signal(signal(e1, ChainSignalKind::ResumeExternal), t0 + Duration::from_secs(30));
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, t0), Admit::Blocked(_)));
        let woken = adm.on_signal(signal(e2, ChainSignalKind::End { completed: true }), t0 + Duration::from_secs(31));
        assert_eq!(woken, vec![q1], "external resume is served before the new start");

        // e1 runs 200ms more, then ends: the 30s wait is not in the sample
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, t0 + Duration::from_secs(31)), Admit::Admitted));
        adm.on_signal(signal(e1, ChainSignalKind::End { completed: true }), t0 + Duration::from_secs(31) + Duration::from_millis(200));
        let held_active = adm.held.get(&e1).map(|h| h.active);
        assert!(held_active.is_none(), "chain released on end");
    }

    /// A reverted (never started) admission is given back and re-admittable.
    #[test]
    fn revert_returns_the_permit() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let now = Instant::now();
        let mut adm = ChainAdmission::new(cfg(1, 1), "p".into());
        let e1 = entry(1);
        adm.on_signal(signal(e1, ChainSignalKind::NewStart { root: root() }), now);
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, now), Admit::Admitted));
        let mut woken = Vec::new();
        adm.release_unstarted(e1, &mut woken);
        assert_eq!(adm.in_progress(&root()), 0);
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, now), Admit::Admitted));
    }

    /// Disabled admission gates nothing and ignores signals.
    #[test]
    fn disabled_is_transparent() {
        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let q1 = handles.insert(());
        let now = Instant::now();
        let mut adm = ChainAdmission::new(ChainAdmissionConfig::default(), "p".into());
        let e1 = entry(1);
        adm.on_signal(signal(e1, ChainSignalKind::NewStart { root: root() }), now);
        assert!(matches!(adm.poll_admit(q1, e1, Duration::ZERO, now), Admit::NotGated));
        assert!(adm.current_limit(&root()).is_none());
    }
}
