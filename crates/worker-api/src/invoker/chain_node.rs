// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Node-wide chain admission state: one Gradient2 controller per (scope, root
//! service), shared by every partition scheduler on the node, like the slot
//! shares. A weight belongs to a scope, so one root service started in two
//! scopes is two chains with two limits. The limit is a node-wide count of
//! chains in progress, which is what the node's slot pool is sized in.
//!
//! Partitions keep their own waiters and clocks (see the scheduler's
//! `ChainAdmission`); this holds the counts and the controllers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Duration;

use metrics::{Gauge, gauge};
use tokio::time::Instant;

use restate_types::config::ChainAdmissionOptions;
use restate_types::{Scope, ServiceName};

use super::chain_metrics::CHAIN_IN_PROGRESS;
use super::gradient2::{ControllerFamily, ControllerParams, Gradient2Controller, UpdateOutcome};
use super::notify::{ChangeNotifier, ChangeSubscription, wake_all};

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

impl ChainAdmissionConfig {
    /// From the node's `[worker.invoker]` options; `pool` is the invoker slot
    /// limit, the default ceiling.
    pub fn from_options(opts: &ChainAdmissionOptions, pool: Option<usize>) -> Self {
        Self {
            enabled: opts.enabled,
            params: ControllerParams {
                min: opts.min.get(),
                max: opts.max.map(|m| m.get()).unwrap_or_else(|| {
                    pool.map(|l| l.min(u32::MAX as usize) as u32)
                        .unwrap_or(u32::MAX)
                }),
                tolerance_permille: opts.tolerance_permille.get(),
                smoothing_permille: opts.smoothing_permille.get(),
                initial: None,
            },
            sample_max: opts.sample_max.into(),
        }
    }
}

/// A chain's identity for admission: the root service inside its scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChainKey {
    pub scope: Option<Scope>,
    pub root: ServiceName,
}

impl ChainKey {
    pub fn scope_label(&self) -> String {
        self.scope
            .as_ref()
            .map(|s| s.as_str().to_owned())
            .unwrap_or_default()
    }
}

impl std::fmt::Display for ChainKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.scope {
            Some(scope) => write!(f, "{scope}/{}", self.root),
            None => write!(f, "/{}", self.root),
        }
    }
}

struct KeyState {
    controller: Gradient2Controller,
    /// Chain permits held on this node.
    in_progress: u32,
    /// Admitted chains that actually took their invoker slot (and have not
    /// paused or ended since).
    running: u32,
    /// Highest `running` since the last controller update: the flight size.
    peak_running: u32,
    m_in_progress: Gauge,
}

struct State {
    cfg: ChainAdmissionConfig,
    keys: HashMap<ChainKey, KeyState>,
    notify: ChangeNotifier,
}

impl State {
    fn key_state(&mut self, key: &ChainKey, now: Instant) -> &mut KeyState {
        if !self.keys.contains_key(key) {
            let scope_label = key.scope_label();
            let m_in_progress = gauge!(CHAIN_IN_PROGRESS, "scope" => scope_label.clone(), "root" => key.root.to_string());
            self.keys.insert(
                key.clone(),
                KeyState {
                    controller: Gradient2Controller::new(
                        self.cfg.params,
                        ControllerFamily::Chain,
                        scope_label,
                        key.root.to_string(),
                        now,
                    ),
                    in_progress: 0,
                    running: 0,
                    peak_running: 0,
                    m_in_progress,
                },
            );
        }
        self.keys.get_mut(key).expect("inserted above")
    }
}

/// Node-wide chain admission. Cheap to clone; disabled = no-op.
#[derive(Clone, Default)]
pub struct ChainNode(Option<Arc<Mutex<State>>>);

impl std::fmt::Debug for ChainNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainNode")
            .field("enabled", &self.0.is_some())
            .finish()
    }
}

impl ChainNode {
    pub fn new(cfg: ChainAdmissionConfig) -> Self {
        if !cfg.enabled {
            return Self(None);
        }
        Self(Some(Arc::new(Mutex::new(State {
            cfg,
            keys: HashMap::new(),
            notify: ChangeNotifier::default(),
        }))))
    }

    pub const fn disabled() -> Self {
        Self(None)
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub fn sample_max(&self) -> Duration {
        match &self.0 {
            Some(state) => state.lock().expect("chain node lock poisoned").cfg.sample_max,
            None => Duration::MAX,
        }
    }

    /// Takes a chain permit for `key` if its limit has room; `top_up` takes
    /// one unconditionally (a chain already under way that this leader had
    /// never seen). Returns whether a permit was taken.
    pub fn try_admit(&self, key: &ChainKey, top_up: bool, now: Instant) -> bool {
        let Some(state) = &self.0 else {
            return true;
        };
        let mut st = state.lock().expect("chain node lock poisoned");
        let ks = st.key_state(key, now);
        let limit = ks.controller.current_limit().get();
        if top_up || ks.in_progress < limit {
            ks.in_progress += 1;
            ks.m_in_progress.set(ks.in_progress as f64);
            true
        } else {
            false
        }
    }

    /// Gives a chain permit back (the chain paused, ended, or never started).
    pub fn release(&self, key: &ChainKey, now: Instant) {
        let Some(state) = &self.0 else {
            return;
        };
        let wake = {
            let mut st = state.lock().expect("chain node lock poisoned");
            let ks = st.key_state(key, now);
            ks.in_progress = ks.in_progress.saturating_sub(1);
            ks.m_in_progress.set(ks.in_progress as f64);
            st.notify.changed()
        };
        wake_all(wake);
    }

    /// An admitted chain took its invoker slot: it is running now.
    pub fn mark_started(&self, key: &ChainKey, now: Instant) {
        let Some(state) = &self.0 else {
            return;
        };
        let mut st = state.lock().expect("chain node lock poisoned");
        let ks = st.key_state(key, now);
        ks.running += 1;
        ks.peak_running = ks.peak_running.max(ks.running);
    }

    /// A running chain paused, ended, or gave its slot back.
    pub fn mark_stopped(&self, key: &ChainKey, now: Instant) {
        let Some(state) = &self.0 else {
            return;
        };
        let mut st = state.lock().expect("chain node lock poisoned");
        let ks = st.key_state(key, now);
        ks.running = ks.running.saturating_sub(1);
    }

    /// Feeds a completed chain's active time to the key's controller. The
    /// limit may grow only if the flight size reached it since the last
    /// update (the limit was binding), see [`Gradient2Controller::on_sample`].
    pub fn sample(&self, key: &ChainKey, active: Duration, now: Instant) -> UpdateOutcome {
        let Some(state) = &self.0 else {
            return UpdateOutcome::IntervalSkip;
        };
        let (outcome, wake) = {
            let mut st = state.lock().expect("chain node lock poisoned");
            let ks = st.key_state(key, now);
            let limit = ks.controller.current_limit().get();
            let limit_binding = ks.peak_running >= limit;
            let outcome = ks
                .controller
                .on_sample(active, ks.running, limit_binding, now);
            if outcome != UpdateOutcome::IntervalSkip {
                // a new interval: the flight size starts from what runs now
                ks.peak_running = ks.running;
            }
            let wake = if outcome == UpdateOutcome::Increase {
                st.notify.changed()
            } else {
                Vec::new()
            };
            (outcome, wake)
        };
        wake_all(wake);
        outcome
    }

    /// Free permits of `key` right now (limit minus chains in progress).
    pub fn headroom(&self, key: &ChainKey, now: Instant) -> u32 {
        let Some(state) = &self.0 else {
            return u32::MAX;
        };
        let mut st = state.lock().expect("chain node lock poisoned");
        let ks = st.key_state(key, now);
        ks.controller
            .current_limit()
            .get()
            .saturating_sub(ks.in_progress)
    }

    pub fn subscribe(&self) -> ChangeSubscription {
        match &self.0 {
            Some(state) => state
                .lock()
                .expect("chain node lock poisoned")
                .notify
                .subscribe(),
            None => ChangeSubscription::detached(),
        }
    }

    /// True (once per change) if a permit may have become free since the
    /// last call; registers `waker` for the next change.
    pub fn poll_changed(&self, sub: &mut ChangeSubscription, waker: &Waker) -> bool {
        match &self.0 {
            Some(state) => state
                .lock()
                .expect("chain node lock poisoned")
                .notify
                .poll_changed(sub, waker),
            None => false,
        }
    }

    /// Test and status helpers.
    pub fn snapshot(&self, key: &ChainKey) -> Option<(u32, u32, u32)> {
        let state = self.0.as_ref()?;
        let st = state.lock().expect("chain node lock poisoned");
        st.keys
            .get(key)
            .map(|ks| (ks.controller.current_limit().get(), ks.in_progress, ks.running))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn key(scope: Option<&str>, root: &str) -> ChainKey {
        ChainKey {
            scope: scope.map(|s| Scope::try_non_interned(s).unwrap()),
            root: ServiceName::new(root),
        }
    }

    /// The same root in two scopes is two chains with two limits; the limit
    /// counts node-wide across whoever admits.
    #[tokio::test(start_paused = true)]
    async fn keyed_by_scope_and_root_node_wide() {
        let node = ChainNode::new(cfg(1, 10));
        let now = Instant::now();
        let (pay, idx) = (key(Some("payment"), "Root"), key(Some("indexing"), "Root"));
        assert!(node.try_admit(&pay, false, now), "partition A admits");
        assert!(!node.try_admit(&pay, false, now), "partition B sees the same limit");
        assert!(node.try_admit(&idx, false, now), "another scope has its own limit");
        node.release(&pay, now);
        assert!(node.try_admit(&pay, false, now));
    }

    /// A release wakes subscribers; a take does not.
    #[tokio::test(start_paused = true)]
    async fn release_notifies_subscribers() {
        let node = ChainNode::new(cfg(1, 10));
        let now = Instant::now();
        let k = key(None, "Root");
        let mut sub = node.subscribe();
        let w = std::task::Waker::noop();
        assert!(!node.poll_changed(&mut sub, w));
        assert!(node.try_admit(&k, false, now));
        assert!(!node.poll_changed(&mut sub, w), "a take frees nothing");
        node.release(&k, now);
        assert!(node.poll_changed(&mut sub, w), "a release may free a permit");
        assert!(!node.poll_changed(&mut sub, w), "once per change");
    }

    /// The limit grows only while the flight size reaches it: holders that
    /// never took a slot (parked at a full pool) do not count.
    #[tokio::test(start_paused = true)]
    async fn limit_grows_only_when_running_reaches_it() {
        let node = ChainNode::new(cfg(4, 100));
        let mut now = Instant::now();
        let k = key(Some("payment"), "Root");
        // 4 permits held, only 3 running: the pool holds the fourth (enough
        // demand for the app-limited guard, but the limit is not what binds)
        for _ in 0..4 {
            assert!(node.try_admit(&k, false, now));
        }
        for _ in 0..3 {
            node.mark_started(&k, now);
        }
        for _ in 0..30 {
            now += Duration::from_millis(1050);
            node.sample(&k, Duration::from_millis(100), now);
        }
        assert_eq!(node.snapshot(&k).unwrap().0, 4, "not binding: no growth");
        // all four run: the limit binds and grows
        node.mark_started(&k, now);
        for _ in 0..30 {
            now += Duration::from_millis(1050);
            node.sample(&k, Duration::from_millis(100), now);
        }
        assert!(node.snapshot(&k).unwrap().0 > 4, "binding: grows");
    }
}
