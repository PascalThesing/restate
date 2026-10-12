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
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Duration;

use metrics::{Counter, Gauge, counter, gauge};
use tokio::time::Instant;

use restate_types::config::ChainAdmissionOptions;
use restate_types::{Scope, ServiceName};

use super::chain_metrics::{CHAIN_IN_PROGRESS, CHAIN_PROBES_TOTAL};
use super::gradient2::{ControllerFamily, ControllerParams, Gradient2Controller, UpdateOutcome};
use super::notify::{ChangeNotifier, ChangeSubscription, wake_all};

/// How often each (scope, root) re-measures its low-load latency (Envoy's
/// adaptive concurrency filter re-measures its minRTT the same way, BBR its
/// min RTT in ProbeRTT). The interval is jittered per key and node so probes
/// never line up.
const PROBE_INTERVAL: Duration = Duration::from_secs(300);
/// Chains started during a probe whose times make up the new baseline.
const PROBE_SAMPLES: usize = 20;
/// A probe that has not collected its samples by then ends without a change.
const PROBE_MAX: Duration = Duration::from_secs(10);

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
    /// The running low-load probe, if any (see `PROBE_INTERVAL`).
    probe: Option<Probe>,
    next_probe_at: Instant,
    /// Tag of the latest probe; chains admitted during it carry it.
    probe_epoch: u64,
    m_in_progress: Gauge,
    m_probe_reset: Counter,
    m_probe_timeout: Counter,
}

struct Probe {
    started: Instant,
    epoch: u64,
    samples: Vec<f64>,
}

/// Probe interval jittered to 75–100 % of `PROBE_INTERVAL`, differently per
/// key, node (randomly seeded hasher) and round.
fn next_probe(key: &ChainKey, round: u64, now: Instant) -> Instant {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    key.hash(&mut h);
    round.hash(&mut h);
    let frac = 0.75 + 0.25 * (h.finish() % 1000) as f64 / 1000.0;
    now + PROBE_INTERVAL.mul_f64(frac)
}

impl KeyState {
    /// The limit admission uses right now: pinned to the minimum during a
    /// probe, so chains started then run at low load.
    fn effective_limit(&self, min: u32) -> u32 {
        let limit = self.controller.current_limit().get();
        if self.probe.is_some() { limit.min(min.max(1)) } else { limit }
    }

    /// Starts a probe when one is due; ends a probe that ran out of time.
    /// Returns true if a probe ended (the limit is back up: wake waiters).
    fn tick_probe(&mut self, key: &ChainKey, now: Instant) -> bool {
        match &self.probe {
            Some(p) if now.saturating_duration_since(p.started) >= PROBE_MAX => {
                self.m_probe_timeout.increment(1);
                self.probe = None;
                self.next_probe_at = next_probe(key, self.probe_epoch, now);
                true
            }
            None if now >= self.next_probe_at => {
                self.probe_epoch += 1;
                self.probe = Some(Probe {
                    started: now,
                    epoch: self.probe_epoch,
                    samples: Vec::with_capacity(PROBE_SAMPLES),
                });
                false
            }
            _ => false,
        }
    }
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
                    probe: None,
                    next_probe_at: next_probe(key, 0, now),
                    probe_epoch: 0,
                    m_in_progress,
                    m_probe_reset: counter!(CHAIN_PROBES_TOTAL, "scope" => key.scope_label(), "root" => key.root.to_string(), "result" => "baseline_reset"),
                    m_probe_timeout: counter!(CHAIN_PROBES_TOTAL, "scope" => key.scope_label(), "root" => key.root.to_string(), "result" => "timeout"),
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
    /// never seen). Returns `None` if no permit was taken, else the probe tag
    /// to hand back to [`Self::sample`] (non-zero only for a chain admitted
    /// during a low-load probe).
    pub fn try_admit(&self, key: &ChainKey, top_up: bool, now: Instant) -> Option<u64> {
        let Some(state) = &self.0 else {
            return Some(0);
        };
        let (admitted, wake) = {
            let mut st = state.lock().expect("chain node lock poisoned");
            let min = st.cfg.params.min;
            let ks = st.key_state(key, now);
            let ended = ks.tick_probe(key, now);
            let admitted = if top_up || ks.in_progress < ks.effective_limit(min) {
                ks.in_progress += 1;
                ks.m_in_progress.set(ks.in_progress as f64);
                Some(ks.probe.as_ref().map(|p| p.epoch).unwrap_or(0))
            } else {
                None
            };
            (admitted, if ended { st.notify.changed() } else { Vec::new() })
        };
        wake_all(wake);
        admitted
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
    ///
    /// `probe_tag` is what [`Self::try_admit`] returned for this chain: a
    /// chain admitted during the current low-load probe also counts towards
    /// the probe, whose median replaces the baseline once it has
    /// `PROBE_SAMPLES` of them.
    pub fn sample(
        &self,
        key: &ChainKey,
        active: Duration,
        probe_tag: u64,
        now: Instant,
    ) -> UpdateOutcome {
        let Some(state) = &self.0 else {
            return UpdateOutcome::IntervalSkip;
        };
        let (outcome, wake) = {
            let mut st = state.lock().expect("chain node lock poisoned");
            let ks = st.key_state(key, now);
            let mut probe_ended = ks.tick_probe(key, now);
            if let Some(p) = &mut ks.probe
                && probe_tag != 0
                && probe_tag == p.epoch
            {
                p.samples.push(active.as_secs_f64() * 1000.0);
                if p.samples.len() >= PROBE_SAMPLES {
                    let mut xs = std::mem::take(&mut p.samples);
                    xs.sort_by(|a, b| a.total_cmp(b));
                    let median = xs[xs.len() / 2];
                    ks.controller.reset_baseline(median);
                    ks.m_probe_reset.increment(1);
                    ks.probe = None;
                    ks.next_probe_at = next_probe(key, ks.probe_epoch, now);
                    probe_ended = true;
                }
            }
            let limit = ks.controller.current_limit().get();
            let limit_binding = ks.peak_running >= limit;
            let outcome = ks
                .controller
                .on_sample(active, ks.running, limit_binding, now);
            if outcome != UpdateOutcome::IntervalSkip {
                // a new interval: the flight size starts from what runs now
                ks.peak_running = ks.running;
            }
            let wake = if outcome == UpdateOutcome::Increase || probe_ended {
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
        let min = st.cfg.params.min;
        let ks = st.key_state(key, now);
        ks.tick_probe(key, now);
        ks.effective_limit(min).saturating_sub(ks.in_progress)
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

    #[cfg(test)]
    fn baseline(&self, key: &ChainKey) -> f64 {
        let st = self.0.as_ref().unwrap().lock().unwrap();
        st.keys[key].controller.long_ema()
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
        assert!(node.try_admit(&pay, false, now).is_some(), "partition A admits");
        assert!(node.try_admit(&pay, false, now).is_none(), "partition B sees the same limit");
        assert!(node.try_admit(&idx, false, now).is_some(), "another scope has its own limit");
        node.release(&pay, now);
        assert!(node.try_admit(&pay, false, now).is_some());
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
        assert!(node.try_admit(&k, false, now).is_some());
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
            assert!(node.try_admit(&k, false, now).is_some());
        }
        for _ in 0..3 {
            node.mark_started(&k, now);
        }
        for _ in 0..30 {
            now += Duration::from_millis(1050);
            node.sample(&k, Duration::from_millis(100), 0, now);
        }
        assert_eq!(node.snapshot(&k).unwrap().0, 4, "not binding: no growth");
        // all four run: the limit binds and grows
        node.mark_started(&k, now);
        for _ in 0..30 {
            now += Duration::from_millis(1050);
            node.sample(&k, Duration::from_millis(100), 0, now);
        }
        assert!(node.snapshot(&k).unwrap().0 > 4, "binding: grows");
    }

    fn cfg_initial(min: u32, max: u32, initial: u32) -> ChainAdmissionConfig {
        let mut c = cfg(min, max);
        c.params.initial = Some(initial);
        c
    }

    /// A due probe pins new starts to the minimum; the median of the chains
    /// started during it replaces a baseline that was learned under load,
    /// and the limit comes back. Chains started before the probe don't count.
    #[tokio::test(start_paused = true)]
    async fn probe_relearns_baseline_at_low_load() {
        let node = ChainNode::new(cfg_initial(4, 100, 50));
        let k = key(Some("payment"), "Root");
        let mut now = Instant::now();
        // learned under load: chains take 1 s
        assert!(node.try_admit(&k, false, now).is_some());
        for _ in 0..20 {
            now += Duration::from_millis(1050);
            node.sample(&k, Duration::from_secs(1), 0, now);
        }
        assert!((node.baseline(&k) - 1000.0).abs() < 50.0);
        node.release(&k, now);
        // the probe is due within the jittered 5 minutes
        now += PROBE_INTERVAL;
        let mut tags = Vec::new();
        for _ in 0..4 {
            tags.push(node.try_admit(&k, false, now).expect("probe admits up to min"));
        }
        assert!(tags.iter().all(|t| *t != 0), "chains started in the probe are tagged");
        assert!(node.try_admit(&k, false, now).is_none(), "pinned to min 4, not the limit 50");
        // an old chain finishing now is not a probe sample
        node.sample(&k, Duration::from_secs(1), 0, now);
        // probe chains run at low load: 100 ms each
        let tag = tags[0];
        for _ in 0..PROBE_SAMPLES {
            node.sample(&k, Duration::from_millis(100), tag, now);
        }
        assert!(
            (node.baseline(&k) - 100.0).abs() < 1.0,
            "baseline reset to the probe median, got {}",
            node.baseline(&k)
        );
        for _ in 0..4 {
            node.release(&k, now);
        }
        assert!(node.headroom(&k, now) > 4, "probe over: the learned limit is back");
    }

    /// A probe that cannot collect its samples ends after PROBE_MAX without
    /// touching the baseline, and admission is back to the learned limit.
    #[tokio::test(start_paused = true)]
    async fn probe_times_out_without_change() {
        let node = ChainNode::new(cfg_initial(4, 100, 50));
        let k = key(None, "Root");
        let mut now = Instant::now();
        assert!(node.try_admit(&k, false, now).is_some());
        for _ in 0..20 {
            now += Duration::from_millis(1050);
            node.sample(&k, Duration::from_secs(1), 0, now);
        }
        node.release(&k, now);
        let before = node.baseline(&k);
        now += PROBE_INTERVAL;
        assert!(node.try_admit(&k, false, now).is_some(), "probe starts");
        assert_eq!(node.headroom(&k, now), 3, "pinned to min during the probe");
        now += PROBE_MAX;
        assert!(node.headroom(&k, now) > 3, "timed out: back to the learned limit");
        assert_eq!(node.baseline(&k), before, "no samples, no change");
    }
}
