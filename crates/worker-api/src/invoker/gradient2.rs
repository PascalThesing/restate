// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adaptive concurrency controller (Netflix Gradient2 style). One instance
//! exists per root service (chain admission); the latency signal is the
//! chain's active end-to-end time.
//!
//! The limit grows/shrinks by the ratio of a slow baseline to the current
//! short sample, clamped to `[min, max]`; growth is additive only and
//! never revokes issued permits.
//!
//! Slow creep (latency rising so gradually that the baseline follows it) is
//! not detected; drift decay only deflates an inflated baseline.

use std::num::NonZeroU32;
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use tokio::time::Instant;

use super::chain_metrics::{
    CHAIN_ACTIVE_TIME_SECONDS, CHAIN_DRIFT_DECAY_TOTAL, CHAIN_GRADIENT, CHAIN_LIMIT,
    CHAIN_LONG_RTT_MS, CHAIN_RUNNING, CHAIN_SAMPLES_TOTAL, CHAIN_SHORT_RTT_MS,
    CHAIN_UPDATES_TOTAL,
};

/// Numeric parameters of one controller, independent of what it governs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ControllerParams {
    pub min: u32,
    pub max: u32,
    pub tolerance_permille: u32,
    pub smoothing_permille: u32,
    /// Cold-start limit. `None` picks a quarter into the corridor when `max`
    /// is explicit, else a small multiple of `min`.
    pub initial: Option<u32>,
}

/// Which metric family a controller reports into.
#[derive(Debug, Clone, Copy)]
pub enum ControllerFamily {
    /// Per scope and root service (chain admission); labels `scope`, `root`.
    Chain,
}

struct MetricNames {
    label: &'static str,
    samples: &'static str,
    hold_time: &'static str,
    in_flight: &'static str,
    limit: &'static str,
    long_rtt: &'static str,
    short_rtt: &'static str,
    gradient: &'static str,
    drift_decay: &'static str,
    updates: &'static str,
}

const CHAIN_METRICS: MetricNames = MetricNames {
    label: "root",
    samples: CHAIN_SAMPLES_TOTAL,
    hold_time: CHAIN_ACTIVE_TIME_SECONDS,
    in_flight: CHAIN_RUNNING,
    limit: CHAIN_LIMIT,
    long_rtt: CHAIN_LONG_RTT_MS,
    short_rtt: CHAIN_SHORT_RTT_MS,
    gradient: CHAIN_GRADIENT,
    drift_decay: CHAIN_DRIFT_DECAY_TOTAL,
    updates: CHAIN_UPDATES_TOTAL,
};

/// Additive growth headroom per update (Netflix queueSize constant).
const QUEUE_SIZE: f64 = 4.0;
/// Minimum interval between limit updates (Temporal ramp-throttle lesson).
const MIN_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// Time constant of the long (baseline) EMA.
const LONG_EMA_TIME_CONSTANT: Duration = Duration::from_secs(120);
/// Number of warm-up samples averaged arithmetically before the EMA takes over.
const WARMUP_SAMPLES: u32 = 10;

/// Outcome of feeding one sample; used for metrics and by the caller to
/// decide whether waiters need waking (limit increased).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    Increase,
    Decrease,
    ClampMin,
    ClampMax,
    AppLimited,
    /// The limit would have grown, but it was not the binding gate (the
    /// share or the pool held the root back): kept as is (RFC 7661).
    ShareLimited,
    IntervalSkip,
}

pub struct Gradient2Controller {
    // -- configuration ------------------------------------------------------
    /// Kept so identical re-upserts (helm redeploys) preserve the learned
    /// state instead of resetting it.
    params: ControllerParams,
    min: f64,
    max: f64,
    tolerance: f64,
    smoothing: f64,
    // -- pre-resolved metric handles (labels are fixed per controller; this
    // -- avoids per-completion String clones on the hot path) ---------------
    m_samples: Counter,
    m_hold_time: Histogram,
    m_in_flight: Gauge,
    m_limit: Gauge,
    m_long_rtt: Gauge,
    m_short_rtt: Gauge,
    m_gradient: Gauge,
    m_drift_decay: Counter,
    m_out_increase: Counter,
    m_out_decrease: Counter,
    m_out_clamp_min: Counter,
    m_out_clamp_max: Counter,
    m_out_app_limited: Counter,
    m_out_share_limited: Counter,

    // -- controller state ---------------------------------------------------
    limit: f64,
    long_ema: f64,
    warmup_count: u32,
    warmup_sum: f64,
    last_update: Instant,
    last_sample: Instant,

}

impl Gradient2Controller {
    pub fn new(
        params: ControllerParams,
        family: ControllerFamily,
        scope_label: String,
        label: String,
        now: Instant,
    ) -> Self {
        let names: &'static MetricNames = match family {
            ControllerFamily::Chain => &CHAIN_METRICS,
        };
        let min = params.min.max(1) as f64;
        let max = (params.max.max(params.min)) as f64;
        let tolerance = params.tolerance_permille as f64 / 1000.0;
        let smoothing = (params.smoothing_permille as f64 / 1000.0).clamp(0.01, 1.0);
        let initial = params
            .initial
            .map(|i| i as f64)
            .unwrap_or((min * 8.0).min(max))
            .clamp(min, max);
        let key = names.label;
        let outcome_counter = |o: &'static str| counter!(names.updates, "scope" => scope_label.clone(), key => label.clone(), "outcome" => o);
        let controller = Self {
            params,
            min,
            max,
            tolerance,
            smoothing,
            m_samples: counter!(names.samples, "scope" => scope_label.clone(), key => label.clone()),
            m_hold_time: histogram!(names.hold_time, "scope" => scope_label.clone(), key => label.clone()),
            m_in_flight: gauge!(names.in_flight, "scope" => scope_label.clone(), key => label.clone()),
            m_limit: gauge!(names.limit, "scope" => scope_label.clone(), key => label.clone()),
            m_long_rtt: gauge!(names.long_rtt, "scope" => scope_label.clone(), key => label.clone()),
            m_short_rtt: gauge!(names.short_rtt, "scope" => scope_label.clone(), key => label.clone()),
            m_gradient: gauge!(names.gradient, "scope" => scope_label.clone(), key => label.clone()),
            m_drift_decay: counter!(names.drift_decay, "scope" => scope_label.clone(), key => label.clone()),
            m_out_increase: outcome_counter("increase"),
            m_out_decrease: outcome_counter("decrease"),
            m_out_clamp_min: outcome_counter("clamp_min"),
            m_out_clamp_max: outcome_counter("clamp_max"),
            m_out_app_limited: outcome_counter("app_limited"),
            m_out_share_limited: outcome_counter("share_limited"),
            limit: initial,
            long_ema: 0.0,
            warmup_count: 0,
            warmup_sum: 0.0,
            last_update: now,
            last_sample: now,
        };
        controller.emit_state_gauges(1.0);
        controller
    }

    /// Current parameters.
    #[allow(dead_code)]
    pub fn params(&self) -> ControllerParams {
        self.params
    }

    pub fn current_limit(&self) -> NonZeroU32 {
        // limit is clamped to [min>=1, ..]; the fallback can't trigger but
        // keeps the conversion total.
        NonZeroU32::new(self.limit as u32).unwrap_or(NonZeroU32::MIN)
    }

    /// Feeds one permit-hold-time sample. Returns the update outcome; on
    /// [`UpdateOutcome::Increase`] the caller should wake waiters blocked on
    /// this rule.
    ///
    /// `in_flight` is the number of chains that actually run; `limit_binding`
    /// says whether the limit itself held starts back since the last update
    /// (the flight size reached the limit). Without that, the limit never
    /// grows: growing a limit that was not used would only inflate it
    /// (RFC 7661). Shrinking on rising latency stays allowed either way.
    pub fn on_sample(
        &mut self,
        hold: Duration,
        in_flight: u32,
        limit_binding: bool,
        now: Instant,
    ) -> UpdateOutcome {
        self.m_samples.increment(1);
        self.m_hold_time.record(hold.as_secs_f64());

        // Guard degenerate samples: a zero-duration hold carries no gradient
        // information; floor at 100µs to keep the ratio finite.
        let sample_ms = (hold.as_secs_f64() * 1000.0).max(0.1);
        self.last_sample = now;

        // Baseline: arithmetic warm-up, then a time-constant EMA (rate
        // independent — sample rates differ wildly across rules).
        if self.warmup_count < WARMUP_SAMPLES {
            self.warmup_count += 1;
            self.warmup_sum += sample_ms;
            self.long_ema = self.warmup_sum / self.warmup_count as f64;
        } else {
            let dt = now
                .saturating_duration_since(self.last_update)
                .as_secs_f64()
                .max(0.001);
            let alpha = 1.0 - (-dt / LONG_EMA_TIME_CONSTANT.as_secs_f64()).exp();
            self.long_ema += alpha * (sample_ms - self.long_ema);
        }

        // Baseline drift decay: after a load spike ends, pull an inflated
        // baseline back down.
        if self.long_ema / sample_ms > 2.0 {
            self.long_ema *= 0.95;
            self.m_drift_decay.increment(1);
        }

        // Update gate: the effect of the previous update must become
        // measurable before the next one. Hot path: most samples land here.
        if now.saturating_duration_since(self.last_update) < MIN_UPDATE_INTERVAL {
            return UpdateOutcome::IntervalSkip;
        }
        self.last_update = now;
        self.m_in_flight.set(in_flight as f64);

        // App-limited guard: no growth without real demand.
        if (in_flight as f64) < self.limit / 2.0 {
            return self.finish_update(UpdateOutcome::AppLimited, sample_ms, 1.0);
        }

        let gradient = (self.tolerance * self.long_ema / sample_ms).clamp(0.5, 1.0);
        let mut new_limit = self.limit * gradient + QUEUE_SIZE;
        new_limit = self.limit * (1.0 - self.smoothing) + new_limit * self.smoothing;

        let outcome = if new_limit > self.limit && !limit_binding {
            new_limit = self.limit;
            UpdateOutcome::ShareLimited
        } else if new_limit <= self.min {
            new_limit = self.min;
            UpdateOutcome::ClampMin
        } else if new_limit >= self.max {
            new_limit = self.max;
            UpdateOutcome::ClampMax
        } else if new_limit > self.limit {
            UpdateOutcome::Increase
        } else {
            UpdateOutcome::Decrease
        };
        self.limit = new_limit;
        self.finish_update(outcome, sample_ms, gradient)
    }

    fn finish_update(
        &self,
        outcome: UpdateOutcome,
        sample_ms: f64,
        gradient: f64,
    ) -> UpdateOutcome {
        match outcome {
            UpdateOutcome::Increase => self.m_out_increase.increment(1),
            UpdateOutcome::Decrease => self.m_out_decrease.increment(1),
            UpdateOutcome::ClampMin => self.m_out_clamp_min.increment(1),
            UpdateOutcome::ClampMax => self.m_out_clamp_max.increment(1),
            UpdateOutcome::AppLimited => self.m_out_app_limited.increment(1),
            UpdateOutcome::ShareLimited => self.m_out_share_limited.increment(1),
            UpdateOutcome::IntervalSkip => {}
        }
        self.m_short_rtt.set(sample_ms);
        self.emit_state_gauges(gradient);
        outcome
    }

    fn emit_state_gauges(&self, gradient: f64) {
        self.m_limit.set(self.limit);
        self.m_long_rtt.set(self.long_ema);
        self.m_gradient.set(gradient);
    }

    #[cfg(test)]
    pub fn limit_f64(&self) -> f64 {
        self.limit
    }

    #[cfg(test)]
    pub fn long_ema(&self) -> f64 {
        self.long_ema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> ControllerParams {
        ControllerParams {
            min: 4,
            max: 300,
            tolerance_permille: 1500,
            smoothing_permille: 200,
            initial: Some(4 + (300 - 4) / 4),
        }
    }

    fn controller(now: Instant) -> Gradient2Controller {
        Gradient2Controller::new(params(), ControllerFamily::Chain, "payment".into(), "Workflow".into(), now)
    }

    fn ms(v: u64) -> Duration {
        Duration::from_millis(v)
    }

    /// Feeds one sample per second (past the update gate) with the given
    /// latency; in_flight tracks the limit so the app-limited guard stays off.
    fn drive(c: &mut Gradient2Controller, now: &mut Instant, latency: Duration, n: usize) {
        for _ in 0..n {
            *now += Duration::from_millis(1050);
            let in_flight = c.current_limit().get();
            c.on_sample(latency, in_flight, true, *now);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cold_start_within_corridor() {
        let now = Instant::now();
        let c = controller(now);
        // corridor start: min + (max-min)/4 = 4 + 74 = 78
        assert_eq!(c.current_limit().get(), 78);

        // no explicit initial: min*8
        let c2 = Gradient2Controller::new(
            ControllerParams {
                min: 4,
                max: 10_000,
                tolerance_permille: 1500,
                smoothing_permille: 200,
                initial: None,
            },
            ControllerFamily::Chain,
            "p".into(),
            "0".into(),
            now,
        );
        assert_eq!(c2.current_limit().get(), 32);
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_latency_grows_additively_and_clamps_at_max() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 400);
        // stable latency -> gradient clamps at 1.0 -> additive growth to max
        assert_eq!(c.current_limit().get(), 300);
    }

    #[tokio::test(start_paused = true)]
    async fn latency_spike_shrinks_bounded_per_update() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 100);
        let before = c.limit_f64();
        // 4x usual latency -> gradient floor 0.5; shrink is smoothing-bounded
        now += Duration::from_millis(1050);
        c.on_sample(ms(400), c.current_limit().get(), true, now);
        let after = c.limit_f64();
        assert!(after < before);
        assert!(after > before * 0.89, "shrink must be <= ~10%/update");
    }

    #[tokio::test(start_paused = true)]
    async fn converges_down_under_sustained_congestion() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 100);
        // congestion ONSET: 5x latency vs baseline -> gradient floors, limit
        // falls fast, well below the healthy level
        drive(&mut c, &mut now, ms(500), 20);
        assert!(c.limit_f64() < 100.0, "limit was {}", c.limit_f64());
        // SUSTAINED congestion: the 120s-EMA baseline adapts to the new normal
        // (by design — G2 reacts to latency *changes*, not absolute levels),
        // so the limit partially recovers but stays below the ceiling
        drive(&mut c, &mut now, ms(500), 180);
        assert!(c.limit_f64() < 300.0, "limit was {}", c.limit_f64());
        // and recovers fully once latency normalizes (drift decay pulls the
        // inflated baseline back down)
        drive(&mut c, &mut now, ms(100), 400);
        assert_eq!(c.current_limit().get(), 300);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_always_within_bounds_under_random_latency() {
        let mut now = Instant::now();
        let mut c = controller(now);
        // deterministic pseudo-random latencies (LCG), 100µs..8s
        let mut seed: u64 = 0x5eed;
        for _ in 0..5_000 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let latency_us = 100 + (seed >> 33) % 8_000_000;
            now += Duration::from_millis(300);
            let in_flight = ((seed >> 7) % 400) as u32;
            c.on_sample(Duration::from_micros(latency_us), in_flight, (seed >> 5) & 1 == 0, now);
            let l = c.limit_f64();
            assert!((4.0..=300.0).contains(&l), "limit out of bounds: {l}");
            assert!(l.is_finite());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn app_limited_blocks_growth() {
        let mut now = Instant::now();
        let mut c = controller(now);
        let before = c.current_limit().get();
        for _ in 0..50 {
            now += Duration::from_millis(1050);
            // healthy latency but almost no in-flight demand
            c.on_sample(ms(100), 1, true, now);
        }
        assert_eq!(c.current_limit().get(), before, "app-limited must not grow");
    }

    #[tokio::test(start_paused = true)]
    async fn interval_gate_skips_fast_samples() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 20);
        let before = c.limit_f64();
        // 100 samples within one second: all gated, limit unchanged
        for _ in 0..100 {
            now += Duration::from_millis(5);
            let out = c.on_sample(ms(100), c.current_limit().get(), true, now);
            assert_eq!(out, UpdateOutcome::IntervalSkip);
        }
        assert_eq!(c.limit_f64(), before);
    }

    #[tokio::test(start_paused = true)]
    async fn drift_decay_pulls_baseline_down() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(800), 50);
        let inflated = c.long_ema();
        // latency collapses to a fifth: decay activates (ratio > 2)
        drive(&mut c, &mut now, ms(100), 30);
        assert!(c.long_ema() < inflated / 2.0);
    }

    #[tokio::test(start_paused = true)]
    async fn degenerate_samples_do_not_poison() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, Duration::ZERO, 30);
        assert!(c.limit_f64().is_finite());
        drive(&mut c, &mut now, Duration::from_secs(3600), 5);
        assert!(c.limit_f64().is_finite());
        assert!((4.0..=300.0).contains(&c.limit_f64()));
    }

    /// A limit that is not the binding gate never grows, even with flat
    /// latency and full demand: it is reported `ShareLimited` and kept.
    /// Shrinking on a latency rise stays allowed.
    #[tokio::test(start_paused = true)]
    async fn no_growth_unless_the_limit_binds() {
        let mut now = Instant::now();
        let mut c = controller(now);
        let start = c.current_limit().get();
        for _ in 0..30 {
            now += Duration::from_millis(1050);
            let out = c.on_sample(ms(100), c.current_limit().get(), false, now);
            assert!(
                matches!(out, UpdateOutcome::ShareLimited | UpdateOutcome::IntervalSkip),
                "got {out:?}"
            );
        }
        assert_eq!(c.current_limit().get(), start, "kept while not binding");
        // binding again: growth resumes
        drive(&mut c, &mut now, ms(100), 5);
        assert!(c.current_limit().get() > start);
        // not binding, but latency 4x: still shrinks
        let before = c.current_limit().get();
        now += Duration::from_millis(1050);
        let out = c.on_sample(ms(400), c.current_limit().get(), false, now);
        assert_eq!(out, UpdateOutcome::Decrease);
        assert!(c.current_limit().get() < before);
    }
}
