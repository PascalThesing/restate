// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram};

pub const VQUEUE_ENQUEUE: &str = "restate.vqueue.scheduler.enqueue.total";
pub const VQUEUE_SCHEDULER_DECISION: &str = "restate.vqueue.scheduler.decision.total";
pub const VQUEUE_RUN_CONFIRMED: &str = "restate.vqueue.scheduler.run_confirmed.total";
pub const VQUEUE_INVOKER_MEMORY_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_memory_wait_ms.total";
pub const VQUEUE_INVOKER_CONCURRENCY_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_concurrency_wait_ms.total";

// Node-level invoker throttling (affects resume/start)
pub const VQUEUE_INVOKER_THROTTLING_WAIT_MS: &str =
    "restate.vqueue.scheduler.invoker_throttling_wait_ms.total";
// Per-vqueue user-defined throttling rules
pub const VQUEUE_THROTTLING_RULES_WAIT_MS: &str =
    "restate.vqueue.scheduler.throttling_rules_wait_ms.total";
pub const VQUEUE_CONCURRENCY_RULES_WAIT_MS: &str =
    "restate.vqueue.scheduler.concurrency_rules_wait_ms.total";
pub const VQUEUE_LOCK_WAIT_MS: &str = "restate.vqueue.scheduler.lock_wait_ms.total";
pub const VQUEUE_DEPLOYMENT_CONCURRENCY_WAIT_MS: &str =
    "restate.vqueue.scheduler.deployment_concurrency_wait_ms.total";

pub const ACTION_YIELD: &str = "yield";
pub const ACTION_RUN: &str = "run";

// -- Chain admission (automatic Gradient2 per scope and root service, node-wide) --
pub use restate_worker_api::invoker::chain_metrics::{
    CHAIN_ACTIVE_TIME_SECONDS, CHAIN_IN_PROGRESS, CHAIN_LIMIT, CHAIN_RUNNING,
    CHAIN_SAMPLES_TOTAL, CHAIN_UPDATES_TOTAL, CHAIN_WAITERS,
};

pub fn describe_metrics() {
    describe_gauge!(
        CHAIN_LIMIT,
        Unit::Count,
        "Chain admission: learned limit of chains in progress per scope and root service (node-wide)"
    );
    describe_gauge!(
        CHAIN_IN_PROGRESS,
        Unit::Count,
        "Chain admission: chains currently holding a permit per scope and root service (node-wide)"
    );
    describe_gauge!(
        CHAIN_RUNNING,
        Unit::Count,
        "Chain admission: admitted chains that hold their invoker slot (the controller's flight size)"
    );
    describe_gauge!(
        CHAIN_WAITERS,
        Unit::Count,
        "Chain admission: queues parked waiting for a chain permit"
    );
    describe_histogram!(
        CHAIN_ACTIVE_TIME_SECONDS,
        Unit::Seconds,
        "Chain admission: active end-to-end time of completed chains (external waits excluded)"
    );
    describe_counter!(
        CHAIN_SAMPLES_TOTAL,
        Unit::Count,
        "Chain admission: latency samples fed to the controller"
    );
    describe_counter!(
        CHAIN_UPDATES_TOTAL,
        Unit::Count,
        "Chain admission: controller updates by outcome"
    );
    describe_counter!(
        VQUEUE_ENQUEUE,
        Unit::Count,
        "Number of entries/invocations in vqueues added to the inbox stage"
    );

    describe_counter!(
        VQUEUE_SCHEDULER_DECISION,
        Unit::Count,
        "Number of entries in vqueues scheduler, broken down by decision"
    );

    describe_counter!(
        VQUEUE_RUN_CONFIRMED,
        Unit::Count,
        "Number of entries/invocations in vqueues where the run request was confirmed"
    );

    describe_counter!(
        VQUEUE_INVOKER_CONCURRENCY_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting for global invoker capacity"
    );

    describe_counter!(
        VQUEUE_INVOKER_MEMORY_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting for invoker memory pool"
    );

    describe_counter!(
        VQUEUE_INVOKER_THROTTLING_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds vqueues waited on node-level invoker throttling"
    );

    describe_counter!(
        VQUEUE_THROTTLING_RULES_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds vqueues spent blocked on user-defined throttling rules"
    );

    describe_counter!(
        VQUEUE_CONCURRENCY_RULES_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting on user-defined concurrency rules"
    );

    describe_counter!(
        VQUEUE_LOCK_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent waiting to acquire virtual object locks"
    );

    describe_counter!(
        VQUEUE_DEPLOYMENT_CONCURRENCY_WAIT_MS,
        Unit::Count,
        "Cumulative number of milliseconds spent blocked on deployment concurrency capacity"
    );
}

pub fn publish_scheduler_decision_metrics(num_run: u32, num_yield: u32) {
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_RUN).increment(num_run as u64);
    counter!(VQUEUE_SCHEDULER_DECISION, "action" => ACTION_YIELD).increment(num_yield as u64);
}
