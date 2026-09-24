// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Chain admission metric names (one Gradient2 controller per scope and root
//! service, node-wide). Labels: `scope` and `root`. `CHAIN_WAITERS` is the
//! only partition-local one and also carries `partition_id`.

pub const CHAIN_LIMIT: &str = "restate.chain_admission.limit";
pub const CHAIN_IN_PROGRESS: &str = "restate.chain_admission.in_progress";
pub const CHAIN_RUNNING: &str = "restate.chain_admission.running";
pub const CHAIN_GRADIENT: &str = "restate.chain_admission.gradient";
pub const CHAIN_SHORT_RTT_MS: &str = "restate.chain_admission.short_rtt_ms";
pub const CHAIN_LONG_RTT_MS: &str = "restate.chain_admission.long_rtt_ms";
pub const CHAIN_UPDATES_TOTAL: &str = "restate.chain_admission.updates.total";
pub const CHAIN_DRIFT_DECAY_TOTAL: &str = "restate.chain_admission.drift_decay.total";
pub const CHAIN_SAMPLES_TOTAL: &str = "restate.chain_admission.samples.total";
pub const CHAIN_ACTIVE_TIME_SECONDS: &str = "restate.chain_admission.active_time.seconds";
pub const CHAIN_WAITERS: &str = "restate.chain_admission.waiters";
