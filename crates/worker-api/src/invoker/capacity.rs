// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use restate_futures_util::concurrency::Concurrency;
use restate_memory::{MemoryPool, NonZeroByteCount};
use restate_types::config::{
    ChainAdmissionOptions, DEFAULT_PER_INVOCATION_INITIAL_MEMORY, ThrottlingOptions,
};

use super::chain_node::{ChainAdmissionConfig, ChainNode};
use super::slot_shares::SlotShares;

pub type TokenBucket<C = gardal::TokioClock> = gardal::SharedTokenBucket<C>;

#[derive(Clone)]
pub struct InvokerCapacity {
    pub concurrency: Concurrency,
    pub invocation_token_bucket: Option<TokenBucket>,
    pub action_token_bucket: Option<TokenBucket>,
    pub memory_pool: MemoryPool,
    /// Outbound initial memory in bytes reserved from the memory pool per invocation.
    pub initial_invocation_memory: NonZeroByteCount,
    /// Weighted shares of `concurrency`, node-wide (disabled unless configured).
    pub slot_shares: SlotShares,
    /// With shares on: consecutive in-flight grants before one freed slot goes
    /// to a waiting new start.
    pub in_flight_priority_burst: u32,
    /// Node-wide chain admission (disabled unless configured).
    pub chain_node: ChainNode,
}

impl InvokerCapacity {
    pub const fn new_unlimited() -> Self {
        Self {
            concurrency: Concurrency::new_unlimited(),
            invocation_token_bucket: None,
            action_token_bucket: None,
            memory_pool: MemoryPool::unlimited(),
            initial_invocation_memory: DEFAULT_PER_INVOCATION_INITIAL_MEMORY,
            slot_shares: SlotShares::disabled(),
            in_flight_priority_burst: 8,
            chain_node: ChainNode::disabled(),
        }
    }

    pub fn new(
        concurrency: Option<NonZeroUsize>,
        invocation_throttling: Option<&ThrottlingOptions>,
        action_throttling: Option<&ThrottlingOptions>,
        memory_pool: MemoryPool,
        initial_invocation_memory: NonZeroByteCount,
        weighted_slot_shares: bool,
        in_flight_priority_burst: u32,
        chain_admission: &ChainAdmissionOptions,
    ) -> Self {
        Self {
            slot_shares: SlotShares::new(concurrency, weighted_slot_shares),
            in_flight_priority_burst,
            chain_node: ChainNode::new(ChainAdmissionConfig::from_options(
                chain_admission,
                concurrency.map(|c| c.get()),
            )),
            concurrency: Concurrency::new(concurrency),
            invocation_token_bucket: invocation_throttling.map(|opts| {
                TokenBucket::new(gardal::Limit::from(opts.clone()), gardal::TokioClock)
            }),
            action_token_bucket: action_throttling.map(|opts| {
                TokenBucket::new(gardal::Limit::from(opts.clone()), gardal::TokioClock)
            }),
            memory_pool,
            initial_invocation_memory,
        }
    }
}
