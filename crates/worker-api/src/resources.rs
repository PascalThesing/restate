// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use smallvec::SmallVec;
use tokio::sync::mpsc;

use restate_futures_util::concurrency::Permit;

use crate::invoker::slot_shares::SlotLease;
use restate_limiter::LimitKey;
use restate_memory::MemoryLease;
use restate_storage_api::vqueue_table::EntryMetadata;
use restate_types::vqueues::EntryId;
use restate_types::Scope;
use restate_util_string::ReString;

// Re-export so consumers can keep importing from `restate_worker_api::resources`.
pub use restate_limiter::{RuleUpdate, UserLimits};

pub enum ResourceManagerUpdate {
    /// User permits released by a completed (or suspended) run attempt.
    PermitReleased {
        kinds: SmallVec<[UserPermitKind; 1]>,
    },
    /// A batch of rule mutations to apply in order. Carries `Vec` rather
    /// than a single `RuleUpdate` so initial seeding and bulk rule-book
    /// diffs can ship as one channel message.
    RulesUpdated(Box<[RuleUpdate]>),
    /// Leader-side chain lifecycle signal (see [`ChainSignal`]).
    ChainSignal(ChainSignal),
}

/// Lifecycle signal for chain admission. A *chain* is a root invocation
/// (one that no other invocation called) together with everything it
/// calls. Roots are marked durably on their vqueue entry
/// (`EntryMetadata::chain_root`); the partition leader emits these signals
/// for the transitions the scheduler cannot observe itself.
#[derive(Debug, Clone)]
pub struct ChainSignal {
    pub entry_id: EntryId,
    pub kind: ChainSignalKind,
}

#[derive(Debug, Clone)]
pub enum ChainSignalKind {
    /// The root stopped consuming anything downstream: it suspended on an
    /// external future (awakeable, sleep, promise, signal) or was paused.
    /// Its permit is released and its clock paused; it is re-admitted (ahead
    /// of new starts) when it runs again.
    Pause,
    /// The root reached a terminal state. `completed` selects whether the
    /// chain's active time becomes a latency sample.
    End { completed: bool },
}

pub enum UserPermitKind {
    // todo: DeploymentConcurrency,
    LimitKeyConcurrency(Scope, LimitKey<ReString>),
}

#[derive(Default, Clone, Copy)]
pub struct ThrottlingToken;

/// Resources reserved from global limiters for a single invocation.
///
/// Bundles a concurrency [`Permit`] and a [`MemoryLease`] so they travel
/// together through the scheduler → leader handoff.
#[non_exhaustive]
pub struct SystemPermit {
    pub invoker_permit: Permit,
    /// The lane's share of the invoker slot; dropped together with the permit.
    pub slot_lease: SlotLease,
    pub throttling_permit: Option<ThrottlingToken>,
    pub memory_lease: MemoryLease,
}

impl Default for SystemPermit {
    fn default() -> Self {
        Self {
            invoker_permit: Permit::new_empty(),
            slot_lease: SlotLease::empty(),
            throttling_permit: None,
            memory_lease: MemoryLease::unlinked(),
        }
    }
}

impl SystemPermit {
    pub fn take(&mut self) -> SystemPermit {
        SystemPermit {
            invoker_permit: self.invoker_permit.split(1).unwrap_or(Permit::new_empty()),
            slot_lease: self.slot_lease.take(),
            throttling_permit: self.throttling_permit.take(),
            memory_lease: self.memory_lease.take(),
        }
    }
}

// A compound permit holds a set of resources and provides remote termination access
// and signaling.
#[must_use]
#[clippy::has_significant_drop]
pub struct ReservedResources {
    pub metadata: EntryMetadata,
    resources: SmallVec<[UserPermitKind; 1]>,
    system_permit: SystemPermit,
    manager_tx: Option<mpsc::UnboundedSender<ResourceManagerUpdate>>,
}

impl ReservedResources {
    pub fn new_empty() -> Self {
        Self {
            metadata: EntryMetadata::default(),
            resources: SmallVec::new(),
            system_permit: SystemPermit::default(),
            manager_tx: None,
        }
    }

    pub fn new(
        metadata: EntryMetadata,
        resources: SmallVec<[UserPermitKind; 1]>,
        system_permit: SystemPermit,
        manager_tx: mpsc::UnboundedSender<ResourceManagerUpdate>,
    ) -> Self {
        Self {
            metadata,
            resources,
            system_permit,
            manager_tx: Some(manager_tx),
        }
    }

    // Moves the initial memory budget and hands it over to the caller.
    pub fn take_memory_budget(&mut self) -> MemoryLease {
        self.system_permit.memory_lease.take()
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

// Release the resources via a channel with the resource manager
impl Drop for ReservedResources {
    fn drop(&mut self) {
        if let Some(manager_tx) = self.manager_tx.take()
            && !self.is_empty()
        {
            let _ = manager_tx.send(ResourceManagerUpdate::PermitReleased {
                kinds: self.resources.drain(..).collect(),
            });
        }
    }
}
