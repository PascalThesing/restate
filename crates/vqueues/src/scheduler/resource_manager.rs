// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod grouped_waiters;
mod invoker;

/// Test-only handle to the grouped waiter list (weight-lifecycle tests live in
/// `scheduler.rs` next to the scope-weights plumbing they exercise).
#[cfg(test)]
pub(super) fn test_grouped_waiters(
    weight_resolver: super::eligible::WeightResolver,
) -> grouped_waiters::GroupedWaiters {
    // lane weights default to 1 here — these tests exercise GROUP weight
    // semantics; lane semantics are tested in grouped_waiters.rs itself
    grouped_waiters::GroupedWaiters::new(
        weight_resolver,
        std::sync::Arc::new(
            |_: &super::eligible::SchedulingGroup, _: &restate_types::ServiceName| {
                std::num::NonZeroU32::MIN
            },
        ),
    )
}
mod chain_admission;
mod invoker_memory;
mod invoker_throttle;
mod locks;
mod permit;
mod share_gate;
mod user_limiter;

pub use self::permit::PermitBuilder;
pub use restate_worker_api::invoker::chain_node::{ChainAdmissionConfig, ChainNode};
pub use restate_worker_api::invoker::gradient2::ControllerParams;

use std::collections::VecDeque;
use std::task::Poll;

use tokio::sync::mpsc;
use tracing::trace;

use restate_futures_util::concurrency::Concurrency;
use restate_limiter::RuleHandle;
use restate_limiter::RuleUpdate;
use restate_memory::{MemoryPool, NonZeroByteCount};
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LoadLocks;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryKey, EntryMetadata};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::EntryKind;
use restate_types::{LockName, Scope};
use restate_util_string::ReString;
use restate_worker_api::invoker::slot_shares::SlotShares;
use restate_worker_api::resources::{ChainSignal, ResourceManagerUpdate, UserPermitKind};
use restate_worker_api::{ResourceKind, UserLimitCounterEntry};

use self::chain_admission::{Admit, ChainAdmission};
use restate_worker_api::invoker::chain_node::ChainKey;
use self::invoker::InvokerConcurrencyLimiter;
use self::invoker_memory::InvokerMemoryLimiter;
use self::invoker_throttle::{InvokerThrottlingLimiter, ThrottlingAcquire};
use self::locks::Locks;
use self::permit::ProvisionalPermit;
use self::share_gate::ShareGate;
use self::user_limiter::UserLimiter;
use super::VQueueHandle;
use super::eligible::{EligibilityTracker, SchedulingGroup, WeightResolver, LaneWeightResolver};
use crate::GlobalTokenBucket;

// A set of queues waiting on a resource
type Waiters = VecDeque<VQueueHandle>;

pub struct ResourceManager {
    // Resources
    locks: Locks,
    /// Limiter for invoker global capacity
    invoker_concurrency: InvokerConcurrencyLimiter,
    invoker_throttling: InvokerThrottlingLimiter,
    invoker_memory: InvokerMemoryLimiter,
    user_limiter: UserLimiter,
    chain: ChainAdmission,
    /// Weighted slot shares for new starts, enforced before the invoker.
    share_gate: ShareGate,
    /// User permits to revert on the next `poll_resources` (a start refused
    /// at claim time gives everything back, but the eligibility tracker is
    /// not at hand there).
    deferred_revert: Vec<PermitBuilder>,
    rx: mpsc::UnboundedReceiver<ResourceManagerUpdate>,
    // We need to keep this alive to:
    // - Keep the receiver alive even if we don't have any resource permits handed out
    tx: mpsc::UnboundedSender<ResourceManagerUpdate>,
}

pub(super) enum AcquireOutcome {
    Acquired(PermitBuilder),
    BlockedOn(ResourceKind),
}

impl ResourceManager {
    pub async fn create<S: LoadLocks + Send + Sync + 'static>(
        storage: S,
        concurrency_limiter: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
        memory_pool: MemoryPool,
        initial_invocation_memory: NonZeroByteCount,
        weight_resolver: WeightResolver,
        lane_weight_resolver: LaneWeightResolver,
        partition_label: String,
        chain_node: ChainNode,
        slot_shares: SlotShares,
        in_flight_priority_burst: u32,
    ) -> Result<Self, StorageError> {
        let locks = Locks::create(storage).await?;

        let (_tx, rx) = mpsc::unbounded_channel();

        Ok(Self {
            invoker_concurrency: InvokerConcurrencyLimiter::new(
                concurrency_limiter,
                weight_resolver,
                lane_weight_resolver,
                slot_shares.clone(),
                in_flight_priority_burst,
            ),
            share_gate: ShareGate::new(slot_shares),
            invoker_throttling: InvokerThrottlingLimiter::new(global_throttling),
            invoker_memory: InvokerMemoryLimiter::new(memory_pool, initial_invocation_memory),
            user_limiter: UserLimiter::create(),
            chain: ChainAdmission::new(chain_node, partition_label),
            deferred_revert: Vec::new(),
            locks,
            rx,
            tx: _tx,
        })
    }

    /// Forward a batch of rule-book updates to the resource manager's
    /// internal channel. Picked up on the next `poll_resources` tick and
    /// applied to the `UserLimiter`.
    pub fn on_rules_updated(&self, updates: Box<[RuleUpdate]>) {
        // Sending via `tx` (which is held alongside `rx` for keep-alive)
        // never fails: the receiver is owned by `self`.
        let _ = self.tx.send(ResourceManagerUpdate::RulesUpdated(updates));
    }

    /// Forward a chain lifecycle signal; applied on the next `poll_resources`
    /// tick so waiters can be woken with the eligibility tracker in hand.
    pub fn on_chain_signal(&self, signal: ChainSignal) {
        let _ = self.tx.send(ResourceManagerUpdate::ChainSignal(signal));
    }

    /// Removes the vqueue from the resource it's blocked on
    pub(super) fn remove_vqueue(&mut self, handle: VQueueHandle, blocked_resource: &ResourceKind) {
        match blocked_resource {
            ResourceKind::Lock { scope, lock_name } => {
                self.locks.remove_from_waiters(handle, scope, lock_name);
            }
            ResourceKind::InvokerConcurrency => {
                self.invoker_concurrency.remove_from_waiters(handle);
            }
            ResourceKind::InvokerThrottling { .. } => {
                self.invoker_throttling.remove_from_waiters(handle);
            }
            ResourceKind::InvokerMemory => {
                self.invoker_memory.remove_from_waiters(handle);
            }
            ResourceKind::DeploymentConcurrency => todo!(),
            ResourceKind::LimitKeyConcurrency {
                scope,
                limit_key,
                blocked_level,
                ..
            } => {
                self.user_limiter
                    .remove_from_waiters(handle, scope, limit_key, *blocked_level);
            }
            ResourceKind::ChainAdmission { scope, root } => {
                self.chain.remove_waiter(
                    handle,
                    &ChainKey {
                        scope: scope.clone(),
                        root: root.clone(),
                    },
                );
            }
            ResourceKind::SlotShare { .. } => {
                self.share_gate.remove(handle);
            }
        }
    }

    /// returns true if queues were woken up
    pub(super) fn release_lock(
        &mut self,
        eligible: &mut EligibilityTracker,
        scope: &Option<Scope>,
        lock_name: &LockName,
    ) {
        trace!("[release_lock] scope: {scope:?}, lock_name: {lock_name}");

        let Some(queues) = self.locks.release_lock(scope, lock_name) else {
            return;
        };

        for queue in queues {
            // notify the scheduler that those queues should be woken up.
            eligible.wake_up_queue(queue);
        }
    }

    /// Reverts will release the lock if the user permit has one
    pub(super) fn revert_permit_builder(
        &mut self,
        eligible: &mut EligibilityTracker,
        builder: PermitBuilder,
    ) {
        let Some(permit) = builder.into_user_permit() else {
            return;
        };

        // Release the lock if we have one held
        if let Some(lock) = permit.lock
            && let Some(queues) = self.locks.release_lock(&lock.scope, &lock.lock_name)
        {
            eligible.wake_up_queues(queues);
        }

        for resource in permit.resources {
            match resource {
                UserPermitKind::LimitKeyConcurrency(scope, limit_key) => {
                    let woken = self.user_limiter.release_concurrency(&scope, &limit_key);
                    eligible.wake_up_queues(woken);
                }
            }
        }

        if let Some(entry_id) = permit.chain_entry {
            let mut woken = Vec::new();
            self.chain
                .release_unstarted(entry_id, tokio::time::Instant::now(), &mut woken);
            eligible.wake_up_queues(woken);
        }
    }

    /// Records that `vqueue` blocks on `resource`, after leaving every other
    /// waiter registration it may still hold from an earlier poll (a queue
    /// woken by one resource can block on another; its old registration would
    /// otherwise count it as waiting there forever).
    fn block_on(
        &mut self,
        vqueue: VQueueHandle,
        chain_key: Option<&ChainKey>,
        resource: ResourceKind,
    ) -> AcquireOutcome {
        if !matches!(resource, ResourceKind::SlotShare { .. }) {
            self.share_gate.remove(vqueue);
        }
        if !matches!(resource, ResourceKind::ChainAdmission { .. })
            && let Some(key) = chain_key
        {
            self.chain.remove_waiter(vqueue, key);
        }
        if !matches!(resource, ResourceKind::InvokerConcurrency) {
            self.invoker_concurrency.remove_from_waiters(vqueue);
        }
        AcquireOutcome::BlockedOn(resource)
    }

    pub(super) fn poll_acquire_permit(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
        meta: &VQueueMeta,
        key: &EntryKey,
        metadata: &EntryMetadata,
        first_run: bool,
        current_permit: &mut PermitBuilder,
    ) -> AcquireOutcome {
        let chain_key = if key.kind() == EntryKind::Invocation {
            self.chain
                .key_of(metadata.chain_root.as_deref(), meta.scope().as_ref())
        } else {
            None
        };
        if !current_permit.has_user_permit() {
            // we need to acquire user permit first

            // When failing short on resources, we register the queue into the first resource
            // we failed to acquire.
            //
            // Note that it's safe to *check* for resources first and then acquire all of them
            // in one go because we are the sole consumer of resources. We achieve this through
            // via the `ProvisionalPermit` type.
            let mut provisional = ProvisionalPermit::default();

            // if the entry holds a lock already, we don't need to acquire a new one.
            if let Some(lock_name) = meta.lock_name()
                && !key.has_lock()
            {
                // needs to acquire a lock
                if !self.locks.is_locked(meta.scope(), lock_name) {
                    provisional.set_lock(meta.scope().clone(), lock_name.clone());
                } else {
                    self.locks.add_to_waiters(vqueue, meta.scope(), lock_name);
                    return self.block_on(vqueue, chain_key.as_ref(), ResourceKind::Lock {
                        scope: meta.scope().clone(),
                        lock_name: lock_name.clone(),
                    });
                }
            }

            // unscoped entries cannot acquire user limits
            if let Some(scope) = meta.scope() {
                let capacity = self
                    .user_limiter
                    .check_concurrency_capacity(scope, meta.limit_key());
                if let Some((blocked_level, blocked_rule)) = capacity.narrowest_blocked() {
                    trace!(
                        %scope,
                        limit_key = %meta.limit_key(),
                        blocked_at = %blocked_level,
                        details = %capacity.display(&self.user_limiter),
                        "User concurrency limit reached",
                    );
                    self.user_limiter.add_to_waiters(
                        vqueue,
                        scope,
                        meta.limit_key(),
                        blocked_level,
                    );
                    return self.block_on(vqueue, chain_key.as_ref(), ResourceKind::LimitKeyConcurrency {
                        scope: scope.clone(),
                        limit_key: meta.limit_key().clone(),
                        blocked_level,
                        blocked_rule,
                    });
                }

                // Stage the permit — counters are incremented in secure()
                provisional.add_permit(UserPermitKind::LimitKeyConcurrency(
                    scope.clone(),
                    meta.limit_key().clone(),
                ));
            }

            if key.kind() == EntryKind::Invocation {
                let now = tokio::time::Instant::now();
                // Weighted slot share, checked before chain admission so a
                // start held back by its share never holds a chain permit.
                // Only new starts (a first run nothing called) are subject
                // to it; in-flight work (children, resumes) is never capped.
                let new_start = first_run && !metadata.has_parent;
                if new_start && self.share_gate.is_enabled() {
                    let service = meta
                        .service_name()
                        .cloned()
                        .unwrap_or_else(super::eligible::unlinked_group);
                    let group = SchedulingGroup::of(meta);
                    let lane = self.invoker_concurrency.lane_of(&group, &service);
                    if !self.share_gate.admit(vqueue, lane) {
                        return self.block_on(
                            vqueue,
                            chain_key.as_ref(),
                            ResourceKind::SlotShare {
                                lane: ReString::new(share_lane_label(&group, &service)),
                            },
                        );
                    }
                }

                // Chain admission (last user check, so a permit is only taken
                // when everything else is available): a root invocation needs
                // a chain permit before its first run (and again after an
                // external wait or a pause). Children and resumes of a running
                // chain are never gated here.
                match self.chain.poll_admit(
                    vqueue,
                    *key.entry_id(),
                    chain_key.clone(),
                    first_run,
                    now,
                ) {
                    Admit::NotGated => {}
                    Admit::Admitted => provisional.set_chain_entry(*key.entry_id()),
                    Admit::Blocked(blocked) => {
                        trace!(chain = %blocked, "Chain admission limit reached");
                        return self.block_on(
                            vqueue,
                            chain_key.as_ref(),
                            ResourceKind::ChainAdmission {
                                scope: blocked.scope,
                                root: blocked.root,
                            },
                        );
                    }
                }
            }

            // All user requirements are satisfied.
            current_permit.set_user_permit(provisional.secure(self));
        }

        // System permit
        match key.kind() {
            EntryKind::Unknown => unreachable!("Cannot acquire system permit for unknown entry"),
            EntryKind::Invocation => {
                // this is invocation. It needs an invoker permit + invoker throttling token
                // Do we have one?
                if !current_permit.has_invoker_permit() {
                    // poll for one or die trying
                    // the service name selects the lane WITHIN the group;
                    // unlinked vqueues pool into the "" lane (they never take
                    // invoker permits for state mutations anyway)
                    let service = meta
                        .service_name()
                        .cloned()
                        .unwrap_or_else(super::eligible::unlinked_group);
                    let group = SchedulingGroup::of(meta);
                    // Claim-time re-check of the weighted slot share: a new
                    // start that passed the early check may have parked at a
                    // full pool and gets its slot here by WRR, so the share
                    // is enforced again. A refused start gives its chain
                    // permit back, so it holds nothing while it waits.
                    let new_start = first_run && !metadata.has_parent;
                    if self.share_gate.is_enabled() {
                        if new_start {
                            let lane = self.invoker_concurrency.lane_of(&group, &service);
                            if !self.share_gate.admit(vqueue, lane) {
                                // give the whole user permit back (lock,
                                // counters, chain permit): the next poll
                                // re-enters the user stage from scratch, so
                                // chain admission is never skipped
                                if let Some(builder) = current_permit.take_user_permit() {
                                    self.deferred_revert.push(builder);
                                    cx.waker().wake_by_ref();
                                }
                                return self.block_on(
                                    vqueue,
                                    chain_key.as_ref(),
                                    ResourceKind::SlotShare {
                                        lane: ReString::new(share_lane_label(&group, &service)),
                                    },
                                );
                            }
                        } else {
                            self.share_gate.remove(vqueue);
                        }
                    }
                    let Some((invoker_permit, slot_lease)) = self.invoker_concurrency.poll_acquire(
                        cx,
                        vqueue,
                        &group,
                        &service,
                        new_start,
                    ) else {
                        return self.block_on(
                            vqueue,
                            chain_key.as_ref(),
                            ResourceKind::InvokerConcurrency,
                        );
                    };
                    current_permit.set_invoker_permit(invoker_permit, slot_lease);
                }

                // If we have the concurrency permit, let's see if we need to wait for throttling
                if !current_permit.has_invoker_throttling_token() {
                    match self.invoker_throttling.poll_acquire(cx, vqueue) {
                        ThrottlingAcquire::Acquired(throttling_token) => {
                            current_permit.set_throttling_permit(throttling_token);
                        }
                        ThrottlingAcquire::Blocked { estimated_retry_at } => {
                            return self.block_on(
                                vqueue,
                                chain_key.as_ref(),
                                ResourceKind::InvokerThrottling { estimated_retry_at },
                            );
                        }
                    }
                }
            }
            EntryKind::StateMutation => {
                // I don't need a system permit here.
            }
        }

        // the chain (if any) has its invoker slot now: it is running
        if let Some(entry_id) = current_permit.chain_entry() {
            self.chain.mark_started(entry_id, tokio::time::Instant::now());
        }
        AcquireOutcome::Acquired(current_permit.take())
    }

    pub(super) fn poll_resources(
        &mut self,
        cx: &mut std::task::Context<'_>,
        eligible: &mut EligibilityTracker,
    ) {
        // check if we have global resource that can move forward
        // drain as many updates as possible
        while let Poll::Ready(Some(update)) = self.rx.poll_recv(cx) {
            match update {
                ResourceManagerUpdate::PermitReleased { kinds } => {
                    for resource in kinds {
                        match resource {
                            UserPermitKind::LimitKeyConcurrency(scope, limit_key) => {
                                let woken =
                                    self.user_limiter.release_concurrency(&scope, &limit_key);
                                eligible.wake_up_queues(woken);
                            }
                        }
                    }
                }
                ResourceManagerUpdate::RulesUpdated(updates) => {
                    let woken = self.user_limiter.apply_rule_updates(updates);
                    eligible.wake_up_queues(woken);
                }
                ResourceManagerUpdate::ChainSignal(signal) => {
                    let woken = self.chain.on_signal(signal, tokio::time::Instant::now());
                    eligible.wake_up_queues(woken);
                }
            }
        }

        while let Poll::Ready(Some(queue)) = self.invoker_concurrency.poll_head(cx) {
            // wake up this vqueue and shift all other waiters to need poll so
            // they can get a chance to be added to the ready ring if they are eligible and
            // still valid.
            tracing::trace!(
                "waking up vqueue {queue:?} because invoker concurrency permit was acquired"
            );
            eligible.wake_up_queue(queue);
        }

        let mut share_woken = Vec::new();
        self.share_gate.poll_wake(cx, &mut share_woken);
        eligible.wake_up_queues(share_woken);

        for builder in std::mem::take(&mut self.deferred_revert) {
            self.revert_permit_builder(eligible, builder);
        }
        let mut chain_woken = Vec::new();
        self.chain
            .poll_wake(cx, tokio::time::Instant::now(), &mut chain_woken);
        eligible.wake_up_queues(chain_woken);

        while let Poll::Ready(Some(queue)) = self.invoker_throttling.poll_head(cx) {
            tracing::trace!(
                "waking up vqueue {queue:?} because invoker throttling token became available"
            );
            eligible.wake_up_queue(queue);
        }
    }

    /// Snapshot of every user-limit counter currently tracked by this partition's
    /// `UserLimiter`. The rows are stamped with the owning partition's key so that
    /// DataFusion can route them into the right scan.
    pub(super) fn scan_user_limit_counters(
        &self,
        partition_key: PartitionKey,
    ) -> Vec<UserLimitCounterEntry> {
        self.user_limiter.scan_counters(partition_key)
    }

    /// Resolve a user-limit rule handle into its pattern string, or `None` if
    /// the rule has been removed since the handle was captured. Used when
    /// lifting internal `ResourceKind` into the public `BlockedResource`.
    pub(super) fn resolve_user_rule(&self, handle: RuleHandle) -> Option<ReString> {
        self.user_limiter
            .resolve_rule(handle)
            .map(|pattern| ReString::new(pattern.to_string()))
    }
}

/// Display label of a share lane: `scope/service`, or `/service` unscoped.
fn share_lane_label(group: &SchedulingGroup, service: &restate_types::ServiceName) -> String {
    match group {
        SchedulingGroup::Scope(scope) => format!("{scope}/{service}"),
        SchedulingGroup::Service(_) => format!("/{service}"),
    }
}
