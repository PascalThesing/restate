// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Chain admission: the durable `EntryMetadata::chain_root` marker written by
//! `vqueue_enqueue` (`state_machine/mod.rs`) and the leader-only
//! `Action::ChainSignal` lifecycle signals emitted for chain roots
//! (`emit_chain_end` in `state_machine/mod.rs`, and the suspend/pause
//! lifecycle commands in `lifecycle/suspend.rs` and `lifecycle/paused.rs`).

use std::time::{Duration, SystemTime};

use googletest::prelude::*;

use crate::partition::state_machine::Action;
use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
use crate::partition::types::InvokerEffectKind;

use restate_limiter::LimitKey;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, InvocationStatusDiscriminants, ReadInvocationStatusTable,
};
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_storage_api::vqueue_table::scheduler::{
    RunAction, SchedulerAction, SchedulerDecisionsCommand,
};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_storage_api::vqueue_table::{EntryKey, EntryStatusHeader, ReadVQueueTable};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, InvocationTermination, ServiceInvocation, Source, TerminationFlavor,
};
use restate_types::journal_events::{Event, PausedEvent};
use restate_types::journal_v2::{
    CallCommand, CallRequest, CommandType, CompletionId, NotificationId, SleepCommand,
};
use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, VQueueId};
use restate_util_string::ReString;
use restate_vqueues::VQueue;
use restate_wal_protocol::v2::{Command, commands};
use restate_worker_api::invoker::Effect;
use restate_worker_api::resources::{ChainSignal, ChainSignalKind};

/// vqueues + chain-root admission both on: the feature combination under
/// test for every case except [`chain_root_marker_requires_the_feature`].
fn chain_features() -> PersistedFeatures {
    PersistedFeatures::from_iter([
        PartitionFeatureChange::EnableVqueues,
        PartitionFeatureChange::EnableChainRootQueues,
    ])
}

/// vqueues alone (no chain-root admission).
fn vqueues_only_features() -> PersistedFeatures {
    PersistedFeatures::from_iter([PartitionFeatureChange::EnableVqueues])
}

/// Starts a fresh root invocation (`Source::Ingress`) without asserting on
/// the resulting actions.
///
/// `fixtures::mock_start_invocation_with_invocation_target` can't be reused
/// here: it asserts `Action::Invoke` is emitted, but with vqueues enabled the
/// invocation is only enqueued (and, if it's a chain root, marked) — it lands
/// in its vqueue's inbox as `InvocationStatus::Inboxed` and is *not*
/// immediately invoked. See `admit_to_run` for the transition to `Invoked`.
async fn start_root(test_env: &mut TestEnv, target: InvocationTarget) -> InvocationId {
    let invocation_id = InvocationId::mock_generate(&target);
    let _ = test_env
        .apply(commands::InvokeCommand::test_envelope(
            ServiceInvocation::initialize(
                invocation_id,
                target,
                Source::Ingress(PartitionProcessorRpcRequestId::new()),
            ),
        ))
        .await;
    invocation_id
}

/// Owned snapshot of the parts of a vqueue entry status header this test
/// suite cares about. Extracted eagerly (rather than handing back
/// `impl EntryStatusHeader` itself) because the trait's associated `Future`
/// captures the borrow of the short-lived transaction it's read through, and
/// the entry's own hidden `impl EntryStatusHeader + 'static` return type gets
/// entangled with that borrow across `.await` in ways the borrow checker
/// won't let escape this function.
struct VqueueSnapshot {
    chain_root: Option<ReString>,
    /// Another invocation called this one: in-flight work for the invoker,
    /// never a new start.
    has_parent: bool,
    vqueue_id: VQueueId,
    entry_key: EntryKey,
}

/// Reads back the vqueue entry status header for `invocation_id`.
///
/// `ReadVQueueTable` is only implemented for `PartitionStoreTransaction`, not
/// `PartitionStore` directly, so this opens a (read-only, uncommitted, simply
/// dropped) transaction rather than using `test_env.storage()` as-is.
async fn vqueue_snapshot(test_env: &mut TestEnv, invocation_id: InvocationId) -> VqueueSnapshot {
    let txn = test_env.storage().transaction();
    let header = txn
        .get_vqueue_entry_status(invocation_id.partition_key(), &EntryId::from(invocation_id))
        .await
        .unwrap()
        .expect("vqueue entry must exist for this invocation");
    VqueueSnapshot {
        chain_root: header.metadata().chain_root.clone(),
        has_parent: header.metadata().has_parent,
        vqueue_id: header.vqueue_id().clone(),
        entry_key: *header.entry_key(),
    }
}

/// Simulates the scheduler admitting `invocation_id`'s vqueue entry to run:
/// the only way (in this state-machine-only test harness, which has no real
/// scheduler component) to move an entry from `Inboxed` to `Invoked`. Every
/// invoker effect (journal entries, suspend, pause, end) is silently dropped
/// unless the invocation is `Invoked` (see `on_invoker_effect`'s
/// `is_status_invoked` guard), so tests that drive those effects must call
/// this first.
async fn admit_to_run(test_env: &mut TestEnv, invocation_id: InvocationId) {
    let VqueueSnapshot {
        vqueue_id: qid,
        entry_key: key,
        ..
    } = vqueue_snapshot(test_env, invocation_id).await;

    let actions = test_env
        .apply(commands::SchedulerDecisionsCommand::test_envelope(
            SchedulerDecisionsCommand {
                qids: vec![(
                    qid,
                    vec![SchedulerAction::Run(RunAction {
                        key,
                        wait_stats: WaitStats::default(),
                    })],
                )],
            },
        ))
        .await;

    // Admission must move the entry from Inbox to Running.
    assert_that!(actions, contains(pat!(Action::VQInvoke { .. })));
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
    );
}

/// Reduces the `ChainSignal` actions found in `actions` to
/// `(entry_id, kind-label)` pairs, in emission order.
///
/// `ChainSignal`/`ChainSignalKind` (`restate_worker_api::resources`) derive
/// only `Debug` and `Clone` (no `PartialEq`), so matching them with
/// `eq(..)`/eq-based googletest matchers doesn't compile. `EntryId` does
/// implement `PartialEq`, so pairing it with a plain string label for the
/// `kind` lets every test assert exact count, entry id, and kind with one
/// `assert_eq!`.
fn chain_signal_log(actions: &[Action]) -> Vec<(EntryId, String)> {
    actions
        .iter()
        .filter_map(|action| match action {
            Action::ChainSignal(ChainSignal { entry_id, kind }) => Some((
                *entry_id,
                match kind {
                    ChainSignalKind::Pause => "pause".to_string(),
                    ChainSignalKind::End { completed } => format!("end:{completed}"),
                },
            )),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 1 & 2: the durable `chain_root` marker and the vqueue id it drives.
// ---------------------------------------------------------------------------

/// An ingress-invoked, non-exclusive root is marked with its service name on
/// its vqueue entry and lands in the dedicated root vqueue (distinct from the
/// plain per-service vqueue). A child it later calls is not a root: `Source`
/// is `Service(..)`, so it gets no marker and uses the plain vqueue id.
#[restate_core::test]
async fn root_gets_chain_root_marker_and_dedicated_vqueue_child_does_not() {
    let mut test_env = TestEnv::create_with_features(chain_features()).await;

    let root_target = InvocationTarget::mock_service();
    let root_id = start_root(&mut test_env, root_target.clone()).await;
    let partition_key = root_id.partition_key();

    let root_snapshot = vqueue_snapshot(&mut test_env, root_id).await;
    assert_eq!(
        root_snapshot.chain_root,
        Some(ReString::from(root_target.service_name().to_string())),
        "an ingress-invoked non-exclusive root must be marked with its service name"
    );
    assert!(
        !root_snapshot.has_parent,
        "nothing called the root: it is a new start for the invoker"
    );

    let root_qid = VQueue::infer_root_vqueue_id_from_invocation(
        partition_key,
        &root_target,
        &LimitKey::None,
    );
    let plain_qid =
        VQueue::infer_vqueue_id_from_invocation(partition_key, &root_target, &LimitKey::None);
    assert_eq!(
        root_snapshot.vqueue_id,
        root_qid,
        "a chain root of a non-exclusive target uses the dedicated root vqueue id"
    );
    assert_ne!(
        root_snapshot.vqueue_id,
        plain_qid,
        "the root vqueue id must differ from the plain (non-root) vqueue id for the same target"
    );

    // Admit the root and have it call a child.
    admit_to_run(&mut test_env, root_id).await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, root_id).await;

    let callee_target = InvocationTarget::mock_service();
    let callee_id = InvocationId::mock_generate(&callee_target);
    let call_command = CallCommand {
        request: CallRequest::mock(callee_id, callee_target.clone()),
        invocation_id_completion_id: 1,
        result_completion_id: 2,
        name: Default::default(),
    };
    let actions = test_env
        .apply(fixtures::invoker_entry_effect(root_id, call_command))
        .await;

    let child = actions
        .iter()
        .find_map(|action| match action {
            Action::NewOutboxMessage {
                message: restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(si),
                ..
            } => Some((**si).clone()),
            _ => None,
        })
        .expect("the Call command must produce a child ServiceInvocation on the outbox");

    let child_id = child.invocation_id;
    let _ = test_env
        .apply(commands::InvokeCommand::test_envelope(child))
        .await;

    // A call child (Source::Service) is never a chain root, but it has a
    // parent: in-flight work of a running chain.
    let child_snapshot = vqueue_snapshot(&mut test_env, child_id).await;
    assert_that!(child_snapshot.chain_root, none());
    assert!(child_snapshot.has_parent, "a call child carries the parent marker");
    assert_eq!(
        child_snapshot.vqueue_id,
        VQueue::infer_vqueue_id_from_invocation(
            child_id.partition_key(),
            &callee_target,
            &LimitKey::None
        ),
        "a non-root invocation uses the plain vqueue id"
    );

    test_env.shutdown().await;
}

/// Without `chain_root_queues`, an otherwise-root invocation gets no marker
/// and uses the plain vqueue id, even though `EnableVqueues` alone is enough
/// to route it through `vqueue_enqueue`.
#[restate_core::test]
async fn chain_root_marker_requires_the_feature() {
    let mut test_env = TestEnv::create_with_features(vqueues_only_features()).await;

    let root_target = InvocationTarget::mock_service();
    let root_id = start_root(&mut test_env, root_target.clone()).await;
    let partition_key = root_id.partition_key();

    let snapshot = vqueue_snapshot(&mut test_env, root_id).await;
    assert_that!(snapshot.chain_root, none());
    assert_eq!(
        snapshot.vqueue_id,
        VQueue::infer_vqueue_id_from_invocation(partition_key, &root_target, &LimitKey::None),
        "without chain_root_queues, even an otherwise-root invocation uses the plain vqueue id"
    );

    test_env.shutdown().await;
}

// ---------------------------------------------------------------------------
// 3 & 4: Pause on suspend, gated on whether a call is awaited.
// ---------------------------------------------------------------------------

/// A root suspending on an external future (a sleep, here) that is not
/// backed by a Call/AttachInvocation/GetInvocationOutput command releases its
/// chain permit: exactly one `ChainSignal::Pause` for its entry id.
#[restate_core::test]
async fn root_suspending_on_external_future_emits_pause() {
    let mut test_env = TestEnv::create_with_features(chain_features()).await;

    let root_id = start_root(&mut test_env, InvocationTarget::mock_service()).await;
    admit_to_run(&mut test_env, root_id).await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, root_id).await;

    let completion_id: CompletionId = 1;
    let wake_up_time: MillisSinceEpoch = (SystemTime::now() + Duration::from_secs(60)).into();
    let sleep_command = SleepCommand {
        wake_up_time,
        name: Default::default(),
        completion_id,
    };
    let _ = test_env
        .apply(fixtures::invoker_entry_effect(root_id, sleep_command))
        .await;

    let actions = test_env
        .apply(fixtures::invoker_suspended(
            root_id,
            NotificationId::for_completion(completion_id),
        ))
        .await;

    assert_eq!(
        chain_signal_log(&actions),
        vec![(EntryId::from(root_id), "pause".to_string())],
        "a root suspending on an external future (sleep) must release its chain permit exactly once"
    );
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&root_id)
            .await
            .unwrap(),
        matchers::storage::is_variant(InvocationStatusDiscriminants::Suspended)
    );

    test_env.shutdown().await;
}

/// A root suspending while awaiting the result of its own one-way/regular
/// call does *not* release its chain permit: it is still consuming its
/// budget downstream. No `ChainSignal` at all.
#[restate_core::test]
async fn root_suspending_on_pending_call_result_emits_no_pause() {
    let mut test_env = TestEnv::create_with_features(chain_features()).await;

    let root_id = start_root(&mut test_env, InvocationTarget::mock_service()).await;
    admit_to_run(&mut test_env, root_id).await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, root_id).await;

    let callee_target = InvocationTarget::mock_service();
    let callee_id = InvocationId::mock_generate(&callee_target);
    let result_completion_id: CompletionId = 2;
    let call_command = CallCommand {
        request: CallRequest::mock(callee_id, callee_target),
        invocation_id_completion_id: 1,
        result_completion_id,
        name: Default::default(),
    };
    let _ = test_env
        .apply(fixtures::invoker_entry_effect(root_id, call_command))
        .await;

    // Sanity check so a passing "no Pause" assertion below isn't vacuous: the
    // completion id we suspend on really is backed by the Call command.
    let (_, command) = test_env
        .storage()
        .get_command_by_completion_id(root_id, result_completion_id)
        .await
        .unwrap()
        .expect("the result completion id must resolve to the Call command");
    assert_eq!(command.command_type(), CommandType::Call);

    let actions = test_env
        .apply(fixtures::invoker_suspended(
            root_id,
            NotificationId::for_completion(result_completion_id),
        ))
        .await;

    // A root suspending while awaiting its own call's result must not
    // release its chain permit.
    assert_that!(chain_signal_log(&actions), empty());
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&root_id)
            .await
            .unwrap(),
        matchers::storage::is_variant(InvocationStatusDiscriminants::Suspended)
    );

    test_env.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5: End on completion / kill.
// ---------------------------------------------------------------------------

/// A root that reaches a successful terminal state emits
/// `ChainSignal::End { completed: true }`.
#[restate_core::test]
async fn root_completing_emits_chain_end_completed() {
    let mut test_env = TestEnv::create_with_features(chain_features()).await;

    let root_id = start_root(&mut test_env, InvocationTarget::mock_service()).await;
    admit_to_run(&mut test_env, root_id).await;

    let actions = test_env.apply(fixtures::invoker_end_effect(root_id)).await;

    assert_eq!(
        chain_signal_log(&actions),
        vec![(EntryId::from(root_id), "end:true".to_string())],
        "a root ending successfully must report a completed chain"
    );
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&root_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );

    test_env.shutdown().await;
}

/// A killed root emits `ChainSignal::End { completed: false }`: `end_status`
/// is forced to `Killed` regardless of how it would otherwise have been
/// derived (mod.rs's `end_status = match flavor { Some(Kill) => Killed, .. }`
/// override), so the chain must not be sampled as a completed latency.
#[restate_core::test]
async fn root_killed_emits_chain_end_not_completed() {
    let mut test_env = TestEnv::create_with_features(chain_features()).await;

    let root_id = start_root(&mut test_env, InvocationTarget::mock_service()).await;
    admit_to_run(&mut test_env, root_id).await;

    let actions = test_env
        .apply(commands::TerminateInvocationCommand::test_envelope(
            InvocationTermination {
                invocation_id: root_id,
                flavor: TerminationFlavor::Kill,
                response_sink: None,
            },
        ))
        .await;

    assert_eq!(
        chain_signal_log(&actions),
        vec![(EntryId::from(root_id), "end:false".to_string())],
        "a killed root must report an incomplete chain (no latency sample)"
    );
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&root_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );

    test_env.shutdown().await;
}

// ---------------------------------------------------------------------------
// 6: Pause on the (invoker-driven) paused lifecycle.
// ---------------------------------------------------------------------------

/// A root that the invoker pauses (transient-error backoff) releases its
/// chain permit: exactly one `ChainSignal::Pause`.
#[restate_core::test]
async fn paused_root_emits_pause() {
    let mut test_env = TestEnv::create_with_features(chain_features()).await;

    let root_id = start_root(&mut test_env, InvocationTarget::mock_service()).await;
    admit_to_run(&mut test_env, root_id).await;

    let actions = test_env
        .apply(commands::InvokerEffectCommand::test_envelope(Effect {
            invocation_id: root_id,
            kind: InvokerEffectKind::Paused {
                paused_event: Event::from(PausedEvent { last_failure: None }).into(),
            },
        }))
        .await;

    assert_eq!(
        chain_signal_log(&actions),
        vec![(EntryId::from(root_id), "pause".to_string())],
        "a paused root must release its chain permit exactly once"
    );
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&root_id)
            .await
            .unwrap(),
        matchers::storage::is_variant(InvocationStatusDiscriminants::Paused)
    );

    test_env.shutdown().await;
}
