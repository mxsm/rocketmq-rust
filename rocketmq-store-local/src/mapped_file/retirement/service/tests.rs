// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use super::*;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::platform::authorize_namespace_transition;
use crate::mapped_file::retirement::platform::AuthorizedNamespaceTransitionResult;
use crate::mapped_file::retirement::platform::NamespaceAbsenceProof;
use crate::mapped_file::retirement::platform::NamespaceTransition;
use crate::mapped_file::retirement::platform::NamespaceTransitionOutcome;
use crate::mapped_file::retirement::registry::reaper::commit_logical_namespace_outcome;
use crate::mapped_file::retirement::registry::reaper::LogicalNamespaceProgress;
use crate::mapped_file::retirement::registry::reaper::NamespacePending;
use crate::mapped_file::retirement::registry::reaper::ReaperDriveFailure;
use crate::mapped_file::retirement::registry::reaper::TombstoneNamespaceProgress;
use crate::mapped_file::retirement::registry::LogicalRemovedCapability;
use crate::mapped_file::retirement::registry::ManagedMappedFileQueueGeneration;
use crate::mapped_file::retirement::registry::ManagedQueueMember;
use crate::mapped_file::retirement::registry::RecoveredRetirementWork;
use crate::mapped_file::retirement::registry::RetirementOperation;
use crate::mapped_file::retirement::registry::RetirementRegistry;
use crate::mapped_file::retirement::registry::TombstonedCapability;
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
use crate::mapped_file::retirement::writer::ManagedLedgerWriter;

const FILE_LENGTH: u64 = 1_024;
const BOOTSTRAP_ID: [u8; 16] = [0x81; 16];

#[derive(Debug)]
struct TestOwner;

fn store_uuid() -> StoreUuid {
    StoreUuid::new([0x82; 16]).expect("test Store UUID is nonzero")
}

fn incarnation() -> FileIncarnationId {
    FileIncarnationId::new(store_uuid(), 1).expect("test incarnation is nonzero")
}

fn physical_key() -> PhysicalFileKey {
    PhysicalFileKey::unix(7, 11)
}

fn canonical_path() -> StoreRelativePath {
    StoreRelativePath::new("commitlog/00000000000000000000").expect("test path is canonical")
}

fn recovered_namespace_fixture() -> (
    RetirementRegistry<TestOwner>,
    ManagedLedgerWriter<ModelLedgerIo>,
    RecoveredRetirementWork<TestOwner>,
) {
    let registry = RetirementRegistry::new_for_test(store_uuid(), 0);
    let owner = Arc::new(TestOwner);
    let queue = ManagedMappedFileQueueGeneration::from_reconciled_members(vec![ManagedQueueMember::new(
        Arc::clone(&owner),
        incarnation(),
        physical_key(),
        canonical_path(),
        0,
        FILE_LENGTH,
        1,
    )
    .expect("managed member is valid")])
    .expect("managed queue generation is valid");
    queue
        .register_reconciled_members(&registry)
        .expect("published identity is registered");
    let operation = RetirementOperation::new(
        incarnation(),
        RetirementReason::TtlExpired,
        1,
        0,
        FILE_LENGTH,
        [0x83; 16],
        physical_key(),
        canonical_path(),
    )
    .expect("retirement operation is valid");
    let reservation = registry
        .prepare_retirement(operation, &owner, &queue.queue_identity())
        .expect("retirement intent is reserved");
    let binding = reservation.binding().clone();
    let mut writer =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), BOOTSTRAP_ID, 4, 1, 1, 0, true, 1)
            .expect("managed writer cursor is valid");
    let token = writer
        .append_retirement_intent(reservation.begin_append())
        .expect("RetirementIntent is durable");
    let handoff = queue
        .handoff_retirement(&registry, token, &binding)
        .expect("queue handoff succeeds exactly once");
    let logical_removed = writer
        .append_logical_removed(handoff)
        .expect("LogicalRemoved is durable");
    (registry, writer, RecoveredRetirementWork::Namespace(logical_removed))
}

fn active_fixture() -> (
    RetirementRegistry<TestOwner>,
    ManagedMappedFileQueueGeneration<TestOwner>,
    Arc<TestOwner>,
    ManagedLedgerWriter<ModelLedgerIo>,
) {
    let registry = RetirementRegistry::new_for_test(store_uuid(), 0);
    let owner = Arc::new(TestOwner);
    let queue = ManagedMappedFileQueueGeneration::from_reconciled_members(vec![ManagedQueueMember::new(
        Arc::clone(&owner),
        incarnation(),
        physical_key(),
        canonical_path(),
        0,
        FILE_LENGTH,
        1,
    )
    .expect("managed member is valid")])
    .expect("managed queue generation is valid");
    queue
        .register_reconciled_members(&registry)
        .expect("published identity is registered");
    let writer = ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), BOOTSTRAP_ID, 4, 1, 1, 0, true, 1)
        .expect("managed writer cursor is valid");
    (registry, queue, owner, writer)
}

struct ImmediateAbsence;

impl NamespaceDriver<ModelLedgerIo, TestOwner> for ImmediateAbsence {
    fn drive_logical(
        &mut self,
        writer: &mut ManagedLedgerWriter<ModelLedgerIo>,
        capability: LogicalRemovedCapability<TestOwner>,
        observation_time_ns: u64,
    ) -> Result<LogicalNamespaceProgress<TestOwner>, ReaperDriveFailure> {
        let authorization = authorize_namespace_transition(capability, NamespaceTransition::DirectUnlink)?;
        let (capability, request) = authorization.into_parts_for_test();
        let proof = NamespaceAbsenceProof::verified_for_test(&request, None);
        commit_logical_namespace_outcome(
            writer,
            AuthorizedNamespaceTransitionResult::for_test(
                capability,
                NamespaceTransitionOutcome::NamespaceAbsentVerified(proof),
            ),
            observation_time_ns,
        )
        .map_err(Into::into)
    }

    fn drive_tombstone(
        &mut self,
        _writer: &mut ManagedLedgerWriter<ModelLedgerIo>,
        _capability: TombstonedCapability<TestOwner>,
        _observation_time_ns: u64,
    ) -> Result<TombstoneNamespaceProgress<TestOwner>, ReaperDriveFailure> {
        unreachable!("the direct-unlink fixture never creates a tombstone")
    }
}

#[test]
fn recovered_namespace_work_reaches_completed_in_one_bounded_batch() {
    let (registry, writer, work) = recovered_namespace_fixture();
    let started = Instant::now();
    let mut core = ManagedRetirementCore::new(registry, writer, ImmediateAbsence, vec![work], started);

    let report = core.drive_batch_at(2, started, 123);

    assert_eq!(report.attempted(), 2);
    assert_eq!(report.completed(), 1);
    assert_eq!(report.pending_tickets(), 0);
    assert_eq!(report.tombstone_backlog(), 0);
    assert!(!report.recovery_required());
    assert_eq!(core.registry().retained_identity_count(), 0);
}

#[test]
fn new_retirement_is_durable_before_the_single_queue_handoff() {
    let (registry, queue, owner, writer) = active_fixture();
    let started = Instant::now();
    let mut core = ManagedRetirementCore::new(registry, writer, ImmediateAbsence, Vec::new(), started);

    let submission = core
        .submit_at(&queue, &owner, RetirementReason::TtlExpired, [0x84; 16], started)
        .expect("durable intent and exact queue handoff succeed");

    assert_eq!(submission.ticket_id(), 1);
    assert_eq!(submission.stage(), ManagedRetirementStage::Namespace);
    assert!(queue.snapshot().is_empty());
    assert_eq!(core.registry().retained_identity_count(), 1);

    let report = core.drive_batch_at(2, started, 223);
    assert_eq!(report.completed(), 1);
    assert_eq!(report.pending_tickets(), 0);
    assert!(!report.recovery_required());
}

#[test]
fn store_destroy_submits_every_active_member_through_the_existing_retirement_pipeline() {
    let registry = RetirementRegistry::new_for_test(store_uuid(), 0);
    let first_owner = Arc::new(TestOwner);
    let second_owner = Arc::new(TestOwner);
    let second_incarnation = FileIncarnationId::new(store_uuid(), 2).expect("second incarnation is nonzero");
    let second_path = StoreRelativePath::new("commitlog/00000000000000001024").expect("second path is canonical");
    let queue = ManagedMappedFileQueueGeneration::from_reconciled_members(vec![
        ManagedQueueMember::new(
            Arc::clone(&first_owner),
            incarnation(),
            physical_key(),
            canonical_path(),
            0,
            FILE_LENGTH,
            1,
        )
        .expect("first managed member is valid"),
        ManagedQueueMember::new(
            Arc::clone(&second_owner),
            second_incarnation,
            PhysicalFileKey::unix(7, 12),
            second_path,
            FILE_LENGTH,
            FILE_LENGTH,
            2,
        )
        .expect("second managed member is valid"),
    ])
    .expect("managed queue generation is valid");
    queue
        .register_reconciled_members(&registry)
        .expect("published identities are registered");
    let writer = ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), BOOTSTRAP_ID, 4, 1, 1, 0, true, 1)
        .expect("managed writer cursor is valid");
    let started = Instant::now();
    let mut core = ManagedRetirementCore::new(registry, writer, ImmediateAbsence, Vec::new(), started);
    let mut nonce = 0x90u8;

    let submitted = core
        .submit_store_destroy_at(
            std::slice::from_ref(&queue),
            || {
                let current = [nonce; 16];
                nonce = nonce.wrapping_add(1);
                current
            },
            started,
        )
        .expect("Store destroy submits every active member");

    assert_eq!(submitted, 2);
    assert!(queue.snapshot().is_empty());
    let report = core.drive_batch_at(4, started, 323);
    assert_eq!(report.completed(), 2);
    assert_eq!(report.pending_tickets(), 0);
    assert!(!report.recovery_required());
}

struct RetryableNamespace;

impl NamespaceDriver<ModelLedgerIo, TestOwner> for RetryableNamespace {
    fn drive_logical(
        &mut self,
        _writer: &mut ManagedLedgerWriter<ModelLedgerIo>,
        capability: LogicalRemovedCapability<TestOwner>,
        _observation_time_ns: u64,
    ) -> Result<LogicalNamespaceProgress<TestOwner>, ReaperDriveFailure> {
        Ok(LogicalNamespaceProgress::Pending {
            capability,
            status: NamespacePending::Unsupported {
                platform: "test",
                reason: "deterministic retry",
            },
        })
    }

    fn drive_tombstone(
        &mut self,
        _writer: &mut ManagedLedgerWriter<ModelLedgerIo>,
        _capability: TombstonedCapability<TestOwner>,
        _observation_time_ns: u64,
    ) -> Result<TombstoneNamespaceProgress<TestOwner>, ReaperDriveFailure> {
        unreachable!("the retryable fixture never creates a tombstone")
    }
}

#[test]
fn retryable_namespace_work_is_owned_and_backed_off() {
    let (registry, writer, work) = recovered_namespace_fixture();
    let started = Instant::now();
    let mut core = ManagedRetirementCore::new(registry, writer, RetryableNamespace, vec![work], started);

    let first = core.drive_batch_at(8, started, 123);
    assert_eq!(first.attempted(), 1);
    assert_eq!(first.pending_tickets(), 1);
    assert_eq!(first.last_failure_stage(), Some(ManagedRetirementStage::Namespace));
    assert_eq!(first.oldest_pending_age(), Duration::ZERO);

    let before_backoff = core.drive_batch_at(8, started + Duration::from_millis(1), 124);
    assert_eq!(before_backoff.attempted(), 0);
    assert_eq!(before_backoff.pending_tickets(), 1);
    assert!(!core.registry().needs_recovery());
}

#[test]
fn store_destroy_admission_requires_completed_shutdown() {
    let mut running = RuntimeAdmission::Running;
    assert!(!running.enter_store_destroy());
    assert_eq!(running, RuntimeAdmission::Running);

    let mut shutdown = RuntimeAdmission::Shutdown;
    assert!(shutdown.enter_store_destroy());
    assert_eq!(shutdown, RuntimeAdmission::StoreDestroy);
    assert!(
        shutdown.enter_store_destroy(),
        "Store-destroy retries remain idempotent"
    );
}
