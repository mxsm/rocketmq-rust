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

use super::*;
use crate::mapped_file::retirement::platform::authorize_namespace_transition;
use crate::mapped_file::retirement::platform::authorize_tombstone_removal;
use crate::mapped_file::retirement::platform::NamespaceAbsenceProof;
use crate::mapped_file::retirement::platform::NamespaceTransition;
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
use crate::mapped_file::retirement::sidecar::IncarnationSnapshotEntry;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::sidecar::RetirementTicketSnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::state::LedgerStateMachine;
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
use crate::mapped_file::retirement::writer::ManagedLedgerWriter;

#[test]
fn replayed_logical_removal_resumes_without_a_live_mapped_file_owner() {
    let recovered = recovered_logical_removal();
    let (registry, mut work) = RetirementRegistry::<TestOwner>::from_recovered_state(&recovered)
        .expect("replay-validated state rebuilds the registry");
    assert_eq!(registry.retained_identity_count(), 1);
    assert_eq!(work.len(), 1);
    let RecoveredRetirementWork::Namespace(logical_removed) = work.pop().expect("one pending ticket is restored")
    else {
        panic!("LogicalRemoved must resume at namespace convergence");
    };

    let authorization = authorize_namespace_transition(logical_removed, NamespaceTransition::DirectUnlink)
        .expect("replayed LogicalRemoved authorizes its exact namespace request");
    let (logical_removed, request) = authorization.into_parts_for_test();
    let proof = NamespaceAbsenceProof::verified_for_test(&request, None);
    let mut writer =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x31; 16], 1, 11, 2, 0, true, 1)
            .expect("replay cursor is valid");
    let namespace_absent = writer
        .append_namespace_absent(logical_removed, proof, 123)
        .expect("replayed stage accepts the next writer-proven append");
    writer
        .append_completed(namespace_absent, 124)
        .expect("the recovered retirement completes");

    assert_eq!(registry.retained_identity_count(), 0);
    assert!(!registry.needs_recovery());
}

#[test]
fn replayed_intent_skips_the_startup_queue_handoff_and_resumes_logical_removal() {
    let recovered = recovered_state(RetirementStage::IntentDurable, None);
    let (registry, mut work) = RetirementRegistry::<TestOwner>::from_recovered_state(&recovered)
        .expect("replay-validated state rebuilds the registry");
    let RecoveredRetirementWork::LogicalRemoval(handoff) = work.pop().expect("one pending ticket is restored") else {
        panic!("IntentDurable must resume at its durable logical-removal append");
    };
    let mut writer = recovered_writer(11);
    let logical_removed = writer
        .append_logical_removed(handoff)
        .expect("startup omission of the retired path replaces a process-local queue CAS");
    complete_direct_retirement(&mut writer, logical_removed);

    assert_eq!(registry.retained_identity_count(), 0);
    assert!(!registry.needs_recovery());
}

#[test]
fn replayed_tombstone_resumes_removal_without_a_live_mapped_file_owner() {
    let tombstone = tombstone_path();
    let recovered = recovered_state(RetirementStage::Tombstoned, Some(tombstone));
    let (registry, mut work) = RetirementRegistry::<TestOwner>::from_recovered_state(&recovered)
        .expect("replay-validated state rebuilds the registry");
    let RecoveredRetirementWork::TombstoneRemoval(tombstoned) = work.pop().expect("one pending ticket is restored")
    else {
        panic!("Tombstoned must resume at exact tombstone removal");
    };
    let authorization =
        authorize_tombstone_removal(tombstoned).expect("replayed Tombstoned authorizes its exact removal request");
    let (tombstoned, request) = authorization.into_parts_for_test();
    let proof = NamespaceAbsenceProof::verified_for_test(&request, None);
    let mut writer = recovered_writer(11);
    let namespace_absent = writer
        .append_namespace_absent_after_tombstone(tombstoned, proof, 123)
        .expect("replayed tombstone advances to durable absence");
    writer
        .append_completed(namespace_absent, 124)
        .expect("replayed tombstone retirement completes");

    assert_eq!(registry.retained_identity_count(), 0);
    assert!(!registry.needs_recovery());
}

#[test]
fn replayed_namespace_absence_resumes_only_the_completion_append() {
    let recovered = recovered_state(RetirementStage::NamespaceAbsent, None);
    let (registry, mut work) = RetirementRegistry::<TestOwner>::from_recovered_state(&recovered)
        .expect("replay-validated state rebuilds the registry");
    let RecoveredRetirementWork::Completion(namespace_absent) = work.pop().expect("one pending ticket is restored")
    else {
        panic!("NamespaceAbsent must resume at Completed");
    };
    let mut writer = recovered_writer(11);
    writer
        .append_completed(namespace_absent, 124)
        .expect("replayed namespace absence completes");

    assert_eq!(registry.retained_identity_count(), 0);
    assert!(!registry.needs_recovery());
}

#[test]
fn completed_retained_rebuilds_identity_reservations_without_retry_work() {
    let recovered = recovered_state(RetirementStage::CompletedRetained, None);
    let (registry, work) = RetirementRegistry::<TestOwner>::from_recovered_state(&recovered)
        .expect("completed-retained identity is reconstructed");

    assert!(work.is_empty());
    assert_eq!(registry.retained_identity_count(), 1);
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(!registry.needs_recovery());
}

fn recovered_logical_removal() -> crate::mapped_file::retirement::state::RecoveredLedgerState {
    recovered_state(RetirementStage::LogicalRemoved, None)
}

fn recovered_state(
    stage: RetirementStage,
    tombstone_path: Option<StoreRelativePath>,
) -> crate::mapped_file::retirement::state::RecoveredLedgerState {
    let incarnation = incarnation(1);
    let canonical_path = canonical_path(0);
    let create_path = StoreRelativePath::new(&format!(
        "commitlog/.create.i0000000000000001.s00000000000000000000.n{}",
        "22".repeat(16)
    ))
    .expect("test create path is canonical");
    let incarnation_entry = IncarnationSnapshotEntry {
        incarnation,
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: FILE_LENGTH,
        create_nonce: OTHER_NONCE,
        physical_key: Some(physical_key(11)),
        canonical_path: canonical_path.clone(),
        create_file_path: create_path,
    };
    let ticket_entry = RetirementTicketSnapshotEntry {
        ticket_id: crate::mapped_file::retirement::identity::TicketId::new(1).expect("test ticket is nonzero"),
        incarnation,
        stage,
        superseded_path_observed: false,
        quarantined: false,
        reason: RetirementReason::TtlExpired,
        stage_sequence: 10,
        mapping_generation: 3,
        segment_offset: 0,
        expected_file_length: FILE_LENGTH,
        retirement_nonce: NONCE,
        target_key: physical_key(11),
        canonical_path,
        tombstone_path,
    };
    LedgerStateMachine::from_snapshot(LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 10,
        create_high_water: 1,
        ticket_high_water: 1,
        entries: vec![
            SnapshotEntry::Incarnation(incarnation_entry),
            SnapshotEntry::RetirementTicket(ticket_entry),
        ],
    })
    .expect("test snapshot is valid")
    .finish(10, 1, 1)
    .expect("test replay state is valid")
}

fn recovered_writer(next_sequence: u64) -> ManagedLedgerWriter<ModelLedgerIo> {
    ManagedLedgerWriter::for_test(
        ModelLedgerIo::empty(),
        store_uuid(),
        [0x31; 16],
        1,
        next_sequence,
        2,
        0,
        true,
        1,
    )
    .expect("replay cursor is valid")
}

fn complete_direct_retirement(
    writer: &mut ManagedLedgerWriter<ModelLedgerIo>,
    logical_removed: LogicalRemovedCapability<TestOwner>,
) {
    let authorization = authorize_namespace_transition(logical_removed, NamespaceTransition::DirectUnlink)
        .expect("LogicalRemoved authorizes its exact namespace request");
    let (logical_removed, request) = authorization.into_parts_for_test();
    let proof = NamespaceAbsenceProof::verified_for_test(&request, None);
    let namespace_absent = writer
        .append_namespace_absent(logical_removed, proof, 123)
        .expect("verified namespace absence is durable");
    writer
        .append_completed(namespace_absent, 124)
        .expect("retirement completes");
}

fn tombstone_path() -> StoreRelativePath {
    canonical_path(0)
        .tombstone_path(
            crate::mapped_file::retirement::identity::TicketId::new(1).expect("test ticket is nonzero"),
            incarnation(1),
            0,
            3,
            &NONCE,
        )
        .expect("test tombstone path is canonical")
}
