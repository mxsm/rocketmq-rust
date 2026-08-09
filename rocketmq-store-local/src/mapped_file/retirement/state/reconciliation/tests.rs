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
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
use crate::mapped_file::retirement::sidecar::IncarnationSnapshotEntry;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::sidecar::RetirementTicketSnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::state::LedgerStateMachine;
use crate::mapped_file::retirement::state::NeedsReconciliation;

#[test]
fn exact_published_incarnation_is_the_only_active_publication_candidate() {
    let incarnation = published_incarnation(7, 0);
    let recovered = recovered(vec![SnapshotEntry::Incarnation(incarnation.clone())]);
    let inventory = inventory([file(
        incarnation.canonical_path.clone(),
        incarnation.physical_key.expect("published key"),
        incarnation.expected_file_length,
    )]);

    let ReconciliationDisposition::Ready(ready) = reconcile(recovered, inventory).expect("exact inventory reconciles")
    else {
        panic!("exact published inventory must be ready");
    };

    let binding = ready
        .active_incarnation(&incarnation.canonical_path)
        .expect("published incarnation is indexed");
    assert_eq!(binding.incarnation(), incarnation.incarnation);
    assert_eq!(binding.physical_key(), incarnation.physical_key.expect("published key"));
    assert_eq!(ready.active_count(), 1);
    assert_eq!(ready.retiring_count(), 0);
    assert_eq!(
        ready.active.keys().map(StoreRelativePath::as_str).collect::<Vec<_>>(),
        vec!["commitlog/00000000000000000000"]
    );
}

#[test]
fn active_segment_claim_preflights_every_binding_before_consuming_handles() {
    let first = published_incarnation(7, 0);
    let mut second = published_incarnation(8, 1_024);
    second.physical_key = Some(PhysicalFileKey::unix(7, 10));
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(first.clone()),
        SnapshotEntry::Incarnation(second.clone()),
    ]);
    let inventory = inventory([
        file(
            first.canonical_path.clone(),
            first.physical_key.expect("published key"),
            first.expected_file_length,
        ),
        file(
            second.canonical_path.clone(),
            second.physical_key.expect("published key"),
            second.expected_file_length,
        ),
    ]);
    let ReconciliationDisposition::Ready(mut ready) = reconcile(recovered, inventory).expect("exact inventory") else {
        panic!("exact published inventory must be ready");
    };

    ready.retained_files.insert(
        first.canonical_path.clone(),
        tempfile::tempfile().expect("first retained handle"),
    );
    assert!(matches!(
        ready.take_active_segments_in_directory("commitlog", 1_024),
        Err(ManagedSegmentClaimError::MissingRetainedHandle)
    ));
    assert_eq!(ready.active_count(), 2);
    assert_eq!(ready.retained_files.len(), 1);

    ready.retained_files.insert(
        second.canonical_path.clone(),
        tempfile::tempfile().expect("second retained handle"),
    );
    assert!(matches!(
        ready.take_active_segments_in_directory("commitlog", 2_048),
        Err(ManagedSegmentClaimError::ConfiguredLengthMismatch {
            expected: 1_024,
            configured: 2_048,
        })
    ));
    assert_eq!(ready.active_count(), 2);
    assert_eq!(ready.retained_files.len(), 2);

    let claimed = ready
        .take_active_segments_in_directory("commitlog", 1_024)
        .expect("the complete preflight permits one atomic claim");
    assert_eq!(
        claimed
            .iter()
            .map(ReconciledSegmentFile::segment_offset)
            .collect::<Vec<_>>(),
        vec![0, 1_024]
    );
    assert_eq!(ready.active_count(), 0);
    assert!(ready.retained_files.is_empty());
}

#[test]
fn intent_filters_the_canonical_path_and_retains_reaper_backlog() {
    let incarnation = published_incarnation(7, 0);
    let ticket = retirement_ticket(1, &incarnation, RetirementStage::IntentDurable, false, None);
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(incarnation.clone()),
        SnapshotEntry::RetirementTicket(ticket.clone()),
    ]);
    let inventory = inventory([file(
        incarnation.canonical_path.clone(),
        incarnation.physical_key.expect("published key"),
        incarnation.expected_file_length,
    )]);

    let ReconciliationDisposition::Ready(ready) = reconcile(recovered, inventory).expect("durable intent reconciles")
    else {
        panic!("intent with its canonical target must be reaper-ready");
    };

    assert!(ready.active_incarnation(&incarnation.canonical_path).is_none());
    assert!(ready.is_retired_path(&incarnation.canonical_path));
    assert_eq!(ready.retiring_count(), 1);
}

#[test]
fn tombstone_ahead_of_intent_requires_exact_durable_stage_recovery() {
    let incarnation = published_incarnation(7, 0);
    let tombstone = tombstone_path(1, &incarnation);
    let ticket = retirement_ticket(1, &incarnation, RetirementStage::IntentDurable, false, None);
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(incarnation.clone()),
        SnapshotEntry::RetirementTicket(ticket.clone()),
    ]);
    let inventory = inventory([file(
        tombstone,
        incarnation.physical_key.expect("published key"),
        incarnation.expected_file_length,
    )]);

    let ReconciliationDisposition::RecoveryRequired(plan) =
        reconcile(recovered, inventory).expect("namespace-ahead state is recoverable")
    else {
        panic!("a durable tombstone observation must be recorded before activation");
    };
    assert_eq!(
        plan.actions(),
        &[
            ReconciliationAction::RecordLogicalRemoved(ticket.ticket_id),
            ReconciliationAction::RecordTombstoned(ticket.ticket_id),
        ]
    );
}

#[test]
fn absent_namespace_ahead_of_logical_removal_requires_absence_then_completion() {
    let incarnation = published_incarnation(7, 0);
    let ticket = retirement_ticket(1, &incarnation, RetirementStage::LogicalRemoved, false, None);
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(incarnation),
        SnapshotEntry::RetirementTicket(ticket.clone()),
    ]);

    let ReconciliationDisposition::RecoveryRequired(plan) =
        reconcile(recovered, inventory([])).expect("namespace-ahead state is recoverable")
    else {
        panic!("absence must be made durable before activation");
    };
    assert_eq!(
        plan.actions(),
        &[
            ReconciliationAction::RecordNamespaceAbsent {
                ticket_id: ticket.ticket_id,
                replacement_key: None,
            },
            ReconciliationAction::RecordCompleted(ticket.ticket_id),
        ]
    );
}

#[test]
fn replacement_requires_superseded_path_before_namespace_progress() {
    let incarnation = published_incarnation(7, 0);
    let ticket = retirement_ticket(1, &incarnation, RetirementStage::LogicalRemoved, false, None);
    let replacement = PhysicalFileKey::unix(7, 99);
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(incarnation.clone()),
        SnapshotEntry::RetirementTicket(ticket.clone()),
    ]);
    let inventory = inventory([file(
        incarnation.canonical_path.clone(),
        replacement,
        incarnation.expected_file_length,
    )]);

    let ReconciliationDisposition::RecoveryRequired(plan) =
        reconcile(recovered, inventory).expect("replacement is recoverable")
    else {
        panic!("replacement evidence must be persisted before activation");
    };
    assert_eq!(
        plan.actions(),
        &[
            ReconciliationAction::RecordSupersededPath {
                ticket_id: ticket.ticket_id,
                replacement_key: replacement,
            },
            ReconciliationAction::RecordNamespaceAbsent {
                ticket_id: ticket.ticket_id,
                replacement_key: Some(replacement),
            },
            ReconciliationAction::RecordCompleted(ticket.ticket_id),
        ]
    );
}

#[test]
fn completed_ticket_is_eligible_only_after_exact_clean_start_revalidation() {
    let incarnation = published_incarnation(7, 0);
    let replacement = PhysicalFileKey::unix(7, 99);
    let ticket = retirement_ticket(1, &incarnation, RetirementStage::CompletedRetained, true, None);
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(incarnation.clone()),
        SnapshotEntry::RetirementTicket(ticket.clone()),
    ]);
    let inventory = inventory([file(
        incarnation.canonical_path.clone(),
        replacement,
        incarnation.expected_file_length,
    )]);

    let ReconciliationDisposition::Ready(ready) = reconcile(recovered, inventory).expect("completed state reconciles")
    else {
        panic!("completed replacement should be clean-start ready");
    };
    assert!(ready.completed_is_revalidated(ticket.ticket_id));
    assert!(ready.is_retired_path(&incarnation.canonical_path));
}

#[test]
fn completed_ticket_rejects_the_original_target_reappearing() {
    let incarnation = published_incarnation(7, 0);
    let ticket = retirement_ticket(1, &incarnation, RetirementStage::CompletedRetained, false, None);
    let recovered = recovered(vec![
        SnapshotEntry::Incarnation(incarnation.clone()),
        SnapshotEntry::RetirementTicket(ticket),
    ]);
    let inventory = inventory([file(
        incarnation.canonical_path,
        incarnation.physical_key.expect("published key"),
        incarnation.expected_file_length,
    )]);

    assert!(matches!(
        reconcile(recovered, inventory),
        Err(ReconciliationError::CompletedTargetReappeared { .. })
    ));
}

#[test]
fn wrong_key_wrong_length_unknown_entry_and_incomplete_parent_fail_closed() {
    let incarnation = published_incarnation(7, 0);

    let wrong_key = inventory([file(
        incarnation.canonical_path.clone(),
        PhysicalFileKey::unix(8, 9),
        incarnation.expected_file_length,
    )]);
    assert!(matches!(
        reconcile(
            recovered(vec![SnapshotEntry::Incarnation(incarnation.clone())]),
            wrong_key
        ),
        Err(ReconciliationError::PhysicalKeyMismatch { .. })
    ));

    let wrong_length = inventory([file(
        incarnation.canonical_path.clone(),
        incarnation.physical_key.expect("published key"),
        incarnation.expected_file_length + 1,
    )]);
    assert!(matches!(
        reconcile(
            recovered(vec![SnapshotEntry::Incarnation(incarnation.clone())]),
            wrong_length
        ),
        Err(ReconciliationError::LengthMismatch { .. })
    ));

    let unknown = inventory([
        file(
            incarnation.canonical_path.clone(),
            incarnation.physical_key.expect("published key"),
            incarnation.expected_file_length,
        ),
        file(
            path("commitlog/00000000000000001024"),
            PhysicalFileKey::unix(7, 10),
            1_024,
        ),
    ]);
    assert!(matches!(
        reconcile(
            recovered(vec![SnapshotEntry::Incarnation(incarnation.clone())]),
            unknown
        ),
        Err(ReconciliationError::UntrackedNamespaceEntry { .. })
    ));

    let incomplete = StableNamespaceInventory::for_test(store_uuid(), [], []);
    assert!(matches!(
        reconcile(recovered(vec![SnapshotEntry::Incarnation(incarnation)]), incomplete),
        Err(ReconciliationError::IncompleteDirectoryInventory { .. })
    ));
}

#[test]
fn allocated_temp_is_bound_then_published_and_missing_allocation_is_resumed() {
    let mut allocated = published_incarnation(7, 0);
    allocated.phase = IncarnationPhase::Allocated;
    allocated.physical_key = None;
    let observed_key = PhysicalFileKey::unix(7, 41);
    let with_temp = inventory([file(
        allocated.create_file_path.clone(),
        observed_key,
        allocated.expected_file_length,
    )]);
    let ReconciliationDisposition::RecoveryRequired(plan) = reconcile(
        recovered(vec![SnapshotEntry::Incarnation(allocated.clone())]),
        with_temp,
    )
    .expect("allocated temp is recoverable") else {
        panic!("allocated temp requires durable Bind and Publish recovery");
    };
    assert_eq!(
        plan.actions(),
        &[
            ReconciliationAction::RecordBound {
                incarnation: allocated.incarnation,
                physical_key: observed_key,
            },
            ReconciliationAction::PublishBoundIncarnation(allocated.incarnation),
        ]
    );

    let ReconciliationDisposition::RecoveryRequired(plan) = reconcile(
        recovered(vec![SnapshotEntry::Incarnation(allocated.clone())]),
        inventory([]),
    )
    .expect("missing allocation namespace is recoverable") else {
        panic!("missing allocated temp must be resumed deterministically");
    };
    assert_eq!(
        plan.actions(),
        &[ReconciliationAction::ResumeAllocation(allocated.incarnation)]
    );
}

#[test]
fn bound_temp_is_published_and_already_renamed_canonical_records_publish_only() {
    let mut bound = published_incarnation(7, 0);
    bound.phase = IncarnationPhase::Bound;
    let key = bound.physical_key.expect("bound key");
    let with_temp = inventory([file(bound.create_file_path.clone(), key, bound.expected_file_length)]);
    let ReconciliationDisposition::RecoveryRequired(plan) =
        reconcile(recovered(vec![SnapshotEntry::Incarnation(bound.clone())]), with_temp)
            .expect("bound temp is recoverable")
    else {
        panic!("bound temp requires publish recovery");
    };
    assert_eq!(
        plan.actions(),
        &[ReconciliationAction::PublishBoundIncarnation(bound.incarnation)]
    );

    let renamed = inventory([file(bound.canonical_path.clone(), key, bound.expected_file_length)]);
    let ReconciliationDisposition::RecoveryRequired(plan) =
        reconcile(recovered(vec![SnapshotEntry::Incarnation(bound.clone())]), renamed)
            .expect("renamed bound incarnation is recoverable")
    else {
        panic!("canonical rename ahead of Publish requires one durable record");
    };
    assert_eq!(
        plan.actions(),
        &[ReconciliationAction::RecordPublished(bound.incarnation)]
    );
}

fn inventory<const N: usize>(entries: [(StoreRelativePath, NamespaceObject); N]) -> StableNamespaceInventory {
    StableNamespaceInventory::for_test(store_uuid(), ["commitlog"], entries)
}

fn file(path: StoreRelativePath, physical_key: PhysicalFileKey, length: u64) -> (StoreRelativePath, NamespaceObject) {
    (
        path,
        NamespaceObject::RegularFile {
            physical_key,
            length,
            content_fingerprint: None,
        },
    )
}

fn recovered(entries: Vec<SnapshotEntry>) -> NeedsReconciliation {
    let create_high_water = entries
        .iter()
        .filter_map(|entry| match entry {
            SnapshotEntry::Incarnation(entry) => Some(entry.incarnation.create_seq()),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let ticket_high_water = entries
        .iter()
        .filter_map(|entry| match entry {
            SnapshotEntry::RetirementTicket(entry) => Some(entry.ticket_id.get()),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let state = LedgerStateMachine::from_snapshot(LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 10,
        create_high_water,
        ticket_high_water,
        entries,
    })
    .expect("test snapshot is valid")
    .finish(10, 1, 1)
    .expect("test replay epochs are nonzero");
    NeedsReconciliation::for_test(state)
}

fn store_uuid() -> StoreUuid {
    StoreUuid::new([1; 16]).expect("test UUID is nonzero")
}

fn path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("test path is canonical")
}

fn published_incarnation(create_seq: u64, segment_offset: u64) -> IncarnationSnapshotEntry {
    let incarnation = FileIncarnationId::new(store_uuid(), create_seq).expect("test incarnation is nonzero");
    IncarnationSnapshotEntry {
        incarnation,
        phase: IncarnationPhase::Published,
        segment_offset,
        expected_file_length: 1_024,
        create_nonce: [0x11; 16],
        physical_key: Some(PhysicalFileKey::unix(7, 9)),
        canonical_path: path(&format!("commitlog/{segment_offset:020}")),
        create_file_path: path(&format!(
            "commitlog/.create.i{create_seq:016x}.s{segment_offset:020}.n{}",
            "11".repeat(16)
        )),
    }
}

fn retirement_ticket(
    ticket_id: u64,
    incarnation: &IncarnationSnapshotEntry,
    stage: RetirementStage,
    superseded_path_observed: bool,
    tombstone_path: Option<StoreRelativePath>,
) -> RetirementTicketSnapshotEntry {
    RetirementTicketSnapshotEntry {
        ticket_id: TicketId::new(ticket_id).expect("test ticket is nonzero"),
        incarnation: incarnation.incarnation,
        stage,
        superseded_path_observed,
        quarantined: false,
        reason: RetirementReason::TtlExpired,
        stage_sequence: 10,
        mapping_generation: 3,
        segment_offset: incarnation.segment_offset,
        expected_file_length: incarnation.expected_file_length,
        retirement_nonce: [0x22; 16],
        target_key: incarnation.physical_key.expect("published key"),
        canonical_path: incarnation.canonical_path.clone(),
        tombstone_path,
    }
}

fn tombstone_path(ticket_id: u64, incarnation: &IncarnationSnapshotEntry) -> StoreRelativePath {
    incarnation
        .canonical_path
        .tombstone_path(
            TicketId::new(ticket_id).expect("test ticket is nonzero"),
            incarnation.incarnation,
            incarnation.segment_offset,
            3,
            &[0x22; 16],
        )
        .expect("test tombstone binding is canonical")
}
