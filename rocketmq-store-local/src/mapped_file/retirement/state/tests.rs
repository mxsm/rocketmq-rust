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
use crate::mapped_file::retirement::codec::GenerationAbortReason;
use crate::mapped_file::retirement::codec::OpenReason;
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotMode;

#[test]
fn snapshot_completed_ticket_remains_retained_and_requires_revalidation() {
    let incarnation = published_incarnation(7);
    let ticket = completed_ticket(42, &incarnation, 9);
    let snapshot = snapshot(
        10,
        7,
        42,
        vec![
            SnapshotEntry::Incarnation(incarnation.clone()),
            SnapshotEntry::RetirementTicket(ticket.clone()),
        ],
    );

    let state = LedgerStateMachine::from_snapshot(snapshot)
        .expect("valid snapshot initializes state")
        .finish(10, 12, 3)
        .expect("nonzero recovery epochs finish state");

    assert_eq!(
        state.retirement_stage(ticket.ticket_id),
        Some(RetirementStage::CompletedRetained)
    );
    assert_eq!(
        state.completed_retained_eligibility(ticket.ticket_id),
        Some(CompletedRetainedEligibility::RequiresCleanStartRevalidation)
    );
    assert_eq!(state.retirement_count(), 1, "M3.2 must not omit stage-5 evidence");
}

#[test]
fn allocate_advances_create_high_water_exactly_once_and_identical_duplicate_is_idempotent() {
    let snapshot = snapshot(10, 7, 0, Vec::new());
    let mut state = LedgerStateMachine::from_snapshot(snapshot).expect("snapshot initializes state");
    let allocate = allocate_record(8);

    state
        .apply(11, Some(allocate.clone()))
        .expect("next create identifier is accepted");
    state
        .apply(12, Some(allocate))
        .expect("identical later duplicate is idempotent");
    let recovered = state.finish(12, 3, 1).expect("state finishes");

    assert_eq!(recovered.create_high_water(), 8);
    assert_eq!(recovered.incarnation_count(), 1);
}

#[test]
fn create_and_ticket_high_water_overflow_fail_closed() {
    let mut create_state = LedgerStateMachine::from_snapshot(snapshot(10, u64::MAX, 0, Vec::new()))
        .expect("maximum high-water snapshot is representable");
    assert_eq!(
        create_state.apply(11, Some(allocate_record(u64::MAX))),
        Err(StateViolation::HighWaterOverflow {
            field: "create_high_water"
        })
    );

    let incarnation = published_incarnation(7);
    let mut ticket_state = LedgerStateMachine::from_snapshot(snapshot(
        10,
        7,
        u64::MAX,
        vec![SnapshotEntry::Incarnation(incarnation.clone())],
    ))
    .expect("maximum ticket high-water snapshot is representable");
    assert_eq!(
        ticket_state.apply(11, Some(intent_record(u64::MAX, &incarnation))),
        Err(StateViolation::HighWaterOverflow {
            field: "ticket_high_water"
        })
    );
}

#[test]
fn reverse_identity_indexes_reject_aliases_and_failed_allocate_is_atomic() {
    let first = published_incarnation(7);
    let mut aliased = published_incarnation(8);
    aliased.canonical_path = first.canonical_path.clone();
    assert!(matches!(
        LedgerStateMachine::from_snapshot(snapshot(
            10,
            8,
            0,
            vec![SnapshotEntry::Incarnation(first), SnapshotEntry::Incarnation(aliased),],
        )),
        Err(StateViolation::InvalidSnapshotState)
    ));

    let mut state =
        LedgerStateMachine::from_snapshot(snapshot(10, 7, 0, Vec::new())).expect("snapshot initializes state");
    state
        .apply(11, Some(allocate_record(8)))
        .expect("first canonical owner is indexed");
    assert_eq!(
        state.apply(12, Some(allocate_record(9))),
        Err(StateViolation::RecordIdentityMismatch)
    );
    let recovered = state.finish(11, 3, 1).expect("failed indexed insert is atomic");
    assert_eq!(recovered.create_high_water(), 8);
    assert_eq!(recovered.incarnation_count(), 1);
}

#[test]
fn retirement_reverse_index_rejects_two_tickets_for_one_incarnation() {
    let incarnation = published_incarnation(7);
    let first = completed_ticket(1, &incarnation, 9);
    let mut second = completed_ticket(2, &incarnation, 9);
    second.stage = RetirementStage::IntentDurable;

    assert!(matches!(
        LedgerStateMachine::from_snapshot(snapshot(
            10,
            7,
            2,
            vec![
                SnapshotEntry::Incarnation(incarnation),
                SnapshotEntry::RetirementTicket(first),
                SnapshotEntry::RetirementTicket(second),
            ],
        )),
        Err(StateViolation::InvalidSnapshotState)
    ));
}

#[test]
fn successor_projection_only_omits_completed_ticket_and_its_sole_reference() {
    let completed_incarnation = published_incarnation(7);
    let completed = completed_ticket(1, &completed_incarnation, 9);
    let source = LedgerStateMachine::from_snapshot(snapshot(
        10,
        7,
        1,
        vec![
            SnapshotEntry::Incarnation(completed_incarnation.clone()),
            SnapshotEntry::RetirementTicket(completed.clone()),
        ],
    ))
    .expect("source snapshot is valid");
    let omitted = LedgerStateMachine::from_snapshot(successor_snapshot(10, 7, 1, Vec::new()))
        .expect("omitted completed evidence is structurally valid");
    source
        .validate_successor_projection(&omitted)
        .expect("completed ticket and its sole-reference incarnation may be omitted");

    let retained = LedgerStateMachine::from_snapshot(successor_snapshot(
        10,
        7,
        1,
        vec![
            SnapshotEntry::Incarnation(completed_incarnation.clone()),
            SnapshotEntry::RetirementTicket(completed),
        ],
    ))
    .expect("retained completed evidence is structurally valid");
    source
        .validate_successor_projection(&retained)
        .expect("retained completed evidence must remain exact");

    let mut incomplete = completed_ticket(1, &completed_incarnation, 9);
    incomplete.stage = RetirementStage::IntentDurable;
    let incomplete_source = LedgerStateMachine::from_snapshot(snapshot(
        10,
        7,
        1,
        vec![
            SnapshotEntry::Incarnation(completed_incarnation),
            SnapshotEntry::RetirementTicket(incomplete),
        ],
    ))
    .expect("incomplete source snapshot is valid");
    assert_eq!(
        incomplete_source.validate_successor_projection(&omitted),
        Err(StateViolation::InvalidSnapshotState)
    );
}

#[test]
fn successor_projection_rejects_ticket_only_completed_omission() {
    let incarnation = published_incarnation(7);
    let ticket = completed_ticket(1, &incarnation, 9);
    let source = LedgerStateMachine::from_snapshot(snapshot(
        10,
        7,
        1,
        vec![
            SnapshotEntry::Incarnation(incarnation.clone()),
            SnapshotEntry::RetirementTicket(ticket),
        ],
    ))
    .expect("source snapshot is valid");
    let ticket_only_omission = LedgerStateMachine::from_snapshot(successor_snapshot(
        10,
        7,
        1,
        vec![SnapshotEntry::Incarnation(incarnation)],
    ))
    .expect("ticket-only omission is structurally representable");

    assert_eq!(
        source.validate_successor_projection(&ticket_only_omission),
        Err(StateViolation::InvalidSnapshotState)
    );
}

#[test]
fn successor_projection_rejects_additions_changes_and_high_water_drift() {
    let source = LedgerStateMachine::from_snapshot(snapshot(10, 7, 0, Vec::new())).expect("source snapshot is valid");
    let addition = LedgerStateMachine::from_snapshot(successor_snapshot(
        10,
        7,
        0,
        vec![SnapshotEntry::Incarnation(published_incarnation(7))],
    ))
    .expect("successor addition is structurally valid");
    assert_eq!(
        source.validate_successor_projection(&addition),
        Err(StateViolation::InvalidSnapshotState)
    );

    let high_water_drift = LedgerStateMachine::from_snapshot(successor_snapshot(10, 8, 0, Vec::new()))
        .expect("high-water-only snapshot is structurally valid");
    assert_eq!(
        source.validate_successor_projection(&high_water_drift),
        Err(StateViolation::InvalidSnapshotState)
    );
}

#[test]
fn incarnation_state_machine_rejects_skips_and_identity_changes() {
    let mut state =
        LedgerStateMachine::from_snapshot(snapshot(10, 7, 0, Vec::new())).expect("snapshot initializes state");
    let allocate = allocate_record(8);
    state.apply(11, Some(allocate.clone())).expect("allocation is valid");

    assert!(matches!(
        state.apply(12, Some(publish_record(8))),
        Err(StateViolation::InvalidIncarnationTransition {
            from: Some(IncarnationPhase::Allocated),
            to: IncarnationPhase::Published,
        })
    ));

    let mut changed = allocate;
    let LedgerRecord::AllocateIncarnation { expected_length, .. } = &mut changed else {
        unreachable!("helper always returns AllocateIncarnation")
    };
    *expected_length += 1;
    assert_eq!(
        state.apply(12, Some(changed)),
        Err(StateViolation::IdentityChangingDuplicate { entity: "incarnation" })
    );
}

#[test]
fn direct_retirement_chain_requires_every_prerequisite_and_preserves_ticket() {
    let incarnation = published_incarnation(7);
    let mut state = LedgerStateMachine::from_snapshot(snapshot(
        10,
        7,
        0,
        vec![SnapshotEntry::Incarnation(incarnation.clone())],
    ))
    .expect("snapshot initializes state");
    let ticket = TicketId::new(1).expect("ticket is nonzero");

    state
        .apply(11, Some(intent_record(ticket.get(), &incarnation)))
        .expect("intent starts retirement");
    assert!(matches!(
        state.apply(12, Some(completed_record(ticket, incarnation.incarnation, 11))),
        Err(StateViolation::InvalidRetirementTransition {
            from: Some(RetirementStage::IntentDurable),
            to: RetirementStage::CompletedRetained,
        })
    ));
    state
        .apply(12, Some(logical_removed_record(ticket, &incarnation)))
        .expect("logical removal follows intent");
    state
        .apply(13, Some(namespace_absent_record(ticket, &incarnation, None)))
        .expect("Unix direct absence follows logical removal");
    state
        .apply(14, Some(completed_record(ticket, incarnation.incarnation, 13)))
        .expect("completion names the exact absence sequence");

    let recovered = state.finish(14, 7, 2).expect("state finishes");
    assert_eq!(
        recovered.retirement_stage(ticket),
        Some(RetirementStage::CompletedRetained)
    );
    assert_eq!(recovered.retirement_count(), 1);
}

#[test]
fn generation_prepared_is_an_append_barrier_until_exact_abort() {
    let mut state =
        LedgerStateMachine::from_snapshot(snapshot(10, 7, 0, Vec::new())).expect("snapshot initializes state");
    let prepared = LedgerRecord::GenerationPrepared {
        store_uuid: store_uuid(),
        source_generation: 1,
        target_generation: 2,
        target_snapshot_generation: 2,
        open_reason: OpenReason::Compaction,
    };
    state
        .apply(11, Some(prepared))
        .expect("generation preparation is valid");

    assert_eq!(
        state.apply(12, Some(allocate_record(8))),
        Err(StateViolation::AppendAfterGenerationPrepared)
    );
    state
        .apply(
            12,
            Some(LedgerRecord::GenerationAborted {
                store_uuid: store_uuid(),
                source_generation: 1,
                target_generation: 2,
                prepared_sequence: 11,
                abort_reason: GenerationAbortReason::Validation,
            }),
        )
        .expect("the exact immediate abort releases the append barrier");
    state
        .apply(13, Some(allocate_record(8)))
        .expect("ordinary appends resume after the durable abort");
}

#[test]
fn tombstone_and_sticky_annotations_do_not_skip_or_erase_retirement_state() {
    let incarnation = published_incarnation(7);
    let mut state = LedgerStateMachine::from_snapshot(snapshot(
        10,
        7,
        0,
        vec![SnapshotEntry::Incarnation(incarnation.clone())],
    ))
    .expect("snapshot initializes state");
    let ticket = TicketId::new(1).expect("ticket is nonzero");
    let tombstone = tombstone_path(ticket, incarnation.incarnation);

    state
        .apply(11, Some(intent_record(ticket.get(), &incarnation)))
        .expect("intent starts retirement");
    state
        .apply(12, Some(logical_removed_record(ticket, &incarnation)))
        .expect("logical removal is required");
    state
        .apply(13, Some(tombstoned_record(ticket, &incarnation, tombstone.clone())))
        .expect("tombstone follows logical removal");
    state
        .apply(14, Some(superseded_record(ticket, &incarnation)))
        .expect("replacement evidence is sticky but does not advance the stage");
    state
        .apply(15, Some(namespace_absent_record(ticket, &incarnation, Some(tombstone))))
        .expect("absence preserves the authorized tombstone path");
    state
        .apply(16, Some(completed_record(ticket, incarnation.incarnation, 15)))
        .expect("completion names the exact absence frame");

    assert_eq!(
        state.finish(16, 9, 3).expect("state finishes").retirement_stage(ticket),
        Some(RetirementStage::CompletedRetained)
    );
}

#[test]
fn quarantine_is_idempotent_only_for_the_exact_persisted_payload() {
    let mut state =
        LedgerStateMachine::from_snapshot(snapshot(10, 7, 0, Vec::new())).expect("snapshot initializes state");
    let record = quarantine_record(path("commitlog/.unknown"), None);
    state
        .apply(11, Some(record.clone()))
        .expect("first audited classification is retained");
    state.apply(12, Some(record)).expect("exact duplicate is idempotent");
    assert_eq!(
        state.apply(
            13,
            Some(quarantine_record(
                path("commitlog/.unknown"),
                Some(path("quarantine/.unknown")),
            )),
        ),
        Err(StateViolation::IdentityChangingDuplicate { entity: "quarantine" })
    );
    assert_eq!(
        state
            .finish(12, 3, 1)
            .expect("failed append is atomic")
            .quarantine_count(),
        1
    );
}

#[test]
fn generation_administration_records_are_bound_to_the_selected_generation() {
    let mut state =
        LedgerStateMachine::from_snapshot(snapshot(10, 7, 0, Vec::new())).expect("snapshot initializes state");
    assert_eq!(
        state.apply(
            11,
            Some(LedgerRecord::MarkerCommitted {
                store_uuid: store_uuid(),
                marker_epoch: 1,
                snapshot_generation: 2,
                log_generation: 2,
                anchor_sequence: 10,
                slot_index: 0,
                slot_crc32: 1,
            }),
        ),
        Err(StateViolation::IllegalGenerationAdministration)
    );
    state
        .apply(
            11,
            Some(LedgerRecord::LogOpened {
                store_uuid: store_uuid(),
                generation: 1,
                snapshot_generation: 1,
                predecessor_log_generation: 0,
                predecessor_terminal_acknowledged_sequence: 10,
                snapshot_base_sequence: 10,
                snapshot_file_length: 108,
                snapshot_file_crc32: 1,
                predecessor_prefix_crc32: 1,
                validated_prefix_length: 172,
                unacknowledged_suffix_length: 0,
                unacknowledged_suffix_crc32: 0,
                open_reason: OpenReason::Compaction,
                predecessor_acknowledgement_epoch: 7,
            }),
        )
        .expect("exact selected-generation opener is accepted");
    state
        .apply(
            12,
            Some(LedgerRecord::MarkerCommitted {
                store_uuid: store_uuid(),
                marker_epoch: 2,
                snapshot_generation: 1,
                log_generation: 1,
                anchor_sequence: 11,
                slot_index: 1,
                slot_crc32: 1,
            }),
        )
        .expect("marker witness immediately follows its anchor");
    assert_eq!(
        state.apply(
            13,
            Some(LedgerRecord::MarkerCommitted {
                store_uuid: store_uuid(),
                marker_epoch: 2,
                snapshot_generation: 1,
                log_generation: 1,
                anchor_sequence: 12,
                slot_index: 1,
                slot_crc32: 1,
            }),
        ),
        Err(StateViolation::IllegalGenerationAdministration)
    );
}

#[test]
fn generation_preparation_reserves_a_representable_target_marker_epoch() {
    let mut state = LedgerStateMachine::from_snapshot(LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: u64::MAX - 1,
        log_generation: u64::MAX - 1,
        predecessor_log_generation: u64::MAX - 2,
        base_sequence: 10,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    })
    .expect("last marker-addressable generation is a valid snapshot");
    assert_eq!(
        state.apply(
            11,
            Some(LedgerRecord::GenerationPrepared {
                store_uuid: store_uuid(),
                source_generation: u64::MAX - 1,
                target_generation: u64::MAX,
                target_snapshot_generation: u64::MAX,
                open_reason: OpenReason::Compaction,
            }),
        ),
        Err(StateViolation::IllegalGenerationAdministration)
    );
}

fn snapshot(
    base_sequence: u64,
    create_high_water: u64,
    ticket_high_water: u64,
    entries: Vec<SnapshotEntry>,
) -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence,
        create_high_water,
        ticket_high_water,
        entries,
    }
}

fn successor_snapshot(
    base_sequence: u64,
    create_high_water: u64,
    ticket_high_water: u64,
    entries: Vec<SnapshotEntry>,
) -> LifecycleSnapshot {
    LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 2,
        log_generation: 2,
        predecessor_log_generation: 1,
        base_sequence,
        create_high_water,
        ticket_high_water,
        entries,
    }
}

fn store_uuid() -> StoreUuid {
    StoreUuid::new([1; 16]).expect("test UUID is nonzero")
}

fn incarnation_id(create_seq: u64) -> FileIncarnationId {
    FileIncarnationId::new(store_uuid(), create_seq).expect("test create sequence is nonzero")
}

fn path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("test path is valid")
}

fn create_path(create_seq: u64) -> StoreRelativePath {
    path(&format!(
        "commitlog/.create.i{create_seq:016x}.s00000000000000000000.n11111111111111111111111111111111"
    ))
}

fn canonical_path() -> StoreRelativePath {
    path("commitlog/00000000000000000000")
}

fn published_incarnation(create_seq: u64) -> IncarnationSnapshotEntry {
    IncarnationSnapshotEntry {
        incarnation: incarnation_id(create_seq),
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: 1_024,
        create_nonce: [0x11; 16],
        physical_key: Some(PhysicalFileKey::unix(7, 9)),
        canonical_path: canonical_path(),
        create_file_path: create_path(create_seq),
    }
}

fn completed_ticket(
    ticket_id: u64,
    incarnation: &IncarnationSnapshotEntry,
    stage_sequence: u64,
) -> RetirementTicketSnapshotEntry {
    RetirementTicketSnapshotEntry {
        ticket_id: TicketId::new(ticket_id).expect("ticket is nonzero"),
        incarnation: incarnation.incarnation,
        stage: RetirementStage::CompletedRetained,
        superseded_path_observed: false,
        quarantined: false,
        reason: RetirementReason::TtlExpired,
        stage_sequence,
        mapping_generation: 3,
        segment_offset: incarnation.segment_offset,
        expected_file_length: incarnation.expected_file_length,
        retirement_nonce: [0x22; 16],
        target_key: incarnation.physical_key.expect("published incarnation has a key"),
        canonical_path: incarnation.canonical_path.clone(),
        tombstone_path: None,
    }
}

fn allocate_record(create_seq: u64) -> LedgerRecord {
    LedgerRecord::AllocateIncarnation {
        incarnation: incarnation_id(create_seq),
        segment_offset: 0,
        expected_length: 1_024,
        create_nonce: [0x11; 16],
        canonical_path: canonical_path(),
        create_file_path: create_path(create_seq),
    }
}

fn publish_record(create_seq: u64) -> LedgerRecord {
    LedgerRecord::PublishIncarnation {
        incarnation: incarnation_id(create_seq),
        expected_length: 1_024,
        physical_key: PhysicalFileKey::unix(7, 9),
        canonical_path: canonical_path(),
        create_file_path: create_path(create_seq),
    }
}

fn intent_record(ticket_id: u64, incarnation: &IncarnationSnapshotEntry) -> LedgerRecord {
    LedgerRecord::RetirementIntent {
        ticket_id: TicketId::new(ticket_id).expect("ticket is nonzero"),
        incarnation: incarnation.incarnation,
        reason: RetirementReason::TtlExpired,
        mapping_generation: 3,
        segment_offset: incarnation.segment_offset,
        expected_length: incarnation.expected_file_length,
        retirement_nonce: [0x22; 16],
        target_key: incarnation.physical_key.expect("published incarnation has a key"),
        canonical_path: incarnation.canonical_path.clone(),
    }
}

fn logical_removed_record(ticket_id: TicketId, incarnation: &IncarnationSnapshotEntry) -> LedgerRecord {
    LedgerRecord::LogicalRemoved {
        ticket_id,
        incarnation: incarnation.incarnation,
        target_key: incarnation.physical_key.expect("published incarnation has a key"),
        canonical_path: incarnation.canonical_path.clone(),
    }
}

fn namespace_absent_record(
    ticket_id: TicketId,
    incarnation: &IncarnationSnapshotEntry,
    tombstone_path: Option<StoreRelativePath>,
) -> LedgerRecord {
    LedgerRecord::NamespaceAbsent {
        ticket_id,
        incarnation: incarnation.incarnation,
        replacement_observed: false,
        observation_time_ns: 1,
        target_key: incarnation.physical_key.expect("published incarnation has a key"),
        canonical_path: incarnation.canonical_path.clone(),
        tombstone_path,
    }
}

fn completed_record(ticket_id: TicketId, incarnation: FileIncarnationId, namespace_sequence: u64) -> LedgerRecord {
    LedgerRecord::Completed {
        ticket_id,
        incarnation,
        completion_time_ns: 2,
        namespace_absent_sequence: namespace_sequence,
    }
}

fn tombstone_path(ticket_id: TicketId, incarnation: FileIncarnationId) -> StoreRelativePath {
    path(&format!(
        "commitlog/.delete.t{:016x}.i{:016x}.s00000000000000000000.m0000000000000003.n{}",
        ticket_id.get(),
        incarnation.create_seq(),
        "22".repeat(16)
    ))
}

fn tombstoned_record(
    ticket_id: TicketId,
    incarnation: &IncarnationSnapshotEntry,
    tombstone_path: StoreRelativePath,
) -> LedgerRecord {
    LedgerRecord::Tombstoned {
        ticket_id,
        incarnation: incarnation.incarnation,
        target_key: incarnation.physical_key.expect("published incarnation has a key"),
        retirement_nonce: [0x22; 16],
        canonical_path: incarnation.canonical_path.clone(),
        tombstone_path,
    }
}

fn superseded_record(ticket_id: TicketId, incarnation: &IncarnationSnapshotEntry) -> LedgerRecord {
    LedgerRecord::SupersededPath {
        ticket_id,
        incarnation: incarnation.incarnation,
        expected_target_key: incarnation.physical_key.expect("published incarnation has a key"),
        observed_replacement_key: PhysicalFileKey::unix(8, 10),
        canonical_path: incarnation.canonical_path.clone(),
    }
}

fn quarantine_record(source_path: StoreRelativePath, destination_path: Option<StoreRelativePath>) -> LedgerRecord {
    LedgerRecord::Quarantined {
        entity_kind: QuarantineEntityKind::Canonical,
        reason: QuarantineReason::UnknownOwner,
        sequence_at_observation: 11,
        physical_key: None,
        content_fingerprint: None,
        source_path,
        destination_path,
    }
}
