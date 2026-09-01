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

use super::plan::*;
use super::types::*;
use super::*;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::decode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::decode_commit_seal;
use crate::mapped_file::retirement::codec::decode_next_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlotState;
use crate::mapped_file::retirement::codec::DecodeOutcome;
use crate::mapped_file::retirement::codec::GenerationAbortReason;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;
use crate::mapped_file::retirement::sidecar::decode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::decode_snapshot;
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
use crate::mapped_file::retirement::sidecar::IncarnationSnapshotEntry;
use crate::mapped_file::retirement::sidecar::QuarantineSnapshotEntry;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::sidecar::RetirementTicketSnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::StoreMeta;

macro_rules! assert_not_clone {
    ($type:ty) => {
        const _: fn() = || {
            trait AmbiguousIfClone<A> {
                fn marker() {}
            }
            impl<T: ?Sized> AmbiguousIfClone<()> for T {}
            impl<T: ?Sized + Clone> AmbiguousIfClone<u8> for T {}
            let _ = <$type as AmbiguousIfClone<_>>::marker;
        };
    };
}

assert_not_clone!(PreparedCompactionFoundation);
assert_not_clone!(CleanStartOmissionEvidence);
assert_not_clone!(CompactionAbandonmentProof);
assert_not_clone!(OldPairReconstructionReceipt);
assert_not_clone!(LaterCleanStartReceipt);
assert_not_clone!(SnapshotEncodedReceipt);
assert_not_clone!(PublishedPairVerifiedReceipt);
assert_not_clone!(ReconciliationReceipt);
assert_not_clone!(FencedCompactionEvidence);

#[test]
fn scheduling_thresholds_are_advisory_only() {
    assert_eq!(
        compaction_schedule(CompactionMetrics {
            active_log_bytes: COMPACTION_LOG_BYTES_THRESHOLD - 1,
            completed_record_count: COMPLETED_RECORD_THRESHOLD - 1,
        }),
        CompactionSchedule::NotScheduled
    );
    assert_eq!(
        compaction_schedule(CompactionMetrics {
            active_log_bytes: COMPACTION_LOG_BYTES_THRESHOLD,
            completed_record_count: 0,
        }),
        CompactionSchedule::Candidate(CompactionTrigger::LogSize)
    );
    assert_eq!(
        compaction_schedule(CompactionMetrics {
            active_log_bytes: 0,
            completed_record_count: COMPLETED_RECORD_THRESHOLD,
        }),
        CompactionSchedule::Candidate(CompactionTrigger::CompletedRecords)
    );
}

#[test]
fn preparation_and_switch_are_split_by_an_exact_committed_prepared_receipt() {
    let preparation = preparation_plan(Vec::new()).expect("verified source admits preparation");
    assert_eq!(preparation.begin(), execute(CompactionAction::AcquireAppendBarrier));
    let barrier = preparation.barrier_held_receipt_for_test();
    assert_eq!(
        preparation.after_barrier(barrier),
        execute(CompactionAction::CommitGenerationPrepared)
    );

    let prepared = preparation.generation_prepared();
    assert_eq!(prepared.sequence(), 41);
    assert_eq!(prepared.acknowledgement_epoch(), 51);
    assert_eq!(prepared.frame_start_offset(), 1_000);
    assert!(matches!(
        decode_unit_record(prepared, 4),
        LedgerRecord::GenerationPrepared {
            source_generation: 4,
            target_generation: 5,
            target_snapshot_generation: 5,
            ..
        }
    ));
    let prepared_sealed_log_length = prepared.sealed_log_length();

    let resulting_prefix_crc32 = 0x89ab_cdef;
    let receipt = preparation.prepared_receipt_for_test(resulting_prefix_crc32);
    let foundation = preparation
        .finish_preparation(receipt)
        .expect("exact committed prepared receipt creates the post-Prepared foundation");
    let plan = CompactionPlan::from_prepared(foundation).expect("prepared foundation plans the switch");

    assert!(matches!(
        decode_unit_record(plan.log_opened(), 5),
        LedgerRecord::LogOpened {
            generation: 5,
            predecessor_log_generation: 4,
            predecessor_terminal_acknowledged_sequence: 41,
            predecessor_acknowledgement_epoch: 51,
            predecessor_prefix_crc32,
            validated_prefix_length,
            ..
        } if predecessor_prefix_crc32 == resulting_prefix_crc32
            && validated_prefix_length == prepared_sealed_log_length
    ));
}

#[test]
fn canonical_store_meta_and_authoritative_inventory_are_bound_before_planning() {
    let canonical = encode_store_meta(&store_meta()).expect("fixture store.meta is canonical");
    let mut changed = store_meta();
    changed.creation_time_ns += 1;
    let parts = verified_source_parts(Vec::new());
    assert!(matches!(
        VerifiedCompactionSource::verified_for_test(changed, canonical, parts),
        Err(CompactionPlanViolation::InvalidFoundation { .. })
    ));

    let plan = standard_plan();
    let snapshot = decode_snapshot(plan.snapshot_bytes()).expect("planned snapshot is canonical");
    let expected_entries = inventory_entries();
    assert_eq!(snapshot.entries.len(), expected_entries.len());
    for entry in expected_entries {
        assert!(snapshot.entries.contains(&entry));
    }
    assert_eq!(snapshot.create_high_water, 7);
    assert_eq!(snapshot.ticket_high_water, 9);
}

#[test]
fn free_space_preflight_counts_both_copies_both_old_pairs_and_margin() {
    let plan = standard_plan();
    let expected = plan
        .snapshot_file_length()
        .checked_mul(2)
        .and_then(|value| value.checked_add(plan.target_log_final_length().checked_mul(2)?))
        .and_then(|value| value.checked_add(11_000))
        .and_then(|value| value.checked_add(22_000))
        .and_then(|value| value.checked_add(COMPACTION_SAFETY_MARGIN_BYTES))
        .expect("test sizing fits");
    assert_eq!(plan.space().required_bytes, expected);

    let one_short = CompactionSpace {
        available_bytes: expected - 1,
        marker_referenced_pair_bytes: [11_000, 22_000],
    };
    assert!(matches!(
        build_plan(Vec::new(), one_short),
        Err(CompactionPlanViolation::InsufficientSpace {
            required,
            available
        }) if required == expected && available == expected - 1
    ));
}

#[test]
fn plan_binds_snapshot_anchor_marker_and_witness_after_prepared() {
    let plan = standard_plan();
    assert_eq!(plan.source_generation(), 4);
    assert_eq!(plan.target_generation(), 5);
    assert_eq!(plan.target_marker_epoch(), 6);
    assert_eq!(plan.snapshot_base_sequence(), 41);
    assert_eq!(plan.snapshot_file_crc32(), crc32(plan.snapshot_bytes()));

    let marker = decode_enabled_marker_slot(plan.marker_slot_bytes(), plan.marker_slot_index())
        .expect("planned marker is byte-valid");
    assert_eq!(marker.marker_epoch, 6);
    assert_eq!(marker.snapshot_generation, 5);
    assert_eq!(marker.anchor_sequence, 42);
    assert_eq!(marker.anchor_frame_crc32, crc32(plan.log_opened().frame()));

    assert_eq!(plan.log_opened().sequence(), 42);
    assert_eq!(plan.log_opened().acknowledgement_epoch(), 52);
    assert_eq!(plan.marker_committed().sequence(), 43);
    assert_eq!(plan.marker_committed().acknowledgement_epoch(), 53);
    assert!(matches!(
        decode_unit_record(plan.marker_committed(), 5),
        LedgerRecord::MarkerCommitted {
            marker_epoch: 6,
            anchor_sequence: 42,
            slot_crc32,
            ..
        } if slot_crc32 == plan.marker_slot_stored_crc32()
    ));
    assert_unit_binding(plan.log_opened(), true, 6);
    assert_unit_binding(plan.marker_committed(), true, 6);
}

#[test]
fn completed_retained_omission_consumes_an_exact_store_frontier_and_entry_proof() {
    let completed = retirement_entry(8, RetirementStage::CompletedRetained, 30);
    let evidence = CleanStartOmissionEvidence::verified_for_test(&store_meta(), 4, 5, 3, 40, completed.clone())
        .expect("exact later clean-start proof is canonical");
    let mut parts = verified_source_parts(vec![evidence]);
    parts
        .entries
        .retain(|entry| !matches!(entry, SnapshotEntry::RetirementTicket(ticket) if ticket.ticket_id == ticket_id(9)));
    let plan = build_plan_from_parts(parts).expect("exact proof may omit its completed pair");
    let snapshot = decode_snapshot(plan.snapshot_bytes()).expect("planned snapshot decodes");
    assert!(!snapshot.entries.iter().any(|entry| matches!(
        entry,
        SnapshotEntry::RetirementTicket(ticket) if ticket.ticket_id == ticket_id(8)
    )));
    assert!(!snapshot.entries.iter().any(|entry| matches!(
        entry,
        SnapshotEntry::Incarnation(incarnation) if incarnation.incarnation == incarnation_id()
    )));
    assert_eq!(plan.omitted_completed_tickets(), &[ticket_id(8)]);

    let evidence = CleanStartOmissionEvidence::verified_for_test(&store_meta(), 4, 5, 3, 40, completed)
        .expect("proof fixture is canonical");
    let mut parts = verified_source_parts(vec![evidence]);
    parts
        .entries
        .retain(|entry| !matches!(entry, SnapshotEntry::RetirementTicket(ticket) if ticket.ticket_id == ticket_id(9)));
    let ticket = parts
        .entries
        .iter_mut()
        .find_map(|entry| match entry {
            SnapshotEntry::RetirementTicket(ticket) if ticket.ticket_id == ticket_id(8) => Some(ticket),
            _ => None,
        })
        .expect("fixture contains the completed retirement ticket");
    ticket.reason = RetirementReason::OffsetTruncate;
    let source = VerifiedCompactionSource::verified_for_test(
        store_meta(),
        encode_store_meta(&store_meta()).expect("fixture metadata encodes"),
        parts,
    )
    .expect("the changed inventory remains internally canonical");
    let preparation = CompactionPreparationPlan::from_verified_source(source).expect("preparation remains valid");
    let receipt = preparation.prepared_receipt_for_test(0x89ab_cdef);
    let foundation = preparation
        .finish_preparation(receipt)
        .expect("prepared receipt remains exact");
    assert!(matches!(
        CompactionPlan::from_prepared(foundation),
        Err(CompactionPlanViolation::InvalidOmissionEvidence { .. })
    ));
}

#[test]
fn completed_retained_omission_rejects_an_incarnation_referenced_by_another_ticket() {
    let completed = retirement_entry(8, RetirementStage::CompletedRetained, 30);
    let evidence = CleanStartOmissionEvidence::verified_for_test(&store_meta(), 4, 5, 3, 40, completed)
        .expect("exact later clean-start proof is canonical");

    assert!(matches!(
        build_plan(vec![evidence], ample_space()),
        Err(CompactionPlanViolation::InvalidOmissionEvidence { .. })
    ));
}

#[test]
fn opaque_receipts_enforce_all_ten_steps_and_reject_cross_plan_reuse() {
    let plan = standard_plan();
    let cases = [
        (plan.begin(), CompactionStep::CanonicalSnapshot),
        (
            plan.after_snapshot_encoded(plan.snapshot_encoded_receipt_for_test()),
            CompactionStep::PublishSnapshot,
        ),
        (
            plan.after_snapshot_published(plan.snapshot_published_receipt_for_test()),
            CompactionStep::PublishLogOpened,
        ),
        (
            plan.after_log_opened_published(plan.log_opened_published_receipt_for_test()),
            CompactionStep::VerifyPublishedPair,
        ),
        (
            plan.after_pair_verified(plan.pair_verified_receipt_for_test()),
            CompactionStep::CommitMarkerSlot,
        ),
        (
            plan.after_marker_committed(plan.marker_committed_receipt_for_test()),
            CompactionStep::CommitLogOpened,
        ),
        (
            plan.after_log_opened_committed(plan.log_opened_committed_receipt_for_test()),
            CompactionStep::CommitMarkerWitness,
        ),
        (
            plan.after_marker_witness_committed(plan.marker_witness_committed_receipt_for_test()),
            CompactionStep::ReplayAndReconcile,
        ),
        (
            plan.after_replay(plan.replay_receipt_for_test()),
            CompactionStep::ReplayAndReconcile,
        ),
    ];
    for (decision, step) in cases {
        let CompactionDecision::Execute(action) = decision else {
            panic!("verified phase receipt must advance")
        };
        assert_eq!(action.protocol_step(), step);
    }

    let CompactionDecision::FencedComplete(evidence) =
        plan.after_reconciliation(plan.reconciliation_receipt_for_test())
    else {
        panic!("verified replay plus reconciliation returns fenced coordinates")
    };
    assert_eq!(evidence.target_generation(), 5);
    assert_eq!(evidence.next_sequence(), 44);
    assert_eq!(evidence.next_acknowledgement_epoch(), 54);

    let other = plan_for_store([0x44; 16]);
    let foreign_receipt = other.pair_verified_receipt_for_test();
    assert!(matches!(
        plan.after_pair_verified(foreign_receipt),
        CompactionDecision::NeedsRecovery(CompactionNeedsRecovery {
            reason: CompactionRecoveryReason::ReceiptMismatch,
            ..
        })
    ));
}

#[test]
fn facade_and_plan_sources_do_not_export_raw_progress_or_mutable_plan_fields() {
    let facade = include_str!("compaction.rs");
    let plan_source = include_str!("compaction/plan.rs");
    assert!(!facade.contains("pub(crate) use"));
    assert!(!plan_source.contains("pub(crate) struct CompactionPlan"));
    assert!(!plan_source.contains("CompactionProgress"));
    assert!(!plan_source.contains("decide(&self, progress"));
}

#[test]
fn every_mutating_or_durability_crash_boundary_stays_fenced() {
    let plan = standard_plan();
    for boundary in crash_boundaries() {
        assert_eq!(
            plan.decision_after_failure(boundary),
            CompactionDecision::NeedsRecovery(CompactionNeedsRecovery {
                step: boundary.protocol_step(),
                reason: CompactionRecoveryReason::OperationFailed(boundary),
            })
        );
    }
}

#[test]
fn generation_abort_requires_a_complete_section_10_1_scan_proof() {
    let plan = standard_plan();
    let proof = plan
        .verify_abandonment_for_test(plan.complete_abandonment_scan_for_test())
        .expect("complete negative scan mints abandonment proof");
    let CompactionAbortDecision::AppendGenerationAborted(unit) = plan.decide_abort(proof, GenerationAbortReason::Io)
    else {
        panic!("verified abandonment proof permits the immediate abort record")
    };
    assert_eq!(unit.sequence(), 42);
    assert_eq!(unit.acknowledgement_epoch(), 52);
    assert_eq!(
        unit.frame_start_offset(),
        plan.generation_prepared().sealed_log_length()
    );

    let mut scan = plan.complete_abandonment_scan_for_test();
    scan.prepared_seal[0] ^= 1;
    assert_eq!(
        plan.verify_abandonment_for_test(scan),
        Err(AbortDeniedReason::PreparedUnitMismatch)
    );

    for corrupt in [
        |scan: &mut AbandonmentScanForTest| scan.marker_slots_reconstructed = false,
        |scan: &mut AbandonmentScanForTest| scan.acknowledgement_slots_reconstructed = false,
        |scan: &mut AbandonmentScanForTest| scan.no_target_marker = false,
        |scan: &mut AbandonmentScanForTest| scan.no_target_acknowledgement_or_seal = false,
        |scan: &mut AbandonmentScanForTest| scan.candidate_exact_or_absent = false,
        |scan: &mut AbandonmentScanForTest| scan.complete_higher_generation_inventory = false,
        |scan: &mut AbandonmentScanForTest| scan.no_gaps_or_unexplained_artifacts = false,
    ] {
        let mut scan = plan.complete_abandonment_scan_for_test();
        corrupt(&mut scan);
        assert!(plan.verify_abandonment_for_test(scan).is_err());
    }
}

#[test]
fn abandonment_proof_is_bound_to_one_exact_prepared_plan() {
    let plan = standard_plan();
    let other = plan_for_store([0x44; 16]);
    let proof = other
        .verify_abandonment_for_test(other.complete_abandonment_scan_for_test())
        .expect("other plan has a complete scan");
    assert!(matches!(
        plan.decide_abort(proof, GenerationAbortReason::Validation),
        CompactionAbortDecision::FailClosed(AbortDeniedReason::ProofMismatch)
    ));
}

#[test]
fn old_pair_gc_consumes_reconstruction_and_later_clean_start_receipts() {
    let meta = store_meta();
    let reconstruction = OldPairReconstructionReceipt::verified_for_test(
        &meta,
        OldPairScanForTest {
            candidate_generation: 4,
            current_generation: 6,
            marker_epoch: 7,
            marker_witness_sequence: 43,
            marker_slots: [
                SlotReference::Valid { generation: 5 },
                SlotReference::Valid { generation: 6 },
            ],
            acknowledgement_slots: [
                SlotReference::Valid { generation: 6 },
                SlotReference::Valid { generation: 6 },
            ],
            acknowledgement_seals_reconstructed: [true, true],
            retained_log_generations: vec![5, 6],
        },
    )
    .expect("slot and seal reconstruction is complete");
    let clean_start = LaterCleanStartReceipt::verified_for_test(&meta, 6, 7, 43, 43)
        .expect("current marker witness was replayed at a later clean start");
    assert_eq!(
        old_pair_gc_decision(reconstruction, clean_start),
        OldPairGcDecision::Eligible(OldPairDeletionPlan::new_for_test(4))
    );
}

#[test]
fn old_pair_gc_proof_minting_rejects_references_ambiguity_and_missing_seals() {
    let meta = store_meta();
    let base = OldPairScanForTest {
        candidate_generation: 4,
        current_generation: 6,
        marker_epoch: 7,
        marker_witness_sequence: 43,
        marker_slots: [
            SlotReference::Valid { generation: 5 },
            SlotReference::Valid { generation: 6 },
        ],
        acknowledgement_slots: [
            SlotReference::Valid { generation: 6 },
            SlotReference::Valid { generation: 6 },
        ],
        acknowledgement_seals_reconstructed: [true, true],
        retained_log_generations: vec![5, 6],
    };

    let mut referenced = base.clone();
    referenced.marker_slots[0] = SlotReference::Valid { generation: 4 };
    assert!(matches!(
        OldPairReconstructionReceipt::verified_for_test(&meta, referenced),
        Err(OldPairRetentionReason::StillReferenced)
    ));

    let mut ambiguous = base.clone();
    ambiguous.acknowledgement_slots[0] = SlotReference::InvalidNonZero;
    assert!(matches!(
        OldPairReconstructionReceipt::verified_for_test(&meta, ambiguous),
        Err(OldPairRetentionReason::AmbiguousSlot)
    ));

    let mut missing_seal = base;
    missing_seal.acknowledgement_seals_reconstructed[1] = false;
    assert!(matches!(
        OldPairReconstructionReceipt::verified_for_test(&meta, missing_seal),
        Err(OldPairRetentionReason::AcknowledgementNotReconstructible)
    ));
}

#[test]
fn old_pair_gc_rejects_receipts_from_different_current_frontiers() {
    let meta = store_meta();
    let reconstruction =
        OldPairReconstructionReceipt::verified_for_test(&meta, OldPairScanForTest::eligible_for_test(4, 6, 7, 43))
            .expect("reconstruction receipt is valid");
    let later = LaterCleanStartReceipt::verified_for_test(&meta, 7, 8, 44, 44)
        .expect("different current frontier is internally valid");
    assert_eq!(
        old_pair_gc_decision(reconstruction, later),
        OldPairGcDecision::Retain(OldPairRetentionReason::ReceiptMismatch)
    );
}

fn standard_plan() -> CompactionPlan {
    build_plan(Vec::new(), ample_space()).expect("standard compaction plan is valid")
}

fn build_plan(
    omissions: Vec<CleanStartOmissionEvidence>,
    space: CompactionSpace,
) -> Result<CompactionPlan, CompactionPlanViolation> {
    build_plan_from_parts(VerifiedSourcePartsForTest {
        space,
        omission_evidence: omissions,
        ..verified_source_parts(Vec::new())
    })
}

fn build_plan_from_parts(parts: VerifiedSourcePartsForTest) -> Result<CompactionPlan, CompactionPlanViolation> {
    let source = VerifiedCompactionSource::verified_for_test(store_meta(), encode_store_meta(&store_meta())?, parts)?;
    let preparation = CompactionPreparationPlan::from_verified_source(source)?;
    let receipt = preparation.prepared_receipt_for_test(0x89ab_cdef);
    let foundation = preparation.finish_preparation(receipt)?;
    CompactionPlan::from_prepared(foundation)
}

fn preparation_plan(
    omissions: Vec<CleanStartOmissionEvidence>,
) -> Result<CompactionPreparationPlan, CompactionPlanViolation> {
    let source = VerifiedCompactionSource::verified_for_test(
        store_meta(),
        encode_store_meta(&store_meta())?,
        verified_source_parts(omissions),
    )?;
    CompactionPreparationPlan::from_verified_source(source)
}

fn plan_for_store(uuid: [u8; 16]) -> CompactionPlan {
    let meta = StoreMeta {
        store_uuid: StoreUuid::new(uuid).expect("test UUID is nonzero"),
        creation_time_ns: 9,
        bootstrap_id: [0x71; 16],
    };
    let mut entries = inventory_entries();
    for entry in &mut entries {
        match entry {
            SnapshotEntry::Incarnation(incarnation) => {
                incarnation.incarnation = FileIncarnationId::new(meta.store_uuid, 7).expect("nonzero incarnation");
            }
            SnapshotEntry::RetirementTicket(ticket) => {
                ticket.incarnation = FileIncarnationId::new(meta.store_uuid, 7).expect("nonzero incarnation");
            }
            SnapshotEntry::Quarantine(_) => {}
        }
    }
    let parts = VerifiedSourcePartsForTest {
        entries,
        ..verified_source_parts(Vec::new())
    };
    let source = VerifiedCompactionSource::verified_for_test(
        meta.clone(),
        encode_store_meta(&meta).expect("metadata encodes"),
        parts,
    )
    .expect("alternate source is valid");
    let preparation = CompactionPreparationPlan::from_verified_source(source).expect("preparation is valid");
    let receipt = preparation.prepared_receipt_for_test(0x1020_3040);
    CompactionPlan::from_prepared(
        preparation
            .finish_preparation(receipt)
            .expect("prepared foundation is exact"),
    )
    .expect("alternate plan is valid")
}

fn verified_source_parts(omission_evidence: Vec<CleanStartOmissionEvidence>) -> VerifiedSourcePartsForTest {
    VerifiedSourcePartsForTest {
        source_generation: 4,
        marker_epoch: 5,
        marker_anchor_sequence: 3,
        terminal_sequence: 40,
        acknowledgement_epoch: 50,
        sealed_log_length: 1_000,
        entries: inventory_entries(),
        create_high_water: 7,
        ticket_high_water: 9,
        omission_evidence,
        space: ample_space(),
        publication_model: PublicationModel::UnixDirectorySync,
    }
}

fn store_meta() -> StoreMeta {
    StoreMeta {
        store_uuid: store_uuid(),
        creation_time_ns: 1,
        bootstrap_id: [0x5a; 16],
    }
}

fn store_uuid() -> StoreUuid {
    StoreUuid::new([0x33; 16]).expect("test UUID is nonzero")
}

fn inventory_entries() -> Vec<SnapshotEntry> {
    vec![
        SnapshotEntry::RetirementTicket(retirement_entry(9, RetirementStage::NamespaceAbsent, 35)),
        SnapshotEntry::Incarnation(incarnation_entry()),
        SnapshotEntry::Quarantine(QuarantineSnapshotEntry {
            entity_kind: QuarantineEntityKind::Sidecar,
            reason: QuarantineReason::MalformedName,
            sequence_at_observation: 20,
            physical_key: None,
            content_fingerprint: None,
            source_path: path("commitlog/unexpected.bin"),
            destination_path: None,
        }),
        SnapshotEntry::RetirementTicket(retirement_entry(8, RetirementStage::CompletedRetained, 30)),
    ]
}

fn incarnation_entry() -> IncarnationSnapshotEntry {
    IncarnationSnapshotEntry {
        incarnation: incarnation_id(),
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: 1_024,
        create_nonce: [0x20; 16],
        physical_key: Some(PhysicalFileKey::unix(7, 9)),
        canonical_path: canonical_path(),
        create_file_path: path(
            "commitlog/.create.i0000000000000007.s00000000000000000000.n20202020202020202020202020202020",
        ),
    }
}

fn retirement_entry(ticket: u64, stage: RetirementStage, stage_sequence: u64) -> RetirementTicketSnapshotEntry {
    RetirementTicketSnapshotEntry {
        ticket_id: ticket_id(ticket),
        incarnation: incarnation_id(),
        stage,
        superseded_path_observed: false,
        quarantined: false,
        reason: RetirementReason::TtlExpired,
        stage_sequence,
        mapping_generation: 1,
        segment_offset: 0,
        expected_file_length: 1_024,
        retirement_nonce: [0x30; 16],
        target_key: PhysicalFileKey::unix(7, 9),
        canonical_path: canonical_path(),
        tombstone_path: None,
    }
}

fn incarnation_id() -> FileIncarnationId {
    FileIncarnationId::new(store_uuid(), 7).expect("test incarnation is nonzero")
}

fn ticket_id(value: u64) -> TicketId {
    TicketId::new(value).expect("test ticket is nonzero")
}

fn canonical_path() -> StoreRelativePath {
    path("commitlog/00000000000000000000")
}

fn path(value: &str) -> StoreRelativePath {
    StoreRelativePath::new(value).expect("test path is canonical")
}

fn ample_space() -> CompactionSpace {
    CompactionSpace {
        available_bytes: 1 << 30,
        marker_referenced_pair_bytes: [11_000, 22_000],
    }
}

fn decode_unit_record(unit: &PlannedDurableUnit, generation: u64) -> LedgerRecord {
    let DecodeOutcome::Frame(frame) =
        decode_next_frame(unit.frame(), unit.sequence(), generation).expect("planned frame decodes")
    else {
        panic!("planned unit contains one complete frame")
    };
    frame
        .decode_record()
        .expect("planned payload decodes")
        .expect("planned record is known")
}

fn assert_unit_binding(unit: &PlannedDurableUnit, activated: bool, marker_epoch: u64) {
    let AcknowledgementSlotState::Populated(slot) =
        decode_acknowledgement_slot(unit.acknowledgement_slot()).expect("planned ACK slot decodes")
    else {
        panic!("planned ACK slot is populated")
    };
    let seal = decode_commit_seal(unit.seal()).expect("planned seal decodes");
    assert_eq!(slot.activated, activated);
    assert_eq!(slot.marker_epoch, marker_epoch);
    assert_eq!(slot.frame_sequence, unit.sequence());
    assert_eq!(slot.frame_end_offset, unit.frame_end_offset());
    assert_eq!(seal.acknowledgement_epoch, unit.acknowledgement_epoch());
    assert_eq!(seal.frame_crc32, crc32(unit.frame()));
}

fn execute(action: CompactionAction) -> CompactionDecision {
    CompactionDecision::Execute(action)
}

fn crash_boundaries() -> Vec<CompactionCrashBoundary> {
    let mut boundaries = vec![CompactionCrashBoundary::AppendBarrierAcquire];
    for boundary in durable_unit_boundaries() {
        boundaries.push(CompactionCrashBoundary::GenerationPrepared(boundary));
    }
    boundaries.push(CompactionCrashBoundary::SnapshotEncode);
    for boundary in artifact_boundaries() {
        boundaries.push(CompactionCrashBoundary::SnapshotPublication(boundary));
        boundaries.push(CompactionCrashBoundary::LogOpenedPublication(boundary));
    }
    boundaries.push(CompactionCrashBoundary::PlatformDurability);
    boundaries.push(CompactionCrashBoundary::PublishedPairReread);
    for boundary in marker_boundaries() {
        boundaries.push(CompactionCrashBoundary::MarkerSlot(boundary));
    }
    for boundary in durable_unit_boundaries() {
        boundaries.push(CompactionCrashBoundary::LogOpenedCommit(boundary));
        boundaries.push(CompactionCrashBoundary::MarkerCommitted(boundary));
    }
    boundaries.push(CompactionCrashBoundary::Replay);
    boundaries.push(CompactionCrashBoundary::Reconciliation);
    boundaries
}

fn durable_unit_boundaries() -> [DurableUnitCrashBoundary; 9] {
    [
        DurableUnitCrashBoundary::FrameAppend,
        DurableUnitCrashBoundary::FrameSync,
        DurableUnitCrashBoundary::AcknowledgementWrite,
        DurableUnitCrashBoundary::AcknowledgementSync,
        DurableUnitCrashBoundary::AcknowledgementReread,
        DurableUnitCrashBoundary::SealAppend,
        DurableUnitCrashBoundary::SealSync,
        DurableUnitCrashBoundary::SealReread,
        DurableUnitCrashBoundary::EofVerification,
    ]
}

fn artifact_boundaries() -> [ArtifactCrashBoundary; 3] {
    [
        ArtifactCrashBoundary::TemporaryWrite,
        ArtifactCrashBoundary::TemporarySync,
        ArtifactCrashBoundary::NoReplacePublish,
    ]
}

fn marker_boundaries() -> [MarkerCrashBoundary; 3] {
    [
        MarkerCrashBoundary::SlotWrite,
        MarkerCrashBoundary::FileSync,
        MarkerCrashBoundary::SlotReread,
    ]
}
