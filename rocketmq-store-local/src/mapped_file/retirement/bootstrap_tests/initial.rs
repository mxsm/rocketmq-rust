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

use super::support::*;
use super::*;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::sidecar::decode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::ENABLED_MARKER_FILE_LENGTH;
use crate::mapped_file::retirement::sidecar::ENABLED_MARKER_SLOT_LENGTH;

#[test]
fn foundation_owns_canonical_store_meta_and_store_initialized_derives_every_identity_field() {
    let meta = store_meta();
    let expected_bytes = encode_store_meta(&meta).expect("meta encodes");
    let evidence = foundation(&meta);
    assert_eq!(evidence.store_meta.canonical_bytes, expected_bytes);
    assert_eq!(
        evidence.store_meta.stored_crc32,
        u32::from_le_bytes(expected_bytes[60..64].try_into().expect("CRC field"))
    );

    let plan = InitialBootstrapPlan::new(evidence).expect("proof creates the first phase");
    assert_eq!(plan.store_initialized.sequence, 1);
    assert_eq!(plan.store_initialized.acknowledgement_epoch, 1);
    assert_eq!(plan.store_initialized.frame_start_offset, 0);
    assert_eq!(plan.store_initialized.frame_end_offset, 108);
    assert_eq!(plan.store_initialized.sealed_log_length, 180);
    assert!(matches!(
        decode_record(&plan.store_initialized, 0),
        LedgerRecord::StoreInitialized {
            store_uuid,
            bootstrap_id,
            creation_time_ns: 7,
        } if store_uuid == meta.store_uuid && bootstrap_id == meta.bootstrap_id
    ));
    assert_slot_and_seal(&plan.store_initialized, 0, false, 0);
}

#[test]
fn foundation_rejects_creation_time_mismatch_instead_of_accepting_separate_raw_meta() {
    let encoded_meta = encode_store_meta(&store_meta()).expect("meta encodes");
    let mut mismatched = store_meta();
    mismatched.creation_time_ns = 8;

    assert_eq!(
        BootstrapFoundationEvidence::from_bytes_for_test(encoded_meta, &mismatched),
        Err(BootstrapPlanViolation::FoundationStoreMetaMismatch)
    );
}

#[test]
fn store_initialized_must_commit_before_the_inventory_proof_can_be_consumed() {
    let constructor: fn(BootstrapFoundationEvidence) -> Result<InitialBootstrapPlan, BootstrapPlanViolation> =
        InitialBootstrapPlan::new;
    let transition: fn(
        InitialBootstrapPlan,
        DurableUnitProgress,
        BootstrapInventoryEvidence,
    ) -> Result<InitialBootstrapInventoryPlan, BootstrapPlanViolation> = InitialBootstrapPlan::consume_inventory;
    assert!(constructor(foundation(&store_meta())).is_ok());

    let plan = initial_store_plan();
    for (progress, step) in unit_progress_steps() {
        assert_eq!(
            plan.decide_store_initialized(progress),
            execute_unit(BootstrapRecord::StoreInitialized, step)
        );
    }
    assert_eq!(
        plan.decide_store_initialized(DurableUnitProgress::Committed),
        BootstrapDecision::RequireBootstrapInventory
    );

    let error = transition(
        initial_store_plan(),
        DurableUnitProgress::SealSynced,
        inventory(&bootstrap_snapshot()),
    )
    .expect_err("an uncommitted StoreInitialized cannot cross the type-state boundary");
    assert_eq!(error, BootstrapPlanViolation::StoreInitializedNotDurable);
}

#[test]
fn inventory_proof_binds_canonical_snapshot_counts_highwaters_and_all_later_units() {
    let snapshot_evidence = inventory(&bootstrap_snapshot());
    assert_eq!(snapshot_evidence.inventory_count, 0);
    assert_eq!(
        snapshot_evidence.snapshot_crc32,
        crc32(&snapshot_evidence.canonical_snapshot)
    );
    let plan = initial_store_plan()
        .consume_inventory(DurableUnitProgress::Committed, snapshot_evidence)
        .expect("durable StoreInitialized accepts the inventory proof");

    assert_eq!(plan.snapshot.encoded.len(), 108);
    assert_eq!(plan.snapshot.file_crc32, crc32(&plan.snapshot.encoded));
    assert_eq!(plan.snapshot.inventory_count, 0);
    assert_eq!(plan.bootstrap_installed.sequence, 2);
    assert_eq!(plan.bootstrap_installed.acknowledgement_epoch, 2);
    assert_eq!(plan.bootstrap_installed.frame_start_offset, 180);
    assert_eq!(plan.bootstrap_installed.frame_end_offset, 312);
    assert_eq!(plan.bootstrap_installed.sealed_log_length, 384);
    assert!(matches!(
        decode_record(&plan.bootstrap_installed, 0),
        LedgerRecord::BootstrapInstalled {
            snapshot_generation: 0,
            snapshot_base_sequence: 1,
            snapshot_file_length: 108,
            inventory_count: 0,
            create_high_water: 0,
            ticket_high_water: 0,
            ..
        }
    ));

    assert_eq!(plan.initial_marker.encoded_file.len(), ENABLED_MARKER_FILE_LENGTH);
    let encoded_slot0 = encode_enabled_marker_slot(&plan.initial_marker.slot0).expect("slot 0 encodes");
    assert_eq!(
        &plan.initial_marker.encoded_file[..ENABLED_MARKER_SLOT_LENGTH],
        encoded_slot0.as_slice()
    );
    assert!(plan.initial_marker.encoded_file[ENABLED_MARKER_SLOT_LENGTH..]
        .iter()
        .all(|byte| *byte == 0));
    assert_eq!(plan.initial_marker.file_crc32, crc32(&plan.initial_marker.encoded_file));
    let marker_file = decode_enabled_marker_file(&plan.initial_marker.encoded_file).expect("entire marker decodes");
    assert_eq!(marker_file.slots[0].as_ref(), Some(&plan.initial_marker.slot0));
    assert_eq!(marker_file.slots[1], None);
    assert_eq!(plan.initial_marker.slot0.marker_epoch, 1);
    assert_eq!(plan.initial_marker.slot0.anchor_sequence, 2);
    assert_eq!(
        plan.initial_marker.slot0.anchor_frame_crc32,
        crc32(&plan.bootstrap_installed.frame)
    );
    assert_eq!(plan.marker_committed.sequence, 3);
    assert_eq!(plan.marker_committed.acknowledgement_epoch, 3);
    assert_eq!(plan.marker_committed.frame_start_offset, 384);
    assert_eq!(plan.marker_committed.frame_end_offset, 484);
    assert_eq!(plan.marker_committed.sealed_log_length, 556);
    assert!(matches!(
        decode_record(&plan.marker_committed, 0),
        LedgerRecord::MarkerCommitted {
            marker_epoch: 1,
            anchor_sequence: 2,
            slot_index: 0,
            slot_crc32,
            ..
        } if slot_crc32 == plan.initial_marker.slot0_stored_crc32
    ));
    assert_slot_and_seal(&plan.bootstrap_installed, 1, false, 0);
    assert_slot_and_seal(&plan.marker_committed, 0, true, 1);
}

#[test]
fn inventory_phase_covers_every_snapshot_unit_marker_and_reconciliation_frontier() {
    let plan = initial_inventory_plan();
    for (progress, step) in [
        (
            ImmutableArtifactProgress::Missing,
            ImmutableArtifactStep::WriteTemporary,
        ),
        (
            ImmutableArtifactProgress::TemporaryWritten,
            ImmutableArtifactStep::SyncTemporary,
        ),
        (
            ImmutableArtifactProgress::TemporarySynced,
            ImmutableArtifactStep::PublishFinalNoReplace,
        ),
        (
            ImmutableArtifactProgress::Published,
            ImmutableArtifactStep::ReopenAndVerify,
        ),
    ] {
        assert_eq!(
            plan.decide(InitialBootstrapProgress::BootstrapSnapshot(progress)),
            BootstrapDecision::Execute(BootstrapAction::AdvanceSnapshot { step })
        );
    }
    assert_eq!(
        plan.decide(InitialBootstrapProgress::BootstrapSnapshot(
            ImmutableArtifactProgress::Verified,
        )),
        execute_unit(BootstrapRecord::BootstrapInstalled, DurableUnitStep::AppendFrame)
    );
    for (progress, step) in unit_progress_steps() {
        assert_eq!(
            plan.decide(InitialBootstrapProgress::BootstrapInstalled(progress)),
            execute_unit(BootstrapRecord::BootstrapInstalled, step)
        );
    }
    assert_eq!(
        plan.decide(InitialBootstrapProgress::BootstrapInstalled(
            DurableUnitProgress::Committed,
        )),
        BootstrapDecision::Execute(BootstrapAction::Reconcile {
            phase: ReconciliationPhase::BeforeMarker,
        })
    );
    assert_eq!(
        plan.decide(InitialBootstrapProgress::PreMarkerReconciled),
        BootstrapDecision::Execute(BootstrapAction::AdvanceInitialMarker {
            step: InitialMarkerStep::WriteTemporary,
        })
    );
    for (progress, step) in initial_marker_progress_steps() {
        assert_eq!(
            plan.decide(InitialBootstrapProgress::InitialMarker(progress)),
            BootstrapDecision::Execute(BootstrapAction::AdvanceInitialMarker { step })
        );
    }
    let verification = InitialMarkerVerificationEvidence::verified_for_test(&plan.initial_marker);
    assert_eq!(
        plan.decide(InitialBootstrapProgress::InitialMarker(
            InitialMarkerProgress::Verified(Box::new(verification)),
        )),
        execute_unit(BootstrapRecord::MarkerCommitted, DurableUnitStep::AppendFrame)
    );
    for (progress, step) in unit_progress_steps() {
        assert_eq!(
            plan.decide(InitialBootstrapProgress::MarkerCommitted(progress)),
            execute_unit(BootstrapRecord::MarkerCommitted, step)
        );
    }
    assert_eq!(
        plan.decide(InitialBootstrapProgress::MarkerCommitted(
            DurableUnitProgress::Committed,
        )),
        BootstrapDecision::Execute(BootstrapAction::Reconcile {
            phase: ReconciliationPhase::AfterMarkerWitness,
        })
    );
}

#[test]
fn initial_marker_creation_has_distinct_crash_and_ambiguity_recovery_evidence() {
    for boundary in [
        BootstrapCrashBoundary::InitialMarkerTemporaryWrite,
        BootstrapCrashBoundary::InitialMarkerTemporarySync,
        BootstrapCrashBoundary::InitialMarkerPublish,
        BootstrapCrashBoundary::InitialMarkerDirectorySync,
        BootstrapCrashBoundary::InitialMarkerReopen,
    ] {
        assert_eq!(
            decision_after_failure(BootstrapFlow::Initial, BootstrapCheckpoint::InitialMarker, boundary),
            BootstrapDecision::NeedsRecovery(NeedsRecovery {
                flow: BootstrapFlow::Initial,
                checkpoint: BootstrapCheckpoint::InitialMarker,
                reason: BootstrapRecoveryReason::OperationFailed(boundary),
            })
        );
    }
    assert_eq!(
        initial_inventory_plan().decide(InitialBootstrapProgress::Ambiguous {
            checkpoint: BootstrapCheckpoint::InitialMarker,
            evidence: BootstrapAmbiguity::InitialMarkerArtifact,
        }),
        BootstrapDecision::NeedsRecovery(NeedsRecovery {
            flow: BootstrapFlow::Initial,
            checkpoint: BootstrapCheckpoint::InitialMarker,
            reason: BootstrapRecoveryReason::AmbiguousPersistedState(BootstrapAmbiguity::InitialMarkerArtifact,),
        })
    );

    let plan = initial_inventory_plan();
    let mismatched = InitialMarkerVerificationEvidence::mismatched_for_test(&plan.initial_marker);
    assert_eq!(
        plan.decide(InitialBootstrapProgress::InitialMarker(
            InitialMarkerProgress::Verified(Box::new(mismatched)),
        )),
        BootstrapDecision::NeedsRecovery(NeedsRecovery {
            flow: BootstrapFlow::Initial,
            checkpoint: BootstrapCheckpoint::InitialMarker,
            reason: BootstrapRecoveryReason::AmbiguousPersistedState(BootstrapAmbiguity::InitialMarkerArtifact,),
        })
    );
}

#[test]
fn late_initial_progress_requires_the_inventory_type_state_and_completion_remains_fenced() {
    let plan = initial_inventory_plan();
    let BootstrapDecision::FencedComplete(evidence) = plan.decide(InitialBootstrapProgress::PostWitnessReconciled)
    else {
        panic!("the final decision must remain fenced");
    };
    assert_eq!(evidence.flow, BootstrapFlow::Initial);
    assert_eq!(evidence.store_uuid, store_uuid());
    assert_eq!(evidence.log_generation, 0);
    assert_eq!(evidence.marker_epoch, 1);
    assert_eq!(evidence.witness_sequence, 3);
    assert_eq!(evidence.acknowledgement_epoch, 3);
    assert_eq!(evidence.sealed_log_length, 556);
}

#[test]
fn forged_inventory_metadata_and_identity_never_create_the_late_phase() {
    let mut count_mismatch = inventory(&bootstrap_snapshot());
    count_mismatch.inventory_count = 1;
    assert!(matches!(
        initial_store_plan().consume_inventory(DurableUnitProgress::Committed, count_mismatch),
        Err(BootstrapPlanViolation::InvalidSnapshot { .. })
    ));

    let mut identity_mismatch = inventory(&bootstrap_snapshot());
    identity_mismatch.store_uuid = StoreUuid::new([9; 16]).expect("UUID is nonzero");
    assert_eq!(
        initial_store_plan().consume_inventory(DurableUnitProgress::Committed, identity_mismatch),
        Err(BootstrapPlanViolation::FoundationIdentityMismatch)
    );
}

#[test]
fn every_crash_boundary_and_ambiguous_frontier_requires_typed_recovery() {
    for flow in [BootstrapFlow::Initial, BootstrapFlow::GenerationSwitch] {
        for boundary in crash_boundaries() {
            assert_eq!(
                decision_after_failure(flow, BootstrapCheckpoint::MarkerSlot, boundary),
                BootstrapDecision::NeedsRecovery(NeedsRecovery {
                    flow,
                    checkpoint: BootstrapCheckpoint::MarkerSlot,
                    reason: BootstrapRecoveryReason::OperationFailed(boundary),
                })
            );
        }
    }
    for ambiguity in ambiguities() {
        assert_eq!(
            initial_inventory_plan().decide(InitialBootstrapProgress::Ambiguous {
                checkpoint: BootstrapCheckpoint::BootstrapInstalled,
                evidence: ambiguity,
            }),
            BootstrapDecision::NeedsRecovery(NeedsRecovery {
                flow: BootstrapFlow::Initial,
                checkpoint: BootstrapCheckpoint::BootstrapInstalled,
                reason: BootstrapRecoveryReason::AmbiguousPersistedState(ambiguity),
            })
        );
        assert_eq!(
            switch_plan().decide(GenerationSwitchProgress::Ambiguous {
                checkpoint: BootstrapCheckpoint::MarkerCommitted,
                evidence: ambiguity,
            }),
            BootstrapDecision::NeedsRecovery(NeedsRecovery {
                flow: BootstrapFlow::GenerationSwitch,
                checkpoint: BootstrapCheckpoint::MarkerCommitted,
                reason: BootstrapRecoveryReason::AmbiguousPersistedState(ambiguity),
            })
        );
    }
}
