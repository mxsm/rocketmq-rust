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
use crate::mapped_file::retirement::codec::OpenReason;

#[test]
fn generation_switch_consumes_the_exact_published_anchor_with_continuous_sequence_and_ack_epochs() {
    let plan = switch_plan();
    assert_eq!(plan.log_opened.sequence, 21);
    assert_eq!(plan.log_opened.acknowledgement_epoch, 21);
    assert_eq!(plan.log_opened.frame_start_offset, 0);
    assert_eq!(plan.log_opened.frame_end_offset, 148);
    assert_eq!(plan.log_opened.sealed_log_length, 220);
    assert!(matches!(
        decode_record(&plan.log_opened, 1),
        LedgerRecord::LogOpened {
            generation: 1,
            snapshot_generation: 1,
            predecessor_log_generation: 0,
            predecessor_terminal_acknowledged_sequence: 20,
            predecessor_acknowledgement_epoch: 20,
            open_reason: OpenReason::Compaction,
            ..
        }
    ));
    assert_eq!(plan.marker_slot.slot.slot_index, 1);
    assert_eq!(plan.marker_slot.slot.marker_epoch, 2);
    assert_eq!(plan.marker_slot.slot.anchor_sequence, 21);
    assert_eq!(plan.marker_slot.slot.anchor_frame_crc32, crc32(&plan.log_opened.frame));
    assert_eq!(plan.marker_committed.sequence, 22);
    assert_eq!(plan.marker_committed.acknowledgement_epoch, 22);
    assert_eq!(plan.marker_committed.frame_start_offset, 220);
    assert_eq!(plan.marker_committed.frame_end_offset, 320);
    assert_eq!(plan.marker_committed.sealed_log_length, 392);
    assert_slot_and_seal(&plan.log_opened, 0, true, 2);
    assert_slot_and_seal(&plan.marker_committed, 1, true, 2);
}

#[test]
fn published_log_opened_must_be_synced_before_marker_and_can_never_be_recreated_by_the_plan() {
    let plan = switch_plan();
    for progress in [
        DurableUnitProgress::Missing,
        DurableUnitProgress::ExactFramePrefix,
        DurableUnitProgress::FrameWritten,
    ] {
        assert_eq!(
            plan.decide(GenerationSwitchProgress::LogOpenedBeforeMarker(progress)),
            BootstrapDecision::NeedsRecovery(NeedsRecovery {
                flow: BootstrapFlow::GenerationSwitch,
                checkpoint: BootstrapCheckpoint::LogOpened,
                reason: BootstrapRecoveryReason::FoundationAnchorNotDurable,
            })
        );
    }
    assert_eq!(
        plan.decide(GenerationSwitchProgress::LogOpenedBeforeMarker(
            DurableUnitProgress::FrameSynced,
        )),
        BootstrapDecision::Execute(BootstrapAction::AdvanceMarker {
            step: MarkerSlotStep::WriteInactiveSlot,
        })
    );
    for progress in [
        DurableUnitProgress::AcknowledgementWritten,
        DurableUnitProgress::AcknowledgementSynced,
        DurableUnitProgress::AcknowledgementVerified,
        DurableUnitProgress::ExactSealPrefix,
        DurableUnitProgress::SealWritten,
        DurableUnitProgress::SealSynced,
        DurableUnitProgress::Committed,
    ] {
        assert_eq!(
            plan.decide(GenerationSwitchProgress::LogOpenedBeforeMarker(progress)),
            BootstrapDecision::NeedsRecovery(NeedsRecovery {
                flow: BootstrapFlow::GenerationSwitch,
                checkpoint: BootstrapCheckpoint::LogOpened,
                reason: BootstrapRecoveryReason::AnchorAcknowledgedBeforeMarker,
            })
        );
    }
}

#[test]
fn generation_switch_marker_ack_seal_witness_and_final_fence_cover_every_frontier() {
    let plan = switch_plan();
    for (progress, step) in marker_progress_steps() {
        assert_eq!(
            plan.decide(GenerationSwitchProgress::MarkerSlot(progress)),
            BootstrapDecision::Execute(BootstrapAction::AdvanceMarker { step })
        );
    }
    assert_eq!(
        plan.decide(GenerationSwitchProgress::MarkerSlot(MarkerSlotProgress::Verified)),
        execute_unit(BootstrapRecord::LogOpened, DurableUnitStep::WriteAcknowledgementSlot)
    );

    for progress in [
        DurableUnitProgress::Missing,
        DurableUnitProgress::ExactFramePrefix,
        DurableUnitProgress::FrameWritten,
    ] {
        assert_eq!(
            plan.decide(GenerationSwitchProgress::LogOpenedAfterMarker(progress)),
            BootstrapDecision::NeedsRecovery(NeedsRecovery {
                flow: BootstrapFlow::GenerationSwitch,
                checkpoint: BootstrapCheckpoint::LogOpened,
                reason: BootstrapRecoveryReason::AnchorMissingAfterMarker,
            })
        );
    }
    for (progress, step) in unit_progress_steps().into_iter().skip(3) {
        assert_eq!(
            plan.decide(GenerationSwitchProgress::LogOpenedAfterMarker(progress)),
            execute_unit(BootstrapRecord::LogOpened, step)
        );
    }
    assert_eq!(
        plan.decide(GenerationSwitchProgress::LogOpenedAfterMarker(
            DurableUnitProgress::Committed,
        )),
        execute_unit(BootstrapRecord::MarkerCommitted, DurableUnitStep::AppendFrame)
    );
    for (progress, step) in unit_progress_steps() {
        assert_eq!(
            plan.decide(GenerationSwitchProgress::MarkerCommitted(progress)),
            execute_unit(BootstrapRecord::MarkerCommitted, step)
        );
    }
    assert_eq!(
        plan.decide(GenerationSwitchProgress::MarkerCommitted(
            DurableUnitProgress::Committed,
        )),
        BootstrapDecision::Execute(BootstrapAction::Reconcile {
            phase: ReconciliationPhase::AfterMarkerWitness,
        })
    );
    let BootstrapDecision::FencedComplete(evidence) = plan.decide(GenerationSwitchProgress::PostWitnessReconciled)
    else {
        panic!("generation switch must remain fenced");
    };
    assert_eq!(evidence.flow, BootstrapFlow::GenerationSwitch);
    assert_eq!(evidence.log_generation, 1);
    assert_eq!(evidence.marker_epoch, 2);
    assert_eq!(evidence.witness_sequence, 22);
    assert_eq!(evidence.acknowledgement_epoch, 22);
}

#[test]
fn generation_switch_constructor_surface_requires_an_opaque_foundation() {
    let constructor: fn(GenerationSwitchFoundationEvidence) -> Result<GenerationSwitchPlan, BootstrapPlanViolation> =
        GenerationSwitchPlan::new;
    assert!(constructor(compaction_proof()).is_ok());
}

#[test]
fn compaction_proof_rejects_discontinuous_or_forged_authoritative_predecessor_fields() {
    let mut discontinuous = compaction_proof();
    discontinuous.common_mut_for_test().predecessor_acknowledgement_epoch = 19;
    assert!(matches!(
        GenerationSwitchPlan::new(discontinuous),
        Err(BootstrapPlanViolation::InvalidGenerationSwitch { .. })
    ));

    let mut forged_prepared = compaction_proof();
    let GenerationSwitchFoundationEvidence::Compaction(evidence) = &mut forged_prepared else {
        panic!("helper returns compaction evidence");
    };
    evidence.prepared.sealed_log_length = 4095;
    assert!(matches!(
        GenerationSwitchPlan::new(forged_prepared),
        Err(BootstrapPlanViolation::InvalidGenerationSwitch { .. })
    ));

    let mut forged_frame = compaction_proof();
    forged_frame.common_mut_for_test().canonical_log_opened_frame[0] ^= 1;
    assert!(matches!(
        GenerationSwitchPlan::new(forged_frame),
        Err(BootstrapPlanViolation::InvalidGenerationSwitch { .. })
    ));
}

#[test]
fn tail_repair_proof_binds_the_exact_suffix_and_rejects_tampering() {
    let suffix = vec![0x11, 0x22, 0x33, 0x44];
    let plan = GenerationSwitchPlan::new(tail_proof(suffix.clone())).expect("tail proof is complete");
    assert!(matches!(
        decode_record(&plan.log_opened, 1),
        LedgerRecord::LogOpened {
            open_reason: OpenReason::TailRepair,
            unacknowledged_suffix_length: 4,
            unacknowledged_suffix_crc32,
            predecessor_acknowledgement_epoch: 20,
            ..
        } if unacknowledged_suffix_crc32 == crc32(&suffix)
    ));

    let mut forged = tail_proof(suffix);
    let GenerationSwitchFoundationEvidence::TailRepair(evidence) = &mut forged else {
        panic!("helper returns tail evidence");
    };
    evidence.tail.suffix[0] ^= 1;
    assert!(matches!(
        GenerationSwitchPlan::new(forged),
        Err(BootstrapPlanViolation::InvalidGenerationSwitch { .. })
    ));
}
