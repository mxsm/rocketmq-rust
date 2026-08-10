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

use std::collections::BTreeSet;

use super::super::codec::crc32;
use super::super::codec::encode_acknowledgement_slot;
use super::super::codec::encode_commit_seal;
use super::super::codec::encode_ledger_frame;
use super::super::codec::AcknowledgementSlot;
use super::super::codec::CommitSeal;
use super::super::codec::GenerationAbortReason;
use super::super::codec::LedgerRecord;
use super::super::codec::OpenReason;
use super::super::codec::COMMIT_SEAL_LENGTH;
use super::super::identity::TicketId;
use super::super::sidecar::decode_snapshot;
use super::super::sidecar::encode_enabled_marker_slot;
use super::super::sidecar::encode_snapshot;
use super::super::sidecar::EnabledMarkerSlot;
use super::super::sidecar::LifecycleSnapshot;
use super::super::sidecar::RetirementStage;
use super::super::sidecar::SnapshotEntry;
use super::super::sidecar::SnapshotMode;
use super::super::sidecar::StoreMeta;
use super::super::sidecar::STORE_META_LENGTH;
use super::types::*;
use super::validate_prepared_foundation;
use super::CompactionAction;
use super::CompactionStep;

/// Pre-step-2 plan. It cannot construct any target-generation artifact.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CompactionPreparationPlan {
    source: VerifiedCompactionSource,
    binding: PreparationBinding,
    generation_prepared: PlannedDurableUnit,
}

/// Post-step-2 authority. It binds the exact committed prepared unit and resulting full prefix.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PreparedCompactionFoundation {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) meta: StoreMeta,
    pub(super) source_generation: u64,
    pub(super) source_marker_epoch: u64,
    pub(super) source_marker_anchor_sequence: u64,
    pub(super) source_terminal_sequence: u64,
    pub(super) generation_prepared: PlannedDurableUnit,
    pub(super) resulting_prefix_crc32: u32,
    pub(super) retained_snapshot: LifecycleSnapshot,
    pub(super) canonical_retained_snapshot: Vec<u8>,
    pub(super) omission_evidence: Vec<CleanStartOmissionEvidence>,
    pub(super) space: CompactionSpace,
    pub(super) publication_model: PublicationModel,
}

impl CompactionPreparationPlan {
    pub(super) fn from_verified_source(source: VerifiedCompactionSource) -> Result<Self, CompactionPlanError> {
        let target_generation = add_one(source.source_generation, "target generation")?;
        let sequence = add_one(source.terminal_sequence, "GenerationPrepared sequence")?;
        let acknowledgement_epoch = add_one(source.acknowledgement_epoch, "GenerationPrepared acknowledgement epoch")?;
        let generation_prepared = plan_unit(
            &source.meta,
            LedgerRecord::GenerationPrepared {
                store_uuid: source.meta.store_uuid,
                source_generation: source.source_generation,
                target_generation,
                target_snapshot_generation: target_generation,
                open_reason: OpenReason::Compaction,
            },
            sequence,
            source.source_generation,
            source.sealed_log_length,
            acknowledgement_epoch,
            true,
            source.marker_epoch,
        )?;
        let binding = PreparationBinding {
            canonical_store_meta: source.canonical_store_meta,
            store_uuid: source.meta.store_uuid,
            source_generation: source.source_generation,
            marker_epoch: source.marker_epoch,
            marker_anchor_sequence: source.marker_anchor_sequence,
            terminal_sequence: source.terminal_sequence,
            acknowledgement_epoch: source.acknowledgement_epoch,
            sealed_log_length: source.sealed_log_length,
        };
        Ok(Self {
            source,
            binding,
            generation_prepared,
        })
    }

    pub(super) const fn begin(&self) -> CompactionDecision {
        execute(CompactionAction::AcquireAppendBarrier)
    }

    pub(super) fn after_barrier(&self, receipt: AppendBarrierReceipt) -> CompactionDecision {
        if receipt.binding != self.binding {
            return needs_recovery(CompactionStep::AppendBarrier, CompactionRecoveryReason::ReceiptMismatch);
        }
        execute(CompactionAction::CommitGenerationPrepared)
    }

    pub(super) const fn generation_prepared(&self) -> &PlannedDurableUnit {
        &self.generation_prepared
    }

    pub(super) fn finish_preparation(
        self,
        receipt: PreparedUnitReceipt,
    ) -> Result<PreparedCompactionFoundation, CompactionPlanError> {
        if receipt.binding != self.binding {
            return Err(CompactionPlanError::InvalidPreparedReceipt {
                reason: "receipt belongs to another Store frontier",
            });
        }
        if receipt.observed_unit != self.generation_prepared {
            return Err(CompactionPlanError::InvalidPreparedReceipt {
                reason: "reopened frame, acknowledgement slot, seal, or coordinates differ",
            });
        }
        let source = self.source;
        let target_generation = add_one(source.source_generation, "target generation")?;
        let recovered_inventory = LifecycleSnapshot {
            mode: SnapshotMode::OrdinaryCompaction,
            store_uuid: source.meta.store_uuid,
            generation: target_generation,
            log_generation: target_generation,
            predecessor_log_generation: source.source_generation,
            base_sequence: receipt.observed_unit.sequence,
            create_high_water: source.create_high_water,
            ticket_high_water: source.ticket_high_water,
            entries: source.entries,
        };
        let canonical_retained_snapshot = encode_snapshot(&recovered_inventory)?;
        let retained_snapshot = decode_snapshot(&canonical_retained_snapshot)?;
        if encode_snapshot(&retained_snapshot)? != canonical_retained_snapshot {
            return Err(CompactionPlanError::InvalidFoundation {
                reason: "authoritative inventory does not round-trip canonically",
            });
        }
        Ok(PreparedCompactionFoundation {
            canonical_store_meta: source.canonical_store_meta,
            meta: source.meta,
            source_generation: source.source_generation,
            source_marker_epoch: source.marker_epoch,
            source_marker_anchor_sequence: source.marker_anchor_sequence,
            source_terminal_sequence: source.terminal_sequence,
            generation_prepared: receipt.observed_unit,
            resulting_prefix_crc32: receipt.resulting_prefix_crc32,
            retained_snapshot,
            canonical_retained_snapshot,
            omission_evidence: source.omission_evidence,
            space: source.space,
            publication_model: source.publication_model,
        })
    }

    #[cfg(test)]
    pub(super) fn barrier_held_receipt_for_test(&self) -> AppendBarrierReceipt {
        AppendBarrierReceipt {
            binding: self.binding.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn prepared_receipt_for_test(&self, resulting_prefix_crc32: u32) -> PreparedUnitReceipt {
        PreparedUnitReceipt {
            binding: self.binding.clone(),
            observed_unit: self.generation_prepared.clone(),
            resulting_prefix_crc32,
        }
    }
}

/// Byte-exact post-Prepared plan. All phase advances require opaque verifier receipts.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CompactionPlan {
    canonical_store_meta: [u8; STORE_META_LENGTH],
    meta: StoreMeta,
    source_generation: u64,
    source_marker_epoch: u64,
    target_generation: u64,
    target_marker_epoch: u64,
    publication_model: PublicationModel,
    generation_prepared: PlannedDurableUnit,
    snapshot: PlannedCompactionSnapshot,
    log_opened: PlannedDurableUnit,
    marker_slot: PlannedCompactionMarker,
    marker_committed: PlannedDurableUnit,
    target_log_final_length: u64,
    space: SufficientCompactionSpace,
    binding: PlanBinding,
}

impl CompactionPlan {
    pub(super) fn from_prepared(foundation: PreparedCompactionFoundation) -> Result<Self, CompactionPlanError> {
        validate_prepared_foundation(&foundation)?;
        let target_generation = add_one(foundation.source_generation, "target generation")?;
        let target_marker_epoch = add_one(foundation.source_marker_epoch, "target marker epoch")?;
        let prepared_sequence = foundation.generation_prepared.sequence;
        let prepared_acknowledgement_epoch = foundation.generation_prepared.acknowledgement_epoch;
        let snapshot = plan_snapshot(&foundation)?;
        let log_opened_sequence = add_one(prepared_sequence, "LogOpened sequence")?;
        let log_opened_acknowledgement_epoch =
            add_one(prepared_acknowledgement_epoch, "LogOpened acknowledgement epoch")?;
        let log_opened = plan_unit(
            &foundation.meta,
            LedgerRecord::LogOpened {
                store_uuid: foundation.meta.store_uuid,
                generation: target_generation,
                snapshot_generation: target_generation,
                predecessor_log_generation: foundation.source_generation,
                predecessor_terminal_acknowledged_sequence: prepared_sequence,
                snapshot_base_sequence: prepared_sequence,
                snapshot_file_length: snapshot.file_length,
                snapshot_file_crc32: snapshot.file_crc32,
                predecessor_prefix_crc32: foundation.resulting_prefix_crc32,
                validated_prefix_length: foundation.generation_prepared.sealed_log_length,
                unacknowledged_suffix_length: 0,
                unacknowledged_suffix_crc32: 0,
                open_reason: OpenReason::Compaction,
                predecessor_acknowledgement_epoch: prepared_acknowledgement_epoch,
            },
            log_opened_sequence,
            target_generation,
            0,
            log_opened_acknowledgement_epoch,
            true,
            target_marker_epoch,
        )?;
        let marker_slot = plan_marker(
            &foundation.meta,
            target_marker_epoch,
            target_generation,
            log_opened_sequence,
            snapshot.file_length,
            snapshot.file_crc32,
            crc32(&log_opened.frame),
        )?;
        let marker_committed_sequence = add_one(log_opened_sequence, "MarkerCommitted sequence")?;
        let marker_committed_acknowledgement_epoch = add_one(
            log_opened_acknowledgement_epoch,
            "MarkerCommitted acknowledgement epoch",
        )?;
        let marker_committed = plan_unit(
            &foundation.meta,
            marker_committed_record(foundation.meta.store_uuid, &marker_slot),
            marker_committed_sequence,
            target_generation,
            log_opened.sealed_log_length,
            marker_committed_acknowledgement_epoch,
            true,
            target_marker_epoch,
        )?;
        let target_log_final_length = marker_committed.sealed_log_length;
        let space = check_space(foundation.space, snapshot.file_length, target_log_final_length)?;
        add_one(marker_committed_sequence, "next record sequence")?;
        add_one(marker_committed_acknowledgement_epoch, "next acknowledgement epoch")?;
        let binding = PlanBinding {
            canonical_store_meta: foundation.canonical_store_meta,
            store_uuid: foundation.meta.store_uuid,
            source_generation: foundation.source_generation,
            target_generation,
            marker_epoch: target_marker_epoch,
            prepared_sequence,
            prepared_acknowledgement_epoch,
            prepared_sealed_log_length: foundation.generation_prepared.sealed_log_length,
            prepared_prefix_crc32: foundation.resulting_prefix_crc32,
            snapshot_file_length: snapshot.file_length,
            snapshot_crc32: snapshot.file_crc32,
            log_opened_frame_crc32: crc32(&log_opened.frame),
            witness_sequence: marker_committed_sequence,
            witness_acknowledgement_epoch: marker_committed_acknowledgement_epoch,
            witness_sealed_log_length: marker_committed.sealed_log_length,
        };
        Ok(Self {
            canonical_store_meta: foundation.canonical_store_meta,
            meta: foundation.meta,
            source_generation: foundation.source_generation,
            source_marker_epoch: foundation.source_marker_epoch,
            target_generation,
            target_marker_epoch,
            publication_model: foundation.publication_model,
            generation_prepared: foundation.generation_prepared,
            snapshot,
            log_opened,
            marker_slot,
            marker_committed,
            target_log_final_length,
            space,
            binding,
        })
    }

    pub(super) const fn begin(&self) -> CompactionDecision {
        execute(CompactionAction::EncodeCanonicalSnapshot)
    }

    pub(super) fn after_snapshot_encoded(&self, receipt: SnapshotEncodedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::CanonicalSnapshot,
            CompactionAction::PublishSnapshot,
        )
    }

    pub(super) fn after_snapshot_published(&self, receipt: SnapshotPublishedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::PublishSnapshot,
            CompactionAction::PublishLogOpened,
        )
    }

    pub(super) fn after_log_opened_published(&self, receipt: LogOpenedPublishedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::PublishLogOpened,
            CompactionAction::VerifyPublishedPair(self.publication_model),
        )
    }

    pub(super) fn after_pair_verified(&self, receipt: PublishedPairVerifiedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::VerifyPublishedPair,
            CompactionAction::CommitMarkerSlot,
        )
    }

    pub(super) fn after_marker_committed(&self, receipt: MarkerCommittedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::CommitMarkerSlot,
            CompactionAction::CommitLogOpened,
        )
    }

    pub(super) fn after_log_opened_committed(&self, receipt: LogOpenedCommittedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::CommitLogOpened,
            CompactionAction::CommitMarkerWitness,
        )
    }

    pub(super) fn after_marker_witness_committed(&self, receipt: MarkerWitnessCommittedReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::CommitMarkerWitness,
            CompactionAction::ReplaySelectedPair,
        )
    }

    pub(super) fn after_replay(&self, receipt: ReplayReceipt) -> CompactionDecision {
        self.advance(
            receipt,
            CompactionStep::ReplayAndReconcile,
            CompactionAction::ReconcileNamespaceAndIndexes,
        )
    }

    pub(super) fn after_reconciliation(&self, receipt: ReconciliationReceipt) -> CompactionDecision {
        if receipt.binding != self.binding {
            return needs_recovery(
                CompactionStep::ReplayAndReconcile,
                CompactionRecoveryReason::ReceiptMismatch,
            );
        }
        let next_sequence = self.marker_committed.sequence + 1;
        let next_acknowledgement_epoch = self.marker_committed.acknowledgement_epoch + 1;
        CompactionDecision::FencedComplete(FencedCompactionEvidence::new(
            self.meta.store_uuid,
            self.source_generation,
            self.target_generation,
            self.target_marker_epoch,
            self.marker_committed.sequence,
            self.marker_committed.acknowledgement_epoch,
            self.marker_committed.sealed_log_length,
            next_sequence,
            next_acknowledgement_epoch,
        ))
    }

    pub(super) const fn decision_after_failure(&self, boundary: CompactionCrashBoundary) -> CompactionDecision {
        needs_recovery(
            boundary.protocol_step(),
            CompactionRecoveryReason::OperationFailed(boundary),
        )
    }

    pub(super) fn decide_abort(
        &self,
        proof: CompactionAbandonmentProof,
        reason: GenerationAbortReason,
    ) -> CompactionAbortDecision {
        if proof.binding != self.binding {
            return CompactionAbortDecision::FailClosed(AbortDeniedReason::ProofMismatch);
        }
        let record = LedgerRecord::GenerationAborted {
            store_uuid: self.meta.store_uuid,
            source_generation: self.source_generation,
            target_generation: self.target_generation,
            prepared_sequence: self.generation_prepared.sequence,
            abort_reason: reason,
        };
        match plan_unit(
            &self.meta,
            record,
            self.log_opened.sequence,
            self.source_generation,
            self.generation_prepared.sealed_log_length,
            self.log_opened.acknowledgement_epoch,
            true,
            self.source_marker_epoch,
        ) {
            Ok(unit) => CompactionAbortDecision::AppendGenerationAborted(Box::new(unit)),
            Err(_) => CompactionAbortDecision::FailClosed(AbortDeniedReason::ProofMismatch),
        }
    }

    fn advance<const STEP: u8>(
        &self,
        receipt: PhaseReceipt<STEP>,
        failed_step: CompactionStep,
        action: CompactionAction,
    ) -> CompactionDecision {
        if receipt.binding != self.binding {
            return needs_recovery(failed_step, CompactionRecoveryReason::ReceiptMismatch);
        }
        execute(action)
    }

    pub(super) const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub(super) const fn target_generation(&self) -> u64 {
        self.target_generation
    }

    pub(super) const fn target_marker_epoch(&self) -> u64 {
        self.target_marker_epoch
    }

    pub(super) const fn generation_prepared(&self) -> &PlannedDurableUnit {
        &self.generation_prepared
    }

    pub(super) const fn log_opened(&self) -> &PlannedDurableUnit {
        &self.log_opened
    }

    pub(super) const fn marker_committed(&self) -> &PlannedDurableUnit {
        &self.marker_committed
    }

    pub(super) fn snapshot_bytes(&self) -> &[u8] {
        &self.snapshot.encoded
    }

    pub(super) const fn snapshot_base_sequence(&self) -> u64 {
        self.snapshot.base_sequence
    }

    pub(super) const fn snapshot_file_length(&self) -> u64 {
        self.snapshot.file_length
    }

    pub(super) const fn snapshot_file_crc32(&self) -> u32 {
        self.snapshot.file_crc32
    }

    pub(super) fn omitted_completed_tickets(&self) -> &[TicketId] {
        &self.snapshot.omitted_completed_tickets
    }

    pub(super) fn marker_slot_bytes(&self) -> &[u8] {
        &self.marker_slot.encoded
    }

    pub(super) const fn marker_slot_index(&self) -> u8 {
        self.marker_slot.slot.slot_index
    }

    pub(super) const fn marker_slot_stored_crc32(&self) -> u32 {
        self.marker_slot.stored_crc32
    }

    pub(super) const fn target_log_final_length(&self) -> u64 {
        self.target_log_final_length
    }

    pub(super) const fn space(&self) -> SufficientCompactionSpace {
        self.space
    }

    #[cfg(test)]
    fn receipt_for_test<const STEP: u8>(&self) -> PhaseReceipt<STEP> {
        PhaseReceipt {
            binding: self.binding.clone(),
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_encoded_receipt_for_test(&self) -> SnapshotEncodedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn snapshot_published_receipt_for_test(&self) -> SnapshotPublishedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn log_opened_published_receipt_for_test(&self) -> LogOpenedPublishedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn pair_verified_receipt_for_test(&self) -> PublishedPairVerifiedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn marker_committed_receipt_for_test(&self) -> MarkerCommittedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn log_opened_committed_receipt_for_test(&self) -> LogOpenedCommittedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn marker_witness_committed_receipt_for_test(&self) -> MarkerWitnessCommittedReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn replay_receipt_for_test(&self) -> ReplayReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn reconciliation_receipt_for_test(&self) -> ReconciliationReceipt {
        self.receipt_for_test()
    }

    #[cfg(test)]
    pub(super) fn complete_abandonment_scan_for_test(&self) -> AbandonmentScanForTest {
        AbandonmentScanForTest {
            prepared_frame: self.generation_prepared.frame.clone(),
            prepared_acknowledgement_slot: self.generation_prepared.acknowledgement_slot,
            prepared_seal: self.generation_prepared.seal,
            marker_slots_reconstructed: true,
            acknowledgement_slots_reconstructed: true,
            no_target_marker: true,
            no_target_acknowledgement_or_seal: true,
            candidate_exact_or_absent: true,
            complete_higher_generation_inventory: true,
            no_gaps_or_unexplained_artifacts: true,
        }
    }

    #[cfg(test)]
    pub(super) fn verify_abandonment_for_test(
        &self,
        scan: AbandonmentScanForTest,
    ) -> Result<CompactionAbandonmentProof, AbortDeniedReason> {
        if scan.prepared_frame != self.generation_prepared.frame
            || scan.prepared_acknowledgement_slot != self.generation_prepared.acknowledgement_slot
            || scan.prepared_seal != self.generation_prepared.seal
        {
            return Err(AbortDeniedReason::PreparedUnitMismatch);
        }
        if !scan.marker_slots_reconstructed || !scan.acknowledgement_slots_reconstructed {
            return Err(AbortDeniedReason::IncompleteSlotReconstruction);
        }
        if !scan.no_target_marker {
            return Err(AbortDeniedReason::TargetMayBeSelected);
        }
        if !scan.no_target_acknowledgement_or_seal {
            return Err(AbortDeniedReason::TargetMayBeAcknowledged);
        }
        if !scan.candidate_exact_or_absent {
            return Err(AbortDeniedReason::AmbiguousCandidate);
        }
        if !scan.complete_higher_generation_inventory || !scan.no_gaps_or_unexplained_artifacts {
            return Err(AbortDeniedReason::IncompleteHigherGenerationScan);
        }
        Ok(CompactionAbandonmentProof {
            binding: self.binding.clone(),
        })
    }
}

fn plan_snapshot(foundation: &PreparedCompactionFoundation) -> Result<PlannedCompactionSnapshot, CompactionPlanError> {
    let mut eligible_tickets = BTreeSet::new();
    let mut eligible_incarnations = BTreeSet::new();
    for evidence in &foundation.omission_evidence {
        let expected_binding = canonical_completed_binding(
            &foundation.meta,
            evidence.selected_generation,
            evidence.replayed_through_sequence,
            &evidence.completed_entry,
        )?;
        if evidence.canonical_store_meta != foundation.canonical_store_meta
            || evidence.selected_generation != foundation.source_generation
            || evidence.marker_epoch != foundation.source_marker_epoch
            || evidence.marker_anchor_sequence != foundation.source_marker_anchor_sequence
            || evidence.replayed_through_sequence == 0
            || evidence.replayed_through_sequence > foundation.source_terminal_sequence
            || evidence.completed_entry.stage != RetirementStage::CompletedRetained
            || evidence.completed_entry.stage_sequence > evidence.replayed_through_sequence
            || evidence.canonical_entry_binding != expected_binding
            || evidence.entry_binding_crc32 != crc32(&expected_binding)
            || !eligible_tickets.insert(evidence.completed_entry.ticket_id)
        {
            return Err(CompactionPlanError::InvalidOmissionEvidence {
                reason: "proof is stale, duplicated, transferred, or bound to another Store frontier",
            });
        }
        if !foundation.retained_snapshot.entries.iter().any(
            |entry| matches!(entry, SnapshotEntry::RetirementTicket(ticket) if ticket == &evidence.completed_entry),
        ) {
            return Err(CompactionPlanError::InvalidOmissionEvidence {
                reason: "proof does not equal the complete retained ticket entry",
            });
        }
        if !foundation.retained_snapshot.entries.iter().any(
            |entry| matches!(entry, SnapshotEntry::Incarnation(incarnation) if incarnation.incarnation == evidence.completed_entry.incarnation),
        ) || foundation.retained_snapshot.entries.iter().any(
            |entry| matches!(entry, SnapshotEntry::RetirementTicket(ticket) if ticket.ticket_id != evidence.completed_entry.ticket_id && ticket.incarnation == evidence.completed_entry.incarnation),
        ) || !eligible_incarnations.insert(evidence.completed_entry.incarnation)
        {
            return Err(CompactionPlanError::InvalidOmissionEvidence {
                reason: "proof does not identify a completed ticket and its sole-reference incarnation",
            });
        }
    }
    let entries = foundation
        .retained_snapshot
        .entries
        .iter()
        .filter(|entry| {
            !matches!(
                entry,
                SnapshotEntry::RetirementTicket(ticket)
                    if ticket.stage == RetirementStage::CompletedRetained
                        && eligible_tickets.contains(&ticket.ticket_id)
            ) && !matches!(
                entry,
                SnapshotEntry::Incarnation(incarnation)
                    if eligible_incarnations.contains(&incarnation.incarnation)
            )
        })
        .cloned()
        .collect();
    let snapshot = LifecycleSnapshot {
        entries,
        ..foundation.retained_snapshot.clone()
    };
    let encoded = encode_snapshot(&snapshot)?;
    let file_length = u64::try_from(encoded.len()).map_err(|_| CompactionPlanError::ArithmeticOverflow {
        field: "snapshot length",
    })?;
    Ok(PlannedCompactionSnapshot {
        file_crc32: crc32(&encoded),
        encoded,
        base_sequence: snapshot.base_sequence,
        file_length,
        omitted_completed_tickets: eligible_tickets.into_iter().collect(),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the planned unit mirrors the persisted frame, acknowledgement, and seal bindings"
)]
fn plan_unit(
    meta: &StoreMeta,
    record: LedgerRecord,
    sequence: u64,
    log_generation: u64,
    frame_start_offset: u64,
    acknowledgement_epoch: u64,
    activated: bool,
    marker_epoch: u64,
) -> Result<PlannedDurableUnit, CompactionPlanError> {
    let frame = encode_ledger_frame(&record, sequence, log_generation)?;
    let frame_length =
        u64::try_from(frame.len()).map_err(|_| CompactionPlanError::ArithmeticOverflow { field: "frame length" })?;
    let frame_end_offset =
        frame_start_offset
            .checked_add(frame_length)
            .ok_or(CompactionPlanError::ArithmeticOverflow {
                field: "frame end offset",
            })?;
    let slot_index = ((acknowledgement_epoch
        .checked_sub(1)
        .ok_or(CompactionPlanError::InvalidFoundation {
            reason: "acknowledgement epoch is zero",
        })?)
        & 1) as u8;
    let slot = AcknowledgementSlot {
        slot_index,
        activated,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        acknowledgement_epoch,
        marker_epoch,
        log_generation,
        frame_sequence: sequence,
        frame_end_offset,
        frame_crc32: crc32(&frame),
    };
    let acknowledgement_slot = encode_acknowledgement_slot(&slot)?;
    let seal = encode_commit_seal(&CommitSeal::from_acknowledgement_slot(&slot, &acknowledgement_slot)?)?;
    let sealed_log_length =
        frame_end_offset
            .checked_add(COMMIT_SEAL_LENGTH as u64)
            .ok_or(CompactionPlanError::ArithmeticOverflow {
                field: "sealed log length",
            })?;
    Ok(PlannedDurableUnit {
        record,
        frame,
        acknowledgement_slot,
        seal,
        sequence,
        acknowledgement_epoch,
        frame_start_offset,
        frame_end_offset,
        sealed_log_length,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the marker plan mirrors the seven independent persisted binding fields"
)]
fn plan_marker(
    meta: &StoreMeta,
    marker_epoch: u64,
    generation: u64,
    anchor_sequence: u64,
    snapshot_file_length: u64,
    snapshot_file_crc32: u32,
    anchor_frame_crc32: u32,
) -> Result<PlannedCompactionMarker, CompactionPlanError> {
    let slot_index = ((marker_epoch
        .checked_sub(1)
        .ok_or(CompactionPlanError::InvalidFoundation {
            reason: "target marker epoch is zero",
        })?)
        & 1) as u8;
    let slot = EnabledMarkerSlot {
        slot_index,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        marker_epoch,
        snapshot_generation: generation,
        log_generation: generation,
        anchor_sequence,
        snapshot_file_length,
        snapshot_file_crc32,
        anchor_frame_crc32,
    };
    let encoded = encode_enabled_marker_slot(&slot)?;
    let stored_crc32 = u32::from_le_bytes([encoded[100], encoded[101], encoded[102], encoded[103]]);
    Ok(PlannedCompactionMarker {
        slot,
        encoded,
        stored_crc32,
    })
}

fn marker_committed_record(
    store_uuid: super::super::identity::StoreUuid,
    marker: &PlannedCompactionMarker,
) -> LedgerRecord {
    LedgerRecord::MarkerCommitted {
        store_uuid,
        marker_epoch: marker.slot.marker_epoch,
        snapshot_generation: marker.slot.snapshot_generation,
        log_generation: marker.slot.log_generation,
        anchor_sequence: marker.slot.anchor_sequence,
        slot_index: marker.slot.slot_index,
        slot_crc32: marker.stored_crc32,
    }
}

const fn execute(action: CompactionAction) -> CompactionDecision {
    CompactionDecision::Execute(action)
}

const fn needs_recovery(step: CompactionStep, reason: CompactionRecoveryReason) -> CompactionDecision {
    CompactionDecision::NeedsRecovery(CompactionNeedsRecovery { step, reason })
}
