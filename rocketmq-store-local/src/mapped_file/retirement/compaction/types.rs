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

use thiserror::Error;

#[cfg(test)]
use super::super::codec::crc32;
use super::super::codec::CodecViolation;
use super::super::codec::LedgerRecord;
use super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use super::super::codec::COMMIT_SEAL_LENGTH;
use super::super::identity::StoreUuid;
use super::super::identity::TicketId;
use super::super::sidecar::decode_store_meta;
use super::super::sidecar::encode_snapshot;
use super::super::sidecar::encode_store_meta;
use super::super::sidecar::EnabledMarkerSlot;
use super::super::sidecar::LifecycleSnapshot;
use super::super::sidecar::RetirementStage;
use super::super::sidecar::RetirementTicketSnapshotEntry;
use super::super::sidecar::SidecarViolation;
use super::super::sidecar::SnapshotEntry;
use super::super::sidecar::SnapshotMode;
use super::super::sidecar::StoreMeta;
use super::super::sidecar::ENABLED_MARKER_SLOT_LENGTH;
use super::super::sidecar::STORE_META_LENGTH;
use super::CompactionAction;
use super::CompactionStep;

pub(super) const COMPACTION_SAFETY_MARGIN_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CompactionSpace {
    pub(super) available_bytes: u64,
    /// Predicted complete sizes of both pairs currently referenced by valid marker slots.
    pub(super) marker_referenced_pair_bytes: [u64; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SufficientCompactionSpace {
    pub(super) required_bytes: u64,
    pub(super) available_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PublicationModel {
    UnixDirectorySync,
    WindowsExternalFence,
}

/// Non-transferable proof of a later clean-start revalidation for one exact retained entry.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CleanStartOmissionEvidence {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) selected_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_anchor_sequence: u64,
    pub(super) replayed_through_sequence: u64,
    pub(super) completed_entry: RetirementTicketSnapshotEntry,
    pub(super) canonical_entry_binding: Vec<u8>,
    pub(super) entry_binding_crc32: u32,
}

impl CleanStartOmissionEvidence {
    #[cfg(test)]
    pub(super) fn verified_for_test(
        meta: &StoreMeta,
        selected_generation: u64,
        marker_epoch: u64,
        marker_anchor_sequence: u64,
        replayed_through_sequence: u64,
        completed_entry: RetirementTicketSnapshotEntry,
    ) -> Result<Self, CompactionPlanViolation> {
        let canonical_store_meta = encode_store_meta(meta)?;
        let canonical_entry_binding =
            canonical_completed_binding(meta, selected_generation, replayed_through_sequence, &completed_entry)?;
        Ok(Self {
            canonical_store_meta,
            selected_generation,
            marker_epoch,
            marker_anchor_sequence,
            replayed_through_sequence,
            completed_entry,
            entry_binding_crc32: crc32(&canonical_entry_binding),
            canonical_entry_binding,
        })
    }
}

/// Source state minted only by authoritative selected-pair replay.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct VerifiedCompactionSource {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) meta: StoreMeta,
    pub(super) source_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_anchor_sequence: u64,
    pub(super) terminal_sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) sealed_log_length: u64,
    pub(super) entries: Vec<SnapshotEntry>,
    pub(super) create_high_water: u64,
    pub(super) ticket_high_water: u64,
    pub(super) omission_evidence: Vec<CleanStartOmissionEvidence>,
    pub(super) space: CompactionSpace,
    pub(super) publication_model: PublicationModel,
}

#[cfg(test)]
pub(super) struct VerifiedSourcePartsForTest {
    pub(super) source_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_anchor_sequence: u64,
    pub(super) terminal_sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) sealed_log_length: u64,
    pub(super) entries: Vec<SnapshotEntry>,
    pub(super) create_high_water: u64,
    pub(super) ticket_high_water: u64,
    pub(super) omission_evidence: Vec<CleanStartOmissionEvidence>,
    pub(super) space: CompactionSpace,
    pub(super) publication_model: PublicationModel,
}

impl VerifiedCompactionSource {
    #[cfg(test)]
    pub(super) fn verified_for_test(
        meta: StoreMeta,
        canonical_store_meta: [u8; STORE_META_LENGTH],
        parts: VerifiedSourcePartsForTest,
    ) -> Result<Self, CompactionPlanViolation> {
        validate_meta_binding(&meta, &canonical_store_meta)?;
        validate_source_frontier(
            parts.source_generation,
            parts.marker_epoch,
            parts.marker_anchor_sequence,
            parts.terminal_sequence,
            parts.acknowledgement_epoch,
            parts.sealed_log_length,
        )?;
        let target_generation = add_one(parts.source_generation, "target generation")?;
        let base_sequence = add_one(parts.terminal_sequence, "GenerationPrepared sequence")?;
        encode_snapshot(&LifecycleSnapshot {
            mode: SnapshotMode::OrdinaryCompaction,
            store_uuid: meta.store_uuid,
            generation: target_generation,
            log_generation: target_generation,
            predecessor_log_generation: parts.source_generation,
            base_sequence,
            create_high_water: parts.create_high_water,
            ticket_high_water: parts.ticket_high_water,
            entries: parts.entries.clone(),
        })?;
        Ok(Self {
            canonical_store_meta,
            meta,
            source_generation: parts.source_generation,
            marker_epoch: parts.marker_epoch,
            marker_anchor_sequence: parts.marker_anchor_sequence,
            terminal_sequence: parts.terminal_sequence,
            acknowledgement_epoch: parts.acknowledgement_epoch,
            sealed_log_length: parts.sealed_log_length,
            entries: parts.entries,
            create_high_water: parts.create_high_water,
            ticket_high_water: parts.ticket_high_water,
            omission_evidence: parts.omission_evidence,
            space: parts.space,
            publication_model: parts.publication_model,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedDurableUnit {
    pub(super) record: LedgerRecord,
    pub(super) frame: Vec<u8>,
    pub(super) acknowledgement_slot: [u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    pub(super) seal: [u8; COMMIT_SEAL_LENGTH],
    pub(super) sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) frame_start_offset: u64,
    pub(super) frame_end_offset: u64,
    pub(super) sealed_log_length: u64,
}

impl PlannedDurableUnit {
    pub(super) fn frame(&self) -> &[u8] {
        &self.frame
    }

    pub(super) fn acknowledgement_slot(&self) -> &[u8; ACKNOWLEDGEMENT_SLOT_LENGTH] {
        &self.acknowledgement_slot
    }

    pub(super) fn seal(&self) -> &[u8; COMMIT_SEAL_LENGTH] {
        &self.seal
    }

    pub(super) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(super) const fn acknowledgement_epoch(&self) -> u64 {
        self.acknowledgement_epoch
    }

    pub(super) const fn frame_start_offset(&self) -> u64 {
        self.frame_start_offset
    }

    pub(super) const fn frame_end_offset(&self) -> u64 {
        self.frame_end_offset
    }

    pub(super) const fn sealed_log_length(&self) -> u64 {
        self.sealed_log_length
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedCompactionSnapshot {
    pub(super) encoded: Vec<u8>,
    pub(super) base_sequence: u64,
    pub(super) file_length: u64,
    pub(super) file_crc32: u32,
    pub(super) omitted_completed_tickets: Vec<TicketId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedCompactionMarker {
    pub(super) slot: EnabledMarkerSlot,
    pub(super) encoded: [u8; ENABLED_MARKER_SLOT_LENGTH],
    pub(super) stored_crc32: u32,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(super) enum CompactionPlanViolation {
    #[error("compaction codec validation failed: {0}")]
    Codec(#[from] CodecViolation),
    #[error("compaction sidecar validation failed: {0}")]
    Sidecar(#[from] SidecarViolation),
    #[error("verified compaction foundation is invalid: {reason}")]
    InvalidFoundation { reason: &'static str },
    #[error("committed GenerationPrepared receipt is invalid: {reason}")]
    InvalidPreparedReceipt { reason: &'static str },
    #[error("completed-ticket omission evidence is invalid: {reason}")]
    InvalidOmissionEvidence { reason: &'static str },
    #[error("compaction arithmetic overflowed while computing {field}")]
    ArithmeticOverflow { field: &'static str },
    #[error("compaction requires {required} free bytes but only {available} are available")]
    InsufficientSpace { required: u64, available: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurableUnitCrashBoundary {
    FrameAppend,
    FrameSync,
    AcknowledgementWrite,
    AcknowledgementSync,
    AcknowledgementReread,
    SealAppend,
    SealSync,
    SealReread,
    EofVerification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArtifactCrashBoundary {
    TemporaryWrite,
    TemporarySync,
    NoReplacePublish,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MarkerCrashBoundary {
    SlotWrite,
    FileSync,
    SlotReread,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompactionCrashBoundary {
    AppendBarrierAcquire,
    GenerationPrepared(DurableUnitCrashBoundary),
    SnapshotEncode,
    SnapshotPublication(ArtifactCrashBoundary),
    LogOpenedPublication(ArtifactCrashBoundary),
    PlatformDurability,
    PublishedPairReread,
    MarkerSlot(MarkerCrashBoundary),
    LogOpenedCommit(DurableUnitCrashBoundary),
    MarkerCommitted(DurableUnitCrashBoundary),
    Replay,
    Reconciliation,
}

impl CompactionCrashBoundary {
    pub(super) const fn protocol_step(self) -> CompactionStep {
        match self {
            Self::AppendBarrierAcquire => CompactionStep::AppendBarrier,
            Self::GenerationPrepared(_) => CompactionStep::GenerationPrepared,
            Self::SnapshotEncode => CompactionStep::CanonicalSnapshot,
            Self::SnapshotPublication(_) => CompactionStep::PublishSnapshot,
            Self::LogOpenedPublication(_) => CompactionStep::PublishLogOpened,
            Self::PlatformDurability | Self::PublishedPairReread => CompactionStep::VerifyPublishedPair,
            Self::MarkerSlot(_) => CompactionStep::CommitMarkerSlot,
            Self::LogOpenedCommit(_) => CompactionStep::CommitLogOpened,
            Self::MarkerCommitted(_) => CompactionStep::CommitMarkerWitness,
            Self::Replay | Self::Reconciliation => CompactionStep::ReplayAndReconcile,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompactionRecoveryReason {
    OperationFailed(CompactionCrashBoundary),
    ReceiptMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CompactionNeedsRecovery {
    pub(super) step: CompactionStep,
    pub(super) reason: CompactionRecoveryReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparationBinding {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) store_uuid: StoreUuid,
    pub(super) source_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_anchor_sequence: u64,
    pub(super) terminal_sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) sealed_log_length: u64,
}

/// Receipt minted only after the Store append barrier is held for this exact source frontier.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct AppendBarrierReceipt {
    pub(super) binding: PreparationBinding,
}

/// Byte-for-byte observation returned after `GenerationPrepared` is acknowledged and sealed.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PreparedUnitReceipt {
    pub(super) binding: PreparationBinding,
    pub(super) observed_unit: PlannedDurableUnit,
    pub(super) resulting_prefix_crc32: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlanBinding {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) store_uuid: StoreUuid,
    pub(super) source_generation: u64,
    pub(super) target_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) prepared_sequence: u64,
    pub(super) prepared_acknowledgement_epoch: u64,
    pub(super) prepared_sealed_log_length: u64,
    pub(super) prepared_prefix_crc32: u32,
    pub(super) snapshot_file_length: u64,
    pub(super) snapshot_crc32: u32,
    pub(super) log_opened_frame_crc32: u32,
    pub(super) witness_sequence: u64,
    pub(super) witness_acknowledgement_epoch: u64,
    pub(super) witness_sealed_log_length: u64,
}

/// A phase receipt is deliberately non-Clone. Only the verifier for that phase may mint it.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PhaseReceipt<const STEP: u8> {
    pub(super) binding: PlanBinding,
}

pub(super) type SnapshotEncodedReceipt = PhaseReceipt<3>;
pub(super) type SnapshotPublishedReceipt = PhaseReceipt<4>;
pub(super) type LogOpenedPublishedReceipt = PhaseReceipt<5>;
pub(super) type PublishedPairVerifiedReceipt = PhaseReceipt<6>;
pub(super) type MarkerCommittedReceipt = PhaseReceipt<7>;
pub(super) type LogOpenedCommittedReceipt = PhaseReceipt<8>;
pub(super) type MarkerWitnessCommittedReceipt = PhaseReceipt<9>;
pub(super) type ReplayReceipt = PhaseReceipt<10>;
pub(super) type ReconciliationReceipt = PhaseReceipt<11>;

/// Persisted coordinates only; it cannot switch writers or release the append barrier.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct FencedCompactionEvidence {
    store_uuid: StoreUuid,
    source_generation: u64,
    target_generation: u64,
    marker_epoch: u64,
    witness_sequence: u64,
    acknowledgement_epoch: u64,
    sealed_log_length: u64,
    next_sequence: u64,
    next_acknowledgement_epoch: u64,
}

impl FencedCompactionEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "the fenced evidence mirrors the persisted switch frontier without granting activation"
    )]
    pub(super) const fn new(
        store_uuid: StoreUuid,
        source_generation: u64,
        target_generation: u64,
        marker_epoch: u64,
        witness_sequence: u64,
        acknowledgement_epoch: u64,
        sealed_log_length: u64,
        next_sequence: u64,
        next_acknowledgement_epoch: u64,
    ) -> Self {
        Self {
            store_uuid,
            source_generation,
            target_generation,
            marker_epoch,
            witness_sequence,
            acknowledgement_epoch,
            sealed_log_length,
            next_sequence,
            next_acknowledgement_epoch,
        }
    }

    pub(super) const fn target_generation(&self) -> u64 {
        self.target_generation
    }

    pub(super) const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub(super) const fn next_acknowledgement_epoch(&self) -> u64 {
        self.next_acknowledgement_epoch
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum CompactionDecision {
    Execute(CompactionAction),
    NeedsRecovery(CompactionNeedsRecovery),
    FencedComplete(FencedCompactionEvidence),
}

/// Opaque proof that the complete section 10.1 scan disproved target selection/acknowledgement.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct CompactionAbandonmentProof {
    pub(super) binding: PlanBinding,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AbandonmentScanForTest {
    pub(super) prepared_frame: Vec<u8>,
    pub(super) prepared_acknowledgement_slot: [u8; ACKNOWLEDGEMENT_SLOT_LENGTH],
    pub(super) prepared_seal: [u8; COMMIT_SEAL_LENGTH],
    pub(super) marker_slots_reconstructed: bool,
    pub(super) acknowledgement_slots_reconstructed: bool,
    pub(super) no_target_marker: bool,
    pub(super) no_target_acknowledgement_or_seal: bool,
    pub(super) candidate_exact_or_absent: bool,
    pub(super) complete_higher_generation_inventory: bool,
    pub(super) no_gaps_or_unexplained_artifacts: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AbortDeniedReason {
    PreparedUnitMismatch,
    IncompleteSlotReconstruction,
    TargetMayBeSelected,
    TargetMayBeAcknowledged,
    AmbiguousCandidate,
    IncompleteHigherGenerationScan,
    ProofMismatch,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum CompactionAbortDecision {
    AppendGenerationAborted(Box<PlannedDurableUnit>),
    FailClosed(AbortDeniedReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SlotReference {
    Unused,
    Valid { generation: u64 },
    InvalidNonZero,
}

/// Opaque proof that marker/ACK slots and their seals were reconstructed from retained logs.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct OldPairReconstructionReceipt {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) store_uuid: StoreUuid,
    pub(super) candidate_generation: u64,
    pub(super) current_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_witness_sequence: u64,
}

/// Opaque proof that this exact current pair passed a later clean-start replay.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct LaterCleanStartReceipt {
    pub(super) canonical_store_meta: [u8; STORE_META_LENGTH],
    pub(super) store_uuid: StoreUuid,
    pub(super) current_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_witness_sequence: u64,
    pub(super) replayed_through_sequence: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OldPairScanForTest {
    pub(super) candidate_generation: u64,
    pub(super) current_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) marker_witness_sequence: u64,
    pub(super) marker_slots: [SlotReference; 2],
    pub(super) acknowledgement_slots: [SlotReference; 2],
    pub(super) acknowledgement_seals_reconstructed: [bool; 2],
    pub(super) retained_log_generations: Vec<u64>,
}

#[cfg(test)]
impl OldPairScanForTest {
    pub(super) fn eligible_for_test(
        candidate_generation: u64,
        current_generation: u64,
        marker_epoch: u64,
        marker_witness_sequence: u64,
    ) -> Self {
        Self {
            candidate_generation,
            current_generation,
            marker_epoch,
            marker_witness_sequence,
            marker_slots: [
                SlotReference::Valid {
                    generation: current_generation.saturating_sub(1),
                },
                SlotReference::Valid {
                    generation: current_generation,
                },
            ],
            acknowledgement_slots: [
                SlotReference::Valid {
                    generation: current_generation,
                },
                SlotReference::Valid {
                    generation: current_generation,
                },
            ],
            acknowledgement_seals_reconstructed: [true, true],
            retained_log_generations: vec![current_generation.saturating_sub(1), current_generation],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationArtifact {
    Snapshot { generation: u64 },
    Log { generation: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeletionMode {
    ExactIndividualFilesOnly,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct OldPairDeletionPlan {
    artifacts: [GenerationArtifact; 2],
    deletion: DeletionMode,
}

impl OldPairDeletionPlan {
    pub(super) const fn for_generation(generation: u64) -> Self {
        Self {
            artifacts: [
                GenerationArtifact::Snapshot { generation },
                GenerationArtifact::Log { generation },
            ],
            deletion: DeletionMode::ExactIndividualFilesOnly,
        }
    }

    #[cfg(test)]
    pub(super) const fn new_for_test(generation: u64) -> Self {
        Self::for_generation(generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OldPairRetentionReason {
    NotOlderThanCurrent,
    AmbiguousSlot,
    StillReferenced,
    AcknowledgementNotReconstructible,
    RetainedSealLogMissing,
    NoLaterCleanStartReplay,
    ReceiptMismatch,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OldPairGcDecision {
    Eligible(OldPairDeletionPlan),
    Retain(OldPairRetentionReason),
}

#[cfg(test)]
impl OldPairReconstructionReceipt {
    pub(super) fn verified_for_test(
        meta: &StoreMeta,
        scan: OldPairScanForTest,
    ) -> Result<Self, OldPairRetentionReason> {
        if scan.candidate_generation >= scan.current_generation {
            return Err(OldPairRetentionReason::NotOlderThanCurrent);
        }
        for slot in scan.marker_slots.into_iter().chain(scan.acknowledgement_slots) {
            match slot {
                SlotReference::InvalidNonZero => return Err(OldPairRetentionReason::AmbiguousSlot),
                SlotReference::Valid { generation } if generation == scan.candidate_generation => {
                    return Err(OldPairRetentionReason::StillReferenced);
                }
                SlotReference::Unused | SlotReference::Valid { .. } => {}
            }
        }
        if scan.acknowledgement_seals_reconstructed.iter().any(|value| !value) {
            return Err(OldPairRetentionReason::AcknowledgementNotReconstructible);
        }
        for slot in scan.acknowledgement_slots {
            let SlotReference::Valid { generation } = slot else {
                return Err(OldPairRetentionReason::AcknowledgementNotReconstructible);
            };
            if !scan.retained_log_generations.contains(&generation) {
                return Err(OldPairRetentionReason::RetainedSealLogMissing);
            }
        }
        if scan.current_generation.checked_add(1) != Some(scan.marker_epoch) || scan.marker_witness_sequence == 0 {
            return Err(OldPairRetentionReason::ReceiptMismatch);
        }
        Ok(Self {
            canonical_store_meta: encode_store_meta(meta).map_err(|_| OldPairRetentionReason::ReceiptMismatch)?,
            store_uuid: meta.store_uuid,
            candidate_generation: scan.candidate_generation,
            current_generation: scan.current_generation,
            marker_epoch: scan.marker_epoch,
            marker_witness_sequence: scan.marker_witness_sequence,
        })
    }
}

#[cfg(test)]
impl LaterCleanStartReceipt {
    pub(super) fn verified_for_test(
        meta: &StoreMeta,
        current_generation: u64,
        marker_epoch: u64,
        marker_witness_sequence: u64,
        replayed_through_sequence: u64,
    ) -> Result<Self, OldPairRetentionReason> {
        if current_generation.checked_add(1) != Some(marker_epoch)
            || marker_witness_sequence == 0
            || replayed_through_sequence < marker_witness_sequence
        {
            return Err(OldPairRetentionReason::NoLaterCleanStartReplay);
        }
        Ok(Self {
            canonical_store_meta: encode_store_meta(meta).map_err(|_| OldPairRetentionReason::ReceiptMismatch)?,
            store_uuid: meta.store_uuid,
            current_generation,
            marker_epoch,
            marker_witness_sequence,
            replayed_through_sequence,
        })
    }
}

pub(super) fn old_pair_gc_decision(
    reconstruction: OldPairReconstructionReceipt,
    clean_start: LaterCleanStartReceipt,
) -> OldPairGcDecision {
    if reconstruction.canonical_store_meta != clean_start.canonical_store_meta
        || reconstruction.store_uuid != clean_start.store_uuid
        || reconstruction.current_generation != clean_start.current_generation
        || reconstruction.marker_epoch != clean_start.marker_epoch
        || reconstruction.marker_witness_sequence != clean_start.marker_witness_sequence
        || clean_start.replayed_through_sequence < reconstruction.marker_witness_sequence
    {
        return OldPairGcDecision::Retain(OldPairRetentionReason::ReceiptMismatch);
    }
    OldPairGcDecision::Eligible(OldPairDeletionPlan::for_generation(reconstruction.candidate_generation))
}

pub(super) fn validate_meta_binding(
    meta: &StoreMeta,
    canonical_store_meta: &[u8; STORE_META_LENGTH],
) -> Result<(), CompactionPlanViolation> {
    if encode_store_meta(meta)? != *canonical_store_meta || decode_store_meta(canonical_store_meta)? != *meta {
        return Err(CompactionPlanViolation::InvalidFoundation {
            reason: "canonical store.meta bytes differ in UUID, bootstrap id, or creation time",
        });
    }
    Ok(())
}

pub(super) fn validate_source_frontier(
    source_generation: u64,
    marker_epoch: u64,
    marker_anchor_sequence: u64,
    terminal_sequence: u64,
    acknowledgement_epoch: u64,
    sealed_log_length: u64,
) -> Result<(), CompactionPlanViolation> {
    if marker_anchor_sequence == 0
        || terminal_sequence == 0
        || marker_anchor_sequence > terminal_sequence
        || acknowledgement_epoch == 0
        || sealed_log_length == 0
    {
        return Err(CompactionPlanViolation::InvalidFoundation {
            reason: "selected source frontier has zero or reversed coordinates",
        });
    }
    if marker_epoch != add_one(source_generation, "source marker epoch")? {
        return Err(CompactionPlanViolation::InvalidFoundation {
            reason: "source marker epoch must equal source generation plus one",
        });
    }
    Ok(())
}

pub(super) fn canonical_completed_binding(
    meta: &StoreMeta,
    selected_generation: u64,
    replayed_through_sequence: u64,
    entry: &RetirementTicketSnapshotEntry,
) -> Result<Vec<u8>, CompactionPlanViolation> {
    if entry.stage != RetirementStage::CompletedRetained || replayed_through_sequence == 0 {
        return Err(CompactionPlanViolation::InvalidOmissionEvidence {
            reason: "only a replayed Completed-retained entry can be bound",
        });
    }
    let generation = add_one(selected_generation, "omission binding generation")?;
    Ok(encode_snapshot(&LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: meta.store_uuid,
        generation,
        log_generation: generation,
        predecessor_log_generation: selected_generation,
        base_sequence: replayed_through_sequence,
        create_high_water: entry.incarnation.create_seq(),
        ticket_high_water: entry.ticket_id.get(),
        entries: vec![SnapshotEntry::RetirementTicket(entry.clone())],
    })?)
}

pub(super) fn check_space(
    input: CompactionSpace,
    snapshot_length: u64,
    target_log_length: u64,
) -> Result<SufficientCompactionSpace, CompactionPlanViolation> {
    let required_bytes = snapshot_length
        .checked_mul(2)
        .and_then(|value| target_log_length.checked_mul(2).and_then(|log| value.checked_add(log)))
        .and_then(|value| value.checked_add(input.marker_referenced_pair_bytes[0]))
        .and_then(|value| value.checked_add(input.marker_referenced_pair_bytes[1]))
        .and_then(|value| value.checked_add(COMPACTION_SAFETY_MARGIN_BYTES))
        .ok_or(CompactionPlanViolation::ArithmeticOverflow {
            field: "free-space requirement",
        })?;
    if input.available_bytes < required_bytes {
        return Err(CompactionPlanViolation::InsufficientSpace {
            required: required_bytes,
            available: input.available_bytes,
        });
    }
    Ok(SufficientCompactionSpace {
        required_bytes,
        available_bytes: input.available_bytes,
    })
}

pub(super) fn add_one(value: u64, field: &'static str) -> Result<u64, CompactionPlanViolation> {
    value
        .checked_add(1)
        .ok_or(CompactionPlanViolation::ArithmeticOverflow { field })
}
