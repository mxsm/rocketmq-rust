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

use super::super::codec::CodecError;
use super::super::codec::LedgerRecord;
use super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH;
use super::super::codec::COMMIT_SEAL_LENGTH;
use super::super::identity::StoreUuid;
use super::super::sidecar::EnabledMarkerSlot;
use super::super::sidecar::SidecarError;
use super::super::sidecar::ENABLED_MARKER_FILE_LENGTH;
use super::super::sidecar::ENABLED_MARKER_SLOT_LENGTH;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(super) enum BootstrapPlanError {
    #[error("bootstrap codec validation failed: {0}")]
    Codec(#[from] CodecError),
    #[error("bootstrap sidecar validation failed: {0}")]
    Sidecar(#[from] SidecarError),
    #[error("bootstrap snapshot is invalid: {reason}")]
    InvalidSnapshot { reason: &'static str },
    #[error("canonical store.meta proof differs from the expected StoreMeta")]
    FoundationStoreMetaMismatch,
    #[error("bootstrap proof belongs to a different store identity")]
    FoundationIdentityMismatch,
    #[error("StoreInitialized is not durably committed; inventory proof cannot be consumed")]
    StoreInitializedNotDurable,
    #[error("generation-switch foundation is invalid: {reason}")]
    InvalidGenerationSwitch { reason: &'static str },
    #[error("marker epoch {actual} does not select snapshot generation {generation}; expected {expected}")]
    MarkerEpochMismatch {
        generation: u64,
        expected: u64,
        actual: u64,
    },
    #[error("bootstrap arithmetic overflowed while computing {field}")]
    ArithmeticOverflow { field: &'static str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum BootstrapRecord {
    StoreInitialized,
    BootstrapInstalled,
    LogOpened,
    MarkerCommitted,
}

/// Replay-classified frontier within the non-batched frame/ACK/seal protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurableUnitProgress {
    Missing,
    ExactFramePrefix,
    FrameWritten,
    FrameSynced,
    AcknowledgementWritten,
    AcknowledgementSynced,
    AcknowledgementVerified,
    ExactSealPrefix,
    SealWritten,
    SealSynced,
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurableUnitStep {
    AppendFrame,
    CompleteFrame,
    SyncFrame,
    WriteAcknowledgementSlot,
    SyncAcknowledgementSlot,
    VerifyAcknowledgementSlot,
    AppendSeal,
    CompleteSeal,
    SyncSeal,
    VerifySealAndEof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImmutableArtifactProgress {
    Missing,
    TemporaryWritten,
    TemporarySynced,
    Published,
    Verified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ImmutableArtifactStep {
    WriteTemporary,
    SyncTemporary,
    PublishFinalNoReplace,
    ReopenAndVerify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MarkerSlotProgress {
    Missing,
    ExactSlotPrefix,
    SlotWritten,
    SlotSynced,
    Verified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MarkerSlotStep {
    WriteInactiveSlot,
    CompleteInactiveSlot,
    SyncMarkerFile,
    ReopenAndVerifySlot,
}

/// Durable frontier for first publication of the complete 208-byte `ENABLED.v1` artifact.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum InitialMarkerProgress {
    Missing,
    TemporaryWritten,
    TemporarySynced,
    Published,
    DirectorySynced,
    Verified(Box<InitialMarkerVerificationEvidence>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InitialMarkerStep {
    WriteTemporary,
    SyncTemporary,
    PublishFinalNoReplace,
    SyncLifecycleDirectory,
    ReopenAndVerifyEntireFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconciliationPhase {
    BeforeMarker,
    AfterMarkerWitness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapAction {
    AdvanceUnit {
        record: BootstrapRecord,
        step: DurableUnitStep,
    },
    AdvanceSnapshot {
        step: ImmutableArtifactStep,
    },
    AdvanceMarker {
        step: MarkerSlotStep,
    },
    AdvanceInitialMarker {
        step: InitialMarkerStep,
    },
    Reconcile {
        phase: ReconciliationPhase,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapFlow {
    Initial,
    GenerationSwitch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapCheckpoint {
    StoreInitialized,
    BootstrapSnapshot,
    BootstrapInstalled,
    LogOpened,
    InitialMarker,
    MarkerSlot,
    MarkerCommitted,
    Reconciliation,
}

/// Every mutating/durability boundary maps to replay rather than inferred success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapCrashBoundary {
    FrameAppend,
    FrameSync,
    AcknowledgementSlotWrite,
    AcknowledgementSlotSync,
    AcknowledgementSlotReread,
    SealAppend,
    SealSync,
    SealReread,
    EofVerification,
    SnapshotWrite,
    SnapshotSync,
    SnapshotPublish,
    SnapshotReopen,
    InitialMarkerTemporaryWrite,
    InitialMarkerTemporarySync,
    InitialMarkerPublish,
    InitialMarkerDirectorySync,
    InitialMarkerReopen,
    MarkerSlotWrite,
    MarkerSync,
    MarkerReread,
    Reconciliation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapAmbiguity {
    UnexpectedArtifact,
    IdentityMismatch,
    NonDeterministicBytes,
    AcknowledgementOrSeal,
    Snapshot,
    InitialMarkerArtifact,
    Marker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapRecoveryReason {
    OperationFailed(BootstrapCrashBoundary),
    AmbiguousPersistedState(BootstrapAmbiguity),
    FoundationAnchorNotDurable,
    AnchorAcknowledgedBeforeMarker,
    AnchorMissingAfterMarker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NeedsRecovery {
    pub(super) flow: BootstrapFlow,
    pub(super) checkpoint: BootstrapCheckpoint,
    pub(super) reason: BootstrapRecoveryReason,
}

/// Persisted coordinates only. This is deliberately not an activation or deletion capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FencedBootstrapEvidence {
    pub(super) flow: BootstrapFlow,
    pub(super) store_uuid: StoreUuid,
    pub(super) log_generation: u64,
    pub(super) marker_epoch: u64,
    pub(super) witness_sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) sealed_log_length: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BootstrapDecision {
    Execute(BootstrapAction),
    RequireBootstrapInventory,
    NeedsRecovery(NeedsRecovery),
    /// Bytes are complete, but publication and capability issuance remain externally fenced.
    FencedComplete(FencedBootstrapEvidence),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedAcknowledgedUnit {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedMarkerSlot {
    pub(super) slot: EnabledMarkerSlot,
    pub(super) encoded: [u8; ENABLED_MARKER_SLOT_LENGTH],
    pub(super) stored_crc32: u32,
}

/// Exact first-generation marker artifact; slot 0 is populated and slot 1 is all zeroes.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PlannedInitialMarker {
    pub(super) slot0: EnabledMarkerSlot,
    pub(super) encoded_file: [u8; ENABLED_MARKER_FILE_LENGTH],
    pub(super) file_crc32: u32,
    pub(super) slot0_stored_crc32: u32,
}

/// Opaque evidence that a verifier reopened and matched the entire 208-byte marker artifact.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct InitialMarkerVerificationEvidence {
    encoded_file: [u8; ENABLED_MARKER_FILE_LENGTH],
    file_crc32: u32,
    slot0_stored_crc32: u32,
}

impl InitialMarkerVerificationEvidence {
    pub(super) fn matches(&self, planned: &PlannedInitialMarker) -> bool {
        self.encoded_file == planned.encoded_file
            && self.file_crc32 == planned.file_crc32
            && self.slot0_stored_crc32 == planned.slot0_stored_crc32
    }

    pub(super) fn from_reopened_bytes(
        encoded_file: [u8; ENABLED_MARKER_FILE_LENGTH],
        planned: &PlannedInitialMarker,
    ) -> Option<Self> {
        if encoded_file != planned.encoded_file {
            return None;
        }
        Some(Self {
            encoded_file,
            file_crc32: planned.file_crc32,
            slot0_stored_crc32: planned.slot0_stored_crc32,
        })
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement::bootstrap) fn verified_for_test(planned: &PlannedInitialMarker) -> Self {
        Self {
            encoded_file: planned.encoded_file,
            file_crc32: planned.file_crc32,
            slot0_stored_crc32: planned.slot0_stored_crc32,
        }
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement::bootstrap) fn mismatched_for_test(planned: &PlannedInitialMarker) -> Self {
        let mut evidence = Self::verified_for_test(planned);
        evidence.encoded_file[0] ^= 1;
        evidence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PlannedSnapshot {
    pub(super) encoded: Vec<u8>,
    pub(super) file_crc32: u32,
    pub(super) inventory_count: u64,
    pub(super) create_high_water: u64,
    pub(super) ticket_high_water: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum InitialBootstrapProgress {
    BootstrapSnapshot(ImmutableArtifactProgress),
    BootstrapInstalled(DurableUnitProgress),
    PreMarkerReconciled,
    InitialMarker(InitialMarkerProgress),
    MarkerCommitted(DurableUnitProgress),
    PostWitnessReconciled,
    Ambiguous {
        checkpoint: BootstrapCheckpoint,
        evidence: BootstrapAmbiguity,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GenerationSwitchProgress {
    LogOpenedBeforeMarker(DurableUnitProgress),
    MarkerSlot(MarkerSlotProgress),
    LogOpenedAfterMarker(DurableUnitProgress),
    MarkerCommitted(DurableUnitProgress),
    PostWitnessReconciled,
    Ambiguous {
        checkpoint: BootstrapCheckpoint,
        evidence: BootstrapAmbiguity,
    },
}
