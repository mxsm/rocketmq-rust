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

use super::codec::crc32;
use super::codec::decode_acknowledgement_slot;
use super::codec::decode_commit_seal;
use super::codec::encode_acknowledgement_slot;
use super::codec::encode_commit_seal;
use super::codec::encode_ledger_frame;
use super::codec::validate_commit_seal_against_slot;
use super::codec::AcknowledgementSlot;
use super::codec::AcknowledgementSlotState;
use super::codec::CodecViolation;
use super::codec::CommitSeal;
use super::codec::LedgerRecord;
use super::codec::COMMIT_SEAL_LENGTH;
use super::identity::StoreUuid;
use super::io::FileLedgerIo;
use super::io::LedgerIo;
use super::io::LedgerIoFailure;
use super::registry::CompletedRetirementReceipt;
use super::registry::DurableRetirementToken;
use super::registry::LogicalRemovedCapability;
use super::registry::NamespaceAbsentCapability;
use super::registry::RegistryViolation;
use super::registry::RetirementHandoffCapability;
use super::registry::RetirementIntentAppend;
use super::registry::TombstonedCapability;
use super::state::WriterRecoveryFrontier;

mod incarnation;

#[allow(
    unused_imports,
    reason = "M3 stages creation receipts before the managed allocation service consumes them"
)]
pub(super) use incarnation::{
    AllocatedIncarnationReceipt, BoundIncarnationReceipt, IncarnationAllocationPlan, IncarnationWriteFailure,
    PublishedIncarnationReceipt,
};

/// Durable append stage whose failure requires replay before another append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterStage {
    OpenBackend,
    AppendFrame,
    SyncFrame,
    WriteAcknowledgementSlot,
    SyncAcknowledgementSlot,
    ReadAcknowledgementSlot,
    AppendSeal,
    SyncSeal,
    ReadSeal,
    VerifyEof,
}

/// Opens the production handle-relative backend and revalidates the replay-selected frontier.
pub(in crate::mapped_file::retirement) fn open_managed_lifecycle_writer(
    retained_root: &std::fs::File,
    frontier: &WriterRecoveryFrontier,
) -> Result<ManagedLedgerWriter<FileLedgerIo>, ManagedLedgerWriterFailure> {
    let io = FileLedgerIo::open_from_store_root(retained_root, frontier.log_generation()).map_err(|source| {
        WriterFailure::Io {
            stage: WriterStage::OpenBackend,
            source,
        }
    })?;
    ManagedLedgerWriter::from_recovery_frontier(io, frontier)
}

/// Whether the in-process writer may accept another record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterStatus {
    Ready,
    NeedsRecovery { failed_stage: WriterStage },
    Exhausted,
}

/// Replay-proven position from which a writer may append exactly one next unit.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WriterCursor {
    store_uuid: StoreUuid,
    bootstrap_id: [u8; 16],
    log_generation: u64,
    next_sequence: u64,
    next_acknowledgement_epoch: u64,
    sealed_log_length: u64,
    activated: bool,
    marker_epoch: u64,
}

impl WriterCursor {
    fn from_recovery_frontier(frontier: &WriterRecoveryFrontier) -> Result<Self, WriterFailure> {
        Self::new(
            frontier.store_uuid(),
            frontier.bootstrap_id(),
            frontier.log_generation(),
            frontier.next_sequence(),
            frontier.next_acknowledgement_epoch(),
            frontier.sealed_log_length(),
            true,
            frontier.marker_epoch(),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the replay cursor mirrors the eight independent persisted watermark fields"
    )]
    fn new(
        store_uuid: StoreUuid,
        bootstrap_id: [u8; 16],
        log_generation: u64,
        next_sequence: u64,
        next_acknowledgement_epoch: u64,
        sealed_log_length: u64,
        activated: bool,
        marker_epoch: u64,
    ) -> Result<Self, WriterFailure> {
        if bootstrap_id == [0; 16] {
            return Err(WriterFailure::InvalidCursor {
                reason: "bootstrap identifier is zero",
            });
        }
        if next_sequence == 0 {
            return Err(WriterFailure::InvalidCursor {
                reason: "next sequence is zero",
            });
        }
        if next_acknowledgement_epoch == 0 {
            return Err(WriterFailure::InvalidCursor {
                reason: "next acknowledgement epoch is zero",
            });
        }
        if activated != (marker_epoch != 0) {
            return Err(WriterFailure::InvalidCursor {
                reason: "activation flag and marker epoch disagree",
            });
        }
        Ok(Self {
            store_uuid,
            bootstrap_id,
            log_generation,
            next_sequence,
            next_acknowledgement_epoch,
            sealed_log_length,
            activated,
            marker_epoch,
        })
    }

    const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    const fn next_acknowledgement_epoch(&self) -> u64 {
        self.next_acknowledgement_epoch
    }

    const fn sealed_log_length(&self) -> u64 {
        self.sealed_log_length
    }
}

/// A post-write validation failure. The durable state is deliberately treated as ambiguous.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
enum WriterVerificationViolation {
    #[error("acknowledgement slot reread differs from the bytes written")]
    AcknowledgementBytesMismatch,
    #[error("acknowledgement slot reread is not the expected populated slot")]
    AcknowledgementStateMismatch,
    #[error("acknowledgement slot reread is invalid: {0}")]
    InvalidAcknowledgement(CodecViolation),
    #[error("commit seal reread differs from the bytes written")]
    CommitSealBytesMismatch,
    #[error("commit seal reread is invalid: {0}")]
    InvalidCommitSeal(CodecViolation),
    #[error("sealed log EOF mismatch: expected {expected}, found {actual}")]
    EofMismatch { expected: u64, actual: u64 },
}

/// Failure from the bounded durable writer.
#[derive(Debug, Error)]
enum WriterFailure {
    #[error("record cannot be encoded before I/O: {0}")]
    Codec(#[from] CodecViolation),
    #[error("invalid replay cursor: {reason}")]
    InvalidCursor { reason: &'static str },
    #[error("writer I/O failed during {stage:?}: {source}")]
    Io {
        stage: WriterStage,
        #[source]
        source: LedgerIoFailure,
    },
    #[error("writer verification failed during {stage:?}: {source}")]
    Verification {
        stage: WriterStage,
        #[source]
        source: WriterVerificationViolation,
    },
    #[error("writer requires replay after failure during {failed_stage:?}")]
    NeedsRecovery { failed_stage: WriterStage },
    #[error("writer monotonic sequence or acknowledgement domain is exhausted")]
    MonotonicDomainExhausted,
    #[error("encoded log offset overflow")]
    OffsetOverflow,
}

/// Proof that one frame, acknowledgement slot, and seal completed the full durability protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableAppendReceipt {
    sequence: u64,
    acknowledgement_epoch: u64,
    frame_start_offset: u64,
    frame_end_offset: u64,
    sealed_log_length: u64,
}

impl DurableAppendReceipt {
    const fn sequence(self) -> u64 {
        self.sequence
    }

    const fn acknowledgement_epoch(self) -> u64 {
        self.acknowledgement_epoch
    }

    const fn frame_start_offset(self) -> u64 {
        self.frame_start_offset
    }

    const fn frame_end_offset(self) -> u64 {
        self.frame_end_offset
    }

    const fn sealed_log_length(self) -> u64 {
        self.sealed_log_length
    }
}

/// Non-batching writer for one frozen-v1 frame/acknowledgement/seal unit at a time.
///
/// This synchronous object must be created and used inside one operation submitted to the
/// Store's injected blocking executor. It does not create a runtime, thread, or detached task.
/// A replay-proven cursor and already-opened handle-relative [`LedgerIo`] backend are required.
/// After the first I/O call, every error is sticky and another append is rejected until replay
/// constructs a new writer. No raw frame or seal append API is exposed.
struct LedgerWriter<I> {
    io: I,
    cursor: WriterCursor,
    status: WriterStatus,
}

impl<I: LedgerIo> LedgerWriter<I> {
    fn new(io: I, cursor: WriterCursor) -> Self {
        Self {
            io,
            cursor,
            status: WriterStatus::Ready,
        }
    }

    const fn status(&self) -> WriterStatus {
        self.status
    }

    /// Appends and durably acknowledges exactly one record.
    ///
    /// Encoding and all bounded offset calculations finish before the first I/O. Once I/O begins,
    /// any write, sync, reread, or EOF failure changes the status to [`WriterStatus::NeedsRecovery`].
    fn append(&mut self, record: &LedgerRecord) -> Result<DurableAppendReceipt, WriterFailure> {
        match self.status {
            WriterStatus::Ready => {}
            WriterStatus::NeedsRecovery { failed_stage } => {
                return Err(WriterFailure::NeedsRecovery { failed_stage });
            }
            WriterStatus::Exhausted => return Err(WriterFailure::MonotonicDomainExhausted),
        }

        let sequence = self.cursor.next_sequence();
        let acknowledgement_epoch = self.cursor.next_acknowledgement_epoch();
        let frame_start_offset = self.cursor.sealed_log_length();
        let frame = encode_ledger_frame(record, sequence, self.cursor.log_generation)?;
        let frame_length = u64::try_from(frame.len()).map_err(|_| WriterFailure::OffsetOverflow)?;
        let frame_end_offset = frame_start_offset
            .checked_add(frame_length)
            .ok_or(WriterFailure::OffsetOverflow)?;
        let sealed_log_length = frame_end_offset
            .checked_add(COMMIT_SEAL_LENGTH as u64)
            .ok_or(WriterFailure::OffsetOverflow)?;
        let slot_index = u8::try_from((acknowledgement_epoch - 1) & 1).map_err(|_| WriterFailure::OffsetOverflow)?;
        let slot = AcknowledgementSlot {
            slot_index,
            activated: self.cursor.activated,
            store_uuid: self.cursor.store_uuid,
            bootstrap_id: self.cursor.bootstrap_id,
            acknowledgement_epoch,
            marker_epoch: self.cursor.marker_epoch,
            log_generation: self.cursor.log_generation,
            frame_sequence: sequence,
            frame_end_offset,
            frame_crc32: crc32(&frame),
        };
        let encoded_slot = encode_acknowledgement_slot(&slot)?;
        let seal = CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot)?;
        let encoded_seal = encode_commit_seal(&seal)?;
        let next_sequence = sequence.checked_add(1);
        let next_acknowledgement_epoch = acknowledgement_epoch.checked_add(1);

        if let Err(source) = self.io.append_log(frame_start_offset, &frame) {
            return Err(self.poison_io(WriterStage::AppendFrame, source));
        }
        if let Err(source) = self.io.sync_log() {
            return Err(self.poison_io(WriterStage::SyncFrame, source));
        }
        if let Err(source) = self.io.write_acknowledgement_slot(slot_index, &encoded_slot) {
            return Err(self.poison_io(WriterStage::WriteAcknowledgementSlot, source));
        }
        if let Err(source) = self.io.sync_acknowledgement_file() {
            return Err(self.poison_io(WriterStage::SyncAcknowledgementSlot, source));
        }
        let reread_slot = match self.io.read_acknowledgement_slot(slot_index) {
            Ok(bytes) => bytes,
            Err(source) => return Err(self.poison_io(WriterStage::ReadAcknowledgementSlot, source)),
        };
        if reread_slot != encoded_slot {
            return Err(self.poison_verification(
                WriterStage::ReadAcknowledgementSlot,
                WriterVerificationViolation::AcknowledgementBytesMismatch,
            ));
        }
        match decode_acknowledgement_slot(&reread_slot) {
            Ok(AcknowledgementSlotState::Populated(decoded)) if decoded == slot => {}
            Ok(_) => {
                return Err(self.poison_verification(
                    WriterStage::ReadAcknowledgementSlot,
                    WriterVerificationViolation::AcknowledgementStateMismatch,
                ));
            }
            Err(source) => {
                return Err(self.poison_verification(
                    WriterStage::ReadAcknowledgementSlot,
                    WriterVerificationViolation::InvalidAcknowledgement(source),
                ));
            }
        }

        if let Err(source) = self.io.append_log(frame_end_offset, &encoded_seal) {
            return Err(self.poison_io(WriterStage::AppendSeal, source));
        }
        if let Err(source) = self.io.sync_log() {
            return Err(self.poison_io(WriterStage::SyncSeal, source));
        }
        let mut reread_seal = [0_u8; COMMIT_SEAL_LENGTH];
        if let Err(source) = self.io.read_log_exact(frame_end_offset, &mut reread_seal) {
            return Err(self.poison_io(WriterStage::ReadSeal, source));
        }
        if reread_seal != encoded_seal {
            return Err(self.poison_verification(
                WriterStage::ReadSeal,
                WriterVerificationViolation::CommitSealBytesMismatch,
            ));
        }
        let decoded_seal = match decode_commit_seal(&reread_seal) {
            Ok(decoded) => decoded,
            Err(source) => {
                return Err(self.poison_verification(
                    WriterStage::ReadSeal,
                    WriterVerificationViolation::InvalidCommitSeal(source),
                ));
            }
        };
        if let Err(source) = validate_commit_seal_against_slot(&decoded_seal, &slot, &encoded_slot) {
            return Err(self.poison_verification(
                WriterStage::ReadSeal,
                WriterVerificationViolation::InvalidCommitSeal(source),
            ));
        }
        let actual_log_length = match self.io.log_len() {
            Ok(length) => length,
            Err(source) => return Err(self.poison_io(WriterStage::VerifyEof, source)),
        };
        if actual_log_length != sealed_log_length {
            return Err(self.poison_verification(
                WriterStage::VerifyEof,
                WriterVerificationViolation::EofMismatch {
                    expected: sealed_log_length,
                    actual: actual_log_length,
                },
            ));
        }

        self.cursor.sealed_log_length = sealed_log_length;
        match (next_sequence, next_acknowledgement_epoch) {
            (Some(sequence), Some(epoch)) => {
                self.cursor.next_sequence = sequence;
                self.cursor.next_acknowledgement_epoch = epoch;
            }
            _ => self.status = WriterStatus::Exhausted,
        }
        Ok(DurableAppendReceipt {
            sequence,
            acknowledgement_epoch,
            frame_start_offset,
            frame_end_offset,
            sealed_log_length,
        })
    }

    fn poison_io(&mut self, stage: WriterStage, source: LedgerIoFailure) -> WriterFailure {
        self.status = WriterStatus::NeedsRecovery { failed_stage: stage };
        WriterFailure::Io { stage, source }
    }

    fn poison_verification(&mut self, stage: WriterStage, source: WriterVerificationViolation) -> WriterFailure {
        self.status = WriterStatus::NeedsRecovery { failed_stage: stage };
        WriterFailure::Verification { stage, source }
    }
}

/// Opaque proof created only by the exact frame/acknowledgement/seal writer path.
///
/// Its fields remain private to this module. The registry may consume the proof but no sibling can
/// fabricate one from raw offsets or a caller-selected record.
pub(super) struct WriterDurabilityProof {
    record: LedgerRecord,
    log_generation: u64,
    receipt: DurableAppendReceipt,
}

impl WriterDurabilityProof {
    pub(super) fn into_parts(self) -> (LedgerRecord, u64, u64, u64, u64, u64, u64) {
        (
            self.record,
            self.log_generation,
            self.receipt.sequence(),
            self.receipt.acknowledgement_epoch(),
            self.receipt.frame_start_offset(),
            self.receipt.frame_end_offset(),
            self.receipt.sealed_log_length(),
        )
    }
}

/// Registry-integrated writer that never exposes a raw append receipt.
pub(super) struct ManagedLedgerWriter<I> {
    writer: LedgerWriter<I>,
    log_generation: u64,
}

impl<I: LedgerIo> ManagedLedgerWriter<I> {
    /// Reopens a writer only after the backend still matches the replay-selected durable frontier.
    ///
    /// Replay and writer construction are separate blocking operations. Re-reading the EOF,
    /// authoritative acknowledgement slot, and its final seal closes that interval without
    /// exposing a writable object when the retained files changed in between.
    pub(super) fn from_recovery_frontier(
        mut io: I,
        frontier: &WriterRecoveryFrontier,
    ) -> Result<Self, ManagedLedgerWriterFailure> {
        let cursor = WriterCursor::from_recovery_frontier(frontier)?;
        verify_recovery_frontier(&mut io, frontier)?;
        Ok(Self {
            writer: LedgerWriter::new(io, cursor),
            log_generation: frontier.log_generation(),
        })
    }

    /// Executes the exact durable writer protocol and commits the matching registry reservation.
    ///
    /// The append guard is consumed by value. Any writer or evidence failure therefore leaves the
    /// registry recovery-fenced instead of silently reusing a possibly durable ticket.
    pub(super) fn append_retirement_intent<O>(
        &mut self,
        intent: RetirementIntentAppend<'_, O>,
    ) -> Result<DurableRetirementToken<O>, ManagedLedgerWriterFailure> {
        let record = intent.intent_record();
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_intent(intent, proof).map_err(Into::into)
    }

    /// Persists the queue handoff as `LogicalRemoved` before namespace work is authorized.
    pub(super) fn append_logical_removed<O>(
        &mut self,
        capability: RetirementHandoffCapability<O>,
    ) -> Result<LogicalRemovedCapability<O>, ManagedLedgerWriterFailure> {
        let record = capability.logical_removed_record();
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_logical_removed(capability, proof).map_err(Into::into)
    }

    /// Persists a sticky replacement observation without advancing the retirement stage.
    pub(super) fn append_superseded_path_after_logical<O>(
        &mut self,
        capability: LogicalRemovedCapability<O>,
        observed_replacement_key: super::identity::PhysicalFileKey,
    ) -> Result<LogicalRemovedCapability<O>, ManagedLedgerWriterFailure> {
        let record = super::registry::superseded_path_record(&capability, observed_replacement_key)?;
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_superseded_path_after_logical(capability, proof).map_err(Into::into)
    }

    /// Persists an exact, verified two-name absence observation.
    pub(super) fn append_namespace_absent<O>(
        &mut self,
        capability: LogicalRemovedCapability<O>,
        proof: super::platform::NamespaceAbsenceProof,
        observation_time_ns: u64,
    ) -> Result<NamespaceAbsentCapability<O>, ManagedLedgerWriterFailure> {
        let observed_replacement_key = proof.replacement_key();
        let record = super::registry::namespace_absent_record(&capability, &proof, observation_time_ns)?;
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_namespace_absent(capability, proof, observed_replacement_key).map_err(Into::into)
    }

    /// Persists the exact tombstone observation before authorizing removal of that name.
    pub(super) fn append_tombstoned<O>(
        &mut self,
        capability: LogicalRemovedCapability<O>,
        proof: super::platform::NamespaceTombstoneProof,
    ) -> Result<TombstonedCapability<O>, ManagedLedgerWriterFailure> {
        let observed_replacement_key = proof.replacement_key();
        let record = super::registry::tombstoned_record(&capability, &proof)?;
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_tombstoned(capability, proof, observed_replacement_key).map_err(Into::into)
    }

    /// Persists verified canonical-and-tombstone absence after a durable tombstone stage.
    pub(super) fn append_namespace_absent_after_tombstone<O>(
        &mut self,
        capability: TombstonedCapability<O>,
        proof: super::platform::NamespaceAbsenceProof,
        observation_time_ns: u64,
    ) -> Result<NamespaceAbsentCapability<O>, ManagedLedgerWriterFailure> {
        let observed_replacement_key = proof.replacement_key();
        let record =
            super::registry::namespace_absent_after_tombstone_record(&capability, &proof, observation_time_ns)?;
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_namespace_absent_after_tombstone(capability, proof, observed_replacement_key)
            .map_err(Into::into)
    }

    /// Persists `Completed` and releases registry identity reservations only after its exact seal.
    pub(super) fn append_completed<O>(
        &mut self,
        capability: NamespaceAbsentCapability<O>,
        completion_time_ns: u64,
    ) -> Result<CompletedRetirementReceipt, ManagedLedgerWriterFailure> {
        let record = super::registry::completed_record(&capability, completion_time_ns);
        let receipt = self.writer.append(&record)?;
        let proof = WriterDurabilityProof {
            record,
            log_generation: self.log_generation,
            receipt,
        };
        super::registry::commit_writer_completed(capability, proof).map_err(Into::into)
    }

    #[cfg(test)]
    #[allow(
        clippy::too_many_arguments,
        reason = "the test constructor mirrors all replay-proven writer cursor fields"
    )]
    pub(super) fn for_test(
        io: I,
        store_uuid: StoreUuid,
        bootstrap_id: [u8; 16],
        log_generation: u64,
        next_sequence: u64,
        next_acknowledgement_epoch: u64,
        sealed_log_length: u64,
        activated: bool,
        marker_epoch: u64,
    ) -> Result<Self, ManagedLedgerWriterFailure> {
        let cursor = WriterCursor::new(
            store_uuid,
            bootstrap_id,
            log_generation,
            next_sequence,
            next_acknowledgement_epoch,
            sealed_log_length,
            activated,
            marker_epoch,
        )?;
        Ok(Self {
            writer: LedgerWriter::new(io, cursor),
            log_generation,
        })
    }

    #[cfg(test)]
    pub(super) const fn io_for_test(&self) -> &I {
        &self.writer.io
    }
}

fn verify_recovery_frontier<I: LedgerIo>(io: &mut I, frontier: &WriterRecoveryFrontier) -> Result<(), WriterFailure> {
    let actual_log_length = io.log_len().map_err(|source| WriterFailure::Io {
        stage: WriterStage::VerifyEof,
        source,
    })?;
    if actual_log_length != frontier.sealed_log_length() {
        return Err(WriterFailure::Verification {
            stage: WriterStage::VerifyEof,
            source: WriterVerificationViolation::EofMismatch {
                expected: frontier.sealed_log_length(),
                actual: actual_log_length,
            },
        });
    }

    let acknowledgement_epoch =
        frontier
            .next_acknowledgement_epoch()
            .checked_sub(1)
            .ok_or(WriterFailure::InvalidCursor {
                reason: "recovery frontier has no acknowledged predecessor",
            })?;
    let frame_sequence = frontier
        .next_sequence()
        .checked_sub(1)
        .ok_or(WriterFailure::InvalidCursor {
            reason: "recovery frontier has no sealed predecessor",
        })?;
    if acknowledgement_epoch == 0 || frame_sequence == 0 {
        return Err(WriterFailure::InvalidCursor {
            reason: "recovery frontier predecessor coordinates are zero",
        });
    }
    let slot_index = u8::try_from((acknowledgement_epoch - 1) & 1).map_err(|_| WriterFailure::OffsetOverflow)?;
    let encoded_slot = io
        .read_acknowledgement_slot(slot_index)
        .map_err(|source| WriterFailure::Io {
            stage: WriterStage::ReadAcknowledgementSlot,
            source,
        })?;
    let slot = match decode_acknowledgement_slot(&encoded_slot) {
        Ok(AcknowledgementSlotState::Populated(slot)) => slot,
        Ok(AcknowledgementSlotState::Unused) => {
            return Err(WriterFailure::Verification {
                stage: WriterStage::ReadAcknowledgementSlot,
                source: WriterVerificationViolation::AcknowledgementStateMismatch,
            });
        }
        Err(source) => {
            return Err(WriterFailure::Verification {
                stage: WriterStage::ReadAcknowledgementSlot,
                source: WriterVerificationViolation::InvalidAcknowledgement(source),
            });
        }
    };
    let slot_sealed_log_length = slot.sealed_log_length().map_err(|source| WriterFailure::Verification {
        stage: WriterStage::ReadAcknowledgementSlot,
        source: WriterVerificationViolation::InvalidAcknowledgement(source),
    })?;
    let slot_matches = slot.slot_index == slot_index
        && slot.activated
        && slot.store_uuid == frontier.store_uuid()
        && slot.bootstrap_id == frontier.bootstrap_id()
        && slot.acknowledgement_epoch == acknowledgement_epoch
        && slot.marker_epoch == frontier.marker_epoch()
        && slot.log_generation == frontier.log_generation()
        && slot.frame_sequence == frame_sequence
        && slot_sealed_log_length == frontier.sealed_log_length();
    if !slot_matches {
        return Err(WriterFailure::Verification {
            stage: WriterStage::ReadAcknowledgementSlot,
            source: WriterVerificationViolation::AcknowledgementStateMismatch,
        });
    }

    let mut encoded_seal = [0_u8; COMMIT_SEAL_LENGTH];
    io.read_log_exact(slot.frame_end_offset, &mut encoded_seal)
        .map_err(|source| WriterFailure::Io {
            stage: WriterStage::ReadSeal,
            source,
        })?;
    let seal = decode_commit_seal(&encoded_seal).map_err(|source| WriterFailure::Verification {
        stage: WriterStage::ReadSeal,
        source: WriterVerificationViolation::InvalidCommitSeal(source),
    })?;
    validate_commit_seal_against_slot(&seal, &slot, &encoded_slot).map_err(|source| WriterFailure::Verification {
        stage: WriterStage::ReadSeal,
        source: WriterVerificationViolation::InvalidCommitSeal(source),
    })
}

/// Failure from the registry-integrated writer boundary.
#[derive(Debug, Error)]
#[error(transparent)]
pub(super) struct ManagedLedgerWriterFailure {
    source: ManagedLedgerWriterFailureSource,
}

impl ManagedLedgerWriterFailure {
    pub(super) const fn is_pre_io_contract(&self) -> bool {
        matches!(
            &self.source,
            ManagedLedgerWriterFailureSource::Registry(_)
                | ManagedLedgerWriterFailureSource::Writer(
                    WriterFailure::Codec(_)
                        | WriterFailure::InvalidCursor { .. }
                        | WriterFailure::MonotonicDomainExhausted
                        | WriterFailure::OffsetOverflow
                )
        )
    }
}

#[derive(Debug, Error)]
enum ManagedLedgerWriterFailureSource {
    #[error(transparent)]
    Writer(WriterFailure),
    #[error(transparent)]
    Registry(RegistryViolation),
}

impl From<WriterFailure> for ManagedLedgerWriterFailure {
    fn from(source: WriterFailure) -> Self {
        Self {
            source: ManagedLedgerWriterFailureSource::Writer(source),
        }
    }
}

impl From<RegistryViolation> for ManagedLedgerWriterFailure {
    fn from(source: RegistryViolation) -> Self {
        Self {
            source: ManagedLedgerWriterFailureSource::Registry(source),
        }
    }
}

#[cfg(test)]
#[path = "test_support/model_io.rs"]
pub(in crate::mapped_file::retirement) mod model_io;

#[cfg(test)]
mod tests {
    use super::super::codec::decode_acknowledgement_slot;
    use super::super::codec::decode_commit_seal;
    use super::super::codec::AcknowledgementSlotState;
    use super::super::codec::LedgerRecord;
    use super::super::identity::FileIncarnationId;
    use super::super::identity::StoreUuid;
    use super::super::identity::TicketId;
    use super::model_io::ModelFaultAction;
    use super::model_io::ModelIoEvent;
    use super::model_io::ModelLedgerIo;
    use super::*;

    #[test]
    fn replay_frontier_constructs_the_exact_activated_writer_cursor() {
        let store_uuid = store_uuid();
        let frontier = WriterRecoveryFrontier::from_validated_replay(store_uuid, [7; 16], 9, 101, 78, 4096, 10);

        let cursor = WriterCursor::from_recovery_frontier(&frontier)
            .expect("replay-validated frontier constructs a writer cursor");

        assert_eq!(cursor.store_uuid, store_uuid);
        assert_eq!(cursor.bootstrap_id, [7; 16]);
        assert_eq!(cursor.log_generation, 9);
        assert_eq!(cursor.next_sequence, 101);
        assert_eq!(cursor.next_acknowledgement_epoch, 78);
        assert_eq!(cursor.sealed_log_length, 4096);
        assert!(cursor.activated);
        assert_eq!(cursor.marker_epoch, 10);
    }

    #[test]
    fn managed_writer_revalidates_the_exact_replay_frontier_before_becoming_writable() {
        let (io, frontier, validation_start) = durable_model_and_frontier();

        let writer = ManagedLedgerWriter::from_recovery_frontier(io, &frontier)
            .expect("the exact durable frontier reopens as writable");

        assert_eq!(
            &writer.io_for_test().events()[validation_start..],
            [
                ModelIoEvent::ReadLogLength,
                ModelIoEvent::ReadAcknowledgementSlot { slot_index: 0 },
                ModelIoEvent::ReadLog {
                    offset: 100,
                    length: COMMIT_SEAL_LENGTH,
                },
            ]
        );
    }

    #[test]
    fn managed_writer_rejects_an_eof_that_drifted_after_replay() {
        let (io, frontier, validation_start) = durable_model_and_frontier();
        let io = io.with_fault(validation_start, ModelFaultAction::ReportExtraEof { extra: 1 });

        let Err(error) = ManagedLedgerWriter::from_recovery_frontier(io, &frontier) else {
            panic!("a changed EOF cannot become writable");
        };

        assert!(matches!(
            error.source,
            ManagedLedgerWriterFailureSource::Writer(WriterFailure::Verification {
                stage: WriterStage::VerifyEof,
                source: WriterVerificationViolation::EofMismatch {
                    expected: 172,
                    actual: 173,
                },
            })
        ));
    }

    #[test]
    fn managed_writer_rejects_an_acknowledgement_that_drifted_after_replay() {
        let (io, frontier, validation_start) = durable_model_and_frontier();
        let io = io.with_fault(validation_start + 1, ModelFaultAction::CorruptRead);

        let Err(error) = ManagedLedgerWriter::from_recovery_frontier(io, &frontier) else {
            panic!("a changed acknowledgement cannot become writable");
        };

        assert!(matches!(
            error.source,
            ManagedLedgerWriterFailureSource::Writer(WriterFailure::Verification {
                stage: WriterStage::ReadAcknowledgementSlot,
                source: WriterVerificationViolation::InvalidAcknowledgement(_),
            })
        ));
    }

    #[test]
    fn managed_writer_rejects_a_commit_seal_that_drifted_after_replay() {
        let (io, frontier, validation_start) = durable_model_and_frontier();
        let io = io.with_fault(validation_start + 2, ModelFaultAction::CorruptRead);

        let Err(error) = ManagedLedgerWriter::from_recovery_frontier(io, &frontier) else {
            panic!("a changed commit seal cannot become writable");
        };

        assert!(matches!(
            error.source,
            ManagedLedgerWriterFailureSource::Writer(WriterFailure::Verification {
                stage: WriterStage::ReadSeal,
                source: WriterVerificationViolation::InvalidCommitSeal(_),
            })
        ));
    }

    #[cfg(windows)]
    #[test]
    fn production_managed_writer_reopens_the_exact_windows_replay_frontier() {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        use windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        use windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
        use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
        use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;

        let (model, frontier, _) = durable_model_and_frontier();
        let store = tempfile::tempdir().expect("temporary Store root");
        let lifecycle = store.path().join(".rocketmq-lifecycle");
        std::fs::create_dir(&lifecycle).expect("lifecycle directory creates");
        std::fs::write(lifecycle.join("retirement.log.g00000000000000000002"), model.log())
            .expect("durable log fixture writes");
        std::fs::write(lifecycle.join("ACKNOWLEDGED.v1"), model.acknowledgement())
            .expect("durable acknowledgement fixture writes");
        let root = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
            .open(store.path())
            .expect("Store root handle opens");

        let writer = open_managed_lifecycle_writer(&root, &frontier)
            .expect("Windows production writer revalidates the replay frontier");

        assert_eq!(writer.writer.status(), WriterStatus::Ready);
    }

    #[test]
    fn append_follows_the_exact_non_batched_durability_protocol() {
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), cursor(100, 77));

        let receipt = writer
            .append(&completed_record())
            .expect("all durability steps succeed");

        assert_eq!(receipt.sequence(), 100);
        assert_eq!(receipt.acknowledgement_epoch(), 77);
        assert_eq!(receipt.frame_start_offset(), 0);
        assert_eq!(receipt.frame_end_offset(), 100);
        assert_eq!(receipt.sealed_log_length(), 172);
        assert_eq!(writer.status(), WriterStatus::Ready);
        assert_eq!(
            writer.io.events(),
            [
                ModelIoEvent::AppendLog {
                    expected_offset: 0,
                    length: 100,
                },
                ModelIoEvent::SyncLog,
                ModelIoEvent::WriteAcknowledgementSlot { slot_index: 0 },
                ModelIoEvent::SyncAcknowledgementFile,
                ModelIoEvent::ReadAcknowledgementSlot { slot_index: 0 },
                ModelIoEvent::AppendLog {
                    expected_offset: 100,
                    length: 72,
                },
                ModelIoEvent::SyncLog,
                ModelIoEvent::ReadLog {
                    offset: 100,
                    length: 72,
                },
                ModelIoEvent::ReadLogLength,
            ]
        );
        assert_eq!(writer.io.log().len(), 172);

        let slot_bytes: &[u8; 104] = writer.io.acknowledgement()[..104]
            .try_into()
            .expect("slot has fixed length");
        let AcknowledgementSlotState::Populated(slot) =
            decode_acknowledgement_slot(slot_bytes).expect("written slot decodes")
        else {
            panic!("writer must populate its inactive slot");
        };
        assert_eq!(slot.acknowledgement_epoch, 77);
        assert_eq!(slot.frame_sequence, 100);
        assert_eq!(slot.frame_end_offset, 100);
        assert!(slot.activated);
        assert!(decode_commit_seal(&writer.io.log()[100..]).is_ok());
    }

    #[test]
    fn every_io_boundary_failure_is_sticky_and_blocks_further_appends() {
        let stages = [
            WriterStage::AppendFrame,
            WriterStage::SyncFrame,
            WriterStage::WriteAcknowledgementSlot,
            WriterStage::SyncAcknowledgementSlot,
            WriterStage::ReadAcknowledgementSlot,
            WriterStage::AppendSeal,
            WriterStage::SyncSeal,
            WriterStage::ReadSeal,
            WriterStage::VerifyEof,
        ];

        for (operation_index, expected_stage) in stages.into_iter().enumerate() {
            let io = ModelLedgerIo::empty().with_fault(operation_index, ModelFaultAction::ErrorBefore);
            let mut writer = LedgerWriter::new(io, cursor(100, 77));

            assert!(matches!(
                writer.append(&completed_record()),
                Err(WriterFailure::Io {
                    stage,
                    ..
                }) if stage == expected_stage
            ));
            assert_eq!(
                writer.status(),
                WriterStatus::NeedsRecovery {
                    failed_stage: expected_stage,
                }
            );
            let event_count = writer.io.events().len();
            assert!(matches!(
                writer.append(&completed_record()),
                Err(WriterFailure::NeedsRecovery { failed_stage }) if failed_stage == expected_stage
            ));
            assert_eq!(writer.io.events().len(), event_count);
        }
    }

    #[test]
    fn partial_frame_acknowledgement_and_seal_writes_are_never_inferred_successful() {
        for (operation_index, length, expected_stage) in [
            (0, 17, WriterStage::AppendFrame),
            (2, 51, WriterStage::WriteAcknowledgementSlot),
            (5, 11, WriterStage::AppendSeal),
        ] {
            let io = ModelLedgerIo::empty().with_fault(operation_index, ModelFaultAction::PartialWrite { length });
            let mut writer = LedgerWriter::new(io, cursor(100, 77));

            assert!(matches!(
                writer.append(&completed_record()),
                Err(WriterFailure::Io { stage, .. }) if stage == expected_stage
            ));
            assert_eq!(
                writer.status(),
                WriterStatus::NeedsRecovery {
                    failed_stage: expected_stage,
                }
            );
        }
    }

    #[test]
    fn reread_or_eof_mismatch_is_sticky_corruption() {
        for (operation_index, action, expected_stage) in [
            (4, ModelFaultAction::CorruptRead, WriterStage::ReadAcknowledgementSlot),
            (7, ModelFaultAction::CorruptRead, WriterStage::ReadSeal),
            (8, ModelFaultAction::ReportExtraEof { extra: 1 }, WriterStage::VerifyEof),
        ] {
            let io = ModelLedgerIo::empty().with_fault(operation_index, action);
            let mut writer = LedgerWriter::new(io, cursor(100, 77));

            assert!(matches!(
                writer.append(&completed_record()),
                Err(WriterFailure::Verification { stage, .. }) if stage == expected_stage
            ));
            assert_eq!(
                writer.status(),
                WriterStatus::NeedsRecovery {
                    failed_stage: expected_stage,
                }
            );
        }
    }

    #[test]
    fn codec_rejection_happens_before_io_and_does_not_poison_the_writer() {
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), cursor(100, 77));
        let invalid = LedgerRecord::Completed {
            ticket_id: ticket_id(),
            incarnation: incarnation(),
            completion_time_ns: 1,
            namespace_absent_sequence: 100,
        };

        assert!(matches!(writer.append(&invalid), Err(WriterFailure::Codec(_))));
        assert_eq!(writer.status(), WriterStatus::Ready);
        assert!(writer.io.events().is_empty());
        assert!(writer.append(&completed_record()).is_ok());
    }

    #[test]
    fn the_final_monotonic_value_commits_once_then_exhausts_the_writer() {
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), cursor(u64::MAX, 77));

        let receipt = writer
            .append(&completed_record())
            .expect("the maximum sequence commits");

        assert_eq!(receipt.sequence(), u64::MAX);
        assert_eq!(writer.status(), WriterStatus::Exhausted);
        let event_count = writer.io.events().len();
        assert!(matches!(
            writer.append(&completed_record()),
            Err(WriterFailure::MonotonicDomainExhausted)
        ));
        assert_eq!(writer.io.events().len(), event_count);
    }

    #[test]
    fn the_final_acknowledgement_epoch_commits_once_then_exhausts_the_writer() {
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), cursor(100, u64::MAX));

        let receipt = writer
            .append(&completed_record())
            .expect("the maximum acknowledgement epoch commits");

        assert_eq!(receipt.acknowledgement_epoch(), u64::MAX);
        assert_eq!(writer.status(), WriterStatus::Exhausted);
        let event_count = writer.io.events().len();
        assert!(matches!(
            writer.append(&completed_record()),
            Err(WriterFailure::MonotonicDomainExhausted)
        ));
        assert_eq!(writer.io.events().len(), event_count);
    }

    #[test]
    fn consecutive_units_alternate_physical_acknowledgement_slots() {
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), cursor(100, 77));

        writer.append(&completed_record()).expect("first unit commits");
        let second = writer.append(&completed_record()).expect("second unit commits");

        assert_eq!(second.sequence(), 101);
        assert_eq!(second.acknowledgement_epoch(), 78);
        assert_eq!(second.frame_start_offset(), 172);
        assert_eq!(
            writer.io.events()[11],
            ModelIoEvent::WriteAcknowledgementSlot { slot_index: 1 }
        );
        let second_slot = &writer.io.acknowledgement()[104..];
        let AcknowledgementSlotState::Populated(slot) =
            decode_acknowledgement_slot(second_slot).expect("second slot decodes")
        else {
            panic!("second slot must be populated");
        };
        assert_eq!(slot.acknowledgement_epoch, 78);
        assert_eq!(slot.frame_sequence, 101);
    }

    #[test]
    fn invalid_cursor_fields_are_rejected_before_a_writer_exists() {
        for (bootstrap_id, sequence, epoch, activated, marker_epoch, reason) in [
            ([0; 16], 1, 1, false, 0, "bootstrap identifier is zero"),
            ([1; 16], 0, 1, false, 0, "next sequence is zero"),
            ([1; 16], 1, 0, false, 0, "next acknowledgement epoch is zero"),
            ([1; 16], 1, 1, false, 5, "activation flag and marker epoch disagree"),
            ([1; 16], 1, 1, true, 0, "activation flag and marker epoch disagree"),
        ] {
            assert!(matches!(
                WriterCursor::new(
                    store_uuid(),
                    bootstrap_id,
                    0,
                    sequence,
                    epoch,
                    0,
                    activated,
                    marker_epoch,
                ),
                Err(WriterFailure::InvalidCursor { reason: actual }) if actual == reason
            ));
        }
    }

    #[test]
    fn offset_overflow_is_a_preflight_error_and_does_not_poison_the_writer() {
        let overflowing = WriterCursor::new(store_uuid(), [1; 16], 2, 100, 77, u64::MAX, true, 5)
            .expect("cursor fields are structurally valid");
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), overflowing);

        assert!(matches!(
            writer.append(&completed_record()),
            Err(WriterFailure::OffsetOverflow)
        ));
        assert_eq!(writer.status(), WriterStatus::Ready);
        assert!(writer.io.events().is_empty());
    }

    #[test]
    fn raw_writer_construction_and_append_capabilities_are_module_private() {
        let source = include_str!("writer.rs").replace("\r\n", "\n");
        let production = source
            .split_once("\n#[cfg(test)]\n#[path")
            .expect("test support follows production writer code")
            .0;
        let capability_declarations = [
            "enum WriterStage",
            "enum WriterStatus",
            "struct WriterCursor",
            "enum WriterVerificationViolation",
            "enum WriterFailure",
            "struct DurableAppendReceipt",
            "struct LedgerWriter",
            "fn new(",
            "fn append(",
        ];

        for line in production.lines().filter(|line| {
            capability_declarations
                .iter()
                .any(|declaration| line.contains(declaration))
        }) {
            assert!(
                !line.trim_start().starts_with("pub"),
                "raw writer capability leaked through `{line}`"
            );
        }
    }

    fn cursor(next_sequence: u64, next_acknowledgement_epoch: u64) -> WriterCursor {
        WriterCursor::new(
            store_uuid(),
            std::array::from_fn(|index| index as u8 + 0x10),
            2,
            next_sequence,
            next_acknowledgement_epoch,
            0,
            true,
            5,
        )
        .expect("test cursor is valid")
    }

    fn durable_model_and_frontier() -> (ModelLedgerIo, WriterRecoveryFrontier, usize) {
        let mut writer = LedgerWriter::new(ModelLedgerIo::empty(), cursor(100, 77));
        let receipt = writer
            .append(&completed_record())
            .expect("fixture durability unit is complete");
        let validation_start = writer.io.events().len();
        let frontier = WriterRecoveryFrontier::from_validated_replay(
            store_uuid(),
            std::array::from_fn(|index| index as u8 + 0x10),
            2,
            receipt.sequence() + 1,
            receipt.acknowledgement_epoch() + 1,
            receipt.sealed_log_length(),
            5,
        );
        (writer.io, frontier, validation_start)
    }

    fn completed_record() -> LedgerRecord {
        LedgerRecord::Completed {
            ticket_id: ticket_id(),
            incarnation: incarnation(),
            completion_time_ns: 0x0102_0304_0506_0708,
            namespace_absent_sequence: 9,
        }
    }

    fn store_uuid() -> StoreUuid {
        StoreUuid::new(std::array::from_fn(|index| index as u8)).expect("test UUID is nonzero")
    }

    fn incarnation() -> FileIncarnationId {
        FileIncarnationId::new(store_uuid(), 7).expect("test incarnation is nonzero")
    }

    fn ticket_id() -> TicketId {
        TicketId::new(42).expect("test ticket is nonzero")
    }
}
