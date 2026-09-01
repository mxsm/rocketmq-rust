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

use std::collections::TryReserveError;
use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use thiserror::Error;

use super::super::codec::LedgerRecord;
use super::super::codec::RetirementReason;
#[cfg(test)]
use super::super::codec::COMMIT_SEAL_LENGTH;
use super::super::identity::FileIncarnationId;
use super::super::identity::IdentityViolation;
use super::super::identity::PhysicalFileKey;
use super::super::identity::StoreRelativePath;
use super::super::identity::TicketId;
use super::RegistryAuthority;

/// Process-local identity for the exact queue publication slot that owns a file.
///
/// Cloning preserves identity; allocating another value cannot reproduce it. The registry stores
/// this value rather than an `Arc` to the queue, avoiding a queue/registry ownership cycle.
#[derive(Clone)]
pub(crate) struct QueueIdentity(Arc<QueueIdentitySeal>);

#[derive(Debug)]
struct QueueIdentitySeal;

impl QueueIdentity {
    /// Allocates a fresh opaque queue identity.
    pub(super) fn allocate() -> Self {
        Self(Arc::new(QueueIdentitySeal))
    }

    /// Returns whether two values name the exact same queue publication slot.
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for QueueIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("QueueIdentity").finish_non_exhaustive()
    }
}

impl PartialEq for QueueIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.same_as(other)
    }
}

impl Eq for QueueIdentity {}

/// Strongly owned runtime registration for one durably published file incarnation.
pub(crate) struct PublishedFileRegistration<O> {
    pub(super) incarnation: FileIncarnationId,
    pub(super) physical_key: PhysicalFileKey,
    pub(super) canonical_path: StoreRelativePath,
    pub(super) segment_offset: u64,
    pub(super) expected_length: u64,
    pub(super) owner: Arc<O>,
    pub(super) queue_identity: QueueIdentity,
}

impl<O> PublishedFileRegistration<O> {
    /// Validates the immutable identity fields before they enter the registry.
    #[allow(
        clippy::too_many_arguments,
        reason = "the registration mirrors one exact published incarnation and its two runtime owners"
    )]
    pub(super) fn new(
        incarnation: FileIncarnationId,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        segment_offset: u64,
        expected_length: u64,
        owner: Arc<O>,
        queue_identity: QueueIdentity,
    ) -> Result<Self, RegistryViolation> {
        if expected_length == 0 {
            return Err(RegistryViolation::ZeroExpectedLength);
        }
        canonical_path
            .validate_segment_binding(segment_offset)
            .map_err(RegistryViolation::InvalidCanonicalPathBinding)?;
        Ok(Self {
            incarnation,
            physical_key,
            canonical_path,
            segment_offset,
            expected_length,
            owner,
            queue_identity,
        })
    }
}

/// Caller-selected operation fields that must remain byte-exact through durable intent and handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetirementOperation {
    incarnation: FileIncarnationId,
    reason: RetirementReason,
    mapping_generation: u64,
    segment_offset: u64,
    expected_length: u64,
    retirement_nonce: [u8; 16],
    target_key: PhysicalFileKey,
    canonical_path: StoreRelativePath,
}

impl RetirementOperation {
    /// Validates a complete prospective `RetirementIntent` except for its allocated ticket.
    #[allow(
        clippy::too_many_arguments,
        reason = "the operation mirrors every persisted retirement-authorization field"
    )]
    pub(crate) fn new(
        incarnation: FileIncarnationId,
        reason: RetirementReason,
        mapping_generation: u64,
        segment_offset: u64,
        expected_length: u64,
        retirement_nonce: [u8; 16],
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    ) -> Result<Self, RegistryViolation> {
        if mapping_generation == 0 {
            return Err(RegistryViolation::ZeroMappingGeneration);
        }
        if expected_length == 0 {
            return Err(RegistryViolation::ZeroExpectedLength);
        }
        if retirement_nonce == [0; 16] {
            return Err(RegistryViolation::ZeroRetirementNonce);
        }
        canonical_path
            .validate_segment_binding(segment_offset)
            .map_err(RegistryViolation::InvalidCanonicalPathBinding)?;
        Ok(Self {
            incarnation,
            reason,
            mapping_generation,
            segment_offset,
            expected_length,
            retirement_nonce,
            target_key,
            canonical_path,
        })
    }

    pub(crate) const fn incarnation(&self) -> FileIncarnationId {
        self.incarnation
    }

    pub(crate) const fn reason(&self) -> RetirementReason {
        self.reason
    }

    pub(crate) const fn mapping_generation(&self) -> u64 {
        self.mapping_generation
    }

    pub(crate) const fn segment_offset(&self) -> u64 {
        self.segment_offset
    }

    pub(crate) const fn expected_length(&self) -> u64 {
        self.expected_length
    }

    pub(crate) const fn retirement_nonce(&self) -> [u8; 16] {
        self.retirement_nonce
    }

    pub(crate) const fn target_key(&self) -> PhysicalFileKey {
        self.target_key
    }

    pub(crate) const fn canonical_path(&self) -> &StoreRelativePath {
        &self.canonical_path
    }
}

/// Exact durable intent identity, including its monotonically allocated ticket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetirementIntentBinding {
    pub(super) ticket_id: TicketId,
    pub(super) operation: RetirementOperation,
}

impl RetirementIntentBinding {
    pub(crate) const fn ticket_id(&self) -> TicketId {
        self.ticket_id
    }

    pub(crate) const fn incarnation(&self) -> FileIncarnationId {
        self.operation.incarnation
    }

    pub(crate) const fn reason(&self) -> RetirementReason {
        self.operation.reason
    }

    pub(crate) const fn mapping_generation(&self) -> u64 {
        self.operation.mapping_generation
    }

    pub(crate) const fn segment_offset(&self) -> u64 {
        self.operation.segment_offset
    }

    pub(crate) const fn expected_length(&self) -> u64 {
        self.operation.expected_length
    }

    pub(crate) const fn retirement_nonce(&self) -> [u8; 16] {
        self.operation.retirement_nonce
    }

    pub(crate) const fn target_key(&self) -> PhysicalFileKey {
        self.operation.target_key
    }

    pub(crate) const fn canonical_path(&self) -> &StoreRelativePath {
        &self.operation.canonical_path
    }

    /// Produces the only record that can satisfy this reservation.
    pub(crate) fn to_record(&self) -> LedgerRecord {
        LedgerRecord::RetirementIntent {
            ticket_id: self.ticket_id,
            incarnation: self.operation.incarnation,
            reason: self.operation.reason,
            mapping_generation: self.operation.mapping_generation,
            segment_offset: self.operation.segment_offset,
            expected_length: self.operation.expected_length,
            retirement_nonce: self.operation.retirement_nonce,
            target_key: self.operation.target_key,
            canonical_path: self.operation.canonical_path.clone(),
        }
    }

    pub(super) fn from_record(record: LedgerRecord) -> Result<Self, RegistryViolation> {
        let LedgerRecord::RetirementIntent {
            ticket_id,
            incarnation,
            reason,
            mapping_generation,
            segment_offset,
            expected_length,
            retirement_nonce,
            target_key,
            canonical_path,
        } = record
        else {
            return Err(RegistryViolation::EvidenceIsNotRetirementIntent);
        };
        let operation = RetirementOperation::new(
            incarnation,
            reason,
            mapping_generation,
            segment_offset,
            expected_length,
            retirement_nonce,
            target_key,
            canonical_path,
        )?;
        Ok(Self { ticket_id, operation })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurableEvidenceSource {
    Writer,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DurabilityCoordinates {
    pub(super) ledger_generation: u64,
    pub(super) sequence: u64,
    pub(super) acknowledgement_epoch: u64,
    pub(super) frame_start_offset: u64,
    pub(super) frame_end_offset: u64,
    pub(super) sealed_log_length: u64,
}

impl DurabilityCoordinates {
    pub(super) fn verified(
        ledger_generation: u64,
        sequence: u64,
        acknowledgement_epoch: u64,
        frame_start_offset: u64,
        frame_end_offset: u64,
        sealed_log_length: u64,
    ) -> Result<Self, RegistryViolation> {
        if sequence == 0 || acknowledgement_epoch == 0 || frame_end_offset <= frame_start_offset {
            return Err(RegistryViolation::InvalidDurabilityCoordinates);
        }
        let expected_sealed_length = frame_end_offset
            .checked_add(super::super::codec::COMMIT_SEAL_LENGTH as u64)
            .ok_or(RegistryViolation::InvalidDurabilityCoordinates)?;
        if sealed_log_length != expected_sealed_length {
            return Err(RegistryViolation::InvalidDurabilityCoordinates);
        }
        Ok(Self {
            ledger_generation,
            sequence,
            acknowledgement_epoch,
            frame_start_offset,
            frame_end_offset,
            sealed_log_length,
        })
    }

    #[cfg(test)]
    fn verified_for_test(
        ledger_generation: u64,
        sequence: u64,
        acknowledgement_epoch: u64,
        frame_start_offset: u64,
    ) -> Result<Self, RegistryViolation> {
        if sequence == 0 || acknowledgement_epoch == 0 {
            return Err(RegistryViolation::InvalidDurabilityCoordinates);
        }
        let frame_end_offset = frame_start_offset
            .checked_add(100)
            .ok_or(RegistryViolation::InvalidDurabilityCoordinates)?;
        let sealed_log_length = frame_end_offset
            .checked_add(COMMIT_SEAL_LENGTH as u64)
            .ok_or(RegistryViolation::InvalidDurabilityCoordinates)?;
        Self::verified(
            ledger_generation,
            sequence,
            acknowledgement_epoch,
            frame_start_offset,
            frame_end_offset,
            sealed_log_length,
        )
    }

    /// Returns whether this writer-proven append is globally later than one ticket's previous
    /// durable append.
    ///
    /// Ledger sequence and acknowledgement epochs are Store-global, not ticket-local. Other
    /// tickets may therefore be appended between two stages of this ticket. Within one log
    /// generation, offsets must still move past the predecessor seal; a later generation may
    /// restart its byte offsets only after the generation protocol has advanced monotonically.
    pub(super) fn is_later_than(&self, predecessor: &Self) -> bool {
        self.sequence > predecessor.sequence
            && self.acknowledgement_epoch > predecessor.acknowledgement_epoch
            && (self.ledger_generation > predecessor.ledger_generation
                || (self.ledger_generation == predecessor.ledger_generation
                    && self.frame_start_offset >= predecessor.sealed_log_length))
    }
}

/// Durable position used to validate the next append for one retirement ticket.
///
/// Live transitions retain the complete writer receipt. A compacted snapshot retains only the
/// stage sequence, so recovery uses the replay frontier as its append barrier without inventing
/// frame offsets or acknowledgement epochs that no longer exist on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurableStagePosition {
    WriterVerified(DurabilityCoordinates),
    Replayed { sequence: u64 },
}

impl DurableStagePosition {
    pub(super) const fn writer_verified(coordinates: DurabilityCoordinates) -> Self {
        Self::WriterVerified(coordinates)
    }

    pub(super) fn replayed(sequence: u64) -> Result<Self, RegistryViolation> {
        if sequence == 0 {
            return Err(RegistryViolation::InvalidDurabilityCoordinates);
        }
        Ok(Self::Replayed { sequence })
    }

    pub(super) const fn sequence(self) -> u64 {
        match self {
            Self::WriterVerified(coordinates) => coordinates.sequence,
            Self::Replayed { sequence } => sequence,
        }
    }

    pub(super) fn accepts(self, successor: &DurabilityCoordinates) -> bool {
        match self {
            Self::WriterVerified(predecessor) => successor.is_later_than(&predecessor),
            Self::Replayed { sequence } => successor.sequence > sequence,
        }
    }
}

/// Opaque proof that the full frame/acknowledgement/seal protocol was verified.
///
/// There is deliberately no production constructor in M3's disabled-write core. The writer and
/// replay modules must later receive narrow adapters that can construct this value from their own
/// private receipts; arbitrary callers cannot mint it.
#[derive(Debug)]
pub(crate) struct DurableIntentEvidence {
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurabilityCoordinates,
    pub(super) source: DurableEvidenceSource,
}

impl DurableIntentEvidence {
    #[cfg(test)]
    pub(super) fn writer_verified_for_test(
        record: LedgerRecord,
        ledger_generation: u64,
        sequence: u64,
        acknowledgement_epoch: u64,
        frame_start_offset: u64,
    ) -> Result<Self, RegistryViolation> {
        Self::verified_for_test(
            record,
            ledger_generation,
            sequence,
            acknowledgement_epoch,
            frame_start_offset,
            DurableEvidenceSource::Writer,
        )
    }

    #[cfg(test)]
    pub(super) fn replay_verified_for_test(
        record: LedgerRecord,
        ledger_generation: u64,
        sequence: u64,
        acknowledgement_epoch: u64,
        frame_start_offset: u64,
    ) -> Result<Self, RegistryViolation> {
        Self::verified_for_test(
            record,
            ledger_generation,
            sequence,
            acknowledgement_epoch,
            frame_start_offset,
            DurableEvidenceSource::Replay,
        )
    }

    #[cfg(test)]
    fn verified_for_test(
        record: LedgerRecord,
        ledger_generation: u64,
        sequence: u64,
        acknowledgement_epoch: u64,
        frame_start_offset: u64,
        source: DurableEvidenceSource,
    ) -> Result<Self, RegistryViolation> {
        Ok(Self {
            binding: RetirementIntentBinding::from_record(record)?,
            durability: DurabilityCoordinates::verified_for_test(
                ledger_generation,
                sequence,
                acknowledgement_epoch,
                frame_start_offset,
            )?,
            source,
        })
    }
}

#[derive(Debug, Default)]
pub(super) struct RegistrySeal {
    needs_recovery: AtomicBool,
}

impl RegistrySeal {
    pub(super) fn fence_recovery(&self) {
        self.needs_recovery.store(true, Ordering::Release);
    }

    pub(super) fn needs_recovery(&self) -> bool {
        self.needs_recovery.load(Ordering::Acquire)
    }
}

/// Non-cloneable capability minted only after exact durable intent proof.
#[derive(Debug)]
pub(crate) struct DurableRetirementToken<O> {
    pub(super) authority: Arc<RegistryAuthority<O>>,
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurabilityCoordinates,
    pub(super) owner: Arc<O>,
    pub(super) queue_identity: QueueIdentity,
    pub(super) armed: bool,
}

impl<O> DurableRetirementToken<O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(crate) const fn durable_sequence(&self) -> u64 {
        self.durability.sequence
    }

    pub(crate) const fn ledger_generation(&self) -> u64 {
        self.durability.ledger_generation
    }
}

impl<O> Drop for DurableRetirementToken<O> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.fence_recovery();
        }
    }
}

/// Capability emitted only after the caller reports a successful exact ArcSwap handoff.
#[derive(Debug)]
pub(crate) struct RetirementHandoffCapability<O> {
    pub(super) authority: Arc<RegistryAuthority<O>>,
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurableStagePosition,
    pub(super) armed: bool,
}

impl<O> RetirementHandoffCapability<O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(in crate::mapped_file::retirement) fn logical_removed_record(&self) -> LedgerRecord {
        LedgerRecord::LogicalRemoved {
            ticket_id: self.binding.ticket_id(),
            incarnation: self.binding.incarnation(),
            target_key: self.binding.target_key(),
            canonical_path: self.binding.canonical_path().clone(),
        }
    }
}

impl<O> Drop for RetirementHandoffCapability<O> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.fence_recovery();
        }
    }
}

/// Non-cloneable authorization for namespace convergence after durable logical removal.
#[derive(Debug)]
pub(crate) struct LogicalRemovedCapability<O> {
    pub(super) authority: Arc<RegistryAuthority<O>>,
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurableStagePosition,
    pub(super) append_durability: DurableStagePosition,
    pub(super) observed_replacement_key: Option<PhysicalFileKey>,
    pub(super) armed: bool,
}

impl<O> LogicalRemovedCapability<O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(crate) const fn durable_sequence(&self) -> u64 {
        self.durability.sequence()
    }

    pub(in crate::mapped_file::retirement) const fn observed_replacement_key(&self) -> Option<PhysicalFileKey> {
        self.observed_replacement_key
    }
}

impl<O> Drop for LogicalRemovedCapability<O> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.fence_recovery();
        }
    }
}

/// Non-cloneable authorization minted after a durable, identity-verified tombstone observation.
#[derive(Debug)]
pub(crate) struct TombstonedCapability<O> {
    pub(super) authority: Arc<RegistryAuthority<O>>,
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurableStagePosition,
    pub(super) append_durability: DurableStagePosition,
    pub(super) tombstone_path: StoreRelativePath,
    pub(super) observed_replacement_key: Option<PhysicalFileKey>,
    pub(super) armed: bool,
}

impl<O> TombstonedCapability<O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(crate) const fn durable_sequence(&self) -> u64 {
        self.durability.sequence()
    }

    pub(crate) const fn tombstone_path(&self) -> &StoreRelativePath {
        &self.tombstone_path
    }
}

impl<O> Drop for TombstonedCapability<O> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.fence_recovery();
        }
    }
}

/// Non-cloneable authorization minted after durable two-name absence verification.
#[derive(Debug)]
pub(crate) struct NamespaceAbsentCapability<O> {
    pub(super) authority: Arc<RegistryAuthority<O>>,
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurableStagePosition,
    pub(super) append_durability: DurableStagePosition,
    pub(super) tombstone_path: Option<StoreRelativePath>,
    pub(super) observed_replacement_key: Option<PhysicalFileKey>,
    pub(super) armed: bool,
}

impl<O> NamespaceAbsentCapability<O> {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(crate) const fn durable_sequence(&self) -> u64 {
        self.durability.sequence()
    }
}

impl<O> Drop for NamespaceAbsentCapability<O> {
    fn drop(&mut self) {
        if self.armed {
            self.authority.fence_recovery();
        }
    }
}

/// Final durable receipt emitted only after registry identity and path reservations are released.
#[derive(Debug)]
pub(crate) struct CompletedRetirementReceipt {
    pub(super) binding: RetirementIntentBinding,
    pub(super) durability: DurabilityCoordinates,
}

impl CompletedRetirementReceipt {
    pub(crate) const fn binding(&self) -> &RetirementIntentBinding {
        &self.binding
    }

    pub(crate) const fn durable_sequence(&self) -> u64 {
        self.durability.sequence
    }
}

/// Typed fail-closed outcomes from registry reservation and capability transitions.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum RegistryViolation {
    #[error("canonical segment path is not bound to its offset: {0}")]
    InvalidCanonicalPathBinding(IdentityViolation),
    #[error("expected file length must be non-zero")]
    ZeroExpectedLength,
    #[error("mapping generation must be non-zero")]
    ZeroMappingGeneration,
    #[error("retirement nonce must be non-zero")]
    ZeroRetirementNonce,
    #[error("registration belongs to a different Store UUID")]
    StoreUuidMismatch,
    #[error("file incarnation {incarnation:?} is already registered")]
    DuplicateIncarnation { incarnation: FileIncarnationId },
    #[error("canonical path {path:?} is reserved by {incumbent:?}")]
    CanonicalPathReserved {
        path: StoreRelativePath,
        incumbent: FileIncarnationId,
    },
    #[error("physical key {physical_key:?} is reserved by {incumbent:?}")]
    PhysicalKeyReserved {
        physical_key: PhysicalFileKey,
        incumbent: FileIncarnationId,
    },
    #[error("mapped-file owner is already registered as {incumbent:?}")]
    OwnerAlreadyRegistered { incumbent: FileIncarnationId },
    #[error("managed queue member has no complete reconciled identity binding")]
    ManagedQueueBindingMissing,
    #[error("replay-validated retirement ticket {ticket_id:?} cannot be represented by the registry")]
    InvalidRecoveredRetirement { ticket_id: TicketId },
    #[error("file incarnation is not registered")]
    UnknownIncarnation { incarnation: FileIncarnationId },
    #[error("file incarnation is not active")]
    IncarnationNotActive { incarnation: FileIncarnationId },
    #[error("another durable-intent reservation already serializes the ticket high-water")]
    IntentReservationBusy,
    #[error("registered physical key differs from the retirement operation")]
    PhysicalKeyMismatch { incarnation: FileIncarnationId },
    #[error("registered canonical path differs from the retirement operation")]
    CanonicalPathMismatch { incarnation: FileIncarnationId },
    #[error("registered segment offset differs from the retirement operation")]
    SegmentOffsetMismatch { incarnation: FileIncarnationId },
    #[error("registered expected length differs from the retirement operation")]
    ExpectedLengthMismatch { incarnation: FileIncarnationId },
    #[error("registered owner Arc differs from the retirement operation")]
    OwnerIdentityMismatch { incarnation: FileIncarnationId },
    #[error("registered queue identity differs from the retirement operation")]
    QueueIdentityMismatch { incarnation: FileIncarnationId },
    #[error("retirement ticket high-water is exhausted")]
    TicketHighWaterExhausted,
    #[error("registry requires replay before another capability transition")]
    NeedsRecovery,
    #[error("durable evidence is not a RetirementIntent")]
    EvidenceIsNotRetirementIntent,
    #[error("durability coordinates are invalid or overflowed")]
    InvalidDurabilityCoordinates,
    #[error("durable evidence does not match reserved ticket {ticket_id:?}")]
    DurableEvidenceMismatch { ticket_id: TicketId },
    #[error("replayed intent restoration requires replay-derived evidence")]
    ReplayEvidenceRequired,
    #[error("an in-flight append can commit only writer-derived evidence")]
    WriterEvidenceRequired,
    #[error("replayed ticket exceeds the recovered durable ticket high-water")]
    ReplayedTicketAboveHighWater { ticket_id: TicketId, high_water: u64 },
    #[error("durable ticket is already registered")]
    DuplicateTicket { ticket_id: TicketId },
    #[error("token was minted by a different registry instance")]
    ForeignToken,
    #[error("token binding does not match the requested handoff")]
    TokenBindingMismatch { ticket_id: TicketId },
    #[error("queue identity does not match the token")]
    TokenQueueIdentityMismatch { ticket_id: TicketId },
    #[error("token is not in the issued state")]
    TokenNotIssued { ticket_id: TicketId },
    #[error("prepared handoff no longer matches registry state")]
    HandoffPreparationLost { ticket_id: TicketId },
    #[error("durable stage evidence does not match ticket {ticket_id:?} or its predecessor")]
    DurableStageEvidenceMismatch { ticket_id: TicketId },
    #[error("namespace proof does not match durable ticket {ticket_id:?}")]
    NamespaceProofMismatch { ticket_id: TicketId },
}

/// Private owner failure for registry construction and registration.
#[derive(Debug, Error)]
pub(in crate::mapped_file::retirement) enum RegistryFault {
    #[error("registry allocation failed")]
    Allocation(#[source] TryReserveError),
    #[error(transparent)]
    Contract(#[from] RegistryViolation),
}
