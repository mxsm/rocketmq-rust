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

use std::collections::BTreeMap;

use thiserror::Error;

use super::codec::LedgerRecord;
use super::identity::FileIncarnationId;
use super::identity::PhysicalFileKey;
use super::identity::StoreRelativePath;
use super::identity::StoreUuid;
use super::identity::TicketId;
use super::sidecar::IncarnationPhase;
use super::sidecar::IncarnationSnapshotEntry;
use super::sidecar::LifecycleSnapshot;
use super::sidecar::QuarantineSnapshotEntry;
use super::sidecar::RetirementStage;
use super::sidecar::RetirementTicketSnapshotEntry;
use super::sidecar::SnapshotEntry;
use super::sidecar::SnapshotMode;

mod apply;
mod projection;
#[allow(
    dead_code,
    reason = "M3 stages pure startup reconciliation before the handle-relative inventory provider is wired"
)]
pub(crate) mod reconciliation;
mod retirement;
mod validation;

/// Eligibility retained for a terminal ticket after replay.
///
/// M3.2 cannot advance this value: only a later clean-start namespace reconciliation may make a
/// completed ticket eligible for omission by a subsequent compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletedRetainedEligibility {
    RequiresCleanStartRevalidation,
}

/// Opaque, replay-validated ledger state that is not a publication capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveredLedgerState {
    store_uuid: StoreUuid,
    generation: u64,
    last_sequence: u64,
    acknowledgement_epoch: u64,
    marker_epoch: u64,
    create_high_water: u64,
    ticket_high_water: u64,
    incarnations: BTreeMap<FileIncarnationId, IncarnationSnapshotEntry>,
    retirements: BTreeMap<TicketId, RetirementState>,
    quarantines: BTreeMap<StoreRelativePath, QuarantineSnapshotEntry>,
}

impl RecoveredLedgerState {
    pub(crate) const fn store_uuid(&self) -> StoreUuid {
        self.store_uuid
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub(crate) const fn acknowledgement_epoch(&self) -> u64 {
        self.acknowledgement_epoch
    }

    pub(crate) const fn marker_epoch(&self) -> u64 {
        self.marker_epoch
    }

    pub(crate) const fn create_high_water(&self) -> u64 {
        self.create_high_water
    }

    pub(crate) const fn ticket_high_water(&self) -> u64 {
        self.ticket_high_water
    }

    pub(crate) fn incarnation_phase(&self, incarnation: FileIncarnationId) -> Option<IncarnationPhase> {
        self.incarnations.get(&incarnation).map(|entry| entry.phase)
    }

    pub(crate) fn retirement_stage(&self, ticket_id: TicketId) -> Option<RetirementStage> {
        self.retirements.get(&ticket_id).map(|state| state.entry.stage)
    }

    pub(crate) fn completed_retained_eligibility(&self, ticket_id: TicketId) -> Option<CompletedRetainedEligibility> {
        self.retirements
            .get(&ticket_id)
            .and_then(|state| state.completed_retained_eligibility)
    }

    pub(crate) fn incarnation_count(&self) -> usize {
        self.incarnations.len()
    }

    pub(crate) fn retirement_count(&self) -> usize {
        self.retirements.len()
    }

    pub(crate) fn quarantine_count(&self) -> usize {
        self.quarantines.len()
    }

    /// Exposes only replay-validated retirement bindings to the registry reconstruction layer.
    ///
    /// Runtime owners and queue identities are intentionally absent: a restart must rebuild
    /// pending durable work without fabricating process-local publication authority.
    pub(super) fn retirement_entries(
        &self,
    ) -> impl Iterator<Item = (&RetirementTicketSnapshotEntry, Option<PhysicalFileKey>)> {
        self.retirements
            .values()
            .map(|state| (&state.entry, state.observed_replacement_key))
    }
}

/// Replay-validated coordinates for reopening the selected ledger writer.
///
/// This value is descriptive evidence, not write authority. Wave-B activation must pair it with
/// the unique, consumed reconciliation session that retains the exclusive Store-root proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriterRecoveryFrontier {
    store_uuid: StoreUuid,
    bootstrap_id: [u8; 16],
    log_generation: u64,
    next_sequence: u64,
    next_acknowledgement_epoch: u64,
    sealed_log_length: u64,
    marker_epoch: u64,
}

impl WriterRecoveryFrontier {
    #[allow(
        clippy::too_many_arguments,
        reason = "the replay frontier mirrors seven independently persisted writer coordinates"
    )]
    pub(super) const fn from_validated_replay(
        store_uuid: StoreUuid,
        bootstrap_id: [u8; 16],
        log_generation: u64,
        next_sequence: u64,
        next_acknowledgement_epoch: u64,
        sealed_log_length: u64,
        marker_epoch: u64,
    ) -> Self {
        Self {
            store_uuid,
            bootstrap_id,
            log_generation,
            next_sequence,
            next_acknowledgement_epoch,
            sealed_log_length,
            marker_epoch,
        }
    }

    pub(crate) const fn store_uuid(&self) -> StoreUuid {
        self.store_uuid
    }

    pub(crate) const fn bootstrap_id(&self) -> [u8; 16] {
        self.bootstrap_id
    }

    pub(crate) const fn log_generation(&self) -> u64 {
        self.log_generation
    }

    pub(crate) const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub(crate) const fn next_acknowledgement_epoch(&self) -> u64 {
        self.next_acknowledgement_epoch
    }

    pub(crate) const fn sealed_log_length(&self) -> u64 {
        self.sealed_log_length
    }

    pub(crate) const fn marker_epoch(&self) -> u64 {
        self.marker_epoch
    }
}

/// Successful ledger replay that still requires namespace and index reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NeedsReconciliation {
    recovered: RecoveredLedgerState,
    writer_frontier: WriterRecoveryFrontier,
}

impl NeedsReconciliation {
    pub(super) const fn new(recovered: RecoveredLedgerState, writer_frontier: WriterRecoveryFrontier) -> Self {
        Self {
            recovered,
            writer_frontier,
        }
    }

    pub(crate) const fn recovered(&self) -> &RecoveredLedgerState {
        &self.recovered
    }

    pub(crate) const fn writer_frontier(&self) -> &WriterRecoveryFrontier {
        &self.writer_frontier
    }

    pub(crate) fn into_parts(self) -> (RecoveredLedgerState, WriterRecoveryFrontier) {
        (self.recovered, self.writer_frontier)
    }

    #[cfg(test)]
    pub(super) fn for_test(recovered: RecoveredLedgerState) -> Self {
        let writer_frontier = WriterRecoveryFrontier::from_validated_replay(
            recovered.store_uuid,
            [1; 16],
            recovered.generation,
            recovered
                .last_sequence
                .checked_add(1)
                .expect("test recovered sequence has writer headroom"),
            recovered
                .acknowledgement_epoch
                .checked_add(1)
                .expect("test acknowledgement epoch has writer headroom"),
            1,
            recovered.marker_epoch,
        );
        Self::new(recovered, writer_frontier)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetirementState {
    entry: RetirementTicketSnapshotEntry,
    observed_replacement_key: Option<PhysicalFileKey>,
    last_stage_payload: Option<RetirementStagePayload>,
    completed_retained_eligibility: Option<CompletedRetainedEligibility>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RetirementStagePayload {
    LogicalRemoved,
    Tombstoned,
    NamespaceAbsent {
        replacement_observed: bool,
        observation_time_ns: u64,
    },
    Completed {
        completion_time_ns: u64,
        namespace_absent_sequence: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedGeneration {
    sequence: u64,
    target_generation: u64,
}

/// Mutable pure state used while applying a selected snapshot and its acknowledged records.
pub(super) struct LedgerStateMachine {
    store_uuid: StoreUuid,
    generation: u64,
    snapshot_base_sequence: u64,
    next_sequence: u64,
    create_high_water: u64,
    ticket_high_water: u64,
    incarnations: BTreeMap<FileIncarnationId, IncarnationSnapshotEntry>,
    incarnation_by_canonical_path: BTreeMap<StoreRelativePath, FileIncarnationId>,
    incarnation_by_create_path: BTreeMap<StoreRelativePath, FileIncarnationId>,
    retirements: BTreeMap<TicketId, RetirementState>,
    ticket_by_incarnation: BTreeMap<FileIncarnationId, TicketId>,
    quarantines: BTreeMap<StoreRelativePath, QuarantineSnapshotEntry>,
    prepared_generation: Option<PreparedGeneration>,
}

impl LedgerStateMachine {
    pub(super) fn from_snapshot(snapshot: impl std::borrow::Borrow<LifecycleSnapshot>) -> Result<Self, StateViolation> {
        let snapshot = std::borrow::Borrow::borrow(&snapshot);
        validation::validate_snapshot_header(snapshot)?;
        let next_sequence = snapshot
            .base_sequence
            .checked_add(1)
            .ok_or(StateViolation::SequenceOverflow)?;
        let mut state = Self {
            store_uuid: snapshot.store_uuid,
            generation: snapshot.generation,
            snapshot_base_sequence: snapshot.base_sequence,
            next_sequence,
            create_high_water: snapshot.create_high_water,
            ticket_high_water: snapshot.ticket_high_water,
            incarnations: BTreeMap::new(),
            incarnation_by_canonical_path: BTreeMap::new(),
            incarnation_by_create_path: BTreeMap::new(),
            retirements: BTreeMap::new(),
            ticket_by_incarnation: BTreeMap::new(),
            quarantines: BTreeMap::new(),
            prepared_generation: None,
        };
        let mut retirements = Vec::new();
        let mut quarantines = Vec::new();
        for entry in &snapshot.entries {
            match entry {
                SnapshotEntry::Incarnation(entry) => state.insert_snapshot_incarnation(entry.clone())?,
                SnapshotEntry::RetirementTicket(entry) => retirements.push(entry.clone()),
                SnapshotEntry::Quarantine(entry) => quarantines.push(entry.clone()),
            }
        }
        for entry in retirements {
            state.insert_snapshot_retirement(entry)?;
        }
        for entry in quarantines {
            state.insert_snapshot_quarantine(entry)?;
        }
        Ok(state)
    }

    pub(super) fn prepared_generation(&self) -> Option<(u64, u64)> {
        self.prepared_generation
            .as_ref()
            .map(|prepared| (prepared.sequence, prepared.target_generation))
    }

    pub(super) fn apply(&mut self, sequence: u64, record: Option<LedgerRecord>) -> Result<(), StateViolation> {
        if sequence != self.next_sequence {
            return Err(StateViolation::SequenceMismatch {
                expected: self.next_sequence,
                actual: sequence,
            });
        }
        let following_sequence = sequence.checked_add(1).ok_or(StateViolation::SequenceOverflow)?;
        self.validate_append_barrier(sequence, record.as_ref())?;
        self.apply_record(sequence, record)?;
        self.next_sequence = following_sequence;
        Ok(())
    }

    pub(super) fn finish(
        self,
        last_sequence: u64,
        acknowledgement_epoch: u64,
        marker_epoch: u64,
    ) -> Result<RecoveredLedgerState, StateViolation> {
        if acknowledgement_epoch == 0 || marker_epoch == 0 {
            return Err(StateViolation::ZeroRecoveryEpoch);
        }
        if self.prepared_generation.is_some() {
            return Err(StateViolation::IllegalGenerationAdministration);
        }
        let expected_last = self
            .next_sequence
            .checked_sub(1)
            .ok_or(StateViolation::SequenceOverflow)?;
        if last_sequence != expected_last {
            return Err(StateViolation::SequenceMismatch {
                expected: expected_last,
                actual: last_sequence,
            });
        }
        Ok(RecoveredLedgerState {
            store_uuid: self.store_uuid,
            generation: self.generation,
            last_sequence,
            acknowledgement_epoch,
            marker_epoch,
            create_high_water: self.create_high_water,
            ticket_high_water: self.ticket_high_water,
            incarnations: self.incarnations,
            retirements: self.retirements,
            quarantines: self.quarantines,
        })
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum StateViolation {
    #[error("snapshot or record belongs to another store UUID")]
    StoreUuidMismatch,
    #[error("expected sequence {expected}, found {actual}")]
    SequenceMismatch { expected: u64, actual: u64 },
    #[error("record sequence domain is exhausted")]
    SequenceOverflow,
    #[error("{field} high-water domain is exhausted")]
    HighWaterOverflow { field: &'static str },
    #[error("{field} must advance from {previous} to {expected}, found {actual}")]
    HighWaterMismatch {
        field: &'static str,
        previous: u64,
        expected: u64,
        actual: u64,
    },
    #[error("duplicate {entity} changes persisted identity")]
    IdentityChangingDuplicate { entity: &'static str },
    #[error("incarnation does not exist")]
    MissingIncarnation,
    #[error("incarnation transition from {from:?} to {to:?} is invalid")]
    InvalidIncarnationTransition {
        from: Option<IncarnationPhase>,
        to: IncarnationPhase,
    },
    #[error("retirement ticket does not exist")]
    MissingRetirementTicket,
    #[error("another retirement ticket already owns this incarnation")]
    ConcurrentRetirementTicket,
    #[error("retirement transition from {from:?} to {to:?} is invalid")]
    InvalidRetirementTransition {
        from: Option<RetirementStage>,
        to: RetirementStage,
    },
    #[error("record identity does not match its persisted incarnation or ticket")]
    RecordIdentityMismatch,
    #[error("generation administration record is illegal in the current state")]
    IllegalGenerationAdministration,
    #[error("ordinary record follows an unmatched GenerationPrepared barrier")]
    AppendAfterGenerationPrepared,
    #[error("snapshot contains internally inconsistent retirement state")]
    InvalidSnapshotState,
    #[error("acknowledgement or marker epoch is zero")]
    ZeroRecoveryEpoch,
}

#[cfg(test)]
mod tests;
