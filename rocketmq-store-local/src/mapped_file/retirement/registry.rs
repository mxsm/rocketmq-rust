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
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;

use super::identity::FileIncarnationId;
use super::identity::PhysicalFileKey;
use super::identity::StoreRelativePath;
use super::identity::StoreUuid;
use super::identity::TicketId;

mod authority;
mod guards;
mod transitions;
mod types;

mod queue_slot;
pub(super) mod reaper;
mod recovery;

pub(super) use queue_slot::CreationPublicationFailure;
pub use queue_slot::ManagedMappedFileQueueGeneration;
pub(in crate::mapped_file) use queue_slot::ManagedQueueMember;
pub use queue_slot::MappedFileQueueGeneration;
pub use queue_slot::MappedFileQueueSnapshot;

pub(crate) use guards::PreparedQueueHandoff;
pub(crate) use guards::PreparedRetirementIntent;
pub(crate) use guards::RetirementIntentAppend;
pub(super) use transitions::commit_writer_completed;
pub(super) use transitions::commit_writer_intent;
pub(super) use transitions::commit_writer_logical_removed;
pub(super) use transitions::commit_writer_namespace_absent;
pub(super) use transitions::commit_writer_namespace_absent_after_tombstone;
pub(super) use transitions::commit_writer_superseded_path_after_logical;
pub(super) use transitions::commit_writer_tombstoned;
pub(super) use transitions::completed_record;
pub(super) use transitions::namespace_absent_after_tombstone_record;
pub(super) use transitions::namespace_absent_record;
pub(super) use transitions::superseded_path_record;
pub(super) use transitions::tombstoned_record;
pub(crate) use types::CompletedRetirementReceipt;
use types::DurabilityCoordinates;
use types::DurableEvidenceSource;
pub(crate) use types::DurableIntentEvidence;
pub(crate) use types::DurableRetirementToken;
use types::DurableStagePosition;
pub(crate) use types::LogicalRemovedCapability;
pub(crate) use types::NamespaceAbsentCapability;
pub(crate) use types::PublishedFileRegistration;
pub(crate) use types::QueueIdentity;
pub(in crate::mapped_file::retirement) use types::RegistryFault;
use types::RegistrySeal;
pub(crate) use types::RegistryViolation;
pub(crate) use types::RetirementHandoffCapability;
pub(crate) use types::RetirementIntentBinding;
pub(crate) use types::RetirementOperation;
pub(crate) use types::TombstonedCapability;

/// Validation failure that preserves ownership of the exact unconsumed token.
#[derive(Debug)]
pub(crate) struct HandoffPreparationFailure<O> {
    token: Box<DurableRetirementToken<O>>,
    error: RegistryViolation,
}

impl<O> HandoffPreparationFailure<O> {
    fn new(token: DurableRetirementToken<O>, error: RegistryViolation) -> Self {
        Self {
            token: Box::new(token),
            error,
        }
    }

    /// Returns the original token and typed validation error.
    pub(crate) fn into_parts(self) -> (DurableRetirementToken<O>, RegistryViolation) {
        (*self.token, self.error)
    }
}

/// Strong process-local ownership for every published or retiring incarnation.
///
/// The registry deliberately serializes preparation of one durable intent at a time. This keeps
/// the next ticket equal to the durable high-water plus one without holding the mutex across any
/// ledger I/O. A reservation advances the high-water only after exact durable evidence returns.
pub(crate) struct RetirementRegistry<O> {
    authority: Arc<RegistryAuthority<O>>,
}

pub(super) struct RegistryAuthority<O> {
    store_uuid: StoreUuid,
    seal: RegistrySeal,
    state: Mutex<RegistryState<O>>,
}

impl<O> fmt::Debug for RegistryAuthority<O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryAuthority")
            .field("store_uuid", &self.store_uuid)
            .finish_non_exhaustive()
    }
}

struct RegistryState<O> {
    ticket_high_water: u64,
    entries: BTreeMap<FileIncarnationId, StrongRegistryEntry<O>>,
    incarnation_by_path: BTreeMap<StoreRelativePath, FileIncarnationId>,
    incarnation_by_key: BTreeMap<PhysicalFileKey, FileIncarnationId>,
    incarnation_by_owner: BTreeMap<usize, FileIncarnationId>,
    incarnation_by_ticket: BTreeMap<TicketId, FileIncarnationId>,
    pending: Option<RetirementIntentBinding>,
    needs_recovery: bool,
}

struct StrongRegistryEntry<O> {
    physical_key: PhysicalFileKey,
    canonical_path: StoreRelativePath,
    segment_offset: u64,
    expected_length: u64,
    runtime_identity: RuntimeIdentity<O>,
    phase: RegistryEntryPhase,
}

enum RuntimeIdentity<O> {
    Active {
        owner: Arc<O>,
        queue_identity: QueueIdentity,
    },
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RegistryEntryPhase {
    Active,
    IntentReserved(TicketId),
    IntentDurable {
        binding: RetirementIntentBinding,
        durability: DurableStagePosition,
        handoff: HandoffState,
    },
    LogicalRemoved {
        binding: RetirementIntentBinding,
        durability: DurableStagePosition,
        append_durability: DurableStagePosition,
        observed_replacement_key: Option<PhysicalFileKey>,
    },
    Tombstoned {
        binding: RetirementIntentBinding,
        durability: DurableStagePosition,
        append_durability: DurableStagePosition,
        tombstone_path: StoreRelativePath,
        observed_replacement_key: Option<PhysicalFileKey>,
    },
    NamespaceAbsent {
        binding: RetirementIntentBinding,
        durability: DurableStagePosition,
        append_durability: DurableStagePosition,
        tombstone_path: Option<StoreRelativePath>,
        observed_replacement_key: Option<PhysicalFileKey>,
    },
    CompletedRetained {
        binding: RetirementIntentBinding,
        durability: DurableStagePosition,
        observed_replacement_key: Option<PhysicalFileKey>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandoffState {
    TokenIssued,
    Prepared,
    Consumed,
}

/// Pending durable work reconstructed without fabricating a live mapped-file owner.
pub(in crate::mapped_file::retirement) enum RecoveredRetirementWork<O> {
    LogicalRemoval(RetirementHandoffCapability<O>),
    Namespace(LogicalRemovedCapability<O>),
    TombstoneRemoval(TombstonedCapability<O>),
    Completion(NamespaceAbsentCapability<O>),
}

impl<O> RetirementRegistry<O> {
    /// Starts a registry from a replay-validated durable ticket high-water.
    fn new(store_uuid: StoreUuid, ticket_high_water: u64) -> Self {
        Self {
            authority: Arc::new(RegistryAuthority {
                store_uuid,
                seal: RegistrySeal::default(),
                state: Mutex::new(RegistryState {
                    ticket_high_water,
                    entries: BTreeMap::new(),
                    incarnation_by_path: BTreeMap::new(),
                    incarnation_by_key: BTreeMap::new(),
                    incarnation_by_owner: BTreeMap::new(),
                    incarnation_by_ticket: BTreeMap::new(),
                    pending: None,
                    needs_recovery: false,
                }),
            }),
        }
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement) fn new_for_test(store_uuid: StoreUuid, ticket_high_water: u64) -> Self {
        Self::new(store_uuid, ticket_high_water)
    }

    /// Adds one published incarnation while reserving its identity, path, and physical key.
    fn register_published(&self, registration: PublishedFileRegistration<O>) -> Result<(), RegistryViolation> {
        self.register_published_batch(vec![registration])
    }

    /// Atomically registers one reconciled queue generation.
    ///
    /// Every identity and uniqueness check completes before the first registry mutation, so a
    /// conflict cannot publish a prefix of the recovered generation.
    fn register_published_batch(
        &self,
        registrations: Vec<PublishedFileRegistration<O>>,
    ) -> Result<(), RegistryViolation> {
        if registrations
            .iter()
            .any(|registration| registration.incarnation.store_uuid() != self.authority.store_uuid)
        {
            return Err(RegistryViolation::StoreUuidMismatch);
        }

        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(RegistryViolation::NeedsRecovery);
        }

        let mut batch_incarnations = BTreeMap::new();
        let mut batch_paths = BTreeMap::new();
        let mut batch_keys = BTreeMap::new();
        let mut batch_owners = BTreeMap::new();
        for registration in &registrations {
            if state.entries.contains_key(&registration.incarnation)
                || batch_incarnations.insert(registration.incarnation, ()).is_some()
            {
                return Err(RegistryViolation::DuplicateIncarnation {
                    incarnation: registration.incarnation,
                });
            }
            if let Some(incumbent) = state
                .incarnation_by_path
                .get(&registration.canonical_path)
                .or_else(|| batch_paths.get(&registration.canonical_path))
            {
                return Err(RegistryViolation::CanonicalPathReserved {
                    path: registration.canonical_path.clone(),
                    incumbent: *incumbent,
                });
            }
            batch_paths.insert(registration.canonical_path.clone(), registration.incarnation);
            if let Some(incumbent) = state
                .incarnation_by_key
                .get(&registration.physical_key)
                .or_else(|| batch_keys.get(&registration.physical_key))
            {
                return Err(RegistryViolation::PhysicalKeyReserved {
                    physical_key: registration.physical_key,
                    incumbent: *incumbent,
                });
            }
            batch_keys.insert(registration.physical_key, registration.incarnation);
            let owner_identity = Arc::as_ptr(&registration.owner) as usize;
            if let Some(incumbent) = state
                .incarnation_by_owner
                .get(&owner_identity)
                .or_else(|| batch_owners.get(&owner_identity))
            {
                return Err(RegistryViolation::OwnerAlreadyRegistered { incumbent: *incumbent });
            }
            batch_owners.insert(owner_identity, registration.incarnation);
        }

        for registration in registrations {
            let owner_identity = Arc::as_ptr(&registration.owner) as usize;
            state
                .incarnation_by_path
                .insert(registration.canonical_path.clone(), registration.incarnation);
            state
                .incarnation_by_key
                .insert(registration.physical_key, registration.incarnation);
            state
                .incarnation_by_owner
                .insert(owner_identity, registration.incarnation);
            state.entries.insert(
                registration.incarnation,
                StrongRegistryEntry {
                    physical_key: registration.physical_key,
                    canonical_path: registration.canonical_path,
                    segment_offset: registration.segment_offset,
                    expected_length: registration.expected_length,
                    runtime_identity: RuntimeIdentity::Active {
                        owner: registration.owner,
                        queue_identity: registration.queue_identity,
                    },
                    phase: RegistryEntryPhase::Active,
                },
            );
        }
        Ok(())
    }

    /// Reserves the exact next ticket and releases the registry mutex before ledger I/O.
    pub(crate) fn prepare_retirement(
        &self,
        operation: RetirementOperation,
        owner: &Arc<O>,
        queue_identity: &QueueIdentity,
    ) -> Result<PreparedRetirementIntent<'_, O>, RegistryViolation> {
        if operation.incarnation().store_uuid() != self.authority.store_uuid {
            return Err(RegistryViolation::StoreUuidMismatch);
        }

        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(RegistryViolation::NeedsRecovery);
        }
        if state.pending.is_some() {
            return Err(RegistryViolation::IntentReservationBusy);
        }
        let ticket_value = state
            .ticket_high_water
            .checked_add(1)
            .ok_or(RegistryViolation::TicketHighWaterExhausted)?;
        let ticket_id = TicketId::new(ticket_value).map_err(|_| RegistryViolation::TicketHighWaterExhausted)?;
        let incarnation = operation.incarnation();
        let entry = state
            .entries
            .get(&incarnation)
            .ok_or(RegistryViolation::UnknownIncarnation { incarnation })?;
        validate_entry(entry, &operation, owner, queue_identity)?;
        if entry.phase != RegistryEntryPhase::Active {
            return Err(RegistryViolation::IncarnationNotActive { incarnation });
        }

        let binding = RetirementIntentBinding { ticket_id, operation };
        state
            .entries
            .get_mut(&incarnation)
            .ok_or(RegistryViolation::UnknownIncarnation { incarnation })?
            .phase = RegistryEntryPhase::IntentReserved(ticket_id);
        state.pending = Some(binding.clone());
        drop(state);

        Ok(PreparedRetirementIntent {
            registry: self,
            binding,
            active: true,
        })
    }

    /// Reconstructs one token from opaque replay proof for an already durable ticket.
    pub(crate) fn restore_replayed_intent(
        &self,
        evidence: DurableIntentEvidence,
        owner: &Arc<O>,
        queue_identity: &QueueIdentity,
    ) -> Result<DurableRetirementToken<O>, RegistryViolation> {
        if evidence.binding.incarnation().store_uuid() != self.authority.store_uuid {
            return Err(RegistryViolation::StoreUuidMismatch);
        }

        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(RegistryViolation::NeedsRecovery);
        }
        if evidence.source != DurableEvidenceSource::Replay {
            return fence_consumed_evidence(&mut state, RegistryViolation::ReplayEvidenceRequired);
        }
        if state.pending.is_some() {
            return fence_consumed_evidence(&mut state, RegistryViolation::IntentReservationBusy);
        }
        let ticket_id = evidence.binding.ticket_id();
        if ticket_id.get() > state.ticket_high_water {
            let high_water = state.ticket_high_water;
            return fence_consumed_evidence(
                &mut state,
                RegistryViolation::ReplayedTicketAboveHighWater { ticket_id, high_water },
            );
        }
        if state.incarnation_by_ticket.contains_key(&ticket_id) {
            return fence_consumed_evidence(&mut state, RegistryViolation::DuplicateTicket { ticket_id });
        }
        let incarnation = evidence.binding.incarnation();
        let validation = state
            .entries
            .get(&incarnation)
            .ok_or(RegistryViolation::UnknownIncarnation { incarnation })
            .and_then(|entry| {
                validate_entry(entry, &evidence.binding.operation, owner, queue_identity)?;
                if entry.phase == RegistryEntryPhase::Active {
                    Ok(())
                } else {
                    Err(RegistryViolation::IncarnationNotActive { incarnation })
                }
            });
        if let Err(error) = validation {
            return fence_consumed_evidence(&mut state, error);
        }
        let Some(entry) = state.entries.get(&incarnation) else {
            return fence_consumed_evidence(&mut state, RegistryViolation::UnknownIncarnation { incarnation });
        };
        let runtime_identity = active_runtime_identity(entry, incarnation);
        let (token_owner, token_queue) = match runtime_identity {
            Ok(identity) => identity,
            Err(error) => return fence_consumed_evidence(&mut state, error),
        };
        let binding = evidence.binding;
        let durability = evidence.durability;

        let Some(entry) = state.entries.get_mut(&incarnation) else {
            return fence_consumed_evidence(&mut state, RegistryViolation::UnknownIncarnation { incarnation });
        };
        entry.phase = RegistryEntryPhase::IntentDurable {
            binding: binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            handoff: HandoffState::TokenIssued,
        };
        state.incarnation_by_ticket.insert(ticket_id, incarnation);
        Ok(DurableRetirementToken {
            authority: Arc::clone(&self.authority),
            binding,
            durability,
            owner: token_owner,
            queue_identity: token_queue,
            armed: true,
        })
    }

    /// Prepares an exact handoff while returning an untouched token for every validation failure.
    ///
    /// The returned capability holds no registry guard. The caller may perform one side-effect-free
    /// ArcSwap compare-and-swap attempt, then either roll back the capability or commit it.
    pub(crate) fn prepare_handoff<'a>(
        &'a self,
        token: DurableRetirementToken<O>,
        expected: &RetirementIntentBinding,
        queue_identity: &QueueIdentity,
    ) -> Result<PreparedQueueHandoff<'a, O>, HandoffPreparationFailure<O>> {
        if !Arc::ptr_eq(&self.authority, &token.authority) {
            return Err(HandoffPreparationFailure::new(token, RegistryViolation::ForeignToken));
        }
        if token.binding != *expected {
            let ticket_id = token.binding.ticket_id();
            return Err(HandoffPreparationFailure::new(
                token,
                RegistryViolation::TokenBindingMismatch { ticket_id },
            ));
        }
        if !token.queue_identity.same_as(queue_identity) {
            let ticket_id = token.binding.ticket_id();
            return Err(HandoffPreparationFailure::new(
                token,
                RegistryViolation::TokenQueueIdentityMismatch { ticket_id },
            ));
        }

        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(HandoffPreparationFailure::new(token, RegistryViolation::NeedsRecovery));
        }
        let ticket_id = token.binding.ticket_id();
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(HandoffPreparationFailure::new(
                token,
                RegistryViolation::TokenNotIssued { ticket_id },
            ));
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(HandoffPreparationFailure::new(
                token,
                RegistryViolation::TokenNotIssued { ticket_id },
            ));
        };
        let transitioned = match &mut entry.phase {
            RegistryEntryPhase::IntentDurable { binding, handoff, .. }
                if binding == &token.binding && *handoff == HandoffState::TokenIssued =>
            {
                *handoff = HandoffState::Prepared;
                true
            }
            _ => false,
        };
        if !transitioned {
            state.needs_recovery = true;
            return Err(HandoffPreparationFailure::new(
                token,
                RegistryViolation::TokenNotIssued { ticket_id },
            ));
        }
        drop(state);
        Ok(PreparedQueueHandoff {
            registry: self,
            token: Some(token),
            armed: true,
        })
    }

    pub(crate) fn ticket_high_water(&self) -> u64 {
        self.authority.state.lock().ticket_high_water
    }

    pub(crate) fn retained_identity_count(&self) -> usize {
        self.authority.state.lock().entries.len()
    }

    pub(crate) fn contains_incarnation(&self, incarnation: FileIncarnationId) -> bool {
        self.authority.state.lock().entries.contains_key(&incarnation)
    }

    pub(crate) fn is_path_reserved(&self, path: &StoreRelativePath) -> bool {
        self.authority.state.lock().incarnation_by_path.contains_key(path)
    }

    pub(crate) fn needs_recovery(&self) -> bool {
        self.authority.needs_recovery()
    }

    #[cfg(test)]
    fn logical_removed_sequence_for_test(&self, ticket_id: TicketId) -> Option<u64> {
        let state = self.authority.state.lock();
        let incarnation = state.incarnation_by_ticket.get(&ticket_id)?;
        match &state.entries.get(incarnation)?.phase {
            RegistryEntryPhase::LogicalRemoved { durability, .. } => Some(durability.sequence()),
            _ => None,
        }
    }

    fn is_recovery_fenced(&self, state: &RegistryState<O>) -> bool {
        state.needs_recovery || self.authority.seal.needs_recovery()
    }

    fn commit_prepared(
        &self,
        binding: &RetirementIntentBinding,
        evidence: DurableIntentEvidence,
    ) -> Result<DurableRetirementToken<O>, RegistryViolation> {
        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(RegistryViolation::NeedsRecovery);
        }
        if evidence.source != DurableEvidenceSource::Writer {
            state.needs_recovery = true;
            return Err(RegistryViolation::WriterEvidenceRequired);
        }
        let exact_pending = state.pending.as_ref() == Some(binding);
        let incarnation = binding.incarnation();
        let exact_phase = state
            .entries
            .get(&incarnation)
            .is_some_and(|entry| entry.phase == RegistryEntryPhase::IntentReserved(binding.ticket_id()));
        if !exact_pending || !exact_phase || evidence.binding != *binding {
            state.needs_recovery = true;
            return Err(RegistryViolation::DurableEvidenceMismatch {
                ticket_id: binding.ticket_id(),
            });
        }
        let ticket_id = binding.ticket_id();
        let Some(expected_ticket) = state.ticket_high_water.checked_add(1) else {
            state.needs_recovery = true;
            return Err(RegistryViolation::TicketHighWaterExhausted);
        };
        if expected_ticket != ticket_id.get() || state.incarnation_by_ticket.contains_key(&ticket_id) {
            state.needs_recovery = true;
            return Err(RegistryViolation::DurableEvidenceMismatch { ticket_id });
        }

        let Some(entry) = state.entries.get(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryViolation::UnknownIncarnation { incarnation });
        };
        let runtime_identity = active_runtime_identity(entry, incarnation);
        let (token_owner, token_queue) = match runtime_identity {
            Ok(identity) => identity,
            Err(error) => {
                state.needs_recovery = true;
                return Err(error);
            }
        };
        let durability = evidence.durability;
        state.ticket_high_water = ticket_id.get();
        state.pending = None;
        state.incarnation_by_ticket.insert(ticket_id, incarnation);
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryViolation::UnknownIncarnation { incarnation });
        };
        entry.phase = RegistryEntryPhase::IntentDurable {
            binding: binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            handoff: HandoffState::TokenIssued,
        };
        Ok(DurableRetirementToken {
            authority: Arc::clone(&self.authority),
            binding: binding.clone(),
            durability,
            owner: token_owner,
            queue_identity: token_queue,
            armed: true,
        })
    }

    fn rollback_prepared(&self, binding: &RetirementIntentBinding) {
        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return;
        }
        let incarnation = binding.incarnation();
        let exact_pending = state.pending.as_ref() == Some(binding);
        let exact_phase = state
            .entries
            .get(&incarnation)
            .is_some_and(|entry| entry.phase == RegistryEntryPhase::IntentReserved(binding.ticket_id()));
        if !exact_pending || !exact_phase {
            state.needs_recovery = true;
            return;
        }
        state.pending = None;
        if let Some(entry) = state.entries.get_mut(&incarnation) {
            entry.phase = RegistryEntryPhase::Active;
        } else {
            state.needs_recovery = true;
        }
    }

    fn rollback_handoff(&self, token: &DurableRetirementToken<O>) -> Result<(), RegistryViolation> {
        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(RegistryViolation::NeedsRecovery);
        }
        let ticket_id = token.binding.ticket_id();
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryViolation::HandoffPreparationLost { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryViolation::HandoffPreparationLost { ticket_id });
        };
        let transitioned = match &mut entry.phase {
            RegistryEntryPhase::IntentDurable { binding, handoff, .. }
                if binding == &token.binding && *handoff == HandoffState::Prepared =>
            {
                *handoff = HandoffState::TokenIssued;
                true
            }
            _ => false,
        };
        if transitioned {
            Ok(())
        } else {
            state.needs_recovery = true;
            Err(RegistryViolation::HandoffPreparationLost { ticket_id })
        }
    }

    fn commit_handoff(
        &self,
        mut token: DurableRetirementToken<O>,
    ) -> Result<RetirementHandoffCapability<O>, RegistryViolation> {
        let mut state = self.authority.state.lock();
        if self.is_recovery_fenced(&state) {
            return Err(RegistryViolation::NeedsRecovery);
        }
        let ticket_id = token.binding.ticket_id();
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryViolation::HandoffPreparationLost { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryViolation::HandoffPreparationLost { ticket_id });
        };
        let transitioned = match &mut entry.phase {
            RegistryEntryPhase::IntentDurable { binding, handoff, .. }
                if binding == &token.binding && *handoff == HandoffState::Prepared =>
            {
                *handoff = HandoffState::Consumed;
                true
            }
            _ => false,
        };
        if !transitioned {
            state.needs_recovery = true;
            return Err(RegistryViolation::HandoffPreparationLost { ticket_id });
        }
        let capability = RetirementHandoffCapability {
            authority: Arc::clone(&self.authority),
            binding: token.binding.clone(),
            durability: DurableStagePosition::writer_verified(token.durability),
            armed: true,
        };
        token.armed = false;
        Ok(capability)
    }

    fn fence_abandoned_handoff(&self) {
        self.authority.fence_recovery();
    }

    fn fence_inflight_intent(&self) {
        self.authority.fence_recovery();
    }
}

fn validate_entry<O>(
    entry: &StrongRegistryEntry<O>,
    operation: &RetirementOperation,
    owner: &Arc<O>,
    queue_identity: &QueueIdentity,
) -> Result<(), RegistryViolation> {
    let incarnation = operation.incarnation();
    if entry.physical_key != operation.target_key() {
        return Err(RegistryViolation::PhysicalKeyMismatch { incarnation });
    }
    if entry.segment_offset != operation.segment_offset() {
        return Err(RegistryViolation::SegmentOffsetMismatch { incarnation });
    }
    if entry.canonical_path != *operation.canonical_path() {
        return Err(RegistryViolation::CanonicalPathMismatch { incarnation });
    }
    if entry.expected_length != operation.expected_length() {
        return Err(RegistryViolation::ExpectedLengthMismatch { incarnation });
    }
    match &entry.runtime_identity {
        RuntimeIdentity::Active {
            owner: registered_owner,
            queue_identity: registered_queue,
        } => {
            if !Arc::ptr_eq(registered_owner, owner) {
                return Err(RegistryViolation::OwnerIdentityMismatch { incarnation });
            }
            if !registered_queue.same_as(queue_identity) {
                return Err(RegistryViolation::QueueIdentityMismatch { incarnation });
            }
        }
        RuntimeIdentity::Recovered => return Err(RegistryViolation::IncarnationNotActive { incarnation }),
    }
    Ok(())
}

fn active_runtime_identity<O>(
    entry: &StrongRegistryEntry<O>,
    incarnation: FileIncarnationId,
) -> Result<(Arc<O>, QueueIdentity), RegistryViolation> {
    match &entry.runtime_identity {
        RuntimeIdentity::Active { owner, queue_identity } => Ok((Arc::clone(owner), queue_identity.clone())),
        RuntimeIdentity::Recovered => Err(RegistryViolation::IncarnationNotActive { incarnation }),
    }
}

fn fence_consumed_evidence<O, T>(
    state: &mut RegistryState<O>,
    error: RegistryViolation,
) -> Result<T, RegistryViolation> {
    state.needs_recovery = true;
    Err(error)
}

#[cfg(test)]
mod tests;
