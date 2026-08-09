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

use super::transitions::merge_observed_replacement_key;
use super::*;

impl<O> RegistryAuthority<O> {
    pub(super) fn fence_recovery(&self) {
        self.seal.fence_recovery();
        self.state.lock().needs_recovery = true;
    }

    pub(super) fn needs_recovery(&self) -> bool {
        self.state.lock().needs_recovery || self.seal.needs_recovery()
    }

    pub(super) fn commit_logical_removed(
        self: &Arc<Self>,
        mut capability: RetirementHandoffCapability<O>,
        durability: DurabilityCoordinates,
    ) -> Result<LogicalRemovedCapability<O>, RegistryError> {
        let ticket_id = capability.binding.ticket_id();
        let mut state = self.state.lock();
        if state.needs_recovery || self.seal.needs_recovery() {
            return Err(RegistryError::NeedsRecovery);
        }
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let exact_predecessor = match &entry.phase {
            RegistryEntryPhase::IntentDurable {
                binding,
                durability: predecessor,
                handoff: HandoffState::Consumed,
            } => {
                binding == &capability.binding
                    && predecessor == &capability.durability
                    && predecessor.accepts(&durability)
            }
            _ => false,
        };
        if !exact_predecessor {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        entry.phase = RegistryEntryPhase::LogicalRemoved {
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            observed_replacement_key: None,
        };
        let next = LogicalRemovedCapability {
            authority: Arc::clone(self),
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            observed_replacement_key: None,
            armed: true,
        };
        capability.armed = false;
        Ok(next)
    }

    pub(super) fn commit_superseded_path_after_logical(
        self: &Arc<Self>,
        mut capability: LogicalRemovedCapability<O>,
        durability: DurabilityCoordinates,
        observed_replacement_key: PhysicalFileKey,
    ) -> Result<LogicalRemovedCapability<O>, RegistryError> {
        let ticket_id = capability.binding.ticket_id();
        let mut state = self.state.lock();
        if state.needs_recovery || self.seal.needs_recovery() {
            return Err(RegistryError::NeedsRecovery);
        }
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let exact_predecessor = match &entry.phase {
            RegistryEntryPhase::LogicalRemoved {
                binding,
                durability: stage_durability,
                append_durability,
                observed_replacement_key: recorded_replacement_key,
            } => {
                binding == &capability.binding
                    && stage_durability == &capability.durability
                    && append_durability == &capability.append_durability
                    && recorded_replacement_key == &capability.observed_replacement_key
                    && append_durability.accepts(&durability)
            }
            _ => false,
        };
        if !exact_predecessor {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        let observed_replacement_key = merge_observed_replacement_key(
            ticket_id,
            capability.observed_replacement_key,
            Some(observed_replacement_key),
        )?;
        entry.phase = RegistryEntryPhase::LogicalRemoved {
            binding: capability.binding.clone(),
            durability: capability.durability,
            append_durability: DurableStagePosition::writer_verified(durability),
            observed_replacement_key,
        };
        let next = LogicalRemovedCapability {
            authority: Arc::clone(self),
            binding: capability.binding.clone(),
            durability: capability.durability,
            append_durability: DurableStagePosition::writer_verified(durability),
            observed_replacement_key,
            armed: true,
        };
        capability.armed = false;
        Ok(next)
    }

    pub(super) fn commit_namespace_absent(
        self: &Arc<Self>,
        mut capability: LogicalRemovedCapability<O>,
        durability: DurabilityCoordinates,
        tombstone_path: Option<StoreRelativePath>,
        observed_replacement_key: Option<PhysicalFileKey>,
    ) -> Result<NamespaceAbsentCapability<O>, RegistryError> {
        let ticket_id = capability.binding.ticket_id();
        let mut state = self.state.lock();
        if state.needs_recovery || self.seal.needs_recovery() {
            return Err(RegistryError::NeedsRecovery);
        }
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let exact_predecessor = match &entry.phase {
            RegistryEntryPhase::LogicalRemoved {
                binding,
                durability: stage_durability,
                append_durability,
                observed_replacement_key: recorded_replacement_key,
            } => {
                binding == &capability.binding
                    && stage_durability == &capability.durability
                    && append_durability == &capability.append_durability
                    && recorded_replacement_key == &capability.observed_replacement_key
                    && append_durability.accepts(&durability)
            }
            _ => false,
        };
        if !exact_predecessor {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        let observed_replacement_key =
            merge_observed_replacement_key(ticket_id, capability.observed_replacement_key, observed_replacement_key)?;
        entry.phase = RegistryEntryPhase::NamespaceAbsent {
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            tombstone_path: tombstone_path.clone(),
            observed_replacement_key,
        };
        let next = NamespaceAbsentCapability {
            authority: Arc::clone(self),
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            tombstone_path,
            observed_replacement_key,
            armed: true,
        };
        capability.armed = false;
        Ok(next)
    }

    pub(super) fn commit_tombstoned(
        self: &Arc<Self>,
        mut capability: LogicalRemovedCapability<O>,
        durability: DurabilityCoordinates,
        tombstone_path: StoreRelativePath,
        observed_replacement_key: Option<PhysicalFileKey>,
    ) -> Result<TombstonedCapability<O>, RegistryError> {
        let ticket_id = capability.binding.ticket_id();
        let mut state = self.state.lock();
        if state.needs_recovery || self.seal.needs_recovery() {
            return Err(RegistryError::NeedsRecovery);
        }
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let exact_predecessor = match &entry.phase {
            RegistryEntryPhase::LogicalRemoved {
                binding,
                durability: stage_durability,
                append_durability,
                observed_replacement_key: recorded_replacement_key,
            } => {
                binding == &capability.binding
                    && stage_durability == &capability.durability
                    && append_durability == &capability.append_durability
                    && recorded_replacement_key == &capability.observed_replacement_key
                    && append_durability.accepts(&durability)
            }
            _ => false,
        };
        if !exact_predecessor {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        let observed_replacement_key =
            merge_observed_replacement_key(ticket_id, capability.observed_replacement_key, observed_replacement_key)?;
        entry.phase = RegistryEntryPhase::Tombstoned {
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            tombstone_path: tombstone_path.clone(),
            observed_replacement_key,
        };
        let next = TombstonedCapability {
            authority: Arc::clone(self),
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            tombstone_path,
            observed_replacement_key,
            armed: true,
        };
        capability.armed = false;
        Ok(next)
    }

    pub(super) fn commit_namespace_absent_after_tombstone(
        self: &Arc<Self>,
        mut capability: TombstonedCapability<O>,
        durability: DurabilityCoordinates,
        observed_replacement_key: Option<PhysicalFileKey>,
    ) -> Result<NamespaceAbsentCapability<O>, RegistryError> {
        let ticket_id = capability.binding.ticket_id();
        let mut state = self.state.lock();
        if state.needs_recovery || self.seal.needs_recovery() {
            return Err(RegistryError::NeedsRecovery);
        }
        let Some(incarnation) = state.incarnation_by_ticket.get(&ticket_id).copied() else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let Some(entry) = state.entries.get_mut(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let exact_predecessor = match &entry.phase {
            RegistryEntryPhase::Tombstoned {
                binding,
                durability: stage_durability,
                append_durability,
                tombstone_path,
                observed_replacement_key: recorded_replacement_key,
            } => {
                binding == &capability.binding
                    && stage_durability == &capability.durability
                    && append_durability == &capability.append_durability
                    && tombstone_path == &capability.tombstone_path
                    && recorded_replacement_key == &capability.observed_replacement_key
                    && append_durability.accepts(&durability)
            }
            _ => false,
        };
        if !exact_predecessor {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        let observed_replacement_key =
            merge_observed_replacement_key(ticket_id, capability.observed_replacement_key, observed_replacement_key)?;
        entry.phase = RegistryEntryPhase::NamespaceAbsent {
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            tombstone_path: Some(capability.tombstone_path.clone()),
            observed_replacement_key,
        };
        let next = NamespaceAbsentCapability {
            authority: Arc::clone(self),
            binding: capability.binding.clone(),
            durability: DurableStagePosition::writer_verified(durability),
            append_durability: DurableStagePosition::writer_verified(durability),
            tombstone_path: Some(capability.tombstone_path.clone()),
            observed_replacement_key,
            armed: true,
        };
        capability.armed = false;
        Ok(next)
    }

    pub(super) fn commit_completed(
        self: &Arc<Self>,
        mut capability: NamespaceAbsentCapability<O>,
        durability: DurabilityCoordinates,
    ) -> Result<CompletedRetirementReceipt, RegistryError> {
        let ticket_id = capability.binding.ticket_id();
        let incarnation = capability.binding.incarnation();
        let mut state = self.state.lock();
        if state.needs_recovery || self.seal.needs_recovery() {
            return Err(RegistryError::NeedsRecovery);
        }
        if state.incarnation_by_ticket.get(&ticket_id) != Some(&incarnation) {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        let Some(entry) = state.entries.get(&incarnation) else {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        };
        let exact_predecessor = match &entry.phase {
            RegistryEntryPhase::NamespaceAbsent {
                binding,
                durability: stage_durability,
                append_durability,
                tombstone_path,
                observed_replacement_key,
            } => {
                binding == &capability.binding
                    && stage_durability == &capability.durability
                    && append_durability == &capability.append_durability
                    && tombstone_path == &capability.tombstone_path
                    && observed_replacement_key == &capability.observed_replacement_key
                    && append_durability.accepts(&durability)
            }
            _ => false,
        };
        if !exact_predecessor {
            state.needs_recovery = true;
            return Err(RegistryError::DurableStageEvidenceMismatch { ticket_id });
        }
        let owner_identity = match &entry.runtime_identity {
            RuntimeIdentity::Active { owner, .. } => Some(Arc::as_ptr(owner) as usize),
            RuntimeIdentity::Recovered => None,
        };
        let canonical_path = entry.canonical_path.clone();
        let physical_key = entry.physical_key;

        state.entries.remove(&incarnation);
        state.incarnation_by_path.remove(&canonical_path);
        state.incarnation_by_key.remove(&physical_key);
        if let Some(owner_identity) = owner_identity {
            state.incarnation_by_owner.remove(&owner_identity);
        }
        state.incarnation_by_ticket.remove(&ticket_id);
        let receipt = CompletedRetirementReceipt {
            binding: capability.binding.clone(),
            durability,
        };
        capability.armed = false;
        Ok(receipt)
    }
}
