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

use super::*;
use crate::mapped_file::retirement::sidecar::RetirementStage;
use crate::mapped_file::retirement::state::reconciliation::ReconciledLedgerState;
use crate::mapped_file::retirement::state::RecoveredLedgerState;

impl<O> RetirementRegistry<O> {
    /// Reconstructs every durable retirement only after namespace reconciliation succeeded.
    pub(in crate::mapped_file::retirement) fn from_reconciled_state(
        reconciled: &ReconciledLedgerState,
    ) -> Result<(Self, Vec<RecoveredRetirementWork<O>>), RegistryError> {
        Self::rebuild(reconciled.recovered())
    }

    #[cfg(test)]
    pub(super) fn from_recovered_state(
        recovered: &RecoveredLedgerState,
    ) -> Result<(Self, Vec<RecoveredRetirementWork<O>>), RegistryError> {
        Self::rebuild(recovered)
    }

    fn rebuild(recovered: &RecoveredLedgerState) -> Result<(Self, Vec<RecoveredRetirementWork<O>>), RegistryError> {
        let registry = Self::new(recovered.store_uuid(), recovered.ticket_high_water());
        let mut rebuilt = RegistryState {
            ticket_high_water: recovered.ticket_high_water(),
            entries: BTreeMap::new(),
            incarnation_by_path: BTreeMap::new(),
            incarnation_by_key: BTreeMap::new(),
            incarnation_by_owner: BTreeMap::new(),
            incarnation_by_ticket: BTreeMap::new(),
            pending: None,
            needs_recovery: false,
        };
        let mut work = Vec::new();
        work.try_reserve_exact(recovered.retirement_count())
            .map_err(|_| RegistryError::RecoveryAllocationFailed)?;

        let replay_frontier = DurableStagePosition::replayed(recovered.last_sequence())?;
        for (entry, observed_replacement_key) in recovered.retirement_entries() {
            if entry.incarnation.store_uuid() != recovered.store_uuid() {
                return Err(RegistryError::StoreUuidMismatch);
            }
            let operation = RetirementOperation::new(
                entry.incarnation,
                entry.reason,
                entry.mapping_generation,
                entry.segment_offset,
                entry.expected_file_length,
                entry.retirement_nonce,
                entry.target_key,
                entry.canonical_path.clone(),
            )?;
            let binding = RetirementIntentBinding {
                ticket_id: entry.ticket_id,
                operation,
            };
            let stage_position = DurableStagePosition::replayed(entry.stage_sequence)?;

            reserve_recovered_identity(&mut rebuilt, entry, &binding)?;
            let (phase, pending_work) = match entry.stage {
                RetirementStage::IntentDurable => {
                    let phase = RegistryEntryPhase::IntentDurable {
                        binding: binding.clone(),
                        durability: replay_frontier,
                        handoff: HandoffState::Consumed,
                    };
                    let capability = RetirementHandoffCapability {
                        authority: Arc::clone(&registry.authority),
                        binding: binding.clone(),
                        durability: replay_frontier,
                        armed: true,
                    };
                    (phase, Some(RecoveredRetirementWork::LogicalRemoval(capability)))
                }
                RetirementStage::LogicalRemoved => {
                    let phase = RegistryEntryPhase::LogicalRemoved {
                        binding: binding.clone(),
                        durability: stage_position,
                        append_durability: replay_frontier,
                        observed_replacement_key,
                    };
                    let capability = LogicalRemovedCapability {
                        authority: Arc::clone(&registry.authority),
                        binding: binding.clone(),
                        durability: stage_position,
                        append_durability: replay_frontier,
                        observed_replacement_key,
                        armed: true,
                    };
                    (phase, Some(RecoveredRetirementWork::Namespace(capability)))
                }
                RetirementStage::Tombstoned => {
                    let tombstone_path =
                        entry
                            .tombstone_path
                            .clone()
                            .ok_or(RegistryError::InvalidRecoveredRetirement {
                                ticket_id: entry.ticket_id,
                            })?;
                    let phase = RegistryEntryPhase::Tombstoned {
                        binding: binding.clone(),
                        durability: stage_position,
                        append_durability: replay_frontier,
                        tombstone_path: tombstone_path.clone(),
                        observed_replacement_key,
                    };
                    let capability = TombstonedCapability {
                        authority: Arc::clone(&registry.authority),
                        binding: binding.clone(),
                        durability: stage_position,
                        append_durability: replay_frontier,
                        tombstone_path,
                        observed_replacement_key,
                        armed: true,
                    };
                    (phase, Some(RecoveredRetirementWork::TombstoneRemoval(capability)))
                }
                RetirementStage::NamespaceAbsent => {
                    let phase = RegistryEntryPhase::NamespaceAbsent {
                        binding: binding.clone(),
                        durability: stage_position,
                        append_durability: replay_frontier,
                        tombstone_path: entry.tombstone_path.clone(),
                        observed_replacement_key,
                    };
                    let capability = NamespaceAbsentCapability {
                        authority: Arc::clone(&registry.authority),
                        binding: binding.clone(),
                        durability: stage_position,
                        append_durability: replay_frontier,
                        tombstone_path: entry.tombstone_path.clone(),
                        observed_replacement_key,
                        armed: true,
                    };
                    (phase, Some(RecoveredRetirementWork::Completion(capability)))
                }
                RetirementStage::CompletedRetained => (
                    RegistryEntryPhase::CompletedRetained {
                        binding: binding.clone(),
                        durability: stage_position,
                        observed_replacement_key,
                    },
                    None,
                ),
            };

            rebuilt.entries.insert(
                entry.incarnation,
                StrongRegistryEntry {
                    physical_key: entry.target_key,
                    canonical_path: entry.canonical_path.clone(),
                    segment_offset: entry.segment_offset,
                    expected_length: entry.expected_file_length,
                    runtime_identity: RuntimeIdentity::Recovered,
                    phase,
                },
            );
            if let Some(pending_work) = pending_work {
                work.push(pending_work);
            }
        }

        *registry.authority.state.lock() = rebuilt;
        Ok((registry, work))
    }
}

fn reserve_recovered_identity<O>(
    state: &mut RegistryState<O>,
    entry: &crate::mapped_file::retirement::sidecar::RetirementTicketSnapshotEntry,
    binding: &RetirementIntentBinding,
) -> Result<(), RegistryError> {
    if entry.incarnation.store_uuid() != binding.incarnation().store_uuid() {
        return Err(RegistryError::StoreUuidMismatch);
    }
    if state.entries.contains_key(&entry.incarnation) {
        return Err(RegistryError::DuplicateIncarnation {
            incarnation: entry.incarnation,
        });
    }
    if let Some(incumbent) = state
        .incarnation_by_path
        .insert(entry.canonical_path.clone(), entry.incarnation)
    {
        return Err(RegistryError::CanonicalPathReserved {
            path: entry.canonical_path.clone(),
            incumbent,
        });
    }
    if let Some(incumbent) = state.incarnation_by_key.insert(entry.target_key, entry.incarnation) {
        return Err(RegistryError::PhysicalKeyReserved {
            physical_key: entry.target_key,
            incumbent,
        });
    }
    if state
        .incarnation_by_ticket
        .insert(entry.ticket_id, entry.incarnation)
        .is_some()
    {
        return Err(RegistryError::DuplicateTicket {
            ticket_id: entry.ticket_id,
        });
    }
    Ok(())
}
