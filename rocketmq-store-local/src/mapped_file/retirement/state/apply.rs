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

impl LedgerStateMachine {
    pub(super) fn validate_append_barrier(
        &self,
        sequence: u64,
        record: Option<&LedgerRecord>,
    ) -> Result<(), StateViolation> {
        let Some(prepared) = &self.prepared_generation else {
            if matches!(record, Some(LedgerRecord::GenerationAborted { .. })) {
                return Err(StateViolation::IllegalGenerationAdministration);
            }
            return Ok(());
        };
        match record {
            Some(LedgerRecord::GenerationAborted {
                store_uuid,
                source_generation,
                target_generation,
                prepared_sequence,
                ..
            }) if *store_uuid == self.store_uuid
                && *source_generation == self.generation
                && *target_generation == prepared.target_generation
                && *prepared_sequence == prepared.sequence
                && prepared.sequence.checked_add(1) == Some(sequence) =>
            {
                Ok(())
            }
            Some(LedgerRecord::GenerationAborted { .. }) => Err(StateViolation::IllegalGenerationAdministration),
            _ => Err(StateViolation::AppendAfterGenerationPrepared),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive persisted-record transition table is kept auditable in one match"
    )]
    pub(super) fn apply_record(&mut self, sequence: u64, record: Option<LedgerRecord>) -> Result<(), StateViolation> {
        let Some(record) = record else {
            return Ok(());
        };
        match record {
            LedgerRecord::StoreInitialized { store_uuid, .. } => {
                self.require_store(store_uuid)?;
                if self.generation != 0 || sequence != 1 {
                    return Err(StateViolation::IllegalGenerationAdministration);
                }
            }
            LedgerRecord::BootstrapInstalled {
                store_uuid,
                snapshot_generation,
                snapshot_base_sequence,
                inventory_count,
                create_high_water,
                ticket_high_water,
                ..
            } => {
                self.require_store(store_uuid)?;
                let inventory_matches =
                    usize::try_from(inventory_count).is_ok_and(|count| count == self.incarnations.len());
                if self.generation != 0
                    || sequence != 2
                    || snapshot_generation != 0
                    || snapshot_base_sequence != 1
                    || create_high_water != self.create_high_water
                    || ticket_high_water != self.ticket_high_water
                    || !inventory_matches
                {
                    return Err(StateViolation::IllegalGenerationAdministration);
                }
            }
            LedgerRecord::LogOpened {
                store_uuid,
                generation,
                snapshot_generation,
                predecessor_log_generation,
                predecessor_terminal_acknowledged_sequence,
                snapshot_base_sequence,
                unacknowledged_suffix_length,
                unacknowledged_suffix_crc32,
                open_reason,
                predecessor_acknowledgement_epoch,
                ..
            } => {
                self.require_store(store_uuid)?;
                let suffix_is_valid = match open_reason {
                    super::super::codec::OpenReason::Compaction => {
                        unacknowledged_suffix_length == 0 && unacknowledged_suffix_crc32 == 0
                    }
                    super::super::codec::OpenReason::TailRepair => {
                        unacknowledged_suffix_length != 0
                            && usize::try_from(unacknowledged_suffix_length)
                                .is_ok_and(|length| length < super::super::codec::MAX_SEALED_RECORD_UNIT_LENGTH)
                    }
                };
                if sequence
                    != self
                        .snapshot_base_sequence
                        .checked_add(1)
                        .ok_or(StateViolation::SequenceOverflow)?
                    || generation != self.generation
                    || snapshot_generation != self.generation
                    || self.generation.checked_sub(1) != Some(predecessor_log_generation)
                    || predecessor_terminal_acknowledged_sequence != self.snapshot_base_sequence
                    || snapshot_base_sequence != self.snapshot_base_sequence
                    || predecessor_acknowledgement_epoch == 0
                    || !suffix_is_valid
                {
                    return Err(StateViolation::IllegalGenerationAdministration);
                }
            }
            LedgerRecord::GenerationPrepared {
                store_uuid,
                source_generation,
                target_generation,
                target_snapshot_generation,
                open_reason,
            } => {
                self.require_store(store_uuid)?;
                let expected_target = self
                    .generation
                    .checked_add(1)
                    .ok_or(StateViolation::IllegalGenerationAdministration)?;
                if source_generation != self.generation
                    || target_generation != expected_target
                    || target_snapshot_generation != expected_target
                    || expected_target.checked_add(1).is_none()
                    || open_reason != super::super::codec::OpenReason::Compaction
                {
                    return Err(StateViolation::IllegalGenerationAdministration);
                }
                self.prepared_generation = Some(PreparedGeneration {
                    sequence,
                    target_generation,
                });
            }
            LedgerRecord::GenerationAborted { .. } => self.prepared_generation = None,
            LedgerRecord::MarkerCommitted {
                store_uuid,
                marker_epoch,
                snapshot_generation,
                log_generation,
                anchor_sequence,
                slot_index,
                ..
            } => {
                self.require_store(store_uuid)?;
                let expected_marker_epoch = self
                    .generation
                    .checked_add(1)
                    .ok_or(StateViolation::IllegalGenerationAdministration)?;
                let expected_anchor = self
                    .snapshot_base_sequence
                    .checked_add(1)
                    .ok_or(StateViolation::SequenceOverflow)?;
                if marker_epoch != expected_marker_epoch
                    || snapshot_generation != self.generation
                    || log_generation != self.generation
                    || anchor_sequence != expected_anchor
                    || anchor_sequence.checked_add(1) != Some(sequence)
                    || slot_index > 1
                    || slot_index != ((marker_epoch - 1) & 1) as u8
                {
                    return Err(StateViolation::IllegalGenerationAdministration);
                }
            }
            LedgerRecord::AllocateIncarnation {
                incarnation,
                segment_offset,
                expected_length,
                create_nonce,
                canonical_path,
                create_file_path,
            } => self.apply_allocate(
                incarnation,
                segment_offset,
                expected_length,
                create_nonce,
                canonical_path,
                create_file_path,
            )?,
            LedgerRecord::BindIncarnation {
                incarnation,
                expected_length,
                physical_key,
                canonical_path,
                create_file_path,
            } => self.apply_bind(
                incarnation,
                expected_length,
                physical_key,
                canonical_path,
                create_file_path,
            )?,
            LedgerRecord::PublishIncarnation {
                incarnation,
                expected_length,
                physical_key,
                canonical_path,
                create_file_path,
            } => self.apply_publish(
                incarnation,
                expected_length,
                physical_key,
                canonical_path,
                create_file_path,
            )?,
            LedgerRecord::RetirementIntent {
                ticket_id,
                incarnation,
                reason,
                mapping_generation,
                segment_offset,
                expected_length,
                retirement_nonce,
                target_key,
                canonical_path,
            } => self.apply_retirement_intent(
                sequence,
                ticket_id,
                incarnation,
                reason,
                mapping_generation,
                segment_offset,
                expected_length,
                retirement_nonce,
                target_key,
                canonical_path,
            )?,
            LedgerRecord::LogicalRemoved {
                ticket_id,
                incarnation,
                target_key,
                canonical_path,
            } => self.apply_logical_removed(sequence, ticket_id, incarnation, target_key, canonical_path)?,
            LedgerRecord::Tombstoned {
                ticket_id,
                incarnation,
                target_key,
                retirement_nonce,
                canonical_path,
                tombstone_path,
            } => self.apply_tombstoned(
                sequence,
                ticket_id,
                incarnation,
                target_key,
                retirement_nonce,
                canonical_path,
                tombstone_path,
            )?,
            LedgerRecord::NamespaceAbsent {
                ticket_id,
                incarnation,
                replacement_observed,
                observation_time_ns,
                target_key,
                canonical_path,
                tombstone_path,
            } => self.apply_namespace_absent(
                sequence,
                ticket_id,
                incarnation,
                replacement_observed,
                observation_time_ns,
                target_key,
                canonical_path,
                tombstone_path,
            )?,
            LedgerRecord::Completed {
                ticket_id,
                incarnation,
                completion_time_ns,
                namespace_absent_sequence,
            } => self.apply_completed(
                sequence,
                ticket_id,
                incarnation,
                completion_time_ns,
                namespace_absent_sequence,
            )?,
            LedgerRecord::SupersededPath {
                ticket_id,
                incarnation,
                expected_target_key,
                observed_replacement_key,
                canonical_path,
            } => self.apply_superseded_path(
                ticket_id,
                incarnation,
                expected_target_key,
                observed_replacement_key,
                canonical_path,
            )?,
            LedgerRecord::Quarantined {
                entity_kind,
                reason,
                sequence_at_observation,
                physical_key,
                content_fingerprint,
                source_path,
                destination_path,
            } => {
                if sequence_at_observation == 0 || sequence_at_observation > sequence {
                    return Err(StateViolation::RecordIdentityMismatch);
                }
                let entry = QuarantineSnapshotEntry {
                    entity_kind,
                    reason,
                    sequence_at_observation,
                    physical_key,
                    content_fingerprint,
                    source_path: source_path.clone(),
                    destination_path,
                };
                if let Some(existing) = self.quarantines.get(&source_path) {
                    if existing != &entry {
                        return Err(StateViolation::IdentityChangingDuplicate { entity: "quarantine" });
                    }
                } else {
                    self.quarantines.insert(source_path, entry);
                }
            }
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "arguments mirror the persisted AllocateIncarnation payload"
    )]
    fn apply_allocate(
        &mut self,
        incarnation: FileIncarnationId,
        segment_offset: u64,
        expected_length: u64,
        create_nonce: [u8; 16],
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    ) -> Result<(), StateViolation> {
        self.require_store(incarnation.store_uuid())?;
        let candidate = IncarnationSnapshotEntry {
            incarnation,
            phase: IncarnationPhase::Allocated,
            segment_offset,
            expected_file_length: expected_length,
            create_nonce,
            physical_key: None,
            canonical_path,
            create_file_path,
        };
        if let Some(existing) = self.incarnations.get(&incarnation) {
            if existing.phase != IncarnationPhase::Allocated {
                return Err(StateViolation::InvalidIncarnationTransition {
                    from: Some(existing.phase),
                    to: IncarnationPhase::Allocated,
                });
            }
            if existing != &candidate {
                return Err(StateViolation::IdentityChangingDuplicate { entity: "incarnation" });
            }
            return Ok(());
        }
        let expected = self
            .create_high_water
            .checked_add(1)
            .ok_or(StateViolation::HighWaterOverflow {
                field: "create_high_water",
            })?;
        if incarnation.create_seq() != expected {
            return Err(StateViolation::HighWaterMismatch {
                field: "create_high_water",
                previous: self.create_high_water,
                expected,
                actual: incarnation.create_seq(),
            });
        }
        if expected_length == 0
            || create_nonce == [0; 16]
            || candidate
                .canonical_path
                .validate_create_binding(
                    &candidate.create_file_path,
                    incarnation,
                    segment_offset,
                    &candidate.create_nonce,
                )
                .is_err()
            || self
                .incarnation_by_canonical_path
                .contains_key(&candidate.canonical_path)
            || self
                .incarnation_by_create_path
                .contains_key(&candidate.create_file_path)
        {
            return Err(StateViolation::RecordIdentityMismatch);
        }
        let canonical_path = candidate.canonical_path.clone();
        let create_file_path = candidate.create_file_path.clone();
        self.create_high_water = expected;
        self.incarnations.insert(incarnation, candidate);
        self.incarnation_by_canonical_path.insert(canonical_path, incarnation);
        self.incarnation_by_create_path.insert(create_file_path, incarnation);
        Ok(())
    }

    fn apply_bind(
        &mut self,
        incarnation: FileIncarnationId,
        expected_length: u64,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    ) -> Result<(), StateViolation> {
        self.require_store(incarnation.store_uuid())?;
        let Some(existing) = self.incarnations.get_mut(&incarnation) else {
            return Err(StateViolation::MissingIncarnation);
        };
        if expected_length != existing.expected_file_length
            || canonical_path != existing.canonical_path
            || create_file_path != existing.create_file_path
        {
            return Err(StateViolation::IdentityChangingDuplicate { entity: "incarnation" });
        }
        match existing.phase {
            IncarnationPhase::Allocated => {
                existing.phase = IncarnationPhase::Bound;
                existing.physical_key = Some(physical_key);
            }
            IncarnationPhase::Bound if existing.physical_key == Some(physical_key) => {}
            IncarnationPhase::Bound => {
                return Err(StateViolation::IdentityChangingDuplicate { entity: "incarnation" });
            }
            IncarnationPhase::Published => {
                return Err(StateViolation::InvalidIncarnationTransition {
                    from: Some(IncarnationPhase::Published),
                    to: IncarnationPhase::Bound,
                });
            }
        }
        Ok(())
    }

    fn apply_publish(
        &mut self,
        incarnation: FileIncarnationId,
        expected_length: u64,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    ) -> Result<(), StateViolation> {
        self.require_store(incarnation.store_uuid())?;
        let Some(existing) = self.incarnations.get_mut(&incarnation) else {
            return Err(StateViolation::MissingIncarnation);
        };
        if existing.phase == IncarnationPhase::Allocated {
            return Err(StateViolation::InvalidIncarnationTransition {
                from: Some(IncarnationPhase::Allocated),
                to: IncarnationPhase::Published,
            });
        }
        if expected_length != existing.expected_file_length
            || Some(physical_key) != existing.physical_key
            || canonical_path != existing.canonical_path
            || create_file_path != existing.create_file_path
        {
            return Err(StateViolation::IdentityChangingDuplicate { entity: "incarnation" });
        }
        if existing.phase == IncarnationPhase::Bound {
            existing.phase = IncarnationPhase::Published;
        }
        Ok(())
    }

    pub(super) fn require_store(&self, store_uuid: StoreUuid) -> Result<(), StateViolation> {
        if store_uuid != self.store_uuid {
            return Err(StateViolation::StoreUuidMismatch);
        }
        Ok(())
    }
}
