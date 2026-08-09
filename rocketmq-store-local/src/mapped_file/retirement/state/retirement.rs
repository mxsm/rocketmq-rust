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
    #[allow(
        clippy::too_many_arguments,
        reason = "arguments mirror the persisted RetirementIntent payload"
    )]
    pub(super) fn apply_retirement_intent(
        &mut self,
        sequence: u64,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        reason: super::super::codec::RetirementReason,
        mapping_generation: u64,
        segment_offset: u64,
        expected_length: u64,
        retirement_nonce: [u8; 16],
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    ) -> Result<(), StateError> {
        self.require_store(incarnation.store_uuid())?;
        if let Some(existing) = self.retirements.get(&ticket_id) {
            if existing.entry.stage != RetirementStage::IntentDurable {
                return Err(StateError::InvalidRetirementTransition {
                    from: Some(existing.entry.stage),
                    to: RetirementStage::IntentDurable,
                });
            }
            if !intent_matches(
                &existing.entry,
                incarnation,
                reason,
                mapping_generation,
                segment_offset,
                expected_length,
                retirement_nonce,
                target_key,
                &canonical_path,
            ) {
                return Err(StateError::IdentityChangingDuplicate {
                    entity: "retirement ticket",
                });
            }
            return Ok(());
        }
        let expected = self
            .ticket_high_water
            .checked_add(1)
            .ok_or(StateError::HighWaterOverflow {
                field: "ticket_high_water",
            })?;
        if ticket_id.get() != expected {
            return Err(StateError::HighWaterMismatch {
                field: "ticket_high_water",
                previous: self.ticket_high_water,
                expected,
                actual: ticket_id.get(),
            });
        }
        let Some(published) = self.incarnations.get(&incarnation) else {
            return Err(StateError::MissingIncarnation);
        };
        if published.phase != IncarnationPhase::Published {
            return Err(StateError::InvalidIncarnationTransition {
                from: Some(published.phase),
                to: IncarnationPhase::Published,
            });
        }
        if segment_offset != published.segment_offset
            || expected_length != published.expected_file_length
            || Some(target_key) != published.physical_key
            || canonical_path != published.canonical_path
            || mapping_generation == 0
            || retirement_nonce == [0; 16]
        {
            return Err(StateError::RecordIdentityMismatch);
        }
        if self.ticket_by_incarnation.contains_key(&incarnation) {
            return Err(StateError::ConcurrentRetirementTicket);
        }
        self.ticket_high_water = expected;
        let entry = RetirementTicketSnapshotEntry {
            ticket_id,
            incarnation,
            stage: RetirementStage::IntentDurable,
            superseded_path_observed: false,
            quarantined: false,
            reason,
            stage_sequence: sequence,
            mapping_generation,
            segment_offset,
            expected_file_length: expected_length,
            retirement_nonce,
            target_key,
            canonical_path,
            tombstone_path: None,
        };
        self.retirements.insert(
            ticket_id,
            RetirementState {
                entry,
                observed_replacement_key: None,
                last_stage_payload: None,
                completed_retained_eligibility: None,
            },
        );
        self.ticket_by_incarnation.insert(incarnation, ticket_id);
        Ok(())
    }

    pub(super) fn apply_logical_removed(
        &mut self,
        sequence: u64,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    ) -> Result<(), StateError> {
        let state = self.retirement_mut(ticket_id, incarnation, target_key, &canonical_path)?;
        match state.entry.stage {
            RetirementStage::IntentDurable => {
                state.entry.stage = RetirementStage::LogicalRemoved;
                state.entry.stage_sequence = sequence;
                state.last_stage_payload = Some(RetirementStagePayload::LogicalRemoved);
            }
            RetirementStage::LogicalRemoved
                if state.last_stage_payload == Some(RetirementStagePayload::LogicalRemoved) => {}
            from => {
                return Err(StateError::InvalidRetirementTransition {
                    from: Some(from),
                    to: RetirementStage::LogicalRemoved,
                });
            }
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "arguments mirror the persisted Tombstoned payload"
    )]
    pub(super) fn apply_tombstoned(
        &mut self,
        sequence: u64,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        target_key: PhysicalFileKey,
        retirement_nonce: [u8; 16],
        canonical_path: StoreRelativePath,
        tombstone_path: StoreRelativePath,
    ) -> Result<(), StateError> {
        let state = self.retirement_mut(ticket_id, incarnation, target_key, &canonical_path)?;
        if retirement_nonce != state.entry.retirement_nonce
            || state
                .entry
                .canonical_path
                .validate_tombstone_binding(
                    &tombstone_path,
                    ticket_id,
                    incarnation,
                    state.entry.segment_offset,
                    state.entry.mapping_generation,
                    &retirement_nonce,
                )
                .is_err()
        {
            return Err(StateError::RecordIdentityMismatch);
        }
        match state.entry.stage {
            RetirementStage::LogicalRemoved => {
                state.entry.stage = RetirementStage::Tombstoned;
                state.entry.stage_sequence = sequence;
                state.entry.tombstone_path = Some(tombstone_path);
                state.last_stage_payload = Some(RetirementStagePayload::Tombstoned);
            }
            RetirementStage::Tombstoned
                if state.entry.tombstone_path.as_ref() == Some(&tombstone_path)
                    && state.last_stage_payload == Some(RetirementStagePayload::Tombstoned) => {}
            from => {
                return Err(StateError::InvalidRetirementTransition {
                    from: Some(from),
                    to: RetirementStage::Tombstoned,
                });
            }
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "arguments mirror the persisted NamespaceAbsent payload"
    )]
    pub(super) fn apply_namespace_absent(
        &mut self,
        sequence: u64,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        replacement_observed: bool,
        observation_time_ns: u64,
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        tombstone_path: Option<StoreRelativePath>,
    ) -> Result<(), StateError> {
        let state = self.retirement_mut(ticket_id, incarnation, target_key, &canonical_path)?;
        let payload = RetirementStagePayload::NamespaceAbsent {
            replacement_observed,
            observation_time_ns,
        };
        match state.entry.stage {
            RetirementStage::LogicalRemoved if tombstone_path.is_none() => {
                state.entry.stage = RetirementStage::NamespaceAbsent;
                state.entry.stage_sequence = sequence;
                state.entry.superseded_path_observed |= replacement_observed;
                state.last_stage_payload = Some(payload);
            }
            RetirementStage::Tombstoned if tombstone_path == state.entry.tombstone_path => {
                state.entry.stage = RetirementStage::NamespaceAbsent;
                state.entry.stage_sequence = sequence;
                state.entry.superseded_path_observed |= replacement_observed;
                state.last_stage_payload = Some(payload);
            }
            RetirementStage::NamespaceAbsent
                if tombstone_path == state.entry.tombstone_path
                    && state.last_stage_payload.as_ref() == Some(&payload) => {}
            from => {
                return Err(StateError::InvalidRetirementTransition {
                    from: Some(from),
                    to: RetirementStage::NamespaceAbsent,
                });
            }
        }
        Ok(())
    }

    pub(super) fn apply_completed(
        &mut self,
        sequence: u64,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        completion_time_ns: u64,
        namespace_absent_sequence: u64,
    ) -> Result<(), StateError> {
        self.require_store(incarnation.store_uuid())?;
        let Some(state) = self.retirements.get_mut(&ticket_id) else {
            return Err(StateError::MissingRetirementTicket);
        };
        if state.entry.incarnation != incarnation {
            return Err(StateError::RecordIdentityMismatch);
        }
        let payload = RetirementStagePayload::Completed {
            completion_time_ns,
            namespace_absent_sequence,
        };
        match state.entry.stage {
            RetirementStage::NamespaceAbsent if state.entry.stage_sequence == namespace_absent_sequence => {
                state.entry.stage = RetirementStage::CompletedRetained;
                state.entry.stage_sequence = sequence;
                state.last_stage_payload = Some(payload);
                state.completed_retained_eligibility =
                    Some(CompletedRetainedEligibility::RequiresCleanStartRevalidation);
            }
            RetirementStage::CompletedRetained if state.last_stage_payload.as_ref() == Some(&payload) => {}
            from => {
                return Err(StateError::InvalidRetirementTransition {
                    from: Some(from),
                    to: RetirementStage::CompletedRetained,
                });
            }
        }
        Ok(())
    }

    pub(super) fn apply_superseded_path(
        &mut self,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        expected_target_key: PhysicalFileKey,
        observed_replacement_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    ) -> Result<(), StateError> {
        let state = self.retirement_mut(ticket_id, incarnation, expected_target_key, &canonical_path)?;
        if let Some(existing) = state.observed_replacement_key {
            if existing != observed_replacement_key {
                return Err(StateError::IdentityChangingDuplicate {
                    entity: "superseded path",
                });
            }
        } else {
            state.observed_replacement_key = Some(observed_replacement_key);
        }
        state.entry.superseded_path_observed = true;
        Ok(())
    }

    fn retirement_mut(
        &mut self,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        target_key: PhysicalFileKey,
        canonical_path: &StoreRelativePath,
    ) -> Result<&mut RetirementState, StateError> {
        self.require_store(incarnation.store_uuid())?;
        let Some(state) = self.retirements.get_mut(&ticket_id) else {
            return Err(StateError::MissingRetirementTicket);
        };
        if state.entry.incarnation != incarnation
            || state.entry.target_key != target_key
            || &state.entry.canonical_path != canonical_path
        {
            return Err(StateError::RecordIdentityMismatch);
        }
        Ok(state)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "comparison mirrors the persisted RetirementIntent payload"
)]
fn intent_matches(
    entry: &RetirementTicketSnapshotEntry,
    incarnation: FileIncarnationId,
    reason: super::super::codec::RetirementReason,
    mapping_generation: u64,
    segment_offset: u64,
    expected_length: u64,
    retirement_nonce: [u8; 16],
    target_key: PhysicalFileKey,
    canonical_path: &StoreRelativePath,
) -> bool {
    entry.incarnation == incarnation
        && entry.reason == reason
        && entry.mapping_generation == mapping_generation
        && entry.segment_offset == segment_offset
        && entry.expected_file_length == expected_length
        && entry.retirement_nonce == retirement_nonce
        && entry.target_key == target_key
        && &entry.canonical_path == canonical_path
}
