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
    pub(super) fn insert_snapshot_incarnation(
        &mut self,
        entry: IncarnationSnapshotEntry,
    ) -> Result<(), StateViolation> {
        if entry.incarnation.store_uuid() != self.store_uuid
            || entry.incarnation.create_seq() > self.create_high_water
            || entry.expected_file_length == 0
            || entry.create_nonce == [0; 16]
            || matches!(entry.phase, IncarnationPhase::Allocated) != entry.physical_key.is_none()
            || entry
                .canonical_path
                .validate_create_binding(
                    &entry.create_file_path,
                    entry.incarnation,
                    entry.segment_offset,
                    &entry.create_nonce,
                )
                .is_err()
        {
            return Err(StateViolation::InvalidSnapshotState);
        }
        if self.incarnations.contains_key(&entry.incarnation)
            || self.incarnation_by_canonical_path.contains_key(&entry.canonical_path)
            || self.incarnation_by_create_path.contains_key(&entry.create_file_path)
        {
            return Err(StateViolation::InvalidSnapshotState);
        }
        let incarnation = entry.incarnation;
        let canonical_path = entry.canonical_path.clone();
        let create_file_path = entry.create_file_path.clone();
        self.incarnations.insert(incarnation, entry);
        self.incarnation_by_canonical_path.insert(canonical_path, incarnation);
        self.incarnation_by_create_path.insert(create_file_path, incarnation);
        Ok(())
    }

    pub(super) fn insert_snapshot_retirement(
        &mut self,
        entry: RetirementTicketSnapshotEntry,
    ) -> Result<(), StateViolation> {
        let Some(incarnation) = self.incarnations.get(&entry.incarnation) else {
            return Err(StateViolation::InvalidSnapshotState);
        };
        let stage_and_tombstone_are_consistent = match entry.stage {
            RetirementStage::IntentDurable | RetirementStage::LogicalRemoved => entry.tombstone_path.is_none(),
            RetirementStage::Tombstoned => entry.tombstone_path.is_some(),
            RetirementStage::NamespaceAbsent | RetirementStage::CompletedRetained => true,
        };
        if entry.incarnation.store_uuid() != self.store_uuid
            || entry.ticket_id.get() > self.ticket_high_water
            || entry.stage_sequence == 0
            || entry.stage_sequence > self.snapshot_base_sequence
            || entry.mapping_generation == 0
            || entry.expected_file_length == 0
            || entry.retirement_nonce == [0; 16]
            || incarnation.phase != IncarnationPhase::Published
            || !retirement_matches_incarnation(&entry, incarnation)
            || !stage_and_tombstone_are_consistent
            || entry.tombstone_path.as_ref().is_some_and(|path| {
                entry
                    .canonical_path
                    .validate_tombstone_binding(
                        path,
                        entry.ticket_id,
                        entry.incarnation,
                        entry.segment_offset,
                        entry.mapping_generation,
                        &entry.retirement_nonce,
                    )
                    .is_err()
            })
            || self.ticket_by_incarnation.contains_key(&entry.incarnation)
            || self.retirements.contains_key(&entry.ticket_id)
        {
            return Err(StateViolation::InvalidSnapshotState);
        }
        let completed_retained_eligibility = (entry.stage == RetirementStage::CompletedRetained)
            .then_some(CompletedRetainedEligibility::RequiresCleanStartRevalidation);
        let state = RetirementState {
            entry,
            observed_replacement_key: None,
            last_stage_payload: None,
            completed_retained_eligibility,
        };
        let ticket_id = state.entry.ticket_id;
        let incarnation = state.entry.incarnation;
        self.retirements.insert(ticket_id, state);
        self.ticket_by_incarnation.insert(incarnation, ticket_id);
        Ok(())
    }

    pub(super) fn insert_snapshot_quarantine(&mut self, entry: QuarantineSnapshotEntry) -> Result<(), StateViolation> {
        if entry.sequence_at_observation == 0 || entry.sequence_at_observation > self.snapshot_base_sequence {
            return Err(StateViolation::InvalidSnapshotState);
        }
        if self.quarantines.insert(entry.source_path.clone(), entry).is_some() {
            return Err(StateViolation::InvalidSnapshotState);
        }
        Ok(())
    }
}

pub(super) fn validate_snapshot_header(snapshot: &LifecycleSnapshot) -> Result<(), StateViolation> {
    let expected_predecessor = snapshot.generation.checked_sub(1).unwrap_or(u64::MAX);
    let mode_matches = match snapshot.mode {
        SnapshotMode::BootstrapInventory => snapshot.generation == 0,
        SnapshotMode::OrdinaryCompaction | SnapshotMode::TailRepair => snapshot.generation > 0,
    };
    if snapshot.generation != snapshot.log_generation
        || snapshot.predecessor_log_generation != expected_predecessor
        || snapshot.base_sequence == 0
        || !mode_matches
    {
        return Err(StateViolation::InvalidSnapshotState);
    }
    Ok(())
}

fn retirement_matches_incarnation(
    retirement: &RetirementTicketSnapshotEntry,
    incarnation: &IncarnationSnapshotEntry,
) -> bool {
    retirement.incarnation == incarnation.incarnation
        && retirement.segment_offset == incarnation.segment_offset
        && retirement.expected_file_length == incarnation.expected_file_length
        && Some(retirement.target_key) == incarnation.physical_key
        && retirement.canonical_path == incarnation.canonical_path
}
