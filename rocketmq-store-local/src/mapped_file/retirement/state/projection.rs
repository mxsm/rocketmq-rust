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
    pub(in crate::mapped_file::retirement) fn validate_successor_projection(
        &self,
        successor: &Self,
    ) -> Result<(), StateError> {
        let source_last_sequence = self.next_sequence.checked_sub(1).ok_or(StateError::SequenceOverflow)?;
        if self.store_uuid != successor.store_uuid
            || self.generation.checked_add(1) != Some(successor.generation)
            || successor.snapshot_base_sequence != source_last_sequence
            || self.create_high_water != successor.create_high_water
            || self.ticket_high_water != successor.ticket_high_water
            || self.quarantines != successor.quarantines
            || successor.prepared_generation.is_some()
        {
            return Err(StateError::InvalidSnapshotState);
        }

        if successor
            .incarnations
            .iter()
            .any(|(id, entry)| self.incarnations.get(id) != Some(entry))
            || successor
                .retirements
                .iter()
                .any(|(id, state)| self.retirements.get(id).map(|source| &source.entry) != Some(&state.entry))
        {
            return Err(StateError::InvalidSnapshotState);
        }

        for (ticket_id, state) in &self.retirements {
            let retained_exact = successor.retirements.get(ticket_id).map(|entry| &entry.entry) == Some(&state.entry);
            let omitted_as_pair = state.entry.stage == RetirementStage::CompletedRetained
                && !successor.retirements.contains_key(ticket_id)
                && !successor.incarnations.contains_key(&state.entry.incarnation);
            if !retained_exact && !omitted_as_pair {
                return Err(StateError::InvalidSnapshotState);
            }
        }
        for (incarnation, entry) in &self.incarnations {
            if successor.incarnations.get(incarnation) == Some(entry) {
                continue;
            }
            let omitted_with_completed_ticket = self
                .ticket_by_incarnation
                .get(incarnation)
                .and_then(|ticket_id| self.retirements.get(ticket_id).map(|state| (ticket_id, state)))
                .is_some_and(|(ticket_id, state)| {
                    state.entry.stage == RetirementStage::CompletedRetained
                        && !successor.retirements.contains_key(ticket_id)
                });
            if !omitted_with_completed_ticket {
                return Err(StateError::InvalidSnapshotState);
            }
        }

        // This only validates an already persisted projection. It does not infer or grant the
        // clean-start revalidation eligibility required to create such an omission.
        Ok(())
    }
}
