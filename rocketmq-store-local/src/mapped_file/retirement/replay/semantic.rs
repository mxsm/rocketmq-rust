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

pub(super) struct SemanticGeneration {
    pub(super) state: LedgerStateMachine,
    pub(super) sealed_through: u64,
    pub(super) applied_through: u64,
}

pub(super) fn replay_generations(
    parsed: &BTreeMap<u64, ParsedGeneration<'_>>,
    evidence: &[SealEvidence],
) -> Result<BTreeMap<u64, SemanticGeneration>, ReplayError> {
    let mut states = BTreeMap::<u64, SemanticGeneration>::new();
    for (&generation_number, generation) in parsed {
        let mut state = LedgerStateMachine::from_snapshot(&generation.snapshot).map_err(ReplayError::State)?;
        if let Some(predecessor_number) = generation_number.checked_sub(1) {
            if let (Some(predecessor), Some(source)) =
                (parsed.get(&predecessor_number), states.get(&predecessor_number))
            {
                validate_transition(predecessor, source, generation, &state, evidence)?;
            }
        }

        let units = evidence
            .get(generation.evidence_range.clone())
            .ok_or(ReplayError::BrokenSealChain {
                generation: generation_number,
            })?;
        let mut sealed_through = generation.snapshot.base_sequence;
        let mut applied_through = sealed_through;
        for unit in units {
            if unit.slot.frame_sequence <= generation.snapshot.base_sequence {
                continue;
            }
            state
                .apply(unit.slot.frame_sequence, unit.record.clone())
                .map_err(ReplayError::State)?;
            sealed_through = unit.slot.frame_sequence;
            applied_through = unit.slot.frame_sequence;
        }
        if units.is_empty() {
            if let Some(frame) = generation.tail.as_ref().and_then(|tail| tail.complete_frame.as_ref()) {
                state
                    .apply(frame.sequence, frame.record.clone())
                    .map_err(ReplayError::State)?;
                applied_through = frame.sequence;
            }
        }
        states.insert(
            generation_number,
            SemanticGeneration {
                state,
                sealed_through,
                applied_through,
            },
        );
    }
    Ok(states)
}

fn validate_transition(
    predecessor: &ParsedGeneration<'_>,
    source: &SemanticGeneration,
    successor: &ParsedGeneration<'_>,
    successor_snapshot: &LedgerStateMachine,
    evidence: &[SealEvidence],
) -> Result<(), ReplayError> {
    source
        .state
        .validate_successor_projection(successor_snapshot)
        .map_err(ReplayError::State)?;
    let terminal = evidence
        .get(predecessor.evidence_range.clone())
        .and_then(|units| units.last())
        .ok_or(ReplayError::AuthoritativeFrameMissing)?;
    let opener = evidence
        .get(successor.evidence_range.clone())
        .and_then(|units| units.first())
        .and_then(|unit| unit.record.as_ref())
        .or_else(|| {
            successor
                .tail
                .as_ref()
                .and_then(|tail| tail.complete_frame.as_ref())
                .and_then(|frame| frame.record.as_ref())
        });
    let Some(LedgerRecord::LogOpened { open_reason, .. }) = opener else {
        return Err(ReplayError::GenerationBindingMismatch);
    };
    match open_reason {
        OpenReason::Compaction => {
            let expected = LedgerRecord::GenerationPrepared {
                store_uuid: successor.snapshot.store_uuid,
                source_generation: predecessor.bytes.generation,
                target_generation: successor.bytes.generation,
                target_snapshot_generation: successor.snapshot.generation,
                open_reason: OpenReason::Compaction,
            };
            if terminal.record.as_ref() != Some(&expected)
                || source.state.prepared_generation()
                    != Some((terminal.slot.frame_sequence, successor.bytes.generation))
                || predecessor.tail.is_some()
            {
                return Err(ReplayError::GenerationBindingMismatch);
            }
        }
        OpenReason::TailRepair => {
            if source.state.prepared_generation().is_some()
                || matches!(terminal.record.as_ref(), Some(LedgerRecord::GenerationPrepared { .. }))
                || predecessor.tail.is_none()
            {
                return Err(ReplayError::GenerationBindingMismatch);
            }
        }
    }
    if successor_snapshot.prepared_generation().is_some() {
        return Err(ReplayError::GenerationBindingMismatch);
    }
    Ok(())
}
