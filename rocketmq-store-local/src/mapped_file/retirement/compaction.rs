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

mod plan;
mod types;

fn validate_prepared_foundation(
    foundation: &plan::PreparedCompactionFoundation,
) -> Result<(), types::CompactionPlanError> {
    types::validate_meta_binding(&foundation.meta, &foundation.canonical_store_meta)?;
    types::validate_source_frontier(
        foundation.source_generation,
        foundation.source_marker_epoch,
        foundation.source_marker_anchor_sequence,
        foundation.source_terminal_sequence,
        foundation.generation_prepared.acknowledgement_epoch.saturating_sub(1),
        foundation.generation_prepared.frame_start_offset,
    )?;
    if super::sidecar::encode_snapshot(&foundation.retained_snapshot)? != foundation.canonical_retained_snapshot
        || super::sidecar::decode_snapshot(&foundation.canonical_retained_snapshot)? != foundation.retained_snapshot
    {
        return Err(types::CompactionPlanError::InvalidFoundation {
            reason: "authoritative recovered inventory is not canonical",
        });
    }
    if foundation.generation_prepared.sequence != foundation.retained_snapshot.base_sequence
        || foundation.generation_prepared.sealed_log_length == 0
    {
        return Err(types::CompactionPlanError::InvalidFoundation {
            reason: "prepared unit and canonical inventory frontier differ",
        });
    }
    Ok(())
}

const COMPACTION_LOG_BYTES_THRESHOLD: u64 = 64 * 1024 * 1024;
const COMPLETED_RECORD_THRESHOLD: u64 = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionMetrics {
    active_log_bytes: u64,
    completed_record_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionTrigger {
    LogSize,
    CompletedRecords,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionSchedule {
    NotScheduled,
    Candidate(CompactionTrigger),
}

/// Evaluates scheduling pressure only; this result is never compaction authority.
const fn compaction_schedule(metrics: CompactionMetrics) -> CompactionSchedule {
    match (
        metrics.active_log_bytes >= COMPACTION_LOG_BYTES_THRESHOLD,
        metrics.completed_record_count >= COMPLETED_RECORD_THRESHOLD,
    ) {
        (false, false) => CompactionSchedule::NotScheduled,
        (true, false) => CompactionSchedule::Candidate(CompactionTrigger::LogSize),
        (false, true) => CompactionSchedule::Candidate(CompactionTrigger::CompletedRecords),
        (true, true) => CompactionSchedule::Candidate(CompactionTrigger::Both),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum CompactionStep {
    AppendBarrier = 1,
    GenerationPrepared = 2,
    CanonicalSnapshot = 3,
    PublishSnapshot = 4,
    PublishLogOpened = 5,
    VerifyPublishedPair = 6,
    CommitMarkerSlot = 7,
    CommitLogOpened = 8,
    CommitMarkerWitness = 9,
    ReplayAndReconcile = 10,
}

impl CompactionStep {
    const fn number(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionAction {
    AcquireAppendBarrier,
    CommitGenerationPrepared,
    EncodeCanonicalSnapshot,
    PublishSnapshot,
    PublishLogOpened,
    VerifyPublishedPair(types::PublicationModel),
    CommitMarkerSlot,
    CommitLogOpened,
    CommitMarkerWitness,
    ReplaySelectedPair,
    ReconcileNamespaceAndIndexes,
}

impl CompactionAction {
    const fn protocol_step(self) -> CompactionStep {
        match self {
            Self::AcquireAppendBarrier => CompactionStep::AppendBarrier,
            Self::CommitGenerationPrepared => CompactionStep::GenerationPrepared,
            Self::EncodeCanonicalSnapshot => CompactionStep::CanonicalSnapshot,
            Self::PublishSnapshot => CompactionStep::PublishSnapshot,
            Self::PublishLogOpened => CompactionStep::PublishLogOpened,
            Self::VerifyPublishedPair(_) => CompactionStep::VerifyPublishedPair,
            Self::CommitMarkerSlot => CompactionStep::CommitMarkerSlot,
            Self::CommitLogOpened => CompactionStep::CommitLogOpened,
            Self::CommitMarkerWitness => CompactionStep::CommitMarkerWitness,
            Self::ReplaySelectedPair | Self::ReconcileNamespaceAndIndexes => CompactionStep::ReplayAndReconcile,
        }
    }
}

#[cfg(test)]
#[path = "compaction_tests.rs"]
mod tests;
