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

use thiserror::Error;

use super::super::codec::CodecViolation;
use super::super::sidecar::SidecarViolation;
use super::super::state::StateViolation;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum ReplayViolation {
    #[error("replay limit {limit} exceeded: actual {actual}, maximum {maximum}")]
    LimitExceeded {
        limit: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("acknowledgement slot {slot_index} is torn or structurally invalid: {source}")]
    InvalidAcknowledgementSlot { slot_index: u8, source: CodecViolation },
    #[error("acknowledgement slot {slot_index} is invalid nonzero and has no unique adjacent seal proof")]
    UnreconstructableAcknowledgementSlot { slot_index: u8 },
    #[error("acknowledgement slot {slot_index} has {candidates} adjacent seal candidates")]
    AmbiguousAcknowledgementSlot { slot_index: u8, candidates: usize },
    #[error("both acknowledgement slots are unused")]
    NoAcknowledgedFrame,
    #[error("acknowledgement history does not form one highest consecutive chain")]
    BrokenAcknowledgementChain,
    #[error("acknowledgement slot {slot_index} has no exact frame/seal evidence")]
    MissingAcknowledgementSealEvidence { slot_index: u8 },
    #[error("acknowledgement slot {slot_index} has {candidates} exact frame/seal proofs")]
    AmbiguousAcknowledgementSealEvidence { slot_index: u8, candidates: usize },
    #[error("authoritative acknowledgement does not bind an exact frame and seal")]
    AuthoritativeFrameMissing,
    #[error("generation {generation} appears more than once")]
    DuplicateGeneration { generation: u64 },
    #[error("selected generation {generation} is absent")]
    MissingSelectedGeneration { generation: u64 },
    #[error("generation relation is ambiguous or has a gap")]
    AmbiguousGenerationSet,
    #[error("marker/store/snapshot/log identity or generation binding differs")]
    GenerationBindingMismatch,
    #[error("generation {generation} log offset {offset} is invalid: {source}")]
    InvalidLog {
        generation: u64,
        offset: u64,
        source: CodecViolation,
    },
    #[error("generation {generation} has an invalid frame/seal sequence")]
    BrokenSealChain { generation: u64 },
    #[error("partial seal differs from the deterministic acknowledged seal")]
    PartialSealMismatch,
    #[error("unacknowledged suffix length {length} must be in 1..{maximum}")]
    InvalidUnacknowledgedSuffixLength { length: usize, maximum: usize },
    #[error("snapshot is invalid: {0}")]
    Snapshot(SidecarViolation),
    #[error("enabled marker is invalid: {0}")]
    Marker(SidecarViolation),
    #[error("replayed state is invalid: {0}")]
    State(StateViolation),
}
