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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailRepairDecision {
    pub(super) generation: u64,
    pub(super) acknowledged_prefix_length: u64,
    pub(super) suffix_length: u32,
    pub(super) suffix_crc32: u32,
}

impl TailRepairDecision {
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn acknowledged_prefix_length(&self) -> u64 {
        self.acknowledged_prefix_length
    }

    pub(crate) const fn suffix_length(&self) -> u32 {
        self.suffix_length
    }

    pub(crate) const fn suffix_crc32(&self) -> u32 {
        self.suffix_crc32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumeGenerationDecision {
    pub(super) source_generation: u64,
    pub(super) target_generation: u64,
}

impl ResumeGenerationDecision {
    pub(crate) const fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub(crate) const fn target_generation(&self) -> u64 {
        self.target_generation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SealEvidence {
    pub(super) slot: AcknowledgementSlot,
    pub(super) encoded_slot: [u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
    pub(super) generation: u64,
    pub(super) sealed_log_length: u64,
    pub(super) frame_start_offset: u64,
    pub(super) encoded_frame: Vec<u8>,
    pub(super) record: Option<LedgerRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedAcknowledgement {
    pub(super) authoritative: AcknowledgementSlot,
    pub(super) encoded_authoritative: [u8; super::super::codec::ACKNOWLEDGEMENT_SLOT_LENGTH],
}

#[derive(Debug)]
pub(super) struct ParsedGeneration<'a> {
    pub(super) bytes: GenerationBytes<'a>,
    pub(super) snapshot: LifecycleSnapshot,
    pub(super) evidence_range: std::ops::Range<usize>,
    pub(super) tail: Option<LogTail>,
}

#[derive(Debug, Clone)]
pub(super) struct LogTail {
    pub(super) offset: u64,
    pub(super) bytes: Vec<u8>,
    pub(super) complete_frame: Option<TrailingFrame>,
}

#[derive(Debug, Clone)]
pub(super) struct TrailingFrame {
    pub(super) sequence: u64,
    pub(super) frame_end_offset: u64,
    pub(super) encoded_frame: Vec<u8>,
    pub(super) record: Option<LedgerRecord>,
    pub(super) following_bytes: Vec<u8>,
}
