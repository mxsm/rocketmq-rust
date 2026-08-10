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

use super::super::codec::ContentFingerprint;
use super::super::codec::QuarantineEntityKind;
use super::super::codec::QuarantineReason;
use super::super::codec::RetirementReason;
use super::super::identity::FileIncarnationId;
use super::super::identity::PhysicalFileKey;
use super::super::identity::StoreRelativePath;
use super::super::identity::StoreUuid;
use super::super::identity::TicketId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotMode {
    OrdinaryCompaction,
    BootstrapInventory,
    TailRepair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncarnationPhase {
    Allocated,
    Bound,
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementStage {
    IntentDurable,
    LogicalRemoved,
    Tombstoned,
    NamespaceAbsent,
    CompletedRetained,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IncarnationSnapshotEntry {
    pub(crate) incarnation: FileIncarnationId,
    pub(crate) phase: IncarnationPhase,
    pub(crate) segment_offset: u64,
    pub(crate) expected_file_length: u64,
    pub(crate) create_nonce: [u8; 16],
    pub(crate) physical_key: Option<PhysicalFileKey>,
    pub(crate) canonical_path: StoreRelativePath,
    pub(crate) create_file_path: StoreRelativePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetirementTicketSnapshotEntry {
    pub(crate) ticket_id: TicketId,
    pub(crate) incarnation: FileIncarnationId,
    pub(crate) stage: RetirementStage,
    pub(crate) superseded_path_observed: bool,
    pub(crate) quarantined: bool,
    pub(crate) reason: RetirementReason,
    pub(crate) stage_sequence: u64,
    pub(crate) mapping_generation: u64,
    pub(crate) segment_offset: u64,
    pub(crate) expected_file_length: u64,
    pub(crate) retirement_nonce: [u8; 16],
    pub(crate) target_key: PhysicalFileKey,
    pub(crate) canonical_path: StoreRelativePath,
    pub(crate) tombstone_path: Option<StoreRelativePath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuarantineSnapshotEntry {
    pub(crate) entity_kind: QuarantineEntityKind,
    pub(crate) reason: QuarantineReason,
    pub(crate) sequence_at_observation: u64,
    pub(crate) physical_key: Option<PhysicalFileKey>,
    pub(crate) content_fingerprint: Option<ContentFingerprint>,
    pub(crate) source_path: StoreRelativePath,
    pub(crate) destination_path: Option<StoreRelativePath>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotEntry {
    Incarnation(IncarnationSnapshotEntry),
    RetirementTicket(RetirementTicketSnapshotEntry),
    Quarantine(QuarantineSnapshotEntry),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LifecycleSnapshot {
    pub(crate) mode: SnapshotMode,
    pub(crate) store_uuid: StoreUuid,
    pub(crate) generation: u64,
    pub(crate) log_generation: u64,
    pub(crate) predecessor_log_generation: u64,
    pub(crate) base_sequence: u64,
    pub(crate) create_high_water: u64,
    pub(crate) ticket_high_water: u64,
    pub(crate) entries: Vec<SnapshotEntry>,
}
