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

use super::super::identity::FileIncarnationId;
use super::super::identity::PhysicalFileKey;
use super::super::identity::StoreRelativePath;
use super::super::identity::StoreUuid;
use super::super::identity::TicketId;
use super::CodecError;
use super::RecordType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationAbortReason {
    Io,
    Space,
    OperatorCancellation,
    Validation,
}

impl GenerationAbortReason {
    pub(super) const fn wire_value(self) -> u32 {
        match self {
            Self::Io => 1,
            Self::Space => 2,
            Self::OperatorCancellation => 3,
            Self::Validation => 4,
        }
    }

    pub(super) fn from_wire(value: u32) -> Result<Self, CodecError> {
        match value {
            1 => Ok(Self::Io),
            2 => Ok(Self::Space),
            3 => Ok(Self::OperatorCancellation),
            4 => Ok(Self::Validation),
            value => Err(CodecError::InvalidEnumValue {
                field: "generation_abort_reason",
                value: u64::from(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenReason {
    Compaction,
    TailRepair,
}

impl OpenReason {
    pub(super) const fn wire_value(self) -> u8 {
        match self {
            Self::Compaction => 0,
            Self::TailRepair => 1,
        }
    }

    pub(super) fn from_wire(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(Self::Compaction),
            1 => Ok(Self::TailRepair),
            value => Err(CodecError::InvalidEnumValue {
                field: "open_reason",
                value: u64::from(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementReason {
    TtlExpired,
    OffsetTruncate,
    Reset,
    DeleteLast,
    StoreDestroy,
    AllocationOrphan,
    TopicRetirement,
    DerivedFileRetirement,
    AuditedOperatorRequest,
}

impl RetirementReason {
    pub(super) const fn wire_value(self) -> u16 {
        match self {
            Self::TtlExpired => 1,
            Self::OffsetTruncate => 2,
            Self::Reset => 3,
            Self::DeleteLast => 4,
            Self::StoreDestroy => 5,
            Self::AllocationOrphan => 6,
            Self::TopicRetirement => 7,
            Self::DerivedFileRetirement => 8,
            Self::AuditedOperatorRequest => 9,
        }
    }

    pub(super) fn from_wire(value: u16) -> Result<Self, CodecError> {
        match value {
            1 => Ok(Self::TtlExpired),
            2 => Ok(Self::OffsetTruncate),
            3 => Ok(Self::Reset),
            4 => Ok(Self::DeleteLast),
            5 => Ok(Self::StoreDestroy),
            6 => Ok(Self::AllocationOrphan),
            7 => Ok(Self::TopicRetirement),
            8 => Ok(Self::DerivedFileRetirement),
            9 => Ok(Self::AuditedOperatorRequest),
            value => Err(CodecError::InvalidEnumValue {
                field: "retirement_reason",
                value: u64::from(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuarantineEntityKind {
    Create,
    Tombstone,
    Sidecar,
    Canonical,
}

impl QuarantineEntityKind {
    pub(super) const fn wire_value(self) -> u8 {
        match self {
            Self::Create => 1,
            Self::Tombstone => 2,
            Self::Sidecar => 3,
            Self::Canonical => 4,
        }
    }

    pub(super) fn from_wire(value: u8) -> Result<Self, CodecError> {
        match value {
            1 => Ok(Self::Create),
            2 => Ok(Self::Tombstone),
            3 => Ok(Self::Sidecar),
            4 => Ok(Self::Canonical),
            value => Err(CodecError::InvalidEnumValue {
                field: "quarantine_entity_kind",
                value: u64::from(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuarantineReason {
    UnknownOwner,
    KeyMismatch,
    MalformedName,
    RestoreRebindRequired,
}

impl QuarantineReason {
    pub(super) const fn wire_value(self) -> u8 {
        match self {
            Self::UnknownOwner => 1,
            Self::KeyMismatch => 2,
            Self::MalformedName => 3,
            Self::RestoreRebindRequired => 4,
        }
    }

    pub(super) fn from_wire(value: u8) -> Result<Self, CodecError> {
        match value {
            1 => Ok(Self::UnknownOwner),
            2 => Ok(Self::KeyMismatch),
            3 => Ok(Self::MalformedName),
            4 => Ok(Self::RestoreRebindRequired),
            value => Err(CodecError::InvalidEnumValue {
                field: "quarantine_reason",
                value: u64::from(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentFingerprint {
    pub(crate) length: u64,
    pub(crate) crc32: u32,
}

/// Typed payload for every record assigned by the v1 format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LedgerRecord {
    StoreInitialized {
        store_uuid: StoreUuid,
        bootstrap_id: [u8; 16],
        creation_time_ns: u64,
    },
    BootstrapInstalled {
        store_uuid: StoreUuid,
        bootstrap_id: [u8; 16],
        snapshot_generation: u64,
        snapshot_base_sequence: u64,
        snapshot_file_length: u64,
        snapshot_file_crc32: u32,
        inventory_count: u64,
        create_high_water: u64,
        ticket_high_water: u64,
    },
    LogOpened {
        store_uuid: StoreUuid,
        generation: u64,
        snapshot_generation: u64,
        predecessor_log_generation: u64,
        predecessor_terminal_acknowledged_sequence: u64,
        snapshot_base_sequence: u64,
        snapshot_file_length: u64,
        snapshot_file_crc32: u32,
        predecessor_prefix_crc32: u32,
        validated_prefix_length: u64,
        unacknowledged_suffix_length: u32,
        unacknowledged_suffix_crc32: u32,
        open_reason: OpenReason,
        predecessor_acknowledgement_epoch: u64,
    },
    GenerationPrepared {
        store_uuid: StoreUuid,
        source_generation: u64,
        target_generation: u64,
        target_snapshot_generation: u64,
        open_reason: OpenReason,
    },
    GenerationAborted {
        store_uuid: StoreUuid,
        source_generation: u64,
        target_generation: u64,
        prepared_sequence: u64,
        abort_reason: GenerationAbortReason,
    },
    MarkerCommitted {
        store_uuid: StoreUuid,
        marker_epoch: u64,
        snapshot_generation: u64,
        log_generation: u64,
        anchor_sequence: u64,
        slot_index: u8,
        slot_crc32: u32,
    },
    AllocateIncarnation {
        incarnation: FileIncarnationId,
        segment_offset: u64,
        expected_length: u64,
        create_nonce: [u8; 16],
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    },
    BindIncarnation {
        incarnation: FileIncarnationId,
        expected_length: u64,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    },
    PublishIncarnation {
        incarnation: FileIncarnationId,
        expected_length: u64,
        physical_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    },
    RetirementIntent {
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        reason: RetirementReason,
        mapping_generation: u64,
        segment_offset: u64,
        expected_length: u64,
        retirement_nonce: [u8; 16],
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    },
    LogicalRemoved {
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    },
    Tombstoned {
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        target_key: PhysicalFileKey,
        retirement_nonce: [u8; 16],
        canonical_path: StoreRelativePath,
        tombstone_path: StoreRelativePath,
    },
    NamespaceAbsent {
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        replacement_observed: bool,
        observation_time_ns: u64,
        target_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
        tombstone_path: Option<StoreRelativePath>,
    },
    Completed {
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        completion_time_ns: u64,
        namespace_absent_sequence: u64,
    },
    SupersededPath {
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        expected_target_key: PhysicalFileKey,
        observed_replacement_key: PhysicalFileKey,
        canonical_path: StoreRelativePath,
    },
    Quarantined {
        entity_kind: QuarantineEntityKind,
        reason: QuarantineReason,
        sequence_at_observation: u64,
        physical_key: Option<PhysicalFileKey>,
        content_fingerprint: Option<ContentFingerprint>,
        source_path: StoreRelativePath,
        destination_path: Option<StoreRelativePath>,
    },
}

impl LedgerRecord {
    pub(crate) const fn record_type(&self) -> RecordType {
        match self {
            Self::StoreInitialized { .. } => RecordType::StoreInitialized,
            Self::BootstrapInstalled { .. } => RecordType::BootstrapInstalled,
            Self::LogOpened { .. } => RecordType::LogOpened,
            Self::GenerationPrepared { .. } => RecordType::GenerationPrepared,
            Self::GenerationAborted { .. } => RecordType::GenerationAborted,
            Self::MarkerCommitted { .. } => RecordType::MarkerCommitted,
            Self::AllocateIncarnation { .. } => RecordType::AllocateIncarnation,
            Self::BindIncarnation { .. } => RecordType::BindIncarnation,
            Self::PublishIncarnation { .. } => RecordType::PublishIncarnation,
            Self::RetirementIntent { .. } => RecordType::RetirementIntent,
            Self::LogicalRemoved { .. } => RecordType::LogicalRemoved,
            Self::Tombstoned { .. } => RecordType::Tombstoned,
            Self::NamespaceAbsent { .. } => RecordType::NamespaceAbsent,
            Self::Completed { .. } => RecordType::Completed,
            Self::SupersededPath { .. } => RecordType::SupersededPath,
            Self::Quarantined { .. } => RecordType::Quarantined,
        }
    }
}
