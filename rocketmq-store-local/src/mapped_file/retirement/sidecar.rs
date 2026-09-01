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

use super::identity::IdentityViolation;

mod fixed;
#[allow(
    unused_imports,
    reason = "M3 exposes the fixed-sidecar facade before the durable writer consumes every item"
)]
pub(crate) use fixed::{
    decode_enabled_marker_file, decode_enabled_marker_slot, decode_store_meta, encode_enabled_marker_file,
    encode_enabled_marker_slot, encode_store_meta, EnabledMarkerFile, EnabledMarkerSlot, StoreMeta,
};

mod snapshot;
#[allow(
    unused_imports,
    reason = "M3 exposes the snapshot facade before compaction and replay consume it"
)]
pub(crate) use snapshot::{decode_snapshot, encode_snapshot};

mod snapshot_payload;
mod types;
#[allow(
    unused_imports,
    reason = "M3 exposes snapshot state types before compaction and replay consume every item"
)]
pub(crate) use types::{
    IncarnationPhase, IncarnationSnapshotEntry, LifecycleSnapshot, QuarantineSnapshotEntry, RetirementStage,
    RetirementTicketSnapshotEntry, SnapshotEntry, SnapshotMode,
};

pub(crate) const STORE_META_LENGTH: usize = 64;
pub(crate) const ENABLED_MARKER_SLOT_LENGTH: usize = 104;
pub(crate) const ENABLED_MARKER_FILE_LENGTH: usize = ENABLED_MARKER_SLOT_LENGTH * 2;
pub(crate) const SNAPSHOT_HEADER_LENGTH: usize = 104;
pub(crate) const MIN_SNAPSHOT_FILE_LENGTH: usize = SNAPSHOT_HEADER_LENGTH + 4;
pub(crate) const MAX_SNAPSHOT_BODY_LENGTH: usize = 268_435_456;
pub(crate) const MAX_SNAPSHOT_ENTRY_COUNT: u32 = 1_000_000;
pub(crate) const MAX_SNAPSHOT_ENTRY_PAYLOAD_LENGTH: usize = 16_384;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum SidecarViolation {
    #[error("{structure} has length {actual}; expected {expected}")]
    InvalidLength {
        structure: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("fixed sidecar ended at offset {offset}; needed {needed}, remaining {remaining}")]
    UnexpectedFixedSidecarEnd {
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    #[error("{structure} magic is invalid: {found:02x?}")]
    InvalidMagic { structure: &'static str, found: [u8; 4] },
    #[error("unsupported {structure} version {major}.{minor}")]
    UnsupportedVersion {
        structure: &'static str,
        major: u16,
        minor: u16,
    },
    #[error("{structure} encoded length is {actual}; expected {expected}")]
    InvalidLengthField {
        structure: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("invalid {field} flags {value:#x}")]
    InvalidFlags { field: &'static str, value: u64 },
    #[error("unsupported marker required-feature bitmap {value:#010x}")]
    InvalidMarkerFeatures { value: u32 },
    #[error("reserved field {field} must be zero, found {value}")]
    NonZeroReserved { field: &'static str, value: u64 },
    #[error("{structure} CRC mismatch: stored {expected:#010x}, computed {actual:#010x}")]
    ChecksumMismatch {
        structure: &'static str,
        expected: u32,
        actual: u32,
    },
    #[error("{field} must not be all zero")]
    ZeroOpaqueIdentifier { field: &'static str },
    #[error("invalid {field}: {source}")]
    InvalidIdentity {
        field: &'static str,
        source: IdentityViolation,
    },
    #[error("physical marker slot index {slot_index} is invalid")]
    InvalidMarkerSlotIndex { slot_index: u8 },
    #[error("marker slot declares index {declared}, but occupies physical slot {physical}")]
    MarkerSlotPositionMismatch { declared: u8, physical: u8 },
    #[error("marker epoch must be nonzero")]
    ZeroMarkerEpoch,
    #[error("snapshot anchor sequence must be nonzero")]
    ZeroSnapshotAnchorSequence,
    #[error("marker snapshot/log generations differ: snapshot={snapshot}, log={log}")]
    MarkerGenerationMismatch { snapshot: u64, log: u64 },
    #[error("marker snapshot length {actual} is below the v1 minimum {minimum}")]
    MarkerSnapshotTooShort { actual: u64, minimum: u64 },
    #[error("ENABLED.v1 has no valid populated slot")]
    NoValidMarkerSlot,
    #[error("marker epochs are not consecutive: slot0={first}, slot1={second}")]
    NonConsecutiveMarkerEpochs { first: u64, second: u64 },
    #[error("populated marker slots do not carry the same store UUID and bootstrap id")]
    MarkerIdentityMismatch,
    #[error("marker slot history is impossible for bootstrap-at-epoch-one alternating writes")]
    InvalidMarkerSlotHistory,
    #[error("marker generations are not consecutive: older={older}, newer={newer}")]
    NonConsecutiveMarkerGenerations { older: u64, newer: u64 },
    #[error("newer marker anchor sequence {newer} does not follow older sequence {older}")]
    NonIncreasingMarkerAnchorSequence { older: u64, newer: u64 },
    #[error("snapshot is too short: length={actual}, minimum={minimum}")]
    SnapshotTooShort { actual: usize, minimum: usize },
    #[error("snapshot length arithmetic overflowed")]
    SnapshotLengthOverflow,
    #[error("snapshot body length {length} exceeds maximum {maximum}")]
    SnapshotBodyTooLarge { length: u64, maximum: u64 },
    #[error("snapshot entry count {count} exceeds maximum {maximum}")]
    SnapshotEntryCountTooLarge { count: u64, maximum: u64 },
    #[error("snapshot entry count {count} cannot fit in body length {body_length}")]
    SnapshotEntryCountExceedsBody { count: u64, body_length: u64 },
    #[error("snapshot body CRC mismatch: stored {expected:#010x}, computed {actual:#010x}")]
    SnapshotBodyChecksumMismatch { expected: u32, actual: u32 },
    #[error("snapshot body has {remaining} unconsumed bytes after the declared entries")]
    TrailingSnapshotBody { remaining: usize },
    #[error("snapshot entry kind {kind} is invalid")]
    InvalidSnapshotEntryKind { kind: u16 },
    #[error("snapshot entry kind {kind} has unsupported version {version}")]
    UnsupportedSnapshotEntryVersion { kind: u16, version: u16 },
    #[error("snapshot entry kind {kind} payload length {length} exceeds maximum {maximum}")]
    SnapshotEntryPayloadTooLarge { kind: u16, length: usize, maximum: usize },
    #[error(
        "snapshot body ended at offset {offset}; entry kind {kind} needs {needed} bytes but only {remaining} remain"
    )]
    TruncatedSnapshotEntry {
        kind: u16,
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    #[error("snapshot entry kind {kind} CRC mismatch: stored {expected:#010x}, computed {actual:#010x}")]
    SnapshotEntryChecksumMismatch { kind: u16, expected: u32, actual: u32 },
    #[error("snapshot entries are not in canonical kind/key order")]
    NonCanonicalSnapshotOrder,
    #[error("snapshot contains duplicate key for entry kind {kind}")]
    DuplicateSnapshotEntry { kind: u16 },
    #[error("snapshot/log generations differ: snapshot={snapshot}, log={log}")]
    SnapshotGenerationMismatch { snapshot: u64, log: u64 },
    #[error("snapshot generation {generation} has invalid predecessor {predecessor}")]
    InvalidSnapshotPredecessor { generation: u64, predecessor: u64 },
    #[error("snapshot mode {mode} is invalid for generation {generation}")]
    SnapshotModeGenerationMismatch { mode: &'static str, generation: u64 },
    #[error("snapshot base sequence must be nonzero")]
    ZeroSnapshotBaseSequence,
    #[error("snapshot entry belongs to a different store UUID")]
    SnapshotStoreUuidMismatch,
    #[error("{field} {high_water} is below represented identifier {represented}")]
    HighWaterBelowRepresented {
        field: &'static str,
        high_water: u64,
        represented: u64,
    },
    #[error("invalid {field} enum value {value}")]
    InvalidEnumValue { field: &'static str, value: u64 },
    #[error("incarnation phase and physical-key presence do not agree")]
    IncarnationPhaseKeyMismatch,
    #[error("expected file length must be nonzero")]
    ZeroExpectedFileLength,
    #[error("mapping generation must be nonzero")]
    ZeroMappingGeneration,
    #[error("stage sequence {sequence} is zero or greater than snapshot base {base_sequence}")]
    StageSequenceOutOfRange { sequence: u64, base_sequence: u64 },
    #[error("quarantine observation sequence {sequence} is zero or greater than snapshot base {base_sequence}")]
    ObservationSequenceOutOfRange { sequence: u64, base_sequence: u64 },
    #[error("retirement stage and tombstone-path presence do not agree")]
    RetirementTombstoneStageMismatch,
    #[error("physical file key kind {kind} is invalid")]
    InvalidPhysicalFileKeyKind { kind: u8 },
    #[error("physical file key reserved bytes must be zero")]
    NonZeroPhysicalFileKeyReserved,
    #[error("absent physical file key must be encoded as 32 zero bytes")]
    InvalidAbsentPhysicalFileKey,
    #[error("{field} is not valid UTF-8")]
    InvalidUtf8Path { field: &'static str },
    #[error("optional path {field} presence does not match flags")]
    OptionalPathFlagMismatch { field: &'static str },
    #[error("quarantine optional fields do not match flags {flags:#06x}")]
    InvalidQuarantineFields { flags: u16 },
    #[error("snapshot payload kind {kind} has {remaining} trailing bytes")]
    TrailingSnapshotPayload { kind: u16, remaining: usize },
    #[error("snapshot payload kind {kind} ended at offset {offset}; needed {needed}, remaining {remaining}")]
    UnexpectedSnapshotPayloadEnd {
        kind: u16,
        offset: usize,
        needed: usize,
        remaining: usize,
    },
}

impl SidecarViolation {
    #[cfg(test)]
    const fn category(&self) -> &'static str {
        match self {
            Self::InvalidLength { .. } | Self::InvalidLengthField { .. } => "length",
            Self::InvalidMagic { .. } => "magic",
            Self::UnsupportedVersion { .. } => "version",
            Self::InvalidFlags { .. } => "flags",
            Self::InvalidMarkerFeatures { .. } => "features",
            Self::NonZeroReserved { .. } => "reserved",
            Self::ChecksumMismatch { .. } => "crc",
            Self::SnapshotBodyChecksumMismatch { .. } => "body_crc",
            Self::InvalidMarkerSlotIndex { .. } | Self::MarkerSlotPositionMismatch { .. } => "slot_index",
            _ => "other",
        }
    }
}

#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;
