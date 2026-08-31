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

//! Contract violations shared by storage capabilities and implementations.

use std::path::PathBuf;

use thiserror::Error;

use crate::progress::DerivedEngine;

/// A deterministic violation of a public Store API contract.
///
/// This type contains caller, persisted-format, and state-invariant evidence.
/// Operational failures such as filesystem I/O are exposed through
/// [`crate::StoreError`] instead, with their private typed causes preserved.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum StoreContractViolation {
    /// A checkpoint artifact exceeded its configured byte budget.
    #[error("checkpoint is {actual} bytes, exceeding the {maximum} byte limit")]
    CheckpointArtifactTooLarge {
        /// Observed bytes.
        actual: u64,
        /// Configured maximum bytes.
        maximum: u64,
    },
    /// A checkpoint artifact contains no payload bytes.
    #[error("checkpoint payload is empty")]
    CheckpointArtifactEmpty,
    /// A symbolic link was found in a checkpoint artifact.
    #[error("checkpoint contains symbolic link {}", .0.display())]
    CheckpointArtifactSymbolicLink(PathBuf),
    /// A discovered checkpoint entry escaped the artifact root.
    #[error("checkpoint path escaped its root: {}", .0.display())]
    CheckpointArtifactPathEscaped(PathBuf),
    /// A checkpoint entry was neither a regular file nor a directory.
    #[error("checkpoint contains unsupported file type {}", .0.display())]
    CheckpointArtifactUnsupportedFileType(PathBuf),
    /// A checkpoint URI is not a supported local file URI.
    #[error("unsupported checkpoint URI: {0}")]
    CheckpointArtifactUnsupportedUri(String),

    /// A checkpoint schema version does not match the supported version.
    #[error("checkpoint schema version {actual} does not match {expected}")]
    CheckpointSchemaVersion {
        /// Supported schema version.
        expected: u16,
        /// Observed schema version.
        actual: u16,
    },
    /// A checkpoint field violates its field-level contract.
    #[error("invalid checkpoint field {field}: {reason}")]
    CheckpointInvalidField {
        /// Stable field name.
        field: &'static str,
        /// Validation reason.
        reason: String,
    },
    /// Checkpoint offsets violate their ordering contract.
    #[error("invalid checkpoint offsets: {0}")]
    CheckpointInvalidOffsets(String),
    /// A checkpoint permits a destructive rollback.
    #[error("checkpoint permits destructive WAL or persistent-volume replacement")]
    CheckpointDestructiveRollback,
    /// Checkpoint restore verification is incomplete.
    #[error("checkpoint restore verification is incomplete")]
    CheckpointRestoreVerificationIncomplete,

    /// A master epoch was not positive.
    #[error("master epoch must be positive, got {0}")]
    HaInvalidMasterEpoch(i32),
    /// A sync-state-set epoch was not positive.
    #[error("sync-state-set epoch must be positive, got {0}")]
    HaInvalidSyncStateSetEpoch(i32),
    /// A broker identifier was negative.
    #[error("broker id must be non-negative, got {0}")]
    HaInvalidBrokerId(i64),
    /// An unsigned wire broker identifier exceeded the canonical representation.
    #[error("broker id exceeds the canonical signed range, got {0}")]
    HaBrokerIdOutOfRange(u64),
    /// A replica policy did not require a remote replica.
    #[error("replica ACK count must include a remote replica, got {0}")]
    HaInvalidReplicaCount(usize),
    /// A legacy ACK count or sentinel was unknown.
    #[error("unsupported legacy ACK policy value {0}")]
    HaInvalidAckPolicy(i32),
    /// A physical offset was negative.
    #[error("HA offset must be non-negative, got {0}")]
    HaInvalidOffset(i64),
    /// A Controller write-lease generation was zero.
    #[error("write-lease generation must be positive, got {0}")]
    HaInvalidLeaseGeneration(u64),
    /// The sync-state set was empty.
    #[error("sync-state set must not be empty")]
    HaEmptySyncStateSet,
    /// The current leader was absent from the sync-state set.
    #[error("leader broker {0} is absent from the sync-state set")]
    HaLeaderMissingFromSyncStateSet(i64),

    /// An appended range is empty.
    #[error("appended range is empty")]
    AppendReceiptEmptyRange,
    /// An appended range is reversed.
    #[error("appended range is reversed")]
    AppendReceiptReversedRange,
    /// A rejected append status carries an appended range.
    #[error("rejected append status cannot carry an appended range")]
    AppendReceiptRejectedStatusWithRange,
    /// An accepted append status does not carry an appended range.
    #[error("accepted append status requires an appended range")]
    AppendReceiptAcceptedStatusWithoutRange,
    /// The appended watermark does not cover the appended range.
    #[error("appended watermark does not cover the appended range")]
    AppendReceiptAppendedWatermarkBehindRange,
    /// The durable watermark does not cover the claimed durability.
    #[error("durable watermark does not cover the claimed durability")]
    AppendReceiptDurableWatermarkBehindRange,
    /// The durable watermark exceeds appended progress.
    #[error("durable watermark exceeds appended progress")]
    AppendReceiptDurableWatermarkAheadOfAppended,
    /// Memory durability under-reports reached local durability.
    #[error("memory durability under-reports reached local durability")]
    AppendReceiptMemoryDurabilityAlreadyCovered,
    /// Replicated durability lacks a canonical acknowledgement decision.
    #[error("replicated durability requires a canonical replication acknowledgement")]
    AppendReceiptReplicatedDurabilityRequiresDecision,
    /// A replication acknowledgement does not cover the appended range.
    #[error("replication acknowledgement does not cover the appended range")]
    AppendReceiptReplicationDecisionBehindRange,

    /// A derived record has zero length.
    #[error("derived record length must be non-zero")]
    DerivedRecordEmpty,
    /// A derived record physical range overflows.
    #[error("derived record physical range overflows")]
    DerivedRecordRangeOverflow,
    /// A derived record belongs to another source epoch.
    #[error("source epoch mismatch: expected {expected}, got {actual}")]
    DerivedCursorSourceEpochMismatch {
        /// Cursor source epoch.
        expected: u64,
        /// Record source epoch.
        actual: u64,
    },
    /// A derived record begins after the next expected offset.
    #[error("derived cursor gap: expected offset {expected}, got {actual}")]
    DerivedCursorGap {
        /// Next expected offset.
        expected: u64,
        /// Record start offset.
        actual: u64,
    },
    /// A derived record partially overlaps the committed prefix.
    #[error("derived record {record_start}..{record_end} partially overlaps committed offset {committed}")]
    DerivedCursorPartialOverlap {
        /// Exclusive committed offset.
        committed: u64,
        /// Record start offset.
        record_start: u64,
        /// Record end offset.
        record_end: u64,
    },
    /// A derived checkpoint has the wrong encoded length.
    #[error("invalid checkpoint length: expected {expected}, got {actual}")]
    DerivedCheckpointInvalidLength {
        /// Required encoded length.
        expected: usize,
        /// Observed encoded length.
        actual: usize,
    },
    /// A derived checkpoint has invalid magic bytes.
    #[error("invalid derived checkpoint magic")]
    DerivedCheckpointInvalidMagic,
    /// A derived checkpoint uses an unsupported version.
    #[error("unsupported derived checkpoint version {0}")]
    DerivedCheckpointUnsupportedVersion(u16),
    /// A derived checkpoint names an unknown engine.
    #[error("unknown derived engine code {0}")]
    DerivedCheckpointUnknownEngine(u8),
    /// A derived checkpoint belongs to another engine.
    #[error(
        "derived checkpoint owner mismatch: expected {}, got {}",
        expected.as_str(),
        actual.as_str()
    )]
    DerivedCheckpointEngineMismatch {
        /// Expected engine owner.
        expected: DerivedEngine,
        /// Encoded engine owner.
        actual: DerivedEngine,
    },
    /// A derived checkpoint uses a non-zero reserved byte.
    #[error("invalid derived checkpoint reserved byte {0}")]
    DerivedCheckpointInvalidReservedByte(u8),
    /// A derived checkpoint fails its integrity check.
    #[error("derived checkpoint checksum mismatch")]
    DerivedCheckpointChecksumMismatch,

    /// A timer snapshot uses an unsupported schema version.
    #[error("unsupported timer snapshot schema version: {0}")]
    TimerSnapshotUnsupportedVersion(u16),
    /// Required timer snapshot metadata is invalid.
    #[error("invalid timer snapshot metadata")]
    TimerSnapshotInvalidMetadata,
    /// A timer snapshot file declaration is invalid.
    #[error("invalid timer snapshot file")]
    TimerSnapshotInvalidFile,
    /// A timer snapshot manifest fails its integrity check.
    #[error("timer snapshot manifest checksum mismatch")]
    TimerSnapshotChecksumMismatch,
    /// Native timer snapshot fields do not match the selected index kind.
    #[error("invalid native Timeline snapshot binding")]
    TimerSnapshotInvalidNativeBinding,
    /// A timer snapshot artifact differs from its manifest declaration.
    #[error("timer snapshot artifact file identity does not match its manifest")]
    TimerSnapshotArtifactDigestMismatch,

    /// A persisted timer engine identifier is not recognized.
    #[error("unknown timer engine id: {0}")]
    TimerUnknownEngine(String),
    /// A timer payload locator cannot identify a non-empty CommitLog record.
    #[error("timer payload locator requires a non-negative offset and non-zero size")]
    TimerInvalidPayloadLocator,
    /// A timer payload-store locator identifies an empty record.
    #[error("timer payload-store locator requires a non-zero length")]
    TimerInvalidPayloadStoreLocator,
    /// A persisted timer route is incomplete.
    #[error("timer route requires a non-zero format version and non-empty delivery token")]
    TimerInvalidRoute,
}
