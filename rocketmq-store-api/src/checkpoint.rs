// Copyright 2023 The RocketMQ Rust Authors
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

//! Store-owned release-checkpoint domain contracts.
//!
//! These types deliberately mirror the stable wire representation while
//! remaining independent from `rocketmq-protocol`. Wire/domain conversion is
//! owned by the Broker maintenance ingress.

use std::error::Error as StdError;
use std::fmt;

use serde::Deserialize;
use serde::Serialize;

/// Current Store checkpoint manifest schema.
pub const CHECKPOINT_SCHEMA_VERSION: u16 = 1;

/// Store implementation that produced a checkpoint.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointBackend {
    /// Local CommitLog/ConsumeQueue/Index files.
    Local,
    /// RocksDB-managed Store state.
    RocksDb,
}

/// Consistent Store offsets captured after the checkpoint flush barrier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointOffsets {
    /// Exclusive end of data accepted by the primary log.
    pub appended_offset: i64,
    /// Exclusive end durably flushed before checkpoint capture.
    pub durable_offset: i64,
    /// Highest physical source offset represented by ConsumeQueue.
    pub consume_queue_offset: i64,
    /// Highest physical source offset represented by Index.
    pub index_offset: i64,
}

impl CheckpointOffsets {
    /// Validates ordering between append, durable, and derived progress.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant error for a negative or out-of-order offset.
    pub fn validate(self) -> Result<(), CheckpointValidationError> {
        if self.appended_offset < 0 || self.durable_offset < 0 || self.consume_queue_offset < 0 || self.index_offset < 0
        {
            return Err(CheckpointValidationError::InvalidOffsets(
                "checkpoint offsets cannot be negative".to_string(),
            ));
        }
        if self.durable_offset > self.appended_offset {
            return Err(CheckpointValidationError::InvalidOffsets(
                "durable_offset cannot exceed appended_offset".to_string(),
            ));
        }
        if self.consume_queue_offset > self.durable_offset || self.index_offset > self.durable_offset {
            return Err(CheckpointValidationError::InvalidOffsets(
                "derived offsets cannot exceed durable_offset".to_string(),
            ));
        }
        Ok(())
    }
}

/// Persistent storage identity that rollback must preserve.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointStorageIdentity {
    /// Stable persistent-volume or local-volume identity.
    pub volume_id: String,
    /// Store/WAL generation expected after restore.
    pub wal_generation: u64,
}

impl CheckpointStorageIdentity {
    fn validate(&self) -> Result<(), CheckpointValidationError> {
        require_identifier("storageIdentity.volumeId", &self.volume_id)?;
        if self.wal_generation == 0 {
            return Err(CheckpointValidationError::InvalidField {
                field: "storageIdentity.walGeneration",
                reason: "must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

/// Identity and integrity metadata for one Store checkpoint artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointArtifact {
    /// Manifest schema.
    pub schema_version: u16,
    /// Globally unique checkpoint ID.
    pub checkpoint_id: String,
    /// Set containing this artifact.
    pub checkpoint_set_id: String,
    /// Storage/release generation.
    pub generation: u64,
    /// Shared write barrier ID.
    pub barrier_id: String,
    /// Artifact creation timestamp.
    pub created_at_unix_millis: u64,
    /// Uncompressed payload length.
    pub length_bytes: u64,
    /// Lowercase SHA-256 of the deterministic artifact payload.
    pub sha256: String,
    /// Durable artifact location.
    pub uri: String,
}

impl CheckpointArtifact {
    /// Validates schema, identity, length, checksum, and URI.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when the artifact is incomplete.
    pub fn validate(&self) -> Result<(), CheckpointValidationError> {
        if self.schema_version != CHECKPOINT_SCHEMA_VERSION {
            return Err(CheckpointValidationError::SchemaVersion {
                expected: CHECKPOINT_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        require_identifier("checkpointId", &self.checkpoint_id)?;
        require_identifier("checkpointSetId", &self.checkpoint_set_id)?;
        require_identifier("barrierId", &self.barrier_id)?;
        if self.generation == 0 {
            return invalid_field("generation", "must be greater than zero");
        }
        if self.created_at_unix_millis == 0 {
            return invalid_field("createdAtUnixMillis", "must be greater than zero");
        }
        if self.length_bytes == 0 {
            return invalid_field("lengthBytes", "must be greater than zero");
        }
        require_sha256(&self.sha256)?;
        if self.uri.trim().is_empty() || self.uri.bytes().any(|byte| matches!(byte, b'\r' | b'\n')) {
            return invalid_field("uri", "must be a non-empty single-line URI");
        }
        Ok(())
    }
}

/// Request to create one Store member checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointRequest {
    pub checkpoint_id: String,
    pub checkpoint_set_id: String,
    pub generation: u64,
    pub barrier_id: String,
    pub member_id: String,
    pub offsets: CheckpointOffsets,
    pub storage_identity: CheckpointStorageIdentity,
}

impl CheckpointRequest {
    /// Validates set binding and pre-flush Store observations.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when a required identity, generation,
    /// offset, or storage identity is invalid.
    pub fn validate(&self) -> Result<(), CheckpointValidationError> {
        require_identifier("checkpointId", &self.checkpoint_id)?;
        require_identifier("checkpointSetId", &self.checkpoint_set_id)?;
        require_identifier("barrierId", &self.barrier_id)?;
        require_identifier("memberId", &self.member_id)?;
        if self.generation == 0 {
            return invalid_field("generation", "must be greater than zero");
        }
        self.offsets.validate()?;
        self.storage_identity.validate()
    }
}

/// Checksummed Store checkpoint manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointManifest {
    /// Shared artifact identity and integrity metadata.
    pub artifact: CheckpointArtifact,
    /// Store member bound by the checkpoint set.
    pub member_id: String,
    /// Backend that created the artifact.
    pub backend: CheckpointBackend,
    /// Offsets observed after all required flushes completed.
    pub offsets: CheckpointOffsets,
    /// Persistent storage identity that rollback must retain.
    pub storage_identity: CheckpointStorageIdentity,
    /// Explicit proof that WAL files remain part of the checkpoint contract.
    pub wal_retained: bool,
    /// Explicit proof that rollback must reuse the existing persistent volume.
    pub persistent_volume_retained: bool,
}

impl CheckpointManifest {
    /// Validates Store checkpoint integrity and rollback invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for incomplete artifact metadata,
    /// invalid offsets/storage identity, or destructive rollback semantics.
    pub fn validate(&self) -> Result<(), CheckpointValidationError> {
        self.artifact.validate()?;
        require_identifier("memberId", &self.member_id)?;
        self.offsets.validate()?;
        self.storage_identity.validate()?;
        if !self.wal_retained || !self.persistent_volume_retained {
            return Err(CheckpointValidationError::DestructiveRollback);
        }
        Ok(())
    }
}

/// Result of non-destructive restore verification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CheckpointRestoreVerification {
    pub checkpoint_id: String,
    pub generation: u64,
    pub verified_at_unix_millis: u64,
    pub checksum_verified: bool,
    pub offsets_verified: bool,
    pub storage_identity_verified: bool,
    pub wal_retained: bool,
    pub persistent_volume_retained: bool,
}

impl CheckpointRestoreVerification {
    /// Validates that restore verification proved every production invariant.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when any integrity, offset, WAL, or PVC
    /// proof is absent.
    pub fn validate(&self) -> Result<(), CheckpointValidationError> {
        require_identifier("checkpointId", &self.checkpoint_id)?;
        if self.generation == 0 || self.verified_at_unix_millis == 0 {
            return invalid_field("generation/verifiedAtUnixMillis", "must be greater than zero");
        }
        if !self.checksum_verified
            || !self.offsets_verified
            || !self.storage_identity_verified
            || !self.wal_retained
            || !self.persistent_volume_retained
        {
            return Err(CheckpointValidationError::RestoreVerificationIncomplete);
        }
        Ok(())
    }
}

/// Store checkpoint domain invariant violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointValidationError {
    SchemaVersion { expected: u16, actual: u16 },
    InvalidField { field: &'static str, reason: String },
    InvalidOffsets(String),
    DestructiveRollback,
    RestoreVerificationIncomplete,
}

impl fmt::Display for CheckpointValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaVersion { expected, actual } => {
                write!(
                    formatter,
                    "checkpoint schema version {actual} does not match {expected}"
                )
            }
            Self::InvalidField { field, reason } => write!(formatter, "invalid checkpoint field {field}: {reason}"),
            Self::InvalidOffsets(reason) => write!(formatter, "invalid checkpoint offsets: {reason}"),
            Self::DestructiveRollback => {
                formatter.write_str("checkpoint permits destructive WAL or persistent-volume replacement")
            }
            Self::RestoreVerificationIncomplete => formatter.write_str("checkpoint restore verification is incomplete"),
        }
    }
}

impl StdError for CheckpointValidationError {}

fn require_identifier(field: &'static str, value: &str) -> Result<(), CheckpointValidationError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/' | b'@'))
    {
        return invalid_field(field, "must be a canonical identifier");
    }
    Ok(())
}

fn require_sha256(value: &str) -> Result<(), CheckpointValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return invalid_field("sha256", "must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn invalid_field<T>(field: &'static str, reason: impl Into<String>) -> Result<T, CheckpointValidationError> {
    Err(CheckpointValidationError::InvalidField {
        field,
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_json_preserves_the_stable_wire_field_names() {
        let request = CheckpointRequest {
            checkpoint_id: "checkpoint-7".to_string(),
            checkpoint_set_id: "set-7".to_string(),
            generation: 7,
            barrier_id: "barrier-42".to_string(),
            member_id: "broker-a".to_string(),
            offsets: CheckpointOffsets {
                appended_offset: 120,
                durable_offset: 120,
                consume_queue_offset: 100,
                index_offset: 100,
            },
            storage_identity: CheckpointStorageIdentity {
                volume_id: "pvc-a".to_string(),
                wal_generation: 7,
            },
        };

        request.validate().expect("valid checkpoint request");
        let value = serde_json::to_value(request).expect("serialize checkpoint request");
        assert!(value.get("checkpointId").is_some());
        assert!(value.get("storageIdentity").is_some());
        assert!(value.get("checkpoint_id").is_none());
    }

    #[test]
    fn destructive_or_unordered_checkpoint_state_is_rejected() {
        let offsets = CheckpointOffsets {
            appended_offset: 100,
            durable_offset: 101,
            consume_queue_offset: 90,
            index_offset: 90,
        };
        assert!(matches!(
            offsets.validate(),
            Err(CheckpointValidationError::InvalidOffsets(_))
        ));
    }
}
