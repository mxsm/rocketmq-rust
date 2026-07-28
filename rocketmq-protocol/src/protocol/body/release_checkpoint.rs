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

//! Versioned wire contracts for release snapshots and Store checkpoints.

use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::fmt;

use rocketmq_error::Sensitive;
use serde::Deserialize;
use serde::Serialize;

/// Current checkpoint manifest schema.
pub const RELEASE_CHECKPOINT_SCHEMA_VERSION: u16 = 1;

/// Non-sensitive capabilities returned after privileged authentication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaintenanceCapabilitiesResponse {
    pub schema_version: u16,
    pub policy_id: String,
    pub policy_version: u64,
    pub operations: Vec<String>,
    pub max_checkpoint_bytes: u64,
    pub max_store_members: u32,
    pub max_concurrent_operations: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<MaintenanceStoreCapabilities>,
}

/// Store identity returned by a Broker maintenance endpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MaintenanceStoreCapabilities {
    pub member_id: String,
    pub backend: ReleaseCheckpointBackend,
    pub storage_identity: ReleaseCheckpointStorageIdentity,
}

/// Store implementation that produced a checkpoint.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseCheckpointBackend {
    /// Local CommitLog/ConsumeQueue/Index files.
    Local,
    /// RocksDB-managed Store state.
    RocksDb,
}

/// Consistent Store offsets captured after the checkpoint flush barrier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseCheckpointOffsets {
    /// Exclusive end of data accepted by the primary log.
    pub appended_offset: i64,
    /// Exclusive end durably flushed before checkpoint capture.
    pub durable_offset: i64,
    /// Highest physical source offset represented by ConsumeQueue.
    pub consume_queue_offset: i64,
    /// Highest physical source offset represented by Index.
    pub index_offset: i64,
}

impl ReleaseCheckpointOffsets {
    /// Validates ordering between append, durable, and derived progress.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant error for a negative or out-of-order offset.
    pub fn validate(self) -> Result<(), ReleaseCheckpointValidationError> {
        if self.appended_offset < 0 || self.durable_offset < 0 || self.consume_queue_offset < 0 || self.index_offset < 0
        {
            return Err(ReleaseCheckpointValidationError::InvalidOffsets(
                "checkpoint offsets cannot be negative".to_string(),
            ));
        }
        if self.durable_offset > self.appended_offset {
            return Err(ReleaseCheckpointValidationError::InvalidOffsets(
                "durable_offset cannot exceed appended_offset".to_string(),
            ));
        }
        if self.consume_queue_offset > self.durable_offset || self.index_offset > self.durable_offset {
            return Err(ReleaseCheckpointValidationError::InvalidOffsets(
                "derived offsets cannot exceed durable_offset".to_string(),
            ));
        }
        Ok(())
    }
}

/// Persistent storage identity that rollback must preserve.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseCheckpointStorageIdentity {
    /// Stable PersistentVolume or local-volume identity.
    pub volume_id: String,
    /// Store/WAL generation expected after restore.
    pub wal_generation: u64,
}

impl ReleaseCheckpointStorageIdentity {
    fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        require_identifier("storageIdentity.volumeId", &self.volume_id)?;
        if self.wal_generation == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "storageIdentity.walGeneration",
                reason: "must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

/// Shared identity and integrity fields for one checkpoint artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseCheckpointArtifact {
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

impl ReleaseCheckpointArtifact {
    /// Validates schema, identity, length, checksum, and URI.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when the artifact is incomplete.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        if self.schema_version != RELEASE_CHECKPOINT_SCHEMA_VERSION {
            return Err(ReleaseCheckpointValidationError::SchemaVersion {
                expected: RELEASE_CHECKPOINT_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        require_identifier("checkpointId", &self.checkpoint_id)?;
        require_identifier("checkpointSetId", &self.checkpoint_set_id)?;
        require_identifier("barrierId", &self.barrier_id)?;
        if self.generation == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "generation",
                reason: "must be greater than zero".to_string(),
            });
        }
        if self.created_at_unix_millis == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "createdAtUnixMillis",
                reason: "must be greater than zero".to_string(),
            });
        }
        if self.length_bytes == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "lengthBytes",
                reason: "must be greater than zero".to_string(),
            });
        }
        require_sha256(&self.sha256)?;
        if self.uri.trim().is_empty() || self.uri.bytes().any(|byte| matches!(byte, b'\r' | b'\n')) {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "uri",
                reason: "must be a non-empty single-line URI".to_string(),
            });
        }
        Ok(())
    }
}

/// Request body for creating a Controller snapshot under a shared barrier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ControllerReleaseSnapshotRequest {
    pub checkpoint_id: String,
    pub checkpoint_set_id: String,
    pub generation: u64,
    pub barrier_id: String,
}

impl ControllerReleaseSnapshotRequest {
    /// Validates the checkpoint-set identity supplied to the Controller.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when an identifier is non-canonical or
    /// the requested storage generation is zero.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        require_identifier("checkpointId", &self.checkpoint_id)?;
        require_identifier("checkpointSetId", &self.checkpoint_set_id)?;
        require_identifier("barrierId", &self.barrier_id)?;
        if self.generation == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "generation",
                reason: "must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

/// Checksummed Controller snapshot manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ControllerReleaseSnapshotManifest {
    /// Shared artifact identity and integrity metadata.
    pub artifact: ReleaseCheckpointArtifact,
    /// OpenRaft snapshot identity embedded in the checksummed payload.
    pub snapshot_id: String,
    /// Last applied Raft log index captured after ReadIndex.
    pub last_applied_index: u64,
    /// Raft term of the last applied entry.
    pub last_applied_term: u64,
    /// Voter node IDs captured in the snapshot membership.
    pub voter_ids: Vec<u64>,
}

impl ControllerReleaseSnapshotManifest {
    /// Validates Controller snapshot invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for invalid artifact metadata, missing
    /// applied state, or empty/duplicate voter membership.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        self.artifact.validate()?;
        require_identifier("snapshotId", &self.snapshot_id)?;
        if self.last_applied_index == 0 || self.last_applied_term == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "lastApplied",
                reason: "term and index must be greater than zero".to_string(),
            });
        }
        let voters = self.voter_ids.iter().copied().collect::<BTreeSet<_>>();
        if voters.is_empty() || voters.len() != self.voter_ids.len() || voters.contains(&0) {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "voterIds",
                reason: "must contain unique non-zero Controller voters".to_string(),
            });
        }
        Ok(())
    }
}

/// Request body for creating one Store member checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StoreReleaseCheckpointRequest {
    pub checkpoint_id: String,
    pub checkpoint_set_id: String,
    pub generation: u64,
    pub barrier_id: String,
    pub member_id: String,
    pub offsets: ReleaseCheckpointOffsets,
    pub storage_identity: ReleaseCheckpointStorageIdentity,
}

impl StoreReleaseCheckpointRequest {
    /// Validates the set binding and pre-flush Store observations.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when any required identity, generation,
    /// offset, or storage identity is invalid.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        require_identifier("checkpointId", &self.checkpoint_id)?;
        require_identifier("checkpointSetId", &self.checkpoint_set_id)?;
        require_identifier("barrierId", &self.barrier_id)?;
        require_identifier("memberId", &self.member_id)?;
        if self.generation == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "generation",
                reason: "must be greater than zero".to_string(),
            });
        }
        self.offsets.validate()?;
        self.storage_identity.validate()
    }
}

/// Checksummed Store checkpoint manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StoreReleaseCheckpointManifest {
    /// Shared artifact identity and integrity metadata.
    pub artifact: ReleaseCheckpointArtifact,
    /// Store member bound by the checkpoint set.
    pub member_id: String,
    /// Backend that created the artifact.
    pub backend: ReleaseCheckpointBackend,
    /// Offsets observed after all required flushes completed.
    pub offsets: ReleaseCheckpointOffsets,
    /// Persistent storage identity that rollback must retain.
    pub storage_identity: ReleaseCheckpointStorageIdentity,
    /// Explicit proof that WAL files remain part of the checkpoint contract.
    pub wal_retained: bool,
    /// Explicit proof that rollback must reuse the existing persistent volume.
    pub persistent_volume_retained: bool,
}

impl StoreReleaseCheckpointManifest {
    /// Validates Store checkpoint integrity and non-destructive rollback invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error for an incomplete artifact, invalid
    /// offsets/storage identity, or a policy that permits WAL/PVC replacement.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        self.artifact.validate()?;
        require_identifier("memberId", &self.member_id)?;
        self.offsets.validate()?;
        self.storage_identity.validate()?;
        if !self.wal_retained || !self.persistent_volume_retained {
            return Err(ReleaseCheckpointValidationError::DestructiveRollback);
        }
        Ok(())
    }
}

/// Complete checkpoint set that binds Controller and all Store members.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseCheckpointSetManifest {
    pub schema_version: u16,
    pub checkpoint_set_id: String,
    pub release_id: String,
    pub generation: u64,
    pub barrier_id: String,
    pub policy_version: u64,
    pub fencing_token: u64,
    pub created_at_unix_millis: u64,
    pub controller: ControllerReleaseSnapshotManifest,
    pub stores: Vec<StoreReleaseCheckpointManifest>,
}

impl fmt::Debug for ReleaseCheckpointSetManifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleaseCheckpointSetManifest")
            .field("schema_version", &self.schema_version)
            .field("checkpoint_set_id", &self.checkpoint_set_id)
            .field("release_id", &self.release_id)
            .field("generation", &self.generation)
            .field("barrier_id", &self.barrier_id)
            .field("policy_version", &self.policy_version)
            .field("fencing_token", &Sensitive::new(self.fencing_token))
            .field("created_at_unix_millis", &self.created_at_unix_millis)
            .field("controller", &self.controller)
            .field("stores", &self.stores)
            .finish()
    }
}

impl ReleaseCheckpointSetManifest {
    /// Validates the complete Controller/Store barrier and member set.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when schema, release identity, policy
    /// version, fencing, membership, or any cross-artifact binding differs.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        if self.schema_version != RELEASE_CHECKPOINT_SCHEMA_VERSION {
            return Err(ReleaseCheckpointValidationError::SchemaVersion {
                expected: RELEASE_CHECKPOINT_SCHEMA_VERSION,
                actual: self.schema_version,
            });
        }
        require_identifier("checkpointSetId", &self.checkpoint_set_id)?;
        require_identifier("releaseId", &self.release_id)?;
        require_identifier("barrierId", &self.barrier_id)?;
        if self.generation == 0 || self.policy_version == 0 || self.fencing_token == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "generation/policyVersion/fencingToken",
                reason: "must be greater than zero".to_string(),
            });
        }
        if self.created_at_unix_millis == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "createdAtUnixMillis",
                reason: "must be greater than zero".to_string(),
            });
        }
        self.controller.validate()?;
        if self.stores.is_empty() {
            return Err(ReleaseCheckpointValidationError::MissingStoreMembers);
        }

        self.validate_binding("controller", &self.controller.artifact)?;
        let mut members = BTreeSet::new();
        for store in &self.stores {
            store.validate()?;
            self.validate_binding(&store.member_id, &store.artifact)?;
            if !members.insert(store.member_id.as_str()) {
                return Err(ReleaseCheckpointValidationError::DuplicateMember(
                    store.member_id.clone(),
                ));
            }
        }
        Ok(())
    }

    fn validate_binding(
        &self,
        member: &str,
        artifact: &ReleaseCheckpointArtifact,
    ) -> Result<(), ReleaseCheckpointValidationError> {
        if artifact.checkpoint_set_id != self.checkpoint_set_id
            || artifact.generation != self.generation
            || artifact.barrier_id != self.barrier_id
        {
            return Err(ReleaseCheckpointValidationError::SetBindingMismatch {
                member: member.to_string(),
            });
        }
        Ok(())
    }
}

/// Result of non-destructive restore verification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseCheckpointRestoreVerification {
    pub checkpoint_id: String,
    pub generation: u64,
    pub verified_at_unix_millis: u64,
    pub checksum_verified: bool,
    pub offsets_verified: bool,
    pub storage_identity_verified: bool,
    pub wal_retained: bool,
    pub persistent_volume_retained: bool,
}

impl ReleaseCheckpointRestoreVerification {
    /// Validates that restore verification proved every production invariant.
    ///
    /// # Errors
    ///
    /// Returns a typed validation error when any integrity, offset, WAL, or PVC
    /// proof is absent.
    pub fn validate(&self) -> Result<(), ReleaseCheckpointValidationError> {
        require_identifier("checkpointId", &self.checkpoint_id)?;
        if self.generation == 0 || self.verified_at_unix_millis == 0 {
            return Err(ReleaseCheckpointValidationError::InvalidField {
                field: "generation/verifiedAtUnixMillis",
                reason: "must be greater than zero".to_string(),
            });
        }
        if !self.checksum_verified
            || !self.offsets_verified
            || !self.storage_identity_verified
            || !self.wal_retained
            || !self.persistent_volume_retained
        {
            return Err(ReleaseCheckpointValidationError::RestoreVerificationIncomplete);
        }
        Ok(())
    }
}

/// Checkpoint schema or cross-artifact invariant violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleaseCheckpointValidationError {
    SchemaVersion { expected: u16, actual: u16 },
    InvalidField { field: &'static str, reason: String },
    InvalidOffsets(String),
    MissingStoreMembers,
    DuplicateMember(String),
    SetBindingMismatch { member: String },
    DestructiveRollback,
    RestoreVerificationIncomplete,
}

impl fmt::Display for ReleaseCheckpointValidationError {
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
            Self::MissingStoreMembers => formatter.write_str("checkpoint set has no Store member"),
            Self::DuplicateMember(member) => write!(formatter, "checkpoint set repeats Store member '{member}'"),
            Self::SetBindingMismatch { member } => {
                write!(formatter, "checkpoint member '{member}' does not match the set barrier")
            }
            Self::DestructiveRollback => {
                formatter.write_str("checkpoint permits destructive WAL or persistent-volume replacement")
            }
            Self::RestoreVerificationIncomplete => formatter.write_str("checkpoint restore verification is incomplete"),
        }
    }
}

impl StdError for ReleaseCheckpointValidationError {}

fn require_identifier(field: &'static str, value: &str) -> Result<(), ReleaseCheckpointValidationError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/' | b'@'))
    {
        return Err(ReleaseCheckpointValidationError::InvalidField {
            field,
            reason: "must be a canonical identifier".to_string(),
        });
    }
    Ok(())
}

fn require_sha256(value: &str) -> Result<(), ReleaseCheckpointValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ReleaseCheckpointValidationError::InvalidField {
            field: "sha256",
            reason: "must be 64 lowercase hexadecimal characters".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(id: &str) -> ReleaseCheckpointArtifact {
        ReleaseCheckpointArtifact {
            schema_version: 1,
            checkpoint_id: id.to_string(),
            checkpoint_set_id: "set-7".to_string(),
            generation: 7,
            barrier_id: "barrier-42".to_string(),
            created_at_unix_millis: 1_800_000_000_000,
            length_bytes: 512,
            sha256: "a".repeat(64),
            uri: format!("file:///checkpoints/{id}"),
        }
    }

    fn store(member: &str) -> StoreReleaseCheckpointManifest {
        StoreReleaseCheckpointManifest {
            artifact: artifact(&format!("store-{member}")),
            member_id: member.to_string(),
            backend: ReleaseCheckpointBackend::Local,
            offsets: ReleaseCheckpointOffsets {
                appended_offset: 120,
                durable_offset: 120,
                consume_queue_offset: 100,
                index_offset: 100,
            },
            storage_identity: ReleaseCheckpointStorageIdentity {
                volume_id: format!("pvc-{member}"),
                wal_generation: 7,
            },
            wal_retained: true,
            persistent_volume_retained: true,
        }
    }

    #[test]
    fn release_checkpoint_set_binds_controller_stores_and_offsets() {
        let manifest = ReleaseCheckpointSetManifest {
            schema_version: 1,
            checkpoint_set_id: "set-7".to_string(),
            release_id: "release-7".to_string(),
            generation: 7,
            barrier_id: "barrier-42".to_string(),
            policy_version: 3,
            fencing_token: 42,
            created_at_unix_millis: 1_800_000_000_000,
            controller: ControllerReleaseSnapshotManifest {
                artifact: artifact("controller"),
                snapshot_id: "snapshot-99".to_string(),
                last_applied_index: 99,
                last_applied_term: 3,
                voter_ids: vec![1, 2, 3],
            },
            stores: vec![store("broker-a"), store("broker-b")],
        };

        manifest.validate().expect("complete set should validate");
    }

    #[test]
    fn release_checkpoint_set_rejects_barrier_drift_and_destructive_restore() {
        let mut store = store("broker-a");
        store.artifact.barrier_id = "other-barrier".to_string();
        store.wal_retained = false;
        let manifest = ReleaseCheckpointSetManifest {
            schema_version: 1,
            checkpoint_set_id: "set-7".to_string(),
            release_id: "release-7".to_string(),
            generation: 7,
            barrier_id: "barrier-42".to_string(),
            policy_version: 3,
            fencing_token: 42,
            created_at_unix_millis: 1_800_000_000_000,
            controller: ControllerReleaseSnapshotManifest {
                artifact: artifact("controller"),
                snapshot_id: "snapshot-99".to_string(),
                last_applied_index: 99,
                last_applied_term: 3,
                voter_ids: vec![1],
            },
            stores: vec![store],
        };

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn release_checkpoint_set_debug_redacts_fencing_token() {
        let manifest = ReleaseCheckpointSetManifest {
            schema_version: 1,
            checkpoint_set_id: "set-7".to_string(),
            release_id: "release-7".to_string(),
            generation: 7,
            barrier_id: "barrier-42".to_string(),
            policy_version: 3,
            fencing_token: 987_654_321,
            created_at_unix_millis: 1_800_000_000_000,
            controller: ControllerReleaseSnapshotManifest {
                artifact: artifact("controller"),
                snapshot_id: "snapshot-99".to_string(),
                last_applied_index: 99,
                last_applied_term: 3,
                voter_ids: vec![1],
            },
            stores: vec![store("broker-a")],
        };

        let debug = format!("{manifest:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("987654321"));
    }
}
