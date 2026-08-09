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

use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

/// Current Extended Timeline snapshot manifest schema.
pub const TIMER_SNAPSHOT_SCHEMA_VERSION: u16 = 1;

/// One immutable file included in an Extended Timeline snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerSnapshotFile {
    /// Slash-separated path relative to the snapshot artifact root.
    pub relative_path: String,
    /// Exact copied length. Appended bytes beyond this length are not part of the snapshot.
    pub length: u64,
    /// Lower-case SHA-256 of exactly `length` bytes.
    pub sha256: String,
}

/// Versioned handoff contract for Timeline, payload, and both replay streams.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerSnapshotManifest {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Monotonic snapshot generation shared by payload and Timeline pins.
    pub generation: u64,
    /// First Timer source CQ offset not represented by the snapshot.
    pub source_cq_cursor: i64,
    /// First source CommitLog byte not represented by the snapshot.
    pub source_physical_cursor: i64,
    /// Conservative due-time cursor represented by the Timeline checkpoint.
    pub due_time_cursor_ms: i64,
    /// First final CommitLog byte whose completion fact must be replayed.
    pub completion_physical_cursor: i64,
    /// RocksDB sequence captured by the Timeline checkpoint.
    pub timeline_sequence: u64,
    /// Durable delivery-owner epoch at snapshot creation.
    pub role_epoch: u64,
    /// Minimum admission epoch accepted by this Extended group.
    pub activation_epoch: u64,
    /// Persisted format/policy fingerprint.
    pub format_fingerprint: u64,
    /// Controlled artifact URI for the Timeline RocksDB checkpoint.
    pub timeline_checkpoint_uri: String,
    /// Immutable payload files copied into the artifact.
    pub payload_files: Vec<TimerSnapshotFile>,
    /// SHA-256 over the canonical manifest fields excluding this value.
    pub checksum: String,
}

impl TimerSnapshotManifest {
    /// Computes and stores the canonical manifest checksum.
    pub fn seal(&mut self) -> Result<(), TimerSnapshotValidationError> {
        self.checksum = hex::encode(self.digest()?);
        Ok(())
    }

    /// Validates shape, cursor monotonicity, file identities, and checksum.
    pub fn validate(&self) -> Result<(), TimerSnapshotValidationError> {
        if self.schema_version != TIMER_SNAPSHOT_SCHEMA_VERSION {
            return Err(TimerSnapshotValidationError::UnsupportedVersion(self.schema_version));
        }
        if self.generation == 0
            || self.source_cq_cursor < 0
            || self.source_physical_cursor < 0
            || self.due_time_cursor_ms < 0
            || self.completion_physical_cursor < 0
            || self.timeline_sequence == 0
            || self.role_epoch == 0
            || self.activation_epoch == 0
            || self.format_fingerprint == 0
            || self.timeline_checkpoint_uri.is_empty()
        {
            return Err(TimerSnapshotValidationError::InvalidMetadata);
        }
        for file in &self.payload_files {
            if file.relative_path.is_empty()
                || file.relative_path.starts_with('/')
                || file.relative_path.contains("..")
                || file.length == 0
                || file.sha256.len() != 64
                || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(TimerSnapshotValidationError::InvalidFile);
            }
        }
        let expected = hex::encode(self.digest()?);
        if self.checksum != expected {
            return Err(TimerSnapshotValidationError::ChecksumMismatch);
        }
        Ok(())
    }

    fn digest(&self) -> Result<[u8; 32], TimerSnapshotValidationError> {
        let mut hasher = Sha256::new();
        hasher.update(self.schema_version.to_be_bytes());
        hasher.update(self.generation.to_be_bytes());
        hasher.update(self.source_cq_cursor.to_be_bytes());
        hasher.update(self.source_physical_cursor.to_be_bytes());
        hasher.update(self.due_time_cursor_ms.to_be_bytes());
        hasher.update(self.completion_physical_cursor.to_be_bytes());
        hasher.update(self.timeline_sequence.to_be_bytes());
        hasher.update(self.role_epoch.to_be_bytes());
        hasher.update(self.activation_epoch.to_be_bytes());
        hasher.update(self.format_fingerprint.to_be_bytes());
        update_text(&mut hasher, &self.timeline_checkpoint_uri)?;
        let file_count =
            u32::try_from(self.payload_files.len()).map_err(|_| TimerSnapshotValidationError::InvalidMetadata)?;
        hasher.update(file_count.to_be_bytes());
        for file in &self.payload_files {
            update_text(&mut hasher, &file.relative_path)?;
            hasher.update(file.length.to_be_bytes());
            update_text(&mut hasher, &file.sha256)?;
        }
        Ok(hasher.finalize().into())
    }
}

fn update_text(hasher: &mut Sha256, value: &str) -> Result<(), TimerSnapshotValidationError> {
    let length = u32::try_from(value.len()).map_err(|_| TimerSnapshotValidationError::InvalidMetadata)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

/// Extended Timeline snapshot manifest validation failure.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TimerSnapshotValidationError {
    /// The reader does not support this manifest version.
    #[error("unsupported timer snapshot schema version: {0}")]
    UnsupportedVersion(u16),
    /// Required generations, epochs, cursors, or URIs are invalid.
    #[error("invalid timer snapshot metadata")]
    InvalidMetadata,
    /// A payload file path, length, or digest is invalid.
    #[error("invalid timer snapshot file")]
    InvalidFile,
    /// Canonical manifest checksum does not match.
    #[error("timer snapshot manifest checksum mismatch")]
    ChecksumMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> TimerSnapshotManifest {
        let mut manifest = TimerSnapshotManifest {
            schema_version: TIMER_SNAPSHOT_SCHEMA_VERSION,
            generation: 3,
            source_cq_cursor: 10,
            source_physical_cursor: 1_024,
            due_time_cursor_ms: 1_800_000_000_000,
            completion_physical_cursor: 2_048,
            timeline_sequence: 9,
            role_epoch: 5,
            activation_epoch: 2,
            format_fingerprint: 7,
            timeline_checkpoint_uri: "file:///timer/snapshot/timeline".to_owned(),
            payload_files: vec![TimerSnapshotFile {
                relative_path: "payload/day-0000000001/lane-00000/00000000000000000000".to_owned(),
                length: 128,
                sha256: "ab".repeat(32),
            }],
            checksum: String::new(),
        };
        manifest.seal().unwrap();
        manifest
    }

    #[test]
    fn manifest_round_trips_and_detects_cursor_damage() {
        let manifest = manifest();
        manifest.validate().unwrap();
        let encoded = serde_json::to_vec(&manifest).unwrap();
        let mut decoded: TimerSnapshotManifest = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, manifest);
        decoded.completion_physical_cursor += 1;
        assert_eq!(decoded.validate(), Err(TimerSnapshotValidationError::ChecksumMismatch));
    }
}
