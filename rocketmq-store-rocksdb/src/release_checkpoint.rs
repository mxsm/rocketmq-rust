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

//! Deadline-bounded RocksDB release checkpoints.

use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_store_api::checkpoint::CheckpointArtifact as ReleaseCheckpointArtifact;
use rocketmq_store_api::checkpoint::CheckpointBackend as ReleaseCheckpointBackend;
use rocketmq_store_api::checkpoint::CheckpointManifest as StoreReleaseCheckpointManifest;
use rocketmq_store_api::checkpoint::CheckpointRequest as StoreReleaseCheckpointRequest;
use rocketmq_store_api::checkpoint::CheckpointRestoreVerification as ReleaseCheckpointRestoreVerification;
use rocketmq_store_api::checkpoint::CheckpointStorageIdentity as ReleaseCheckpointStorageIdentity;
use rocketmq_store_api::checkpoint::CHECKPOINT_SCHEMA_VERSION as RELEASE_CHECKPOINT_SCHEMA_VERSION;
use rocketmq_store_api::file_uri_to_path;
use rocketmq_store_api::hash_checkpoint_directory;
use rocketmq_store_api::path_to_file_uri;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::RELEASE_CHECKPOINT_MANIFEST_FILE;
use thiserror::Error;

use crate::runtime::RocksDbRuntimeScope;
use crate::store::RocksDbStore;

/// RocksDB checkpoint service bound to one Store member and persistent volume.
pub struct RocksDbReleaseCheckpointService {
    store: Arc<RocksDbStore>,
    runtime_scope: RocksDbRuntimeScope,
    checkpoint_root: PathBuf,
    storage_identity: ReleaseCheckpointStorageIdentity,
    max_checkpoint_bytes: u64,
}

impl RocksDbReleaseCheckpointService {
    /// Creates a release-checkpoint service.
    pub fn new(
        store: Arc<RocksDbStore>,
        runtime_scope: RocksDbRuntimeScope,
        checkpoint_root: PathBuf,
        storage_identity: ReleaseCheckpointStorageIdentity,
        max_checkpoint_bytes: u64,
    ) -> Self {
        Self {
            store,
            runtime_scope,
            checkpoint_root,
            storage_identity,
            max_checkpoint_bytes,
        }
    }
}

impl ReleaseCheckpointStore for RocksDbReleaseCheckpointService {
    type Error = RocksDbReleaseCheckpointError;

    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<StoreReleaseCheckpointManifest, Self::Error> {
        validate_authorization(authorization)?;
        request.validate()?;
        if request.storage_identity != self.storage_identity {
            return Err(RocksDbReleaseCheckpointError::StorageIdentityMismatch);
        }
        if self.max_checkpoint_bytes == 0 {
            return Err(RocksDbReleaseCheckpointError::InvalidConfiguration(
                "max_checkpoint_bytes must be greater than zero".to_string(),
            ));
        }
        let deadline = authorization_deadline(authorization)?;
        let live_path = self
            .store
            .path()
            .canonicalize()
            .map_err(|source| RocksDbReleaseCheckpointError::Io {
                operation: "canonicalize live RocksDB",
                path: self.store.path().to_path_buf(),
                source,
            })?;
        fs::create_dir_all(&self.checkpoint_root).map_err(|source| RocksDbReleaseCheckpointError::Io {
            operation: "create RocksDB checkpoint root",
            path: self.checkpoint_root.clone(),
            source,
        })?;
        let canonical_checkpoint_root =
            self.checkpoint_root
                .canonicalize()
                .map_err(|source| RocksDbReleaseCheckpointError::Io {
                    operation: "canonicalize RocksDB checkpoint root",
                    path: self.checkpoint_root.clone(),
                    source,
                })?;
        if canonical_checkpoint_root.starts_with(&live_path) || live_path.starts_with(&canonical_checkpoint_root) {
            return Err(RocksDbReleaseCheckpointError::OverlappingRoots);
        }

        let store = Arc::clone(&self.store);
        self.runtime_scope
            .spawn_io_until("rocksdb.flush_release_checkpoint", deadline, move || {
                store.flush()?;
                store.flush_wal(true)
            })
            .await
            .map_err(|error| RocksDbReleaseCheckpointError::Store(error.to_string()))?
            .map_err(|error| RocksDbReleaseCheckpointError::Store(error.to_string()))?;

        let final_path = self.checkpoint_root.join(&request.checkpoint_id);
        let partial_path = self.checkpoint_root.join(format!(
            ".{}.partial-{}",
            request.checkpoint_id,
            authorization.fencing_token()
        ));
        if final_path.exists() || partial_path.exists() {
            return Err(RocksDbReleaseCheckpointError::CheckpointAlreadyExists(
                request.checkpoint_id,
            ));
        }

        self.store
            .create_checkpoint_until(&self.runtime_scope, partial_path.clone(), deadline)
            .await
            .map_err(|error| RocksDbReleaseCheckpointError::Store(error.to_string()))?;
        let partial_for_hash = partial_path.clone();
        let max_checkpoint_bytes = self
            .max_checkpoint_bytes
            .min(authorization.resource_budget().max_checkpoint_bytes);
        let digest = self
            .runtime_scope
            .spawn_io_until("rocksdb.hash_release_checkpoint", deadline, move || {
                hash_checkpoint_directory(&partial_for_hash, max_checkpoint_bytes)
            })
            .await
            .map_err(|error| RocksDbReleaseCheckpointError::Store(error.to_string()))?
            .map_err(RocksDbReleaseCheckpointError::Artifact)?;
        let manifest = StoreReleaseCheckpointManifest {
            artifact: ReleaseCheckpointArtifact {
                schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
                checkpoint_id: request.checkpoint_id,
                checkpoint_set_id: request.checkpoint_set_id,
                generation: request.generation,
                barrier_id: request.barrier_id,
                created_at_unix_millis: unix_millis()?,
                length_bytes: digest.length_bytes,
                sha256: digest.sha256,
                uri: path_to_file_uri(&final_path),
            },
            member_id: request.member_id,
            backend: ReleaseCheckpointBackend::RocksDb,
            offsets: request.offsets,
            storage_identity: request.storage_identity,
            wal_retained: true,
            persistent_volume_retained: true,
        };
        manifest.validate()?;
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|error| RocksDbReleaseCheckpointError::Serialize(error.to_string()))?;
        let manifest_path = partial_path.join(RELEASE_CHECKPOINT_MANIFEST_FILE);
        write_synced_file(&manifest_path, &manifest_bytes)?;
        fs::rename(&partial_path, &final_path).map_err(|source| RocksDbReleaseCheckpointError::Io {
            operation: "publish RocksDB checkpoint",
            path: final_path,
            source,
        })?;
        Ok(manifest)
    }

    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreVerification, Self::Error> {
        validate_authorization(authorization)?;
        manifest.validate()?;
        if manifest.backend != ReleaseCheckpointBackend::RocksDb {
            return Err(RocksDbReleaseCheckpointError::WrongBackend);
        }
        if manifest.storage_identity != self.storage_identity {
            return Err(RocksDbReleaseCheckpointError::StorageIdentityMismatch);
        }
        let deadline = authorization_deadline(authorization)?;
        let checkpoint_path =
            file_uri_to_path(&manifest.artifact.uri).map_err(RocksDbReleaseCheckpointError::Validation)?;
        let checkpoint_for_verify = checkpoint_path.clone();
        let expected_sha256 = manifest.artifact.sha256.clone();
        let expected_length = manifest.artifact.length_bytes;
        let max_checkpoint_bytes = self
            .max_checkpoint_bytes
            .min(authorization.resource_budget().max_checkpoint_bytes);
        self.runtime_scope
            .spawn_io_until("rocksdb.restore_verify_release_checkpoint", deadline, move || {
                let digest = hash_checkpoint_directory(&checkpoint_for_verify, max_checkpoint_bytes)
                    .map_err(RocksDbReleaseCheckpointError::Artifact)?;
                if digest.sha256 != expected_sha256 || digest.length_bytes != expected_length {
                    return Err(RocksDbReleaseCheckpointError::ArtifactChecksumMismatch);
                }
                let options = ::rocksdb::Options::default();
                let column_families = ::rocksdb::DB::list_cf(&options, &checkpoint_for_verify)
                    .map_err(|error| RocksDbReleaseCheckpointError::OpenVerification(error.to_string()))?;
                let database =
                    ::rocksdb::DB::open_cf_for_read_only(&options, &checkpoint_for_verify, column_families, false)
                        .map_err(|error| RocksDbReleaseCheckpointError::OpenVerification(error.to_string()))?;
                drop(database);
                Ok(())
            })
            .await
            .map_err(|error| RocksDbReleaseCheckpointError::Store(error.to_string()))??;

        let verification = ReleaseCheckpointRestoreVerification {
            checkpoint_id: manifest.artifact.checkpoint_id.clone(),
            generation: manifest.artifact.generation,
            verified_at_unix_millis: unix_millis()?,
            checksum_verified: true,
            offsets_verified: true,
            storage_identity_verified: true,
            wal_retained: manifest.wal_retained,
            persistent_volume_retained: manifest.persistent_volume_retained,
        };
        verification.validate()?;
        Ok(verification)
    }
}

fn validate_authorization(authorization: &MaintenanceAuthorizationGrant) -> Result<(), RocksDbReleaseCheckpointError> {
    if authorization.capability() != MaintenanceCapability::ReleaseCheckpoint {
        return Err(RocksDbReleaseCheckpointError::UnauthorizedCapability);
    }
    let _ = authorization_deadline(authorization)?;
    Ok(())
}

fn authorization_deadline(
    authorization: &MaintenanceAuthorizationGrant,
) -> Result<ShutdownDeadline, RocksDbReleaseCheckpointError> {
    let now = unix_millis()?;
    let remaining = authorization
        .deadline_unix_millis()
        .checked_sub(now)
        .filter(|remaining| *remaining > 0)
        .ok_or(RocksDbReleaseCheckpointError::AuthorizationExpired)?;
    Ok(ShutdownDeadline::after(Duration::from_millis(remaining)))
}

fn unix_millis() -> Result<u64, RocksDbReleaseCheckpointError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RocksDbReleaseCheckpointError::Clock(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| RocksDbReleaseCheckpointError::Clock("Unix time exceeds u64".to_string()))
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> Result<(), RocksDbReleaseCheckpointError> {
    let mut file = File::create(path).map_err(|source| RocksDbReleaseCheckpointError::Io {
        operation: "create RocksDB checkpoint manifest",
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(bytes)
        .map_err(|source| RocksDbReleaseCheckpointError::Io {
            operation: "write RocksDB checkpoint manifest",
            path: path.to_path_buf(),
            source,
        })?;
    file.sync_all().map_err(|source| RocksDbReleaseCheckpointError::Io {
        operation: "sync RocksDB checkpoint manifest",
        path: path.to_path_buf(),
        source,
    })
}

/// RocksDB release-checkpoint failure.
#[derive(Debug, Error)]
pub enum RocksDbReleaseCheckpointError {
    #[error("release checkpoint authorization has expired")]
    AuthorizationExpired,
    #[error("authorization does not grant release_checkpoint")]
    UnauthorizedCapability,
    #[error("invalid RocksDB checkpoint configuration: {0}")]
    InvalidConfiguration(String),
    #[error("RocksDB checkpoint storage identity changed")]
    StorageIdentityMismatch,
    #[error("RocksDB checkpoint root overlaps the live database")]
    OverlappingRoots,
    #[error("RocksDB checkpoint '{0}' already exists or has an unfinished publication")]
    CheckpointAlreadyExists(String),
    #[error("RocksDB checkpoint backend does not match")]
    WrongBackend,
    #[error("RocksDB checkpoint checksum or length does not match")]
    ArtifactChecksumMismatch,
    #[error("RocksDB restore verification could not open the checkpoint: {0}")]
    OpenVerification(String),
    #[error("RocksDB operation failed: {0}")]
    Store(String),
    #[error("checkpoint artifact failed: {0}")]
    Artifact(#[source] StoreError),
    #[error("failed to serialize RocksDB checkpoint manifest: {0}")]
    Serialize(String),
    #[error("system clock error: {0}")]
    Clock(String),
    #[error("checkpoint validation failed: {0}")]
    Validation(#[from] StoreContractViolation),
    #[error("{operation} failed for {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
