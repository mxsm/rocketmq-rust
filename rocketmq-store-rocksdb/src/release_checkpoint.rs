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

use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
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
use rocketmq_store_api::path_to_file_uri;
use rocketmq_store_api::CheckpointDirectoryDigest;
use rocketmq_store_api::ReleaseCheckpointCreateOutcome;
use rocketmq_store_api::ReleaseCheckpointCreateRejection;
use rocketmq_store_api::ReleaseCheckpointRestoreOutcome;
use rocketmq_store_api::ReleaseCheckpointRestoreRejection;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::RELEASE_CHECKPOINT_MANIFEST_FILE;
use sha2::Digest;
use sha2::Sha256;

use crate::runtime::RocksDbRuntimeScope;
use crate::store::RocksDbStore;

type CheckpointArtifactHasher = fn(&Path, u64, StoreOperation) -> Result<CheckpointDirectoryDigest, StoreError>;
type CheckpointClock = fn() -> Result<u64, RocksDbReleaseCheckpointError>;

/// RocksDB checkpoint service bound to one Store member and persistent volume.
pub struct RocksDbReleaseCheckpointService {
    store: Arc<RocksDbStore>,
    runtime_scope: RocksDbRuntimeScope,
    checkpoint_root: PathBuf,
    storage_identity: ReleaseCheckpointStorageIdentity,
    max_checkpoint_bytes: u64,
    artifact_hasher: CheckpointArtifactHasher,
    clock: CheckpointClock,
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
            artifact_hasher: hash_rocksdb_checkpoint_directory,
            clock: unix_millis,
        }
    }

    #[cfg(test)]
    fn with_artifact_hasher(mut self, artifact_hasher: CheckpointArtifactHasher) -> Self {
        self.artifact_hasher = artifact_hasher;
        self
    }

    #[cfg(test)]
    fn with_clock(mut self, clock: CheckpointClock) -> Self {
        self.clock = clock;
        self
    }

    async fn create_release_checkpoint_inner(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<StoreReleaseCheckpointManifest, RocksDbReleaseCheckpointError> {
        validate_authorization(authorization, self.clock)?;
        request.validate()?;
        if request.storage_identity != self.storage_identity {
            return Err(RocksDbReleaseCheckpointError::Violation(
                RocksDbReleaseCheckpointViolation::StorageIdentityMismatch,
            ));
        }
        if self.max_checkpoint_bytes == 0 {
            return Err(RocksDbReleaseCheckpointError::Violation(
                RocksDbReleaseCheckpointViolation::InvalidConfiguration,
            ));
        }
        let deadline = authorization_deadline(authorization, self.clock)?;
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
            return Err(RocksDbReleaseCheckpointError::Violation(
                RocksDbReleaseCheckpointViolation::OverlappingRoots,
            ));
        }

        let store = Arc::clone(&self.store);
        self.runtime_scope
            .spawn_io_until(
                "rocksdb.flush_release_checkpoint",
                rocketmq_store_api::StoreOperation::Flush,
                deadline,
                move || {
                    store.flush(rocketmq_store_api::StoreOperation::Flush)?;
                    store.flush_wal(rocketmq_store_api::StoreOperation::Flush, true)
                },
            )
            .await
            .map_err(RocksDbReleaseCheckpointError::Store)?
            .map_err(RocksDbReleaseCheckpointError::Store)?;

        let final_path = self.checkpoint_root.join(&request.checkpoint_id);
        let partial_path = self.checkpoint_root.join(format!(
            ".{}.partial-{}",
            request.checkpoint_id,
            authorization.fencing_token()
        ));
        if final_path.exists() || partial_path.exists() {
            return Err(RocksDbReleaseCheckpointError::CheckpointAlreadyExists);
        }

        self.store
            .create_checkpoint_until(&self.runtime_scope, partial_path.clone(), deadline)
            .await
            .map_err(RocksDbReleaseCheckpointError::Store)?;
        let partial_for_hash = partial_path.clone();
        let max_checkpoint_bytes = self
            .max_checkpoint_bytes
            .min(authorization.resource_budget().max_checkpoint_bytes);
        let artifact_hasher = self.artifact_hasher;
        let digest = self
            .runtime_scope
            .spawn_io_until(
                "rocksdb.hash_release_checkpoint",
                rocketmq_store_api::StoreOperation::Flush,
                deadline,
                move || artifact_hasher(&partial_for_hash, max_checkpoint_bytes, StoreOperation::Flush),
            )
            .await
            .map_err(RocksDbReleaseCheckpointError::Store)?
            .map_err(RocksDbReleaseCheckpointError::Artifact)?;
        let manifest = StoreReleaseCheckpointManifest {
            artifact: ReleaseCheckpointArtifact {
                schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
                checkpoint_id: request.checkpoint_id,
                checkpoint_set_id: request.checkpoint_set_id,
                generation: request.generation,
                barrier_id: request.barrier_id,
                created_at_unix_millis: (self.clock)()?,
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
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(RocksDbReleaseCheckpointError::Serialize)?;
        let manifest_path = partial_path.join(RELEASE_CHECKPOINT_MANIFEST_FILE);
        write_synced_file(&manifest_path, &manifest_bytes)?;
        fs::rename(&partial_path, &final_path).map_err(|source| RocksDbReleaseCheckpointError::Io {
            operation: "publish RocksDB checkpoint",
            path: final_path,
            source,
        })?;
        Ok(manifest)
    }

    async fn restore_verify_release_checkpoint_inner(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreVerification, RocksDbReleaseCheckpointError> {
        validate_authorization(authorization, self.clock)?;
        manifest.validate()?;
        if manifest.backend != ReleaseCheckpointBackend::RocksDb {
            return Err(RocksDbReleaseCheckpointError::Violation(
                RocksDbReleaseCheckpointViolation::WrongBackend,
            ));
        }
        if manifest.storage_identity != self.storage_identity {
            return Err(RocksDbReleaseCheckpointError::Violation(
                RocksDbReleaseCheckpointViolation::StorageIdentityMismatch,
            ));
        }
        let deadline = authorization_deadline(authorization, self.clock)?;
        let checkpoint_path =
            file_uri_to_path(&manifest.artifact.uri).map_err(RocksDbReleaseCheckpointError::Validation)?;
        let checkpoint_for_verify = checkpoint_path.clone();
        let expected_sha256 = manifest.artifact.sha256.clone();
        let expected_length = manifest.artifact.length_bytes;
        let max_checkpoint_bytes = self
            .max_checkpoint_bytes
            .min(authorization.resource_budget().max_checkpoint_bytes);
        let artifact_hasher = self.artifact_hasher;
        self.runtime_scope
            .spawn_io_until(
                "rocksdb.restore_verify_release_checkpoint",
                rocketmq_store_api::StoreOperation::Read,
                deadline,
                move || {
                    let digest = artifact_hasher(&checkpoint_for_verify, max_checkpoint_bytes, StoreOperation::Read)
                        .map_err(RocksDbReleaseCheckpointError::Artifact)?;
                    if digest.sha256 != expected_sha256 || digest.length_bytes != expected_length {
                        return Err(RocksDbReleaseCheckpointError::Violation(
                            RocksDbReleaseCheckpointViolation::ArtifactChecksumMismatch,
                        ));
                    }
                    let options = ::rocksdb::Options::default();
                    let column_families = ::rocksdb::DB::list_cf(&options, &checkpoint_for_verify)
                        .map_err(RocksDbReleaseCheckpointError::Native)?;
                    let database =
                        ::rocksdb::DB::open_cf_for_read_only(&options, &checkpoint_for_verify, column_families, false)
                            .map_err(RocksDbReleaseCheckpointError::Native)?;
                    drop(database);
                    Ok(())
                },
            )
            .await
            .map_err(RocksDbReleaseCheckpointError::Store)??;

        let verification = ReleaseCheckpointRestoreVerification {
            checkpoint_id: manifest.artifact.checkpoint_id.clone(),
            generation: manifest.artifact.generation,
            verified_at_unix_millis: (self.clock)()?,
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

impl ReleaseCheckpointStore for RocksDbReleaseCheckpointService {
    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<ReleaseCheckpointCreateOutcome, StoreError> {
        match self.create_release_checkpoint_inner(authorization, request).await {
            Ok(manifest) => Ok(ReleaseCheckpointCreateOutcome::Created(manifest)),
            Err(RocksDbReleaseCheckpointError::AuthorizationExpired) => Ok(ReleaseCheckpointCreateOutcome::Rejected(
                ReleaseCheckpointCreateRejection::AuthorizationExpired,
            )),
            Err(RocksDbReleaseCheckpointError::UnauthorizedCapability) => Ok(ReleaseCheckpointCreateOutcome::Rejected(
                ReleaseCheckpointCreateRejection::CapabilityNotGranted,
            )),
            Err(RocksDbReleaseCheckpointError::CheckpointAlreadyExists) => Ok(
                ReleaseCheckpointCreateOutcome::Rejected(ReleaseCheckpointCreateRejection::AlreadyExists),
            ),
            Err(RocksDbReleaseCheckpointError::Artifact(source)) => {
                if let Some((actual_bytes, maximum_bytes)) = checkpoint_capacity_rejection(&source) {
                    Ok(ReleaseCheckpointCreateOutcome::Rejected(
                        ReleaseCheckpointCreateRejection::CapacityExceeded {
                            actual_bytes,
                            maximum_bytes,
                        },
                    ))
                } else {
                    Err(source)
                }
            }
            Err(error) => Err(rocksdb_checkpoint_error(StoreOperation::Flush, error)),
        }
    }

    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreOutcome, StoreError> {
        match self
            .restore_verify_release_checkpoint_inner(authorization, manifest)
            .await
        {
            Ok(verification) => Ok(ReleaseCheckpointRestoreOutcome::Verified(verification)),
            Err(RocksDbReleaseCheckpointError::AuthorizationExpired) => Ok(ReleaseCheckpointRestoreOutcome::Rejected(
                ReleaseCheckpointRestoreRejection::AuthorizationExpired,
            )),
            Err(RocksDbReleaseCheckpointError::UnauthorizedCapability) => Ok(
                ReleaseCheckpointRestoreOutcome::Rejected(ReleaseCheckpointRestoreRejection::CapabilityNotGranted),
            ),
            Err(error) => Err(rocksdb_checkpoint_error(StoreOperation::Read, error)),
        }
    }
}

fn checkpoint_capacity_rejection(error: &StoreError) -> Option<(u64, u64)> {
    let mut source = std::error::Error::source(error);
    while let Some(current) = source {
        if let Some(StoreContractViolation::CheckpointArtifactTooLarge { actual, maximum }) =
            current.downcast_ref::<StoreContractViolation>()
        {
            return Some((*actual, *maximum));
        }
        source = current.source();
    }
    None
}

fn hash_rocksdb_checkpoint_directory(
    checkpoint_root: &Path,
    max_checkpoint_bytes: u64,
    operation: StoreOperation,
) -> Result<CheckpointDirectoryDigest, StoreError> {
    let checkpoint_root = checkpoint_root.canonicalize().map_err(|source| {
        rocksdb_checkpoint_artifact_io_error(operation, "canonicalize checkpoint", checkpoint_root, source)
    })?;
    let files = collect_rocksdb_checkpoint_files(&checkpoint_root, operation)?;
    let mut hasher = Sha256::new();
    let mut length_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];

    for (relative, path) in files {
        if relative == Path::new(RELEASE_CHECKPOINT_MANIFEST_FILE) {
            continue;
        }
        hash_rocksdb_checkpoint_relative_path(&mut hasher, &relative);
        let mut input = BufReader::new(File::open(&path).map_err(|source| {
            rocksdb_checkpoint_artifact_io_error(operation, "open checkpoint file", &path, source)
        })?);
        loop {
            let read = input.read(&mut buffer).map_err(|source| {
                rocksdb_checkpoint_artifact_io_error(operation, "read checkpoint file", &path, source)
            })?;
            if read == 0 {
                break;
            }
            length_bytes = length_bytes.checked_add(read as u64).ok_or_else(|| {
                rocksdb_checkpoint_artifact_contract(
                    operation,
                    StoreContractViolation::CheckpointArtifactTooLarge {
                        actual: u64::MAX,
                        maximum: max_checkpoint_bytes,
                    },
                )
            })?;
            if length_bytes > max_checkpoint_bytes {
                return Err(rocksdb_checkpoint_artifact_contract(
                    operation,
                    StoreContractViolation::CheckpointArtifactTooLarge {
                        actual: length_bytes,
                        maximum: max_checkpoint_bytes,
                    },
                ));
            }
            hasher.update(&buffer[..read]);
        }
    }

    if length_bytes == 0 {
        return Err(rocksdb_checkpoint_artifact_contract(
            operation,
            StoreContractViolation::CheckpointArtifactEmpty,
        ));
    }
    let digest = hasher.finalize();
    Ok(CheckpointDirectoryDigest {
        length_bytes,
        sha256: encode_sha256_hex(&digest),
    })
}

fn encode_sha256_hex(digest: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn collect_rocksdb_checkpoint_files(
    root: &Path,
    operation: StoreOperation,
) -> Result<Vec<(PathBuf, PathBuf)>, StoreError> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).map_err(|source| {
            rocksdb_checkpoint_artifact_io_error(operation, "read checkpoint directory", &directory, source)
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| {
                rocksdb_checkpoint_artifact_io_error(operation, "read checkpoint entry", &directory, source)
            })?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|source| {
                rocksdb_checkpoint_artifact_io_error(operation, "inspect checkpoint entry", &path, source)
            })?;
            if metadata.file_type().is_symlink() {
                return Err(rocksdb_checkpoint_artifact_contract(
                    operation,
                    StoreContractViolation::CheckpointArtifactSymbolicLink(path),
                ));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| {
                        rocksdb_checkpoint_artifact_contract(
                            operation,
                            StoreContractViolation::CheckpointArtifactPathEscaped(path.clone()),
                        )
                    })?
                    .to_path_buf();
                files.push((relative, path));
            } else {
                return Err(rocksdb_checkpoint_artifact_contract(
                    operation,
                    StoreContractViolation::CheckpointArtifactUnsupportedFileType(path),
                ));
            }
        }
    }
    files.sort_by_cached_key(|entry| rocksdb_checkpoint_portable_relative_path(&entry.0));
    Ok(files)
}

fn hash_rocksdb_checkpoint_relative_path(hasher: &mut Sha256, relative: &Path) {
    let portable = rocksdb_checkpoint_portable_relative_path(relative);
    hasher.update((portable.len() as u64).to_le_bytes());
    hasher.update(portable.as_bytes());
}

fn rocksdb_checkpoint_portable_relative_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn rocksdb_checkpoint_artifact_contract(operation: StoreOperation, source: StoreContractViolation) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_REQUEST_INVALID, operation)
        .in_component(StoreComponent::RocksDb)
        .with_source(source)
}

fn rocksdb_checkpoint_artifact_io_error(
    owner_operation: StoreOperation,
    operation: &'static str,
    path: &Path,
    source: std::io::Error,
) -> StoreError {
    StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, owner_operation)
        .in_component(StoreComponent::RocksDb)
        .with_source(RocksDbCheckpointArtifactIoError {
            operation,
            path: path.to_path_buf(),
            source,
        })
}

struct RocksDbCheckpointArtifactIoError {
    operation: &'static str,
    path: PathBuf,
    source: std::io::Error,
}

impl fmt::Display for RocksDbCheckpointArtifactIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RocksDB checkpoint artifact I/O failed")
    }
}

impl fmt::Debug for RocksDbCheckpointArtifactIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksDbCheckpointArtifactIoError")
            .field("operation_present", &!self.operation.is_empty())
            .field("path_present", &!self.path.as_os_str().is_empty())
            .field("source_present", &true)
            .finish()
    }
}

impl StdError for RocksDbCheckpointArtifactIoError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

fn rocksdb_checkpoint_error(operation: StoreOperation, error: RocksDbReleaseCheckpointError) -> StoreError {
    match error {
        RocksDbReleaseCheckpointError::Store(source) => source,
        RocksDbReleaseCheckpointError::Artifact(source) => source,
        RocksDbReleaseCheckpointError::Violation(violation) => {
            let (descriptor, component) = match violation {
                RocksDbReleaseCheckpointViolation::InvalidConfiguration => {
                    (&rocketmq_error::STORAGE_REQUEST_INVALID, StoreComponent::Configuration)
                }
                RocksDbReleaseCheckpointViolation::ArtifactChecksumMismatch => {
                    (&rocketmq_error::STORAGE_STATE_CORRUPTED, StoreComponent::RocksDb)
                }
                RocksDbReleaseCheckpointViolation::StorageIdentityMismatch
                | RocksDbReleaseCheckpointViolation::OverlappingRoots
                | RocksDbReleaseCheckpointViolation::WrongBackend => {
                    (&rocketmq_error::STORAGE_REQUEST_INVALID, StoreComponent::RocksDb)
                }
            };
            StoreError::new(descriptor, operation).in_component(component)
        }
        RocksDbReleaseCheckpointError::Native(source) => {
            let descriptor = if operation == StoreOperation::Read {
                &rocketmq_error::STORAGE_READ_FAILED
            } else {
                &rocketmq_error::STORAGE_WRITE_FAILED
            };
            StoreError::new(descriptor, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(source)
        }
        RocksDbReleaseCheckpointError::Serialize(source) => {
            StoreError::new(&rocketmq_error::STORAGE_WRITE_FAILED, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(source)
        }
        RocksDbReleaseCheckpointError::Clock(source) => {
            StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(source)
        }
        RocksDbReleaseCheckpointError::ClockOverflow(source) => {
            StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(source)
        }
        RocksDbReleaseCheckpointError::Runtime(source) => crate::error::runtime_error(operation, source),
        error @ RocksDbReleaseCheckpointError::Validation(_) => {
            StoreError::new(&rocketmq_error::STORAGE_REQUEST_INVALID, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(error)
        }
        error @ RocksDbReleaseCheckpointError::Io { .. } => {
            StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(error)
        }
        error @ (RocksDbReleaseCheckpointError::AuthorizationExpired
        | RocksDbReleaseCheckpointError::UnauthorizedCapability
        | RocksDbReleaseCheckpointError::CheckpointAlreadyExists) => {
            StoreError::new(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation)
                .in_component(StoreComponent::RocksDb)
                .with_source(error)
        }
    }
}

fn validate_authorization(
    authorization: &MaintenanceAuthorizationGrant,
    clock: CheckpointClock,
) -> Result<(), RocksDbReleaseCheckpointError> {
    if authorization.capability() != MaintenanceCapability::ReleaseCheckpoint {
        return Err(RocksDbReleaseCheckpointError::UnauthorizedCapability);
    }
    let _ = authorization_deadline(authorization, clock)?;
    Ok(())
}

fn authorization_deadline(
    authorization: &MaintenanceAuthorizationGrant,
    clock: CheckpointClock,
) -> Result<ShutdownDeadline, RocksDbReleaseCheckpointError> {
    let now = clock()?;
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
        .map_err(RocksDbReleaseCheckpointError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(RocksDbReleaseCheckpointError::ClockOverflow)
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

#[derive(Clone, Copy)]
enum RocksDbReleaseCheckpointViolation {
    InvalidConfiguration,
    StorageIdentityMismatch,
    OverlappingRoots,
    WrongBackend,
    ArtifactChecksumMismatch,
}

/// RocksDB release-checkpoint failure.
enum RocksDbReleaseCheckpointError {
    AuthorizationExpired,
    UnauthorizedCapability,
    CheckpointAlreadyExists,
    Violation(RocksDbReleaseCheckpointViolation),
    Native(::rocksdb::Error),
    Store(StoreError),
    Artifact(StoreError),
    Serialize(serde_json::Error),
    Clock(std::time::SystemTimeError),
    ClockOverflow(std::num::TryFromIntError),
    Runtime(rocketmq_runtime::RuntimeError),
    Validation(StoreContractViolation),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
}

impl From<StoreContractViolation> for RocksDbReleaseCheckpointError {
    fn from(source: StoreContractViolation) -> Self {
        Self::Validation(source)
    }
}

impl From<rocketmq_runtime::RuntimeError> for RocksDbReleaseCheckpointError {
    fn from(source: rocketmq_runtime::RuntimeError) -> Self {
        Self::Runtime(source)
    }
}

impl fmt::Display for RocksDbReleaseCheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RocksDB release-checkpoint operation failed")
    }
}

impl fmt::Debug for RocksDbReleaseCheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::AuthorizationExpired => "AuthorizationExpired",
            Self::UnauthorizedCapability => "UnauthorizedCapability",
            Self::CheckpointAlreadyExists => "CheckpointAlreadyExists",
            Self::Violation(_) => "Violation",
            Self::Native(_) => "Native",
            Self::Store(_) => "Store",
            Self::Artifact(_) => "Artifact",
            Self::Serialize(_) => "Serialize",
            Self::Clock(_) => "Clock",
            Self::ClockOverflow(_) => "ClockOverflow",
            Self::Runtime(_) => "Runtime",
            Self::Validation(_) => "Validation",
            Self::Io { .. } => "Io",
        };
        let io_context_present = matches!(
            self,
            Self::Io {
                operation,
                path,
                ..
            } if !operation.is_empty() && !path.as_os_str().is_empty()
        );
        f.debug_struct("RocksDbReleaseCheckpointError")
            .field("kind", &kind)
            .field("source_present", &self.source().is_some())
            .field("io_context_present", &io_context_present)
            .finish()
    }
}

impl StdError for RocksDbReleaseCheckpointError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Native(source) => Some(source),
            Self::Store(source) | Self::Artifact(source) => Some(source),
            Self::Serialize(source) => Some(source),
            Self::Clock(source) => Some(source),
            Self::ClockOverflow(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Validation(source) => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::AuthorizationExpired
            | Self::UnauthorizedCapability
            | Self::CheckpointAlreadyExists
            | Self::Violation(_) => None,
        }
    }
}

#[cfg(test)]
#[path = "release_checkpoint_tests.rs"]
mod tests;
