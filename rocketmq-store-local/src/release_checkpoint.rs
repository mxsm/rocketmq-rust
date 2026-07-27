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

//! Atomic, checksummed release checkpoints for the local Store backend.

use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_auth::MaintenanceAuthorizationGrant;
use rocketmq_auth::MaintenanceCapability;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointArtifact;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointBackend;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointOffsets;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointRestoreVerification;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointStorageIdentity;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointValidationError;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointRequest;
use rocketmq_protocol::protocol::body::release_checkpoint::RELEASE_CHECKPOINT_SCHEMA_VERSION;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_store_api::ReleaseCheckpointStore;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

/// Manifest stored beside every local checkpoint payload.
pub const RELEASE_CHECKPOINT_MANIFEST_FILE: &str = ".rocketmq-release-checkpoint.json";

/// Store-specific barrier and restore verifier injected by the composition root.
///
/// Implementations must stop Store writes, flush CommitLog, ConsumeQueue, and
/// Index, and retain the write lease inside [`LocalReleaseCheckpointSnapshot`].
/// The release-checkpoint service keeps that snapshot alive until the copied
/// artifact is durably published.
#[allow(async_fn_in_trait)]
pub trait LocalReleaseCheckpointBarrier: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Acquires the Store write barrier and returns a flushed, stable snapshot.
    async fn begin_release_checkpoint(
        &self,
        request: &StoreReleaseCheckpointRequest,
        deadline: ShutdownDeadline,
    ) -> Result<LocalReleaseCheckpointSnapshot, Self::Error>;

    /// Restores and validates the checkpoint in an isolated verification target.
    async fn verify_release_checkpoint_restore(
        &self,
        checkpoint_root: &Path,
        manifest: &StoreReleaseCheckpointManifest,
        deadline: ShutdownDeadline,
    ) -> Result<ReleaseCheckpointOffsets, Self::Error>;
}

/// Flushed Local Store view retained under an opaque write-barrier lease.
///
/// Dropping this value releases the backend-specific lease. Callers cannot
/// accidentally release writes between the flush barrier and artifact
/// publication because the lease is owned by the snapshot itself.
pub struct LocalReleaseCheckpointSnapshot {
    source_root: PathBuf,
    storage_identity: ReleaseCheckpointStorageIdentity,
    offsets: ReleaseCheckpointOffsets,
    _write_barrier: Box<dyn Send>,
}

impl LocalReleaseCheckpointSnapshot {
    /// Creates a stable snapshot from a backend-owned write barrier.
    pub fn new(
        source_root: PathBuf,
        storage_identity: ReleaseCheckpointStorageIdentity,
        offsets: ReleaseCheckpointOffsets,
        write_barrier: impl Send + 'static,
    ) -> Self {
        Self {
            source_root,
            storage_identity,
            offsets,
            _write_barrier: Box::new(write_barrier),
        }
    }

    pub fn source_root(&self) -> &Path {
        &self.source_root
    }

    pub const fn storage_identity(&self) -> &ReleaseCheckpointStorageIdentity {
        &self.storage_identity
    }

    pub const fn offsets(&self) -> ReleaseCheckpointOffsets {
        self.offsets
    }
}

impl fmt::Debug for LocalReleaseCheckpointSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalReleaseCheckpointSnapshot")
            .field("source_root", &self.source_root)
            .field("storage_identity", &self.storage_identity)
            .field("offsets", &self.offsets)
            .finish_non_exhaustive()
    }
}

/// Local Store release-checkpoint service.
pub struct LocalReleaseCheckpointService<B> {
    barrier: Arc<B>,
    checkpoint_root: PathBuf,
    storage_io: BlockingExecutor,
    max_checkpoint_bytes: u64,
}

impl<B> LocalReleaseCheckpointService<B> {
    /// Creates a checkpoint service from an injected Store barrier and bounded
    /// storage executor.
    pub fn new(
        barrier: Arc<B>,
        checkpoint_root: PathBuf,
        storage_io: BlockingExecutor,
        max_checkpoint_bytes: u64,
    ) -> Self {
        Self {
            barrier,
            checkpoint_root,
            storage_io,
            max_checkpoint_bytes,
        }
    }
}

impl<B> ReleaseCheckpointStore for LocalReleaseCheckpointService<B>
where
    B: LocalReleaseCheckpointBarrier,
{
    type Error = LocalReleaseCheckpointError;

    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<StoreReleaseCheckpointManifest, Self::Error> {
        validate_authorization(authorization)?;
        request.validate()?;
        let deadline = authorization_deadline(authorization)?;
        let snapshot = self
            .barrier
            .begin_release_checkpoint(&request, deadline)
            .await
            .map_err(|error| LocalReleaseCheckpointError::Barrier(error.to_string()))?;
        if snapshot.storage_identity() != &request.storage_identity {
            return Err(LocalReleaseCheckpointError::StorageIdentityMismatch);
        }
        if snapshot.offsets() != request.offsets {
            return Err(LocalReleaseCheckpointError::BarrierOffsetsChanged {
                expected: request.offsets,
                actual: snapshot.offsets(),
            });
        }

        let source_root = snapshot.source_root().to_path_buf();
        let checkpoint_root = self.checkpoint_root.clone();
        let max_checkpoint_bytes = self
            .max_checkpoint_bytes
            .min(authorization.resource_budget().max_checkpoint_bytes);
        let fencing_token = authorization.fencing_token();
        let created_at_unix_millis = unix_millis()?;
        let request_for_copy = request.clone();
        let manifest = self
            .storage_io
            .spawn_io_until("local-store.create-release-checkpoint", deadline, move || {
                create_atomic_checkpoint(
                    &source_root,
                    &checkpoint_root,
                    request_for_copy,
                    created_at_unix_millis,
                    fencing_token,
                    max_checkpoint_bytes,
                )
            })
            .await
            .map_err(|error| LocalReleaseCheckpointError::Runtime(error.to_string()))??;
        drop(snapshot);
        manifest.validate()?;
        Ok(manifest)
    }

    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreVerification, Self::Error> {
        validate_authorization(authorization)?;
        manifest.validate()?;
        if manifest.backend != ReleaseCheckpointBackend::Local {
            return Err(LocalReleaseCheckpointError::WrongBackend);
        }
        let deadline = authorization_deadline(authorization)?;
        let checkpoint_path = file_uri_to_path(&manifest.artifact.uri)?;
        let expected_sha256 = manifest.artifact.sha256.clone();
        let expected_length = manifest.artifact.length_bytes;
        let max_checkpoint_bytes = self
            .max_checkpoint_bytes
            .min(authorization.resource_budget().max_checkpoint_bytes);
        let checkpoint_path_for_hash = checkpoint_path.clone();
        let digest = self
            .storage_io
            .spawn_io_until("local-store.hash-release-checkpoint", deadline, move || {
                hash_checkpoint_directory(&checkpoint_path_for_hash, max_checkpoint_bytes)
            })
            .await
            .map_err(|error| LocalReleaseCheckpointError::Runtime(error.to_string()))??;
        if digest.sha256 != expected_sha256 || digest.length_bytes != expected_length {
            return Err(LocalReleaseCheckpointError::ArtifactChecksumMismatch);
        }

        let restored_offsets = self
            .barrier
            .verify_release_checkpoint_restore(&checkpoint_path, manifest, deadline)
            .await
            .map_err(|error| LocalReleaseCheckpointError::RestoreVerification(error.to_string()))?;
        if restored_offsets != manifest.offsets {
            return Err(LocalReleaseCheckpointError::RestoreOffsetsChanged {
                expected: manifest.offsets,
                actual: restored_offsets,
            });
        }

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

/// Deterministic SHA-256 and byte length for a checkpoint directory.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointDirectoryDigest {
    pub length_bytes: u64,
    pub sha256: String,
}

/// Hashes all regular files in stable relative-path order.
///
/// Symlinks and the embedded manifest are rejected/skipped respectively. The
/// byte budget is enforced while reading so an artifact cannot bypass the
/// configured maintenance resource limit.
#[doc(hidden)]
pub fn hash_checkpoint_directory(
    checkpoint_root: &Path,
    max_checkpoint_bytes: u64,
) -> Result<CheckpointDirectoryDigest, LocalReleaseCheckpointError> {
    let checkpoint_root = checkpoint_root
        .canonicalize()
        .map_err(|source| io_error("canonicalize checkpoint", checkpoint_root, source))?;
    let files = collect_regular_files(&checkpoint_root)?;
    let mut hasher = Sha256::new();
    let mut length_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];

    for (relative, path) in files {
        if relative == Path::new(RELEASE_CHECKPOINT_MANIFEST_FILE) {
            continue;
        }
        hash_relative_path(&mut hasher, &relative);
        let mut input =
            BufReader::new(File::open(&path).map_err(|source| io_error("open checkpoint file", &path, source))?);
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|source| io_error("read checkpoint file", &path, source))?;
            if read == 0 {
                break;
            }
            length_bytes =
                length_bytes
                    .checked_add(read as u64)
                    .ok_or(LocalReleaseCheckpointError::CheckpointTooLarge {
                        actual: u64::MAX,
                        maximum: max_checkpoint_bytes,
                    })?;
            if length_bytes > max_checkpoint_bytes {
                return Err(LocalReleaseCheckpointError::CheckpointTooLarge {
                    actual: length_bytes,
                    maximum: max_checkpoint_bytes,
                });
            }
            hasher.update(&buffer[..read]);
        }
    }

    if length_bytes == 0 {
        return Err(LocalReleaseCheckpointError::EmptyCheckpoint);
    }
    Ok(CheckpointDirectoryDigest {
        length_bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn create_atomic_checkpoint(
    source_root: &Path,
    checkpoint_root: &Path,
    request: StoreReleaseCheckpointRequest,
    created_at_unix_millis: u64,
    fencing_token: u64,
    max_checkpoint_bytes: u64,
) -> Result<StoreReleaseCheckpointManifest, LocalReleaseCheckpointError> {
    if max_checkpoint_bytes == 0 {
        return Err(LocalReleaseCheckpointError::InvalidConfiguration(
            "max_checkpoint_bytes must be greater than zero".to_string(),
        ));
    }
    let source_root = source_root
        .canonicalize()
        .map_err(|source| io_error("canonicalize Store root", source_root, source))?;
    fs::create_dir_all(checkpoint_root)
        .map_err(|source| io_error("create checkpoint root", checkpoint_root, source))?;
    let checkpoint_root = checkpoint_root
        .canonicalize()
        .map_err(|source| io_error("canonicalize checkpoint root", checkpoint_root, source))?;
    if checkpoint_root.starts_with(&source_root) || source_root.starts_with(&checkpoint_root) {
        return Err(LocalReleaseCheckpointError::OverlappingRoots);
    }

    let final_path = checkpoint_root.join(&request.checkpoint_id);
    let partial_path = checkpoint_root.join(format!(".{}.partial-{fencing_token}", request.checkpoint_id));
    if final_path.exists() || partial_path.exists() {
        return Err(LocalReleaseCheckpointError::CheckpointAlreadyExists(
            request.checkpoint_id,
        ));
    }

    fs::create_dir(&partial_path).map_err(|source| io_error("create partial checkpoint", &partial_path, source))?;
    let copy_result = copy_checkpoint_payload(&source_root, &partial_path, max_checkpoint_bytes);
    let digest = match copy_result {
        Ok(digest) => digest,
        Err(error) => {
            let _ = fs::remove_dir_all(&partial_path);
            return Err(error);
        }
    };
    let manifest = StoreReleaseCheckpointManifest {
        artifact: ReleaseCheckpointArtifact {
            schema_version: RELEASE_CHECKPOINT_SCHEMA_VERSION,
            checkpoint_id: request.checkpoint_id,
            checkpoint_set_id: request.checkpoint_set_id,
            generation: request.generation,
            barrier_id: request.barrier_id,
            created_at_unix_millis,
            length_bytes: digest.length_bytes,
            sha256: digest.sha256,
            uri: path_to_file_uri(&final_path),
        },
        member_id: request.member_id,
        backend: ReleaseCheckpointBackend::Local,
        offsets: request.offsets,
        storage_identity: request.storage_identity,
        wal_retained: true,
        persistent_volume_retained: true,
    };
    manifest.validate()?;
    let manifest_path = partial_path.join(RELEASE_CHECKPOINT_MANIFEST_FILE);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|source| LocalReleaseCheckpointError::Serialize(source.to_string()))?;
    write_synced_file(&manifest_path, &manifest_bytes)?;
    sync_directory(&partial_path)?;
    fs::rename(&partial_path, &final_path).map_err(|source| io_error("publish checkpoint", &final_path, source))?;
    sync_directory(&checkpoint_root)?;
    Ok(manifest)
}

fn copy_checkpoint_payload(
    source_root: &Path,
    destination_root: &Path,
    max_checkpoint_bytes: u64,
) -> Result<CheckpointDirectoryDigest, LocalReleaseCheckpointError> {
    let files = collect_regular_files(source_root)?;
    let mut hasher = Sha256::new();
    let mut length_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];

    for (relative, source) in files {
        if relative == Path::new(RELEASE_CHECKPOINT_MANIFEST_FILE) {
            return Err(LocalReleaseCheckpointError::ReservedManifestInSource);
        }
        hash_relative_path(&mut hasher, &relative);
        let destination = destination_root.join(&relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|source| io_error("create checkpoint directory", parent, source))?;
        }
        let mut input =
            BufReader::new(File::open(&source).map_err(|error| io_error("open Store file", &source, error))?);
        let output =
            File::create(&destination).map_err(|error| io_error("create checkpoint file", &destination, error))?;
        let mut output = BufWriter::new(output);
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|error| io_error("read Store file", &source, error))?;
            if read == 0 {
                break;
            }
            length_bytes =
                length_bytes
                    .checked_add(read as u64)
                    .ok_or(LocalReleaseCheckpointError::CheckpointTooLarge {
                        actual: u64::MAX,
                        maximum: max_checkpoint_bytes,
                    })?;
            if length_bytes > max_checkpoint_bytes {
                return Err(LocalReleaseCheckpointError::CheckpointTooLarge {
                    actual: length_bytes,
                    maximum: max_checkpoint_bytes,
                });
            }
            hasher.update(&buffer[..read]);
            output
                .write_all(&buffer[..read])
                .map_err(|error| io_error("write checkpoint file", &destination, error))?;
        }
        output
            .flush()
            .map_err(|error| io_error("flush checkpoint file", &destination, error))?;
        output
            .get_ref()
            .sync_all()
            .map_err(|error| io_error("sync checkpoint file", &destination, error))?;
    }

    if length_bytes == 0 {
        return Err(LocalReleaseCheckpointError::EmptyCheckpoint);
    }
    Ok(CheckpointDirectoryDigest {
        length_bytes,
        sha256: hex::encode(hasher.finalize()),
    })
}

fn collect_regular_files(root: &Path) -> Result<Vec<(PathBuf, PathBuf)>, LocalReleaseCheckpointError> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        let entries =
            fs::read_dir(&directory).map_err(|source| io_error("read checkpoint directory", &directory, source))?;
        for entry in entries {
            let entry = entry.map_err(|source| io_error("read checkpoint entry", &directory, source))?;
            let path = entry.path();
            let metadata =
                fs::symlink_metadata(&path).map_err(|source| io_error("inspect checkpoint entry", &path, source))?;
            if metadata.file_type().is_symlink() {
                return Err(LocalReleaseCheckpointError::SymbolicLink(path));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| LocalReleaseCheckpointError::PathEscaped(path.clone()))?
                    .to_path_buf();
                files.push((relative, path));
            } else {
                return Err(LocalReleaseCheckpointError::UnsupportedFileType(path));
            }
        }
    }
    files.sort_by_cached_key(|entry| portable_relative_path(&entry.0));
    Ok(files)
}

fn hash_relative_path(hasher: &mut Sha256, relative: &Path) {
    let portable = portable_relative_path(relative);
    hasher.update((portable.len() as u64).to_le_bytes());
    hasher.update(portable.as_bytes());
}

fn portable_relative_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> Result<(), LocalReleaseCheckpointError> {
    let mut file = File::create(path).map_err(|source| io_error("create checkpoint manifest", path, source))?;
    file.write_all(bytes)
        .map_err(|source| io_error("write checkpoint manifest", path, source))?;
    file.sync_all()
        .map_err(|source| io_error("sync checkpoint manifest", path, source))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), LocalReleaseCheckpointError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync checkpoint directory", path, source))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), LocalReleaseCheckpointError> {
    Ok(())
}

/// Converts a local path to the file URI emitted in checkpoint manifests.
#[doc(hidden)]
pub fn path_to_file_uri(path: &Path) -> String {
    let raw = path.to_string_lossy();
    #[cfg(windows)]
    let raw = raw.strip_prefix(r"\\?\").map_or_else(|| raw.to_string(), str::to_owned);
    #[cfg(not(windows))]
    let raw = raw.into_owned();
    let portable = raw.replace('\\', "/");
    if portable.starts_with('/') {
        format!("file://{portable}")
    } else {
        format!("file:///{portable}")
    }
}

/// Resolves a checkpoint URI created by [`path_to_file_uri`].
#[doc(hidden)]
pub fn file_uri_to_path(uri: &str) -> Result<PathBuf, LocalReleaseCheckpointError> {
    let raw = uri
        .strip_prefix("file:///")
        .ok_or_else(|| LocalReleaseCheckpointError::UnsupportedUri(uri.to_string()))?;
    #[cfg(windows)]
    let path = PathBuf::from(raw.replace('/', "\\"));
    #[cfg(not(windows))]
    let path = PathBuf::from(format!("/{raw}"));
    Ok(path)
}

fn validate_authorization(authorization: &MaintenanceAuthorizationGrant) -> Result<(), LocalReleaseCheckpointError> {
    if authorization.capability() != MaintenanceCapability::ReleaseCheckpoint {
        return Err(LocalReleaseCheckpointError::UnauthorizedCapability);
    }
    let _ = authorization_deadline(authorization)?;
    Ok(())
}

fn authorization_deadline(
    authorization: &MaintenanceAuthorizationGrant,
) -> Result<ShutdownDeadline, LocalReleaseCheckpointError> {
    let now = unix_millis()?;
    let remaining = authorization
        .deadline_unix_millis()
        .checked_sub(now)
        .filter(|remaining| *remaining > 0)
        .ok_or(LocalReleaseCheckpointError::AuthorizationExpired)?;
    Ok(ShutdownDeadline::after(Duration::from_millis(remaining)))
}

fn unix_millis() -> Result<u64, LocalReleaseCheckpointError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LocalReleaseCheckpointError::Clock(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| LocalReleaseCheckpointError::Clock("Unix time exceeds u64".to_string()))
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> LocalReleaseCheckpointError {
    LocalReleaseCheckpointError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Local release-checkpoint failure.
#[derive(Debug, Error)]
pub enum LocalReleaseCheckpointError {
    #[error("release checkpoint authorization has expired")]
    AuthorizationExpired,
    #[error("authorization does not grant release_checkpoint")]
    UnauthorizedCapability,
    #[error("invalid local checkpoint configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Store checkpoint storage identity changed before flush")]
    StorageIdentityMismatch,
    #[error("Store flush barrier failed: {0}")]
    Barrier(String),
    #[error("Store flush offsets changed: expected {expected:?}, actual {actual:?}")]
    BarrierOffsetsChanged {
        expected: ReleaseCheckpointOffsets,
        actual: ReleaseCheckpointOffsets,
    },
    #[error("checkpoint restore verification failed: {0}")]
    RestoreVerification(String),
    #[error("restored checkpoint offsets changed: expected {expected:?}, actual {actual:?}")]
    RestoreOffsetsChanged {
        expected: ReleaseCheckpointOffsets,
        actual: ReleaseCheckpointOffsets,
    },
    #[error("checkpoint artifact checksum or length does not match its manifest")]
    ArtifactChecksumMismatch,
    #[error("checkpoint backend is not local")]
    WrongBackend,
    #[error("checkpoint root overlaps the live Store root")]
    OverlappingRoots,
    #[error("checkpoint '{0}' already exists or has an unfinished publication")]
    CheckpointAlreadyExists(String),
    #[error("checkpoint source contains reserved manifest file")]
    ReservedManifestInSource,
    #[error("checkpoint is empty")]
    EmptyCheckpoint,
    #[error("checkpoint size {actual} exceeds resource limit {maximum}")]
    CheckpointTooLarge { actual: u64, maximum: u64 },
    #[error("checkpoint source contains a symbolic link: {0}")]
    SymbolicLink(PathBuf),
    #[error("checkpoint source contains an unsupported file type: {0}")]
    UnsupportedFileType(PathBuf),
    #[error("checkpoint path escaped its root: {0}")]
    PathEscaped(PathBuf),
    #[error("unsupported checkpoint URI: {0}")]
    UnsupportedUri(String),
    #[error("failed to serialize checkpoint manifest: {0}")]
    Serialize(String),
    #[error("system clock error: {0}")]
    Clock(String),
    #[error("blocking checkpoint operation failed: {0}")]
    Runtime(String),
    #[error("checkpoint validation failed: {0}")]
    Validation(#[from] ReleaseCheckpointValidationError),
    #[error("{operation} failed for {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}
