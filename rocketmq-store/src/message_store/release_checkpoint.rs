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

//! Production composition for authorized Store release checkpoints.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;

use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_store_api::checkpoint::CheckpointBackend as ReleaseCheckpointBackend;
use rocketmq_store_api::checkpoint::CheckpointManifest as StoreReleaseCheckpointManifest;
use rocketmq_store_api::checkpoint::CheckpointOffsets as ReleaseCheckpointOffsets;
use rocketmq_store_api::checkpoint::CheckpointRequest as StoreReleaseCheckpointRequest;
use rocketmq_store_api::checkpoint::CheckpointRestoreVerification as ReleaseCheckpointRestoreVerification;
use rocketmq_store_api::checkpoint::CheckpointStorageIdentity as ReleaseCheckpointStorageIdentity;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointBarrier;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointError;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointService;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointSnapshot;
use thiserror::Error;
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::OwnedMessageStore;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::message_store::local_file_message_store::LocalReleaseCheckpointWriteLease;

const STORAGE_IDENTITY_FILE: &str = ".rocketmq-storage-identity.json";

/// Release-checkpoint service bound to the Broker-owned Store backend.
///
/// The service retains only a weak Store reference, so it cannot extend the
/// Store lifecycle beyond the Broker composition root.
pub struct StoreReleaseCheckpointService {
    store: Weak<OwnedMessageStore>,
    checkpoint_root: PathBuf,
    storage_io: BlockingExecutor,
    service_context: ChildServiceContext,
    storage_identity: OnceCell<ReleaseCheckpointStorageIdentity>,
}

impl StoreReleaseCheckpointService {
    pub fn new(store: Weak<OwnedMessageStore>, checkpoint_root: PathBuf, service_context: ChildServiceContext) -> Self {
        Self {
            store,
            checkpoint_root,
            storage_io: service_context.storage_io().clone(),
            service_context,
            storage_identity: OnceCell::new(),
        }
    }

    /// Returns the currently selected Store backend.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error after the Broker detaches its Store.
    pub fn backend(&self) -> Result<ReleaseCheckpointBackend, StoreReleaseCheckpointError> {
        match self
            .store
            .upgrade()
            .ok_or(StoreReleaseCheckpointError::StoreUnavailable)?
            .as_ref()
        {
            OwnedMessageStore::LocalFileStore(_) => Ok(ReleaseCheckpointBackend::Local),
            #[cfg(feature = "rocksdb_store")]
            OwnedMessageStore::RocksDBStore(_) => Ok(ReleaseCheckpointBackend::RocksDb),
        }
    }

    /// Loads or durably creates the stable volume/WAL identity.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the Store is detached, the request deadline
    /// expires, or the identity record cannot be validated or persisted.
    pub async fn storage_identity(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
    ) -> Result<ReleaseCheckpointStorageIdentity, StoreReleaseCheckpointError> {
        let deadline = authorization_deadline(authorization)?;
        self.storage_identity
            .get_or_try_init(|| async {
                let source_root = self.source_root()?;
                self.storage_io
                    .spawn_io_until("store.load-release-checkpoint-identity", deadline, move || {
                        load_or_create_storage_identity(&source_root)
                    })
                    .await
                    .map_err(StoreReleaseCheckpointError::Runtime)?
            })
            .await
            .cloned()
    }

    fn source_root(&self) -> Result<PathBuf, StoreReleaseCheckpointError> {
        let store = self
            .store
            .upgrade()
            .ok_or(StoreReleaseCheckpointError::StoreUnavailable)?;
        let local_store = match store.as_ref() {
            OwnedMessageStore::LocalFileStore(local_store) => local_store.as_ref(),
            #[cfg(feature = "rocksdb_store")]
            OwnedMessageStore::RocksDBStore(rocksdb_store) => rocksdb_store.local_file_store(),
        };
        Ok(PathBuf::from(
            local_store.message_store_config_ref().store_path_root_dir.as_str(),
        ))
    }
}

impl ReleaseCheckpointStore for StoreReleaseCheckpointService {
    type Error = StoreReleaseCheckpointError;

    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<StoreReleaseCheckpointManifest, Self::Error> {
        let storage_identity = self.storage_identity(authorization).await?;
        if request.storage_identity != storage_identity {
            return Err(StoreReleaseCheckpointError::StorageIdentityMismatch);
        }
        let store = self
            .store
            .upgrade()
            .ok_or(StoreReleaseCheckpointError::StoreUnavailable)?;
        match store.as_ref() {
            OwnedMessageStore::LocalFileStore(_) => {
                let barrier = Arc::new(OwnedLocalCheckpointBarrier {
                    store: Arc::downgrade(&store),
                    storage_identity,
                    storage_io: self.storage_io.clone(),
                });
                LocalReleaseCheckpointService::new(
                    barrier,
                    self.checkpoint_root.clone(),
                    self.storage_io.clone(),
                    authorization.resource_budget().max_checkpoint_bytes,
                )
                .create_release_checkpoint(authorization, request)
                .await
                .map_err(StoreReleaseCheckpointError::Local)
            }
            #[cfg(feature = "rocksdb_store")]
            OwnedMessageStore::RocksDBStore(rocksdb_message_store) => {
                let service = rocketmq_store_rocksdb::release_checkpoint::RocksDbReleaseCheckpointService::new(
                    rocksdb_message_store.rocksdb_store(),
                    rocketmq_store_rocksdb::runtime::RocksDbRuntimeScope::new(
                        self.service_context.child("store.release-checkpoint.rocksdb"),
                    ),
                    self.checkpoint_root.clone(),
                    storage_identity,
                    authorization.resource_budget().max_checkpoint_bytes,
                );
                service
                    .create_release_checkpoint(authorization, request)
                    .await
                    .map_err(StoreReleaseCheckpointError::RocksDb)
            }
        }
    }

    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreVerification, Self::Error> {
        let storage_identity = self.storage_identity(authorization).await?;
        if manifest.storage_identity != storage_identity {
            return Err(StoreReleaseCheckpointError::StorageIdentityMismatch);
        }
        let store = self
            .store
            .upgrade()
            .ok_or(StoreReleaseCheckpointError::StoreUnavailable)?;
        match store.as_ref() {
            OwnedMessageStore::LocalFileStore(_) => {
                let barrier = Arc::new(OwnedLocalCheckpointBarrier {
                    store: Arc::downgrade(&store),
                    storage_identity,
                    storage_io: self.storage_io.clone(),
                });
                LocalReleaseCheckpointService::new(
                    barrier,
                    self.checkpoint_root.clone(),
                    self.storage_io.clone(),
                    authorization.resource_budget().max_checkpoint_bytes,
                )
                .restore_verify_release_checkpoint(authorization, manifest)
                .await
                .map_err(StoreReleaseCheckpointError::Local)
            }
            #[cfg(feature = "rocksdb_store")]
            OwnedMessageStore::RocksDBStore(rocksdb_message_store) => {
                let service = rocketmq_store_rocksdb::release_checkpoint::RocksDbReleaseCheckpointService::new(
                    rocksdb_message_store.rocksdb_store(),
                    rocketmq_store_rocksdb::runtime::RocksDbRuntimeScope::new(
                        self.service_context.child("store.release-checkpoint.rocksdb"),
                    ),
                    self.checkpoint_root.clone(),
                    storage_identity,
                    authorization.resource_budget().max_checkpoint_bytes,
                );
                service
                    .restore_verify_release_checkpoint(authorization, manifest)
                    .await
                    .map_err(StoreReleaseCheckpointError::RocksDb)
            }
        }
    }
}

struct OwnedLocalCheckpointBarrier {
    store: Weak<OwnedMessageStore>,
    storage_identity: ReleaseCheckpointStorageIdentity,
    storage_io: BlockingExecutor,
}

impl LocalReleaseCheckpointBarrier for OwnedLocalCheckpointBarrier {
    type Error = StoreReleaseCheckpointError;

    async fn begin_release_checkpoint(
        &self,
        _request: &StoreReleaseCheckpointRequest,
        deadline: ShutdownDeadline,
    ) -> Result<LocalReleaseCheckpointSnapshot, Self::Error> {
        let store = self
            .store
            .upgrade()
            .ok_or(StoreReleaseCheckpointError::StoreUnavailable)?;
        let local_store = match store.as_ref() {
            OwnedMessageStore::LocalFileStore(local_store) => local_store.as_ref(),
            #[cfg(feature = "rocksdb_store")]
            OwnedMessageStore::RocksDBStore(_) => return Err(StoreReleaseCheckpointError::WrongBackend),
        };
        let source_root = PathBuf::from(local_store.message_store_config_ref().store_path_root_dir.as_str());
        let (offsets, write_lease) = local_store.begin_release_checkpoint(deadline).await?;
        Ok(LocalReleaseCheckpointSnapshot::new(
            source_root,
            self.storage_identity.clone(),
            offsets,
            LocalSnapshotLease {
                _store: store,
                _write_lease: write_lease,
            },
        ))
    }

    async fn verify_release_checkpoint_restore(
        &self,
        checkpoint_root: &Path,
        manifest: &StoreReleaseCheckpointManifest,
        deadline: ShutdownDeadline,
    ) -> Result<ReleaseCheckpointOffsets, Self::Error> {
        if manifest.storage_identity != self.storage_identity {
            return Err(StoreReleaseCheckpointError::StorageIdentityMismatch);
        }
        let checkpoint_root = checkpoint_root.to_path_buf();
        let expected_identity = self.storage_identity.clone();
        let offsets = manifest.offsets;
        self.storage_io
            .spawn_io_until("store.verify-local-release-checkpoint", deadline, move || {
                verify_local_checkpoint_layout(&checkpoint_root, &expected_identity, offsets)
            })
            .await
            .map_err(StoreReleaseCheckpointError::Runtime)??;
        Ok(offsets)
    }
}

struct LocalSnapshotLease {
    _store: Arc<OwnedMessageStore>,
    _write_lease: LocalReleaseCheckpointWriteLease,
}

fn load_or_create_storage_identity(
    source_root: &Path,
) -> Result<ReleaseCheckpointStorageIdentity, StoreReleaseCheckpointError> {
    let identity_path = source_root.join(STORAGE_IDENTITY_FILE);
    if identity_path.exists() {
        return read_storage_identity(&identity_path);
    }
    let identity = ReleaseCheckpointStorageIdentity {
        volume_id: format!("volume-{}", Uuid::new_v4()),
        wal_generation: 1,
    };
    fs::create_dir_all(source_root).map_err(|source| StoreReleaseCheckpointError::IdentityIo {
        operation: "create Store root",
        path: source_root.to_path_buf(),
        source,
    })?;
    let partial_path = source_root.join(format!(".{STORAGE_IDENTITY_FILE}.partial-{}", Uuid::new_v4()));
    let result = (|| {
        let bytes = serde_json::to_vec_pretty(&identity).map_err(StoreReleaseCheckpointError::IdentityDecode)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial_path)
            .map_err(|source| StoreReleaseCheckpointError::IdentityIo {
                operation: "create storage identity",
                path: partial_path.clone(),
                source,
            })?;
        file.write_all(&bytes).and_then(|_| file.sync_all()).map_err(|source| {
            StoreReleaseCheckpointError::IdentityIo {
                operation: "persist storage identity",
                path: partial_path.clone(),
                source,
            }
        })?;
        fs::rename(&partial_path, &identity_path).map_err(|source| StoreReleaseCheckpointError::IdentityIo {
            operation: "publish storage identity",
            path: identity_path.clone(),
            source,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&partial_path);
    }
    result?;
    Ok(identity)
}

fn read_storage_identity(path: &Path) -> Result<ReleaseCheckpointStorageIdentity, StoreReleaseCheckpointError> {
    let bytes = fs::read(path).map_err(|source| StoreReleaseCheckpointError::IdentityIo {
        operation: "read storage identity",
        path: path.to_path_buf(),
        source,
    })?;
    let identity: ReleaseCheckpointStorageIdentity =
        serde_json::from_slice(&bytes).map_err(StoreReleaseCheckpointError::IdentityDecode)?;
    if identity.volume_id.trim().is_empty() || identity.wal_generation == 0 {
        return Err(StoreReleaseCheckpointError::InvalidStorageIdentity);
    }
    Ok(identity)
}

fn verify_local_checkpoint_layout(
    checkpoint_root: &Path,
    expected_identity: &ReleaseCheckpointStorageIdentity,
    offsets: ReleaseCheckpointOffsets,
) -> Result<(), StoreReleaseCheckpointError> {
    let identity = read_storage_identity(&checkpoint_root.join(STORAGE_IDENTITY_FILE))?;
    if &identity != expected_identity {
        return Err(StoreReleaseCheckpointError::StorageIdentityMismatch);
    }
    for required in ["commitlog", "consumequeue", "index"] {
        let path = checkpoint_root.join(required);
        if !path.is_dir() {
            return Err(StoreReleaseCheckpointError::RestoreLayout(format!(
                "checkpoint is missing {required}"
            )));
        }
    }
    let store_checkpoint = StoreCheckpoint::read_snapshot(checkpoint_root.join("checkpoint")).map_err(|source| {
        StoreReleaseCheckpointError::IdentityIo {
            operation: "read isolated Store checkpoint",
            path: checkpoint_root.join("checkpoint"),
            source,
        }
    })?;
    if store_checkpoint.index_safe_phy_offset < offsets.index_offset.max(0) as u64 {
        return Err(StoreReleaseCheckpointError::RestoreLayout(
            "persisted index safe offset is behind the manifest".to_string(),
        ));
    }
    if store_checkpoint.master_flushed_offset > offsets.appended_offset.max(0) as u64
        || store_checkpoint.confirm_phy_offset > offsets.appended_offset.max(0) as u64
    {
        return Err(StoreReleaseCheckpointError::RestoreLayout(
            "persisted Store checkpoint offsets exceed the manifest".to_string(),
        ));
    }
    Ok(())
}

fn authorization_deadline(
    authorization: &MaintenanceAuthorizationGrant,
) -> Result<ShutdownDeadline, StoreReleaseCheckpointError> {
    let now = rocketmq_runtime::common::time_utils::current_millis();
    let remaining = authorization
        .deadline_unix_millis()
        .checked_sub(now)
        .filter(|remaining| *remaining > 0)
        .ok_or(StoreReleaseCheckpointError::AuthorizationExpired)?;
    Ok(ShutdownDeadline::after(std::time::Duration::from_millis(remaining)))
}

/// Failure from the Broker-composed Store checkpoint service.
#[derive(Debug, Error)]
pub enum StoreReleaseCheckpointError {
    #[error("Store is unavailable")]
    StoreUnavailable,
    #[error("release-checkpoint authorization expired")]
    AuthorizationExpired,
    #[error("checkpoint storage identity does not match the mounted Store")]
    StorageIdentityMismatch,
    #[error("checkpoint request does not match the selected Store backend")]
    WrongBackend,
    #[error("invalid storage identity encoding")]
    IdentityDecode(#[source] serde_json::Error),
    #[error("storage identity requires a volume ID and non-zero WAL generation")]
    InvalidStorageIdentity,
    #[error("local restore verification failed: {0}")]
    RestoreLayout(String),
    #[error("checkpoint runtime failed")]
    Runtime(#[source] RuntimeError),
    #[error("Store checkpoint barrier failed: {0}")]
    Store(#[from] crate::store_error::StoreError),
    #[error("Local Store checkpoint failed: {0}")]
    Local(#[source] LocalReleaseCheckpointError),
    #[cfg(feature = "rocksdb_store")]
    #[error("RocksDB Store checkpoint failed: {0}")]
    RocksDb(#[source] rocketmq_store_rocksdb::release_checkpoint::RocksDbReleaseCheckpointError),
    #[error("{operation} failed for {path}: {source}")]
    IdentityIo {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn storage_identity_is_persisted_and_reloaded_without_generation_drift() {
        let temp = tempdir().expect("temporary Store root");
        let first = load_or_create_storage_identity(temp.path()).expect("create storage identity");
        let second = load_or_create_storage_identity(temp.path()).expect("reload storage identity");

        assert_eq!(first, second);
        assert_eq!(first.wal_generation, 1);
        assert!(temp.path().join(STORAGE_IDENTITY_FILE).is_file());
    }

    #[test]
    fn local_restore_verification_requires_identity_layout_and_index_progress() {
        let temp = tempdir().expect("temporary checkpoint");
        for directory in ["commitlog", "consumequeue", "index"] {
            fs::create_dir(temp.path().join(directory)).expect("checkpoint directory");
        }
        let identity = load_or_create_storage_identity(temp.path()).expect("storage identity");
        let checkpoint = StoreCheckpoint::new(temp.path().join("checkpoint")).expect("Store checkpoint");
        checkpoint.set_index_safe_phy_offset(64);
        checkpoint.flush().expect("persist Store checkpoint");

        let offsets = ReleaseCheckpointOffsets {
            appended_offset: 96,
            durable_offset: 96,
            consume_queue_offset: 80,
            index_offset: 64,
        };
        verify_local_checkpoint_layout(temp.path(), &identity, offsets).expect("valid isolated checkpoint");

        let behind = ReleaseCheckpointOffsets {
            index_offset: 65,
            ..offsets
        };
        assert!(verify_local_checkpoint_layout(temp.path(), &identity, behind).is_err());
    }
}
