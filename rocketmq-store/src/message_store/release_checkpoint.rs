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
use rocketmq_store_api::checkpoint::CheckpointStorageIdentity as ReleaseCheckpointStorageIdentity;
use rocketmq_store_api::ReleaseCheckpointCreateOutcome;
use rocketmq_store_api::ReleaseCheckpointCreateRejection;
use rocketmq_store_api::ReleaseCheckpointRestoreOutcome;
use rocketmq_store_api::ReleaseCheckpointRestoreRejection;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointBarrier;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointService;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointSnapshot;
use tokio::sync::OnceCell;
use uuid::Uuid;

use super::StorePorts;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::message_store::local_file_message_store::LocalReleaseCheckpointWriteLease;

const STORAGE_IDENTITY_FILE: &str = ".rocketmq-storage-identity.json";

/// Release-checkpoint service bound to the Broker-owned Store backend.
///
/// The service retains only a weak Store reference, so it cannot extend the
/// Store lifecycle beyond the Broker composition root.
pub struct StoreReleaseCheckpointService {
    store: Weak<StorePorts>,
    checkpoint_root: PathBuf,
    storage_io: BlockingExecutor,
    service_context: ChildServiceContext,
    storage_identity: OnceCell<ReleaseCheckpointStorageIdentity>,
}

impl StoreReleaseCheckpointService {
    pub fn new(store: Weak<StorePorts>, checkpoint_root: PathBuf, service_context: ChildServiceContext) -> Self {
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
    pub fn backend(&self) -> Result<ReleaseCheckpointBackend, StoreError> {
        match self
            .store
            .upgrade()
            .ok_or_else(|| store_unavailable(StoreOperation::Admin))?
            .as_ref()
        {
            StorePorts::LocalFileStore(_) => Ok(ReleaseCheckpointBackend::Local),
            #[cfg(feature = "rocksdb_store")]
            StorePorts::RocksDBStore(_) => Ok(ReleaseCheckpointBackend::RocksDb),
        }
    }

    /// Loads or durably creates the stable volume/WAL identity.
    ///
    /// # Errors
    ///
    /// Returns `Ok(None)` when the authorization expires before the operation begins.
    /// Genuine Store lifecycle, runtime, identity-decode, and I/O failures are returned with
    /// [`StoreOperation::Admin`].
    pub async fn storage_identity(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
    ) -> Result<Option<ReleaseCheckpointStorageIdentity>, StoreError> {
        self.storage_identity_for(authorization, StoreOperation::Admin).await
    }

    async fn storage_identity_for(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        operation: StoreOperation,
    ) -> Result<Option<ReleaseCheckpointStorageIdentity>, StoreError> {
        let Some(deadline) = authorization_deadline(authorization) else {
            return Ok(None);
        };
        self.storage_identity
            .get_or_try_init(|| async {
                let source_root = self.source_root(operation)?;
                self.storage_io
                    .spawn_io_until("store.load-release-checkpoint-identity", deadline, move || {
                        load_or_create_storage_identity(&source_root, operation)
                    })
                    .await
                    .map_err(|source| runtime_store_error(operation, source))?
            })
            .await
            .cloned()
            .map(Some)
    }

    fn source_root(&self, operation: StoreOperation) -> Result<PathBuf, StoreError> {
        let store = self.store.upgrade().ok_or_else(|| store_unavailable(operation))?;
        let local_store = match store.as_ref() {
            StorePorts::LocalFileStore(local_store) => local_store.as_ref(),
            #[cfg(feature = "rocksdb_store")]
            StorePorts::RocksDBStore(rocksdb_store) => rocksdb_store.local_file_store(),
        };
        Ok(PathBuf::from(
            local_store.message_store_config_ref().store_path_root_dir.as_str(),
        ))
    }
}

impl ReleaseCheckpointStore for StoreReleaseCheckpointService {
    async fn create_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        request: StoreReleaseCheckpointRequest,
    ) -> Result<ReleaseCheckpointCreateOutcome, StoreError> {
        let storage_identity = match self.storage_identity_for(authorization, StoreOperation::Flush).await? {
            Some(identity) => identity,
            None => {
                return Ok(ReleaseCheckpointCreateOutcome::Rejected(
                    ReleaseCheckpointCreateRejection::AuthorizationExpired,
                ));
            }
        };
        if request.storage_identity != storage_identity {
            return Err(request_invalid(StoreOperation::Flush));
        }
        let store = self
            .store
            .upgrade()
            .ok_or_else(|| store_unavailable(StoreOperation::Flush))?;
        match store.as_ref() {
            StorePorts::LocalFileStore(_) => {
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
            }
            #[cfg(feature = "rocksdb_store")]
            StorePorts::RocksDBStore(rocksdb_message_store) => {
                let service = rocketmq_store_rocksdb::release_checkpoint::RocksDbReleaseCheckpointService::new(
                    rocksdb_message_store.rocksdb_store(),
                    rocketmq_store_rocksdb::runtime::RocksDbRuntimeScope::new(
                        self.service_context.component("store.release-checkpoint.rocksdb"),
                    ),
                    self.checkpoint_root.clone(),
                    storage_identity,
                    authorization.resource_budget().max_checkpoint_bytes,
                );
                service.create_release_checkpoint(authorization, request).await
            }
        }
    }

    async fn restore_verify_release_checkpoint(
        &self,
        authorization: &MaintenanceAuthorizationGrant,
        manifest: &StoreReleaseCheckpointManifest,
    ) -> Result<ReleaseCheckpointRestoreOutcome, StoreError> {
        let storage_identity = match self.storage_identity_for(authorization, StoreOperation::Read).await? {
            Some(identity) => identity,
            None => {
                return Ok(ReleaseCheckpointRestoreOutcome::Rejected(
                    ReleaseCheckpointRestoreRejection::AuthorizationExpired,
                ));
            }
        };
        if manifest.storage_identity != storage_identity {
            return Err(request_invalid(StoreOperation::Read));
        }
        let store = self
            .store
            .upgrade()
            .ok_or_else(|| store_unavailable(StoreOperation::Read))?;
        match store.as_ref() {
            StorePorts::LocalFileStore(_) => {
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
            }
            #[cfg(feature = "rocksdb_store")]
            StorePorts::RocksDBStore(rocksdb_message_store) => {
                let service = rocketmq_store_rocksdb::release_checkpoint::RocksDbReleaseCheckpointService::new(
                    rocksdb_message_store.rocksdb_store(),
                    rocketmq_store_rocksdb::runtime::RocksDbRuntimeScope::new(
                        self.service_context.component("store.release-checkpoint.rocksdb"),
                    ),
                    self.checkpoint_root.clone(),
                    storage_identity,
                    authorization.resource_budget().max_checkpoint_bytes,
                );
                service.restore_verify_release_checkpoint(authorization, manifest).await
            }
        }
    }
}

struct OwnedLocalCheckpointBarrier {
    store: Weak<StorePorts>,
    storage_identity: ReleaseCheckpointStorageIdentity,
    storage_io: BlockingExecutor,
}

impl LocalReleaseCheckpointBarrier for OwnedLocalCheckpointBarrier {
    type Error = StoreError;

    async fn begin_release_checkpoint(
        &self,
        _request: &StoreReleaseCheckpointRequest,
        deadline: ShutdownDeadline,
    ) -> Result<LocalReleaseCheckpointSnapshot, Self::Error> {
        let store = self
            .store
            .upgrade()
            .ok_or_else(|| store_unavailable(StoreOperation::Flush))?;
        let local_store = match store.as_ref() {
            StorePorts::LocalFileStore(local_store) => local_store.as_ref(),
            #[cfg(feature = "rocksdb_store")]
            StorePorts::RocksDBStore(_) => {
                return Err(request_invalid(StoreOperation::Flush));
            }
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
            return Err(request_invalid(StoreOperation::Read));
        }
        let checkpoint_root = checkpoint_root.to_path_buf();
        let expected_identity = self.storage_identity.clone();
        let offsets = manifest.offsets;
        let verification = self
            .storage_io
            .spawn_io_until("store.verify-local-release-checkpoint", deadline, move || {
                verify_local_checkpoint_layout(&checkpoint_root, &expected_identity, offsets)
            })
            .await
            .map_err(|source| runtime_store_error(StoreOperation::Read, source))?;
        verification?;
        Ok(offsets)
    }
}

struct LocalSnapshotLease {
    _store: Arc<StorePorts>,
    _write_lease: LocalReleaseCheckpointWriteLease,
}

fn load_or_create_storage_identity(
    source_root: &Path,
    operation: StoreOperation,
) -> Result<ReleaseCheckpointStorageIdentity, StoreError> {
    let identity_path = source_root.join(STORAGE_IDENTITY_FILE);
    if identity_path.exists() {
        return read_storage_identity(&identity_path, operation);
    }
    let identity = ReleaseCheckpointStorageIdentity {
        volume_id: format!("volume-{}", Uuid::new_v4()),
        wal_generation: 1,
    };
    fs::create_dir_all(source_root).map_err(|source| io_store_error(operation, source))?;
    let partial_path = source_root.join(format!(".{STORAGE_IDENTITY_FILE}.partial-{}", Uuid::new_v4()));
    let result = (|| {
        let bytes = serde_json::to_vec_pretty(&identity).map_err(|source| decode_store_error(operation, source))?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial_path)
            .map_err(|source| io_store_error(operation, source))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|source| io_store_error(operation, source))?;
        fs::rename(&partial_path, &identity_path).map_err(|source| io_store_error(operation, source))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&partial_path);
    }
    result?;
    Ok(identity)
}

fn read_storage_identity(
    path: &Path,
    operation: StoreOperation,
) -> Result<ReleaseCheckpointStorageIdentity, StoreError> {
    let bytes = fs::read(path).map_err(|source| io_store_error(operation, source))?;
    let identity: ReleaseCheckpointStorageIdentity =
        serde_json::from_slice(&bytes).map_err(|source| decode_store_error(operation, source))?;
    if identity.volume_id.trim().is_empty() || identity.wal_generation == 0 {
        return Err(state_corrupted(operation));
    }
    Ok(identity)
}

fn verify_local_checkpoint_layout(
    checkpoint_root: &Path,
    expected_identity: &ReleaseCheckpointStorageIdentity,
    offsets: ReleaseCheckpointOffsets,
) -> Result<(), StoreError> {
    let identity = read_storage_identity(&checkpoint_root.join(STORAGE_IDENTITY_FILE), StoreOperation::Read)?;
    if &identity != expected_identity {
        return Err(request_invalid(StoreOperation::Read));
    }
    for required in ["commitlog", "consumequeue", "index"] {
        let path = checkpoint_root.join(required);
        if !path.is_dir() {
            return Err(state_corrupted(StoreOperation::Read));
        }
    }
    let store_checkpoint = StoreCheckpoint::read_snapshot(checkpoint_root.join("checkpoint"))
        .map_err(|source| io_store_error(StoreOperation::Read, source))?;
    if store_checkpoint.index_safe_phy_offset < offsets.index_offset.max(0) as u64 {
        return Err(state_corrupted(StoreOperation::Read));
    }
    if store_checkpoint.master_flushed_offset > offsets.appended_offset.max(0) as u64
        || store_checkpoint.confirm_phy_offset > offsets.appended_offset.max(0) as u64
    {
        return Err(state_corrupted(StoreOperation::Read));
    }
    Ok(())
}

fn authorization_deadline(authorization: &MaintenanceAuthorizationGrant) -> Option<ShutdownDeadline> {
    let now = rocketmq_runtime::common::time_utils::current_millis();
    let remaining = authorization
        .deadline_unix_millis()
        .checked_sub(now)
        .filter(|remaining| *remaining > 0)?;
    Some(ShutdownDeadline::after(std::time::Duration::from_millis(remaining)))
}

fn store_error(descriptor: &'static rocketmq_error::ErrorDescriptor, operation: StoreOperation) -> StoreError {
    StoreError::new(descriptor, operation).in_component(StoreComponent::Store)
}

fn store_unavailable(operation: StoreOperation) -> StoreError {
    store_error(&rocketmq_error::STORAGE_LIFECYCLE_NOT_STARTED, operation)
}

fn request_invalid(operation: StoreOperation) -> StoreError {
    store_error(&rocketmq_error::STORAGE_REQUEST_INVALID, operation)
}

fn state_corrupted(operation: StoreOperation) -> StoreError {
    store_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
}

fn io_store_error(operation: StoreOperation, source: std::io::Error) -> StoreError {
    store_error(&rocketmq_error::STORAGE_IO_FAILED, operation).with_source(source)
}

fn decode_store_error(operation: StoreOperation, source: serde_json::Error) -> StoreError {
    state_corrupted(operation).with_source(source)
}

fn runtime_store_error(operation: StoreOperation, source: RuntimeError) -> StoreError {
    store_error(runtime_error_descriptor(&source), operation).with_source(source)
}

fn runtime_error_descriptor(source: &RuntimeError) -> &'static rocketmq_error::ErrorDescriptor {
    match source {
        RuntimeError::InvalidConfig(_) | RuntimeError::Configuration(_) => &rocketmq_error::STORAGE_REQUEST_INVALID,
        RuntimeError::BuildRuntime(_) | RuntimeError::Io(_) => &rocketmq_error::STORAGE_IO_FAILED,
        RuntimeError::NoCurrentRuntime | RuntimeError::TaskGroupClosing { .. } => {
            &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE
        }
        RuntimeError::InsideTokioRuntime(_) | RuntimeError::UnsupportedBlockingKind { .. } => {
            &rocketmq_error::STORAGE_OPERATION_UNSUPPORTED
        }
        RuntimeError::BlockingQueueTimeout { .. } | RuntimeError::BlockingTaskTimeoutStillRunning { .. } => {
            &rocketmq_error::STORAGE_OPERATION_TIMED_OUT
        }
        RuntimeError::BlockingQueueFull { .. } => &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED,
        RuntimeError::BlockingJoin { .. }
        | RuntimeError::ScheduledTaskExists { .. }
        | RuntimeError::LifecycleOperation { .. } => &rocketmq_error::STORAGE_INTERNAL_FAILURE,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use rocketmq_security_api::MaintenanceAuthorizationContext;
    use rocketmq_security_api::MaintenanceAuthorizer;
    use rocketmq_security_api::MaintenanceCapability;
    use rocketmq_security_api::MaintenancePolicy;
    use rocketmq_security_api::MaintenancePrincipalBinding;
    use rocketmq_security_api::MaintenanceRequestClass;
    use rocketmq_security_api::MaintenanceResourceBudget;
    use rocketmq_security_api::MaintenanceRole;
    use rocketmq_security_api::MaintenanceRoleGrant;
    use rocketmq_security_api::MAINTENANCE_POLICY_SCHEMA_VERSION;
    use tempfile::tempdir;

    fn expired_release_authorization() -> MaintenanceAuthorizationGrant {
        let policy = MaintenancePolicy {
            schema_version: MAINTENANCE_POLICY_SCHEMA_VERSION,
            policy_id: "rocketmq.store-release-checkpoint-test".to_owned(),
            policy_version: 1,
            require_authentication: true,
            require_authorization: true,
            require_fencing_token: true,
            max_request_lifetime_millis: 60_000,
            resource_budget: MaintenanceResourceBudget {
                max_checkpoint_bytes: 1024,
                max_store_members: 1,
                max_concurrent_operations: 1,
            },
            principal_bindings: vec![MaintenancePrincipalBinding {
                principal: "release-operator".to_owned(),
                roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
            }],
            role_grants: vec![MaintenanceRoleGrant {
                role: MaintenanceRole::ReleaseOperator,
                capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
            }],
        };
        let authorizer = MaintenanceAuthorizer::new(policy.into_validated().expect("valid maintenance policy"));
        authorizer
            .authorize(
                Some(&MaintenanceAuthorizationContext {
                    authentication_enabled: true,
                    authorization_enabled: true,
                    principal: Some("release-operator".to_owned()),
                    request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                    capability: MaintenanceCapability::ReleaseCheckpoint,
                    deadline_unix_millis: 1,
                    fencing_token: Some(7),
                }),
                0,
            )
            .expect("authorization is initially valid")
    }

    #[test]
    fn storage_identity_is_persisted_and_reloaded_without_generation_drift() {
        let temp = tempdir().expect("temporary Store root");
        let first =
            load_or_create_storage_identity(temp.path(), StoreOperation::Admin).expect("create storage identity");
        let second =
            load_or_create_storage_identity(temp.path(), StoreOperation::Admin).expect("reload storage identity");

        assert_eq!(first, second);
        assert_eq!(first.wal_generation, 1);
        assert!(temp.path().join(STORAGE_IDENTITY_FILE).is_file());
    }

    #[tokio::test]
    async fn expired_authorization_rejects_storage_identity_before_store_access() {
        let temp = tempdir().expect("temporary checkpoint root");
        let service = StoreReleaseCheckpointService::new(
            Weak::new(),
            temp.path().to_path_buf(),
            crate::runtime::test_service_context("release-checkpoint-expiry-test"),
        );

        let identity = service
            .storage_identity(&expired_release_authorization())
            .await
            .expect("authorization expiry is a semantic rejection");

        assert!(identity.is_none());
        assert!(!temp.path().join(STORAGE_IDENTITY_FILE).exists());
    }

    #[test]
    fn local_restore_verification_requires_identity_layout_and_index_progress() {
        let temp = tempdir().expect("temporary checkpoint");
        for directory in ["commitlog", "consumequeue", "index"] {
            fs::create_dir(temp.path().join(directory)).expect("checkpoint directory");
        }
        let identity = load_or_create_storage_identity(temp.path(), StoreOperation::Read).expect("storage identity");
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

    #[test]
    fn deterministic_identity_mismatch_is_source_free() {
        let error = request_invalid(StoreOperation::Read);

        assert_eq!(&rocketmq_error::STORAGE_REQUEST_INVALID, error.descriptor());
        assert_eq!(StoreOperation::Read, error.operation());
        assert_eq!(StoreComponent::Store, error.component());
        assert!(std::error::Error::source(&error).is_none());
        assert!(error
            .public_view()
            .expect("valid public view")
            .fields()
            .next()
            .is_none());
    }

    #[test]
    fn operational_checkpoint_source_remains_typed_once() {
        let error = io_store_error(StoreOperation::Flush, std::io::Error::other("checkpoint write failed"));

        assert_eq!(&rocketmq_error::STORAGE_IO_FAILED, error.descriptor());
        assert_eq!(StoreOperation::Flush, error.operation());
        assert_eq!(StoreComponent::Store, error.component());
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .is_some());
    }
}
