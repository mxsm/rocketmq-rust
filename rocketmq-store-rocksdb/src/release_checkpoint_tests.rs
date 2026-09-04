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

use std::collections::BTreeSet;

use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenancePolicy;
use rocketmq_security_api::MaintenancePrincipalBinding;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_security_api::MaintenanceResourceBudget;
use rocketmq_security_api::MaintenanceRole;
use rocketmq_security_api::MaintenanceRoleGrant;
use rocketmq_store_api::checkpoint::CheckpointOffsets as ReleaseCheckpointOffsets;
use tempfile::TempDir;

use super::*;
use crate::config::RocksDbConfig;
use crate::store::KeyValueStore;

#[derive(Debug, thiserror::Error)]
#[error("private RocksDB checkpoint cause")]
struct CheckpointCause;

struct CheckpointArtifactIoCause {
    path: PathBuf,
    source: std::io::Error,
}

impl fmt::Display for CheckpointArtifactIoCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RocksDB checkpoint artifact I/O failed")
    }
}

impl fmt::Debug for CheckpointArtifactIoCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckpointArtifactIoCause")
            .field("path_present", &!self.path.as_os_str().is_empty())
            .field("source_present", &true)
            .finish()
    }
}

impl StdError for CheckpointArtifactIoCause {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

struct PublicCheckpointFixture {
    _temp: TempDir,
    store: Arc<RocksDbStore>,
    runtime: rocketmq_runtime::RuntimeContext,
    checkpoint_root: PathBuf,
    storage_identity: ReleaseCheckpointStorageIdentity,
    grant: MaintenanceAuthorizationGrant,
    request: StoreReleaseCheckpointRequest,
}

impl PublicCheckpointFixture {
    fn new(checkpoint_id: &str) -> Self {
        let temp = TempDir::new().expect("create checkpoint fixture root");
        let store = Arc::new(
            RocksDbStore::open(RocksDbConfig {
                enabled: true,
                path: temp.path().join("live-db"),
                ..RocksDbConfig::default()
            })
            .expect("open checkpoint fixture RocksDB")
            .expect("enable checkpoint fixture RocksDB"),
        );
        store
            .put_cf(StoreOperation::Append, "default", b"offset", b"120")
            .expect("write checkpoint fixture data");
        let storage_identity = ReleaseCheckpointStorageIdentity {
            volume_id: "pvc-rocksdb-checkpoint-regression".to_string(),
            wal_generation: 7,
        };
        let policy = MaintenancePolicy {
            schema_version: 1,
            policy_id: "rocketmq.rocksdb-checkpoint-regression".to_string(),
            policy_version: 1,
            require_authentication: true,
            require_authorization: true,
            require_fencing_token: true,
            max_request_lifetime_millis: 60_000,
            resource_budget: MaintenanceResourceBudget {
                max_checkpoint_bytes: 16 * 1024 * 1024,
                max_store_members: 1,
                max_concurrent_operations: 1,
            },
            principal_bindings: vec![MaintenancePrincipalBinding {
                principal: "release-operator".to_string(),
                roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
            }],
            role_grants: vec![MaintenanceRoleGrant {
                role: MaintenanceRole::ReleaseOperator,
                capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
            }],
        };
        let authorizer = MaintenanceAuthorizer::new(policy.into_validated().expect("validate checkpoint policy"));
        let now = unix_millis().expect("clock after Unix epoch");
        let grant = authorizer
            .authorize(
                Some(&MaintenanceAuthorizationContext {
                    authentication_enabled: true,
                    authorization_enabled: true,
                    principal: Some("release-operator".to_string()),
                    request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                    capability: MaintenanceCapability::ReleaseCheckpoint,
                    deadline_unix_millis: now + 30_000,
                    fencing_token: Some(77),
                }),
                now,
            )
            .expect("authorize checkpoint fixture");
        let request = StoreReleaseCheckpointRequest {
            checkpoint_id: checkpoint_id.to_string(),
            checkpoint_set_id: "set-generation-7".to_string(),
            generation: 7,
            barrier_id: "barrier-77".to_string(),
            member_id: "rocksdb-a".to_string(),
            offsets: ReleaseCheckpointOffsets {
                appended_offset: 120,
                durable_offset: 120,
                consume_queue_offset: 120,
                index_offset: 120,
            },
            storage_identity: storage_identity.clone(),
        };

        Self {
            checkpoint_root: temp.path().join("checkpoints"),
            _temp: temp,
            store,
            runtime: rocketmq_runtime::RuntimeContext::from_current("rocksdb-checkpoint-regression"),
            storage_identity,
            grant,
            request,
        }
    }

    fn service(&self, component: &'static str) -> RocksDbReleaseCheckpointService {
        RocksDbReleaseCheckpointService::new(
            Arc::clone(&self.store),
            RocksDbRuntimeScope::new(self.runtime.service_context(component)),
            self.checkpoint_root.clone(),
            self.storage_identity.clone(),
            16 * 1024 * 1024,
        )
    }

    fn service_with_hasher(
        &self,
        component: &'static str,
        artifact_hasher: CheckpointArtifactHasher,
    ) -> RocksDbReleaseCheckpointService {
        self.service(component).with_artifact_hasher(artifact_hasher)
    }

    fn service_with_clock(&self, component: &'static str, clock: CheckpointClock) -> RocksDbReleaseCheckpointService {
        self.service(component).with_clock(clock)
    }
}

fn artifact_contract_failure(
    _path: &Path,
    _maximum_bytes: u64,
    operation: StoreOperation,
) -> Result<CheckpointDirectoryDigest, StoreError> {
    Err(StoreError::new(&rocketmq_error::STORAGE_REQUEST_INVALID, operation)
        .in_component(StoreComponent::RocksDb)
        .with_source(StoreContractViolation::CheckpointArtifactEmpty))
}

fn artifact_io_failure(
    _path: &Path,
    _maximum_bytes: u64,
    operation: StoreOperation,
) -> Result<CheckpointDirectoryDigest, StoreError> {
    Err(StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
        .in_component(StoreComponent::RocksDb)
        .with_source(CheckpointArtifactIoCause {
            path: PathBuf::from("sensitive-create-checkpoint-path-canary"),
            source: std::io::Error::other("sensitive-create-checkpoint-source-canary"),
        }))
}

fn owner_correct_restore_failure(
    _path: &Path,
    _maximum_bytes: u64,
    operation: StoreOperation,
) -> Result<CheckpointDirectoryDigest, StoreError> {
    Err(StoreError::new(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, operation)
        .in_component(StoreComponent::RocksDb)
        .with_detail("sensitive-restore-checkpoint-detail-canary")
        .with_source(CheckpointArtifactIoCause {
            path: PathBuf::from("sensitive-restore-checkpoint-path-canary"),
            source: std::io::Error::other("sensitive-restore-checkpoint-source-canary"),
        }))
}

fn clock_overflow_failure() -> Result<u64, RocksDbReleaseCheckpointError> {
    let source = u64::try_from(u128::MAX).expect_err("u128::MAX must overflow u64");
    Err(RocksDbReleaseCheckpointError::ClockOverflow(source))
}

#[tokio::test]
async fn public_create_supplies_artifact_contract_owner() {
    let fixture = PublicCheckpointFixture::new("contract-failure");
    let service = fixture.service_with_hasher("contract-failure", artifact_contract_failure);

    let error = service
        .create_release_checkpoint(&fixture.grant, fixture.request.clone())
        .await
        .expect_err("artifact contract failure must remain an operational create error");

    assert_eq!(&rocketmq_error::STORAGE_REQUEST_INVALID, error.descriptor());
    assert_eq!(StoreOperation::Flush, error.operation());
    assert_eq!(StoreComponent::RocksDb, error.component());
    let direct_source = error.source().expect("preserve the typed artifact contract source");
    assert!(direct_source.downcast_ref::<StoreContractViolation>().is_some());
    assert!(direct_source.downcast_ref::<StoreError>().is_none());
}

#[tokio::test]
async fn public_create_supplies_artifact_io_owner_and_redacts_private_evidence() {
    let fixture = PublicCheckpointFixture::new("io-failure");
    let service = fixture.service_with_hasher("io-failure", artifact_io_failure);

    let error = service
        .create_release_checkpoint(&fixture.grant, fixture.request.clone())
        .await
        .expect_err("artifact I/O failure must remain an operational create error");

    assert_eq!(&rocketmq_error::STORAGE_IO_FAILED, error.descriptor());
    assert_eq!(StoreOperation::Flush, error.operation());
    assert_eq!(StoreComponent::RocksDb, error.component());
    let direct_source = error.source().expect("preserve the typed artifact I/O source");
    assert!(direct_source.downcast_ref::<CheckpointArtifactIoCause>().is_some());
    assert!(direct_source.downcast_ref::<StoreError>().is_none());
    assert!(source_chain_contains::<std::io::Error>(&error));
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains("sensitive-create-checkpoint-path-canary"));
    assert!(!rendered.contains("sensitive-create-checkpoint-source-canary"));
}

#[tokio::test]
async fn public_restore_preserves_owner_correct_store_error_without_nesting() {
    let fixture = PublicCheckpointFixture::new("restore-passthrough");
    let create_service = fixture.service("restore-passthrough-create");
    let outcome = create_service
        .create_release_checkpoint(&fixture.grant, fixture.request.clone())
        .await
        .expect("create checkpoint fixture for restore");
    let ReleaseCheckpointCreateOutcome::Created(manifest) = outcome else {
        panic!("checkpoint fixture creation must succeed");
    };
    let restore_service = fixture.service_with_hasher("restore-passthrough-verify", owner_correct_restore_failure);

    let error = restore_service
        .restore_verify_release_checkpoint(&fixture.grant, &manifest)
        .await
        .expect_err("owner-correct restore failure must pass through");

    assert_eq!(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, error.descriptor());
    assert_eq!(StoreOperation::Read, error.operation());
    assert_eq!(StoreComponent::RocksDb, error.component());
    let direct_source = error.source().expect("preserve the original typed restore source");
    assert!(direct_source.downcast_ref::<CheckpointArtifactIoCause>().is_some());
    assert!(direct_source.downcast_ref::<StoreError>().is_none());
    assert!(source_chain_contains::<std::io::Error>(&error));
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains("sensitive-restore-checkpoint-detail-canary"));
    assert!(!rendered.contains("sensitive-restore-checkpoint-path-canary"));
    assert!(!rendered.contains("sensitive-restore-checkpoint-source-canary"));
}

#[tokio::test]
async fn public_create_preserves_clock_overflow_source() {
    let fixture = PublicCheckpointFixture::new("clock-overflow");
    let service = fixture.service_with_clock("clock-overflow", clock_overflow_failure);

    let error = service
        .create_release_checkpoint(&fixture.grant, fixture.request.clone())
        .await
        .expect_err("clock conversion overflow must remain an operational create error");

    assert_eq!(&rocketmq_error::STORAGE_INTERNAL_FAILURE, error.descriptor());
    assert_eq!(StoreOperation::Flush, error.operation());
    assert_eq!(StoreComponent::RocksDb, error.component());
    let direct_source = error.source().expect("preserve the integer conversion source");
    assert!(direct_source.downcast_ref::<std::num::TryFromIntError>().is_some());
    assert!(direct_source.downcast_ref::<StoreError>().is_none());
}

#[test]
fn contained_store_error_is_forwarded_without_remapping() {
    let original = StoreError::new(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, StoreOperation::Append)
        .in_component(StoreComponent::CommitLog)
        .with_source(CheckpointCause);

    let error = rocksdb_checkpoint_error(StoreOperation::Flush, RocksDbReleaseCheckpointError::Store(original));

    assert_eq!(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, error.descriptor());
    assert_eq!(StoreOperation::Append, error.operation());
    assert_eq!(StoreComponent::CommitLog, error.component());
    assert!(std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<CheckpointCause>())
        .is_some());
}

#[test]
fn checkpoint_artifact_helper_contract_uses_create_owner() {
    let root = tempfile::tempdir().expect("create empty checkpoint root");
    let helper_error = hash_rocksdb_checkpoint_directory(root.path(), 1024, StoreOperation::Flush)
        .expect_err("an empty checkpoint artifact must be rejected");

    let error = rocksdb_checkpoint_error(
        StoreOperation::Flush,
        RocksDbReleaseCheckpointError::Artifact(helper_error),
    );

    assert_eq!(&rocketmq_error::STORAGE_REQUEST_INVALID, error.descriptor());
    assert_eq!(StoreOperation::Flush, error.operation());
    assert_eq!(StoreComponent::RocksDb, error.component());
    let direct_source = error.source().expect("preserve the typed artifact contract source");
    assert!(direct_source.downcast_ref::<StoreContractViolation>().is_some());
    assert!(direct_source.downcast_ref::<StoreError>().is_none());
}

#[test]
fn checkpoint_artifact_helper_io_uses_restore_owner() {
    let missing = std::env::temp_dir().join("sensitive-rocksdb-checkpoint-helper-path-canary");
    let helper_error = hash_rocksdb_checkpoint_directory(&missing, 1024, StoreOperation::Read)
        .expect_err("a missing checkpoint artifact must fail with I/O");

    let error = rocksdb_checkpoint_error(
        StoreOperation::Read,
        RocksDbReleaseCheckpointError::Artifact(helper_error),
    );

    assert_eq!(&rocketmq_error::STORAGE_IO_FAILED, error.descriptor());
    assert_eq!(StoreOperation::Read, error.operation());
    assert_eq!(StoreComponent::RocksDb, error.component());
    let direct_source = error.source().expect("preserve the typed artifact I/O source");
    assert!(direct_source
        .downcast_ref::<RocksDbCheckpointArtifactIoError>()
        .is_some());
    assert!(direct_source.downcast_ref::<StoreError>().is_none());
    assert!(source_chain_contains::<std::io::Error>(&error));
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains("sensitive-rocksdb-checkpoint-helper-path-canary"));
}

#[test]
fn private_checkpoint_hasher_matches_store_api_artifact_contract() {
    let root = tempfile::tempdir().expect("create checkpoint hash contract fixture");
    let nested = root.path().join("nested");
    fs::create_dir_all(&nested).expect("create nested checkpoint directory");
    fs::write(root.path().join("a.sst"), b"alpha").expect("write first checkpoint file");
    fs::write(nested.join("b.sst"), b"beta").expect("write second checkpoint file");
    fs::write(root.path().join(RELEASE_CHECKPOINT_MANIFEST_FILE), b"excluded").expect("write excluded manifest");

    let shared = rocketmq_store_api::hash_checkpoint_directory(root.path(), 1024)
        .expect("shared checkpoint artifact contract must hash fixture");
    let rocksdb = hash_rocksdb_checkpoint_directory(root.path(), 1024, StoreOperation::Flush)
        .expect("private RocksDB checkpoint hasher must hash fixture");
    assert_eq!(rocksdb, shared);

    let shared_capacity = rocketmq_store_api::hash_checkpoint_directory(root.path(), 3)
        .expect_err("shared helper must enforce the byte budget");
    let rocksdb_capacity = hash_rocksdb_checkpoint_directory(root.path(), 3, StoreOperation::Flush)
        .expect_err("private RocksDB helper must enforce the byte budget");
    assert_eq!(
        checkpoint_capacity_rejection(&rocksdb_capacity),
        checkpoint_capacity_rejection(&shared_capacity)
    );
}

#[test]
fn checkpoint_io_leaf_redacts_path_and_source_text() {
    let leaf = RocksDbReleaseCheckpointError::Io {
        operation: "read sensitive-rocksdb-checkpoint-operation-canary",
        path: PathBuf::from("sensitive-rocksdb-checkpoint-path-canary"),
        source: std::io::Error::other("sensitive-rocksdb-checkpoint-source-canary"),
    };

    let rendered = format!("{leaf} {leaf:?}");
    assert!(!rendered.contains("sensitive-rocksdb-checkpoint-operation-canary"));
    assert!(!rendered.contains("sensitive-rocksdb-checkpoint-path-canary"));
    assert!(!rendered.contains("sensitive-rocksdb-checkpoint-source-canary"));
    assert!(leaf
        .source()
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some());
}

#[test]
fn capacity_rejection_recovers_the_typed_contract_source() {
    let error = StoreError::new(&rocketmq_error::STORAGE_CAPACITY_EXHAUSTED, StoreOperation::Flush).with_source(
        StoreContractViolation::CheckpointArtifactTooLarge {
            actual: 17,
            maximum: 16,
        },
    );

    assert_eq!(Some((17, 16)), checkpoint_capacity_rejection(&error));
}

#[test]
fn rocksdb_checkpoint_leaf_mapping_is_operation_aware_and_typed() {
    let error = rocksdb_checkpoint_error(
        StoreOperation::Read,
        RocksDbReleaseCheckpointError::Violation(RocksDbReleaseCheckpointViolation::InvalidConfiguration),
    );

    assert_eq!(&rocketmq_error::STORAGE_REQUEST_INVALID, error.descriptor());
    assert_eq!(StoreOperation::Read, error.operation());
    assert_eq!(StoreComponent::Configuration, error.component());
    assert!(std::error::Error::source(&error).is_none());
    assert!(error
        .public_view()
        .expect("valid public view")
        .fields()
        .next()
        .is_none());
    assert!(!format!("{error:?}").contains("configuration"));
}

fn source_chain_contains<T>(error: &(dyn StdError + 'static)) -> bool
where
    T: StdError + 'static,
{
    let mut current = Some(error);
    while let Some(source) = current {
        if source.downcast_ref::<T>().is_some() {
            return true;
        }
        current = source.source();
    }
    false
}
