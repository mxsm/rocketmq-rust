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
use std::fs;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_auth::MaintenanceAuthorizationContext;
use rocketmq_auth::MaintenanceAuthorizer;
use rocketmq_auth::MaintenanceCapability;
use rocketmq_auth::MaintenancePolicy;
use rocketmq_auth::MaintenancePolicyReference;
use rocketmq_auth::MaintenancePrincipalBinding;
use rocketmq_auth::MaintenanceRequestClass;
use rocketmq_auth::MaintenanceResourceBudget;
use rocketmq_auth::MaintenanceRole;
use rocketmq_auth::MaintenanceRoleGrant;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointOffsets;
use rocketmq_protocol::protocol::body::release_checkpoint::ReleaseCheckpointStorageIdentity;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointRequest;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_store_rocksdb::release_checkpoint::RocksDbReleaseCheckpointService;
use rocketmq_store_rocksdb::runtime::RocksDbRuntimeScope;
use rocketmq_store_rocksdb::store::KeyValueStore;
use rocketmq_store_rocksdb::RocksDbConfig;
use rocketmq_store_rocksdb::RocksDbStore;
use sha2::Digest;
use sha2::Sha256;
use tempfile::TempDir;

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after Unix epoch")
        .as_millis() as u64
}

fn authorizer(temp: &TempDir) -> MaintenanceAuthorizer {
    let policy = MaintenancePolicy {
        schema_version: 1,
        policy_id: "rocketmq.rocksdb-checkpoint-test".to_string(),
        policy_version: 1,
        require_authentication: true,
        require_authorization: true,
        require_fencing_token: true,
        max_request_lifetime_millis: 60_000,
        resource_budget: MaintenanceResourceBudget {
            max_checkpoint_bytes: 16 * 1024 * 1024,
            max_store_members: 4,
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
    let path = temp.path().join("maintenance-policy.json");
    let bytes = serde_json::to_vec_pretty(&policy).expect("serialize policy");
    fs::write(&path, &bytes).expect("write policy");
    let loaded = MaintenancePolicyReference {
        path,
        version: 1,
        sha256: hex::encode(Sha256::digest(&bytes)),
    }
    .load_from(temp.path())
    .expect("load policy");
    MaintenanceAuthorizer::new(loaded)
}

#[tokio::test]
async fn release_checkpoint_restore_flushes_hashes_and_reopens_rocksdb_read_only() {
    let temp = TempDir::new().expect("create test root");
    let database_path = temp.path().join("live-db");
    let checkpoint_root = temp.path().join("checkpoints");
    let config = RocksDbConfig {
        enabled: true,
        path: database_path,
        ..RocksDbConfig::default()
    };
    let store = Arc::new(RocksDbStore::open(config).expect("open RocksDB"));
    store
        .put_cf("default", b"offset", b"120")
        .expect("write checkpoint data");
    let runtime = rocketmq_runtime::RuntimeContext::from_current("rocksdb-release-checkpoint-test");
    let service_context = runtime.service_context("checkpoint");
    let runtime_scope = RocksDbRuntimeScope::new(service_context);
    let storage_identity = ReleaseCheckpointStorageIdentity {
        volume_id: "pvc-rocksdb-a".to_string(),
        wal_generation: 7,
    };
    let service = RocksDbReleaseCheckpointService::new(
        store,
        runtime_scope,
        checkpoint_root,
        storage_identity.clone(),
        16 * 1024 * 1024,
    );
    let authorizer = authorizer(&temp);
    let now = now_millis();
    let context = MaintenanceAuthorizationContext {
        authentication_enabled: true,
        authorization_enabled: true,
        principal: Some("release-operator".to_string()),
        request_class: MaintenanceRequestClass::PrivilegedMaintenance,
        capability: MaintenanceCapability::ReleaseCheckpoint,
        deadline_unix_millis: now + 30_000,
        fencing_token: Some(77),
    };
    let grant = authorizer.authorize(Some(&context), now).expect("authorize checkpoint");
    let offsets = ReleaseCheckpointOffsets {
        appended_offset: 120,
        durable_offset: 120,
        consume_queue_offset: 120,
        index_offset: 120,
    };
    let request = StoreReleaseCheckpointRequest {
        checkpoint_id: "rocksdb-a-generation-7".to_string(),
        checkpoint_set_id: "set-generation-7".to_string(),
        generation: 7,
        barrier_id: "barrier-77".to_string(),
        member_id: "rocksdb-a".to_string(),
        offsets,
        storage_identity,
    };

    let manifest = service
        .create_release_checkpoint(&grant, request)
        .await
        .expect("create RocksDB checkpoint");
    assert_eq!(manifest.artifact.sha256.len(), 64);
    assert_eq!(manifest.offsets, offsets);

    let verification = service
        .restore_verify_release_checkpoint(&grant, &manifest)
        .await
        .expect("restore-verify RocksDB checkpoint");
    assert!(verification.checksum_verified);
    assert!(verification.offsets_verified);
    assert!(verification.storage_identity_verified);
}
