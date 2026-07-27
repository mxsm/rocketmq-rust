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
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointManifest;
use rocketmq_protocol::protocol::body::release_checkpoint::StoreReleaseCheckpointRequest;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_store_api::ReleaseCheckpointStore;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointBarrier;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointService;
use rocketmq_store_local::release_checkpoint::LocalReleaseCheckpointSnapshot;
use sha2::Digest;
use sha2::Sha256;
use tempfile::TempDir;

#[derive(Clone)]
struct TestBarrier {
    source_root: PathBuf,
    identity: ReleaseCheckpointStorageIdentity,
    offsets: ReleaseCheckpointOffsets,
}

impl LocalReleaseCheckpointBarrier for TestBarrier {
    type Error = std::io::Error;

    async fn begin_release_checkpoint(
        &self,
        _request: &StoreReleaseCheckpointRequest,
        deadline: ShutdownDeadline,
    ) -> Result<LocalReleaseCheckpointSnapshot, Self::Error> {
        if deadline.is_expired() {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "deadline expired"));
        }
        for path in [
            self.source_root.join("commitlog").join("00000000000000000000"),
            self.source_root
                .join("consumequeue")
                .join("TopicA")
                .join("0")
                .join("00000000000000000000"),
            self.source_root.join("index").join("20260728000000000000"),
        ] {
            fs::OpenOptions::new().write(true).open(path)?.sync_all()?;
        }
        Ok(LocalReleaseCheckpointSnapshot::new(
            self.source_root.clone(),
            self.identity.clone(),
            self.offsets,
            (),
        ))
    }

    async fn verify_release_checkpoint_restore(
        &self,
        checkpoint_root: &Path,
        manifest: &StoreReleaseCheckpointManifest,
        deadline: ShutdownDeadline,
    ) -> Result<ReleaseCheckpointOffsets, Self::Error> {
        if deadline.is_expired() {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "deadline expired"));
        }
        if manifest.storage_identity != self.identity {
            return Err(std::io::Error::other("storage identity changed"));
        }
        let restore_root = self.source_root.parent().expect("source parent").join("restore-verify");
        fs::create_dir_all(&restore_root)?;
        copy_tree(checkpoint_root, &restore_root)?;
        assert!(restore_root.join("commitlog").exists());
        assert!(restore_root.join("consumequeue").exists());
        assert!(restore_root.join("index").exists());
        Ok(manifest.offsets)
    }
}

fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            fs::create_dir_all(&destination_path)?;
            copy_tree(&source_path, &destination_path)?;
        } else {
            fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after Unix epoch")
        .as_millis() as u64
}

fn authorizer(temp: &TempDir) -> MaintenanceAuthorizer {
    let policy = MaintenancePolicy {
        schema_version: 1,
        policy_id: "rocketmq.release-checkpoint-test".to_string(),
        policy_version: 1,
        require_authentication: true,
        require_authorization: true,
        require_fencing_token: true,
        max_request_lifetime_millis: 60_000,
        resource_budget: MaintenanceResourceBudget {
            max_checkpoint_bytes: 1_048_576,
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

fn create_store_layout(root: &Path) {
    let files = [
        ("commitlog/00000000000000000000", b"acknowledged-message".as_slice()),
        (
            "consumequeue/TopicA/0/00000000000000000000",
            b"consume-offset".as_slice(),
        ),
        ("index/20260728000000000000", b"message-index".as_slice()),
    ];
    for (relative, bytes) in files {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().expect("parent")).expect("create Store directory");
        fs::write(path, bytes).expect("write Store file");
    }
}

#[tokio::test]
async fn release_checkpoint_restore_flushes_copies_hashes_and_verifies_local_store() {
    let temp = TempDir::new().expect("create test root");
    let source_root = temp.path().join("live-store");
    let checkpoint_root = temp.path().join("checkpoints");
    create_store_layout(&source_root);
    let offsets = ReleaseCheckpointOffsets {
        appended_offset: 120,
        durable_offset: 120,
        consume_queue_offset: 100,
        index_offset: 100,
    };
    let storage_identity = ReleaseCheckpointStorageIdentity {
        volume_id: "pvc-broker-a".to_string(),
        wal_generation: 7,
    };
    let barrier = Arc::new(TestBarrier {
        source_root,
        identity: storage_identity.clone(),
        offsets,
    });
    let runtime = rocketmq_runtime::RuntimeContext::from_current("local-release-checkpoint-test");
    let service = LocalReleaseCheckpointService::new(
        barrier,
        checkpoint_root,
        runtime.service_context("checkpoint").storage_io().clone(),
        1_048_576,
    );
    let authorizer = authorizer(&temp);
    let now = now_millis();
    let context = MaintenanceAuthorizationContext {
        authentication_enabled: true,
        authorization_enabled: true,
        principal: Some("release-operator".to_string()),
        request_class: MaintenanceRequestClass::PrivilegedMaintenance,
        capability: MaintenanceCapability::ReleaseCheckpoint,
        deadline_unix_millis: now + Duration::from_secs(30).as_millis() as u64,
        fencing_token: Some(42),
    };
    let grant = authorizer.authorize(Some(&context), now).expect("authorize checkpoint");
    let request = StoreReleaseCheckpointRequest {
        checkpoint_id: "broker-a-generation-7".to_string(),
        checkpoint_set_id: "set-generation-7".to_string(),
        generation: 7,
        barrier_id: "barrier-42".to_string(),
        member_id: "broker-a".to_string(),
        offsets,
        storage_identity,
    };

    let manifest = service
        .create_release_checkpoint(&grant, request)
        .await
        .expect("create local checkpoint");
    assert_eq!(manifest.artifact.generation, 7);
    assert_eq!(manifest.offsets.durable_offset, 120);
    assert_eq!(manifest.artifact.sha256.len(), 64);
    assert!(manifest.wal_retained);
    assert!(manifest.persistent_volume_retained);

    let verification = service
        .restore_verify_release_checkpoint(&grant, &manifest)
        .await
        .expect("restore-verify local checkpoint");
    assert!(verification.checksum_verified);
    assert!(verification.offsets_verified);
    assert!(verification.storage_identity_verified);
}
