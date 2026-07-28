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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;

use rocketmq_controller::verify_controller_release_snapshot;
use rocketmq_controller::Controller;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerConfigReader;
use rocketmq_controller::Node;
use rocketmq_controller::RaftController;
use rocketmq_controller::StorageBackendType;
use rocketmq_protocol::protocol::body::release_checkpoint::ControllerReleaseSnapshotRequest;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenancePolicy;
use rocketmq_security_api::MaintenancePrincipalBinding;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_security_api::MaintenanceResourceBudget;
use rocketmq_security_api::MaintenanceRole;
use rocketmq_security_api::MaintenanceRoleGrant;
use tempfile::TempDir;

fn reserve_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
    let address = listener.local_addr().expect("reserved address");
    drop(listener);
    address
}

fn authorized_release_operator() -> MaintenanceAuthorizationGrant {
    let policy = MaintenancePolicy {
        schema_version: 1,
        policy_id: "rocketmq.production-maintenance".to_string(),
        policy_version: 7,
        require_authentication: true,
        require_authorization: true,
        require_fencing_token: true,
        max_request_lifetime_millis: 30_000,
        resource_budget: MaintenanceResourceBudget {
            max_checkpoint_bytes: 64 * 1024 * 1024,
            max_store_members: 8,
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
    let authorizer = MaintenanceAuthorizer::new(policy.into_validated().expect("validate maintenance policy contract"));
    let now = current_millis();
    authorizer
        .authorize(
            Some(&MaintenanceAuthorizationContext {
                authentication_enabled: true,
                authorization_enabled: true,
                principal: Some("release-operator".to_string()),
                request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                capability: MaintenanceCapability::ReleaseCheckpoint,
                deadline_unix_millis: now + 15_000,
                fencing_token: Some(42),
            }),
            now,
        )
        .expect("authorize release operator")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_release_snapshot_is_read_index_bound_and_restore_verified() {
    let address = reserve_address();
    let checkpoint_directory = TempDir::new().expect("create Controller checkpoint directory");
    let config = ControllerConfigReader::new(
        ControllerConfig::default()
            .with_node_info(1, address)
            .with_storage_backend(StorageBackendType::Memory)
            .with_election_timeout_ms(100)
            .with_heartbeat_interval_ms(30)
            .with_maintenance_checkpoint_root(checkpoint_directory.path().to_string_lossy()),
    );
    let service_context =
        rocketmq_runtime::RuntimeContext::from_current("controller-release-snapshot").service_context("controller");
    let mut controller = RaftController::new_open_raft(config, service_context);
    controller.startup().await.expect("start Controller");
    controller
        .initialize_cluster(BTreeMap::from([(
            1,
            Node {
                node_id: 1,
                rpc_addr: address.to_string(),
            },
        )]))
        .await
        .expect("initialize single-node cluster");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !controller.is_leader() || !controller.has_committed_log().unwrap_or(false) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Controller should become leader");

    let authorization = authorized_release_operator();
    let snapshot = controller
        .create_release_snapshot(
            &authorization,
            ControllerReleaseSnapshotRequest {
                checkpoint_id: "controller-checkpoint-7".to_string(),
                checkpoint_set_id: "checkpoint-set-7".to_string(),
                generation: 7,
                barrier_id: "barrier-42".to_string(),
            },
        )
        .await
        .expect("create release snapshot");

    assert_eq!(snapshot.manifest.artifact.generation, 7);
    assert_eq!(snapshot.manifest.voter_ids, vec![1]);
    assert!(snapshot.manifest.last_applied_index > 0);
    assert!(
        verify_controller_release_snapshot(&snapshot.payload, &snapshot.manifest)
            .expect("verify release snapshot")
            .checksum_verified
    );
    assert!(
        controller
            .verify_release_snapshot(&authorization, &snapshot.manifest)
            .await
            .expect("restore-verify persisted release snapshot")
            .checksum_verified
    );

    let mut corrupted = snapshot.payload.clone();
    *corrupted.last_mut().expect("snapshot payload") ^= 1;
    assert!(
        verify_controller_release_snapshot(&corrupted, &snapshot.manifest).is_err(),
        "corrupted snapshot must fail before restore"
    );
    fs::write(
        checkpoint_directory
            .path()
            .join("objects")
            .join(format!("{}.snapshot", snapshot.manifest.artifact.sha256)),
        corrupted,
    )
    .expect("corrupt persisted snapshot object");
    assert!(
        controller
            .verify_release_snapshot(&authorization, &snapshot.manifest)
            .await
            .is_err(),
        "persisted snapshot corruption must fail restore verification"
    );

    controller.shutdown().await.expect("shutdown Controller");
}
