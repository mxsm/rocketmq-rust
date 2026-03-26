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

//! Snapshot functionality tests for the OpenRaft controller state machine.

use std::collections::BTreeMap;
use std::collections::HashSet;

use openraft::RaftSnapshotBuilder;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;
use rocketmq_rust::ArcMut;

fn test_config(port: u16) -> ArcMut<ControllerConfig> {
    ArcMut::new(
        ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse().expect("valid socket addr"))
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    )
}

#[tokio::test]
async fn test_snapshot_creation() {
    let node = RaftNodeManager::new(test_config(39876)).await.unwrap();

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:39876".to_string(),
        },
    );

    node.initialize_cluster(nodes).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    node.client_write(ControllerRequest::ApplyBrokerId {
        cluster_name: "test-cluster".to_string(),
        broker_name: "broker-a".to_string(),
        broker_address: "127.0.0.1:10911".to_string(),
        applied_broker_id: 1,
        register_check_code: "check-code".to_string(),
    })
    .await
    .unwrap();

    node.client_write(ControllerRequest::RegisterBroker {
        cluster_name: "test-cluster".to_string(),
        broker_name: "broker-a".to_string(),
        broker_address: "127.0.0.1:10911".to_string(),
        broker_id: 1,
        alive_broker_ids: HashSet::from([1]),
    })
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let next_broker_id = node
        .store()
        .state_machine
        .replicas_info_manager()
        .get_next_broker_id("test-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id");
    assert_eq!(next_broker_id, 2);
}

#[tokio::test]
async fn test_state_machine_snapshot() {
    let mut state_machine = StateMachine::new(test_config(49876));

    let snapshot_result = state_machine.build_snapshot().await;
    assert!(snapshot_result.is_ok(), "Failed to build snapshot");

    let snapshot = snapshot_result.unwrap();
    assert!(!snapshot.snapshot.get_ref().is_empty(), "Snapshot should contain data");
}

#[tokio::test]
async fn test_snapshot_install() {
    use openraft::storage::RaftStateMachine;

    let mut source = StateMachine::new(test_config(59876));
    let replicas_info_manager = source.replicas_info_manager();
    let apply_result =
        replicas_info_manager.apply_broker_id("test-cluster", "broker-a", "127.0.0.1:10911", 1, "check-code");
    for event in apply_result.events() {
        replicas_info_manager.apply_event(event.as_ref());
    }

    let snapshot = source.build_snapshot().await.unwrap();

    let mut target = StateMachine::new(test_config(60876));
    let result = target.install_snapshot(&snapshot.meta, snapshot.snapshot).await;
    assert!(result.is_ok(), "Failed to install snapshot");

    let next_broker_id = target
        .replicas_info_manager()
        .get_next_broker_id("test-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id");
    assert_eq!(next_broker_id, 2);
}
