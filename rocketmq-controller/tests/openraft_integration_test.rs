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

//! Integration tests for the OpenRaft-backed controller state machine.

use std::collections::BTreeMap;
use std::collections::HashSet;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::openraft::Store;
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
async fn test_node_creation() {
    let node = RaftNodeManager::new(test_config(9876)).await;
    assert!(node.is_ok(), "Failed to create Raft node: {:?}", node.err());
}

#[tokio::test]
async fn test_cluster_initialization() {
    let node = RaftNodeManager::new(test_config(19876)).await.unwrap();

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:19876".to_string(),
        },
    );

    let result = node.initialize_cluster(nodes).await;
    assert!(result.is_ok(), "Failed to initialize cluster: {:?}", result.err());

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let is_leader = node.is_leader().await.unwrap();
    assert!(is_leader, "Node should be the leader in a single-node cluster");
}

#[tokio::test]
async fn test_log_store() {
    use openraft::storage::RaftLogReader;
    use openraft::storage::RaftLogStorage;
    use rocketmq_controller::openraft::LogStore;

    let mut store = LogStore::new();

    let log_state = store.get_log_state().await.unwrap();
    assert!(log_state.last_log_id.is_none());
    assert!(log_state.last_purged_log_id.is_none());

    let vote = openraft::Vote::new(1, 1);
    store.save_vote(&vote).await.unwrap();

    let saved_vote = store.read_vote().await.unwrap();
    assert_eq!(saved_vote, Some(vote));
}

#[tokio::test]
async fn test_state_machine_uses_replicas_info_manager() {
    let state_machine = StateMachine::new(test_config(29876));
    let next_broker_id = state_machine
        .replicas_info_manager()
        .get_next_broker_id("test-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id");

    assert_eq!(next_broker_id, 1);
}

#[tokio::test]
async fn test_storage_operations() {
    use openraft::storage::RaftLogStorage;

    let store = Store::new(test_config(39876));

    let mut log_store = store.log_store.clone();
    let log_state = log_store.get_log_state().await.unwrap();
    assert!(log_state.last_log_id.is_none());

    let next_broker_id = store
        .state_machine
        .replicas_info_manager()
        .get_next_broker_id("test-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id");
    assert_eq!(next_broker_id, 1);
}

#[tokio::test]
async fn test_client_write_updates_replicas_info_manager() {
    let node = RaftNodeManager::new(test_config(49876)).await.unwrap();

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:49876".to_string(),
        },
    );

    node.initialize_cluster(nodes).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let apply_result = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "test-broker".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "check-code".to_string(),
        })
        .await;
    assert!(apply_result.is_ok(), "ApplyBrokerId failed: {:?}", apply_result.err());

    let register_result = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: "test-broker".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            broker_id: 1,
            alive_broker_ids: HashSet::from([1]),
        })
        .await;
    assert!(
        register_result.is_ok(),
        "RegisterBroker failed: {:?}",
        register_result.err()
    );

    let next_broker_id = node
        .store()
        .state_machine
        .replicas_info_manager()
        .get_next_broker_id("test-cluster", "test-broker")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id");
    assert_eq!(next_broker_id, 2);
}
