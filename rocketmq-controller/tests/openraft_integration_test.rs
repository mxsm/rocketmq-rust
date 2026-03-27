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
use std::path::Path;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::StorageBackendType;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::openraft::Store;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::ControllerResponseHeader;
use rocketmq_controller::typ::Node;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_rust::ArcMut;

fn test_config(port: u16) -> ArcMut<ControllerConfig> {
    ArcMut::new(
        ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse().expect("valid socket addr"))
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    )
}

fn persistent_test_config(port: u16, storage_path: &Path) -> ArcMut<ControllerConfig> {
    ArcMut::new(
        ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse().expect("valid socket addr"))
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300)
            .with_storage_backend(StorageBackendType::File)
            .with_storage_path(storage_path.to_string_lossy().into_owned()),
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

#[tokio::test]
async fn test_single_node_restart_recovers_state_from_file_storage() {
    let temp_root = tempfile::tempdir().expect("create temp dir for persistent openraft controller storage");
    let storage_path = temp_root.path().join("controller-openraft-restart");
    let config = persistent_test_config(59876, &storage_path);

    let node = RaftNodeManager::new(config.clone()).await.unwrap();

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:59876".to_string(),
        },
    );

    node.initialize_cluster(nodes).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let apply_master = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "master-check-code".to_string(),
        })
        .await
        .expect("apply master broker id before restart");
    assert_eq!(apply_master.data.response_code, ResponseCode::Success as i32);

    let apply_replica = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker".to_string(),
            broker_address: "127.0.0.1:10912".to_string(),
            applied_broker_id: 2,
            register_check_code: "replica-check-code".to_string(),
        })
        .await
        .expect("apply replica broker id before restart");
    assert_eq!(apply_replica.data.response_code, ResponseCode::Success as i32);

    let alive_broker_ids = HashSet::from([1_u64, 2_u64]);
    let register_master = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            broker_id: 1,
            alive_broker_ids: alive_broker_ids.clone(),
        })
        .await
        .expect("register master broker before restart");
    assert_eq!(register_master.data.response_code, ResponseCode::Success as i32);

    let register_replica = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker".to_string(),
            broker_address: "127.0.0.1:10912".to_string(),
            broker_id: 2,
            alive_broker_ids: alive_broker_ids.clone(),
        })
        .await
        .expect("register replica broker before restart");
    assert_eq!(register_replica.data.response_code, ResponseCode::Success as i32);

    let elect_response = node
        .client_write(ControllerRequest::ElectMaster {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker".to_string(),
            broker_id: Some(1),
            designate_elect: false,
            alive_broker_ids: alive_broker_ids.clone(),
            live_broker_infos: Default::default(),
        })
        .await
        .expect("elect master before restart");
    assert_eq!(elect_response.data.response_code, ResponseCode::Success as i32);

    let elect_header = match elect_response.data.header {
        Some(ControllerResponseHeader::ElectMaster(header)) => header,
        _ => panic!("elect master should return elect-master response header"),
    };

    let alter_response = node
        .client_write(ControllerRequest::AlterSyncStateSet {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker".to_string(),
            master_broker_id: 1,
            master_epoch: elect_header.master_epoch.expect("master epoch before restart"),
            new_sync_state_set: HashSet::from([1_u64, 2_u64]),
            sync_state_set_epoch: elect_header
                .sync_state_set_epoch
                .expect("sync state set epoch before restart"),
            alive_broker_ids,
        })
        .await
        .expect("alter sync state set before restart");
    assert_eq!(alter_response.data.response_code, ResponseCode::Success as i32);

    node.shutdown().await.expect("shutdown node before restart");

    let restarted_node = RaftNodeManager::new(config).await.unwrap();
    let replicas_info_manager = restarted_node.store().state_machine.replicas_info_manager();

    assert_eq!(
        replicas_info_manager.cluster_name("restart-broker").as_deref(),
        Some("test-cluster")
    );
    assert_eq!(
        replicas_info_manager.broker_ids("restart-broker"),
        HashSet::from([1_u64, 2_u64])
    );

    let replica_info = replicas_info_manager.get_replica_info("restart-broker");
    assert!(
        replica_info.is_success(),
        "replica info should be recovered after restart"
    );
    let replica_header = replica_info.response().expect("replica info response");
    assert_eq!(replica_header.master_broker_id, Some(1));
    assert_eq!(replica_header.master_address.as_deref(), Some("127.0.0.1:10911"));
    assert_eq!(replica_header.master_epoch, Some(1));

    for _ in 0..20 {
        if restarted_node.is_leader().await.unwrap_or(false) {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    assert!(
        restarted_node.is_leader().await.unwrap_or(false),
        "restarted single node should recover leadership without reinitializing cluster"
    );

    let post_restart_write = restarted_node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "restart-broker-after".to_string(),
            broker_address: "127.0.0.1:10913".to_string(),
            applied_broker_id: 1,
            register_check_code: "post-restart-check-code".to_string(),
        })
        .await
        .expect("post-restart write should succeed");
    assert_eq!(post_restart_write.data.response_code, ResponseCode::Success as i32);
}
