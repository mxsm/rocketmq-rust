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
use std::collections::HashMap;
use std::collections::HashSet;

use openraft::storage::RaftStateMachine;
use openraft::RaftSnapshotBuilder;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::openraft::StateMachine;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::ControllerResponseHeader;
use rocketmq_controller::typ::Node;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
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

#[tokio::test]
async fn test_snapshot_install_preserves_master_and_sync_state_set() {
    let node = RaftNodeManager::new(test_config(61876)).await.unwrap();

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: "127.0.0.1:61876".to_string(),
        },
    );

    node.initialize_cluster(nodes).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let apply_master = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-sync".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "master-check-code".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(apply_master.data.response_code, ResponseCode::Success as i32);

    let apply_replica = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-sync".to_string(),
            broker_address: "127.0.0.1:10912".to_string(),
            applied_broker_id: 2,
            register_check_code: "replica-check-code".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(apply_replica.data.response_code, ResponseCode::Success as i32);

    let alive_broker_ids = HashSet::from([1_u64, 2_u64]);
    let register_master = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-sync".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            broker_id: 1,
            alive_broker_ids: alive_broker_ids.clone(),
        })
        .await
        .unwrap();
    assert_eq!(register_master.data.response_code, ResponseCode::Success as i32);

    let register_replica = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-sync".to_string(),
            broker_address: "127.0.0.1:10912".to_string(),
            broker_id: 2,
            alive_broker_ids: alive_broker_ids.clone(),
        })
        .await
        .unwrap();
    assert_eq!(register_replica.data.response_code, ResponseCode::Success as i32);

    let elect_response = node
        .client_write(ControllerRequest::ElectMaster {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-sync".to_string(),
            broker_id: Some(1),
            designate_elect: false,
            alive_broker_ids: alive_broker_ids.clone(),
            live_broker_infos: HashMap::new(),
        })
        .await
        .unwrap();
    assert_eq!(elect_response.data.response_code, ResponseCode::Success as i32);

    let elect_header = match elect_response.data.header {
        Some(ControllerResponseHeader::ElectMaster(header)) => header,
        _ => panic!("elect master should return elect-master response header"),
    };
    assert_eq!(elect_header.master_broker_id, Some(1));
    assert_eq!(elect_header.master_epoch, Some(1));
    assert_eq!(elect_header.sync_state_set_epoch, Some(1));

    let alter_response = node
        .client_write(ControllerRequest::AlterSyncStateSet {
            cluster_name: "test-cluster".to_string(),
            broker_name: "broker-sync".to_string(),
            master_broker_id: 1,
            master_epoch: elect_header.master_epoch.expect("master epoch"),
            new_sync_state_set: HashSet::from([1_u64, 2_u64]),
            sync_state_set_epoch: elect_header.sync_state_set_epoch.expect("sync state set epoch"),
            alive_broker_ids,
        })
        .await
        .unwrap();
    assert_eq!(alter_response.data.response_code, ResponseCode::Success as i32);

    let mut source = node.store().state_machine.clone();
    let snapshot = source.build_snapshot().await.unwrap();

    let mut target = StateMachine::new(test_config(62876));
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();

    let replica_info = target.replicas_info_manager().get_replica_info("broker-sync");
    assert!(
        replica_info.is_success(),
        "replica info should be restored from snapshot"
    );
    let replica_header = replica_info.response().expect("replica info header");
    assert_eq!(replica_header.master_broker_id, Some(1));
    assert_eq!(replica_header.master_address.as_deref(), Some("127.0.0.1:10911"));
    assert_eq!(replica_header.master_epoch, Some(1));

    let sync_state_set: SyncStateSet =
        serde_json::from_slice(replica_info.body().expect("replica sync state body")).expect("decode sync state set");
    assert_eq!(sync_state_set.get_sync_state_set_epoch(), 2);
    assert_eq!(
        sync_state_set.get_sync_state_set().cloned().unwrap_or_default(),
        HashSet::from([1_i64, 2_i64])
    );
    assert_eq!(
        target.replicas_info_manager().cluster_name("broker-sync").as_deref(),
        Some("test-cluster")
    );
    assert_eq!(
        target.replicas_info_manager().broker_ids("broker-sync"),
        HashSet::from([1_u64, 2_u64])
    );
}
