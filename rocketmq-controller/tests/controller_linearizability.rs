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

//! Linearizable controller read-path tests.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::ControllerConfigReader;
use rocketmq_controller::config::StorageBackendType;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;

fn reserve_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
    let address = listener.local_addr().expect("reserved address");
    drop(listener);
    address
}

async fn wait_for_leader(node: &RaftNodeManager) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if node.is_leader().await.unwrap_or(false) && node.has_committed_log() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("single-node controller should elect itself");
}

#[tokio::test]
async fn read_index_observes_the_preceding_committed_write() {
    let address = reserve_address();
    let config = ControllerConfigReader::new(
        ControllerConfig::default()
            .with_node_info(1, address)
            .with_storage_backend(StorageBackendType::Memory)
            .with_election_timeout_ms(100)
            .with_heartbeat_interval_ms(30),
    );
    let node = RaftNodeManager::new(config).await.expect("create Raft node");
    node.initialize_cluster(BTreeMap::from([(
        1,
        Node {
            node_id: 1,
            rpc_addr: address.to_string(),
        },
    )]))
    .await
    .expect("initialize cluster");
    wait_for_leader(&node).await;

    node.client_write(ControllerRequest::ApplyBrokerId {
        cluster_name: "linearizable-cluster".to_string(),
        broker_name: "broker-a".to_string(),
        broker_address: "127.0.0.1:10911".to_string(),
        applied_broker_id: 1,
        register_check_code: "check-code".to_string(),
    })
    .await
    .expect("commit broker id");

    let barrier = node
        .ensure_linearizable_read()
        .await
        .expect("complete ReadIndex barrier");
    assert!(barrier.is_some(), "ReadIndex should return the applied log boundary");
    let next_broker_id = node
        .store()
        .state_machine
        .read_view()
        .get_next_broker_id("linearizable-cluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .expect("next broker id after ReadIndex");
    assert_eq!(next_broker_id, 2);

    node.shutdown().await.expect("shutdown Raft node");
}
