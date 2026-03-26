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

//! Example: single-node OpenRaft controller cluster.

use std::collections::BTreeMap;
use std::collections::HashSet;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;
use rocketmq_rust::ArcMut;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    println!("=== OpenRaft Single Node Example ===\n");

    let node_id = 1;
    let listen_addr = "127.0.0.1:60109".parse()?;
    let config = ArcMut::new(
        ControllerConfig::new_node(node_id, listen_addr)
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300),
    );

    let node = RaftNodeManager::new(config).await?;

    let mut nodes = BTreeMap::new();
    nodes.insert(
        node_id,
        Node {
            node_id,
            rpc_addr: listen_addr.to_string(),
        },
    );
    node.initialize_cluster(nodes).await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    println!("Leader elected: {}", node.is_leader().await?);

    let apply_response = node
        .client_write(ControllerRequest::ApplyBrokerId {
            cluster_name: "DefaultCluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            applied_broker_id: 1,
            register_check_code: "broker-a-check-code".to_string(),
        })
        .await?;
    println!("ApplyBrokerId response: {:?}", apply_response.data);

    let register_response = node
        .client_write(ControllerRequest::RegisterBroker {
            cluster_name: "DefaultCluster".to_string(),
            broker_name: "broker-a".to_string(),
            broker_address: "127.0.0.1:10911".to_string(),
            broker_id: 1,
            alive_broker_ids: HashSet::from([1]),
        })
        .await?;
    println!("RegisterBroker response: {:?}", register_response.data);

    let next_broker_id = node
        .store()
        .state_machine
        .replicas_info_manager()
        .get_next_broker_id("DefaultCluster", "broker-a")
        .response()
        .and_then(|header| header.next_broker_id)
        .unwrap_or_default();
    println!("Next broker id for broker-a: {}", next_broker_id);

    node.shutdown().await?;
    Ok(())
}
