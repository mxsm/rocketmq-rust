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

//! Example: three-node OpenRaft controller cluster.

use std::collections::BTreeMap;
use std::collections::HashSet;

use clap::Parser;
use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::config::RaftPeer;
use rocketmq_controller::openraft::RaftNodeManager;
use rocketmq_controller::typ::ControllerRequest;
use rocketmq_controller::typ::Node;
use rocketmq_rust::ArcMut;

#[derive(Parser, Debug)]
#[clap(name = "three-node-cluster")]
struct Args {
    #[clap(long, default_value = "1")]
    node_id: u64,

    #[clap(long)]
    init: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let peers = vec![
        RaftPeer {
            id: 1,
            addr: "127.0.0.1:9876".parse()?,
        },
        RaftPeer {
            id: 2,
            addr: "127.0.0.1:9877".parse()?,
        },
        RaftPeer {
            id: 3,
            addr: "127.0.0.1:9878".parse()?,
        },
    ];

    let current_peer = peers
        .iter()
        .find(|peer| peer.id == args.node_id)
        .ok_or("invalid node id")?;
    let config = ArcMut::new(
        ControllerConfig::new_node(args.node_id, current_peer.addr)
            .with_election_timeout_ms(1000)
            .with_heartbeat_interval_ms(300)
            .with_raft_peers(peers.clone()),
    );

    let node = RaftNodeManager::new(config).await?;

    if args.node_id == 1 && args.init {
        let mut nodes = BTreeMap::new();
        for peer in &peers {
            nodes.insert(
                peer.id,
                Node {
                    node_id: peer.id,
                    rpc_addr: peer.addr.to_string(),
                },
            );
        }
        node.initialize_cluster(nodes).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let is_leader = node.is_leader().await?;
    let leader_id = node.get_leader().await?;
    println!("Is Leader: {}", is_leader);
    println!("Current Leader: {:?}", leader_id);

    if is_leader {
        let broker_name = format!("broker-from-node-{}", args.node_id);

        let apply_response = node
            .client_write(ControllerRequest::ApplyBrokerId {
                cluster_name: "DefaultCluster".to_string(),
                broker_name: broker_name.clone(),
                broker_address: format!("127.0.0.1:1091{}", args.node_id),
                applied_broker_id: args.node_id,
                register_check_code: format!("{broker_name}-check-code"),
            })
            .await?;
        println!("ApplyBrokerId response: {:?}", apply_response.data);

        let register_response = node
            .client_write(ControllerRequest::RegisterBroker {
                cluster_name: "DefaultCluster".to_string(),
                broker_name: broker_name.clone(),
                broker_address: format!("127.0.0.1:1091{}", args.node_id),
                broker_id: args.node_id,
                alive_broker_ids: HashSet::from([args.node_id]),
            })
            .await?;
        println!("RegisterBroker response: {:?}", register_response.data);

        let next_broker_id = node
            .store()
            .state_machine
            .replicas_info_manager()
            .get_next_broker_id("DefaultCluster", &broker_name)
            .response()
            .and_then(|header| header.next_broker_id)
            .unwrap_or_default();
        println!("Next broker id for {}: {}", broker_name, next_broker_id);
    }

    println!("Node is running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;

    node.shutdown().await?;
    Ok(())
}
