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
use std::time::Duration;

use rocketmq_common::common::controller::ControllerConfig;
use rocketmq_controller::typ::Node;
use rocketmq_controller::Controller;
use rocketmq_controller::RaftController;
use rocketmq_rust::ArcMut;

fn test_config(port: u16) -> ArcMut<ControllerConfig> {
    ArcMut::new(
        ControllerConfig::default()
            .with_node_info(1, format!("127.0.0.1:{port}").parse().expect("valid test address"))
            .with_election_timeout_ms(300)
            .with_heartbeat_interval_ms(100),
    )
}

#[tokio::test]
async fn test_open_raft_controller_lifecycle() {
    let config = test_config(60201);
    let mut controller = RaftController::new_open_raft(config);

    assert!(controller.startup().await.is_ok());
    assert!(!controller.is_leader()); // Default is false
    assert!(controller.shutdown().await.is_ok());
}

#[tokio::test]
async fn test_raft_controller_wrapper_initializes_openraft_cluster() {
    let port = 60202;
    let config = test_config(port);

    let mut controller = RaftController::new_open_raft(config);
    controller.startup().await.expect("start openraft controller");

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: format!("127.0.0.1:{port}"),
        },
    );
    controller
        .initialize_cluster(nodes)
        .await
        .expect("initialize single-node openraft cluster");

    for _ in 0..30 {
        if controller.is_leader() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(controller.is_leader(), "OpenRaft controller should become leader");
    assert!(controller.shutdown().await.is_ok());
}
