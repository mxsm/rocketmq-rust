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
use std::net::TcpListener;
use std::time::Duration;

use rocketmq_controller::Controller;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerConfigReader;
use rocketmq_controller::Node;
use rocketmq_controller::RaftController;
use rocketmq_controller::StorageBackendType;

fn test_service_context(name: &'static str) -> rocketmq_runtime::ChildServiceContext {
    rocketmq_runtime::RuntimeContext::from_current(name).service_context("controller-test")
}

fn test_config() -> ControllerConfigReader {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
    let address = listener.local_addr().expect("read reserved address");
    ControllerConfigReader::new(
        ControllerConfig::default()
            .with_node_info(1, address)
            .with_election_timeout_ms(300)
            .with_heartbeat_interval_ms(100)
            .with_storage_backend(StorageBackendType::Memory),
    )
}

#[tokio::test]
async fn test_open_raft_controller_lifecycle() {
    let config = test_config();
    let mut controller = RaftController::new_open_raft(config, test_service_context("raft-controller-lifecycle"));

    assert!(controller.startup().await.is_ok());
    assert!(!controller.is_leader()); // Default is false
    assert!(controller.shutdown().await.is_ok());
}

#[tokio::test]
async fn test_raft_controller_wrapper_initializes_openraft_cluster() {
    let config = test_config();
    let address = config.snapshot().local_raft_addr();

    let mut controller = RaftController::new_open_raft(config, test_service_context("raft-controller-cluster"));
    controller.startup().await.expect("start openraft controller");

    let mut nodes = BTreeMap::new();
    nodes.insert(
        1,
        Node {
            node_id: 1,
            rpc_addr: address.to_string(),
        },
    );
    controller
        .initialize_cluster(nodes)
        .await
        .expect("initialize single-node openraft cluster");

    tokio::time::timeout(Duration::from_secs(3), async {
        while !controller.is_leader() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("OpenRaft controller should become leader before the deadline");

    assert!(controller.is_leader(), "OpenRaft controller should become leader");
    assert!(controller.shutdown().await.is_ok());
}
