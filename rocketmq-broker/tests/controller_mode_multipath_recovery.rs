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

//! Controller-side promotion contract for a recovered multipath Broker store.

use rocketmq_controller::ApplyBrokerIdEvent;
use rocketmq_controller::BrokerLiveInfoSnapshot;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerConfigReader;
use rocketmq_controller::ElectMasterEvent;
use rocketmq_controller::ReplicasInfoManager;

fn heartbeat(store_ready: bool) -> BrokerLiveInfoSnapshot {
    BrokerLiveInfoSnapshot {
        cluster_name: "multipath-cluster".to_owned(),
        broker_name: "broker-a".to_owned(),
        broker_addr: "127.0.0.1:10911".to_owned(),
        broker_id: 0,
        last_update_timestamp: 1_000,
        heartbeat_timeout_millis: 30_000,
        epoch: 1,
        max_offset: 64,
        confirm_offset: 48,
        election_priority: Some(1),
        store_ready,
    }
}

#[test]
fn controller_excludes_an_unrecovered_store_from_promotion_and_leases() {
    let manager = ReplicasInfoManager::new(ControllerConfigReader::new(ControllerConfig::new_node(
        1,
        "127.0.0.1:9876".parse().unwrap(),
    )));
    manager
        .try_apply_event(&ApplyBrokerIdEvent::new(
            "multipath-cluster",
            "broker-a",
            "127.0.0.1:10911",
            0,
            "check-0",
        ))
        .unwrap();
    manager
        .try_apply_event(&ElectMasterEvent::with_new_master("broker-a", 0))
        .unwrap();

    let unready = heartbeat(false);
    manager.on_broker_heartbeat(unready.identity(), unready.clone());
    assert!(manager.is_broker_active_at("multipath-cluster", "broker-a", 0, 1_001));
    assert!(!manager.is_broker_promotion_ready_at("multipath-cluster", "broker-a", 0, 1_001));
    assert!(manager.check_not_active_broker(1_001).is_empty());
    assert!(manager.grant_write_lease(&unready.identity(), &unready, true).is_none());

    let ready = heartbeat(true);
    manager.on_broker_heartbeat(ready.identity(), ready.clone());
    assert!(manager.is_broker_active_at("multipath-cluster", "broker-a", 0, 1_001));
    assert!(manager.is_broker_promotion_ready_at("multipath-cluster", "broker-a", 0, 1_001));
    assert!(manager.grant_write_lease(&ready.identity(), &ready, true).is_some());
}
