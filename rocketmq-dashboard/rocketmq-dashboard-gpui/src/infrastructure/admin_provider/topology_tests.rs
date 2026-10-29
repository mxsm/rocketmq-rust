// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;
#[path = "topology_route_tests.rs"]
mod route_tests;
use crate::infrastructure::admin_session::TopologyAdminData;
use rocketmq_admin_core::core::{infrastructure_observation::*, topology::*};
use rocketmq_dashboard_common::{
    TopologyEdgeKind, TopologyElection, TopologyNodeId, TopologySource, TopologySourceState,
};
fn key(cluster: &str, id: u64) -> TopologyBrokerKey {
    TopologyBrokerKey {
        cluster: cluster.into(),
        broker_name: "same-name".into(),
        broker_id: id,
        address: format!("{cluster}-{id}:10911"),
    }
}
fn replication() -> TopologyReplicationRead {
    let mut quality = TopologyReadQuality::complete(1);
    quality.failures = 1;
    TopologyReplicationRead {
        groups: TopologyRead {
            quality,
            items: vec![TopologyReplicationRow {
                cluster: "a".into(),
                broker_name: "same-name".into(),
                election: TopologyMasterElection::Unknown,
                controllers: Vec::new(),
                members: vec![key("a", 0), key("a", 1)],
                ha: None,
                sync_state: None,
            }],
        },
        controllers: Vec::new(),
        controller_total: 0,
        controller_total_complete: true,
    }
}
#[test]
fn topology_provider_keeps_unknown_replication_distinct_from_observed_ha_and_controller_sets() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        controls
            .topology_replies
            .lock()
            .unwrap()
            .push_back(TopologyAdminData::Replication(replication()));
        let unknown = provider
            .topology_layer(1, TopologySource::Replication, 100)
            .await
            .unwrap();
        assert_eq!(unknown.state, TopologySourceState::Partial);
        assert!(unknown.edges.is_empty());
        let group = &unknown.replication[0];
        assert_eq!(group.election, TopologyElection::Unknown);
        assert!(
            group
                .replicas
                .iter()
                .all(|replica| replica.in_sync.is_none() && replica.lag_bytes.is_none())
        );
        assert!(group.ha_observed_at_ms.is_none() && group.config_observed_at_ms.is_none());
        let mut known = replication();
        known.groups.quality.failures = 0;
        known.groups.quality.successful_reads = 3;
        let group = &mut known.groups.items[0];
        group.election = TopologyMasterElection::Controller;
        group.controllers = vec!["controller:9878".into()];
        let replica = LogicalBrokerInstance {
            broker_name: group.broker_name.clone(),
            broker_id: 1,
        };
        group.ha = Some(BrokerHaObservation {
            broker_name: group.broker_name.clone(),
            broker_id: 0,
            master_commit_log_max_offset: 512,
            in_sync_slave_count: 1,
            pending_group_transfer_request_count: 0,
            pending_group_transfer_oldest_wait_millis: 0,
            group_transfer_ack_notify_count: 0,
            connections: vec![HaConnectionObservation {
                replica: replica.clone(),
                slave_ack_offset: 448,
                diff: 64,
                in_sync: true,
                transferred_bytes_per_second: 32,
                transfer_from_where: 448,
            }],
        });
        group.sync_state = Some(BrokerSyncStateObservation {
            broker_name: group.broker_name.clone(),
            master_broker_id: 0,
            master_epoch: 7,
            sync_state_set_epoch: 8,
            in_sync_replicas: vec![replica],
            not_in_sync_replicas: Vec::new(),
        });
        known.controllers.push(TopologyControllerRow {
            address: "controller:9878".into(),
            metadata: Some(ControllerMetadataObservation {
                controller_name: "logical-controller".into(),
                group: Some("raft-a".into()),
                leader_id: Some("n0".into()),
                is_leader: Some(true),
                peer_count: Some(3),
                last_log_index: Some(20),
                committed_log_index: Some(19),
                applied_log_index: Some(18),
            }),
        });
        known.controller_total = 1;
        controls
            .topology_replies
            .lock()
            .unwrap()
            .push_back(TopologyAdminData::Replication(known));
        let observed = provider
            .topology_layer(1, TopologySource::Replication, 200)
            .await
            .unwrap();
        let group = &observed.replication[0];
        assert_eq!(group.election, TopologyElection::Controller);
        assert_eq!(
            (
                group.replicas[0].lag_bytes,
                group.replicas[0].in_sync,
                group.replicas[0].bytes_per_second
            ),
            (Some(64), Some(true), Some(32))
        );
        assert_eq!(group.sync_state.as_ref().unwrap().in_sync_broker_ids, vec![1]);
        assert_eq!(
            (group.ha_observed_at_ms, group.sync_observed_at_ms),
            (Some(200), Some(200))
        );
        assert!(
            observed
                .edges
                .iter()
                .any(|edge| edge.id.kind == TopologyEdgeKind::Replication && edge.in_sync == Some(true))
        );
        assert_eq!(observed.controllers[0].committed_log_index, Some(19));
        provider.switch(snapshot(2, ConnectionScope::Proxy)).await.unwrap();
        assert!(
            provider
                .topology_layer(2, TopologySource::Replication, 300)
                .await
                .is_err()
        );
        assert_eq!(controls.topology_reads.lock().unwrap().len(), 2);
        provider.shutdown().await;
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
#[test]
fn topology_provider_preserves_composite_brokers_fractional_rates_and_pretruncation_totals() {
    let runtime = runtime();
    let controls = Arc::new(FakeControls::default());
    let provider = provider(&runtime, controls.clone(), Ok(()), None, applied());
    runtime.block_on(async {
        provider.switch(snapshot(1, ConnectionScope::NameServer)).await.unwrap();
        let mut quality = TopologyReadQuality::complete(40);
        quality.truncated = true;
        quality.inventory_complete = false;
        quality.successful_reads = 1;
        quality.failures = 1;
        controls
            .topology_replies
            .lock()
            .unwrap()
            .push_back(TopologyAdminData::Brokers(TopologyRead {
                quality,
                items: vec![
                    TopologyBrokerRow {
                        identity: key("a", 0),
                        version: None,
                        produce_tps: Some(0.5),
                        consume_tps: None,
                        runtime_available: true,
                    },
                    TopologyBrokerRow {
                        identity: key("b", 0),
                        version: None,
                        produce_tps: None,
                        consume_tps: None,
                        runtime_available: false,
                    },
                ],
            }));
        let layer = provider.topology_layer(1, TopologySource::Brokers, 100).await.unwrap();
        let nodes = layer
            .nodes
            .iter()
            .filter(|node| matches!(node.id, TopologyNodeId::Broker(_)))
            .collect::<Vec<_>>();
        assert_ne!(nodes[0].id, nodes[1].id);
        assert_eq!(nodes[0].traffic.produce_tps, Some(0.5));
        assert_eq!(nodes[1].traffic.produce_tps, None);
        assert_eq!(layer.total, 40);
        assert!(layer.total_complete && layer.truncated && !layer.inventory_complete);
        assert_eq!(layer.state, TopologySourceState::Partial);
        assert_eq!(
            layer
                .edges
                .iter()
                .filter(|edge| matches!(edge.id.from, TopologyNodeId::NameServer(_)))
                .count(),
            2
        );
        provider.shutdown().await;
    });
    assert!(runtime.shutdown_runtime_blocking().unwrap().is_healthy());
}
