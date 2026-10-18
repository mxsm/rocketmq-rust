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

//! The replication layer of the topology: how each broker group elects its master, how far its
//! slaves lag behind, and the Controllers of the groups in Controller mode.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::admin::ControllerReading;
use crate::admin::MasterElection;
use crate::admin::ReplicationObservation;
use crate::admin::ReplicationTarget;
use crate::model::BrokerInfo;
use crate::model::TopologyCluster;
use crate::model::TopologyController;
use crate::model::TopologyControllerGroupState;
use crate::model::TopologyControllerStatus;
use crate::model::TopologyMasterElection;

/// Groups with a registered master: their broker config and HA state are read from it.
pub(super) fn replication_targets(brokers: &[BrokerInfo]) -> Vec<ReplicationTarget> {
    brokers
        .iter()
        .filter(|broker| broker.broker_id == 0)
        .map(|broker| (broker.cluster_name.as_str(), broker.broker_name.as_str()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|(cluster_name, broker_name)| ReplicationTarget {
            cluster_name: cluster_name.to_string(),
            broker_name: broker_name.to_string(),
        })
        .collect()
}

/// Adds the readings to the groups they belong to and returns the Controllers. A slave the master
/// reports no connection from is not in sync.
pub(super) fn apply_replication(
    clusters: &mut [TopologyCluster],
    observation: ReplicationObservation,
) -> Vec<TopologyController> {
    let mut readings = observation
        .groups
        .into_iter()
        .map(|group| ((group.cluster_name.clone(), group.broker_name.clone()), group))
        .collect::<BTreeMap<_, _>>();
    for cluster in clusters.iter_mut() {
        for group in &mut cluster.brokers {
            let Some(reading) = readings.remove(&(cluster.cluster_name.clone(), group.broker_name.clone())) else {
                continue;
            };
            group.election = match reading.election {
                MasterElection::Configured => TopologyMasterElection::Configured,
                MasterElection::Controller => TopologyMasterElection::Controller,
                MasterElection::Unknown => TopologyMasterElection::Unknown,
            };
            group.controller_addresses = reading.controller_addresses;
            if let Some(ha) = reading.ha {
                group.in_sync_slave_count = Some(ha.in_sync_slave_count);
                for replica in &mut group.replicas {
                    let connection = ha
                        .connections
                        .iter()
                        .find(|connection| connection.replica.broker_id == replica.broker_id);
                    replica.in_sync = Some(connection.is_some_and(|connection| connection.in_sync));
                    replica.lag_bytes = connection.map(|connection| connection.diff);
                    replica.transfer_bytes_per_second =
                        connection.map(|connection| connection.transferred_bytes_per_second);
                }
            }
            group.controller_state = reading.sync_state.map(|state| TopologyControllerGroupState {
                master_epoch: state.master_epoch,
                sync_state_set_epoch: state.sync_state_set_epoch,
                in_sync_broker_ids: state.in_sync_replicas.iter().map(|replica| replica.broker_id).collect(),
                out_of_sync_broker_ids: state
                    .not_in_sync_replicas
                    .iter()
                    .map(|replica| replica.broker_id)
                    .collect(),
            });
        }
    }
    observation.controllers.into_iter().map(controller).collect()
}

fn controller(reading: ControllerReading) -> TopologyController {
    let Some(metadata) = reading.metadata else {
        return TopologyController {
            address: reading.address,
            status: TopologyControllerStatus::Unavailable,
            leader: None,
            leader_id: None,
            group: None,
            peer_count: None,
            last_log_index: None,
            committed_log_index: None,
            applied_log_index: None,
        };
    };
    TopologyController {
        address: reading.address,
        status: TopologyControllerStatus::Available,
        leader: metadata.is_leader,
        leader_id: metadata.leader_id,
        group: metadata.group,
        peer_count: metadata.peer_count,
        last_log_index: metadata.last_log_index,
        committed_log_index: metadata.committed_log_index,
        applied_log_index: metadata.applied_log_index,
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::infrastructure_observation::BrokerHaObservation;
    use rocketmq_admin_core::core::infrastructure_observation::BrokerSyncStateObservation;
    use rocketmq_admin_core::core::infrastructure_observation::ControllerMetadataObservation;
    use rocketmq_admin_core::core::infrastructure_observation::HaConnectionObservation;
    use rocketmq_admin_core::core::infrastructure_observation::LogicalBrokerInstance;

    use super::*;
    use crate::admin::GroupReplication;
    use crate::model::TopologyBrokerGroup;
    use crate::model::TopologyReplica;

    fn broker(cluster_name: &str, broker_name: &str, broker_id: u64) -> BrokerInfo {
        BrokerInfo {
            cluster_name: cluster_name.to_string(),
            broker_name: broker_name.to_string(),
            broker_id,
            address: format!("10.0.0.{broker_id}:10911"),
            role: String::new(),
            version: "V5_3_1".to_string(),
            produce_tps: 0.0,
            consume_tps: 0.0,
        }
    }

    fn replica(broker_id: u64) -> TopologyReplica {
        TopologyReplica {
            broker_id,
            address: format!("10.0.0.{broker_id}:10911"),
            lag_bytes: None,
            in_sync: None,
            transfer_bytes_per_second: None,
        }
    }

    fn cluster(cluster_name: &str, broker_name: &str, slave_ids: &[u64]) -> TopologyCluster {
        TopologyCluster {
            cluster_name: cluster_name.to_string(),
            produce_tps: 0.0,
            consume_tps: 0.0,
            brokers: vec![TopologyBrokerGroup {
                broker_name: broker_name.to_string(),
                master_address: Some("10.0.0.0:10911".to_string()),
                slave_addresses: Vec::new(),
                version: String::new(),
                produce_tps: 0.0,
                consume_tps: 0.0,
                election: TopologyMasterElection::Unknown,
                controller_addresses: Vec::new(),
                replicas: slave_ids.iter().copied().map(replica).collect(),
                in_sync_slave_count: None,
                controller_state: None,
            }],
        }
    }

    fn instance(broker_name: &str, broker_id: u64) -> LogicalBrokerInstance {
        LogicalBrokerInstance {
            broker_name: broker_name.to_string(),
            broker_id,
        }
    }

    #[test]
    fn reads_every_group_with_a_master_once() {
        let brokers = [
            broker("c1", "broker-b", 0),
            broker("c1", "broker-b", 2),
            broker("c1", "broker-a", 0),
            broker("c2", "broker-x", 3),
        ];

        let targets = replication_targets(&brokers);

        assert_eq!(
            targets
                .iter()
                .map(|target| (target.cluster_name.as_str(), target.broker_name.as_str()))
                .collect::<Vec<_>>(),
            [("c1", "broker-a"), ("c1", "broker-b")]
        );
    }

    #[test]
    fn adds_lag_sync_state_and_controllers_to_the_view() {
        let mut clusters = vec![
            cluster("ControllerCluster", "broker-c", &[2, 3]),
            cluster("DefaultCluster", "broker-a", &[1]),
        ];
        let observation = ReplicationObservation {
            groups: vec![
                GroupReplication {
                    cluster_name: "ControllerCluster".to_string(),
                    broker_name: "broker-c".to_string(),
                    election: MasterElection::Controller,
                    controller_addresses: vec!["10.0.1.1:9878".to_string(), "10.0.1.2:9878".to_string()],
                    ha: Some(BrokerHaObservation {
                        broker_name: "broker-c".to_string(),
                        broker_id: 0,
                        master_commit_log_max_offset: 4_096,
                        in_sync_slave_count: 1,
                        pending_group_transfer_request_count: 0,
                        pending_group_transfer_oldest_wait_millis: 0,
                        group_transfer_ack_notify_count: 0,
                        connections: vec![HaConnectionObservation {
                            replica: instance("broker-c", 2),
                            slave_ack_offset: 4_000,
                            diff: 96,
                            in_sync: true,
                            transferred_bytes_per_second: 2_048,
                            transfer_from_where: 4_000,
                        }],
                    }),
                    sync_state: Some(BrokerSyncStateObservation {
                        broker_name: "broker-c".to_string(),
                        master_broker_id: 0,
                        master_epoch: 3,
                        sync_state_set_epoch: 5,
                        in_sync_replicas: vec![instance("broker-c", 0), instance("broker-c", 2)],
                        not_in_sync_replicas: vec![instance("broker-c", 3)],
                    }),
                },
                GroupReplication {
                    cluster_name: "DefaultCluster".to_string(),
                    broker_name: "broker-a".to_string(),
                    election: MasterElection::Configured,
                    controller_addresses: Vec::new(),
                    ha: None,
                    sync_state: None,
                },
            ],
            controllers: vec![
                ControllerReading {
                    address: "10.0.1.1:9878".to_string(),
                    metadata: Some(ControllerMetadataObservation {
                        controller_name: "controller-1".to_string(),
                        group: None,
                        leader_id: Some("1".to_string()),
                        is_leader: Some(true),
                        peer_count: Some(2),
                        last_log_index: Some(12),
                        committed_log_index: Some(12),
                        applied_log_index: Some(11),
                    }),
                },
                ControllerReading {
                    address: "10.0.1.2:9878".to_string(),
                    metadata: None,
                },
            ],
        };

        let controllers = apply_replication(&mut clusters, observation);

        let controller_group = &clusters[0].brokers[0];
        assert_eq!(controller_group.election, TopologyMasterElection::Controller);
        assert_eq!(controller_group.in_sync_slave_count, Some(1));
        assert_eq!(
            controller_group
                .replicas
                .iter()
                .map(|replica| (
                    replica.broker_id,
                    replica.in_sync,
                    replica.lag_bytes,
                    replica.transfer_bytes_per_second
                ))
                .collect::<Vec<_>>(),
            // broker 3 has no HA connection: not in sync, lag unknown.
            [(2, Some(true), Some(96), Some(2_048)), (3, Some(false), None, None)]
        );
        assert_eq!(
            controller_group.controller_state,
            Some(TopologyControllerGroupState {
                master_epoch: 3,
                sync_state_set_epoch: 5,
                in_sync_broker_ids: vec![0, 2],
                out_of_sync_broker_ids: vec![3],
            })
        );

        let configured = &clusters[1].brokers[0];
        assert_eq!(configured.election, TopologyMasterElection::Configured);
        assert_eq!(configured.replicas[0].in_sync, None);
        assert_eq!(configured.controller_state, None);

        assert_eq!(controllers.len(), 2);
        assert_eq!(controllers[0].status, TopologyControllerStatus::Available);
        assert_eq!(controllers[0].leader, Some(true));
        assert_eq!(controllers[0].applied_log_index, Some(11));
        assert_eq!(controllers[1].status, TopologyControllerStatus::Unavailable);
        assert_eq!(controllers[1].leader, None);
    }
}
