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

use std::sync::Mutex;

use rocketmq_protocol::protocol::body::broker_replicas_info::ReplicaIdentity;
use rocketmq_protocol::protocol::body::broker_replicas_info::ReplicasInfo;
use rocketmq_protocol::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

use super::*;

const CLUSTER: &str = "cluster-a";
const ADDRESS_A: &str = "broker-a.internal:10911";
const ADDRESS_A_SLAVE: &str = "broker-a-slave.internal:10911";
const ADDRESS_B: &str = "broker-b.internal:10911";
const CONTROLLER_A: &str = "10.0.0.1:9878";
const CONTROLLER_B: &str = "10.0.0.2:9878";

type TopologyEntry<'a> = (&'a str, &'a str, &'a [(u64, &'a str)]);

#[derive(Default)]
struct FakeSource {
    cluster_info: ClusterInfo,
    ha: BTreeMap<String, ObservationResult<HARuntimeInfo>>,
    sync: BTreeMap<String, ObservationResult<serde_json::Value>>,
    metadata: BTreeMap<String, ObservationResult<GetMetaDataResponseHeader>>,
    nameserver: BTreeMap<String, ObservationResult<HashMap<CheetahString, CheetahString>>>,
    cluster_calls: Mutex<u32>,
    ha_calls: Mutex<Vec<String>>,
    sync_calls: Mutex<Vec<String>>,
    metadata_calls: Mutex<Vec<String>>,
    nameserver_calls: Mutex<Vec<String>>,
}

impl InfrastructureObservationSource for FakeSource {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError> {
        *self.cluster_calls.lock().unwrap() += 1;
        Ok(self.cluster_info.clone())
    }

    async fn ha_runtime(&self, endpoint: CheetahString) -> ObservationResult<HARuntimeInfo> {
        self.ha_calls.lock().unwrap().push(endpoint.to_string());
        self.ha
            .get(endpoint.as_str())
            .cloned()
            .unwrap_or_else(|| Ok(master_runtime(Vec::new())))
    }

    async fn sync_state(
        &self,
        endpoint: CheetahString,
        _broker_names: Vec<CheetahString>,
    ) -> ObservationResult<BrokerReplicasInfo> {
        self.sync_calls.lock().unwrap().push(endpoint.to_string());
        self.sync
            .get(endpoint.as_str())
            .cloned()
            .unwrap_or_else(|| Ok(serde_json::to_value(BrokerReplicasInfo::new()).unwrap()))
            .and_then(|value| {
                serde_json::from_value(value).map_err(|_| {
                    InfrastructureObservationReadError::new(
                        InfrastructureObservationReadErrorCode::InvalidResponse,
                        false,
                    )
                })
            })
    }

    async fn controller_metadata(&self, endpoint: CheetahString) -> ObservationResult<GetMetaDataResponseHeader> {
        self.metadata_calls.lock().unwrap().push(endpoint.to_string());
        self.metadata.get(endpoint.as_str()).cloned().unwrap_or_else(|| {
            Ok(GetMetaDataResponseHeader {
                controller_leader_address: Some("secret-leader.internal:9878".into()),
                peers: Some("peer-a.internal:9878;peer-b.internal:9878".into()),
                ..Default::default()
            })
        })
    }

    async fn nameserver_config(
        &self,
        endpoint: CheetahString,
    ) -> ObservationResult<HashMap<CheetahString, CheetahString>> {
        self.nameserver_calls.lock().unwrap().push(endpoint.to_string());
        self.nameserver
            .get(endpoint.as_str())
            .cloned()
            .unwrap_or_else(|| Ok(HashMap::new()))
    }
}

#[tokio::test]
async fn ha_uses_selected_cluster_embedded_identity_and_master_only() {
    let mut cluster_info = topology(&[
        (CLUSTER, "broker-a", &[(0, ADDRESS_A), (1, ADDRESS_A_SLAVE)]),
        (CLUSTER, "broker-b", &[(0, ADDRESS_B)]),
    ]);
    cluster_info
        .cluster_addr_table
        .as_mut()
        .unwrap()
        .entry("other-cluster".into())
        .or_default()
        .insert("broker-a".into());
    let source = FakeSource {
        cluster_info,
        ..Default::default()
    };
    let request = QueryHaStatusRequest::try_new(CLUSTER, ["broker-a".to_string()], false, Vec::new()).unwrap();
    let result = query_ha_status_from(&source, &[], &request).await.unwrap();
    assert_eq!(result.data.brokers.len(), 1);
    assert_eq!(&*source.ha_calls.lock().unwrap(), &[ADDRESS_A]);

    let mut mismatched = topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A)])]);
    mismatched
        .broker_addr_table
        .as_mut()
        .unwrap()
        .get_mut("broker-a")
        .unwrap()
        .set_cluster("other-cluster".into());
    let source = FakeSource {
        cluster_info: mismatched,
        ..Default::default()
    };
    assert!(query_ha_status_from(&source, &[], &request).await.is_err());
    assert!(source.ha_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn ha_unknown_selector_and_target_caps_issue_no_observation_rpc() {
    let source = FakeSource {
        cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A)])]),
        ..Default::default()
    };
    let unknown = QueryHaStatusRequest::try_new(CLUSTER, ["broker-z".to_string()], false, Vec::new()).unwrap();
    assert!(query_ha_status_from(&source, &[], &unknown).await.is_err());
    assert!(source.ha_calls.lock().unwrap().is_empty());

    let sixty_four = topology_owned(64);
    assert_eq!(
        resolve_ha_topology(&sixty_four, CLUSTER, &[]).unwrap().masters.len(),
        MAX_HA_BROKER_TARGETS
    );
    let sixty_five = topology_owned(65);
    assert!(resolve_ha_topology(&sixty_five, CLUSTER, &[]).is_err());

    let large_cluster = topology_owned(256);
    assert_eq!(
        resolve_ha_topology(&large_cluster, CLUSTER, &["broker-00".to_string()])
            .unwrap()
            .masters
            .len(),
        1,
        "a small selector must not inherit the full-cluster cap"
    );

    let exact_instances = topology_with_instances(MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER);
    assert_eq!(
        resolve_ha_topology(&exact_instances, CLUSTER, &[])
            .unwrap()
            .masters
            .len(),
        1
    );
    let too_many_instances = topology_with_instances(MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER + 1);
    let topology = resolve_ha_topology(&too_many_instances, CLUSTER, &[]).unwrap();
    assert!(topology.masters.is_empty());
    assert_eq!(topology.failures.len(), 1);
}

#[test]
fn ha_projection_enforces_connection_caps_identity_and_numeric_invariants() {
    let (connections, identities) = valid_connections(MAX_HA_CONNECTIONS_PER_BROKER);
    let runtime = master_runtime(connections.clone());
    assert!(project_ha_runtime("broker-a", 0, MAX_HA_CONNECTIONS_PER_BROKER, runtime, &identities).is_some());

    let (too_many, identities) = valid_connections(MAX_HA_CONNECTIONS_PER_BROKER + 1);
    assert!(project_ha_runtime(
        "broker-a",
        0,
        MAX_HA_CONNECTIONS_PER_BROKER,
        master_runtime(too_many),
        &identities
    )
    .is_none());

    let invalid =
        |mut connections: Vec<HAConnectionRuntimeInfo>,
         mut identities: BTreeMap<String, Option<LogicalBrokerInstance>>,
         mutation: fn(&mut HAConnectionRuntimeInfo, &mut BTreeMap<String, Option<LogicalBrokerInstance>>)| {
            mutation(&mut connections[0], &mut identities);
            project_ha_runtime(
                "broker-a",
                0,
                MAX_HA_CONNECTIONS_PER_BROKER,
                master_runtime(connections),
                &identities,
            )
            .is_none()
        };
    let (connections, identities) = valid_connections(1);
    assert!(invalid(connections.clone(), identities.clone(), |connection, _| {
        connection.diff = -1
    }));
    assert!(invalid(connections.clone(), identities.clone(), |connection, _| {
        connection.diff = 9
    }));
    assert!(invalid(connections.clone(), identities.clone(), |connection, _| {
        connection.slave_ack_offset = 101
    }));
    assert!(invalid(connections.clone(), identities.clone(), |connection, _| {
        connection.transfer_from_where = 101
    }));
    assert!(invalid(
        connections.clone(),
        identities.clone(),
        |connection, identities| {
            identities.insert(
                connection.addr.clone(),
                Some(LogicalBrokerInstance {
                    broker_name: "other-broker".to_string(),
                    broker_id: 1,
                }),
            );
        }
    ));
    assert!(invalid(
        connections.clone(),
        identities.clone(),
        |connection, identities| {
            identities.insert(
                connection.addr.clone(),
                Some(LogicalBrokerInstance {
                    broker_name: "broker-a".to_string(),
                    broker_id: 0,
                }),
            );
        }
    ));

    let mut duplicated = connections;
    duplicated.push(duplicated[0].clone());
    let mut runtime = master_runtime(duplicated);
    runtime.in_sync_slave_nums = 2;
    assert!(project_ha_runtime("broker-a", 0, MAX_HA_CONNECTIONS_PER_BROKER, runtime, &identities).is_none());
    let mut runtime = master_runtime(valid_connections(1).0);
    runtime.in_sync_slave_nums = 0;
    assert!(project_ha_runtime(
        "broker-a",
        0,
        MAX_HA_CONNECTIONS_PER_BROKER,
        runtime,
        &valid_connections(1).1
    )
    .is_none());
    let (connections, identities) = valid_connections(1);
    let mut disconnected_in_sync_member = master_runtime(connections);
    disconnected_in_sync_member.in_sync_slave_nums = 2;
    assert!(project_ha_runtime("broker-a", 0, 2, disconnected_in_sync_member.clone(), &identities).is_some());
    assert!(project_ha_runtime("broker-a", 0, 1, disconnected_in_sync_member, &identities).is_none());
    let (connections, identities) = valid_connections(1);
    let mut runtime = master_runtime(connections.clone());
    runtime.master_commit_log_max_offset = u64::MAX;
    assert!(project_ha_runtime("broker-a", 0, MAX_HA_CONNECTIONS_PER_BROKER, runtime, &identities).is_none());
    let mut runtime = master_runtime(connections.clone());
    runtime.ha_connection_info[0].transferred_byte_in_second = u64::MAX;
    assert!(project_ha_runtime("broker-a", 0, MAX_HA_CONNECTIONS_PER_BROKER, runtime, &identities).is_none());
    let mut runtime = master_runtime(connections);
    runtime.pending_group_transfer_request_count = u64::MAX;
    assert!(project_ha_runtime("broker-a", 0, MAX_HA_CONNECTIONS_PER_BROKER, runtime, &identities).is_none());
}

#[tokio::test]
async fn ha_address_reverse_lookup_failure_is_sanitized_partial_or_total() {
    let connection = HAConnectionRuntimeInfo {
        addr: "unmapped-secret.internal:10911".to_string(),
        ..Default::default()
    };
    let source = FakeSource {
        cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A)])]),
        ha: BTreeMap::from([(ADDRESS_A.to_string(), Ok(master_runtime(vec![connection.clone()])))]),
        ..Default::default()
    };
    let request = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), false, Vec::new()).unwrap();
    let error = query_ha_status_from(&source, &[], &request).await.unwrap_err();
    assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
    assert!(!format!("{error:?}").contains("unmapped-secret"));

    let source = FakeSource {
        cluster_info: topology(&[
            (CLUSTER, "broker-a", &[(0, ADDRESS_A)]),
            (CLUSTER, "broker-b", &[(0, ADDRESS_B)]),
        ]),
        ha: BTreeMap::from([
            (ADDRESS_A.to_string(), Ok(master_runtime(Vec::new()))),
            (ADDRESS_B.to_string(), Ok(master_runtime(vec![connection]))),
        ]),
        ..Default::default()
    };
    let result = query_ha_status_from(&source, &[], &request).await.unwrap();
    assert!(result.partial);
    assert_eq!(result.data.brokers.len(), 1);
    let wire = serde_json::to_string(&result).unwrap();
    assert!(!wire.contains("unmapped-secret"));
    assert!(!wire.contains(ADDRESS_A));
}

#[tokio::test]
async fn ha_query_wide_row_budget_reserves_whole_sources_at_exact_and_plus_one() {
    let exact_source = budget_ha_source(39);
    let request = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), false, Vec::new()).unwrap();
    let exact = query_ha_status_from(&exact_source, &[], &request).await.unwrap();
    assert!(!exact.partial);
    assert_eq!(ha_result_rows(&exact.data), MAX_INFRASTRUCTURE_QUERY_ROWS);
    assert_eq!(exact.data.brokers.len(), 16);

    let overflow_source = budget_ha_source(40);
    let overflow = query_ha_status_from(&overflow_source, &[], &request).await.unwrap();
    assert!(overflow.partial);
    assert_eq!(
        ha_result_rows(&overflow.data),
        15 * MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER
    );
    assert_eq!(
        overflow.data.brokers.len(),
        15,
        "the overflowing source must append no partial rows"
    );
    assert_eq!(overflow.source_failures[0].logical_target(), "broker-15");

    let total_source = oversized_sync_source(16, MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER);
    let controllers = [ControllerObservationTarget::new("controller-a", CONTROLLER_A)];
    let request = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), true, Vec::new()).unwrap();
    let error = query_ha_status_from(&total_source, &controllers, &request)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some("ADMIN_QUERY_ALL_SOURCES_FAILED"));
    assert_eq!(total_source.sync_calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn maximum_ha_and_sync_shape_remains_bounded_and_whole_source_partial() {
    let mut source = oversized_sync_source(MAX_SYNC_BROKERS, MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER);
    source
        .ha
        .values_mut()
        .for_each(|runtime| *runtime = Ok(master_runtime(Vec::new())));
    let sync = source.sync.remove(CONTROLLER_A).unwrap();
    source.metadata.clear();
    let controllers = (1..=MAX_CONTROLLER_TARGETS)
        .map(|index| {
            let endpoint = format!("10.2.0.{index}:9878");
            source.sync.insert(endpoint.clone(), sync.clone());
            source
                .metadata
                .insert(endpoint.clone(), Ok(controller_metadata(&endpoint, true, &[&endpoint])));
            ControllerObservationTarget::new(format!("controller-{index:02}"), endpoint)
        })
        .collect::<Vec<_>>();
    let request = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), true, Vec::new()).unwrap();
    let result = query_ha_status_from(&source, &controllers, &request).await.unwrap();
    assert!(result.partial);
    assert_eq!(result.data.brokers.len(), MAX_SYNC_BROKERS);
    assert!(result.data.controller_sync_states.is_empty());
    assert!(ha_result_rows(&result.data) <= MAX_INFRASTRUCTURE_QUERY_ROWS);
    assert_eq!(source.sync_calls.lock().unwrap().len(), MAX_CONTROLLER_TARGETS);
}

#[tokio::test]
async fn sync_state_is_opt_in_alias_bounded_and_address_free() {
    let state = valid_sync_state();
    let source = FakeSource {
        cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A), (1, ADDRESS_A_SLAVE)])]),
        sync: BTreeMap::from([(CONTROLLER_A.to_string(), Ok(serde_json::to_value(state).unwrap()))]),
        metadata: BTreeMap::from([(
            CONTROLLER_A.to_string(),
            Ok(controller_metadata(CONTROLLER_A, true, &[CONTROLLER_A])),
        )]),
        ..Default::default()
    };
    let controllers = vec![ControllerObservationTarget::new("controller-a", CONTROLLER_A)];
    let without = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), false, Vec::new()).unwrap();
    query_ha_status_from(&source, &controllers, &without).await.unwrap();
    assert!(source.sync_calls.lock().unwrap().is_empty());

    let with = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), true, ["controller-a".to_string()]).unwrap();
    let result = query_ha_status_from(&source, &controllers, &with).await.unwrap();
    let sync = &result.data.controller_sync_states[0].brokers[0];
    assert_eq!(sync.master_epoch, 7);
    assert_eq!(sync.not_in_sync_replicas[0].broker_id, 1);
    let wire = serde_json::to_string(&result).unwrap();
    assert!(!wire.contains(ADDRESS_A));
    assert!(!wire.contains(CONTROLLER_A));

    let no_config = FakeSource {
        cluster_info: source.cluster_info.clone(),
        ..Default::default()
    };
    assert!(query_ha_status_from(&no_config, &[], &with).await.is_err());
    assert!(no_config.ha_calls.lock().unwrap().is_empty());
    assert!(no_config.sync_calls.lock().unwrap().is_empty());
    assert_eq!(*no_config.cluster_calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn sync_state_follows_only_a_uniquely_configured_leader_endpoint() {
    let controllers = vec![
        ControllerObservationTarget::new("controller-a", CONTROLLER_A),
        ControllerObservationTarget::new("controller-b", CONTROLLER_B),
    ];
    let source = FakeSource {
        cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A), (1, ADDRESS_A_SLAVE)])]),
        metadata: BTreeMap::from([(
            CONTROLLER_B.to_string(),
            Ok(controller_metadata(CONTROLLER_A, false, &[CONTROLLER_A, CONTROLLER_B])),
        )]),
        sync: BTreeMap::from([(
            CONTROLLER_A.to_string(),
            Ok(serde_json::to_value(valid_sync_state()).unwrap()),
        )]),
        ..Default::default()
    };
    let request = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), true, ["controller-b".to_string()]).unwrap();
    let result = query_ha_status_from(&source, &controllers, &request).await.unwrap();
    assert!(!result.partial);
    assert_eq!(&*source.metadata_calls.lock().unwrap(), &[CONTROLLER_B]);
    assert_eq!(&*source.sync_calls.lock().unwrap(), &[CONTROLLER_A]);

    for advertised in ["private-other-cluster.internal:9878", "not-an-endpoint"] {
        let source = FakeSource {
            cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A), (1, ADDRESS_A_SLAVE)])]),
            metadata: BTreeMap::from([(
                CONTROLLER_B.to_string(),
                Ok(controller_metadata(advertised, false, &[advertised, CONTROLLER_B])),
            )]),
            ..Default::default()
        };
        let result = query_ha_status_from(&source, &controllers, &request).await.unwrap();
        assert!(result.partial);
        assert!(source.sync_calls.lock().unwrap().is_empty());
        assert!(!serde_json::to_string(&result).unwrap().contains(advertised));
    }

    let ambiguous = vec![
        ControllerObservationTarget::new("controller-a", "LEADER.internal:9878"),
        ControllerObservationTarget::new("controller-c", "leader.INTERNAL:9878"),
        ControllerObservationTarget::new("controller-b", CONTROLLER_B),
    ];
    let source = FakeSource {
        cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A), (1, ADDRESS_A_SLAVE)])]),
        metadata: BTreeMap::from([(
            CONTROLLER_B.to_string(),
            Ok(controller_metadata(
                "leader.internal:9878",
                false,
                &["leader.internal:9878", CONTROLLER_B],
            )),
        )]),
        ..Default::default()
    };
    let result = query_ha_status_from(&source, &ambiguous, &request).await.unwrap();
    assert!(result.partial);
    assert!(source.sync_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sync_routing_rejects_incomplete_controller_metadata_before_second_rpc() {
    let controllers = vec![
        ControllerObservationTarget::new("controller-a", CONTROLLER_A),
        ControllerObservationTarget::new("controller-b", CONTROLLER_B),
    ];
    let request = QueryHaStatusRequest::try_new(CLUSTER, Vec::new(), true, ["controller-b".to_string()]).unwrap();
    let valid_peers = format!("{CONTROLLER_A};{CONTROLLER_B}");
    let too_many_peers = (1..=MAX_CONTROLLER_PEERS + 1)
        .map(|index| format!("10.1.0.{index}:9878"))
        .collect::<Vec<_>>()
        .join(";");
    let headers = [
        GetMetaDataResponseHeader {
            peers: Some(valid_peers.clone().into()),
            ..Default::default()
        },
        controller_metadata(CONTROLLER_A, true, &[CONTROLLER_A, CONTROLLER_B]),
        GetMetaDataResponseHeader {
            peers: Some(format!("{CONTROLLER_A},{CONTROLLER_B}").into()),
            ..controller_metadata(CONTROLLER_A, false, &[CONTROLLER_A, CONTROLLER_B])
        },
        GetMetaDataResponseHeader {
            peers: Some(format!("{CONTROLLER_A};{CONTROLLER_A}").into()),
            ..controller_metadata(CONTROLLER_A, false, &[CONTROLLER_A, CONTROLLER_B])
        },
        GetMetaDataResponseHeader {
            peers: Some(too_many_peers.into()),
            ..controller_metadata(CONTROLLER_A, false, &[CONTROLLER_A, CONTROLLER_B])
        },
        GetMetaDataResponseHeader {
            peers: Some(CONTROLLER_B.into()),
            ..controller_metadata(CONTROLLER_A, false, &[CONTROLLER_A, CONTROLLER_B])
        },
        GetMetaDataResponseHeader {
            last_log_index: Some(1),
            committed_log_index: Some(2),
            applied_log_index: Some(1),
            ..controller_metadata(CONTROLLER_A, false, &[CONTROLLER_A, CONTROLLER_B])
        },
    ];
    for header in headers {
        let source = FakeSource {
            cluster_info: topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A)])]),
            metadata: BTreeMap::from([(CONTROLLER_B.to_string(), Ok(header))]),
            ..Default::default()
        };
        let result = query_ha_status_from(&source, &controllers, &request).await.unwrap();
        assert!(result.partial);
        assert!(source.sync_calls.lock().unwrap().is_empty());
        let wire = serde_json::to_string(&result).unwrap();
        assert!(!wire.contains(CONTROLLER_A));
        assert!(!wire.contains(CONTROLLER_B));
    }
}

#[test]
fn sync_projection_requires_exact_complete_canonical_replica_sets() {
    let topology = resolve_ha_topology(
        &topology(&[(CLUSTER, "broker-a", &[(0, ADDRESS_A), (1, ADDRESS_A_SLAVE)])]),
        CLUSTER,
        &[],
    )
    .unwrap();
    let selected = BTreeSet::from(["broker-a".to_string()]);
    assert!(project_sync_state("controller-a", valid_sync_state(), &selected, &topology.identities).is_some());
    assert!(project_sync_state(
        "controller-a",
        BrokerReplicasInfo::new(),
        &selected,
        &topology.identities
    )
    .is_none());

    let mut extra = valid_sync_state();
    extra.add_replica_info(
        "broker-extra".into(),
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            1,
            1,
            vec![ReplicaIdentity::new("broker-extra", 0, ADDRESS_A)],
            Vec::new(),
        ),
    );
    assert!(project_sync_state("controller-a", extra, &selected, &topology.identities).is_none());

    for replicas in [
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            0,
            1,
            vec![ReplicaIdentity::new("broker-a", 0, ADDRESS_A)],
            Vec::new(),
        ),
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            1,
            0,
            vec![ReplicaIdentity::new("broker-a", 0, ADDRESS_A)],
            Vec::new(),
        ),
        ReplicasInfo::new(
            u64::MAX,
            ADDRESS_A,
            1,
            1,
            vec![ReplicaIdentity::new("broker-a", u64::MAX, ADDRESS_A)],
            Vec::new(),
        ),
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            1,
            1,
            vec![ReplicaIdentity::new("other-broker", 0, ADDRESS_A)],
            Vec::new(),
        ),
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            1,
            1,
            vec![ReplicaIdentity::new("broker-a", 1, ADDRESS_A_SLAVE)],
            Vec::new(),
        ),
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            1,
            1,
            vec![ReplicaIdentity::new("broker-a", 0, ADDRESS_A)],
            vec![ReplicaIdentity::new("broker-a", 0, ADDRESS_A)],
        ),
    ] {
        assert!(project_sync_state(
            "controller-a",
            sync_state_from(replicas),
            &selected,
            &topology.identities
        )
        .is_none());
    }

    let (exact, identities) = bounded_sync_state(MAX_SYNC_REPLICAS_PER_BROKER);
    assert!(project_sync_state("controller-a", exact, &selected, &identities).is_some());
    let (overflow, identities) = bounded_sync_state(MAX_SYNC_REPLICAS_PER_BROKER + 1);
    assert!(project_sync_state("controller-a", overflow, &selected, &identities).is_none());

    let (exact, selected, identities) = bounded_sync_broker_table(MAX_SYNC_BROKERS);
    assert!(project_sync_state("controller-a", exact, &selected, &identities).is_some());
    let (overflow, selected, identities) = bounded_sync_broker_table(MAX_SYNC_BROKERS + 1);
    assert!(project_sync_state("controller-a", overflow, &selected, &identities).is_none());
}

#[tokio::test]
async fn controller_selection_metadata_projection_and_failures_are_closed() {
    let controllers = vec![
        ControllerObservationTarget::new("controller-b", CONTROLLER_B),
        ControllerObservationTarget::new("controller-a", CONTROLLER_A),
    ];
    let source = FakeSource {
        metadata: BTreeMap::from([
            (
                CONTROLLER_A.to_string(),
                Ok(GetMetaDataResponseHeader {
                    group: Some("group-a".into()),
                    controller_leader_id: Some("1".into()),
                    controller_leader_address: Some(CONTROLLER_A.into()),
                    is_leader: Some(true),
                    peers: Some(format!("{CONTROLLER_A};{CONTROLLER_B}").into()),
                    last_log_index: Some(3),
                    committed_log_index: Some(2),
                    applied_log_index: Some(1),
                }),
            ),
            (CONTROLLER_B.to_string(), Err(unavailable())),
        ]),
        ..Default::default()
    };
    let request = QueryControllerMetadataRequest::try_new(CLUSTER, Vec::new()).unwrap();
    let result = query_controller_metadata_from(&source, &controllers, &request)
        .await
        .unwrap();
    assert!(result.partial);
    assert_eq!(result.data.controllers[0].peer_count, Some(2));
    assert_eq!(result.data.controllers[0].last_log_index, Some(3));
    let wire = serde_json::to_string(&result).unwrap();
    assert!(!wire.contains("secret-leader"));
    assert!(!wire.contains(CONTROLLER_A));

    let unknown = QueryControllerMetadataRequest::try_new(CLUSTER, ["controller-z".to_string()]).unwrap();
    let source = FakeSource::default();
    assert!(query_controller_metadata_from(&source, &controllers, &unknown)
        .await
        .is_err());
    assert!(source.metadata_calls.lock().unwrap().is_empty());

    let total = QueryControllerMetadataRequest::try_new(CLUSTER, ["controller-b".to_string()]).unwrap();
    assert!(
        query_controller_metadata_from(&source_with_failure(), &controllers, &total)
            .await
            .is_err()
    );
}

#[test]
fn controller_metadata_validates_real_peer_syntax_optional_groups_and_log_progress() {
    let valid = validate_controller_metadata(
        CONTROLLER_A,
        controller_metadata(CONTROLLER_A, true, &[CONTROLLER_A, CONTROLLER_B]),
    )
    .unwrap()
    .into_observation("controller-a");
    assert_eq!(valid.peer_count, Some(2));
    assert!(validate_controller_metadata(CONTROLLER_A, GetMetaDataResponseHeader::default()).is_some());

    let invalid_headers = [
        GetMetaDataResponseHeader {
            peers: Some(format!("{CONTROLLER_A},{CONTROLLER_B}").into()),
            ..Default::default()
        },
        GetMetaDataResponseHeader {
            peers: Some("[2001:0db8:0:0:0:0:0:1]:9878;[2001:db8::1]:9878".into()),
            ..Default::default()
        },
        GetMetaDataResponseHeader {
            controller_leader_id: Some("1".into()),
            ..Default::default()
        },
        GetMetaDataResponseHeader {
            last_log_index: Some(1),
            committed_log_index: Some(2),
            applied_log_index: Some(1),
            ..Default::default()
        },
        GetMetaDataResponseHeader {
            committed_log_index: Some(1),
            ..Default::default()
        },
        GetMetaDataResponseHeader {
            last_log_index: Some(u64::MAX),
            ..Default::default()
        },
    ];
    for header in invalid_headers {
        assert!(validate_controller_metadata(CONTROLLER_A, header).is_none());
    }

    let exact = (1..=MAX_CONTROLLER_PEERS)
        .map(|index| format!("10.0.1.{index}:9878"))
        .collect::<Vec<_>>()
        .join(";");
    let exact = validate_controller_metadata(
        CONTROLLER_A,
        GetMetaDataResponseHeader {
            peers: Some(exact.into()),
            ..Default::default()
        },
    )
    .unwrap()
    .into_observation("controller-a");
    assert_eq!(exact.peer_count, Some(MAX_CONTROLLER_PEERS));
    let too_many = (1..=MAX_CONTROLLER_PEERS + 1)
        .map(|index| format!("10.0.2.{index}:9878"))
        .collect::<Vec<_>>()
        .join(";");
    assert!(validate_controller_metadata(
        CONTROLLER_A,
        GetMetaDataResponseHeader {
            peers: Some(too_many.into()),
            ..Default::default()
        }
    )
    .is_none());
    assert!(validate_controller_metadata(
        CONTROLLER_A,
        GetMetaDataResponseHeader {
            peers: Some(";".repeat(MAX_CONTROLLER_PEERS_BYTES + 1).into()),
            ..Default::default()
        }
    )
    .is_none());
}

#[tokio::test]
async fn nameserver_alias_allowlist_differences_and_failure_semantics_are_stable() {
    let endpoints = parse_nameserver_endpoints("namesrv-b.internal:9876;namesrv-a.internal:9876").unwrap();
    let source = FakeSource {
        nameserver: BTreeMap::from([
            (
                "namesrv-a.internal:9876".to_string(),
                Ok(HashMap::from([
                    ("clusterTest".into(), "false".into()),
                    ("clientRequestThreadPoolNums".into(), "8".into()),
                    ("rocketmqHome".into(), "/private/path".into()),
                    ("secretToken".into(), "do-not-return".into()),
                ])),
            ),
            (
                "namesrv-b.internal:9876".to_string(),
                Ok(HashMap::from([
                    ("clusterTest".into(), "true".into()),
                    ("clientRequestThreadPoolNums".into(), "8".into()),
                ])),
            ),
        ]),
        ..Default::default()
    };
    let request = QueryNameserverConfigSummaryRequest::try_new(CLUSTER).unwrap();
    let result = query_nameserver_config_summary_from(&source, &endpoints, &request)
        .await
        .unwrap();
    assert_eq!(result.data.nameservers[0].nameserver_name, "nameserver-1");
    assert_eq!(result.data.nameservers[1].nameserver_name, "nameserver-2");
    assert_eq!(
        result.data.inconsistent_fields,
        [NameserverConfigDifferenceField::ClusterTest]
    );
    let wire = serde_json::to_string(&result).unwrap();
    assert!(!wire.contains("private/path"));
    assert!(!wire.contains("do-not-return"));
    assert!(!wire.contains("namesrv-a.internal"));

    let invalid = FakeSource {
        nameserver: BTreeMap::from([(
            "namesrv-a.internal:9876".to_string(),
            Ok(HashMap::from([("clusterTest".into(), "not-a-bool".into())])),
        )]),
        ..Default::default()
    };
    let one = parse_nameserver_endpoints("namesrv-a.internal:9876").unwrap();
    assert!(query_nameserver_config_summary_from(&invalid, &one, &request)
        .await
        .is_err());

    let partial = FakeSource {
        nameserver: BTreeMap::from([
            ("namesrv-a.internal:9876".to_string(), Ok(HashMap::new())),
            ("namesrv-b.internal:9876".to_string(), Err(unavailable())),
        ]),
        ..Default::default()
    };
    assert!(
        query_nameserver_config_summary_from(&partial, &endpoints, &request)
            .await
            .unwrap()
            .partial
    );
}

#[test]
fn nameserver_allowlist_matches_runtime_types_and_exact_ranges() {
    for (key, valid, invalid) in [
        (
            "clientRequestThreadPoolNums",
            vec!["1", "4096"],
            vec!["-1", "0", "4097", "2147483648", "4294967296"],
        ),
        (
            "clientRequestThreadPoolQueueCapacity",
            vec!["1", "10000000"],
            vec!["-1", "0", "10000001", "2147483648", "4294967296"],
        ),
        (
            "unRegisterBrokerQueueCapacity",
            vec!["1", "10000000"],
            vec!["-1", "0", "10000001", "2147483648", "4294967296"],
        ),
        (
            "scanNotActiveBrokerInterval",
            vec!["1", "3600000"],
            vec!["-1", "0", "3600001", "18446744073709551616"],
        ),
    ] {
        for value in valid {
            assert!(parse_nameserver_config(&HashMap::from([(key.into(), value.into())])).is_some());
        }
        for value in invalid {
            assert!(parse_nameserver_config(&HashMap::from([(key.into(), value.into())])).is_none());
        }
    }
    for key in [
        "clusterTest",
        "orderMessageEnable",
        "returnOrderTopicConfigToBroker",
        "supportActingMaster",
    ] {
        for value in ["true", "false"] {
            assert!(parse_nameserver_config(&HashMap::from([(key.into(), value.into())])).is_some());
        }
        for value in ["TRUE", "1", " true", "false "] {
            assert!(parse_nameserver_config(&HashMap::from([(key.into(), value.into())])).is_none());
        }
    }
    let ignored = parse_nameserver_config(&HashMap::from([
        ("rocketmqHome".into(), "/private/path".into()),
        ("authToken".into(), "secret".into()),
        ("version".into(), "future".into()),
    ]))
    .unwrap();
    assert_eq!(ignored, NameserverConfigValues::default());
}

#[test]
fn configured_target_caps_and_debug_redact_endpoints() {
    let controllers = (0..MAX_CONTROLLER_TARGETS)
        .map(|index| {
            ControllerObservationTarget::new(format!("controller-{index}"), format!("c-{index}.internal:9878"))
        })
        .collect();
    assert_eq!(
        validate_controller_targets(controllers).unwrap().len(),
        MAX_CONTROLLER_TARGETS
    );
    let overflow = (0..=MAX_CONTROLLER_TARGETS)
        .map(|index| {
            ControllerObservationTarget::new(format!("controller-{index}"), format!("c-{index}.internal:9878"))
        })
        .collect();
    assert!(validate_controller_targets(overflow).is_err());
    assert!(parse_nameserver_endpoints(
        &(0..MAX_NAMESERVER_TARGETS)
            .map(|index| format!("n-{index}.internal:9876"))
            .collect::<Vec<_>>()
            .join(";")
    )
    .is_ok());
    assert!(parse_nameserver_endpoints(
        &(0..=MAX_NAMESERVER_TARGETS)
            .map(|index| format!("n-{index}.internal:9876"))
            .collect::<Vec<_>>()
            .join(";")
    )
    .is_err());
    let debug = format!("{:?}", ControllerObservationTarget::new("controller-a", CONTROLLER_A));
    assert!(!debug.contains(CONTROLLER_A));
    let debug = format!(
        "{:?}",
        crate::core::admin::AdminBuilder::new().namesrv_addr("private-nameserver.internal:9876")
    );
    assert!(!debug.contains("private-nameserver"));
}

fn topology(entries: &[TopologyEntry<'_>]) -> ClusterInfo {
    let mut broker_table = HashMap::new();
    let mut cluster_table = HashMap::<CheetahString, std::collections::HashSet<CheetahString>>::new();
    for (cluster, broker_name, endpoints) in entries {
        let broker = BrokerData::new(
            (*cluster).into(),
            (*broker_name).into(),
            endpoints
                .iter()
                .map(|(id, endpoint)| (*id, CheetahString::from(*endpoint)))
                .collect(),
            None,
        );
        broker_table.insert((*broker_name).into(), broker);
        cluster_table
            .entry((*cluster).into())
            .or_default()
            .insert((*broker_name).into());
    }
    ClusterInfo::new(Some(broker_table), Some(cluster_table))
}

fn topology_owned(count: usize) -> ClusterInfo {
    let mut broker_table = HashMap::new();
    let mut members = std::collections::HashSet::new();
    for index in 0..count {
        let name = format!("broker-{index:02}");
        members.insert(CheetahString::from(name.clone()));
        broker_table.insert(
            CheetahString::from(name.clone()),
            BrokerData::new(
                CLUSTER.into(),
                name.into(),
                HashMap::from([(0, CheetahString::from(format!("b-{index}.internal:10911")))]),
                None,
            ),
        );
    }
    ClusterInfo::new(Some(broker_table), Some(HashMap::from([(CLUSTER.into(), members)])))
}

fn topology_with_instances(count: usize) -> ClusterInfo {
    let endpoints = (0..count)
        .map(|index| {
            (
                u64::try_from(index).unwrap(),
                CheetahString::from(format!("b-{index}.internal:10911")),
            )
        })
        .collect();
    ClusterInfo::new(
        Some(HashMap::from([(
            "broker-a".into(),
            BrokerData::new(CLUSTER.into(), "broker-a".into(), endpoints, None),
        )])),
        Some(HashMap::from([(
            CLUSTER.into(),
            std::collections::HashSet::from(["broker-a".into()]),
        )])),
    )
}

fn budget_ha_source(last_connection_count: usize) -> FakeSource {
    let mut broker_table = HashMap::new();
    let mut members = std::collections::HashSet::new();
    let mut ha = BTreeMap::new();
    for broker_index in 0..16 {
        let broker_name = format!("broker-{broker_index:02}");
        let master = format!("budget-{broker_index}-0.internal:10911");
        let connection_count = if broker_index == 15 {
            last_connection_count
        } else {
            MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER - 1
        };
        let mut addresses = HashMap::from([(0, CheetahString::from(master.clone()))]);
        let mut connections = Vec::new();
        for replica_index in 1..=connection_count {
            let endpoint = format!("budget-{broker_index}-{replica_index}.internal:10911");
            addresses.insert(u64::try_from(replica_index).unwrap(), endpoint.clone().into());
            connections.push(HAConnectionRuntimeInfo {
                addr: endpoint,
                slave_ack_offset: 90,
                diff: 10,
                in_sync: true,
                transferred_byte_in_second: 1,
                transfer_from_where: 80,
            });
        }
        members.insert(CheetahString::from(broker_name.clone()));
        broker_table.insert(
            CheetahString::from(broker_name.clone()),
            BrokerData::new(CLUSTER.into(), broker_name.into(), addresses, None),
        );
        ha.insert(master, Ok(master_runtime(connections)));
    }
    FakeSource {
        cluster_info: ClusterInfo::new(Some(broker_table), Some(HashMap::from([(CLUSTER.into(), members)]))),
        ha,
        ..Default::default()
    }
}

fn oversized_sync_source(broker_count: usize, replica_count: usize) -> FakeSource {
    let mut broker_table = HashMap::new();
    let mut members = std::collections::HashSet::new();
    let mut ha = BTreeMap::new();
    let mut sync_state = BrokerReplicasInfo::new();
    for broker_index in 0..broker_count {
        let broker_name = format!("sync-budget-{broker_index:02}");
        let mut addresses = HashMap::new();
        let mut replicas = Vec::new();
        for replica_index in 0..replica_count {
            let broker_id = u64::try_from(replica_index).unwrap();
            let endpoint = format!("sync-budget-{broker_index}-{replica_index}.internal:10911");
            addresses.insert(broker_id, CheetahString::from(endpoint.clone()));
            replicas.push(ReplicaIdentity::new(&broker_name, broker_id, endpoint));
        }
        let master = addresses.get(&0).unwrap().clone();
        let mut invalid_runtime = master_runtime(Vec::new());
        invalid_runtime.master = false;
        ha.insert(master.to_string(), Ok(invalid_runtime));
        members.insert(CheetahString::from(broker_name.clone()));
        broker_table.insert(
            CheetahString::from(broker_name.clone()),
            BrokerData::new(CLUSTER.into(), broker_name.clone().into(), addresses, None),
        );
        sync_state.add_replica_info(
            broker_name.clone().into(),
            ReplicasInfo::new(0, master, 1, 1, replicas, Vec::new()),
        );
    }
    FakeSource {
        cluster_info: ClusterInfo::new(Some(broker_table), Some(HashMap::from([(CLUSTER.into(), members)]))),
        ha,
        sync: BTreeMap::from([(CONTROLLER_A.to_string(), Ok(serde_json::to_value(sync_state).unwrap()))]),
        metadata: BTreeMap::from([(
            CONTROLLER_A.to_string(),
            Ok(controller_metadata(CONTROLLER_A, true, &[CONTROLLER_A])),
        )]),
        ..Default::default()
    }
}

fn ha_result_rows(result: &QueryHaStatusResult) -> usize {
    result
        .brokers
        .iter()
        .fold(0usize, |rows, broker| rows + 1 + broker.connections.len())
        + result
            .controller_sync_states
            .iter()
            .map(controller_sync_state_rows)
            .sum::<usize>()
}

fn valid_connections(
    count: usize,
) -> (
    Vec<HAConnectionRuntimeInfo>,
    BTreeMap<String, Option<LogicalBrokerInstance>>,
) {
    let mut connections = Vec::new();
    let mut identities = BTreeMap::new();
    for index in 0..count {
        let endpoint = format!("replica-{index}.internal:10911");
        connections.push(HAConnectionRuntimeInfo {
            addr: endpoint.clone(),
            slave_ack_offset: 90,
            diff: 10,
            in_sync: true,
            transferred_byte_in_second: 1_000,
            transfer_from_where: 80,
        });
        identities.insert(
            endpoint,
            Some(LogicalBrokerInstance {
                broker_name: "broker-a".to_string(),
                broker_id: u64::try_from(index + 1).unwrap(),
            }),
        );
    }
    (connections, identities)
}

fn master_runtime(connections: Vec<HAConnectionRuntimeInfo>) -> HARuntimeInfo {
    let connections = connections
        .into_iter()
        .map(|mut connection| {
            if connection.slave_ack_offset == 0 && connection.diff == 0 {
                connection.slave_ack_offset = 100;
            }
            connection
        })
        .collect::<Vec<_>>();
    HARuntimeInfo {
        master: true,
        master_commit_log_max_offset: 100,
        in_sync_slave_nums: i32::try_from(connections.iter().filter(|connection| connection.in_sync).count()).unwrap(),
        pending_group_transfer_request_count: 1,
        pending_group_transfer_oldest_wait_millis: 2,
        group_transfer_ack_notify_count: 3,
        ha_connection_info: connections,
        ..Default::default()
    }
}

fn valid_sync_state() -> BrokerReplicasInfo {
    let mut state = BrokerReplicasInfo::new();
    state.add_replica_info(
        "broker-a".into(),
        ReplicasInfo::new(
            0,
            ADDRESS_A,
            7,
            9,
            vec![ReplicaIdentity::new_with_alive("broker-a", 0, ADDRESS_A, true)],
            vec![ReplicaIdentity::new_with_alive("broker-a", 1, ADDRESS_A_SLAVE, true)],
        ),
    );
    state
}

fn sync_state_from(replicas: ReplicasInfo) -> BrokerReplicasInfo {
    let mut state = BrokerReplicasInfo::new();
    state.add_replica_info("broker-a".into(), replicas);
    state
}

fn bounded_sync_state(count: usize) -> (BrokerReplicasInfo, BTreeMap<String, Option<LogicalBrokerInstance>>) {
    let mut identities = BTreeMap::new();
    let mut replicas = Vec::new();
    for index in 0..count {
        let broker_id = u64::try_from(index).unwrap();
        let endpoint = format!("sync-{index}.internal:10911");
        replicas.push(ReplicaIdentity::new("broker-a", broker_id, endpoint.clone()));
        identities.insert(
            endpoint,
            Some(LogicalBrokerInstance {
                broker_name: "broker-a".to_string(),
                broker_id,
            }),
        );
    }
    let master_endpoint = "sync-0.internal:10911";
    (
        sync_state_from(ReplicasInfo::new(0, master_endpoint, 1, 1, replicas, Vec::new())),
        identities,
    )
}

fn bounded_sync_broker_table(
    count: usize,
) -> (
    BrokerReplicasInfo,
    BTreeSet<String>,
    BTreeMap<String, Option<LogicalBrokerInstance>>,
) {
    let mut state = BrokerReplicasInfo::new();
    let mut selected = BTreeSet::new();
    let mut identities = BTreeMap::new();
    for index in 0..count {
        let broker_name = format!("broker-{index:02}");
        let endpoint = format!("sync-broker-{index}.internal:10911");
        selected.insert(broker_name.clone());
        identities.insert(
            endpoint.clone(),
            Some(LogicalBrokerInstance {
                broker_name: broker_name.clone(),
                broker_id: 0,
            }),
        );
        state.add_replica_info(
            broker_name.clone().into(),
            ReplicasInfo::new(
                0,
                endpoint.clone(),
                1,
                1,
                vec![ReplicaIdentity::new(broker_name, 0, endpoint)],
                Vec::new(),
            ),
        );
    }
    (state, selected, identities)
}

fn controller_metadata(leader_endpoint: &str, is_leader: bool, peers: &[&str]) -> GetMetaDataResponseHeader {
    GetMetaDataResponseHeader {
        controller_leader_id: Some("1".into()),
        controller_leader_address: Some(leader_endpoint.into()),
        is_leader: Some(is_leader),
        peers: Some(peers.join(";").into()),
        last_log_index: Some(3),
        committed_log_index: Some(2),
        applied_log_index: Some(1),
        ..Default::default()
    }
}

const fn unavailable() -> InfrastructureObservationReadError {
    InfrastructureObservationReadError::new(InfrastructureObservationReadErrorCode::SourceUnavailable, true)
}

fn source_with_failure() -> FakeSource {
    FakeSource {
        metadata: BTreeMap::from([(CONTROLLER_B.to_string(), Err(unavailable()))]),
        ..Default::default()
    }
}
