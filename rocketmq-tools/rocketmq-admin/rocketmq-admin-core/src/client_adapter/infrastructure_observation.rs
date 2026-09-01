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

//! Read-client implementation of address-free infrastructure observations.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::InfrastructureObservationReadError;
use rocketmq_client_rust::InfrastructureObservationReadErrorCode;
use rocketmq_client_rust::MQAdminInfrastructureObservationReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_protocol::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;

use crate::core::broker::canonical_remoting_endpoint;
use crate::core::infrastructure_observation::BrokerHaObservation;
use crate::core::infrastructure_observation::BrokerSyncStateObservation;
use crate::core::infrastructure_observation::ControllerMetadataObservation;
use crate::core::infrastructure_observation::ControllerSyncStateObservation;
use crate::core::infrastructure_observation::HaConnectionObservation;
use crate::core::infrastructure_observation::LogicalBrokerInstance;
use crate::core::infrastructure_observation::NameserverConfigDifferenceField;
use crate::core::infrastructure_observation::NameserverConfigObservation;
use crate::core::infrastructure_observation::NameserverConfigValues;
use crate::core::infrastructure_observation::QueryControllerMetadataRequest;
use crate::core::infrastructure_observation::QueryControllerMetadataResult;
use crate::core::infrastructure_observation::QueryHaStatusRequest;
use crate::core::infrastructure_observation::QueryHaStatusResult;
use crate::core::infrastructure_observation::QueryNameserverConfigSummaryRequest;
use crate::core::infrastructure_observation::QueryNameserverConfigSummaryResult;
use crate::core::infrastructure_observation::MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER;
use crate::core::infrastructure_observation::MAX_CONTROLLER_PEERS;
use crate::core::infrastructure_observation::MAX_CONTROLLER_TARGETS;
use crate::core::infrastructure_observation::MAX_HA_BROKER_TARGETS;
use crate::core::infrastructure_observation::MAX_HA_CONNECTIONS_PER_BROKER;
use crate::core::infrastructure_observation::MAX_INFRASTRUCTURE_QUERY_ROWS;
use crate::core::infrastructure_observation::MAX_NAMESERVER_TARGETS;
use crate::core::infrastructure_observation::MAX_SYNC_BROKERS;
use crate::core::infrastructure_observation::MAX_SYNC_REPLICAS_PER_BROKER;
use crate::core::query::AdminQueryFailureCode;
use crate::core::query::AdminQueryResult;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::AdminError;
use crate::core::AdminResult;
use crate::read_client_adapter::ControllerObservationTarget;

type ObservationResult<T> = Result<T, InfrastructureObservationReadError>;

const MAX_NAMESERVER_THREAD_COUNT: i32 = 4_096;
const MAX_NAMESERVER_QUEUE_CAPACITY: i32 = 10_000_000;
const MAX_NAMESERVER_SCAN_INTERVAL_MILLIS: u64 = 3_600_000;
const MAX_WIRE_I64: u64 = i64::MAX as u64;
const MAX_REMOTING_ENDPOINT_BYTES: usize = 512;
const MAX_CONTROLLER_PEERS_BYTES: usize =
    MAX_CONTROLLER_PEERS * MAX_REMOTING_ENDPOINT_BYTES + (MAX_CONTROLLER_PEERS - 1);

#[derive(Debug)]
struct QueryRowBudget {
    remaining: usize,
}

impl QueryRowBudget {
    const fn new() -> Self {
        Self {
            remaining: MAX_INFRASTRUCTURE_QUERY_ROWS,
        }
    }

    fn reserve_source(&mut self, rows: usize) -> bool {
        let Some(remaining) = self.remaining.checked_sub(rows) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

#[allow(async_fn_in_trait)]
trait InfrastructureObservationSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError>;
    async fn ha_runtime(&self, endpoint: CheetahString) -> ObservationResult<HARuntimeInfo>;
    async fn sync_state(
        &self,
        endpoint: CheetahString,
        broker_names: Vec<CheetahString>,
    ) -> ObservationResult<BrokerReplicasInfo>;
    async fn controller_metadata(&self, endpoint: CheetahString) -> ObservationResult<GetMetaDataResponseHeader>;
    async fn nameserver_config(
        &self,
        endpoint: CheetahString,
    ) -> ObservationResult<HashMap<CheetahString, CheetahString>>;
}

impl InfrastructureObservationSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, RocketMQError> {
        MQAdminReadExt::examine_broker_cluster_info(self).await
    }

    async fn ha_runtime(&self, endpoint: CheetahString) -> ObservationResult<HARuntimeInfo> {
        MQAdminInfrastructureObservationReadExt::broker_ha_runtime_at(self, endpoint).await
    }

    async fn sync_state(
        &self,
        endpoint: CheetahString,
        broker_names: Vec<CheetahString>,
    ) -> ObservationResult<BrokerReplicasInfo> {
        MQAdminInfrastructureObservationReadExt::controller_sync_state_at(self, endpoint, broker_names).await
    }

    async fn controller_metadata(&self, endpoint: CheetahString) -> ObservationResult<GetMetaDataResponseHeader> {
        MQAdminInfrastructureObservationReadExt::controller_metadata_at(self, endpoint).await
    }

    async fn nameserver_config(
        &self,
        endpoint: CheetahString,
    ) -> ObservationResult<HashMap<CheetahString, CheetahString>> {
        MQAdminInfrastructureObservationReadExt::nameserver_config_at(self, endpoint).await
    }
}

pub(crate) fn validate_controller_targets(
    mut targets: Vec<ControllerObservationTarget>,
) -> AdminResult<Vec<ControllerObservationTarget>> {
    if targets.len() > MAX_CONTROLLER_TARGETS {
        return Err(AdminError::invalid_argument(
            "controller_targets",
            format!("must contain at most {MAX_CONTROLLER_TARGETS} configured Controllers"),
        ));
    }
    targets.sort_by(|left, right| left.name().cmp(right.name()));
    let mut names = BTreeSet::new();
    let mut endpoints = BTreeSet::new();
    for target in &targets {
        if !safe_logical_identifier(target.name()) {
            return Err(AdminError::invalid_argument(
                "controller_targets",
                "Controller names must be bounded logical aliases",
            ));
        }
        let Some(endpoint) = canonical_remoting_endpoint(target.endpoint()) else {
            return Err(AdminError::invalid_argument(
                "controller_targets",
                "Controller endpoints must be bounded remoting endpoints",
            ));
        };
        if !names.insert(target.name()) || !endpoints.insert(endpoint) {
            return Err(AdminError::invalid_argument(
                "controller_targets",
                "Controller names and endpoints must be unique",
            ));
        }
    }
    Ok(targets)
}

pub(crate) fn parse_nameserver_endpoints(value: &str) -> AdminResult<Vec<CheetahString>> {
    let mut endpoints = value
        .split(';')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .collect::<Vec<_>>();
    if endpoints.len() > MAX_NAMESERVER_TARGETS {
        return Err(AdminError::invalid_argument(
            "namesrv_addr",
            format!("must contain at most {MAX_NAMESERVER_TARGETS} configured NameServers"),
        ));
    }
    endpoints.sort_unstable_by_key(|endpoint| canonical_remoting_endpoint(endpoint));
    let canonical = endpoints
        .iter()
        .map(|endpoint| canonical_remoting_endpoint(endpoint))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            AdminError::invalid_argument(
                "namesrv_addr",
                "configured NameServers must be bounded remoting endpoints",
            )
        })?;
    if canonical.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(AdminError::invalid_argument(
            "namesrv_addr",
            "configured NameServer endpoints must be unique",
        ));
    }
    Ok(endpoints.into_iter().map(CheetahString::from).collect())
}

pub(crate) async fn query_ha_status(
    source: &DefaultMQAdminExt,
    controllers: &[ControllerObservationTarget],
    request: &QueryHaStatusRequest,
) -> AdminResult<AdminQueryResult<QueryHaStatusResult>> {
    query_ha_status_from(source, controllers, request).await
}

pub(crate) async fn query_controller_metadata(
    source: &DefaultMQAdminExt,
    controllers: &[ControllerObservationTarget],
    request: &QueryControllerMetadataRequest,
) -> AdminResult<AdminQueryResult<QueryControllerMetadataResult>> {
    query_controller_metadata_from(source, controllers, request).await
}

pub(crate) async fn query_nameserver_config_summary(
    source: &DefaultMQAdminExt,
    nameservers: &[CheetahString],
    request: &QueryNameserverConfigSummaryRequest,
) -> AdminResult<AdminQueryResult<QueryNameserverConfigSummaryResult>> {
    query_nameserver_config_summary_from(source, nameservers, request).await
}

async fn query_ha_status_from<S: InfrastructureObservationSource>(
    source: &S,
    controllers: &[ControllerObservationTarget],
    request: &QueryHaStatusRequest,
) -> AdminResult<AdminQueryResult<QueryHaStatusResult>> {
    let request = QueryHaStatusRequest::try_new(
        &request.cluster,
        request.broker_names.clone(),
        request.include_sync_state,
        request.controller_names.clone(),
    )?;
    let selected_controllers = if request.include_sync_state {
        resolve_controller_targets(controllers, &request.controller_names)?
    } else {
        Vec::new()
    };
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let topology = resolve_ha_topology(&cluster_info, &request.cluster, &request.broker_names)?;

    let mut failures = topology.failures;
    let mut successful_sources = 0usize;
    let mut brokers = Vec::new();
    let mut row_budget = QueryRowBudget::new();
    for target in topology.masters {
        match source.ha_runtime(target.endpoint).await {
            Ok(runtime) => {
                match project_ha_runtime(
                    &target.broker_name,
                    target.broker_id,
                    target.configured_slave_count,
                    runtime,
                    &topology.identities,
                ) {
                    Some(row) => {
                        let rows = 1usize.saturating_add(row.connections.len());
                        if row_budget.reserve_source(rows) {
                            successful_sources += 1;
                            brokers.push(row);
                        } else {
                            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, &target.broker_name));
                        }
                    }
                    None => failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, &target.broker_name)),
                }
            }
            Err(error) => failures.push(observation_failure(
                AdminQuerySource::BrokerHaRuntime,
                &target.broker_name,
                error,
            )),
        }
    }

    let broker_names = topology.selected_broker_names;
    let wire_broker_names = broker_names.iter().map(CheetahString::from).collect::<Vec<_>>();
    let mut controller_sync_states = Vec::new();
    for target in selected_controllers {
        let metadata = match source.controller_metadata(CheetahString::from(target.endpoint())).await {
            Ok(metadata) => metadata,
            Err(error) => {
                failures.push(observation_failure(
                    AdminQuerySource::ControllerSyncState,
                    target.name(),
                    error,
                ));
                continue;
            }
        };
        let Some(validated_metadata) = validate_controller_metadata(target.endpoint(), metadata) else {
            failures.push(invalid_failure(AdminQuerySource::ControllerSyncState, target.name()));
            continue;
        };
        let Some(leader_endpoint) = configured_controller_leader(controllers, &validated_metadata) else {
            failures.push(invalid_failure(AdminQuerySource::ControllerSyncState, target.name()));
            continue;
        };
        match source
            .sync_state(CheetahString::from(leader_endpoint), wire_broker_names.clone())
            .await
        {
            Ok(sync_state) => {
                match project_sync_state(target.name(), sync_state, &broker_names, &topology.identities) {
                    Some(row) => {
                        let rows = controller_sync_state_rows(&row);
                        if row_budget.reserve_source(rows) {
                            successful_sources += 1;
                            controller_sync_states.push(row);
                        } else {
                            failures.push(invalid_failure(AdminQuerySource::ControllerSyncState, target.name()));
                        }
                    }
                    None => failures.push(invalid_failure(AdminQuerySource::ControllerSyncState, target.name())),
                }
            }
            Err(error) => failures.push(observation_failure(
                AdminQuerySource::ControllerSyncState,
                target.name(),
                error,
            )),
        }
    }
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    controller_sync_states.sort_by(|left, right| left.controller_name.cmp(&right.controller_name));
    AdminQueryResult::from_sources(
        QueryHaStatusResult {
            brokers,
            controller_sync_states,
        },
        successful_sources,
        failures,
    )
}

async fn query_controller_metadata_from<S: InfrastructureObservationSource>(
    source: &S,
    controllers: &[ControllerObservationTarget],
    request: &QueryControllerMetadataRequest,
) -> AdminResult<AdminQueryResult<QueryControllerMetadataResult>> {
    let request = QueryControllerMetadataRequest::try_new(&request.cluster, request.controller_names.clone())?;
    let selected = resolve_controller_targets(controllers, &request.controller_names)?;
    let mut successful_sources = 0usize;
    let mut failures = Vec::new();
    let mut rows = Vec::new();
    for target in selected {
        match source.controller_metadata(CheetahString::from(target.endpoint())).await {
            Ok(header) => match validate_controller_metadata(target.endpoint(), header) {
                Some(validated) => {
                    successful_sources += 1;
                    rows.push(validated.into_observation(target.name()));
                }
                None => failures.push(invalid_failure(AdminQuerySource::ControllerMetadata, target.name())),
            },
            Err(error) => failures.push(observation_failure(
                AdminQuerySource::ControllerMetadata,
                target.name(),
                error,
            )),
        }
    }
    rows.sort_by(|left, right| left.controller_name.cmp(&right.controller_name));
    AdminQueryResult::from_sources(
        QueryControllerMetadataResult { controllers: rows },
        successful_sources,
        failures,
    )
}

async fn query_nameserver_config_summary_from<S: InfrastructureObservationSource>(
    source: &S,
    nameservers: &[CheetahString],
    request: &QueryNameserverConfigSummaryRequest,
) -> AdminResult<AdminQueryResult<QueryNameserverConfigSummaryResult>> {
    QueryNameserverConfigSummaryRequest::try_new(&request.cluster)?;
    if nameservers.is_empty() {
        return Err(AdminError::invalid_argument(
            "namesrv_addr",
            "no configured NameServer targets are available",
        ));
    }
    if nameservers.len() > MAX_NAMESERVER_TARGETS {
        return Err(AdminError::invalid_argument(
            "namesrv_addr",
            format!("must contain at most {MAX_NAMESERVER_TARGETS} configured NameServers"),
        ));
    }
    let mut successful_sources = 0usize;
    let mut failures = Vec::new();
    let mut rows = Vec::new();
    for (index, endpoint) in nameservers.iter().enumerate() {
        let name = format!("nameserver-{}", index + 1);
        match source.nameserver_config(endpoint.clone()).await {
            Ok(config) => match parse_nameserver_config(&config) {
                Some(values) => {
                    successful_sources += 1;
                    rows.push(NameserverConfigObservation {
                        nameserver_name: name,
                        values,
                    });
                }
                None => failures.push(invalid_failure(AdminQuerySource::NameserverConfig, &name)),
            },
            Err(error) => failures.push(observation_failure(AdminQuerySource::NameserverConfig, &name, error)),
        }
    }
    let inconsistent_fields = nameserver_differences(&rows);
    AdminQueryResult::from_sources(
        QueryNameserverConfigSummaryResult {
            nameservers: rows,
            inconsistent_fields,
        },
        successful_sources,
        failures,
    )
}

struct HaTopology {
    masters: Vec<HaMasterTarget>,
    identities: BTreeMap<String, Option<LogicalBrokerInstance>>,
    selected_broker_names: BTreeSet<String>,
    failures: Vec<AdminSourceFailure>,
}

struct HaMasterTarget {
    broker_name: String,
    broker_id: u64,
    configured_slave_count: usize,
    endpoint: CheetahString,
}

fn resolve_ha_topology(cluster_info: &ClusterInfo, cluster: &str, selectors: &[String]) -> AdminResult<HaTopology> {
    let membership = cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|table| table.get(cluster))
        .ok_or_else(|| AdminError::not_found("cluster", cluster))?;
    let selected_broker_names = if selectors.is_empty() {
        if membership.is_empty() {
            return Err(AdminError::not_found("cluster", cluster));
        }
        if membership.len() > MAX_HA_BROKER_TARGETS {
            return Err(target_limit_error());
        }
        membership.iter().map(ToString::to_string).collect::<BTreeSet<_>>()
    } else {
        for selector in selectors {
            if !membership.contains(selector.as_str()) {
                return Err(AdminError::not_found("broker", selector));
            }
        }
        selectors.iter().cloned().collect()
    };
    if selected_broker_names.len() > MAX_HA_BROKER_TARGETS {
        return Err(target_limit_error());
    }

    let broker_table = cluster_info.broker_addr_table.as_ref();
    let mut identities = BTreeMap::<String, Option<LogicalBrokerInstance>>::new();
    let mut failures = Vec::new();
    let mut master_candidates = Vec::new();
    for broker_name in &selected_broker_names {
        let Some(broker) = broker_table.and_then(|table| table.get(broker_name.as_str())) else {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, broker_name));
            continue;
        };
        if broker.cluster() != cluster
            || broker.broker_name().as_str() != broker_name
            || !safe_logical_identifier(broker_name)
        {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, broker_name));
            continue;
        }
        if broker.broker_addrs().is_empty() || broker.broker_addrs().len() > MAX_BROKER_INSTANCES_PER_LOGICAL_BROKER {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, broker_name));
            continue;
        }
        let mut local_endpoints = BTreeSet::new();
        let mut local_identities = Vec::with_capacity(broker.broker_addrs().len());
        let mut valid = true;
        for (broker_id, endpoint) in broker.broker_addrs() {
            let Some(canonical) = canonical_remoting_endpoint(endpoint.as_str()) else {
                valid = false;
                break;
            };
            if *broker_id > MAX_WIRE_I64 || !local_endpoints.insert(canonical.clone()) {
                valid = false;
                break;
            }
            local_identities.push((
                canonical,
                LogicalBrokerInstance {
                    broker_name: broker_name.clone(),
                    broker_id: *broker_id,
                },
            ));
        }
        let Some(master_endpoint) = broker.broker_addrs().get(&mix_all::MASTER_ID) else {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, broker_name));
            continue;
        };
        let Some(master_canonical) = canonical_remoting_endpoint(master_endpoint.as_str()) else {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, broker_name));
            continue;
        };
        if !valid {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, broker_name));
            continue;
        }
        for (endpoint, identity) in local_identities {
            match identities.entry(endpoint) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(Some(identity));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
            }
        }
        master_candidates.push((
            broker_name.clone(),
            master_endpoint.clone(),
            master_canonical,
            broker.broker_addrs().len() - 1,
        ));
    }

    let mut masters = Vec::new();
    for (broker_name, endpoint, canonical, configured_slave_count) in master_candidates {
        if !identities
            .get(&canonical)
            .and_then(Option::as_ref)
            .is_some_and(|identity| identity.broker_name == broker_name && identity.broker_id == mix_all::MASTER_ID)
        {
            failures.push(invalid_failure(AdminQuerySource::BrokerHaRuntime, &broker_name));
            continue;
        }
        masters.push(HaMasterTarget {
            broker_name,
            broker_id: mix_all::MASTER_ID,
            configured_slave_count,
            endpoint,
        });
    }
    Ok(HaTopology {
        masters,
        identities,
        selected_broker_names,
        failures,
    })
}

fn target_limit_error() -> AdminError {
    AdminError::backend_view(
        "resolve_ha_targets",
        "HA_OBSERVATION_TARGET_LIMIT_EXCEEDED",
        "selected cluster has too many logical Broker masters",
        None,
        422,
        false,
    )
}

fn resolve_controller_targets<'a>(
    configured: &'a [ControllerObservationTarget],
    selectors: &[String],
) -> AdminResult<Vec<&'a ControllerObservationTarget>> {
    if configured.is_empty() {
        return Err(AdminError::invalid_argument(
            "controller_names",
            "no configured logical Controller aliases are available",
        ));
    }
    if configured.len() > MAX_CONTROLLER_TARGETS {
        return Err(AdminError::invalid_argument(
            "controller_names",
            format!("must contain at most {MAX_CONTROLLER_TARGETS} configured Controllers"),
        ));
    }
    let by_name = configured
        .iter()
        .map(|target| (target.name(), target))
        .collect::<BTreeMap<_, _>>();
    let names = if selectors.is_empty() {
        by_name.keys().map(|name| (*name).to_string()).collect::<Vec<_>>()
    } else {
        selectors.to_vec()
    };
    let mut selected = Vec::with_capacity(names.len());
    for name in names {
        selected.push(
            by_name
                .get(name.as_str())
                .copied()
                .ok_or_else(|| AdminError::not_found("controller", &name))?,
        );
    }
    Ok(selected)
}

fn project_ha_runtime(
    broker_name: &str,
    broker_id: u64,
    configured_slave_count: usize,
    runtime: HARuntimeInfo,
    identities: &BTreeMap<String, Option<LogicalBrokerInstance>>,
) -> Option<BrokerHaObservation> {
    let reported_in_sync = usize::try_from(runtime.in_sync_slave_nums).ok()?;
    let observed_in_sync = runtime
        .ha_connection_info
        .iter()
        .filter(|connection| connection.in_sync)
        .count();
    if !runtime.master
        || runtime.master_commit_log_max_offset > MAX_WIRE_I64
        || reported_in_sync > MAX_HA_CONNECTIONS_PER_BROKER
        || reported_in_sync > configured_slave_count
        || observed_in_sync > reported_in_sync
        || runtime.ha_connection_info.len() > MAX_HA_CONNECTIONS_PER_BROKER
        || runtime.pending_group_transfer_request_count > MAX_WIRE_I64
        || runtime.pending_group_transfer_oldest_wait_millis > MAX_WIRE_I64
        || runtime.group_transfer_ack_notify_count > MAX_WIRE_I64
    {
        return None;
    }
    let mut connections = Vec::new();
    let mut unique_ids = BTreeSet::new();
    let mut unique_endpoints = BTreeSet::new();
    for connection in runtime.ha_connection_info {
        let canonical = canonical_remoting_endpoint(&connection.addr)?;
        let replica = identities.get(&canonical).and_then(Option::as_ref)?.clone();
        let expected_diff = runtime
            .master_commit_log_max_offset
            .checked_sub(connection.slave_ack_offset)?;
        if replica.broker_name != broker_name
            || replica.broker_id == mix_all::MASTER_ID
            || replica.broker_id > MAX_WIRE_I64
            || connection.slave_ack_offset > MAX_WIRE_I64
            || connection.transfer_from_where > MAX_WIRE_I64
            || connection.transferred_byte_in_second > MAX_WIRE_I64
            || connection.slave_ack_offset > runtime.master_commit_log_max_offset
            || connection.transfer_from_where > runtime.master_commit_log_max_offset
            || connection.diff < 0
            || u64::try_from(connection.diff).ok()? != expected_diff
            || !unique_ids.insert(replica.broker_id)
            || !unique_endpoints.insert(canonical)
        {
            return None;
        }
        connections.push(HaConnectionObservation {
            replica,
            slave_ack_offset: connection.slave_ack_offset,
            diff: connection.diff,
            in_sync: connection.in_sync,
            transferred_bytes_per_second: connection.transferred_byte_in_second,
            transfer_from_where: connection.transfer_from_where,
        });
    }
    connections.sort_by(|left, right| {
        left.replica
            .broker_name
            .cmp(&right.replica.broker_name)
            .then(left.replica.broker_id.cmp(&right.replica.broker_id))
    });
    Some(BrokerHaObservation {
        broker_name: broker_name.to_string(),
        broker_id,
        master_commit_log_max_offset: runtime.master_commit_log_max_offset,
        in_sync_slave_count: u32::try_from(reported_in_sync).ok()?,
        pending_group_transfer_request_count: runtime.pending_group_transfer_request_count,
        pending_group_transfer_oldest_wait_millis: runtime.pending_group_transfer_oldest_wait_millis,
        group_transfer_ack_notify_count: runtime.group_transfer_ack_notify_count,
        connections,
    })
}

fn project_sync_state(
    controller_name: &str,
    sync_state: BrokerReplicasInfo,
    selected_brokers: &BTreeSet<String>,
    identities: &BTreeMap<String, Option<LogicalBrokerInstance>>,
) -> Option<ControllerSyncStateObservation> {
    let table = sync_state.get_replicas_info_table();
    if selected_brokers.is_empty()
        || selected_brokers.len() > MAX_SYNC_BROKERS
        || table.len() != selected_brokers.len()
        || table.keys().map(|name| name.as_str()).collect::<BTreeSet<_>>()
            != selected_brokers.iter().map(String::as_str).collect::<BTreeSet<_>>()
    {
        return None;
    }
    let mut brokers = Vec::new();
    for (broker_name, replicas) in table {
        if replicas.get_master_epoch() <= 0
            || replicas.get_sync_state_set_epoch() <= 0
            || replicas.get_master_broker_id() > MAX_WIRE_I64
            || replicas.get_in_sync_replicas().len() > MAX_SYNC_REPLICAS_PER_BROKER
            || replicas.get_not_in_sync_replicas().len() > MAX_SYNC_REPLICAS_PER_BROKER
            || replicas
                .get_in_sync_replicas()
                .len()
                .saturating_add(replicas.get_not_in_sync_replicas().len())
                > MAX_SYNC_REPLICAS_PER_BROKER
        {
            return None;
        }
        let master_endpoint = canonical_remoting_endpoint(replicas.get_master_address())?;
        let master = identities.get(&master_endpoint).and_then(Option::as_ref)?;
        if master.broker_name != broker_name.as_str() || master.broker_id != replicas.get_master_broker_id() {
            return None;
        }
        let mut seen = BTreeSet::new();
        let mut in_sync_replicas = project_replicas(
            replicas.get_in_sync_replicas(),
            identities,
            broker_name.as_str(),
            &mut seen,
        )?;
        if !in_sync_replicas.iter().any(|replica| replica == master) {
            return None;
        }
        let mut not_in_sync_replicas = project_replicas(
            replicas.get_not_in_sync_replicas(),
            identities,
            broker_name.as_str(),
            &mut seen,
        )?;
        in_sync_replicas.sort_by(logical_broker_order);
        not_in_sync_replicas.sort_by(logical_broker_order);
        brokers.push(BrokerSyncStateObservation {
            broker_name: broker_name.to_string(),
            master_broker_id: replicas.get_master_broker_id(),
            master_epoch: replicas.get_master_epoch(),
            sync_state_set_epoch: replicas.get_sync_state_set_epoch(),
            in_sync_replicas,
            not_in_sync_replicas,
        });
    }
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    Some(ControllerSyncStateObservation {
        controller_name: controller_name.to_string(),
        brokers,
    })
}

fn project_replicas(
    replicas: &[rocketmq_protocol::protocol::body::broker_replicas_info::ReplicaIdentity],
    identities: &BTreeMap<String, Option<LogicalBrokerInstance>>,
    broker_name: &str,
    seen: &mut BTreeSet<(String, u64)>,
) -> Option<Vec<LogicalBrokerInstance>> {
    let mut rows = Vec::new();
    for replica in replicas {
        let canonical = canonical_remoting_endpoint(replica.get_broker_address().as_str())?;
        let identity = identities.get(&canonical).and_then(Option::as_ref)?;
        if replica.get_broker_name().as_str() != broker_name
            || identity.broker_name != broker_name
            || identity.broker_name != replica.get_broker_name().as_str()
            || identity.broker_id != replica.get_broker_id()
            || identity.broker_id > MAX_WIRE_I64
            || !seen.insert((identity.broker_name.clone(), identity.broker_id))
        {
            return None;
        }
        rows.push(identity.clone());
    }
    Some(rows)
}

fn logical_broker_order(left: &LogicalBrokerInstance, right: &LogicalBrokerInstance) -> std::cmp::Ordering {
    left.broker_name
        .cmp(&right.broker_name)
        .then(left.broker_id.cmp(&right.broker_id))
}

fn controller_sync_state_rows(state: &ControllerSyncStateObservation) -> usize {
    state.brokers.iter().fold(1usize, |rows, broker| {
        rows.saturating_add(1)
            .saturating_add(broker.in_sync_replicas.len())
            .saturating_add(broker.not_in_sync_replicas.len())
    })
}

fn configured_controller_leader<'a>(
    configured: &'a [ControllerObservationTarget],
    metadata: &ValidatedControllerMetadata,
) -> Option<&'a str> {
    let leader = metadata.leader_endpoint.as_deref()?;
    let mut owners = configured
        .iter()
        .filter(|target| canonical_remoting_endpoint(target.endpoint()).as_deref() == Some(leader));
    let owner = owners.next()?;
    owners.next().is_none().then_some(owner.endpoint())
}

struct ValidatedControllerMetadata {
    group: Option<String>,
    leader_id: Option<String>,
    leader_endpoint: Option<String>,
    is_leader: Option<bool>,
    peer_count: Option<usize>,
    last_log_index: Option<u64>,
    committed_log_index: Option<u64>,
    applied_log_index: Option<u64>,
}

impl ValidatedControllerMetadata {
    fn into_observation(self, controller_name: &str) -> ControllerMetadataObservation {
        ControllerMetadataObservation {
            controller_name: controller_name.to_string(),
            group: self.group,
            leader_id: self.leader_id,
            is_leader: self.is_leader,
            peer_count: self.peer_count,
            last_log_index: self.last_log_index,
            committed_log_index: self.committed_log_index,
            applied_log_index: self.applied_log_index,
        }
    }
}

fn validate_controller_metadata(
    controller_endpoint: &str,
    header: GetMetaDataResponseHeader,
) -> Option<ValidatedControllerMetadata> {
    let group = safe_optional_value(header.group.map(String::from))?;
    let leader_id = match header.controller_leader_id.as_deref() {
        None => None,
        Some(value) => {
            let value = value.parse::<u64>().ok()?;
            Some((value <= MAX_WIRE_I64).then(|| value.to_string())?)
        }
    };
    let leader_endpoint = match header.controller_leader_address.as_deref() {
        None => None,
        Some(endpoint) => Some(canonical_remoting_endpoint(endpoint)?),
    };
    if leader_id.is_some() != leader_endpoint.is_some() || leader_id.is_some() != header.is_leader.is_some() {
        return None;
    }
    let peer_count = validate_controller_peers(header.peers.as_deref(), leader_endpoint.as_deref())?;
    if leader_endpoint.is_some() && peer_count.is_none() {
        return None;
    }
    if let (Some(is_leader), Some(leader)) = (header.is_leader, leader_endpoint.as_deref()) {
        let is_target = canonical_remoting_endpoint(controller_endpoint).as_deref() == Some(leader);
        if is_leader != is_target {
            return None;
        }
    }
    if !valid_log_progress(
        header.last_log_index,
        header.committed_log_index,
        header.applied_log_index,
    ) {
        return None;
    }
    Some(ValidatedControllerMetadata {
        group,
        leader_id,
        leader_endpoint,
        is_leader: header.is_leader,
        peer_count,
        last_log_index: header.last_log_index,
        committed_log_index: header.committed_log_index,
        applied_log_index: header.applied_log_index,
    })
}

fn validate_controller_peers(peers: Option<&str>, leader: Option<&str>) -> Option<Option<usize>> {
    let Some(peers) = peers else {
        return Some(None);
    };
    if peers.is_empty() || peers.len() > MAX_CONTROLLER_PEERS_BYTES {
        return None;
    }
    let mut canonical = BTreeSet::new();
    let mut count = 0usize;
    for peer in peers.split(';').take(MAX_CONTROLLER_PEERS + 1) {
        count = count.saturating_add(1);
        if count > MAX_CONTROLLER_PEERS || peer.is_empty() || peer.trim() != peer {
            return None;
        }
        let peer = peer.parse::<std::net::SocketAddr>().ok()?.to_string();
        if !canonical.insert(peer) {
            return None;
        }
    }
    if canonical.len() != count || leader.is_some_and(|leader| !canonical.contains(leader)) {
        return None;
    }
    Some(Some(count))
}

fn valid_log_progress(last: Option<u64>, committed: Option<u64>, applied: Option<u64>) -> bool {
    if [last, committed, applied]
        .into_iter()
        .flatten()
        .any(|value| value > MAX_WIRE_I64)
    {
        return false;
    }
    match (last, committed, applied) {
        (None, None, None) | (Some(_), None, None) => true,
        (Some(last), Some(committed), None) => last >= committed,
        (Some(last), Some(committed), Some(applied)) => last >= committed && committed >= applied,
        _ => false,
    }
}

fn safe_optional_value(value: Option<String>) -> Option<Option<String>> {
    match value {
        None => Some(None),
        Some(value) => {
            let value = value.trim();
            safe_logical_identifier(value).then(|| Some(value.to_string()))
        }
    }
}

fn parse_nameserver_config(config: &HashMap<CheetahString, CheetahString>) -> Option<NameserverConfigValues> {
    Some(NameserverConfigValues {
        cluster_test: parse_optional_bool(config, "clusterTest")?,
        order_message_enable: parse_optional_bool(config, "orderMessageEnable")?,
        return_order_topic_config_to_broker: parse_optional_bool(config, "returnOrderTopicConfigToBroker")?,
        client_request_thread_pool_nums: parse_optional_bounded_i32(
            config,
            "clientRequestThreadPoolNums",
            MAX_NAMESERVER_THREAD_COUNT,
        )?,
        client_request_thread_pool_queue_capacity: parse_optional_bounded_i32(
            config,
            "clientRequestThreadPoolQueueCapacity",
            MAX_NAMESERVER_QUEUE_CAPACITY,
        )?,
        scan_not_active_broker_interval_ms: parse_optional_bounded_u64(
            config,
            "scanNotActiveBrokerInterval",
            MAX_NAMESERVER_SCAN_INTERVAL_MILLIS,
        )?,
        unregister_broker_queue_capacity: parse_optional_bounded_i32(
            config,
            "unRegisterBrokerQueueCapacity",
            MAX_NAMESERVER_QUEUE_CAPACITY,
        )?,
        support_acting_master: parse_optional_bool(config, "supportActingMaster")?,
    })
}

fn parse_optional_bool(config: &HashMap<CheetahString, CheetahString>, key: &str) -> Option<Option<bool>> {
    match config.get(key) {
        None => Some(None),
        Some(value) => match value.as_str() {
            "true" => Some(Some(true)),
            "false" => Some(Some(false)),
            _ => None,
        },
    }
}

fn parse_optional_bounded_i32(
    config: &HashMap<CheetahString, CheetahString>,
    key: &str,
    maximum: i32,
) -> Option<Option<i32>> {
    match config.get(key) {
        None => Some(None),
        Some(value) => value
            .trim()
            .parse::<i32>()
            .ok()
            .filter(|value| (1..=maximum).contains(value))
            .map(Some),
    }
}

fn parse_optional_bounded_u64(
    config: &HashMap<CheetahString, CheetahString>,
    key: &str,
    maximum: u64,
) -> Option<Option<u64>> {
    match config.get(key) {
        None => Some(None),
        Some(value) => value
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|value| (1..=maximum).contains(value))
            .map(Some),
    }
}

fn nameserver_differences(rows: &[NameserverConfigObservation]) -> Vec<NameserverConfigDifferenceField> {
    let Some(baseline) = rows.first().map(|row| &row.values) else {
        return Vec::new();
    };
    let mut differences = Vec::new();
    macro_rules! difference {
        ($field:ident, $variant:ident) => {
            if rows.iter().any(|row| row.values.$field != baseline.$field) {
                differences.push(NameserverConfigDifferenceField::$variant);
            }
        };
    }
    difference!(cluster_test, ClusterTest);
    difference!(order_message_enable, OrderMessageEnable);
    difference!(return_order_topic_config_to_broker, ReturnOrderTopicConfigToBroker);
    difference!(client_request_thread_pool_nums, ClientRequestThreadPoolNums);
    difference!(
        client_request_thread_pool_queue_capacity,
        ClientRequestThreadPoolQueueCapacity
    );
    difference!(scan_not_active_broker_interval_ms, ScanNotActiveBrokerIntervalMs);
    difference!(unregister_broker_queue_capacity, UnregisterBrokerQueueCapacity);
    difference!(support_acting_master, SupportActingMaster);
    differences
}

fn observation_failure(
    source: AdminQuerySource,
    logical_target: &str,
    error: InfrastructureObservationReadError,
) -> AdminSourceFailure {
    let code = match error.code() {
        InfrastructureObservationReadErrorCode::SourceUnavailable => AdminQueryFailureCode::SourceUnavailable,
        InfrastructureObservationReadErrorCode::Timeout => AdminQueryFailureCode::Timeout,
        InfrastructureObservationReadErrorCode::PermissionDenied => AdminQueryFailureCode::PermissionDenied,
        InfrastructureObservationReadErrorCode::NotFound => AdminQueryFailureCode::NotFound,
        InfrastructureObservationReadErrorCode::RateLimited => AdminQueryFailureCode::RateLimited,
        InfrastructureObservationReadErrorCode::InvalidResponse => AdminQueryFailureCode::InvalidResponse,
    };
    AdminSourceFailure::new(source, code, error.retryable(), logical_target)
}

fn invalid_failure(source: AdminQuerySource, logical_target: &str) -> AdminSourceFailure {
    AdminSourceFailure::new(source, AdminQueryFailureCode::InvalidResponse, false, logical_target)
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    AdminError::backend_view(
        operation,
        "INFRASTRUCTURE_OBSERVATION_SOURCE_UNAVAILABLE",
        "infrastructure observation source is unavailable",
        None,
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}

fn safe_logical_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value.parse::<std::net::IpAddr>().is_err()
        && value.parse::<std::net::SocketAddr>().is_err()
        && !value.contains([':', '/', '\\', '@', '=', '&', '?'])
        && !value.chars().any(char::is_control)
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
}

#[cfg(test)]
#[path = "infrastructure_observation/tests.rs"]
mod tests;
