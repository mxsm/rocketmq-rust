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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use cheetah_string::CheetahString;
use rocketmq_client_rust::{ConsumerAdmin as _, MQAdminReadExt as _};
use rocketmq_error::ErrorKind;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::body::{
    broker_body::cluster_info::ClusterInfo, consumer_connection::ConsumerConnection,
};

use super::{
    backend_error, classify_consumer_group, collect_master_broker_targets, map_consumer_connection,
    map_consumer_progress, map_consumer_running_info, normalize_consumer_group, ConsumerGroupMeta,
};
use crate::{
    client_adapter::{lifecycle::AdminSession, services::consumer::ConsumerService},
    core::{
        consumer::{
            DashboardConsumerConfigAttribute, DashboardConsumerRunningInfoRequest, SubscriptionGroupConfigCasState,
        },
        consumer_workspace::{
            ConsumerClientsResult, ConsumerConfigPresence, ConsumerConfigPresenceResult, ConsumerConfigPresenceTarget,
            ConsumerConfigTarget, ConsumerConfigurationResult, ConsumerConnectionTarget,
            ConsumerConnectionsAtTargetsResult, ConsumerExactTargetsRequest, ConsumerInventoryItem,
            ConsumerInventoryRequest, ConsumerInventoryResult, ConsumerProgressResult, ConsumerResourceRequest,
            ConsumerWorkspaceAdmin, ConsumerWorkspaceTarget, ProducerConnectionsRequest, ProducerConnectionsResult,
            ProducerInventoryItem, ProducerInventoryResult, WorkspaceFailureCode, WorkspaceFailureStage,
            WorkspaceObservation, WorkspaceObservationState, WorkspaceTargetFailure, WorkspaceUnknownReason,
        },
        dashboard::{DashboardProducerConnection, DashboardProducerConnections},
        AdminFuture,
    },
};

impl ConsumerWorkspaceAdmin for AdminSession {
    fn consumer_inventory<'a>(
        &'a self,
        request: &'a ConsumerInventoryRequest,
    ) -> AdminFuture<'a, ConsumerInventoryResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let discovery = discover_groups(&self.inner, &cluster_info).await;
            let forwarded_address = request
                .forwarded_address
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(CheetahString::from);
            let mut successful_evidence = discovery.successful_target_count;
            let mut total_evidence = discovery.target_count;
            let mut failures = discovery.failures;
            let mut items = Vec::with_capacity(discovery.groups.len());
            for (group, meta) in &discovery.groups {
                if request.skip_system_groups && crate::core::consumer::is_system_consumer_group(group) {
                    continue;
                }
                let connection = observe_group_connection(&self.inner, group, forwarded_address.clone()).await;
                let progress = rocketmq_client_rust::MQAdminReadExt::examine_consume_stats(
                    &self.inner,
                    CheetahString::from(group.as_str()),
                    None,
                    None,
                    forwarded_address.clone(),
                    forwarded_address.as_ref().map(|_| 3_000),
                )
                .await;
                total_evidence += 2;
                let (client_count, consume_type, message_model) = match connection {
                    Ok(connection) => {
                        let consume_type = connection.get_consume_type();
                        let message_model = connection.get_message_model();
                        if consume_type.is_some() && message_model.is_some() {
                            successful_evidence += 1;
                        } else {
                            failures.push(WorkspaceTargetFailure {
                                target: group.clone(),
                                stage: WorkspaceFailureStage::Clients,
                                code: WorkspaceFailureCode::InvalidData,
                                retryable: false,
                            });
                        }
                        (
                            WorkspaceObservation::Complete {
                                value: connection.get_connection_set().len(),
                            },
                            consume_type.map_or(
                                WorkspaceObservation::Unknown {
                                    reason: WorkspaceUnknownReason::InvalidResponse,
                                },
                                |value| WorkspaceObservation::Complete {
                                    value: value.to_string(),
                                },
                            ),
                            message_model.map_or(
                                WorkspaceObservation::Unknown {
                                    reason: WorkspaceUnknownReason::InvalidResponse,
                                },
                                |value| WorkspaceObservation::Complete {
                                    value: value.to_string(),
                                },
                            ),
                        )
                    }
                    Err(error) => {
                        failures.push(safe_failure(group.clone(), WorkspaceFailureStage::Clients, &error));
                        unknown_connection_fields()
                    }
                };
                let diff_total = match progress {
                    Ok(stats) => {
                        successful_evidence += 1;
                        WorkspaceObservation::Complete {
                            value: stats.compute_total_diff(),
                        }
                    }
                    Err(error) => {
                        failures.push(safe_failure(group.clone(), WorkspaceFailureStage::Progress, &error));
                        WorkspaceObservation::Unknown {
                            reason: WorkspaceUnknownReason::Unavailable,
                        }
                    }
                };
                items.push(ConsumerInventoryItem {
                    group: group.clone(),
                    category: classify_consumer_group(group, meta),
                    client_count,
                    diff_total,
                    consume_type,
                    message_model,
                    targets: targets_for_meta(&cluster_info, meta),
                });
            }
            items.sort_by(|left, right| left.group.cmp(&right.group));
            Ok(ConsumerInventoryResult {
                items,
                targets: all_targets(&cluster_info),
                observation: observation_for_counts(successful_evidence, total_evidence),
                failures,
            })
        })
    }

    fn consumer_clients<'a>(&'a self, request: &'a ConsumerResourceRequest) -> AdminFuture<'a, ConsumerClientsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.group)?;
            let address = match request
                .forwarded_address
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                Some(address) => Some(CheetahString::from(address)),
                None => first_group_target(&self.inner, &group).await?,
            };
            let observation = match address {
                Some(address) => match self
                    .inner
                    .observe_consumer_connection_at(CheetahString::from(group.as_str()), address)
                    .await
                {
                    Ok(connection) => WorkspaceObservation::Complete {
                        value: map_consumer_connection(&group, connection),
                    },
                    Err(_) => WorkspaceObservation::Unknown {
                        reason: WorkspaceUnknownReason::Unavailable,
                    },
                },
                None => WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
            };
            Ok(ConsumerClientsResult { observation })
        })
    }

    fn consumer_progress<'a>(
        &'a self,
        request: &'a ConsumerResourceRequest,
    ) -> AdminFuture<'a, ConsumerProgressResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.group)?;
            let address = request
                .forwarded_address
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(CheetahString::from);
            let observation = match rocketmq_client_rust::MQAdminReadExt::examine_consume_stats(
                &self.inner,
                CheetahString::from(group.as_str()),
                None,
                None,
                address.clone(),
                address.as_ref().map(|_| 3_000),
            )
            .await
            {
                Ok(stats) => WorkspaceObservation::Complete {
                    value: map_consumer_progress(&group, stats, &HashMap::new()),
                },
                Err(_) => WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
            };
            Ok(ConsumerProgressResult { observation })
        })
    }

    fn consumer_configuration<'a>(&'a self, group: &'a str) -> AdminFuture<'a, ConsumerConfigurationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(group)?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let discovery = discover_groups(&self.inner, &cluster_info).await;
            let mut targets = Vec::new();
            let mut failures = discovery.failures;
            if let Some(meta) = discovery.groups.get(&group) {
                for target in targets_for_meta(&cluster_info, meta) {
                    match self
                        .inner
                        .subscription_group_config_with_version(
                            CheetahString::from(target.broker_address.as_str()),
                            CheetahString::from(group.as_str()),
                        )
                        .await
                    {
                        Ok(versioned) => {
                            let config = versioned.config;
                            match checked_config_state(versioned.version, &config) {
                                Some(state) => targets.push(ConsumerConfigTarget {
                                    target,
                                    observation: WorkspaceObservation::Complete { value: state },
                                    entries: safe_config_entries(&state),
                                }),
                                None => {
                                    failures.push(WorkspaceTargetFailure {
                                        target: target.broker_name.clone(),
                                        stage: WorkspaceFailureStage::Configuration,
                                        code: WorkspaceFailureCode::InvalidData,
                                        retryable: false,
                                    });
                                    targets.push(ConsumerConfigTarget {
                                        target,
                                        observation: WorkspaceObservation::Unknown {
                                            reason: WorkspaceUnknownReason::InvalidResponse,
                                        },
                                        entries: Vec::new(),
                                    });
                                }
                            }
                        }
                        Err(error) => {
                            failures.push(safe_failure(
                                target.broker_name.clone(),
                                WorkspaceFailureStage::Configuration,
                                &error,
                            ));
                            targets.push(ConsumerConfigTarget {
                                target,
                                observation: WorkspaceObservation::Unknown {
                                    reason: WorkspaceUnknownReason::Unavailable,
                                },
                                entries: Vec::new(),
                            });
                        }
                    }
                }
            }
            let successful = targets
                .iter()
                .filter(|target| matches!(target.observation, WorkspaceObservation::Complete { .. }))
                .count();
            let observation = if successful == targets.len() && !targets.is_empty() && failures.is_empty() {
                WorkspaceObservationState::Complete
            } else if successful > 0 {
                WorkspaceObservationState::Partial
            } else {
                WorkspaceObservationState::Unknown
            };
            Ok(ConsumerConfigurationResult {
                group,
                targets,
                observation,
                failures,
            })
        })
    }

    fn consumer_config_presence<'a>(
        &'a self,
        request: &'a ConsumerExactTargetsRequest,
    ) -> AdminFuture<'a, ConsumerConfigPresenceResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.group)?;
            let mut targets = Vec::with_capacity(request.targets.len());
            let mut failures = Vec::new();
            for target in &request.targets {
                let presence = match self
                    .inner
                    .subscription_group_config_with_version(
                        CheetahString::from(required("brokerAddress", &target.broker_address)?),
                        CheetahString::from(group.as_str()),
                    )
                    .await
                {
                    Ok(_) => ConsumerConfigPresence::Present,
                    Err(error) if config_is_absent(&error) => ConsumerConfigPresence::Absent,
                    Err(error) => {
                        failures.push(safe_failure(
                            target.broker_name.clone(),
                            WorkspaceFailureStage::Configuration,
                            &error,
                        ));
                        ConsumerConfigPresence::Unknown
                    }
                };
                targets.push(ConsumerConfigPresenceTarget {
                    target: target.clone(),
                    presence,
                });
            }
            Ok(ConsumerConfigPresenceResult { targets, failures })
        })
    }

    fn consumer_connections_at_targets<'a>(
        &'a self,
        request: &'a ConsumerExactTargetsRequest,
    ) -> AdminFuture<'a, ConsumerConnectionsAtTargetsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.group)?;
            let mut targets = Vec::with_capacity(request.targets.len());
            let mut failures = Vec::new();
            for target in &request.targets {
                let observation = match self
                    .inner
                    .observe_consumer_connection_at(
                        CheetahString::from(group.as_str()),
                        CheetahString::from(required("brokerAddress", &target.broker_address)?),
                    )
                    .await
                {
                    Ok(connection) => WorkspaceObservation::Complete {
                        value: map_consumer_connection(&group, connection),
                    },
                    Err(error) => {
                        failures.push(safe_failure(
                            target.broker_name.clone(),
                            WorkspaceFailureStage::Clients,
                            &error,
                        ));
                        WorkspaceObservation::Unknown {
                            reason: WorkspaceUnknownReason::Unavailable,
                        }
                    }
                };
                targets.push(ConsumerConnectionTarget {
                    target: target.clone(),
                    observation,
                });
            }
            Ok(ConsumerConnectionsAtTargetsResult { targets, failures })
        })
    }

    fn consumer_diagnostic<'a>(
        &'a self,
        request: &'a DashboardConsumerRunningInfoRequest,
    ) -> AdminFuture<'a, crate::core::consumer::DashboardConsumerRunningInfo> {
        Box::pin(async move {
            self.ensure_open()?;
            let running_info = ConsumerService::query_dashboard_consumer_running_info_with_admin(&self.inner, request)
                .await
                .map_err(|error| backend_error("get_consumer_running_info", error))?;
            Ok(map_consumer_running_info(
                request.consumer_group(),
                request.client_id(),
                request.include_jstack(),
                request.max_output_bytes(),
                running_info,
            ))
        })
    }

    fn producer_inventory(&self) -> AdminFuture<'_, ProducerInventoryResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let targets = collect_master_broker_targets(&cluster_info);
            let mut clients_by_group = BTreeMap::<String, BTreeSet<ProducerClientIdentity>>::new();
            let mut failures = Vec::new();
            let mut successful = 0usize;
            for (broker_name, address) in &targets {
                match rocketmq_client_rust::MQAdminReadExt::get_all_producer_info(&self.inner, address.clone()).await {
                    Ok(table) => {
                        successful += 1;
                        merge_producer_inventory_clients(&mut clients_by_group, &table);
                    }
                    Err(error) => failures.push(safe_failure(
                        broker_name.clone(),
                        WorkspaceFailureStage::Inventory,
                        &error,
                    )),
                }
            }
            let items = clients_by_group
                .into_iter()
                .map(|(group, clients)| ProducerInventoryItem {
                    group,
                    client_count: if failures.is_empty() {
                        WorkspaceObservation::Complete { value: clients.len() }
                    } else {
                        WorkspaceObservation::Partial {
                            value: clients.len(),
                            successful_target_count: successful,
                            failures: failures.clone(),
                        }
                    },
                })
                .collect();
            let observation = observation_for_counts(successful, targets.len());
            Ok(ProducerInventoryResult {
                items,
                observation,
                failures,
            })
        })
    }

    fn producer_connections<'a>(
        &'a self,
        request: &'a ProducerConnectionsRequest,
    ) -> AdminFuture<'a, ProducerConnectionsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic = required("topic", &request.topic)?;
            let group = required("producerGroup", &request.group)?;
            let route = match self
                .inner
                .examine_topic_route_info(CheetahString::from(topic.as_str()))
                .await
            {
                Ok(Some(route)) => route,
                Ok(None) => {
                    return Ok(ProducerConnectionsResult {
                        observation: WorkspaceObservation::Unknown {
                            reason: WorkspaceUnknownReason::InvalidResponse,
                        },
                    });
                }
                Err(_) => {
                    return Ok(ProducerConnectionsResult {
                        observation: WorkspaceObservation::Unknown {
                            reason: WorkspaceUnknownReason::Unavailable,
                        },
                    });
                }
            };
            let targets = producer_route_targets(&route);
            if targets.is_empty() {
                return Ok(ProducerConnectionsResult {
                    observation: WorkspaceObservation::Unknown {
                        reason: WorkspaceUnknownReason::InvalidResponse,
                    },
                });
            }
            let mut successful = 0usize;
            let mut connections = Vec::new();
            let mut failures = Vec::new();
            for (broker_name, broker_address) in &targets {
                match self
                    .inner
                    .observe_producer_connection_at(CheetahString::from(group.as_str()), broker_address.clone())
                    .await
                {
                    Ok(connection) => {
                        successful += 1;
                        connections.push(connection);
                    }
                    Err(error) => failures.push(safe_failure(
                        broker_name.clone(),
                        WorkspaceFailureStage::Clients,
                        &error,
                    )),
                }
            }
            let value = map_producer_connections(&topic, &group, connections);
            let observation = if successful == targets.len() {
                WorkspaceObservation::Complete { value }
            } else if successful > 0 {
                WorkspaceObservation::Partial {
                    value,
                    successful_target_count: successful,
                    failures,
                }
            } else {
                WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                }
            };
            Ok(ProducerConnectionsResult { observation })
        })
    }
}

type ProducerClientIdentity = (String, String, i32, i32);

fn merge_producer_inventory_clients(
    clients_by_group: &mut BTreeMap<String, BTreeSet<ProducerClientIdentity>>,
    table: &rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo,
) {
    for (group, clients) in table.data() {
        let identities = clients_by_group.entry(group.clone()).or_default();
        identities.extend(clients.iter().map(|client| {
            (
                client.client_id().to_owned(),
                client.remote_ip().to_owned(),
                i32::from(client.language()),
                client.version(),
            )
        }));
    }
}

struct GroupDiscovery {
    groups: BTreeMap<String, ConsumerGroupMeta>,
    successful_target_count: usize,
    target_count: usize,
    failures: Vec<WorkspaceTargetFailure>,
}

async fn discover_groups(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> GroupDiscovery {
    let targets = collect_master_broker_targets(cluster_info);
    let mut groups = BTreeMap::<String, ConsumerGroupMeta>::new();
    let mut successful_target_count = 0usize;
    let mut failures = Vec::new();
    for (broker_name, address) in &targets {
        match admin.get_all_subscription_group(address.clone(), 5_000).await {
            Ok(wrapper) => {
                successful_target_count += 1;
                for (group, config) in wrapper.get_subscription_group_table() {
                    let meta = groups.entry(group.to_string()).or_default();
                    meta.broker_names.insert(broker_name.clone());
                    meta.broker_addresses.insert(address.to_string());
                    meta.orderly_flags.push(config.consume_message_orderly());
                }
            }
            Err(error) => failures.push(safe_failure(
                broker_name.clone(),
                WorkspaceFailureStage::Inventory,
                &error,
            )),
        }
    }
    GroupDiscovery {
        groups,
        successful_target_count,
        target_count: targets.len(),
        failures,
    }
}

async fn observe_group_connection(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    group: &str,
    forwarded_address: Option<CheetahString>,
) -> rocketmq_error::RocketMQResult<ConsumerConnection> {
    match forwarded_address {
        Some(address) => {
            admin
                .observe_consumer_connection_at(CheetahString::from(group), address)
                .await
        }
        None => {
            rocketmq_client_rust::MQAdminReadExt::examine_consumer_connection_info(
                admin,
                CheetahString::from(group),
                None,
            )
            .await
        }
    }
}

async fn first_group_target(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    group: &str,
) -> crate::core::AdminResult<Option<CheetahString>> {
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let discovery = discover_groups(admin, &cluster_info).await;
    Ok(discovery.groups.get(group).and_then(|meta| {
        let mut addresses = meta.broker_addresses.iter().collect::<Vec<_>>();
        addresses.sort();
        addresses.first().map(|address| CheetahString::from(address.as_str()))
    }))
}

fn targets_for_meta(cluster_info: &ClusterInfo, meta: &ConsumerGroupMeta) -> Vec<ConsumerWorkspaceTarget> {
    let mut targets = meta
        .broker_names
        .iter()
        .filter_map(|broker_name| {
            let broker_address = meta
                .broker_addresses
                .iter()
                .find(|address| broker_address_matches(cluster_info, broker_name, address))?
                .clone();
            Some(ConsumerWorkspaceTarget {
                cluster_name: cluster_for_broker(cluster_info, broker_name).unwrap_or_default(),
                broker_name: broker_name.clone(),
                broker_address,
            })
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    targets
}

fn all_targets(cluster_info: &ClusterInfo) -> Vec<ConsumerWorkspaceTarget> {
    collect_master_broker_targets(cluster_info)
        .into_iter()
        .map(|(broker_name, broker_address)| ConsumerWorkspaceTarget {
            cluster_name: cluster_for_broker(cluster_info, &broker_name).unwrap_or_default(),
            broker_name,
            broker_address: broker_address.to_string(),
        })
        .collect()
}

fn cluster_for_broker(cluster_info: &ClusterInfo, broker_name: &str) -> Option<String> {
    let mut clusters = cluster_info
        .cluster_addr_table
        .as_ref()?
        .iter()
        .filter(|(_, brokers)| brokers.iter().any(|broker| broker.as_str() == broker_name))
        .map(|(cluster, _)| cluster.to_string())
        .collect::<Vec<_>>();
    clusters.sort();
    clusters.into_iter().next()
}

fn broker_address_matches(cluster_info: &ClusterInfo, broker_name: &str, address: &str) -> bool {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .is_some_and(|broker| broker.broker_addrs().values().any(|value| value.as_str() == address))
}

fn unknown_connection_fields() -> (
    WorkspaceObservation<usize>,
    WorkspaceObservation<String>,
    WorkspaceObservation<String>,
) {
    (
        WorkspaceObservation::Unknown {
            reason: WorkspaceUnknownReason::Unavailable,
        },
        WorkspaceObservation::Unknown {
            reason: WorkspaceUnknownReason::Unavailable,
        },
        WorkspaceObservation::Unknown {
            reason: WorkspaceUnknownReason::Unavailable,
        },
    )
}

fn observation_for_counts(successful: usize, total: usize) -> WorkspaceObservationState {
    if total > 0 && successful == total {
        WorkspaceObservationState::Complete
    } else if successful > 0 {
        WorkspaceObservationState::Partial
    } else {
        WorkspaceObservationState::Unknown
    }
}

fn safe_config_entries(state: &SubscriptionGroupConfigCasState) -> Vec<DashboardConsumerConfigAttribute> {
    vec![
        config_entry("retryMaxTimes", state.retry_max_times),
        config_entry("retryQueueNums", state.retry_queue_nums),
        config_entry("consumeTimeoutMinutes", state.consume_timeout_minutes),
        config_entry("consumeEnable", state.consume_enable),
        config_entry("consumeFromMinEnable", state.consume_from_min_enable),
        config_entry("consumeBroadcastEnable", state.consume_broadcast_enable),
        config_entry("consumeMessageOrderly", state.consume_message_orderly),
        config_entry(
            "notifyConsumerIdsChangedEnable",
            state.notify_consumer_ids_changed_enable,
        ),
    ]
}

fn config_entry(key: &str, value: impl ToString) -> DashboardConsumerConfigAttribute {
    DashboardConsumerConfigAttribute {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn checked_config_state(
    version: u64,
    config: &rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig,
) -> Option<SubscriptionGroupConfigCasState> {
    Some(SubscriptionGroupConfigCasState {
        version,
        retry_max_times: u32::try_from(config.retry_max_times()).ok()?,
        retry_queue_nums: u32::try_from(config.retry_queue_nums()).ok()?,
        consume_timeout_minutes: u32::try_from(config.consume_timeout_minute()).ok()?,
        consume_enable: config.consume_enable(),
        consume_from_min_enable: config.consume_from_min_enable(),
        consume_broadcast_enable: config.consume_broadcast_enable(),
        consume_message_orderly: config.consume_message_orderly(),
        broker_id: config.broker_id(),
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly(),
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable(),
        group_sys_flag: config.group_sys_flag(),
    })
}

fn required(field: &'static str, value: &str) -> crate::core::AdminResult<String> {
    let value = value.trim();
    if value.is_empty() {
        Err(crate::core::AdminError::invalid_argument(field, "must not be empty"))
    } else {
        Ok(value.to_string())
    }
}

fn safe_failure(
    target: String,
    stage: WorkspaceFailureStage,
    error: &rocketmq_error::RocketMQError,
) -> WorkspaceTargetFailure {
    WorkspaceTargetFailure {
        target,
        stage,
        code: WorkspaceFailureCode::Unavailable,
        retryable: error.boundary_view().is_retryable(),
    }
}

fn config_is_absent(error: &rocketmq_error::RocketMQError) -> bool {
    matches!(
        error.kind(),
        ErrorKind::SubscriptionGroupNotExist | ErrorKind::QueryNotFound
    )
}

fn map_producer_connections(
    topic: &str,
    group: &str,
    producer_connections: impl IntoIterator<
        Item = rocketmq_protocol::protocol::body::producer_connection::ProducerConnection,
    >,
) -> DashboardProducerConnections {
    let mut deduplicated = BTreeMap::new();
    for connection in producer_connections {
        for item in connection.connection_set() {
            let identity = (
                item.get_client_id().to_string(),
                item.get_client_addr().to_string(),
                item.get_language().to_string(),
                item.get_version(),
            );
            deduplicated
                .entry(identity.clone())
                .or_insert_with(|| DashboardProducerConnection {
                    client_id: identity.0,
                    client_addr: identity.1,
                    language: identity.2,
                    version: identity.3,
                    version_desc: RocketMqVersion::from_ordinal(identity.3.max(0) as u32)
                        .name()
                        .to_string(),
                });
        }
    }
    let connections = deduplicated.into_values().collect();
    DashboardProducerConnections {
        topic: topic.to_string(),
        producer_group: group.to_string(),
        connections,
    }
}

fn producer_route_targets(
    route: &rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData,
) -> Vec<(String, CheetahString)> {
    let mut targets = route
        .broker_datas
        .iter()
        .filter_map(|broker| {
            let address = broker.broker_addrs().get(&MASTER_ID).cloned().or_else(|| {
                let mut addresses = broker.broker_addrs().iter().collect::<Vec<_>>();
                addresses.sort_unstable_by_key(|(broker_id, _)| **broker_id);
                addresses.first().map(|(_, address)| (*address).clone())
            })?;
            Some((broker.broker_name().to_string(), address))
        })
        .collect::<Vec<_>>();
    targets.sort();
    targets.dedup();
    targets
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;

    #[test]
    fn observation_counts_never_turn_no_success_into_complete_empty() {
        assert_eq!(observation_for_counts(0, 2), WorkspaceObservationState::Unknown);
        assert_eq!(observation_for_counts(1, 2), WorkspaceObservationState::Partial);
        assert_eq!(observation_for_counts(2, 2), WorkspaceObservationState::Complete);
        assert_eq!(observation_for_counts(2, 6), WorkspaceObservationState::Partial);
    }

    #[test]
    fn complete_discovery_with_all_group_children_failed_is_partial() {
        let failures = [
            WorkspaceTargetFailure {
                target: "orders".into(),
                stage: WorkspaceFailureStage::Clients,
                code: WorkspaceFailureCode::Unavailable,
                retryable: true,
            },
            WorkspaceTargetFailure {
                target: "orders".into(),
                stage: WorkspaceFailureStage::Progress,
                code: WorkspaceFailureCode::Unavailable,
                retryable: true,
            },
        ];

        assert_eq!(observation_for_counts(2, 4), WorkspaceObservationState::Partial);
        assert_eq!(failures.len(), 2);
        assert!(failures
            .iter()
            .any(|failure| failure.stage == WorkspaceFailureStage::Clients));
        assert!(failures
            .iter()
            .any(|failure| failure.stage == WorkspaceFailureStage::Progress));
    }

    #[test]
    fn producer_route_targets_include_every_authoritative_broker_stably() {
        let broker = |name: &str, address: &str| {
            rocketmq_protocol::protocol::route::route_data_view::BrokerData::new(
                "cluster-a".into(),
                CheetahString::from(name),
                HashMap::from([(MASTER_ID, CheetahString::from(address))]),
                None,
            )
        };
        let route = rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData {
            broker_datas: vec![
                broker("broker-b", "10.0.0.2:10911"),
                broker("broker-a", "10.0.0.1:10911"),
            ],
            ..Default::default()
        };

        assert_eq!(
            producer_route_targets(&route),
            vec![
                ("broker-a".into(), CheetahString::from("10.0.0.1:10911")),
                ("broker-b".into(), CheetahString::from("10.0.0.2:10911")),
            ]
        );
        assert_eq!(observation_for_counts(1, 2), WorkspaceObservationState::Partial);
        assert_eq!(observation_for_counts(0, 2), WorkspaceObservationState::Unknown);
    }

    #[test]
    fn producer_inventory_deduplicates_exact_client_identity_across_brokers() {
        use rocketmq_protocol::protocol::{body::producer_info::ProducerInfo, LanguageCode};

        let client_on_broker_a = ProducerInfo::new("producer-1", "10.0.1.7:41000", LanguageCode::JAVA, 403, 10);
        let client_on_broker_b = ProducerInfo::new("producer-1", "10.0.1.7:41000", LanguageCode::JAVA, 403, 20);
        let table_a =
            rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo::new(HashMap::from([(
                "orders-producer".to_owned(),
                vec![client_on_broker_a],
            )]));
        let table_b =
            rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo::new(HashMap::from([(
                "orders-producer".to_owned(),
                vec![client_on_broker_b],
            )]));
        let mut clients_by_group = BTreeMap::new();

        merge_producer_inventory_clients(&mut clients_by_group, &table_a);
        merge_producer_inventory_clients(&mut clients_by_group, &table_b);

        assert_eq!(clients_by_group["orders-producer"].len(), 1);
    }

    #[test]
    fn malformed_negative_configuration_is_not_fabricated_as_zero() {
        let mut config =
            rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::default();
        config.set_retry_max_times(-1);

        assert!(checked_config_state(7, &config).is_none());
    }

    #[test]
    fn running_info_allowlist_excludes_nameserver_and_arbitrary_secrets() {
        assert!(super::super::query::running_info_property_is_allowlisted(
            ConsumerRunningInfo::PROP_CONSUME_TYPE
        ));
        assert!(!super::super::query::running_info_property_is_allowlisted(
            ConsumerRunningInfo::PROP_NAMESERVER_ADDR
        ));
        assert!(!super::super::query::running_info_property_is_allowlisted(
            "credential.token"
        ));
    }

    #[test]
    fn producer_empty_connections_are_a_complete_empty_observation() {
        let mapped = map_producer_connections(
            "orders",
            "orders-producer",
            [rocketmq_protocol::protocol::body::producer_connection::ProducerConnection::new()],
        );
        let observation = WorkspaceObservation::Complete { value: mapped };
        assert!(observation.value().expect("value").connections.is_empty());
    }
}
