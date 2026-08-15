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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::{BrokerAdmin as _, ConsumerAdmin as _, OffsetAdmin as _, RouteAdmin as _, TopicAdmin as _};
use rocketmq_error::RocketMQError;
use rocketmq_model::topic::is_system_topic;
use rocketmq_model::topic::TopicConfig;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::broker_stats_data::BrokerStatsData;

use crate::client_adapter::lifecycle::AdminSession;
use crate::client_adapter::producer::*;
use crate::core::stable_error_message;
use crate::core::topic::CanonicalTopicBatchUpsertRequest;
use crate::core::topic::DeleteTopicAdminRequest;
use crate::core::topic::DeleteTopicsInBrokerRequest;
use crate::core::topic::GetTopicConfigRequest;
use crate::core::topic::GetTopicRouteRequest;
use crate::core::topic::ListTopicsRequest;
use crate::core::topic::ListTopicsResult;
use crate::core::topic::ResetTopicConsumerOffsetRequest;
use crate::core::topic::TopicAdmin;
use crate::core::topic::TopicBatchMutationAdmin;
use crate::core::topic::TopicBatchMutationOutcome;
use crate::core::topic::TopicBatchOrderConfigOutcome;
use crate::core::topic::TopicBatchTargetOutcome;
use crate::core::topic::TopicBatchUpsertRequest;
use crate::core::topic::TopicBroker;
use crate::core::topic::TopicCatalog;
use crate::core::topic::TopicCatalogItem;
use crate::core::topic::TopicCatalogRequest;
use crate::core::topic::TopicConfigDetail;
use crate::core::topic::TopicConsumerGroups;
use crate::core::topic::TopicConsumerInfo;
use crate::core::topic::TopicConsumers;
use crate::core::topic::TopicCurrentStats;
use crate::core::topic::TopicCurrentStatsFailure;
use crate::core::topic::TopicCurrentStatsItem;
use crate::core::topic::TopicMutationOutcome;
use crate::core::topic::TopicQueue;
use crate::core::topic::TopicQueueOffset;
use crate::core::topic::TopicRoute;
use crate::core::topic::TopicSendRequest;
use crate::core::topic::TopicSendResult;
use crate::core::topic::TopicStats;
use crate::core::topic::TopicSummary;
use crate::core::topic::TopicTargetOption;
use crate::core::topic::UpsertTopicRequest;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

const TOPIC_PUT_NUMS: &str = "TOPIC_PUT_NUMS";
const GROUP_GET_NUMS: &str = "GROUP_GET_NUMS";
const MESSAGE_TYPE_ATTRIBUTE: &str = "message.type";

#[derive(Clone, Debug)]
struct TopicBrokerConfigSnapshot {
    broker_name: String,
    cluster_name: Option<String>,
    config: TopicConfig,
}

#[derive(Default)]
struct TopicCurrentStatsAccumulator {
    produced_msg_count_24h: u64,
    consumed_msg_count_24h: u64,
    in_tps: f64,
    out_tps: f64,
    consumer_group_count: usize,
}

struct TopicCurrentStatsRow {
    topic: String,
    consumer_group: Option<String>,
    in_tps: f64,
    out_tps: Option<f64>,
    in_msg_count_24h: u64,
    out_msg_count_24h: Option<u64>,
}

impl TopicAdmin for AdminSession {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;

            let Some(cluster_name) = request.cluster.as_deref() else {
                return Ok(ListTopicsResult {
                    topics: topic_list
                        .topic_list
                        .into_iter()
                        .map(|topic| TopicSummary {
                            topic: topic.to_string(),
                            cluster: None,
                            consumer_group: None,
                        })
                        .collect(),
                });
            };

            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let cluster_brokers = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(cluster_name))
                .cloned()
                .unwrap_or_default();

            let mut topics = Vec::new();
            for topic in topic_list.topic_list {
                if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                    continue;
                }
                let route = self
                    .inner
                    .examine_topic_route_info(topic.clone())
                    .await
                    .map_err(|error| backend_error("examine_topic_route_info", error))?;
                let Some(route) = route else {
                    continue;
                };
                if !route
                    .broker_datas
                    .iter()
                    .any(|broker| cluster_brokers.contains(broker.broker_name()))
                {
                    continue;
                }

                let group_list = self
                    .inner
                    .query_topic_consume_by_who(topic.clone())
                    .await
                    .map_err(|error| backend_error("query_topic_consume_by_who", error))?;
                if group_list.get_group_list().is_empty() {
                    topics.push(TopicSummary {
                        topic: topic.to_string(),
                        cluster: Some(cluster_name.to_string()),
                        consumer_group: None,
                    });
                } else {
                    topics.extend(group_list.get_group_list().iter().map(|group| TopicSummary {
                        topic: topic.to_string(),
                        cluster: Some(cluster_name.to_string()),
                        consumer_group: Some(group.to_string()),
                    }));
                }
            }
            topics.sort_by(|left, right| {
                left.topic
                    .cmp(&right.topic)
                    .then(left.consumer_group.cmp(&right.consumer_group))
            });
            Ok(ListTopicsResult { topics })
        })
    }

    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = self
                .inner
                .examine_topic_route_info(CheetahString::from(request.topic.as_str()))
                .await
                .map_err(|error| backend_error("examine_topic_route_info", error))?;
            Ok(route.map(map_topic_route))
        })
    }

    fn get_topic_catalog<'a>(&'a mut self, request: &'a TopicCatalogRequest) -> AdminFuture<'a, TopicCatalog> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let targets = cluster_targets_from_cluster_info(&cluster_info);
            let topic_configs = collect_topic_configs(&mut self.inner, &cluster_info).await?;

            let mut topics = topic_list
                .topic_list
                .into_iter()
                .map(|topic| topic.to_string())
                .collect::<Vec<_>>();
            topics.sort();

            let mut items = Vec::new();
            for topic in topics {
                let configs = topic_configs.get(&topic).map(Vec::as_slice);
                let (category, message_type, system_topic) = classify_topic(&topic, configs);
                if request.skip_system_topics && system_topic {
                    continue;
                }
                if request.skip_retry_and_dlq_topics && matches!(category.as_str(), "RETRY" | "DLQ") {
                    continue;
                }

                let route = self
                    .inner
                    .examine_topic_route_info(CheetahString::from(topic.as_str()))
                    .await
                    .map_err(|error| backend_error("examine_topic_route_info", error))?
                    .unwrap_or_default();
                let (clusters, brokers, read_queue_count, write_queue_count, perm) = summarize_route(&route);
                items.push(TopicCatalogItem {
                    topic,
                    category,
                    message_type,
                    clusters,
                    brokers,
                    read_queue_count,
                    write_queue_count,
                    perm,
                    order: summarize_order(configs),
                    system_topic,
                });
            }

            Ok(TopicCatalog { items, targets })
        })
    }

    fn get_topic_current_stats(&mut self) -> AdminFuture<'_, TopicCurrentStats> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let mut rows = Vec::new();
            let mut failures = Vec::new();

            for topic in topic_list.topic_list {
                if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                    continue;
                }
                match collect_topic_current_detail(&self.inner, &topic).await {
                    Ok(mut topic_rows) => rows.append(&mut topic_rows),
                    Err(error) => failures.push(TopicCurrentStatsFailure {
                        topic: topic.to_string(),
                        error: error.to_string(),
                    }),
                }
            }

            Ok(TopicCurrentStats {
                items: build_topic_current_stats_items(rows),
                failures,
            })
        })
    }

    fn get_topic_stats<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicStats> {
        Box::pin(async move {
            self.ensure_open()?;
            let route = require_topic_route(&mut self.inner, topic).await?;
            let mut stats = TopicStatsTable::new();
            let mut successful_brokers = 0usize;
            let mut last_error = None;

            for broker in &route.broker_datas {
                if let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) {
                    match self
                        .inner
                        .examine_topic_stats(CheetahString::from(topic), Some(master_addr.clone()))
                        .await
                    {
                        Ok(broker_stats) => {
                            stats.get_offset_table_mut().extend(broker_stats.into_offset_table());
                            successful_brokers += 1;
                        }
                        Err(error) => {
                            tracing::warn!(
                                topic,
                                broker = %broker.broker_name(),
                                address = %master_addr,
                                error = %error,
                                "failed to load topic stats from broker"
                            );
                            last_error = Some(error);
                        }
                    }
                }
            }

            if successful_brokers == 0 {
                return Err(last_error
                    .map(|error| backend_error("examine_topic_stats", error))
                    .unwrap_or_else(|| {
                        AdminError::backend(
                            "examine_topic_stats",
                            format!("topic `{topic}` has no reachable master broker"),
                        )
                    }));
            }
            if stats.get_offset_table().is_empty() {
                return Err(AdminError::invalid_argument(
                    "topic",
                    format!("topic `{topic}` returned no queue offset data"),
                ));
            }
            Ok(map_topic_stats(topic, &stats))
        })
    }

    fn get_topic_config<'a>(&'a mut self, request: &'a GetTopicConfigRequest) -> AdminFuture<'a, TopicConfigDetail> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let route = require_topic_route(&mut self.inner, &request.topic).await?;
            let configs = collect_route_topic_configs(&mut self.inner, &route, &request.topic).await?;
            let selected = match request.broker_name.as_deref() {
                Some(name) => configs
                    .iter()
                    .find(|config| config.broker_name == name)
                    .ok_or_else(|| {
                        AdminError::invalid_argument("brokerName", format!("broker `{name}` was not found"))
                    })?,
                None => configs.first().ok_or_else(|| {
                    AdminError::invalid_argument("topic", format!("topic `{}` has no online broker", request.topic))
                })?,
            };
            Ok(build_topic_config_detail(
                &request.topic,
                &cluster_info,
                selected,
                &configs,
            ))
        })
    }

    fn upsert_topic<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move { self.upsert_topic_config(request, true).await })
    }

    fn delete_topic<'a>(&'a mut self, request: &'a DeleteTopicAdminRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic = request.topic.trim();
            if topic.is_empty() {
                return Err(AdminError::invalid_argument("topic", "must not be empty"));
            }

            if let Some(broker_name) = request.broker_name.as_deref() {
                let cluster_info = self
                    .inner
                    .examine_broker_cluster_info()
                    .await
                    .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
                let broker_addr = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerName",
                        format!("broker `{broker_name}` was not found in the current cluster view"),
                    )
                })?;
                self.inner
                    .delete_topic_in_broker(HashSet::from([broker_addr]), CheetahString::from(topic))
                    .await
                    .map_err(|error| backend_error("delete_topic_in_broker", error))?;
                return Ok(TopicMutationOutcome {
                    message: format!("Topic `{topic}` was deleted from broker `{broker_name}`."),
                    target_count: 1,
                });
            }

            let clusters = if let Some(cluster_name) = request.cluster_name.as_ref() {
                vec![cluster_name.clone()]
            } else {
                let route = require_topic_route(&mut self.inner, topic).await?;
                let mut clusters = route
                    .broker_datas
                    .iter()
                    .map(|broker| broker.cluster().to_string())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                clusters.sort();
                clusters
            };
            if clusters.is_empty() {
                return Err(AdminError::invalid_argument(
                    "topic",
                    format!("topic `{topic}` has no cluster mapping to delete"),
                ));
            }
            for cluster_name in &clusters {
                self.inner
                    .delete_topic(CheetahString::from(topic), CheetahString::from(cluster_name.as_str()))
                    .await
                    .map_err(|error| backend_error("delete_topic", error))?;
            }
            Ok(TopicMutationOutcome {
                message: format!("Topic `{topic}` was deleted from {} cluster(s).", clusters.len()),
                target_count: clusters.len(),
            })
        })
    }

    fn delete_topics_in_broker<'a>(
        &'a mut self,
        request: &'a DeleteTopicsInBrokerRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.ensure_open()?;
            self.client_mut()
                .delete_topic_in_broker_list(
                    HashSet::from([CheetahString::from(request.broker_addr.as_str())]),
                    request
                        .topics
                        .iter()
                        .map(|topic| CheetahString::from(topic.as_str()))
                        .collect(),
                )
                .await
                .map_err(|error| backend_error("delete_topic_in_broker_list", error))?;
            Ok(TopicMutationOutcome {
                message: format!(
                    "deleted {} topics through one broker batch request",
                    request.topics.len()
                ),
                target_count: 1,
            })
        })
    }

    fn get_topic_consumer_groups<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumerGroups> {
        Box::pin(async move {
            self.ensure_open()?;
            let groups = self
                .inner
                .query_topic_consume_by_who(CheetahString::from(topic))
                .await
                .map_err(|error| backend_error("query_topic_consume_by_who", error))?;
            let mut groups = groups
                .group_list
                .into_iter()
                .map(|group| group.to_string())
                .collect::<Vec<_>>();
            groups.sort();
            Ok(TopicConsumerGroups { groups })
        })
    }

    fn get_topic_consumers<'a>(&'a mut self, topic: &'a str) -> AdminFuture<'a, TopicConsumers> {
        Box::pin(async move {
            self.ensure_open()?;
            let groups = self
                .inner
                .query_topic_consume_by_who(CheetahString::from(topic))
                .await
                .map_err(|error| backend_error("query_topic_consume_by_who", error))?;
            let mut group_names = groups
                .group_list
                .into_iter()
                .map(|group| group.to_string())
                .collect::<Vec<_>>();
            group_names.sort();

            let mut items = Vec::with_capacity(group_names.len());
            for consumer_group in group_names {
                let stats = self
                    .inner
                    .examine_consume_stats(
                        CheetahString::from(consumer_group.as_str()),
                        Some(CheetahString::from(topic)),
                        None,
                        None,
                        None,
                    )
                    .await
                    .map_err(|error| backend_error("examine_consume_stats", error))?;
                items.push(TopicConsumerInfo {
                    consumer_group,
                    total_diff: stats.compute_total_diff(),
                    inflight_diff: stats.compute_inflight_total_diff(),
                    consume_tps: stats.get_consume_tps(),
                });
            }
            Ok(TopicConsumers { items })
        })
    }

    fn reset_topic_consumer_offset<'a>(
        &'a mut self,
        request: &'a ResetTopicConsumerOffsetRequest,
    ) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move {
            self.ensure_open()?;
            let offsets = self
                .inner
                .reset_offset_by_timestamp(
                    None,
                    CheetahString::from(request.topic.as_str()),
                    CheetahString::from(request.consumer_group.as_str()),
                    request.reset_timestamp,
                    request.force,
                )
                .await;
            let affected_queues = match offsets {
                Ok(offsets) => offsets.len(),
                Err(error) if is_consumer_not_online_error(&error) => self
                    .inner
                    .reset_offset_by_timestamp_old(
                        None,
                        CheetahString::from(request.consumer_group.as_str()),
                        CheetahString::from(request.topic.as_str()),
                        request.reset_timestamp,
                        request.force,
                    )
                    .await
                    .map_err(|error| backend_error("reset_offset_by_timestamp_old", error))?
                    .len(),
                Err(error) => return Err(backend_error("reset_offset_by_timestamp", error)),
            };
            Ok(TopicMutationOutcome {
                message: format!(
                    "Consumer group `{}` offset was reset for {affected_queues} queue(s).",
                    request.consumer_group
                ),
                target_count: affected_queues,
            })
        })
    }

    fn send_topic_test_message<'a>(&'a mut self, request: &'a TopicSendRequest) -> AdminFuture<'a, TopicSendResult> {
        Box::pin(async move {
            self.ensure_open()?;
            if request.topic.trim().is_empty() {
                return Err(AdminError::invalid_argument("topic", "must not be empty"));
            }
            if request.message_body.is_empty() {
                return Err(AdminError::invalid_argument("messageBody", "must not be empty"));
            }

            let route = require_topic_route(&mut self.inner, &request.topic).await?;
            let configs = collect_route_topic_configs(&mut self.inner, &route, &request.topic).await?;
            let selected = configs.first().ok_or_else(|| {
                AdminError::invalid_argument("topic", format!("topic `{}` has no online broker", request.topic))
            })?;
            let message_type = selected.config.get_topic_message_type().to_string();
            let client_config = self.inner.client_config().clone_client_config();
            let producer_group = unique_producer_group(self.clock.now_millis(), message_type == "TRANSACTION");

            if message_type == "TRANSACTION" {
                send_transaction_message(self.client_runtime(), client_config, producer_group, request).await
            } else {
                send_normal_message(self.client_runtime(), client_config, producer_group, request).await
            }
        })
    }
}

impl TopicBatchMutationAdmin for AdminSession {
    fn upsert_topic_batch<'a>(
        &'a mut self,
        request: &'a TopicBatchUpsertRequest,
    ) -> AdminFuture<'a, TopicBatchMutationOutcome> {
        Box::pin(async move { run_topic_batch_workflow(self, request).await })
    }
}

trait TopicBatchExecutor {
    fn upsert_topic_local<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome>;

    fn reconcile_order_config<'a>(
        &'a mut self,
        request: &'a CanonicalTopicBatchUpsertRequest,
        successful_brokers: &'a [String],
    ) -> AdminFuture<'a, ()>;
}

impl TopicBatchExecutor for AdminSession {
    fn upsert_topic_local<'a>(&'a mut self, request: &'a UpsertTopicRequest) -> AdminFuture<'a, TopicMutationOutcome> {
        Box::pin(async move { self.upsert_topic_config(request, false).await })
    }

    fn reconcile_order_config<'a>(
        &'a mut self,
        request: &'a CanonicalTopicBatchUpsertRequest,
        successful_brokers: &'a [String],
    ) -> AdminFuture<'a, ()> {
        Box::pin(async move {
            self.reconcile_order_topic_config_internal(request, successful_brokers)
                .await
        })
    }
}

async fn run_topic_batch_workflow<E>(
    executor: &mut E,
    request: &TopicBatchUpsertRequest,
) -> AdminResult<TopicBatchMutationOutcome>
where
    E: TopicBatchExecutor,
{
    let request = request.canonical_for_execution()?;
    let mut targets = Vec::with_capacity(request.broker_names.len());
    for broker_name in &request.broker_names {
        let local_request = UpsertTopicRequest {
            cluster_names: Vec::new(),
            broker_names: vec![broker_name.clone()],
            topic: request.topic.clone(),
            write_queue_nums: request.write_queue_nums,
            read_queue_nums: request.read_queue_nums,
            perm: request.perm,
            order: request.order,
            message_type: request.message_type.clone(),
        };
        match executor.upsert_topic_local(&local_request).await {
            Ok(outcome) => targets.push(TopicBatchTargetOutcome {
                broker_name: broker_name.clone(),
                success: true,
                message: outcome.message,
            }),
            Err(error) => targets.push(TopicBatchTargetOutcome {
                broker_name: broker_name.clone(),
                success: false,
                message: stable_error_message(&error),
            }),
        }
    }
    let successful_brokers = targets
        .iter()
        .filter(|target| target.success)
        .map(|target| target.broker_name.clone())
        .collect::<Vec<_>>();
    let order_config = if successful_brokers.is_empty() {
        None
    } else {
        Some(
            match executor.reconcile_order_config(&request, &successful_brokers).await {
                Ok(()) => TopicBatchOrderConfigOutcome {
                    success: true,
                    message: "Order topic configuration reconciled".to_string(),
                },
                Err(error) => TopicBatchOrderConfigOutcome {
                    success: false,
                    message: stable_error_message(&error),
                },
            },
        )
    };
    Ok(TopicBatchMutationOutcome { targets, order_config })
}

impl AdminSession {
    async fn upsert_topic_config(
        &mut self,
        request: &UpsertTopicRequest,
        reconcile_order_config: bool,
    ) -> AdminResult<TopicMutationOutcome> {
        self.ensure_open()?;
        let topic = request.topic.trim();
        if topic.is_empty() {
            return Err(AdminError::invalid_argument("topic", "must not be empty"));
        }
        if request.cluster_names.is_empty() && request.broker_names.is_empty() {
            return Err(AdminError::invalid_argument(
                "targets",
                "select at least one cluster or broker",
            ));
        }

        let cluster_info = self
            .inner
            .examine_broker_cluster_info()
            .await
            .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
        let mut target_addrs = HashSet::new();
        let mut target_broker_names = HashSet::new();
        for cluster_name in &request.cluster_names {
            for (broker_name, broker_addr) in master_targets_by_cluster_name(&cluster_info, cluster_name)? {
                target_broker_names.insert(broker_name);
                target_addrs.insert(broker_addr);
            }
        }
        for broker_name in &request.broker_names {
            let address = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                AdminError::invalid_argument(
                    "brokerName",
                    format!("broker `{broker_name}` was not found in the current cluster view"),
                )
            })?;
            target_broker_names.insert(broker_name.clone());
            target_addrs.insert(address);
        }
        if target_addrs.is_empty() {
            return Err(AdminError::invalid_argument(
                "targets",
                "no writable broker target could be resolved",
            ));
        }

        let mut attributes = HashMap::new();
        attributes.insert(
            CheetahString::from(format!("+{MESSAGE_TYPE_ATTRIBUTE}")),
            CheetahString::from(normalize_message_type(request.message_type.as_deref())),
        );
        let topic_config = TopicConfig {
            topic_name: Some(CheetahString::from(topic)),
            read_queue_nums: request.read_queue_nums.max(1),
            write_queue_nums: request.write_queue_nums.max(1),
            perm: request.perm,
            order: request.order,
            attributes,
            ..TopicConfig::default()
        };
        for broker_addr in &target_addrs {
            self.inner
                .create_and_update_topic_config(broker_addr.clone(), topic_config.clone())
                .await
                .map_err(|error| backend_error("create_and_update_topic_config", error))?;
        }
        if reconcile_order_config && request.order {
            let order_conf = build_order_conf(&target_broker_names, topic_config.write_queue_nums);
            self.inner
                .create_or_update_order_conf(CheetahString::from(topic), CheetahString::from(order_conf), true)
                .await
                .map_err(|error| backend_error("create_or_update_order_conf", error))?;
        }

        Ok(TopicMutationOutcome {
            message: format!("Topic `{topic}` was saved successfully."),
            target_count: target_addrs.len(),
        })
    }

    async fn reconcile_order_topic_config_internal(
        &mut self,
        request: &CanonicalTopicBatchUpsertRequest,
        successful_brokers: &[String],
    ) -> AdminResult<()> {
        self.ensure_open()?;
        if request.order {
            let broker_names = successful_brokers.iter().cloned().collect::<HashSet<_>>();
            let order_conf = build_order_conf(&broker_names, request.write_queue_nums);
            self.inner
                .create_or_update_order_conf(
                    CheetahString::from(request.topic.as_str()),
                    CheetahString::from(order_conf),
                    true,
                )
                .await
                .map_err(|error| backend_error("create_or_update_order_conf", error))
        } else {
            self.inner
                .delete_kv_config(
                    CheetahString::from_static_str("ORDER_TOPIC_CONFIG"),
                    CheetahString::from(request.topic.as_str()),
                )
                .await
                .map_err(|error| backend_error("delete_order_topic_config", error))
        }
    }
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    let context = (!view.context().is_empty()).then(|| view.context().to_string());
    AdminError::backend_view(
        operation,
        view.code().as_str(),
        view.message(),
        context,
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}

mod query;

use query::*;

#[cfg(test)]
mod tests {
    use super::build_order_conf;
    use super::build_topic_current_stats_items;
    use super::classify_topic;
    use super::normalize_message_type;
    use super::run_topic_batch_workflow;
    use super::CanonicalTopicBatchUpsertRequest;
    use super::TopicBatchExecutor;
    use super::TopicBrokerConfigSnapshot;
    use super::TopicCurrentStatsRow;
    use crate::core::topic::TopicBatchUpsertRequest;
    use crate::core::topic::TopicMutationOutcome;
    use crate::core::AdminError;
    use crate::core::AdminFuture;
    use rocketmq_model::topic::TopicConfig;
    use std::collections::HashSet;

    #[test]
    fn topic_classification_preserves_dashboard_rules() {
        assert_eq!(classify_topic("%RETRY%group-a", None).0, "RETRY");
        assert_eq!(classify_topic("%DLQ%group-a", None).0, "DLQ");
        assert_eq!(classify_topic("TBW102", None).0, "SYSTEM");

        let mut normal = TopicConfig::new("TopicTest");
        normal.attributes.insert("message.type".into(), "FIFO".into());
        let configs = vec![TopicBrokerConfigSnapshot {
            broker_name: "broker-a".to_string(),
            cluster_name: Some("DefaultCluster".to_string()),
            config: normal,
        }];
        assert_eq!(classify_topic("TopicTest", Some(&configs)).1, "FIFO");
    }

    #[test]
    fn current_stats_use_max_input_and_sum_output() {
        let items = build_topic_current_stats_items(vec![
            TopicCurrentStatsRow {
                topic: "TopicA".to_string(),
                consumer_group: Some("GroupA".to_string()),
                in_tps: 12.0,
                out_tps: Some(3.0),
                in_msg_count_24h: 100,
                out_msg_count_24h: Some(20),
            },
            TopicCurrentStatsRow {
                topic: "TopicA".to_string(),
                consumer_group: Some("GroupB".to_string()),
                in_tps: 12.0,
                out_tps: Some(5.0),
                in_msg_count_24h: 100,
                out_msg_count_24h: Some(30),
            },
        ]);
        assert_eq!(items[0].produced_msg_count_24h, 100);
        assert_eq!(items[0].consumed_msg_count_24h, 50);
        assert_eq!(items[0].out_tps, 8.0);
        assert_eq!(items[0].consumer_group_count, 2);
    }

    #[test]
    fn order_config_and_message_type_are_stable() {
        let brokers = HashSet::from(["broker-b".to_string(), "broker-a".to_string()]);
        assert_eq!(build_order_conf(&brokers, 8), "broker-a:8;broker-b:8");
        assert_eq!(normalize_message_type(None), "NORMAL");
        assert_eq!(normalize_message_type(Some("transaction")), "TRANSACTION");
    }

    #[tokio::test]
    async fn batch_workflow_reconciles_order_once_with_the_complete_successful_set() {
        let request = batch_request(true);
        let mut executor = RecordingBatchExecutor::default();

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("batch workflow");

        assert_eq!(executor.local_targets, ["broker-a", "broker-b"]);
        assert_eq!(
            executor.reconciliations,
            vec![(true, vec!["broker-a".to_string(), "broker-b".to_string()])]
        );
        assert!(outcome.targets.iter().all(|target| target.success));
        assert!(outcome.order_config.expect("order result").success);
    }

    #[tokio::test]
    async fn batch_workflow_deletes_order_config_for_unordered_topic() {
        let request = batch_request(false);
        let mut executor = RecordingBatchExecutor::default();

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("batch workflow");

        assert_eq!(
            executor.reconciliations,
            vec![(false, vec!["broker-a".to_string(), "broker-b".to_string()])]
        );
        assert!(outcome.order_config.expect("delete result").success);
    }

    #[tokio::test]
    async fn batch_workflow_reconciles_only_successful_brokers_after_a_partial_failure() {
        let request = batch_request(true);
        let mut executor = RecordingBatchExecutor {
            failing_target: Some("broker-a".into()),
            ..Default::default()
        };

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("structured partial batch result");

        assert_eq!(executor.local_targets, ["broker-a", "broker-b"]);
        assert_eq!(executor.reconciliations, vec![(true, vec!["broker-b".to_string()])]);
        assert!(!outcome.targets[0].success);
        assert!(outcome.targets[1].success);
    }

    #[tokio::test]
    async fn batch_workflow_skips_reconciliation_when_every_local_update_fails() {
        let request = batch_request(true);
        let mut executor = RecordingBatchExecutor {
            fail_all: true,
            ..Default::default()
        };

        let outcome = run_topic_batch_workflow(&mut executor, &request)
            .await
            .expect("structured all-failure batch result");

        assert_eq!(executor.local_targets, ["broker-a", "broker-b"]);
        assert!(executor.reconciliations.is_empty());
        assert!(outcome.order_config.is_none());
    }

    #[tokio::test]
    async fn batch_workflow_revalidates_unchecked_input_before_any_side_effect() {
        let requests = [
            TopicBatchUpsertRequest::unchecked_for_execution_test(
                "orders topic".into(),
                vec!["broker-a".into()],
                8,
                8,
                6,
                true,
                None,
            ),
            TopicBatchUpsertRequest::unchecked_for_execution_test(
                "orders".into(),
                vec!["broker:a".into()],
                8,
                8,
                6,
                true,
                None,
            ),
            TopicBatchUpsertRequest::unchecked_for_execution_test(
                "orders".into(),
                vec!["broker-a".into()],
                0,
                8,
                6,
                true,
                None,
            ),
        ];
        for request in requests {
            let mut executor = RecordingBatchExecutor::default();

            assert!(run_topic_batch_workflow(&mut executor, &request).await.is_err());
            assert!(executor.local_targets.is_empty());
            assert!(executor.reconciliations.is_empty());
        }
    }

    fn batch_request(order: bool) -> TopicBatchUpsertRequest {
        TopicBatchUpsertRequest::try_new(
            "orders",
            vec!["broker-b".into(), "broker-a".into()],
            8,
            8,
            6,
            order,
            Some("NORMAL".into()),
        )
        .expect("valid batch request")
    }

    #[derive(Default)]
    struct RecordingBatchExecutor {
        local_targets: Vec<String>,
        reconciliations: Vec<(bool, Vec<String>)>,
        failing_target: Option<String>,
        fail_all: bool,
    }

    impl TopicBatchExecutor for RecordingBatchExecutor {
        fn upsert_topic_local<'a>(
            &'a mut self,
            request: &'a super::UpsertTopicRequest,
        ) -> AdminFuture<'a, TopicMutationOutcome> {
            self.local_targets.push(request.broker_names[0].clone());
            let should_fail =
                self.fail_all || self.failing_target.as_deref() == request.broker_names.first().map(String::as_str);
            Box::pin(async move {
                if should_fail {
                    Err(AdminError::backend("local_topic_update", "unavailable"))
                } else {
                    Ok(TopicMutationOutcome {
                        message: "saved".to_string(),
                        target_count: 1,
                    })
                }
            })
        }

        fn reconcile_order_config<'a>(
            &'a mut self,
            request: &'a CanonicalTopicBatchUpsertRequest,
            successful_brokers: &'a [String],
        ) -> AdminFuture<'a, ()> {
            self.reconciliations.push((request.order, successful_brokers.to_vec()));
            Box::pin(async { Ok(()) })
        }
    }
}
