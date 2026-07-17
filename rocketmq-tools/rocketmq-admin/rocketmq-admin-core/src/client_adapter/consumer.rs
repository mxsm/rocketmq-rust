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

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::SystemTime;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin_adapter_compat::error::RocketMQError;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_protocol::common::key_builder::KeyBuilder;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::subscription::group_retry_policy::GroupRetryPolicy;
use rocketmq_protocol::protocol::subscription::group_retry_policy_type::GroupRetryPolicyType;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use serde_json::json;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::consumer;
use crate::core::consumer::ConsumerAdmin;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

mod mutation;
mod query;

use self::mutation::consumer_internal_topics;
use self::mutation::resolve_consumer_target_broker_names;
use self::mutation::resolve_master_broker_addr;
use self::mutation::validate_consumer_limits;
use self::query::build_consumer_group_item;
use self::query::collect_consumer_group_meta;
use self::query::collect_queue_client_mapping;
use self::query::map_consumer_config;
use self::query::map_consumer_connection;
use self::query::map_consumer_progress;
use self::query::message_queue_allocation;
use self::query::ConsumerGroupMeta;

const CID_RMQ_SYS_PREFIX: &str = "CID_RMQ_SYS_";
const SYSTEM_CONSUMER_GROUPS: &[&str] = &[
    "TOOLS_CONSUMER",
    "FILTERSRV_CONSUMER",
    "SELF_TEST_C_GROUP",
    "CID_ONS-HTTP-PROXY",
    "CID_ONSAPI_PULL",
    "CID_ONSAPI_PERMISSION",
    "CID_ONSAPI_OWNER",
    "CID_RMQ_SYS_TRANS",
    "CID_DefaultHeartBeatSyncerTopic",
];

impl ConsumerAdmin for AdminSession {
    fn list_consumer_groups<'a>(
        &'a mut self,
        _request: &'a consumer::ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, consumer::ListConsumerGroupsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let mut groups = Vec::new();
            for retry_topic in topic_list
                .topic_list
                .into_iter()
                .filter(|topic| topic.starts_with(RETRY_GROUP_TOPIC_PREFIX))
            {
                let group = KeyBuilder::parse_group(retry_topic.as_str());
                let mut summary = consumer::ConsumerGroupSummary {
                    group: group.clone(),
                    version: 0,
                    client_count: 0,
                    consume_type: format!("{:?}", ConsumeType::ConsumePassively),
                    message_model: format!("{:?}", MessageModel::Clustering),
                    consume_tps: 0.0,
                    diff_total: 0,
                };

                if let Ok(stats) = self
                    .inner
                    .examine_consume_stats(CheetahString::from(group.as_str()), None, None, None, None)
                    .await
                {
                    summary.consume_tps = stats.get_consume_tps();
                    summary.diff_total = stats.compute_total_diff();
                }
                if let Ok(connection) = self
                    .inner
                    .examine_consumer_connection_info(CheetahString::from(group.as_str()), None)
                    .await
                {
                    summary.client_count = connection.get_connection_set().len() as i32;
                    summary.consume_type = format!(
                        "{:?}",
                        connection.get_consume_type().unwrap_or(ConsumeType::ConsumePassively)
                    );
                    summary.message_model = format!(
                        "{:?}",
                        connection.get_message_model().unwrap_or(MessageModel::Clustering)
                    );
                    summary.version = connection.compute_min_version();
                }
                groups.push(summary);
            }
            groups.sort_by(|left, right| left.group.cmp(&right.group));
            Ok(consumer::ListConsumerGroupsResult { groups })
        })
    }

    fn query_consumer_lag<'a>(
        &'a mut self,
        request: &'a consumer::QueryConsumerLagRequest,
    ) -> AdminFuture<'a, consumer::QueryConsumerLagResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let stats = self
                .inner
                .examine_consume_stats(
                    CheetahString::from(request.consumer_group.as_str()),
                    Some(CheetahString::from(request.topic.as_str())),
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|error| backend_error("examine_consume_stats", error))?;
            let allocation = if request.include_client_ip {
                message_queue_allocation(&self.inner, request.consumer_group.as_str()).await
            } else {
                HashMap::new()
            };

            let mut queues = stats.get_offset_table().keys().cloned().collect::<Vec<_>>();
            queues.sort();
            let mut result = consumer::QueryConsumerLagResult {
                consume_tps: stats.get_consume_tps(),
                ..consumer::QueryConsumerLagResult::default()
            };
            for queue in queues {
                let Some(offset) = stats.get_offset_table().get(&queue) else {
                    continue;
                };
                let lag = offset.get_broker_offset() - offset.get_consumer_offset();
                let inflight = offset.get_pull_offset() - offset.get_consumer_offset();
                result.total_lag += lag;
                result.inflight_total += inflight;
                result.rows.push(consumer::ConsumerLagRow {
                    topic: queue.topic().to_string(),
                    broker_name: queue.broker_name().to_string(),
                    queue_id: queue.queue_id(),
                    broker_offset: offset.get_broker_offset(),
                    consumer_offset: offset.get_consumer_offset(),
                    lag,
                    inflight,
                    last_timestamp: offset.get_last_timestamp(),
                    client_ip: allocation.get(&queue).cloned(),
                });
            }
            Ok(result)
        })
    }

    fn query_dashboard_consumer_groups<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerGroupListRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerGroupListResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let group_map = collect_consumer_group_meta(&mut self.inner, &cluster_info).await?;
            let address = normalize_address(request.address.as_deref());
            let mut items = Vec::with_capacity(group_map.len());
            for (group, meta) in group_map {
                items.push(
                    build_consumer_group_item(&mut self.inner, &group, &meta, request.skip_sys_group, address.clone())
                        .await,
                );
            }
            items.sort_by(|left, right| left.display_group_name.cmp(&right.display_group_name));
            Ok(consumer::DashboardConsumerGroupListResult { items })
        })
    }

    fn query_dashboard_consumer_connection<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerConnectionRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerConnection> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            let connection = self
                .inner
                .examine_consumer_connection_info(
                    CheetahString::from(group.as_str()),
                    first_address(request.address.as_deref()),
                )
                .await
                .map_err(|error| backend_error("examine_consumer_connection_info", error))?;
            Ok(map_consumer_connection(&group, connection))
        })
    }

    fn query_dashboard_consumer_progress<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerProgressRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerProgress> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            let broker_addresses = parse_addresses(request.address.as_deref());
            let stats = if broker_addresses.is_empty() {
                self.inner
                    .examine_consume_stats(CheetahString::from(group.as_str()), None, None, None, None)
                    .await
                    .map_err(|error| backend_error("examine_consume_stats", error))?
            } else {
                let mut merged_stats = ConsumeStats::new();
                for broker_addr in broker_addresses {
                    let stats = self
                        .inner
                        .examine_consume_stats(
                            CheetahString::from(group.as_str()),
                            None,
                            None,
                            Some(broker_addr),
                            Some(3_000),
                        )
                        .await
                        .map_err(|error| backend_error("examine_consume_stats", error))?;
                    merged_stats.consume_tps += stats.get_consume_tps();
                    merged_stats
                        .get_offset_table_mut()
                        .extend(stats.get_offset_table().clone());
                }
                merged_stats
            };

            let queue_client_map =
                collect_queue_client_mapping(&mut self.inner, &group, stats.get_offset_table()).await;
            Ok(map_consumer_progress(&group, stats, &queue_client_map))
        })
    }

    fn query_dashboard_consumer_config<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerConfigRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerConfig> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let broker_address = match normalize_address(request.address.as_deref()) {
                Some(address) => address,
                None => {
                    let group_map = collect_consumer_group_meta(&mut self.inner, &cluster_info).await?;
                    let meta = group_map.get(&group).ok_or_else(|| {
                        AdminError::invalid_argument(
                            "consumerGroup",
                            format!("Consumer group `{group}` was not found in the current cluster view."),
                        )
                    })?;
                    resolve_first_consumer_broker_address(meta).ok_or_else(|| {
                        AdminError::invalid_argument(
                            "consumerGroup",
                            format!("No broker address was found for consumer group `{group}`."),
                        )
                    })?
                }
            };
            let config = self
                .inner
                .examine_subscription_group_config(broker_address.clone(), CheetahString::from(group.as_str()))
                .await
                .map_err(|error| backend_error("examine_subscription_group_config", error))?;
            let broker_name =
                resolve_broker_name_by_address(&cluster_info, broker_address.as_str()).unwrap_or_default();
            Ok(map_consumer_config(
                &group,
                broker_name,
                broker_address.to_string(),
                &config,
            ))
        })
    }

    fn upsert_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerUpsertRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            validate_consumer_limits(request)?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let broker_names = resolve_consumer_target_broker_names(
                &cluster_info,
                &request.cluster_name_list,
                &request.broker_name_list,
            )?;

            let mut config = SubscriptionGroupConfig::default();
            config.set_group_name(CheetahString::from(group.as_str()));
            config.set_consume_enable(request.consume_enable);
            config.set_consume_from_min_enable(request.consume_from_min_enable);
            config.set_consume_broadcast_enable(request.consume_broadcast_enable);
            config.set_consume_message_orderly(request.consume_message_orderly);
            config.set_retry_queue_nums(request.retry_queue_nums);
            config.set_retry_max_times(request.retry_max_times);
            config.set_broker_id(request.broker_id);
            config.set_which_broker_when_consume_slowly(request.which_broker_when_consume_slowly);
            config.set_notify_consumer_ids_changed_enable(request.notify_consumer_ids_changed_enable);
            config.set_group_sys_flag(request.group_sys_flag);
            config.set_consume_timeout_minute(request.consume_timeout_minute);

            for broker_name in &broker_names {
                let broker_addr = resolve_master_broker_addr(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerNameList",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    )
                })?;
                self.inner
                    .create_and_update_subscription_group_config(broker_addr, config.clone())
                    .await
                    .map_err(|error| backend_error("create_and_update_subscription_group_config", error))?;
            }

            Ok(consumer::DashboardConsumerMutationResult {
                consumer_group: group,
                broker_names,
                updated: true,
            })
        })
    }

    fn delete_dashboard_consumer_group<'a>(
        &'a mut self,
        request: &'a consumer::DashboardConsumerDeleteRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerMutationResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let group = normalize_consumer_group(&request.consumer_group)?;
            let mut selected_broker_names = request
                .broker_name_list
                .iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            selected_broker_names.sort();
            selected_broker_names.dedup();
            if selected_broker_names.is_empty() {
                return Err(AdminError::invalid_argument(
                    "brokerNameList",
                    "Select at least one broker before deleting the consumer group.",
                ));
            }

            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let group_map = collect_consumer_group_meta(&mut self.inner, &cluster_info).await?;
            let meta = group_map.get(&group).ok_or_else(|| {
                AdminError::invalid_argument(
                    "consumerGroup",
                    format!("Consumer group `{group}` was not found in the current cluster view."),
                )
            })?;
            let delete_in_namesrv = !meta.broker_names.is_empty()
                && meta
                    .broker_names
                    .iter()
                    .all(|broker_name| selected_broker_names.contains(broker_name));

            for broker_name in &selected_broker_names {
                let broker_addr = resolve_master_broker_addr(&cluster_info, broker_name).ok_or_else(|| {
                    AdminError::invalid_argument(
                        "brokerNameList",
                        format!("Broker `{broker_name}` does not have a reachable master address."),
                    )
                })?;
                self.inner
                    .delete_subscription_group(broker_addr.clone(), CheetahString::from(group.as_str()), Some(true))
                    .await
                    .map_err(|error| backend_error("delete_subscription_group", error))?;

                for topic in consumer_internal_topics(&group) {
                    self.inner
                        .delete_topic_in_broker(
                            HashSet::from([broker_addr.clone()]),
                            CheetahString::from(topic.as_str()),
                        )
                        .await
                        .map_err(|error| backend_error("delete_topic_in_broker", error))?;
                    if delete_in_namesrv {
                        let namesrv_targets = self
                            .inner
                            .get_name_server_address_list()
                            .await
                            .into_iter()
                            .collect::<HashSet<_>>();
                        self.inner
                            .delete_topic_in_name_server(namesrv_targets, None, CheetahString::from(topic.as_str()))
                            .await
                            .map_err(|error| backend_error("delete_topic_in_name_server", error))?;
                    }
                }
            }

            Ok(consumer::DashboardConsumerMutationResult {
                consumer_group: group,
                broker_names: selected_broker_names,
                updated: false,
            })
        })
    }
}

fn normalize_consumer_group(value: &str) -> AdminResult<String> {
    let value = value.strip_prefix("%SYS%").unwrap_or(value).trim();
    if value.is_empty() {
        Err(AdminError::invalid_argument(
            "consumerGroup",
            "Consumer group is required.",
        ))
    } else {
        Ok(value.to_string())
    }
}

fn normalize_address(value: Option<&str>) -> Option<CheetahString> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CheetahString::from)
}

fn parse_addresses(value: Option<&str>) -> Vec<CheetahString> {
    value
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CheetahString::from)
        .collect()
}

fn first_address(value: Option<&str>) -> Option<CheetahString> {
    parse_addresses(value).into_iter().next()
}

fn classify_consumer_group(group: &str, meta: &ConsumerGroupMeta) -> String {
    if is_system_consumer_group(group) {
        "SYSTEM".to_string()
    } else if !meta.orderly_flags.is_empty() && meta.orderly_flags.iter().all(|flag| *flag) {
        "FIFO".to_string()
    } else {
        "NORMAL".to_string()
    }
}

fn is_system_consumer_group(group: &str) -> bool {
    group.starts_with(CID_RMQ_SYS_PREFIX) || SYSTEM_CONSUMER_GROUPS.contains(&group)
}

fn collect_master_broker_targets(cluster_info: &ClusterInfo) -> Vec<(String, CheetahString)> {
    let mut targets = Vec::new();
    if let Some(table) = cluster_info.broker_addr_table.as_ref() {
        for (broker_name, broker_data) in table {
            if let Some(address) = broker_data.broker_addrs().get(&MASTER_ID) {
                targets.push((broker_name.to_string(), address.clone()));
            }
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    targets
}

fn resolve_first_consumer_broker_address(meta: &ConsumerGroupMeta) -> Option<CheetahString> {
    let mut addresses = meta.broker_addresses.iter().collect::<Vec<_>>();
    addresses.sort();
    addresses.first().map(|value| CheetahString::from(value.as_str()))
}

fn resolve_broker_name_by_address(cluster_info: &ClusterInfo, address: &str) -> Option<String> {
    cluster_info.broker_addr_table.as_ref().and_then(|table| {
        table.iter().find_map(|(name, broker)| {
            broker
                .broker_addrs()
                .values()
                .any(|value| value.as_str() == address)
                .then(|| name.to_string())
        })
    })
}

fn consume_from_where_label(value: ConsumeFromWhere) -> String {
    #[allow(deprecated)]
    match value {
        ConsumeFromWhere::ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET".to_string(),
        ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => {
            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST".to_string()
        }
        ConsumeFromWhere::ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET".to_string(),
        ConsumeFromWhere::ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET".to_string(),
        ConsumeFromWhere::ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET".to_string(),
        ConsumeFromWhere::ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP".to_string(),
    }
}

fn serialize_group_retry_policy_json(policy: &GroupRetryPolicy) -> Result<String, serde_json::Error> {
    let retry_policy = match policy.type_() {
        GroupRetryPolicyType::Exponential => json!({
            "type": "EXPONENTIAL",
            "initial": policy.exponential_retry_policy().map(|value| value.initial()).unwrap_or(5_000),
            "max": policy.exponential_retry_policy().map(|value| value.max()).unwrap_or(7_200_000),
            "multiplier": policy.exponential_retry_policy().map(|value| value.multiplier()).unwrap_or(2),
        }),
        GroupRetryPolicyType::Customized => json!({
            "type": "CUSTOMIZED",
            "next": policy.customized_retry_policy().map(|value| value.next().to_vec()).unwrap_or_else(|| vec![
                1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000,
                360_000, 420_000, 480_000, 540_000, 600_000, 1_200_000, 1_800_000, 3_600_000,
                7_200_000,
            ]),
        }),
    };
    serde_json::to_string_pretty(&json!({
        "type": policy.type_(),
        "exponentialRetryPolicy": policy.exponential_retry_policy(),
        "customizedRetryPolicy": policy.customized_retry_policy(),
        "retryPolicy": retry_policy,
    }))
}

fn now_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_consumer_group_preserves_dashboard_categories() {
        assert_eq!(
            classify_consumer_group("CID_RMQ_SYS_TRANS", &ConsumerGroupMeta::default()),
            "SYSTEM"
        );
        assert_eq!(
            classify_consumer_group("TOOLS_CONSUMER", &ConsumerGroupMeta::default()),
            "SYSTEM"
        );
        let fifo = ConsumerGroupMeta {
            orderly_flags: vec![true, true],
            ..ConsumerGroupMeta::default()
        };
        assert_eq!(classify_consumer_group("group-fifo", &fifo), "FIFO");
        let normal = ConsumerGroupMeta {
            orderly_flags: vec![true, false],
            ..ConsumerGroupMeta::default()
        };
        assert_eq!(classify_consumer_group("group-normal", &normal), "NORMAL");
    }

    #[test]
    fn normalize_consumer_group_strips_dashboard_system_prefix() {
        assert_eq!(normalize_consumer_group(" %SYS%group-a ").unwrap(), "%SYS%group-a");
        assert_eq!(normalize_consumer_group("%SYS%group-a").unwrap(), "group-a");
    }

    #[test]
    fn resolve_first_consumer_broker_address_is_sorted() {
        let meta = ConsumerGroupMeta {
            broker_addresses: HashSet::from(["172.19.80.2:10911".to_string(), "172.19.80.1:10911".to_string()]),
            ..ConsumerGroupMeta::default()
        };
        assert_eq!(
            resolve_first_consumer_broker_address(&meta).as_deref(),
            Some("172.19.80.1:10911")
        );
    }
}
