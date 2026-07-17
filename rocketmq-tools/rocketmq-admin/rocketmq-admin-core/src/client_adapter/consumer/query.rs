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

use cheetah_string::CheetahString;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use super::backend_error;
use super::classify_consumer_group;
use super::collect_master_broker_targets;
use super::consume_from_where_label;
use super::now_timestamp_millis;
use super::serialize_group_retry_policy_json;
use crate::core::consumer;
use crate::core::AdminError;
use crate::core::AdminResult;

#[derive(Clone, Debug, Default)]
pub(super) struct ConsumerGroupMeta {
    pub(super) broker_names: HashSet<String>,
    pub(super) broker_addresses: HashSet<String>,
    pub(super) orderly_flags: Vec<bool>,
}

pub(super) async fn collect_consumer_group_meta(
    admin: &mut rocketmq_client_rust::DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> AdminResult<HashMap<String, ConsumerGroupMeta>> {
    let mut group_map = HashMap::new();
    let mut successful_brokers = 0usize;
    let mut last_error = None;
    for (broker_name, broker_addr) in collect_master_broker_targets(cluster_info) {
        let wrapper = match admin.get_all_subscription_group(broker_addr.clone(), 5_000).await {
            Ok(wrapper) => {
                successful_brokers += 1;
                wrapper
            }
            Err(error) => {
                tracing::warn!(
                    "Failed to fetch subscription groups from broker `{}` while collecting consumer groups: {}",
                    broker_addr,
                    error
                );
                last_error = Some(error);
                continue;
            }
        };
        for entry in wrapper.get_subscription_group_table().iter() {
            let meta = group_map
                .entry(entry.key().to_string())
                .or_insert_with(ConsumerGroupMeta::default);
            meta.broker_names.insert(broker_name.clone());
            meta.broker_addresses.insert(broker_addr.to_string());
            meta.orderly_flags.push(entry.value().consume_message_orderly());
        }
    }

    if successful_brokers == 0 {
        return Err(last_error.map_or_else(
            || {
                AdminError::backend(
                    "get_all_subscription_group",
                    "No broker subscription metadata could be loaded.",
                )
            },
            |error| backend_error("get_all_subscription_group", error),
        ));
    }
    Ok(group_map)
}

pub(super) async fn build_consumer_group_item(
    admin: &mut rocketmq_client_rust::DefaultMQAdminExt,
    group: &str,
    meta: &ConsumerGroupMeta,
    skip_sys_group: bool,
    address: Option<CheetahString>,
) -> consumer::DashboardConsumerGroupItem {
    let category = classify_consumer_group(group, meta);
    let display_group_name = if category == "SYSTEM" && !skip_sys_group {
        format!("%SYS%{group}")
    } else {
        group.to_string()
    };
    let mut consume_tps = 0_i64;
    let mut diff_total = 0_i64;
    if let Ok(stats) = admin
        .examine_consume_stats(CheetahString::from(group), None, None, None, None)
        .await
    {
        consume_tps = stats.get_consume_tps().round() as i64;
        diff_total = stats.compute_total_diff();
    }

    let mut connection_count = 0;
    let mut message_model = "UNKNOWN".to_string();
    let mut consume_type = "UNKNOWN".to_string();
    let mut version = None;
    let mut version_desc = "OFFLINE".to_string();
    if let Ok(connection) = admin
        .examine_consumer_connection_info(CheetahString::from(group), address)
        .await
    {
        connection_count = connection.get_connection_set().len();
        if let Some(model) = connection.get_message_model() {
            message_model = model.to_string();
        }
        if let Some(kind) = connection.get_consume_type() {
            consume_type = kind.to_string();
        }
        let min_version = connection.compute_min_version();
        if min_version != i32::MAX {
            version = Some(min_version);
            version_desc = RocketMqVersion::from_ordinal(min_version as u32).name().to_string();
        }
    }

    let mut broker_names = meta.broker_names.iter().cloned().collect::<Vec<_>>();
    broker_names.sort();
    let mut broker_addresses = meta.broker_addresses.iter().cloned().collect::<Vec<_>>();
    broker_addresses.sort();
    consumer::DashboardConsumerGroupItem {
        display_group_name,
        raw_group_name: group.to_string(),
        category,
        connection_count,
        consume_tps,
        diff_total,
        message_model,
        consume_type,
        version,
        version_desc,
        broker_names,
        broker_addresses,
        update_timestamp: now_timestamp_millis(),
    }
}

pub(super) fn map_consumer_connection(
    group: &str,
    connection: ConsumerConnection,
) -> consumer::DashboardConsumerConnection {
    let mut connections = connection
        .get_connection_set()
        .iter()
        .map(|item| consumer::DashboardConsumerConnectionItem {
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            version_desc: RocketMqVersion::from_ordinal(item.get_version() as u32)
                .name()
                .to_string(),
        })
        .collect::<Vec<_>>();
    connections.sort_by(|left, right| left.client_id.cmp(&right.client_id));
    let mut subscriptions = connection
        .get_subscription_table()
        .values()
        .map(|item| consumer::DashboardConsumerSubscriptionItem {
            topic: item.topic.to_string(),
            sub_string: item.sub_string.to_string(),
            expression_type: item.expression_type.to_string(),
            tags_set: item.tags_set.iter().map(ToString::to_string).collect(),
            code_set: item.code_set.iter().copied().collect(),
            sub_version: item.sub_version,
        })
        .collect::<Vec<_>>();
    subscriptions.sort_by(|left, right| left.topic.cmp(&right.topic));
    consumer::DashboardConsumerConnection {
        consumer_group: group.to_string(),
        connection_count: connections.len(),
        consume_type: connection
            .get_consume_type()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        message_model: connection
            .get_message_model()
            .map(|value| value.to_string())
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        consume_from_where: connection
            .get_consume_from_where()
            .map(consume_from_where_label)
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        connections,
        subscriptions,
    }
}

pub(super) fn map_consumer_progress(
    group: &str,
    stats: ConsumeStats,
    queue_client_map: &HashMap<MessageQueue, String>,
) -> consumer::DashboardConsumerProgress {
    let mut by_topic = HashMap::<String, Vec<consumer::DashboardConsumerTopicQueue>>::new();
    for (queue, offset) in stats.get_offset_table() {
        by_topic
            .entry(queue.topic().to_string())
            .or_default()
            .push(consumer::DashboardConsumerTopicQueue {
                broker_name: queue.broker_name().to_string(),
                queue_id: queue.queue_id(),
                broker_offset: offset.get_broker_offset(),
                consumer_offset: offset.get_consumer_offset(),
                diff_total: offset.get_broker_offset() - offset.get_consumer_offset(),
                client_info: queue_client_map.get(queue).cloned().unwrap_or_default(),
                last_timestamp: offset.get_last_timestamp(),
            });
    }
    let mut topics = by_topic
        .into_iter()
        .map(|(topic, mut queues)| {
            queues.sort_by(|left, right| {
                left.broker_name
                    .cmp(&right.broker_name)
                    .then(left.queue_id.cmp(&right.queue_id))
            });
            consumer::DashboardConsumerTopicDetail {
                topic,
                diff_total: queues.iter().map(|item| item.diff_total).sum(),
                last_timestamp: queues.iter().map(|item| item.last_timestamp).max().unwrap_or_default(),
                queues,
            }
        })
        .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.topic.cmp(&right.topic));
    consumer::DashboardConsumerProgress {
        consumer_group: group.to_string(),
        topic_count: topics.len(),
        total_diff: topics.iter().map(|item| item.diff_total).sum(),
        topics,
    }
}

pub(super) fn map_consumer_config(
    group: &str,
    broker_name: String,
    broker_address: String,
    config: &SubscriptionGroupConfig,
) -> consumer::DashboardConsumerConfig {
    let mut subscription_topics = config
        .subscription_data_set()
        .into_iter()
        .flat_map(|items| items.iter())
        .map(|item| item.topic().to_string())
        .collect::<Vec<_>>();
    subscription_topics.sort();
    subscription_topics.dedup();
    let mut attributes = config
        .attributes()
        .iter()
        .map(|(key, value)| consumer::DashboardConsumerConfigAttribute {
            key: key.to_string(),
            value: value.to_string(),
        })
        .collect::<Vec<_>>();
    attributes.sort_by(|left, right| left.key.cmp(&right.key));
    consumer::DashboardConsumerConfig {
        consumer_group: group.to_string(),
        broker_name,
        broker_address,
        consume_enable: config.consume_enable(),
        consume_from_min_enable: config.consume_from_min_enable(),
        consume_broadcast_enable: config.consume_broadcast_enable(),
        consume_message_orderly: config.consume_message_orderly(),
        retry_queue_nums: config.retry_queue_nums(),
        retry_max_times: config.retry_max_times(),
        broker_id: config.broker_id(),
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly(),
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable(),
        group_sys_flag: config.group_sys_flag(),
        consume_timeout_minute: config.consume_timeout_minute(),
        group_retry_policy_json: serialize_group_retry_policy_json(config.group_retry_policy())
            .unwrap_or_else(|_| "{}".to_string()),
        subscription_topics,
        attributes,
    }
}

pub(super) async fn collect_queue_client_mapping(
    admin: &mut rocketmq_client_rust::DefaultMQAdminExt,
    group: &str,
    offset_table: &HashMap<MessageQueue, OffsetWrapper>,
) -> HashMap<MessageQueue, String> {
    let mut topics = offset_table
        .keys()
        .map(|queue| queue.topic().to_string())
        .collect::<Vec<_>>();
    topics.sort();
    topics.dedup();
    let mut result = HashMap::new();
    for topic in topics {
        match admin
            .get_consume_status(
                CheetahString::from(topic.as_str()),
                CheetahString::from(group),
                CheetahString::new(),
            )
            .await
        {
            Ok(client_offsets) => {
                for (client_id, offsets) in client_offsets {
                    for queue in offsets.into_keys() {
                        result.insert(queue, client_id.to_string());
                    }
                }
            }
            Err(error) => tracing::warn!(
                "Failed to fetch consumer status for topic `{}` and group `{}`: {}",
                topic,
                group,
                error
            ),
        }
    }
    result
}

pub(super) async fn message_queue_allocation(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    group: &str,
) -> HashMap<MessageQueue, String> {
    let mut allocation = HashMap::new();
    let Ok(connection) = admin
        .examine_consumer_connection_info(CheetahString::from(group), None)
        .await
    else {
        return allocation;
    };
    for client in connection.get_connection_set() {
        let client_id = client.get_client_id().clone();
        let Ok(running_info) = admin
            .get_consumer_running_info(CheetahString::from(group), client_id.clone(), false, None)
            .await
        else {
            continue;
        };
        let client_ip = client_id.split('@').next().unwrap_or_default().to_string();
        for queue in running_info.mq_table.keys() {
            allocation.insert(queue.clone(), client_ip.clone());
        }
    }
    allocation
}
