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
use std::future::Future;

use cheetah_string::CheetahString;
use rocketmq_client_rust::ConsumerAdmin as _;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;
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

pub(super) async fn query_consumer_connection_at<Query, QueryFuture>(
    group: &str,
    address: Option<CheetahString>,
    query: Query,
) -> rocketmq_error::RocketMQResult<ConsumerConnection>
where
    Query: FnOnce(CheetahString, Option<CheetahString>) -> QueryFuture,
    QueryFuture: Future<Output = rocketmq_error::RocketMQResult<ConsumerConnection>>,
{
    query(CheetahString::from(group), address).await
}

pub(super) async fn query_consumer_progress_at<Query, QueryFuture>(
    group: &str,
    address: Option<CheetahString>,
    timeout_millis: Option<u64>,
    query: Query,
) -> rocketmq_error::RocketMQResult<ConsumeStats>
where
    Query: FnOnce(CheetahString, Option<CheetahString>, Option<u64>) -> QueryFuture,
    QueryFuture: Future<Output = rocketmq_error::RocketMQResult<ConsumeStats>>,
{
    query(CheetahString::from(group), address, timeout_millis).await
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
        for (group_name, group_config) in wrapper.get_subscription_group_table() {
            let meta = group_map
                .entry(group_name.to_string())
                .or_insert_with(ConsumerGroupMeta::default);
            meta.broker_names.insert(broker_name.clone());
            meta.broker_addresses.insert(broker_addr.to_string());
            meta.orderly_flags.push(group_config.consume_message_orderly());
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
    if let Ok(stats) = query_consumer_progress_at(group, address.clone(), None, |group, address, timeout| {
        admin.examine_consume_stats(group, None, None, address, timeout)
    })
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
    if let Ok(connection) = query_consumer_connection_at(group, address, |group, address| {
        admin.examine_consumer_connection_info(group, address)
    })
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
    connections.sort_by(|left, right| {
        left.client_id
            .cmp(&right.client_id)
            .then(left.client_addr.cmp(&right.client_addr))
            .then(left.language.cmp(&right.language))
            .then(left.version.cmp(&right.version))
            .then(left.version_desc.cmp(&right.version_desc))
    });
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

pub(super) fn map_consumer_running_info(
    group: &str,
    client_id: &str,
    include_jstack: bool,
    max_output_bytes: usize,
    running_info: ConsumerRunningInfo,
) -> consumer::DashboardConsumerRunningInfo {
    let mut budget = Utf8Budget::new(max_output_bytes);
    let properties = running_info
        .properties
        .into_iter()
        .filter(|(key, _)| running_info_property_is_allowlisted(key))
        .map(|(key, value)| consumer::DashboardConsumerConfigAttribute {
            key: budget.take(&key),
            value: budget.take(&value),
        })
        .collect();

    let mut subscriptions = running_info
        .subscription_set
        .into_iter()
        .map(|item| consumer::DashboardConsumerSubscriptionItem {
            topic: item.topic.to_string(),
            sub_string: item.sub_string.to_string(),
            expression_type: item.expression_type.to_string(),
            tags_set: item.tags_set.into_iter().map(|tag| tag.to_string()).collect(),
            code_set: item.code_set.into_iter().collect(),
            sub_version: item.sub_version,
        })
        .collect::<Vec<_>>();
    subscriptions.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.expression_type.cmp(&right.expression_type))
            .then(left.sub_string.cmp(&right.sub_string))
            .then(left.tags_set.cmp(&right.tags_set))
            .then(left.code_set.cmp(&right.code_set))
            .then(left.sub_version.cmp(&right.sub_version))
    });

    let mut process_queues = running_info
        .mq_table
        .into_iter()
        .map(|(queue, info)| consumer::DashboardConsumerProcessQueue {
            topic: queue.topic().to_string(),
            broker_name: queue.broker_name().to_string(),
            queue_id: queue.queue_id(),
            cached_message_count: i64::from(info.cached_msg_count),
            cached_message_size_in_mib: i64::from(info.cached_msg_size_in_mib),
            commit_offset: info.commit_offset,
            dropped: info.droped,
            last_consume_timestamp: i64::try_from(info.last_consume_timestamp).unwrap_or(i64::MAX),
        })
        .collect::<Vec<_>>();
    process_queues.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.broker_name.cmp(&right.broker_name))
            .then(left.queue_id.cmp(&right.queue_id))
    });

    let jstack = if include_jstack {
        match running_info.jstack {
            Some(jstack) => Some(budget.take(&jstack)),
            None => {
                budget.truncated = true;
                None
            }
        }
    } else {
        None
    };

    consumer::DashboardConsumerRunningInfo::new(
        group.to_string(),
        client_id.to_string(),
        properties,
        subscriptions,
        process_queues,
        jstack,
        budget.truncated,
    )
}

pub(super) fn running_info_property_is_allowlisted(key: &str) -> bool {
    matches!(
        key,
        ConsumerRunningInfo::PROP_THREADPOOL_CORE_SIZE
            | ConsumerRunningInfo::PROP_CONSUME_ORDERLY
            | ConsumerRunningInfo::PROP_CONSUME_TYPE
            | ConsumerRunningInfo::PROP_CLIENT_VERSION
            | ConsumerRunningInfo::PROP_CONSUMER_START_TIMESTAMP
    )
}

struct Utf8Budget {
    remaining: usize,
    truncated: bool,
}

impl Utf8Budget {
    const fn new(max_output_bytes: usize) -> Self {
        Self {
            remaining: max_output_bytes,
            truncated: false,
        }
    }

    fn take(&mut self, value: &str) -> String {
        if value.len() <= self.remaining {
            self.remaining -= value.len();
            return value.to_string();
        }

        self.truncated = true;
        let mut end = self.remaining.min(value.len());
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.remaining -= end;
        value[..end].to_string()
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
        let client_ip = client_id.split_char('@').next().unwrap_or_default().to_string();
        for queue in running_info.mq_table.keys() {
            allocation.insert(queue.clone(), client_ip.clone());
        }
    }
    allocation
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_model::version::RocketMqVersion;
    use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
    use rocketmq_protocol::protocol::body::connection::Connection;
    use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
    use rocketmq_protocol::protocol::body::consumer_running_info::ConsumerRunningInfo;
    use rocketmq_protocol::protocol::body::process_queue_info::ProcessQueueInfo;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_protocol::protocol::LanguageCode;

    use super::*;

    #[derive(Default)]
    struct CapturingQueryClient {
        connection_addresses: Vec<Option<String>>,
        progress_addresses: Vec<Option<String>>,
    }

    impl CapturingQueryClient {
        fn query_connection(&mut self, address: Option<CheetahString>) {
            self.connection_addresses.push(address.map(|value| value.to_string()));
        }

        fn query_progress(&mut self, address: Option<CheetahString>) {
            self.progress_addresses.push(address.map(|value| value.to_string()));
        }
    }

    fn connection_fixture() -> ConsumerConnection {
        let mut client = Connection::new();
        client.set_client_id(CheetahString::from_static_str("client-a"));
        client.set_client_addr(CheetahString::from_static_str("10.0.0.8:12000"));
        client.set_version(RocketMqVersion::V5_3_0 as i32);

        let mut subscription = SubscriptionData {
            topic: CheetahString::from_static_str("orders"),
            sub_string: CheetahString::from_static_str("created || paid"),
            ..SubscriptionData::default()
        };
        subscription.tags_set.extend([
            CheetahString::from_static_str("created"),
            CheetahString::from_static_str("paid"),
        ]);

        let mut connection = ConsumerConnection::new();
        connection.insert_connection(client);
        connection
            .get_subscription_table_mut()
            .insert(CheetahString::from_static_str("orders"), subscription);
        connection
    }

    fn duplicate_client_id_connection_fixture() -> ConsumerConnection {
        let mut connection = ConsumerConnection::new();
        for (client_addr, language, version) in [
            ("10.0.0.9:12000", LanguageCode::JAVA, RocketMqVersion::V5_3_0),
            ("10.0.0.8:12000", LanguageCode::RUST, RocketMqVersion::V5_2_0),
            ("10.0.0.8:12000", LanguageCode::JAVA, RocketMqVersion::V5_3_0),
            ("10.0.0.8:12000", LanguageCode::JAVA, RocketMqVersion::V5_2_0),
        ] {
            let mut client = Connection::new();
            client.set_client_id(CheetahString::from_static_str("client-a"));
            client.set_client_addr(CheetahString::from(client_addr));
            client.set_language(language);
            client.set_version(version as i32);
            connection.insert_connection(client);
        }
        connection
    }

    fn consume_stats_fixture() -> ConsumeStats {
        let mut stats = ConsumeStats::new();
        for (topic, broker_name, queue_id, broker_offset, consumer_offset) in [
            ("payments", "broker-b", 1, 24, 20),
            ("orders", "broker-b", 1, 115, 100),
            ("orders", "broker-a", 0, 123, 100),
        ] {
            let mut offset = OffsetWrapper::new();
            offset.set_broker_offset(broker_offset);
            offset.set_consumer_offset(consumer_offset);
            stats
                .get_offset_table_mut()
                .insert(MessageQueue::from_parts(topic, broker_name, queue_id), offset);
        }
        stats
    }

    fn client_map_fixture() -> HashMap<MessageQueue, String> {
        HashMap::from([
            (
                MessageQueue::from_parts("orders", "broker-a", 0),
                "10.0.0.8@client-a".to_string(),
            ),
            (
                MessageQueue::from_parts("orders", "broker-b", 1),
                "10.0.0.9@client-b".to_string(),
            ),
        ])
    }

    fn running_info_fixture() -> ConsumerRunningInfo {
        let mut running_info = ConsumerRunningInfo::new();
        running_info
            .properties
            .insert(ConsumerRunningInfo::PROP_CONSUME_TYPE.to_string(), "PUSH".to_string());
        running_info
            .properties
            .insert("credential.token".to_string(), "must-not-cross-boundary".to_string());
        running_info.subscription_set.extend([
            SubscriptionData {
                topic: CheetahString::from_static_str("payments"),
                sub_string: CheetahString::from_static_str("*"),
                ..SubscriptionData::default()
            },
            SubscriptionData {
                topic: CheetahString::from_static_str("orders"),
                sub_string: CheetahString::from_static_str("created"),
                ..SubscriptionData::default()
            },
        ]);
        running_info.mq_table.insert(
            MessageQueue::from_parts("payments", "broker-b", 1),
            ProcessQueueInfo {
                commit_offset: 17,
                cached_msg_count: 4,
                cached_msg_size_in_mib: 2,
                droped: true,
                last_consume_timestamp: 23,
                ..ProcessQueueInfo::default()
            },
        );
        running_info.mq_table.insert(
            MessageQueue::from_parts("orders", "broker-a", 0),
            ProcessQueueInfo {
                commit_offset: 11,
                cached_msg_count: 3,
                cached_msg_size_in_mib: 1,
                last_consume_timestamp: 19,
                ..ProcessQueueInfo::default()
            },
        );
        running_info.jstack = Some("栈栈".to_string());
        running_info
    }

    #[test]
    fn connection_mapping_preserves_client_and_subscription_identity() {
        let mapped = map_consumer_connection("orders-consumer", connection_fixture());
        assert_eq!(mapped.consumer_group, "orders-consumer");
        assert_eq!(mapped.connections[0].client_id, "client-a");
        assert_eq!(mapped.connections[0].client_addr, "10.0.0.8:12000");
        assert_eq!(mapped.connections[0].version_desc, "V5_3_0");
        assert_eq!(mapped.subscriptions[0].topic, "orders");
        assert_eq!(mapped.subscriptions[0].expression_type, "TAG");
        assert_eq!(mapped.subscriptions[0].tags_set, vec!["created", "paid"]);
    }

    #[test]
    fn connection_mapping_totally_orders_duplicate_client_ids() {
        let expected = vec![
            ("10.0.0.8:12000", "JAVA", "V5_2_0"),
            ("10.0.0.8:12000", "JAVA", "V5_3_0"),
            ("10.0.0.8:12000", "RUST", "V5_2_0"),
            ("10.0.0.9:12000", "JAVA", "V5_3_0"),
        ];

        for _ in 0..32 {
            let mapped = map_consumer_connection("orders-consumer", duplicate_client_id_connection_fixture());
            assert_eq!(
                mapped
                    .connections
                    .iter()
                    .map(|item| {
                        (
                            item.client_addr.as_str(),
                            item.language.as_str(),
                            item.version_desc.as_str(),
                        )
                    })
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn query_address_forwarding_preserves_explicit_proxy_and_discovery() {
        let mut fake = CapturingQueryClient::default();
        let proxy = Some(CheetahString::from_static_str("proxy-a:8081"));

        query_consumer_connection_at("orders-consumer", proxy.clone(), |_, address| {
            fake.query_connection(address);
            std::future::ready(Ok(connection_fixture()))
        })
        .await
        .expect("explicit connection query");
        query_consumer_progress_at("orders-consumer", proxy, Some(3_000), |_, address, _| {
            fake.query_progress(address);
            std::future::ready(Ok(consume_stats_fixture()))
        })
        .await
        .expect("explicit progress query");
        query_consumer_connection_at("orders-consumer", None, |_, address| {
            fake.query_connection(address);
            std::future::ready(Ok(connection_fixture()))
        })
        .await
        .expect("discovered connection query");
        query_consumer_progress_at("orders-consumer", None, None, |_, address, _| {
            fake.query_progress(address);
            std::future::ready(Ok(consume_stats_fixture()))
        })
        .await
        .expect("discovered progress query");

        assert_eq!(fake.connection_addresses, vec![Some("proxy-a:8081".to_string()), None]);
        assert_eq!(fake.progress_addresses, vec![Some("proxy-a:8081".to_string()), None]);
    }

    #[test]
    fn progress_mapping_groups_and_sorts_queues_by_topic_broker_and_queue() {
        let mapped = map_consumer_progress("orders-consumer", consume_stats_fixture(), &client_map_fixture());
        assert_eq!(
            mapped.topics.iter().map(|item| item.topic.as_str()).collect::<Vec<_>>(),
            vec!["orders", "payments"]
        );
        assert_eq!(mapped.topics[0].queues[0].broker_name, "broker-a");
        assert_eq!(mapped.topics[0].queues[0].queue_id, 0);
        assert_eq!(mapped.topics[0].queues[0].client_info, "10.0.0.8@client-a");
        assert_eq!(mapped.total_diff, 42);
    }

    #[test]
    fn running_info_mapping_sorts_sections_and_bounds_multibyte_text_and_jstack() {
        let mapped = map_consumer_running_info("orders-consumer", "10.0.0.8@client-a", true, 4, running_info_fixture());
        let debug = format!("{mapped:?}");
        let mapped = mapped.into_parts();

        assert_eq!(
            mapped
                .properties
                .iter()
                .map(|item| (item.key.as_str(), item.value.as_str()))
                .collect::<Vec<_>>(),
            vec![("PROP", "")]
        );
        assert_eq!(
            mapped
                .subscriptions
                .iter()
                .map(|item| item.topic.as_str())
                .collect::<Vec<_>>(),
            vec!["orders", "payments"]
        );
        assert_eq!(
            mapped
                .process_queues
                .iter()
                .map(|item| (item.topic.as_str(), item.broker_name.as_str(), item.queue_id))
                .collect::<Vec<_>>(),
            vec![("orders", "broker-a", 0), ("payments", "broker-b", 1)]
        );
        assert_eq!(mapped.jstack.as_deref(), Some(""));
        assert!(mapped.truncated);
        assert!(!debug.contains("must-not-cross-boundary"));
    }

    #[test]
    fn running_info_mapping_omits_jstack_when_it_was_not_requested() {
        let mapped = map_consumer_running_info(
            "orders-consumer",
            "10.0.0.8@client-a",
            false,
            1_048_576,
            running_info_fixture(),
        )
        .into_parts();

        assert_eq!(mapped.jstack, None);
        assert!(!mapped.truncated);
    }

    #[test]
    fn running_info_mapping_marks_requested_but_absent_jstack_as_truncated() {
        let mut running_info = running_info_fixture();
        running_info.jstack = None;

        let mapped = map_consumer_running_info("orders-consumer", "10.0.0.8@client-a", true, 1_048_576, running_info)
            .into_parts();

        assert_eq!(mapped.jstack, None);
        assert!(mapped.truncated);
    }
}
