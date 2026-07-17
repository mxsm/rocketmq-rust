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

use crate::consumer::types::ConsumerConfigAttributeItem;
use crate::consumer::types::ConsumerConfigView;
use crate::consumer::types::ConsumerConnectionItem;
use crate::consumer::types::ConsumerConnectionView;
use crate::consumer::types::ConsumerError;
use crate::consumer::types::ConsumerGroupListItem;
use crate::consumer::types::ConsumerGroupListSummary;
use crate::consumer::types::ConsumerMutationResult;
use crate::consumer::types::ConsumerSubscriptionItem;
use crate::consumer::types::ConsumerTopicDetailItem;
use crate::consumer::types::ConsumerTopicDetailQueueItem;
use crate::consumer::types::ConsumerTopicDetailView;
use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::consumer::DashboardConsumerConfig;
use rocketmq_admin_core::core::consumer::DashboardConsumerConnection;
use rocketmq_admin_core::core::consumer::DashboardConsumerGroupItem;
use rocketmq_admin_core::core::consumer::DashboardConsumerMutationResult as AdminConsumerMutationResult;
use rocketmq_admin_core::core::consumer::DashboardConsumerProgress;

pub(super) fn map_consumer_group_item(item: DashboardConsumerGroupItem) -> ConsumerGroupListItem {
    ConsumerGroupListItem {
        display_group_name: item.display_group_name,
        raw_group_name: item.raw_group_name,
        category: item.category,
        connection_count: item.connection_count,
        consume_tps: item.consume_tps,
        diff_total: item.diff_total,
        message_model: item.message_model,
        consume_type: item.consume_type,
        version: item.version,
        version_desc: item.version_desc,
        broker_names: item.broker_names,
        broker_addresses: item.broker_addresses,
        update_timestamp: item.update_timestamp,
    }
}

pub(super) fn build_summary(items: &[ConsumerGroupListItem]) -> ConsumerGroupListSummary {
    let mut summary = ConsumerGroupListSummary {
        total_groups: items.len(),
        normal_groups: 0,
        fifo_groups: 0,
        system_groups: 0,
    };

    for item in items {
        match item.category.as_str() {
            "SYSTEM" => summary.system_groups += 1,
            "FIFO" => summary.fifo_groups += 1,
            _ => summary.normal_groups += 1,
        }
    }

    summary
}

pub(super) fn map_consumer_connection_view(connection: DashboardConsumerConnection) -> ConsumerConnectionView {
    ConsumerConnectionView {
        consumer_group: connection.consumer_group,
        connection_count: connection.connection_count,
        consume_type: connection.consume_type,
        message_model: connection.message_model,
        consume_from_where: connection.consume_from_where,
        connections: connection
            .connections
            .into_iter()
            .map(|item| ConsumerConnectionItem {
                client_id: item.client_id,
                client_addr: item.client_addr,
                language: item.language,
                version: item.version,
                version_desc: item.version_desc,
            })
            .collect(),
        subscriptions: connection
            .subscriptions
            .into_iter()
            .map(|item| ConsumerSubscriptionItem {
                topic: item.topic,
                sub_string: item.sub_string,
                expression_type: item.expression_type,
                tags_set: item.tags_set,
                code_set: item.code_set,
                sub_version: item.sub_version,
            })
            .collect(),
    }
}

pub(super) fn map_consumer_config_view(config: DashboardConsumerConfig) -> ConsumerConfigView {
    ConsumerConfigView {
        consumer_group: config.consumer_group,
        broker_name: config.broker_name,
        broker_address: config.broker_address,
        consume_enable: config.consume_enable,
        consume_from_min_enable: config.consume_from_min_enable,
        consume_broadcast_enable: config.consume_broadcast_enable,
        consume_message_orderly: config.consume_message_orderly,
        retry_queue_nums: config.retry_queue_nums,
        retry_max_times: config.retry_max_times,
        broker_id: config.broker_id,
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable,
        group_sys_flag: config.group_sys_flag,
        consume_timeout_minute: config.consume_timeout_minute,
        group_retry_policy_json: config.group_retry_policy_json,
        subscription_topic_count: config.subscription_topics.len(),
        subscription_topics: config.subscription_topics,
        attributes: config
            .attributes
            .into_iter()
            .map(|item| ConsumerConfigAttributeItem {
                key: item.key,
                value: item.value,
            })
            .collect(),
    }
}

pub(super) fn map_consumer_topic_detail_view(progress: DashboardConsumerProgress) -> ConsumerTopicDetailView {
    ConsumerTopicDetailView {
        consumer_group: progress.consumer_group,
        topic_count: progress.topic_count,
        total_diff: progress.total_diff,
        topics: progress
            .topics
            .into_iter()
            .map(|topic| ConsumerTopicDetailItem {
                topic: topic.topic,
                diff_total: topic.diff_total,
                last_timestamp: topic.last_timestamp,
                queue_stat_info_list: topic
                    .queues
                    .into_iter()
                    .map(|queue| ConsumerTopicDetailQueueItem {
                        broker_name: queue.broker_name,
                        queue_id: queue.queue_id,
                        broker_offset: queue.broker_offset,
                        consumer_offset: queue.consumer_offset,
                        diff_total: queue.diff_total,
                        client_info: queue.client_info,
                        last_timestamp: queue.last_timestamp,
                    })
                    .collect(),
            })
            .collect(),
    }
}

pub(super) fn map_consumer_mutation_result(result: AdminConsumerMutationResult) -> ConsumerMutationResult {
    ConsumerMutationResult {
        consumer_group: result.consumer_group,
        broker_names: result.broker_names,
        updated: result.updated,
    }
}

pub(super) fn map_admin_error(error: AdminError) -> ConsumerError {
    match error {
        AdminError::InvalidArgument { reason, .. } => ConsumerError::Validation(reason),
        error => ConsumerError::Admin(error),
    }
}

#[cfg(test)]
mod tests {
    use super::build_summary;
    use super::map_admin_error;
    use super::map_consumer_config_view;
    use super::map_consumer_connection_view;
    use super::map_consumer_group_item;
    use super::map_consumer_mutation_result;
    use super::map_consumer_topic_detail_view;
    use crate::consumer::types::ConsumerError;
    use rocketmq_admin_core::core::AdminError;
    use rocketmq_admin_core::core::consumer::DashboardConsumerConfig;
    use rocketmq_admin_core::core::consumer::DashboardConsumerConfigAttribute;
    use rocketmq_admin_core::core::consumer::DashboardConsumerConnection;
    use rocketmq_admin_core::core::consumer::DashboardConsumerConnectionItem;
    use rocketmq_admin_core::core::consumer::DashboardConsumerGroupItem;
    use rocketmq_admin_core::core::consumer::DashboardConsumerMutationResult;
    use rocketmq_admin_core::core::consumer::DashboardConsumerProgress;
    use rocketmq_admin_core::core::consumer::DashboardConsumerSubscriptionItem;
    use rocketmq_admin_core::core::consumer::DashboardConsumerTopicDetail;
    use rocketmq_admin_core::core::consumer::DashboardConsumerTopicQueue;

    #[test]
    fn group_mapping_and_summary_preserve_dashboard_fields() {
        let normal = map_consumer_group_item(group("group-a", "NORMAL"));
        let fifo = map_consumer_group_item(group("group-b", "FIFO"));
        let system = map_consumer_group_item(group("%SYS%tools", "SYSTEM"));
        let summary = build_summary(&[normal.clone(), fifo, system]);

        assert_eq!(normal.display_group_name, "group-a");
        assert_eq!(normal.connection_count, 2);
        assert_eq!(normal.broker_names, vec!["broker-a"]);
        assert_eq!(summary.total_groups, 3);
        assert_eq!(summary.normal_groups, 1);
        assert_eq!(summary.fifo_groups, 1);
        assert_eq!(summary.system_groups, 1);
    }

    #[test]
    fn connection_mapping_preserves_connections_and_subscriptions() {
        let view = map_consumer_connection_view(DashboardConsumerConnection {
            consumer_group: "group-a".into(),
            connection_count: 1,
            consume_type: "PUSH".into(),
            message_model: "CLUSTERING".into(),
            consume_from_where: "CONSUME_FROM_LAST_OFFSET".into(),
            connections: vec![DashboardConsumerConnectionItem {
                client_id: "client-a".into(),
                client_addr: "127.0.0.1:10911".into(),
                language: "JAVA".into(),
                version: 493,
                version_desc: "V5_4_0".into(),
            }],
            subscriptions: vec![DashboardConsumerSubscriptionItem {
                topic: "TopicTest".into(),
                sub_string: "*".into(),
                expression_type: "TAG".into(),
                tags_set: vec!["TagA".into()],
                code_set: vec![1],
                sub_version: 7,
            }],
        });

        assert_eq!(view.consumer_group, "group-a");
        assert_eq!(view.connections[0].version_desc, "V5_4_0");
        assert_eq!(view.subscriptions[0].topic, "TopicTest");
    }

    #[test]
    fn progress_mapping_preserves_topic_and_queue_totals() {
        let view = map_consumer_topic_detail_view(DashboardConsumerProgress {
            consumer_group: "group-a".into(),
            topic_count: 1,
            total_diff: 100,
            topics: vec![DashboardConsumerTopicDetail {
                topic: "TopicA".into(),
                diff_total: 100,
                last_timestamp: 1_700_000_000_000,
                queues: vec![DashboardConsumerTopicQueue {
                    broker_name: "broker-a".into(),
                    queue_id: 0,
                    broker_offset: 120,
                    consumer_offset: 20,
                    diff_total: 100,
                    client_info: "client-a".into(),
                    last_timestamp: 1_700_000_000_000,
                }],
            }],
        });

        assert_eq!(view.total_diff, 100);
        assert_eq!(view.topics[0].queue_stat_info_list[0].client_info, "client-a");
    }

    #[test]
    fn config_mapping_counts_topics_and_maps_attributes() {
        let view = map_consumer_config_view(DashboardConsumerConfig {
            consumer_group: "group-a".into(),
            broker_name: "broker-a".into(),
            broker_address: "127.0.0.1:10911".into(),
            consume_enable: true,
            consume_from_min_enable: false,
            consume_broadcast_enable: true,
            consume_message_orderly: false,
            retry_queue_nums: 4,
            retry_max_times: 8,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
            group_retry_policy_json: "{}".into(),
            subscription_topics: vec!["TopicA".into(), "TopicB".into()],
            attributes: vec![DashboardConsumerConfigAttribute {
                key: "key".into(),
                value: "value".into(),
            }],
        });

        assert_eq!(view.subscription_topic_count, 2);
        assert_eq!(view.attributes[0].key, "key");
    }

    #[test]
    fn mutation_and_error_mapping_preserve_ui_contract() {
        let result = map_consumer_mutation_result(DashboardConsumerMutationResult {
            consumer_group: "group-a".into(),
            broker_names: vec!["broker-a".into()],
            updated: true,
        });
        let error = map_admin_error(AdminError::invalid_argument("consumer_group", "required"));

        assert!(result.updated);
        assert!(matches!(error, ConsumerError::Validation(reason) if reason == "required"));
    }

    fn group(name: &str, category: &str) -> DashboardConsumerGroupItem {
        DashboardConsumerGroupItem {
            display_group_name: name.into(),
            raw_group_name: name.into(),
            category: category.into(),
            connection_count: 2,
            consume_tps: 3,
            diff_total: 4,
            message_model: "CLUSTERING".into(),
            consume_type: "PUSH".into(),
            version: Some(493),
            version_desc: "V5_4_0".into(),
            broker_names: vec!["broker-a".into()],
            broker_addresses: vec!["127.0.0.1:10911".into()],
            update_timestamp: 5,
        }
    }
}
