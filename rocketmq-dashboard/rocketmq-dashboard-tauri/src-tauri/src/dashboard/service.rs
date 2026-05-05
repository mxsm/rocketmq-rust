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

use crate::cluster::service::ClusterManager;
use crate::cluster::types::ClusterBrokerCardItem;
use crate::cluster::types::ClusterHomePageResponse;
use crate::dashboard::types::DashboardBrokerOverviewResponse;
use crate::dashboard::types::DashboardBrokerSummary;
use crate::dashboard::types::DashboardBrokerTopItem;
use crate::dashboard::types::DashboardBrokerTpsItem;
use crate::dashboard::types::DashboardError;
use crate::dashboard::types::DashboardResult;
use crate::dashboard::types::DashboardTopicCategoryItem;
use crate::dashboard::types::DashboardTopicCurrentResponse;
use crate::dashboard::types::DashboardTopicQueueItem;
use crate::dashboard::types::DashboardTopicTopItem;
use crate::topic::service::TopicManager;
use crate::topic::types::TopicCurrentStatsItem;
use crate::topic::types::TopicListItem;
use crate::topic::types::TopicListResponse;
use rocketmq_dashboard_common::ClusterHomePageRequest;
use rocketmq_dashboard_common::DashboardBrokerOverviewRequest;
use rocketmq_dashboard_common::TopicListRequest;
use std::collections::BTreeMap;
use std::collections::HashSet;

const TOP_LIMIT: usize = 10;

pub(crate) async fn get_dashboard_broker_overview(
    cluster_manager: &ClusterManager,
    request: DashboardBrokerOverviewRequest,
) -> DashboardResult<DashboardBrokerOverviewResponse> {
    let cluster_response = cluster_manager
        .get_cluster_home_page(ClusterHomePageRequest {
            force_refresh: request.force_refresh,
        })
        .await
        .map_err(|error| DashboardError::Cluster(error.to_string()))?;

    Ok(build_broker_overview(cluster_response))
}

pub(crate) async fn query_dashboard_topic_current(
    topic_manager: &TopicManager,
) -> DashboardResult<DashboardTopicCurrentResponse> {
    let topic_response = topic_manager
        .get_topic_list(TopicListRequest {
            skip_sys_process: false,
            skip_retry_and_dlq: false,
        })
        .await
        .map_err(|error| DashboardError::Topic(error.to_string()))?;
    let stats_response = topic_manager
        .get_topic_current_stats()
        .await
        .map_err(|error| DashboardError::Topic(error.to_string()))?;

    for failure in &stats_response.failures {
        log::warn!(
            "Failed to collect dashboard topic current stats for `{}`: {}",
            failure.topic,
            failure.error
        );
    }

    Ok(build_topic_current(topic_response, stats_response.items))
}

fn build_broker_overview(cluster_response: ClusterHomePageResponse) -> DashboardBrokerOverviewResponse {
    DashboardBrokerOverviewResponse {
        current_namesrv: cluster_response.current_namesrv,
        use_vip_channel: cluster_response.use_vip_channel,
        use_tls: cluster_response.use_tls,
        summary: DashboardBrokerSummary {
            total_clusters: cluster_response.summary.total_clusters,
            total_brokers: cluster_response.summary.total_brokers,
            total_masters: cluster_response.summary.total_masters,
            total_slaves: cluster_response.summary.total_slaves,
            active_brokers: cluster_response.summary.active_brokers,
            inactive_brokers: cluster_response.summary.inactive_brokers,
            brokers_with_status_errors: cluster_response.summary.brokers_with_status_errors,
        },
        broker_top: build_broker_top(&cluster_response.items),
        broker_tps: build_broker_tps(&cluster_response.items),
    }
}

fn build_topic_current(
    topic_response: TopicListResponse,
    topic_stats: Vec<TopicCurrentStatsItem>,
) -> DashboardTopicCurrentResponse {
    DashboardTopicCurrentResponse {
        current_namesrv: topic_response.current_namesrv,
        use_vip_channel: topic_response.use_vip_channel,
        use_tls: topic_response.use_tls,
        total_topics: topic_response.total,
        topic_top: build_topic_top(topic_stats, &topic_response.items),
        topic_queue_top: build_topic_queue_top(&topic_response.items),
        topic_category_distribution: build_topic_category_distribution(&topic_response.items),
    }
}

fn build_broker_top(items: &[ClusterBrokerCardItem]) -> Vec<DashboardBrokerTopItem> {
    let mut sorted = items.to_vec();
    sorted.sort_by(|left, right| {
        right
            .today_received_total
            .cmp(&left.today_received_total)
            .then(left.broker_name.cmp(&right.broker_name))
            .then(left.broker_id.cmp(&right.broker_id))
    });

    sorted
        .into_iter()
        .take(TOP_LIMIT)
        .map(|item| DashboardBrokerTopItem {
            cluster_name: item.cluster_name,
            broker_name: item.broker_name,
            broker_id: item.broker_id,
            address: item.address,
            received_total: item.today_received_total,
        })
        .collect()
}

fn build_broker_tps(items: &[ClusterBrokerCardItem]) -> Vec<DashboardBrokerTpsItem> {
    let mut sorted = items.to_vec();
    sorted.sort_by(|left, right| {
        let left_total = left.produce_tps + left.consume_tps;
        let right_total = right.produce_tps + right.consume_tps;
        right_total
            .total_cmp(&left_total)
            .then(left.broker_name.cmp(&right.broker_name))
            .then(left.broker_id.cmp(&right.broker_id))
    });

    sorted
        .into_iter()
        .take(TOP_LIMIT)
        .map(|item| {
            let total_tps = item.produce_tps + item.consume_tps;
            DashboardBrokerTpsItem {
                cluster_name: item.cluster_name,
                broker_name: item.broker_name,
                broker_id: item.broker_id,
                address: item.address,
                produce_tps: item.produce_tps,
                consume_tps: item.consume_tps,
                total_tps,
            }
        })
        .collect()
}

fn build_topic_queue_top(items: &[TopicListItem]) -> Vec<DashboardTopicQueueItem> {
    let mut sorted = items.to_vec();
    sorted.sort_by(|left, right| {
        let left_total = left.read_queue_count + left.write_queue_count;
        let right_total = right.read_queue_count + right.write_queue_count;
        right_total.cmp(&left_total).then(left.topic.cmp(&right.topic))
    });

    sorted
        .into_iter()
        .take(TOP_LIMIT)
        .map(|item| DashboardTopicQueueItem {
            topic: item.topic,
            category: item.category,
            read_queue_count: item.read_queue_count,
            write_queue_count: item.write_queue_count,
            total_queue_count: item.read_queue_count + item.write_queue_count,
        })
        .collect()
}

fn build_topic_top(items: Vec<TopicCurrentStatsItem>, topic_items: &[TopicListItem]) -> Vec<DashboardTopicTopItem> {
    let java_collectable_topics = topic_items
        .iter()
        .filter(|item| !item.system_topic && !matches!(item.category.as_str(), "RETRY" | "DLQ"))
        .map(|item| item.topic.as_str())
        .collect::<HashSet<_>>();
    let mut sorted = items;
    sorted.retain(|item| java_collectable_topics.contains(item.topic.as_str()));
    sorted.sort_by(|left, right| right.total_msg.cmp(&left.total_msg).then(left.topic.cmp(&right.topic)));

    sorted
        .into_iter()
        .take(TOP_LIMIT)
        .map(|item| DashboardTopicTopItem {
            topic: item.topic,
            total_msg: item.total_msg,
            produced_msg_count_24h: item.produced_msg_count_24h,
            consumed_msg_count_24h: item.consumed_msg_count_24h,
            in_tps: item.in_tps,
            out_tps: item.out_tps,
            consumer_group_count: item.consumer_group_count,
        })
        .collect()
}

fn build_topic_category_distribution(items: &[TopicListItem]) -> Vec<DashboardTopicCategoryItem> {
    let mut counts = BTreeMap::<String, usize>::new();
    for item in items {
        *counts.entry(item.category.clone()).or_insert(0) += 1;
    }

    let mut distribution = counts
        .into_iter()
        .map(|(category, count)| DashboardTopicCategoryItem { category, count })
        .collect::<Vec<_>>();
    distribution.sort_by(|left, right| right.count.cmp(&left.count).then(left.category.cmp(&right.category)));
    distribution
}

#[cfg(test)]
mod tests {
    use super::build_broker_top;
    use super::build_broker_tps;
    use super::build_topic_category_distribution;
    use super::build_topic_queue_top;
    use super::build_topic_top;
    use crate::cluster::types::ClusterBrokerCardItem;
    use crate::topic::types::TopicCurrentStatsItem;
    use crate::topic::types::TopicListItem;
    use std::collections::BTreeMap;

    fn broker(name: &str, broker_id: u64, received: i64, produce_tps: f64, consume_tps: f64) -> ClusterBrokerCardItem {
        ClusterBrokerCardItem {
            cluster_name: "DefaultCluster".into(),
            broker_name: name.into(),
            broker_id,
            role: if broker_id == 0 { "MASTER" } else { "SLAVE" }.into(),
            address: format!("127.0.0.1:{}", 10911 + broker_id),
            version: "V5_4_0".into(),
            produce_tps,
            consume_tps,
            today_received_total: received,
            yesterday_produce: 0,
            yesterday_consume: 0,
            today_produce: 0,
            today_consume: 0,
            is_active: true,
            status_load_error: None,
            raw_status: BTreeMap::new(),
        }
    }

    fn topic(name: &str, category: &str, read_queue_count: u32, write_queue_count: u32) -> TopicListItem {
        topic_with_system_flag(name, category, read_queue_count, write_queue_count, false)
    }

    fn system_topic(name: &str) -> TopicListItem {
        topic_with_system_flag(name, "SYSTEM", 1, 1, true)
    }

    fn topic_with_system_flag(
        name: &str,
        category: &str,
        read_queue_count: u32,
        write_queue_count: u32,
        system_topic: bool,
    ) -> TopicListItem {
        TopicListItem {
            topic: name.into(),
            category: category.into(),
            message_type: category.into(),
            clusters: vec!["DefaultCluster".into()],
            brokers: vec!["broker-a".into()],
            read_queue_count,
            write_queue_count,
            perm: 6,
            order: false,
            system_topic,
        }
    }

    fn topic_stats(topic: &str, total_msg: u64) -> TopicCurrentStatsItem {
        TopicCurrentStatsItem {
            topic: topic.into(),
            total_msg,
            produced_msg_count_24h: total_msg + 10,
            consumed_msg_count_24h: total_msg,
            in_tps: 1.0,
            out_tps: 2.0,
            consumer_group_count: 1,
        }
    }

    #[test]
    fn broker_top_uses_received_total_descending() {
        let items = vec![broker("broker-b", 0, 20, 1.0, 2.0), broker("broker-a", 0, 50, 1.0, 1.0)];

        let top = build_broker_top(&items);

        assert_eq!(top[0].broker_name, "broker-a");
        assert_eq!(top[0].received_total, 50);
    }

    #[test]
    fn broker_tps_uses_combined_produce_and_consume_tps() {
        let items = vec![broker("broker-a", 0, 0, 1.0, 1.0), broker("broker-b", 0, 0, 5.0, 2.0)];

        let top = build_broker_tps(&items);

        assert_eq!(top[0].broker_name, "broker-b");
        assert_eq!(top[0].total_tps, 7.0);
    }

    #[test]
    fn topic_queue_top_uses_read_and_write_queue_count() {
        let items = vec![topic("topic-a", "NORMAL", 2, 2), topic("topic-b", "FIFO", 8, 4)];

        let top = build_topic_queue_top(&items);

        assert_eq!(top[0].topic, "topic-b");
        assert_eq!(top[0].total_queue_count, 12);
    }

    #[test]
    fn topic_top_uses_java_total_msg_descending() {
        let items = vec![topic_stats("topic-a", 20), topic_stats("topic-b", 50)];
        let topic_items = vec![topic("topic-a", "NORMAL", 2, 2), topic("topic-b", "NORMAL", 2, 2)];

        let top = build_topic_top(items, &topic_items);

        assert_eq!(top[0].topic, "topic-b");
        assert_eq!(top[0].total_msg, 50);
    }

    #[test]
    fn topic_top_filters_java_dashboard_non_collectable_topics() {
        let items = vec![
            topic_stats("TopicTest1111", 7),
            topic_stats("SCHEDULE_TOPIC_XXXX", 90),
            topic_stats("%RETRY%group-a", 80),
            topic_stats("%DLQ%group-a", 70),
        ];
        let topic_items = vec![
            topic("TopicTest1111", "NORMAL", 2, 2),
            system_topic("SCHEDULE_TOPIC_XXXX"),
            topic("%RETRY%group-a", "RETRY", 1, 1),
            topic("%DLQ%group-a", "DLQ", 1, 1),
        ];

        let top = build_topic_top(items, &topic_items);

        assert_eq!(top.len(), 1);
        assert_eq!(top[0].topic, "TopicTest1111");
        assert_eq!(top[0].total_msg, 7);
    }

    #[test]
    fn topic_category_distribution_counts_and_sorts_categories() {
        let items = vec![
            topic("topic-a", "NORMAL", 2, 2),
            topic("topic-b", "FIFO", 8, 4),
            topic("topic-c", "NORMAL", 1, 1),
        ];

        let distribution = build_topic_category_distribution(&items);

        assert_eq!(distribution[0].category, "NORMAL");
        assert_eq!(distribution[0].count, 2);
        assert_eq!(distribution[1].category, "FIFO");
        assert_eq!(distribution[1].count, 1);
    }
}
