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

use super::*;

pub(super) fn map_topic_route(topic: &str, route: &TopicRouteData) -> dashboard::DashboardTopicRoute {
    let mut brokers = route
        .broker_datas
        .iter()
        .map(|broker| dashboard::DashboardTopicRouteBroker {
            broker_name: broker.broker_name().to_string(),
            broker_addrs: {
                let mut addresses = broker
                    .broker_addrs()
                    .values()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                addresses.sort();
                addresses
            },
        })
        .collect::<Vec<_>>();
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    let mut queues = route
        .queue_datas
        .iter()
        .map(|queue| dashboard::DashboardTopicRouteQueue {
            broker_name: queue.broker_name().to_string(),
            read_queue_nums: queue.read_queue_nums(),
            write_queue_nums: queue.write_queue_nums(),
            perm: queue.perm(),
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    dashboard::DashboardTopicRoute {
        topic: topic.to_string(),
        brokers,
        queues,
    }
}

pub(super) fn topic_info_from_route(
    topic: &str,
    route: Option<&dashboard::DashboardTopicRoute>,
) -> dashboard::DashboardTopicInfo {
    let broker_name = route
        .and_then(|route| route.brokers.first())
        .map(|broker| broker.broker_name.clone());
    let read_queue_count = route
        .map(|route| route.queues.iter().map(|queue| queue.read_queue_nums).sum())
        .unwrap_or_default();
    let write_queue_count = route
        .map(|route| route.queues.iter().map(|queue| queue.write_queue_nums).sum())
        .unwrap_or_default();
    let perm = route
        .and_then(|route| route.queues.iter().map(|queue| queue.perm).max())
        .unwrap_or_default();
    dashboard::DashboardTopicInfo {
        topic: topic.to_string(),
        broker_name,
        read_queue_count,
        write_queue_count,
        perm,
        category: classify_topic(topic).to_string(),
    }
}

pub(super) fn map_topic_stats(topic: &str, stats: &TopicStatsTable) -> dashboard::DashboardTopicStats {
    let values = stats.get_offset_table().values();
    dashboard::DashboardTopicStats {
        topic: topic.to_string(),
        queue_count: stats.get_offset_table().len(),
        total_min_offset: values.clone().map(|offset| offset.get_min_offset()).sum(),
        total_max_offset: values.map(|offset| offset.get_max_offset()).sum(),
    }
}

pub(super) fn find_master_addr_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker| broker.broker_addrs().get(&MASTER_ID).cloned())
}

pub(super) fn master_targets_by_cluster_name(
    cluster_info: &ClusterInfo,
    cluster_name: &str,
) -> Result<Vec<(String, CheetahString)>, AdminError> {
    let cluster_table = cluster_info
        .cluster_addr_table
        .as_ref()
        .ok_or_else(|| AdminError::backend("examine_broker_cluster_info", "missing cluster address data"))?;
    let broker_table = cluster_info
        .broker_addr_table
        .as_ref()
        .ok_or_else(|| AdminError::backend("examine_broker_cluster_info", "missing broker address data"))?;
    let broker_names = cluster_table.get(cluster_name).ok_or_else(|| {
        AdminError::invalid_argument("clusterName", format!("cluster `{cluster_name}` was not found"))
    })?;
    let mut targets = broker_names
        .iter()
        .filter_map(|broker_name| {
            broker_table
                .get(broker_name)
                .and_then(|broker| broker.broker_addrs().get(&MASTER_ID))
                .map(|address| (broker_name.to_string(), address.clone()))
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(targets)
}

pub(super) fn classify_topic(topic: &str) -> &'static str {
    if topic.starts_with("%RETRY%") {
        "retry"
    } else if topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
        "dlq"
    } else if topic.starts_with("RMQ_SYS_") || topic == "TBW102" {
        "system"
    } else {
        "normal"
    }
}

pub(super) fn normalize_message_type(value: Option<&str>) -> &'static str {
    match value.unwrap_or("NORMAL").trim().to_ascii_uppercase().as_str() {
        "FIFO" => "FIFO",
        "DELAY" => "DELAY",
        "TRANSACTION" => "TRANSACTION",
        "UNSPECIFIED" => "UNSPECIFIED",
        _ => "NORMAL",
    }
}

pub(super) fn validate_delete_clusters(topic: &str, clusters: &[String]) -> Result<(), AdminError> {
    if clusters.is_empty() {
        Err(AdminError::invalid_argument(
            "topic",
            format!("topic `{topic}` has no cluster mapping to delete"),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn build_order_conf(brokers: &HashSet<String>, write_queue_nums: u32) -> String {
    let mut brokers = brokers.iter().collect::<Vec<_>>();
    brokers.sort();
    brokers
        .into_iter()
        .map(|broker| format!("{broker}:{write_queue_nums}"))
        .collect::<Vec<_>>()
        .join(";")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_is_trimmed_and_preserves_unspecified() {
        assert_eq!(normalize_message_type(Some(" FIFO ")), "FIFO");
        assert_eq!(normalize_message_type(Some("UNSPECIFIED")), "UNSPECIFIED");
        assert_eq!(normalize_message_type(Some("unknown")), "NORMAL");
    }

    #[test]
    fn deleting_a_topic_requires_at_least_one_cluster_mapping() {
        let error = validate_delete_clusters("TopicA", &[]).expect_err("empty clusters must fail");
        assert!(matches!(error, AdminError::InvalidArgument { .. }));
        assert!(validate_delete_clusters("TopicA", &["DefaultCluster".to_string()]).is_ok());
    }
}
