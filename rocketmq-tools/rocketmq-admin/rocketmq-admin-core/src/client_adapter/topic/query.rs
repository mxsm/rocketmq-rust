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

pub(super) fn map_topic_route(route: TopicRouteData) -> TopicRoute {
    let mut brokers = route
        .broker_datas
        .iter()
        .map(|broker| TopicBroker {
            cluster: broker.cluster().to_string(),
            broker_name: broker.broker_name().to_string(),
            broker_addrs: broker
                .broker_addrs()
                .iter()
                .map(|(broker_id, address)| (*broker_id, address.to_string()))
                .collect::<BTreeMap<_, _>>(),
            zone_name: broker.zone_name().map(ToString::to_string),
            enable_acting_master: broker.enable_acting_master(),
        })
        .collect::<Vec<_>>();
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    let mut queues = route
        .queue_datas
        .iter()
        .map(|queue| TopicQueue {
            broker_name: queue.broker_name().to_string(),
            read_queue_nums: queue.read_queue_nums(),
            write_queue_nums: queue.write_queue_nums(),
            perm: queue.perm(),
            topic_sys_flag: queue.topic_sys_flag(),
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    TopicRoute { brokers, queues }
}

pub(super) async fn require_topic_route(
    admin: &mut rocketmq_client_rust::DefaultMQAdminExt,
    topic: &str,
) -> Result<TopicRouteData, AdminError> {
    admin
        .examine_topic_route_info(CheetahString::from(topic))
        .await
        .map_err(|error| backend_error("examine_topic_route_info", error))?
        .ok_or_else(|| AdminError::invalid_argument("topic", format!("topic `{topic}` was not found")))
}

pub(super) fn summarize_route(route: &TopicRouteData) -> (Vec<String>, Vec<String>, u32, u32, i32) {
    let mut clusters = route
        .broker_datas
        .iter()
        .map(|broker| broker.cluster().to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    clusters.sort();
    let mut brokers = route
        .broker_datas
        .iter()
        .map(|broker| broker.broker_name().to_string())
        .collect::<Vec<_>>();
    brokers.sort();
    brokers.dedup();
    let read_queue_count = route.queue_datas.iter().map(|queue| queue.read_queue_nums()).sum();
    let write_queue_count = route.queue_datas.iter().map(|queue| queue.write_queue_nums()).sum();
    let perm = route
        .queue_datas
        .first()
        .map(|queue| queue.perm() as i32)
        .unwrap_or_default();
    (clusters, brokers, read_queue_count, write_queue_count, perm)
}

pub(super) fn classify_topic(topic: &str, configs: Option<&[TopicBrokerConfigSnapshot]>) -> (String, String, bool) {
    if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
        return ("RETRY".to_string(), "RETRY".to_string(), false);
    }
    if topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
        return ("DLQ".to_string(), "DLQ".to_string(), false);
    }
    if is_dashboard_system_topic(topic) {
        return ("SYSTEM".to_string(), "SYSTEM".to_string(), true);
    }
    let message_type = configs
        .and_then(summarize_message_type)
        .unwrap_or_else(|| "UNSPECIFIED".to_string());
    (message_type.clone(), message_type, false)
}

pub(super) fn is_dashboard_system_topic(topic: &str) -> bool {
    topic == "TBW102" || is_system_topic(topic)
}

pub(super) fn summarize_message_type(configs: &[TopicBrokerConfigSnapshot]) -> Option<String> {
    let message_types = configs
        .iter()
        .map(|item| item.config.get_topic_message_type().to_string())
        .collect::<HashSet<_>>();
    if message_types.len() == 1 {
        message_types.into_iter().next()
    } else {
        Some("UNSPECIFIED".to_string())
    }
}

pub(super) fn summarize_order(configs: Option<&[TopicBrokerConfigSnapshot]>) -> bool {
    configs
        .map(|items| items.iter().all(|item| item.config.order))
        .unwrap_or(false)
}

pub(super) fn normalize_message_type(message_type: Option<&str>) -> String {
    match message_type.unwrap_or("NORMAL").trim().to_uppercase().as_str() {
        "FIFO" => "FIFO".to_string(),
        "DELAY" => "DELAY".to_string(),
        "TRANSACTION" => "TRANSACTION".to_string(),
        "UNSPECIFIED" => "UNSPECIFIED".to_string(),
        _ => "NORMAL".to_string(),
    }
}

pub(super) async fn collect_topic_configs(
    admin: &mut rocketmq_client_rust::DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> Result<HashMap<String, Vec<TopicBrokerConfigSnapshot>>, AdminError> {
    let mut topic_configs = HashMap::new();
    for (broker_name, broker_addr) in collect_master_broker_targets(cluster_info) {
        let wrapper: TopicConfigSerializeWrapper = admin
            .get_all_topic_config(broker_addr, SEND_TIMEOUT_MILLIS)
            .await
            .map_err(|error| backend_error("get_all_topic_config", error))?;
        if let Some(config_table) = wrapper.topic_config_table() {
            for (topic, config) in config_table {
                topic_configs
                    .entry(topic.to_string())
                    .or_insert_with(Vec::new)
                    .push(TopicBrokerConfigSnapshot {
                        broker_name: broker_name.clone(),
                        cluster_name: find_cluster_name_by_broker_name(cluster_info, &broker_name),
                        config: config.clone(),
                    });
            }
        }
    }
    for configs in topic_configs.values_mut() {
        configs.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    }
    Ok(topic_configs)
}

pub(super) fn collect_master_broker_targets(cluster_info: &ClusterInfo) -> Vec<(String, CheetahString)> {
    let mut targets = Vec::new();
    if let Some(table) = cluster_info.broker_addr_table.as_ref() {
        for (broker_name, broker) in table {
            if let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) {
                targets.push((broker_name.to_string(), master_addr.clone()));
            }
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    targets
}

pub(super) fn cluster_targets_from_cluster_info(cluster_info: &ClusterInfo) -> Vec<TopicTargetOption> {
    let mut targets = Vec::new();
    if let Some(table) = cluster_info.cluster_addr_table.as_ref() {
        for (cluster_name, broker_names) in table {
            let mut broker_names = broker_names.iter().map(ToString::to_string).collect::<Vec<_>>();
            broker_names.sort();
            targets.push(TopicTargetOption {
                cluster_name: cluster_name.to_string(),
                broker_names,
            });
        }
    }
    targets.sort_by(|left, right| left.cluster_name.cmp(&right.cluster_name));
    targets
}

pub(super) fn find_master_addr_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker| broker.broker_addrs().get(&MASTER_ID).cloned())
}

pub(super) fn find_cluster_name_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<String> {
    cluster_info.cluster_addr_table.as_ref().and_then(|table| {
        table
            .iter()
            .find(|(_, broker_names)| broker_names.iter().any(|item| item.as_str() == broker_name))
            .map(|(cluster_name, _)| cluster_name.to_string())
    })
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
        AdminError::invalid_argument(
            "clusterName",
            format!("cluster `{cluster_name}` was not found in the current NameServer view"),
        )
    })?;
    let mut targets = Vec::new();
    for broker_name in broker_names {
        if let Some(master_addr) = broker_table
            .get(broker_name)
            .and_then(|broker| broker.broker_addrs().get(&MASTER_ID))
        {
            targets.push((broker_name.to_string(), master_addr.clone()));
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(targets)
}

pub(super) async fn collect_route_topic_configs(
    admin: &mut rocketmq_client_rust::DefaultMQAdminExt,
    route: &TopicRouteData,
    topic: &str,
) -> Result<Vec<TopicBrokerConfigSnapshot>, AdminError> {
    let mut configs = Vec::new();
    for broker in &route.broker_datas {
        if let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) {
            let config = admin
                .examine_topic_config(master_addr.clone(), CheetahString::from(topic))
                .await
                .map_err(|error| backend_error("examine_topic_config", error))?;
            configs.push(TopicBrokerConfigSnapshot {
                broker_name: broker.broker_name().to_string(),
                cluster_name: Some(broker.cluster().to_string()),
                config,
            });
        }
    }
    configs.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    Ok(configs)
}

pub(super) fn build_topic_config_detail(
    topic: &str,
    cluster_info: &ClusterInfo,
    selected: &TopicBrokerConfigSnapshot,
    configs: &[TopicBrokerConfigSnapshot],
) -> TopicConfigDetail {
    let mut broker_name_list = configs
        .iter()
        .map(|config| config.broker_name.clone())
        .collect::<Vec<_>>();
    broker_name_list.sort();
    broker_name_list.dedup();
    let mut cluster_name_list = configs
        .iter()
        .filter_map(|config| {
            config
                .cluster_name
                .clone()
                .or_else(|| find_cluster_name_by_broker_name(cluster_info, &config.broker_name))
        })
        .collect::<Vec<_>>();
    cluster_name_list.sort();
    cluster_name_list.dedup();
    TopicConfigDetail {
        topic_name: topic.to_string(),
        broker_name: selected.broker_name.clone(),
        cluster_name: selected
            .cluster_name
            .clone()
            .or_else(|| find_cluster_name_by_broker_name(cluster_info, &selected.broker_name)),
        broker_name_list,
        cluster_name_list,
        read_queue_nums: selected.config.read_queue_nums as i32,
        write_queue_nums: selected.config.write_queue_nums as i32,
        perm: selected.config.perm as i32,
        order: selected.config.order,
        message_type: selected.config.get_topic_message_type().to_string(),
        attributes: selected
            .config
            .attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        inconsistent_fields: detect_inconsistent_topic_fields(configs),
    }
}

pub(super) fn detect_inconsistent_topic_fields(configs: &[TopicBrokerConfigSnapshot]) -> Vec<String> {
    let Some(baseline) = configs.first() else {
        return Vec::new();
    };
    if configs.len() == 1 {
        return Vec::new();
    }
    let baseline_attributes = baseline
        .config
        .attributes
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    let baseline_message_type = baseline.config.get_topic_message_type().to_string();
    let mut inconsistent_fields = Vec::new();
    if configs
        .iter()
        .any(|config| config.config.read_queue_nums != baseline.config.read_queue_nums)
    {
        inconsistent_fields.push("readQueueNums".to_string());
    }
    if configs
        .iter()
        .any(|config| config.config.write_queue_nums != baseline.config.write_queue_nums)
    {
        inconsistent_fields.push("writeQueueNums".to_string());
    }
    if configs.iter().any(|config| config.config.perm != baseline.config.perm) {
        inconsistent_fields.push("perm".to_string());
    }
    if configs
        .iter()
        .any(|config| config.config.order != baseline.config.order)
    {
        inconsistent_fields.push("order".to_string());
    }
    if configs
        .iter()
        .any(|config| config.config.get_topic_message_type().to_string() != baseline_message_type)
    {
        inconsistent_fields.push("messageType".to_string());
    }
    if configs.iter().any(|config| {
        config
            .config
            .attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>()
            != baseline_attributes
    }) {
        inconsistent_fields.push("attributes".to_string());
    }
    inconsistent_fields
}

pub(super) fn build_order_conf(broker_names: &HashSet<String>, write_queue_nums: u32) -> String {
    let mut broker_names = broker_names.iter().cloned().collect::<Vec<_>>();
    broker_names.sort();
    broker_names
        .into_iter()
        .map(|broker_name| format!("{broker_name}:{write_queue_nums}"))
        .collect::<Vec<_>>()
        .join(";")
}

pub(super) fn map_topic_stats(topic: &str, stats: &TopicStatsTable) -> TopicStats {
    let mut offsets = stats
        .get_offset_table()
        .iter()
        .map(|(queue, offset)| TopicQueueOffset {
            broker_name: queue.broker_name().to_string(),
            queue_id: queue.queue_id(),
            min_offset: offset.get_min_offset(),
            max_offset: offset.get_max_offset(),
            last_update_timestamp: offset.get_last_update_timestamp(),
        })
        .collect::<Vec<_>>();
    offsets.sort_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then(left.queue_id.cmp(&right.queue_id))
    });
    TopicStats {
        topic: topic.to_string(),
        total_message_count: offsets
            .iter()
            .map(|offset| (offset.max_offset - offset.min_offset).max(0))
            .sum(),
        queue_count: offsets.len(),
        offsets,
    }
}

pub(super) async fn collect_topic_current_detail(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    topic: &CheetahString,
) -> Result<Vec<TopicCurrentStatsRow>, AdminError> {
    let route = admin
        .examine_topic_route_info(topic.clone())
        .await
        .map_err(|error| backend_error("examine_topic_route_info", error))?;
    let group_list = admin
        .query_topic_consume_by_who(topic.clone())
        .await
        .map_err(|error| backend_error("query_topic_consume_by_who", error))?;
    let mut in_tps = 0.0;
    let mut in_msg_count_24h = 0;
    if let Some(route) = &route {
        for broker in &route.broker_datas {
            if let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) {
                if let Ok(stats) = admin
                    .view_broker_stats_data(
                        master_addr.clone(),
                        CheetahString::from_static_str(TOPIC_PUT_NUMS),
                        topic.clone(),
                    )
                    .await
                {
                    in_tps += stats.get_stats_minute().get_tps();
                    in_msg_count_24h += compute_24_hour_sum(&stats);
                }
            }
        }
    }

    let mut rows = Vec::new();
    if group_list.group_list.is_empty() {
        rows.push(TopicCurrentStatsRow {
            topic: topic.to_string(),
            consumer_group: None,
            in_tps,
            out_tps: None,
            in_msg_count_24h,
            out_msg_count_24h: None,
        });
        return Ok(rows);
    }

    for group in group_list.group_list {
        let mut out_tps = 0.0;
        let mut out_msg_count_24h = 0;
        if let Some(route) = &route {
            for broker in &route.broker_datas {
                if let Some(master_addr) = broker.broker_addrs().get(&MASTER_ID) {
                    let key = CheetahString::from(format!("{topic}@{group}"));
                    if let Ok(stats) = admin
                        .view_broker_stats_data(
                            master_addr.clone(),
                            CheetahString::from_static_str(GROUP_GET_NUMS),
                            key,
                        )
                        .await
                    {
                        out_tps += stats.get_stats_minute().get_tps();
                        out_msg_count_24h += compute_24_hour_sum(&stats);
                    }
                }
            }
        }
        rows.push(TopicCurrentStatsRow {
            topic: topic.to_string(),
            consumer_group: Some(group.to_string()),
            in_tps,
            out_tps: Some(out_tps),
            in_msg_count_24h,
            out_msg_count_24h: Some(out_msg_count_24h),
        });
    }
    Ok(rows)
}

pub(super) fn compute_24_hour_sum(stats: &BrokerStatsData) -> u64 {
    if stats.get_stats_day().get_sum() != 0 {
        return stats.get_stats_day().get_sum();
    }
    if stats.get_stats_hour().get_sum() != 0 {
        return stats.get_stats_hour().get_sum();
    }
    stats.get_stats_minute().get_sum()
}

pub(super) fn build_topic_current_stats_items(rows: Vec<TopicCurrentStatsRow>) -> Vec<TopicCurrentStatsItem> {
    let mut by_topic = BTreeMap::<String, TopicCurrentStatsAccumulator>::new();
    for row in rows {
        let entry = by_topic.entry(row.topic).or_default();
        entry.produced_msg_count_24h = entry.produced_msg_count_24h.max(row.in_msg_count_24h);
        entry.in_tps = entry.in_tps.max(row.in_tps);
        if let Some(out_msg_count_24h) = row.out_msg_count_24h {
            entry.consumed_msg_count_24h += out_msg_count_24h;
        }
        if let Some(out_tps) = row.out_tps {
            entry.out_tps += out_tps;
        }
        if row.consumer_group.is_some() {
            entry.consumer_group_count += 1;
        }
    }
    by_topic
        .into_iter()
        .map(|(topic, stats)| TopicCurrentStatsItem {
            topic,
            total_msg: stats.consumed_msg_count_24h,
            produced_msg_count_24h: stats.produced_msg_count_24h,
            consumed_msg_count_24h: stats.consumed_msg_count_24h,
            in_tps: stats.in_tps,
            out_tps: stats.out_tps,
            consumer_group_count: stats.consumer_group_count,
        })
        .collect()
}
