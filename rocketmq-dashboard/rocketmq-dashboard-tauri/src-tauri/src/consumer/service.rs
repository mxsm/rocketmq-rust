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

use crate::consumer::admin::ManagedConsumerAdmin;
use crate::consumer::types::ConsumerConnectionItem;
use crate::consumer::types::ConsumerConnectionView;
use crate::consumer::types::ConsumerError;
use crate::consumer::types::ConsumerGroupListItem;
use crate::consumer::types::ConsumerGroupListResponse;
use crate::consumer::types::ConsumerGroupListSummary;
use crate::consumer::types::ConsumerResult;
use crate::consumer::types::ConsumerSubscriptionItem;
use crate::consumer::types::ConsumerTopicDetailItem;
use crate::consumer::types::ConsumerTopicDetailQueueItem;
use crate::consumer::types::ConsumerTopicDetailView;
use crate::nameserver::NameServerRuntimeState;
use cheetah_string::CheetahString;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_dashboard_common::ConsumerConnectionQueryRequest;
use rocketmq_dashboard_common::ConsumerGroupListRequest;
use rocketmq_dashboard_common::ConsumerGroupRefreshRequest;
use rocketmq_dashboard_common::ConsumerTopicDetailQueryRequest;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct ConsumerManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedConsumerAdmin>>>,
}

#[derive(Clone, Debug, Default)]
struct ConsumerGroupMeta {
    broker_addresses: HashSet<String>,
    orderly_flags: Vec<bool>,
}

impl ConsumerManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn query_consumer_groups(
        &self,
        request: ConsumerGroupListRequest,
    ) -> ConsumerResult<ConsumerGroupListResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let snapshot = session_guard
                .as_ref()
                .expect("consumer admin session should be initialized before use")
                .snapshot
                .clone();
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_groups_with_admin(&mut session.admin, &snapshot, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_groups failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_groups` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn refresh_consumer_group(
        &self,
        request: ConsumerGroupRefreshRequest,
    ) -> ConsumerResult<ConsumerGroupListItem> {
        if request.consumer_group.trim().is_empty() {
            return Err(ConsumerError::Validation("Consumer group is required.".into()));
        }

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.refresh_consumer_group_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "refresh_consumer_group failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `refresh_consumer_group` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn query_consumer_connection(
        &self,
        request: ConsumerConnectionQueryRequest,
    ) -> ConsumerResult<ConsumerConnectionView> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_connection_with_admin(&mut session.admin, &group_name, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_connection failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_connection` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn query_consumer_topic_detail(
        &self,
        request: ConsumerTopicDetailQueryRequest,
    ) -> ConsumerResult<ConsumerTopicDetailView> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;

        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("consumer admin session should be initialized before use");
                self.query_consumer_topic_detail_with_admin(&mut session.admin, &group_name, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_topic_detail failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_topic_detail` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedConsumerAdmin>) -> ConsumerResult<()> {
        let generation = self.runtime.generation();
        let needs_reconnect = session_slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(generation));

        if needs_reconnect {
            self.reset_admin_session(session_slot, "refreshing consumer admin session")
                .await;
            let session = ManagedConsumerAdmin::connect(&self.runtime).await?;
            log::info!(
                "Connected consumer admin session for namesrv `{}` at generation {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                session.generation
            );
            *session_slot = Some(session);
        }

        Ok(())
    }

    async fn reset_admin_session(&self, session_slot: &mut Option<ManagedConsumerAdmin>, reason: &str) {
        if let Some(mut session) = session_slot.take() {
            log::info!(
                "Shutting down consumer admin session for namesrv `{}`: {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                reason
            );
            session.shutdown().await;
        }
    }

    fn should_reset_session<T>(result: &ConsumerResult<T>) -> bool {
        match result {
            Err(ConsumerError::RocketMQ(message)) => is_reconnect_worthy_error(message),
            _ => false,
        }
    }

    async fn query_consumer_groups_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        snapshot: &NameServerConfigSnapshot,
        request: ConsumerGroupListRequest,
    ) -> ConsumerResult<ConsumerGroupListResponse> {
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
        let group_map = collect_consumer_group_meta(admin, &cluster_info).await?;
        let address = normalize_address(request.address.as_deref());

        let mut items = Vec::new();
        for (raw_group_name, meta) in group_map {
            items.push(
                build_consumer_group_item(admin, &raw_group_name, &meta, request.skip_sys_group, address.clone()).await,
            );
        }

        items.sort_by(|left, right| left.display_group_name.cmp(&right.display_group_name));
        let summary = build_summary(&items);

        Ok(ConsumerGroupListResponse {
            items,
            summary,
            current_namesrv: snapshot.current_namesrv.clone().unwrap_or_default(),
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        })
    }

    async fn refresh_consumer_group_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: ConsumerGroupRefreshRequest,
    ) -> ConsumerResult<ConsumerGroupListItem> {
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
        let group_map = collect_consumer_group_meta(admin, &cluster_info).await?;
        let raw_group_name = strip_system_prefix(&request.consumer_group);
        let meta = group_map.get(raw_group_name.as_str()).ok_or_else(|| {
            ConsumerError::Validation(format!(
                "Consumer group `{}` was not found in the current cluster view.",
                request.consumer_group
            ))
        })?;

        Ok(build_consumer_group_item(
            admin,
            raw_group_name.as_str(),
            meta,
            false,
            normalize_address(request.address.as_deref()),
        )
        .await)
    }

    async fn query_consumer_connection_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        raw_group_name: &str,
        request: ConsumerConnectionQueryRequest,
    ) -> ConsumerResult<ConsumerConnectionView> {
        let connection = admin
            .examine_consumer_connection_info(raw_group_name.into(), first_address(request.address.as_deref()))
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;

        Ok(build_consumer_connection_view(raw_group_name, connection))
    }

    async fn query_consumer_topic_detail_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        raw_group_name: &str,
        request: ConsumerTopicDetailQueryRequest,
    ) -> ConsumerResult<ConsumerTopicDetailView> {
        let broker_addresses = parse_addresses(request.address.as_deref());
        let stats = if broker_addresses.is_empty() {
            admin
                .examine_consume_stats(raw_group_name.into(), None, None, None, None)
                .await
                .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?
        } else {
            let mut merged_stats = ConsumeStats::new();
            for broker_addr in broker_addresses {
                let stats = admin
                    .examine_consume_stats(raw_group_name.into(), None, None, Some(broker_addr), Some(3_000))
                    .await
                    .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
                merged_stats.consume_tps += stats.get_consume_tps();
                merged_stats
                    .get_offset_table_mut()
                    .extend(stats.get_offset_table().clone());
            }
            merged_stats
        };

        let queue_client_map = collect_queue_client_mapping(admin, raw_group_name, stats.get_offset_table()).await;

        Ok(build_consumer_topic_detail_view(
            raw_group_name,
            stats,
            &queue_client_map,
        ))
    }
}

async fn collect_consumer_group_meta(
    admin: &mut DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> ConsumerResult<HashMap<String, ConsumerGroupMeta>> {
    let mut group_map = HashMap::new();
    let mut successful_brokers = 0usize;
    let mut last_error = None;
    for (_broker_name, broker_addr) in collect_master_broker_targets(cluster_info) {
        let wrapper = match admin.get_all_subscription_group(broker_addr.clone(), 5_000).await {
            Ok(wrapper) => {
                successful_brokers += 1;
                wrapper
            }
            Err(error) => {
                let message = error.to_string();
                log::warn!(
                    "Failed to fetch subscription groups from broker `{}` while collecting consumer groups: {}",
                    broker_addr,
                    message
                );
                last_error = Some(message);
                continue;
            }
        };
        for entry in wrapper.get_subscription_group_table().iter() {
            let group_name = entry.key().to_string();
            let config = entry.value();
            let meta = group_map.entry(group_name).or_insert_with(ConsumerGroupMeta::default);
            meta.broker_addresses.insert(broker_addr.to_string());
            meta.orderly_flags.push(config.consume_message_orderly());
        }
    }

    if successful_brokers == 0 {
        return Err(ConsumerError::RocketMQ(last_error.unwrap_or_else(|| {
            "No broker subscription metadata could be loaded.".to_string()
        })));
    }

    Ok(group_map)
}

async fn build_consumer_group_item(
    admin: &mut DefaultMQAdminExt,
    raw_group_name: &str,
    meta: &ConsumerGroupMeta,
    skip_sys_group: bool,
    address: Option<CheetahString>,
) -> ConsumerGroupListItem {
    let category = classify_consumer_group(raw_group_name, meta);
    let display_group_name = if category == "SYSTEM" && !skip_sys_group {
        format!("%SYS%{raw_group_name}")
    } else {
        raw_group_name.to_string()
    };

    let mut consume_tps = 0_i64;
    let mut diff_total = 0_i64;
    if let Ok(stats) = admin
        .examine_consume_stats(raw_group_name.into(), None, None, None, None)
        .await
    {
        consume_tps = stats.get_consume_tps().round() as i64;
        diff_total = stats.compute_total_diff();
    }

    let mut connection_count = 0_usize;
    let mut message_model = "UNKNOWN".to_string();
    let mut consume_type = "UNKNOWN".to_string();
    let mut version = None;
    let mut version_desc = "OFFLINE".to_string();

    if let Ok(connection) = admin
        .examine_consumer_connection_info(raw_group_name.into(), address)
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

    let mut broker_addresses: Vec<String> = meta.broker_addresses.iter().cloned().collect();
    broker_addresses.sort();

    ConsumerGroupListItem {
        display_group_name,
        raw_group_name: raw_group_name.to_string(),
        category,
        connection_count,
        consume_tps,
        diff_total,
        message_model,
        consume_type,
        version,
        version_desc,
        broker_addresses,
        update_timestamp: now_timestamp_millis(),
    }
}

fn build_summary(items: &[ConsumerGroupListItem]) -> ConsumerGroupListSummary {
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

fn classify_consumer_group(raw_group_name: &str, meta: &ConsumerGroupMeta) -> String {
    if is_system_consumer_group(raw_group_name) {
        "SYSTEM".to_string()
    } else if !meta.orderly_flags.is_empty() && meta.orderly_flags.iter().all(|flag| *flag) {
        "FIFO".to_string()
    } else {
        "NORMAL".to_string()
    }
}

fn collect_master_broker_targets(cluster_info: &ClusterInfo) -> Vec<(String, CheetahString)> {
    let mut targets = Vec::new();
    if let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() {
        for (broker_name, broker_data) in broker_addr_table {
            if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                targets.push((broker_name.to_string(), master_addr.clone()));
            }
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    targets
}

fn normalize_address(address: Option<&str>) -> Option<CheetahString> {
    address
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CheetahString::from)
}

fn parse_addresses(address: Option<&str>) -> Vec<CheetahString> {
    address
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(CheetahString::from)
        .collect()
}

fn first_address(address: Option<&str>) -> Option<CheetahString> {
    parse_addresses(address).into_iter().next()
}

fn strip_system_prefix(group_name: &str) -> String {
    group_name
        .strip_prefix("%SYS%")
        .unwrap_or(group_name)
        .trim()
        .to_string()
}

fn validate_consumer_group_name(group_name: &str) -> ConsumerResult<String> {
    let normalized = strip_system_prefix(group_name);
    if normalized.trim().is_empty() {
        Err(ConsumerError::Validation("Consumer group is required.".into()))
    } else {
        Ok(normalized)
    }
}

fn is_system_consumer_group(group_name: &str) -> bool {
    mix_all::is_sys_consumer_group(group_name)
        || matches!(
            group_name,
            mix_all::TOOLS_CONSUMER_GROUP
                | mix_all::FILTERSRV_CONSUMER_GROUP
                | mix_all::SELF_TEST_CONSUMER_GROUP
                | mix_all::ONS_HTTP_PROXY_GROUP
                | mix_all::CID_ONSAPI_PULL_GROUP
                | mix_all::CID_ONSAPI_PERMISSION_GROUP
                | mix_all::CID_ONSAPI_OWNER_GROUP
                | mix_all::CID_SYS_RMQ_TRANS
                | "CID_DefaultHeartBeatSyncerTopic"
        )
}

fn now_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn is_reconnect_worthy_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    [
        "connect",
        "connection refused",
        "timed out",
        "timeout",
        "channel inactive",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "name server",
        "namesrv",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn build_consumer_connection_view(raw_group_name: &str, connection: ConsumerConnection) -> ConsumerConnectionView {
    let mut connections: Vec<ConsumerConnectionItem> = connection
        .get_connection_set()
        .iter()
        .map(|item| ConsumerConnectionItem {
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            version_desc: RocketMqVersion::from_ordinal(item.get_version() as u32)
                .name()
                .to_string(),
        })
        .collect();
    connections.sort_by(|left, right| left.client_id.cmp(&right.client_id));

    let mut subscriptions: Vec<ConsumerSubscriptionItem> = connection
        .get_subscription_table()
        .values()
        .map(|item| ConsumerSubscriptionItem {
            topic: item.topic.to_string(),
            sub_string: item.sub_string.to_string(),
            expression_type: item.expression_type.to_string(),
            tags_set: item.tags_set.iter().map(|value| value.to_string()).collect(),
            code_set: item.code_set.iter().copied().collect(),
            sub_version: item.sub_version,
        })
        .collect();
    subscriptions.sort_by(|left, right| left.topic.cmp(&right.topic));

    ConsumerConnectionView {
        consumer_group: raw_group_name.to_string(),
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

async fn collect_queue_client_mapping(
    admin: &mut DefaultMQAdminExt,
    raw_group_name: &str,
    offset_table: &HashMap<MessageQueue, OffsetWrapper>,
) -> HashMap<MessageQueue, String> {
    let mut topics: Vec<String> = offset_table
        .keys()
        .map(|message_queue| message_queue.topic_str().to_string())
        .collect();
    topics.sort();
    topics.dedup();

    let mut queue_client_map = HashMap::new();
    for topic in topics {
        match admin
            .get_consume_status(topic.clone().into(), raw_group_name.into(), CheetahString::new())
            .await
        {
            Ok(client_offsets) => {
                for (client_id, offsets) in client_offsets {
                    for message_queue in offsets.into_keys() {
                        queue_client_map.insert(message_queue, client_id.to_string());
                    }
                }
            }
            Err(error) => {
                log::warn!(
                    "Failed to fetch consumer status for topic `{}` and group `{}` while building detail view: {}",
                    topic,
                    raw_group_name,
                    error
                );
            }
        }
    }

    queue_client_map
}

fn build_consumer_topic_detail_view(
    raw_group_name: &str,
    stats: ConsumeStats,
    queue_client_map: &HashMap<MessageQueue, String>,
) -> ConsumerTopicDetailView {
    let mut by_topic: HashMap<String, Vec<ConsumerTopicDetailQueueItem>> = HashMap::new();
    for (message_queue, offset) in stats.get_offset_table() {
        by_topic
            .entry(message_queue.topic_str().to_string())
            .or_default()
            .push(ConsumerTopicDetailQueueItem {
                broker_name: message_queue.broker_name().to_string(),
                queue_id: message_queue.queue_id(),
                broker_offset: offset.get_broker_offset(),
                consumer_offset: offset.get_consumer_offset(),
                diff_total: offset.get_broker_offset() - offset.get_consumer_offset(),
                client_info: queue_client_map.get(message_queue).cloned().unwrap_or_default(),
                last_timestamp: offset.get_last_timestamp(),
            });
    }

    let mut topics: Vec<ConsumerTopicDetailItem> = by_topic
        .into_iter()
        .map(|(topic, mut queue_stat_info_list)| {
            queue_stat_info_list.sort_by(|left, right| {
                left.broker_name
                    .cmp(&right.broker_name)
                    .then(left.queue_id.cmp(&right.queue_id))
            });
            let diff_total = queue_stat_info_list.iter().map(|item| item.diff_total).sum();
            let last_timestamp = queue_stat_info_list
                .iter()
                .map(|item| item.last_timestamp)
                .max()
                .unwrap_or_default();
            ConsumerTopicDetailItem {
                topic,
                diff_total,
                last_timestamp,
                queue_stat_info_list,
            }
        })
        .collect();
    topics.sort_by(|left, right| left.topic.cmp(&right.topic));

    ConsumerTopicDetailView {
        consumer_group: raw_group_name.to_string(),
        topic_count: topics.len(),
        total_diff: topics.iter().map(|item| item.diff_total).sum(),
        topics,
    }
}

#[cfg(test)]
mod tests {
    use super::ConsumerGroupMeta;
    use super::build_consumer_connection_view;
    use super::build_consumer_topic_detail_view;
    use super::build_summary;
    use super::classify_consumer_group;
    use super::is_reconnect_worthy_error;
    use super::is_system_consumer_group;
    use super::strip_system_prefix;
    use crate::consumer::types::ConsumerGroupListItem;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_remoting::protocol::LanguageCode;
    use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
    use rocketmq_remoting::protocol::body::connection::Connection;
    use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
    use std::collections::HashMap;

    #[test]
    fn classify_consumer_group_marks_system_fifo_and_normal() {
        let system = ConsumerGroupMeta::default();
        assert_eq!(classify_consumer_group("CID_RMQ_SYS_TRANS", &system), "SYSTEM");

        let fifo = ConsumerGroupMeta {
            broker_addresses: Default::default(),
            orderly_flags: vec![true, true],
        };
        assert_eq!(classify_consumer_group("group-fifo", &fifo), "FIFO");

        let normal = ConsumerGroupMeta {
            broker_addresses: Default::default(),
            orderly_flags: vec![true, false],
        };
        assert_eq!(classify_consumer_group("group-normal", &normal), "NORMAL");
    }

    #[test]
    fn strip_system_prefix_handles_prefixed_group_names() {
        assert_eq!(strip_system_prefix("%SYS%TOOLS_CONSUMER"), "TOOLS_CONSUMER");
        assert_eq!(strip_system_prefix("group-a"), "group-a");
    }

    #[test]
    fn build_summary_counts_categories() {
        let summary = build_summary(&[
            item("group-a", "NORMAL"),
            item("group-b", "FIFO"),
            item("%SYS%CID_RMQ_SYS_TRANS", "SYSTEM"),
        ]);

        assert_eq!(summary.total_groups, 3);
        assert_eq!(summary.normal_groups, 1);
        assert_eq!(summary.fifo_groups, 1);
        assert_eq!(summary.system_groups, 1);
    }

    #[test]
    fn is_system_consumer_group_matches_java_dashboard_extras() {
        assert!(is_system_consumer_group("TOOLS_CONSUMER"));
        assert!(is_system_consumer_group("CID_DefaultHeartBeatSyncerTopic"));
        assert!(!is_system_consumer_group("please_rename_unique_group_name"));
    }

    #[test]
    fn build_consumer_connection_view_maps_connections_and_subscriptions() {
        let mut connection = ConsumerConnection::new();

        let mut item = Connection::new();
        item.set_client_id(CheetahString::from("client-a"));
        item.set_client_addr(CheetahString::from("127.0.0.1:10911"));
        item.set_language(LanguageCode::JAVA);
        item.set_version(ordinal_for_v5_4_0());
        connection.insert_connection(item);
        connection.set_consume_type(ConsumeType::ConsumePassively);
        connection.set_message_model(MessageModel::Clustering);
        connection.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

        let subscription = SubscriptionData {
            topic: CheetahString::from("TopicTest"),
            sub_string: CheetahString::from("*"),
            expression_type: CheetahString::from("TAG"),
            ..Default::default()
        };
        connection
            .get_subscription_table_mut()
            .insert(CheetahString::from("TopicTest"), subscription);

        let view = build_consumer_connection_view("group-a", connection);

        assert_eq!(view.consumer_group, "group-a");
        assert_eq!(view.connection_count, 1);
        assert_eq!(view.consume_type, "PUSH");
        assert_eq!(view.message_model, "CLUSTERING");
        assert_eq!(view.consume_from_where, "CONSUME_FROM_LAST_OFFSET");
        assert_eq!(view.connections[0].version_desc, "V5_4_0");
        assert_eq!(view.subscriptions[0].topic, "TopicTest");
    }

    #[test]
    fn build_consumer_topic_detail_view_groups_queues_by_topic() {
        let mut stats = ConsumeStats::new();
        let mq_a = MessageQueue::from_parts("TopicA", "broker-a", 0);
        let mq_b = MessageQueue::from_parts("TopicA", "broker-a", 1);
        let mq_c = MessageQueue::from_parts("%RETRY%group-a", "broker-b", 0);

        let mut offset_a = OffsetWrapper::new();
        offset_a.set_broker_offset(120);
        offset_a.set_consumer_offset(20);
        offset_a.set_last_timestamp(1_700_000_000_000);

        let mut offset_b = OffsetWrapper::new();
        offset_b.set_broker_offset(220);
        offset_b.set_consumer_offset(120);
        offset_b.set_last_timestamp(1_700_000_100_000);

        let mut offset_c = OffsetWrapper::new();
        offset_c.set_broker_offset(20);
        offset_c.set_consumer_offset(10);
        offset_c.set_last_timestamp(1_700_000_200_000);

        stats.get_offset_table_mut().insert(mq_a, offset_a);
        stats.get_offset_table_mut().insert(mq_b, offset_b);
        stats.get_offset_table_mut().insert(mq_c, offset_c);

        let mut queue_client_map = HashMap::new();
        queue_client_map.insert(
            MessageQueue::from_parts("TopicA", "broker-a", 0),
            "client-a".to_string(),
        );
        queue_client_map.insert(
            MessageQueue::from_parts("TopicA", "broker-a", 1),
            "client-b".to_string(),
        );

        let view = build_consumer_topic_detail_view("group-a", stats, &queue_client_map);

        assert_eq!(view.consumer_group, "group-a");
        assert_eq!(view.topic_count, 2);
        assert_eq!(view.total_diff, 210);
        assert_eq!(view.topics[0].topic, "%RETRY%group-a");
        assert_eq!(view.topics[1].topic, "TopicA");
        assert_eq!(view.topics[1].diff_total, 200);
        assert_eq!(view.topics[1].queue_stat_info_list.len(), 2);
        assert_eq!(view.topics[1].queue_stat_info_list[0].client_info, "client-a");
        assert_eq!(view.topics[1].queue_stat_info_list[1].client_info, "client-b");
        assert_eq!(view.topics[0].queue_stat_info_list[0].client_info, "");
    }

    #[test]
    fn reconnect_error_detection_only_matches_transport_failures() {
        assert!(is_reconnect_worthy_error("connection refused by broker"));
        assert!(is_reconnect_worthy_error("request timeout while talking to namesrv"));
        assert!(!is_reconnect_worthy_error("consumer group not found"));
        assert!(!is_reconnect_worthy_error("subscription group metadata is empty"));
    }

    const fn ordinal_for_v5_4_0() -> i32 {
        493
    }

    fn item(group: &str, category: &str) -> ConsumerGroupListItem {
        ConsumerGroupListItem {
            display_group_name: group.to_string(),
            raw_group_name: group.to_string(),
            category: category.to_string(),
            connection_count: 0,
            consume_tps: 0,
            diff_total: 0,
            message_model: "UNKNOWN".to_string(),
            consume_type: "UNKNOWN".to_string(),
            version: None,
            version_desc: "OFFLINE".to_string(),
            broker_addresses: Vec::new(),
            update_timestamp: 0,
        }
    }
}
