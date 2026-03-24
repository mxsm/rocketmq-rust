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
use crate::consumer::types::ConsumerConfigAttributeItem;
use crate::consumer::types::ConsumerConfigView;
use crate::consumer::types::ConsumerConnectionItem;
use crate::consumer::types::ConsumerConnectionView;
use crate::consumer::types::ConsumerError;
use crate::consumer::types::ConsumerGroupListItem;
use crate::consumer::types::ConsumerGroupListResponse;
use crate::consumer::types::ConsumerGroupListSummary;
use crate::consumer::types::ConsumerMutationResult;
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
use rocketmq_dashboard_common::ConsumerConfigQueryRequest;
use rocketmq_dashboard_common::ConsumerConnectionQueryRequest;
use rocketmq_dashboard_common::ConsumerCreateOrUpdateRequest;
use rocketmq_dashboard_common::ConsumerDeleteRequest;
use rocketmq_dashboard_common::ConsumerGroupListRequest;
use rocketmq_dashboard_common::ConsumerGroupRefreshRequest;
use rocketmq_dashboard_common::ConsumerTopicDetailQueryRequest;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::subscription::group_retry_policy::GroupRetryPolicy;
use rocketmq_remoting::protocol::subscription::group_retry_policy_type::GroupRetryPolicyType;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use serde_json::json;
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
    broker_names: HashSet<String>,
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

    pub(crate) async fn query_consumer_config(
        &self,
        request: ConsumerConfigQueryRequest,
    ) -> ConsumerResult<ConsumerConfigView> {
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
                self.query_consumer_config_with_admin(&mut session.admin, &group_name, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "query_consumer_config failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `query_consumer_config` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn create_or_update_consumer_group(
        &self,
        request: ConsumerCreateOrUpdateRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;
        validate_consumer_targets(&request.cluster_name_list, &request.broker_name_list)?;
        validate_consumer_limits(&request)?;

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;

        let result = {
            let session = session_guard
                .as_mut()
                .expect("consumer admin session should be initialized before use");
            self.create_or_update_consumer_group_with_admin(&mut session.admin, &group_name, request)
                .await
        };

        let should_reset = Self::should_reset_session(&result);
        if should_reset {
            self.reset_admin_session(&mut session_guard, "create_or_update_consumer_group failed")
                .await;
        }
        drop(session_guard);

        result
    }

    pub(crate) async fn delete_consumer_group(
        &self,
        request: ConsumerDeleteRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        let group_name = validate_consumer_group_name(&request.consumer_group)?;
        if request.broker_name_list.is_empty() {
            return Err(ConsumerError::Validation(
                "Select at least one broker before deleting the consumer group.".into(),
            ));
        }

        let mut session_guard = self.admin_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;

        let result = {
            let session = session_guard
                .as_mut()
                .expect("consumer admin session should be initialized before use");
            self.delete_consumer_group_with_admin(&mut session.admin, &group_name, request)
                .await
        };

        let should_reset = Self::should_reset_session(&result);
        if should_reset {
            self.reset_admin_session(&mut session_guard, "delete_consumer_group failed")
                .await;
        }
        drop(session_guard);

        result
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

    async fn query_consumer_config_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        raw_group_name: &str,
        request: ConsumerConfigQueryRequest,
    ) -> ConsumerResult<ConsumerConfigView> {
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;

        let broker_address = match normalize_address(request.address.as_deref()) {
            Some(address) => address,
            None => {
                let group_map = collect_consumer_group_meta(admin, &cluster_info).await?;
                let meta = group_map.get(raw_group_name).ok_or_else(|| {
                    ConsumerError::Validation(format!(
                        "Consumer group `{}` was not found in the current cluster view.",
                        raw_group_name
                    ))
                })?;
                resolve_first_consumer_broker_address(meta).ok_or_else(|| {
                    ConsumerError::Validation(format!(
                        "No broker address was found for consumer group `{}`.",
                        raw_group_name
                    ))
                })?
            }
        };

        let config = admin
            .examine_subscription_group_config(broker_address.clone(), raw_group_name.into())
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;

        let broker_name = resolve_broker_name_by_address(&cluster_info, broker_address.as_str()).unwrap_or_default();

        Ok(build_consumer_config_view(
            raw_group_name,
            broker_name,
            broker_address.to_string(),
            &config,
        ))
    }

    async fn create_or_update_consumer_group_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        raw_group_name: &str,
        request: ConsumerCreateOrUpdateRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
        let broker_names =
            resolve_consumer_target_broker_names(&cluster_info, &request.cluster_name_list, &request.broker_name_list)?;

        let mut config = SubscriptionGroupConfig::default();
        config.set_group_name(raw_group_name.into());
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
                ConsumerError::Validation(format!(
                    "Broker `{}` does not have a reachable master address.",
                    broker_name
                ))
            })?;
            admin
                .create_and_update_subscription_group_config(broker_addr, config.clone())
                .await
                .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
        }

        Ok(ConsumerMutationResult {
            consumer_group: raw_group_name.to_string(),
            broker_names,
            updated: true,
        })
    }

    async fn delete_consumer_group_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        raw_group_name: &str,
        request: ConsumerDeleteRequest,
    ) -> ConsumerResult<ConsumerMutationResult> {
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
        let group_map = collect_consumer_group_meta(admin, &cluster_info).await?;
        let meta = group_map.get(raw_group_name).ok_or_else(|| {
            ConsumerError::Validation(format!(
                "Consumer group `{}` was not found in the current cluster view.",
                raw_group_name
            ))
        })?;

        let mut selected_broker_names: Vec<String> = request
            .broker_name_list
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        selected_broker_names.sort();
        selected_broker_names.dedup();

        let current_broker_names = meta.broker_names.clone();
        let delete_in_namesrv = !current_broker_names.is_empty()
            && current_broker_names
                .iter()
                .all(|broker_name| selected_broker_names.contains(broker_name));

        for broker_name in &selected_broker_names {
            let broker_addr = resolve_master_broker_addr(&cluster_info, broker_name).ok_or_else(|| {
                ConsumerError::Validation(format!(
                    "Broker `{}` does not have a reachable master address.",
                    broker_name
                ))
            })?;

            admin
                .delete_subscription_group(broker_addr.clone(), raw_group_name.into(), Some(true))
                .await
                .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;

            for topic in consumer_internal_topics(raw_group_name) {
                let broker_targets = HashSet::from([broker_addr.clone()]);
                admin
                    .delete_topic_in_broker(broker_targets, topic.clone().into())
                    .await
                    .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;

                if delete_in_namesrv {
                    let namesrv_targets: HashSet<CheetahString> =
                        admin.get_name_server_address_list().await.into_iter().collect();
                    admin
                        .delete_topic_in_name_server(namesrv_targets, None, topic.into())
                        .await
                        .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;
                }
            }
        }

        Ok(ConsumerMutationResult {
            consumer_group: raw_group_name.to_string(),
            broker_names: selected_broker_names,
            updated: false,
        })
    }
}

async fn collect_consumer_group_meta(
    admin: &mut DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> ConsumerResult<HashMap<String, ConsumerGroupMeta>> {
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
            meta.broker_names.insert(broker_name.clone());
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
    let mut broker_names: Vec<String> = meta.broker_names.iter().cloned().collect();
    broker_names.sort();

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
        broker_names,
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

fn resolve_first_consumer_broker_address(meta: &ConsumerGroupMeta) -> Option<CheetahString> {
    let mut broker_addresses: Vec<&String> = meta.broker_addresses.iter().collect();
    broker_addresses.sort();
    broker_addresses
        .first()
        .map(|value| CheetahString::from(value.as_str()))
}

fn resolve_master_broker_addr(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker_data| broker_data.broker_addrs().get(&mix_all::MASTER_ID).cloned())
}

fn resolve_consumer_target_broker_names(
    cluster_info: &ClusterInfo,
    cluster_name_list: &[String],
    broker_name_list: &[String],
) -> ConsumerResult<Vec<String>> {
    let mut targets = HashSet::new();

    if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
        for cluster_name in cluster_name_list {
            let normalized = cluster_name.trim();
            if normalized.is_empty() {
                continue;
            }
            let broker_names = cluster_addr_table.get(normalized).ok_or_else(|| {
                ConsumerError::Validation(format!(
                    "Cluster `{}` was not found in the current cluster view.",
                    normalized
                ))
            })?;
            for broker_name in broker_names {
                targets.insert(broker_name.to_string());
            }
        }
    }

    for broker_name in broker_name_list {
        let normalized = broker_name.trim();
        if normalized.is_empty() {
            continue;
        }
        if resolve_master_broker_addr(cluster_info, normalized).is_none() {
            return Err(ConsumerError::Validation(format!(
                "Broker `{}` was not found in the current cluster view.",
                normalized
            )));
        }
        targets.insert(normalized.to_string());
    }

    if targets.is_empty() {
        return Err(ConsumerError::Validation(
            "Select at least one cluster or broker before saving the consumer group.".into(),
        ));
    }

    let mut values: Vec<String> = targets.into_iter().collect();
    values.sort();
    Ok(values)
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

fn validate_consumer_targets(cluster_name_list: &[String], broker_name_list: &[String]) -> ConsumerResult<()> {
    if cluster_name_list.iter().all(|value| value.trim().is_empty())
        && broker_name_list.iter().all(|value| value.trim().is_empty())
    {
        return Err(ConsumerError::Validation(
            "Select at least one cluster or broker before saving the consumer group.".into(),
        ));
    }
    Ok(())
}

fn validate_consumer_limits(request: &ConsumerCreateOrUpdateRequest) -> ConsumerResult<()> {
    if request.retry_queue_nums < 0 {
        return Err(ConsumerError::Validation(
            "Retry queues must be zero or greater.".into(),
        ));
    }
    if request.retry_max_times < -1 {
        return Err(ConsumerError::Validation("Max retries must be -1 or greater.".into()));
    }
    if request.consume_timeout_minute <= 0 {
        return Err(ConsumerError::Validation(
            "Consume timeout must be greater than zero.".into(),
        ));
    }
    Ok(())
}

fn consumer_internal_topics(raw_group_name: &str) -> [String; 2] {
    [
        format!("{}{}", mix_all::RETRY_GROUP_TOPIC_PREFIX, raw_group_name),
        format!("{}{}", mix_all::DLQ_GROUP_TOPIC_PREFIX, raw_group_name),
    ]
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

fn build_consumer_config_view(
    raw_group_name: &str,
    broker_name: String,
    broker_address: String,
    config: &SubscriptionGroupConfig,
) -> ConsumerConfigView {
    let mut subscription_topics: Vec<String> = config
        .subscription_data_set()
        .into_iter()
        .flat_map(|items| items.iter())
        .map(|item| item.topic().to_string())
        .collect();
    subscription_topics.sort();
    subscription_topics.dedup();

    let mut attributes: Vec<ConsumerConfigAttributeItem> = config
        .attributes()
        .iter()
        .map(|(key, value)| ConsumerConfigAttributeItem {
            key: key.to_string(),
            value: value.to_string(),
        })
        .collect();
    attributes.sort_by(|left, right| left.key.cmp(&right.key));

    ConsumerConfigView {
        consumer_group: raw_group_name.to_string(),
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
        subscription_topic_count: subscription_topics.len(),
        subscription_topics,
        attributes,
    }
}

fn resolve_broker_name_by_address(cluster_info: &ClusterInfo, broker_address: &str) -> Option<String> {
    cluster_info.broker_addr_table.as_ref().and_then(|table| {
        table.iter().find_map(|(broker_name, broker_data)| {
            broker_data
                .broker_addrs()
                .values()
                .any(|addr| addr.as_str() == broker_address)
                .then(|| broker_name.to_string())
        })
    })
}

fn serialize_group_retry_policy_json(policy: &GroupRetryPolicy) -> Result<String, serde_json::Error> {
    let effective_retry_policy = match policy.type_() {
        GroupRetryPolicyType::Exponential => json!({
            "type": "EXPONENTIAL",
            "initial": policy
                .exponential_retry_policy()
                .map(|value| value.initial())
                .unwrap_or(5_000),
            "max": policy
                .exponential_retry_policy()
                .map(|value| value.max())
                .unwrap_or(7_200_000),
            "multiplier": policy
                .exponential_retry_policy()
                .map(|value| value.multiplier())
                .unwrap_or(2),
        }),
        GroupRetryPolicyType::Customized => json!({
            "type": "CUSTOMIZED",
            "next": policy
                .customized_retry_policy()
                .map(|value| value.next().to_vec())
                .unwrap_or_else(|| vec![
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
        "retryPolicy": effective_retry_policy,
    }))
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
    use super::build_consumer_config_view;
    use super::build_consumer_connection_view;
    use super::build_consumer_topic_detail_view;
    use super::build_summary;
    use super::classify_consumer_group;
    use super::is_reconnect_worthy_error;
    use super::is_system_consumer_group;
    use super::resolve_broker_name_by_address;
    use super::resolve_first_consumer_broker_address;
    use super::strip_system_prefix;
    use crate::consumer::types::ConsumerGroupListItem;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_remoting::protocol::LanguageCode;
    use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
    use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_remoting::protocol::body::connection::Connection;
    use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    use std::collections::HashMap;
    use std::collections::HashSet;

    #[test]
    fn classify_consumer_group_marks_system_fifo_and_normal() {
        let system = ConsumerGroupMeta::default();
        assert_eq!(classify_consumer_group("CID_RMQ_SYS_TRANS", &system), "SYSTEM");

        let fifo = ConsumerGroupMeta {
            broker_names: Default::default(),
            broker_addresses: Default::default(),
            orderly_flags: vec![true, true],
        };
        assert_eq!(classify_consumer_group("group-fifo", &fifo), "FIFO");

        let normal = ConsumerGroupMeta {
            broker_names: Default::default(),
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

    #[test]
    fn resolve_first_consumer_broker_address_returns_sorted_first_value() {
        let meta = ConsumerGroupMeta {
            broker_names: HashSet::new(),
            broker_addresses: HashSet::from(["172.19.80.2:10911".to_string(), "172.19.80.1:10911".to_string()]),
            orderly_flags: Vec::new(),
        };

        let resolved = resolve_first_consumer_broker_address(&meta).expect("expected a broker address");

        assert_eq!(resolved.as_str(), "172.19.80.1:10911");
    }

    #[test]
    fn build_consumer_config_view_maps_subscription_group_config_fields() {
        let mut config = SubscriptionGroupConfig::default();
        config.set_group_name("group-a".into());
        config.set_consume_enable(false);
        config.set_consume_from_min_enable(false);
        config.set_consume_broadcast_enable(true);
        config.set_consume_message_orderly(true);
        config.set_retry_queue_nums(4);
        config.set_retry_max_times(8);
        config.set_broker_id(1);
        config.set_which_broker_when_consume_slowly(2);
        config.set_notify_consumer_ids_changed_enable(false);
        config.set_group_sys_flag(7);
        config.set_consume_timeout_minute(21);
        config.set_attributes(HashMap::from([("a".into(), "1".into()), ("b".into(), "2".into())]));
        config.set_subscription_data_set(Some(HashSet::from([
            rocketmq_remoting::protocol::subscription::simple_subscription_data::SimpleSubscriptionData::new(
                "TopicB".to_string(),
                "TAG".to_string(),
                "*".to_string(),
                1,
            ),
            rocketmq_remoting::protocol::subscription::simple_subscription_data::SimpleSubscriptionData::new(
                "TopicA".to_string(),
                "TAG".to_string(),
                "*".to_string(),
                2,
            ),
        ])));

        let view = build_consumer_config_view(
            "group-a",
            "broker-a".to_string(),
            "172.19.80.1:10911".to_string(),
            &config,
        );

        assert_eq!(view.consumer_group, "group-a");
        assert_eq!(view.broker_name, "broker-a");
        assert_eq!(view.broker_address, "172.19.80.1:10911");
        assert!(!view.consume_enable);
        assert!(!view.consume_from_min_enable);
        assert!(view.consume_broadcast_enable);
        assert!(view.consume_message_orderly);
        assert_eq!(view.retry_queue_nums, 4);
        assert_eq!(view.retry_max_times, 8);
        assert_eq!(view.broker_id, 1);
        assert_eq!(view.which_broker_when_consume_slowly, 2);
        assert!(!view.notify_consumer_ids_changed_enable);
        assert_eq!(view.group_sys_flag, 7);
        assert_eq!(view.consume_timeout_minute, 21);
        assert_eq!(view.subscription_topic_count, 2);
        assert_eq!(
            view.subscription_topics,
            vec!["TopicA".to_string(), "TopicB".to_string()]
        );
        assert_eq!(view.attributes.len(), 2);
        assert_eq!(view.attributes[0].key, "a");
        assert!(view.group_retry_policy_json.contains("\"retryPolicy\""));
        assert!(view.group_retry_policy_json.contains("\"CUSTOMIZED\""));
    }

    #[test]
    fn resolve_broker_name_by_address_finds_matching_broker() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, "172.19.80.1:10911".into());
        let broker_data = BrokerData::new("DefaultCluster".into(), "broker-a".into(), broker_addrs, None);

        let cluster_info = ClusterInfo::new(Some(HashMap::from([("broker-a".into(), broker_data)])), None);

        let resolved = resolve_broker_name_by_address(&cluster_info, "172.19.80.1:10911");
        assert_eq!(resolved.as_deref(), Some("broker-a"));
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
            broker_names: Vec::new(),
            broker_addresses: Vec::new(),
            update_timestamp: 0,
        }
    }
}
