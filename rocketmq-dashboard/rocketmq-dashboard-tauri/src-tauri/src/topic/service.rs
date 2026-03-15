// Copyright 2023 The RocketMQ Rust Authors
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

use crate::nameserver::NameServerRuntimeState;
use crate::topic::admin::ManagedTopicAdmin;
use crate::topic::types::TopicBrokerAddressView;
use crate::topic::types::TopicConfigView;
use crate::topic::types::TopicConsumerGroupListResponse;
use crate::topic::types::TopicConsumerInfoResponse;
use crate::topic::types::TopicConsumerInfoView;
use crate::topic::types::TopicError;
use crate::topic::types::TopicListItem;
use crate::topic::types::TopicListResponse;
use crate::topic::types::TopicMutationResult;
use crate::topic::types::TopicResult;
use crate::topic::types::TopicRouteBrokerView;
use crate::topic::types::TopicRouteQueueView;
use crate::topic::types::TopicRouteView;
use crate::topic::types::TopicSendMessageResult;
use crate::topic::types::TopicStatusOffsetView;
use crate::topic::types::TopicStatusView;
use crate::topic::types::TopicTargetOption;
use cheetah_string::CheetahString;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::attribute::Attribute;
use rocketmq_common::common::attribute::topic_attributes::TopicAttributes;
use rocketmq_common::common::config::TopicConfig as RocketMqTopicConfig;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_dashboard_common::DeleteTopicRequest;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::ResetOffsetRequest;
use rocketmq_dashboard_common::SendTopicMessageRequest;
use rocketmq_dashboard_common::TopicConfigQueryRequest;
use rocketmq_dashboard_common::TopicConfigRequest;
use rocketmq_dashboard_common::TopicListRequest;
use rocketmq_dashboard_common::TopicQueryRequest;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct TopicManager {
    runtime: Arc<NameServerRuntimeState>,
}

#[derive(Clone, Debug)]
struct TopicBrokerConfigSnapshot {
    broker_name: String,
    cluster_name: Option<String>,
    config: RocketMqTopicConfig,
}

impl TopicManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self { runtime }
    }

    pub(crate) async fn get_topic_list(&self, request: TopicListRequest) -> TopicResult<TopicListResponse> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self
            .get_topic_list_with_admin(&mut managed.admin, &managed.snapshot, request)
            .await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn get_topic_route(&self, request: TopicQueryRequest) -> TopicResult<TopicRouteView> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self.get_topic_route_with_admin(&mut managed.admin, request).await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn get_topic_stats(&self, request: TopicQueryRequest) -> TopicResult<TopicStatusView> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self.get_topic_stats_with_admin(&mut managed.admin, request).await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn get_topic_config(&self, request: TopicConfigQueryRequest) -> TopicResult<TopicConfigView> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self.get_topic_config_with_admin(&mut managed.admin, request).await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn create_or_update_topic(&self, request: TopicConfigRequest) -> TopicResult<TopicMutationResult> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self
            .create_or_update_topic_with_admin(&mut managed.admin, request)
            .await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn delete_topic(&self, request: DeleteTopicRequest) -> TopicResult<TopicMutationResult> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self.delete_topic_with_admin(&mut managed.admin, request).await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn get_topic_consumer_groups(
        &self,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerGroupListResponse> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self
            .get_topic_consumer_groups_with_admin(&mut managed.admin, request)
            .await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn get_topic_consumers(
        &self,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerInfoResponse> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self.get_topic_consumers_with_admin(&mut managed.admin, request).await;
        managed.shutdown().await;
        result
    }

    pub(crate) async fn reset_consumer_offset(&self, request: ResetOffsetRequest) -> TopicResult<TopicMutationResult> {
        self.apply_offset_reset(request, false).await
    }

    pub(crate) async fn skip_message_accumulate(
        &self,
        request: ResetOffsetRequest,
    ) -> TopicResult<TopicMutationResult> {
        self.apply_offset_reset(request, true).await
    }

    pub(crate) async fn send_topic_message(
        &self,
        request: SendTopicMessageRequest,
    ) -> TopicResult<TopicSendMessageResult> {
        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let result = self
            .send_topic_message_with_admin(&mut managed.admin, &managed.snapshot, request)
            .await;
        managed.shutdown().await;
        result
    }

    async fn get_topic_list_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        snapshot: &NameServerConfigSnapshot,
        request: TopicListRequest,
    ) -> TopicResult<TopicListResponse> {
        let topic_list = admin
            .fetch_all_topic_list()
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        let targets = cluster_targets_from_cluster_info(&cluster_info);
        let topic_configs = collect_topic_configs(admin, &cluster_info).await?;

        let mut topics: Vec<String> = topic_list.topic_list.iter().map(|topic| topic.to_string()).collect();
        topics.sort();

        let mut items = Vec::new();
        for topic in topics {
            let config_snapshots = topic_configs.get(&topic).map(Vec::as_slice);
            let (category, message_type, system_topic) = classify_topic(&topic, config_snapshots);
            if request.skip_sys_process && system_topic {
                continue;
            }
            if request.skip_retry_and_dlq && matches!(category.as_str(), "RETRY" | "DLQ") {
                continue;
            }

            let route = admin
                .examine_topic_route_info(topic.clone().into())
                .await
                .map_err(|error| TopicError::RocketMQ(error.to_string()))?
                .unwrap_or_default();
            let (clusters, brokers, read_queue_count, write_queue_count, perm) = summarize_route(&route);

            items.push(TopicListItem {
                topic,
                category,
                message_type,
                clusters,
                brokers,
                read_queue_count,
                write_queue_count,
                perm,
                order: summarize_order(config_snapshots),
                system_topic,
            });
        }

        let response = TopicListResponse {
            total: items.len(),
            items,
            targets,
            current_namesrv: snapshot.current_namesrv.clone().unwrap_or_default(),
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        };
        Ok(response)
    }

    async fn get_topic_route_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicRouteView> {
        let route = require_topic_route(admin, &request.topic).await?;
        Ok(map_route_view(&request.topic, &route))
    }

    async fn get_topic_stats_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicStatusView> {
        let stats = admin
            .examine_topic_stats(request.topic.clone().into(), None)
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        Ok(map_status_view(&request.topic, &stats))
    }

    async fn get_topic_config_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: TopicConfigQueryRequest,
    ) -> TopicResult<TopicConfigView> {
        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        let route = require_topic_route(admin, &request.topic).await?;
        let broker_configs = collect_route_topic_configs(admin, &route, &request.topic).await?;
        let selected_config = match request.broker_name.as_deref() {
            Some(name) => broker_configs
                .iter()
                .find(|config| config.broker_name == name)
                .ok_or_else(|| TopicError::Validation(format!("Broker `{name}` was not found.")))?,
            None => broker_configs
                .first()
                .ok_or_else(|| TopicError::Validation(format!("Topic `{}` has no online broker.", request.topic)))?,
        };

        Ok(build_topic_config_view(
            &request.topic,
            &cluster_info,
            selected_config,
            &broker_configs,
        ))
    }

    async fn create_or_update_topic_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: TopicConfigRequest,
    ) -> TopicResult<TopicMutationResult> {
        if request.topic_name.trim().is_empty() {
            return Err(TopicError::Validation("Topic name is required.".into()));
        }
        if request.cluster_name_list.is_empty() && request.broker_name_list.is_empty() {
            return Err(TopicError::Validation(
                "Select at least one cluster or broker before saving the topic.".into(),
            ));
        }

        let cluster_info = admin
            .examine_broker_cluster_info()
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        let mut target_addrs = HashSet::new();
        let mut target_broker_names = HashSet::new();
        for cluster_name in &request.cluster_name_list {
            for (broker_name, broker_addr) in master_targets_by_cluster_name(&cluster_info, cluster_name)? {
                target_broker_names.insert(broker_name);
                target_addrs.insert(broker_addr);
            }
        }
        for broker_name in &request.broker_name_list {
            let addr = find_master_addr_by_broker_name(&cluster_info, broker_name).ok_or_else(|| {
                TopicError::Validation(format!(
                    "Broker `{broker_name}` was not found in the current cluster view."
                ))
            })?;
            target_broker_names.insert(broker_name.clone());
            target_addrs.insert(addr);
        }

        if target_addrs.is_empty() {
            return Err(TopicError::Validation(
                "No writable broker target could be resolved.".into(),
            ));
        }

        let mut attributes = HashMap::new();
        attributes.insert(
            TopicAttributes::topic_message_type_attribute().name().clone(),
            normalize_message_type(request.message_type.as_deref()).into(),
        );

        let topic_config = RocketMqTopicConfig {
            topic_name: Some(request.topic_name.clone().into()),
            read_queue_nums: request.read_queue_nums.max(1) as u32,
            write_queue_nums: request.write_queue_nums.max(1) as u32,
            perm: request.perm.max(0) as u32,
            order: request.order,
            attributes,
            ..RocketMqTopicConfig::default()
        };

        for broker_addr in &target_addrs {
            admin
                .create_and_update_topic_config(broker_addr.clone(), topic_config.clone())
                .await
                .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        }

        if request.order {
            let order_conf = build_order_conf(&target_broker_names, topic_config.write_queue_nums);
            admin
                .create_or_update_order_conf(request.topic_name.clone().into(), order_conf.into(), true)
                .await
                .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        }

        Ok(TopicMutationResult {
            success: true,
            message: format!("Topic `{}` was saved successfully.", request.topic_name),
            topic_name: Some(request.topic_name),
            affected_queues: None,
        })
    }

    async fn delete_topic_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: DeleteTopicRequest,
    ) -> TopicResult<TopicMutationResult> {
        let topic = request.topic.trim().to_string();
        if topic.is_empty() {
            return Err(TopicError::Validation("Topic name is required.".into()));
        }

        let clusters = if let Some(cluster_name) = request.cluster_name {
            vec![cluster_name]
        } else {
            let route = require_topic_route(admin, &topic).await?;
            let mut unique_clusters: Vec<String> = route
                .broker_datas
                .iter()
                .map(|broker| broker.cluster().to_string())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            unique_clusters.sort();
            unique_clusters
        };

        if clusters.is_empty() {
            return Err(TopicError::Validation(format!(
                "Topic `{topic}` has no cluster mapping to delete."
            )));
        }

        for cluster_name in &clusters {
            admin
                .delete_topic(topic.clone().into(), cluster_name.clone().into())
                .await
                .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        }

        Ok(TopicMutationResult {
            success: true,
            message: format!("Topic `{topic}` was deleted from {} cluster(s).", clusters.len()),
            topic_name: Some(topic),
            affected_queues: None,
        })
    }

    async fn get_topic_consumer_groups_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerGroupListResponse> {
        let groups = admin
            .query_topic_consume_by_who(request.topic.clone().into())
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
        let mut consumer_groups: Vec<String> = groups.group_list.into_iter().map(|group| group.to_string()).collect();
        consumer_groups.sort();
        Ok(TopicConsumerGroupListResponse {
            topic: request.topic,
            consumer_groups,
        })
    }

    async fn get_topic_consumers_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerInfoResponse> {
        let groups = self
            .get_topic_consumer_groups_with_admin(
                admin,
                TopicQueryRequest {
                    topic: request.topic.clone(),
                },
            )
            .await?;
        let mut items = Vec::new();
        for consumer_group in groups.consumer_groups {
            let stats = admin
                .examine_consume_stats(
                    consumer_group.clone().into(),
                    Some(request.topic.clone().into()),
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
            items.push(TopicConsumerInfoView {
                consumer_group,
                total_diff: stats.compute_total_diff(),
                inflight_diff: stats.compute_inflight_total_diff(),
                consume_tps: stats.get_consume_tps(),
            });
        }

        Ok(TopicConsumerInfoResponse {
            topic: request.topic,
            items,
        })
    }

    async fn apply_offset_reset(
        &self,
        request: ResetOffsetRequest,
        skip_accumulate: bool,
    ) -> TopicResult<TopicMutationResult> {
        if request.consumer_group_list.is_empty() {
            return Err(TopicError::Validation("Select at least one consumer group.".into()));
        }

        let mut managed = ManagedTopicAdmin::connect(&self.runtime).await?;
        let mut affected_queues = 0usize;
        for consumer_group in &request.consumer_group_list {
            let offsets = managed
                .admin
                .reset_offset_by_timestamp(
                    None,
                    request.topic.clone().into(),
                    consumer_group.clone().into(),
                    request.reset_time as u64,
                    request.force,
                )
                .await;
            match offsets {
                Ok(offsets) => {
                    affected_queues += offsets.len();
                }
                Err(error) if is_consumer_not_online_error(&error) => {
                    let rollback_stats = managed
                        .admin
                        .reset_offset_by_timestamp_old(
                            consumer_group.clone().into(),
                            request.topic.clone().into(),
                            request.reset_time as u64,
                            request.force,
                        )
                        .await
                        .map_err(|fallback_error| TopicError::RocketMQ(fallback_error.to_string()))?;
                    affected_queues += rollback_stats.len();
                }
                Err(error) => return Err(TopicError::RocketMQ(error.to_string())),
            }
        }
        managed.shutdown().await;

        Ok(TopicMutationResult {
            success: true,
            message: if skip_accumulate {
                format!(
                    "Skipped accumulated messages for {} consumer group(s).",
                    request.consumer_group_list.len()
                )
            } else {
                format!(
                    "Reset offsets for {} consumer group(s).",
                    request.consumer_group_list.len()
                )
            },
            topic_name: Some(request.topic),
            affected_queues: Some(affected_queues),
        })
    }

    async fn send_topic_message_with_admin(
        &self,
        admin: &mut DefaultMQAdminExt,
        snapshot: &NameServerConfigSnapshot,
        request: SendTopicMessageRequest,
    ) -> TopicResult<TopicSendMessageResult> {
        let topic_config = self
            .get_topic_config_with_admin(
                admin,
                TopicConfigQueryRequest {
                    topic: request.topic.clone(),
                    broker_name: None,
                },
            )
            .await?;
        if !matches!(topic_config.message_type.as_str(), "NORMAL" | "UNSPECIFIED") {
            return Err(TopicError::Validation(format!(
                "Desktop send is currently limited to NORMAL topics. `{}` is `{}`.",
                request.topic, topic_config.message_type
            )));
        }

        let current_namesrv = snapshot.current_namesrv.clone().ok_or_else(|| {
            TopicError::Configuration("No active NameServer is configured. Add and select a NameServer first.".into())
        })?;

        let mut client_config = ClientConfig::new();
        client_config.set_namesrv_addr(current_namesrv.into());
        client_config.set_vip_channel_enabled(snapshot.use_vip_channel);
        client_config.set_use_tls(snapshot.use_tls);

        let mut producer = DefaultMQProducer::builder()
            .producer_group(format!("dashboard-topic-sender-{}", Uuid::new_v4()))
            .client_config(client_config)
            .build();

        producer
            .start()
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;

        let mut builder = Message::builder()
            .topic(request.topic.clone())
            .body_slice(request.message_body.as_bytes())
            .trace_switch(request.trace_enabled);
        if !request.tag.trim().is_empty() {
            builder = builder.tags(request.tag.clone());
        }
        if !request.key.trim().is_empty() {
            builder = builder.key(request.key.clone());
        }

        let send_result = producer
            .send_with_timeout(builder.build_unchecked(), 5_000)
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()));
        producer.shutdown().await;
        let send_result = send_result?
            .ok_or_else(|| TopicError::RocketMQ("Broker acknowledged send without returning a result.".into()))?;

        Ok(TopicSendMessageResult {
            topic: request.topic,
            send_status: format!("{:?}", send_result.send_status),
            message_id: send_result.msg_id.map(|message_id| message_id.to_string()),
            broker_name: send_result
                .message_queue
                .as_ref()
                .map(|queue| queue.broker_name().to_string()),
            queue_id: send_result.message_queue.as_ref().map(|queue| queue.queue_id()),
            queue_offset: send_result.queue_offset,
            transaction_id: send_result.transaction_id,
            region_id: send_result.region_id,
        })
    }
}

fn classify_topic(topic: &str, configs: Option<&[TopicBrokerConfigSnapshot]>) -> (String, String, bool) {
    if topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
        return ("RETRY".into(), "RETRY".into(), false);
    }
    if topic.starts_with(mix_all::DLQ_GROUP_TOPIC_PREFIX) {
        return ("DLQ".into(), "DLQ".into(), false);
    }
    if TopicValidator::is_system_topic(topic) {
        return ("SYSTEM".into(), "SYSTEM".into(), true);
    }

    let message_type = configs
        .and_then(summarize_message_type)
        .unwrap_or_else(|| "UNSPECIFIED".into());
    (message_type.clone(), message_type, false)
}

fn normalize_message_type(message_type: Option<&str>) -> String {
    match message_type.unwrap_or("NORMAL").trim().to_uppercase().as_str() {
        "FIFO" => "FIFO".into(),
        "DELAY" => "DELAY".into(),
        "TRANSACTION" => "TRANSACTION".into(),
        "UNSPECIFIED" => "UNSPECIFIED".into(),
        _ => "NORMAL".into(),
    }
}

async fn require_topic_route(admin: &mut DefaultMQAdminExt, topic: &str) -> TopicResult<TopicRouteData> {
    admin
        .examine_topic_route_info(topic.to_string().into())
        .await
        .map_err(|error| TopicError::RocketMQ(error.to_string()))?
        .ok_or_else(|| TopicError::Validation(format!("Topic `{topic}` was not found.")))
}

fn summarize_route(route: &TopicRouteData) -> (Vec<String>, Vec<String>, u32, u32, i32) {
    let mut clusters: Vec<String> = route
        .broker_datas
        .iter()
        .map(|broker| broker.cluster().to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    clusters.sort();

    let mut brokers: Vec<String> = route
        .broker_datas
        .iter()
        .map(|broker| broker.broker_name().to_string())
        .collect();
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

fn summarize_message_type(configs: &[TopicBrokerConfigSnapshot]) -> Option<String> {
    let message_types: HashSet<String> = configs
        .iter()
        .map(|item| item.config.get_topic_message_type().to_string())
        .collect();
    if message_types.len() == 1 {
        message_types.into_iter().next()
    } else {
        Some("UNSPECIFIED".into())
    }
}

fn summarize_order(configs: Option<&[TopicBrokerConfigSnapshot]>) -> bool {
    configs
        .map(|items| items.iter().all(|item| item.config.order))
        .unwrap_or(false)
}

async fn collect_topic_configs(
    admin: &mut DefaultMQAdminExt,
    cluster_info: &ClusterInfo,
) -> TopicResult<HashMap<String, Vec<TopicBrokerConfigSnapshot>>> {
    let mut topic_configs = HashMap::new();
    for (broker_name, broker_addr) in collect_master_broker_targets(cluster_info) {
        let wrapper: TopicConfigSerializeWrapper = admin
            .get_all_topic_config(broker_addr, 5_000)
            .await
            .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
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
    for snapshots in topic_configs.values_mut() {
        snapshots.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    }
    Ok(topic_configs)
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

fn cluster_targets_from_cluster_info(cluster_info: &ClusterInfo) -> Vec<TopicTargetOption> {
    let mut items = Vec::new();
    if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
        for (cluster_name, broker_names) in cluster_addr_table {
            let mut brokers: Vec<String> = broker_names.iter().map(|item| item.to_string()).collect();
            brokers.sort();
            items.push(TopicTargetOption {
                cluster_name: cluster_name.to_string(),
                broker_names: brokers,
            });
        }
    }
    items.sort_by(|left, right| left.cluster_name.cmp(&right.cluster_name));
    items
}

fn find_master_addr_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<CheetahString> {
    cluster_info
        .broker_addr_table
        .as_ref()
        .and_then(|table| table.get(broker_name))
        .and_then(|broker_data| broker_data.broker_addrs().get(&mix_all::MASTER_ID).cloned())
}

fn find_cluster_name_by_broker_name(cluster_info: &ClusterInfo, broker_name: &str) -> Option<String> {
    cluster_info.cluster_addr_table.as_ref().and_then(|table| {
        table
            .iter()
            .find(|(_, broker_names)| broker_names.iter().any(|item| item.as_str() == broker_name))
            .map(|(cluster_name, _)| cluster_name.to_string())
    })
}

fn master_targets_by_cluster_name(
    cluster_info: &ClusterInfo,
    cluster_name: &str,
) -> TopicResult<Vec<(String, CheetahString)>> {
    let cluster_addr_table = cluster_info
        .cluster_addr_table
        .as_ref()
        .ok_or_else(|| TopicError::RocketMQ("NameServer did not return cluster address data.".into()))?;
    let broker_addr_table = cluster_info
        .broker_addr_table
        .as_ref()
        .ok_or_else(|| TopicError::RocketMQ("NameServer did not return broker address data.".into()))?;
    let broker_names = cluster_addr_table.get(cluster_name).ok_or_else(|| {
        TopicError::Validation(format!(
            "Cluster `{cluster_name}` was not found in the current NameServer view."
        ))
    })?;
    let mut addrs = Vec::new();
    for broker_name in broker_names {
        if let Some(master_addr) = broker_addr_table
            .get(broker_name)
            .and_then(|broker_data| broker_data.broker_addrs().get(&mix_all::MASTER_ID))
        {
            addrs.push((broker_name.to_string(), master_addr.clone()));
        }
    }
    addrs.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(addrs)
}

async fn collect_route_topic_configs(
    admin: &mut DefaultMQAdminExt,
    route: &TopicRouteData,
    topic: &str,
) -> TopicResult<Vec<TopicBrokerConfigSnapshot>> {
    let mut snapshots = Vec::new();
    for broker in &route.broker_datas {
        if let Some(master_addr) = broker.broker_addrs().get(&mix_all::MASTER_ID) {
            let config = admin
                .examine_topic_config(master_addr.clone(), topic.to_string().into())
                .await
                .map_err(|error| TopicError::RocketMQ(error.to_string()))?;
            snapshots.push(TopicBrokerConfigSnapshot {
                broker_name: broker.broker_name().to_string(),
                cluster_name: Some(broker.cluster().to_string()),
                config,
            });
        }
    }
    snapshots.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    Ok(snapshots)
}

fn build_topic_config_view(
    topic: &str,
    cluster_info: &ClusterInfo,
    selected_config: &TopicBrokerConfigSnapshot,
    all_configs: &[TopicBrokerConfigSnapshot],
) -> TopicConfigView {
    let mut broker_name_list: Vec<String> = all_configs.iter().map(|item| item.broker_name.clone()).collect();
    broker_name_list.sort();
    broker_name_list.dedup();

    let mut cluster_name_list: Vec<String> = all_configs
        .iter()
        .filter_map(|item| {
            item.cluster_name
                .clone()
                .or_else(|| find_cluster_name_by_broker_name(cluster_info, &item.broker_name))
        })
        .collect();
    cluster_name_list.sort();
    cluster_name_list.dedup();

    TopicConfigView {
        topic_name: topic.to_string(),
        broker_name: selected_config.broker_name.clone(),
        cluster_name: selected_config
            .cluster_name
            .clone()
            .or_else(|| find_cluster_name_by_broker_name(cluster_info, &selected_config.broker_name)),
        broker_name_list,
        cluster_name_list,
        read_queue_nums: selected_config.config.read_queue_nums as i32,
        write_queue_nums: selected_config.config.write_queue_nums as i32,
        perm: selected_config.config.perm as i32,
        order: selected_config.config.order,
        message_type: selected_config.config.get_topic_message_type().to_string(),
        attributes: selected_config
            .config
            .attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        inconsistent_fields: detect_inconsistent_topic_fields(all_configs),
    }
}

fn detect_inconsistent_topic_fields(configs: &[TopicBrokerConfigSnapshot]) -> Vec<String> {
    if configs.len() <= 1 {
        return Vec::new();
    }

    let baseline = &configs[0].config;
    let baseline_attributes: HashMap<String, String> = baseline
        .attributes
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let baseline_message_type = baseline.get_topic_message_type().to_string();
    let mut inconsistent_fields = Vec::new();

    if configs
        .iter()
        .any(|item| item.config.read_queue_nums != baseline.read_queue_nums)
    {
        inconsistent_fields.push("readQueueNums".into());
    }
    if configs
        .iter()
        .any(|item| item.config.write_queue_nums != baseline.write_queue_nums)
    {
        inconsistent_fields.push("writeQueueNums".into());
    }
    if configs.iter().any(|item| item.config.perm != baseline.perm) {
        inconsistent_fields.push("perm".into());
    }
    if configs.iter().any(|item| item.config.order != baseline.order) {
        inconsistent_fields.push("order".into());
    }
    if configs
        .iter()
        .any(|item| item.config.get_topic_message_type().to_string() != baseline_message_type)
    {
        inconsistent_fields.push("messageType".into());
    }
    if configs.iter().any(|item| {
        item.config
            .attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>()
            != baseline_attributes
    }) {
        inconsistent_fields.push("attributes".into());
    }

    inconsistent_fields
}

fn build_order_conf(broker_names: &HashSet<String>, write_queue_nums: u32) -> String {
    let mut ordered_brokers: Vec<String> = broker_names.iter().cloned().collect();
    ordered_brokers.sort();
    ordered_brokers
        .into_iter()
        .map(|broker_name| format!("{broker_name}:{write_queue_nums}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn is_consumer_not_online_error(error: &RocketMQError) -> bool {
    matches!(
        error,
        RocketMQError::BrokerOperationFailed { code, .. }
            if ResponseCode::from(*code) == ResponseCode::ConsumerNotOnline
    )
}

#[cfg(test)]
mod topic_config_tests {
    use super::*;

    fn snapshot(
        broker_name: &str,
        cluster_name: &str,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
        order: bool,
        message_type: &str,
    ) -> TopicBrokerConfigSnapshot {
        let mut config = RocketMqTopicConfig {
            read_queue_nums,
            write_queue_nums,
            perm,
            order,
            ..RocketMqTopicConfig::default()
        };
        config.attributes.insert(
            TopicAttributes::topic_message_type_attribute().name().clone(),
            message_type.into(),
        );
        TopicBrokerConfigSnapshot {
            broker_name: broker_name.to_string(),
            cluster_name: Some(cluster_name.to_string()),
            config,
        }
    }

    #[test]
    fn detect_inconsistent_topic_fields_reports_divergence() {
        let snapshots = vec![
            snapshot("broker-a", "DefaultCluster", 8, 8, 6, false, "NORMAL"),
            snapshot("broker-b", "DefaultCluster", 16, 8, 6, true, "FIFO"),
        ];

        let fields = detect_inconsistent_topic_fields(&snapshots);

        assert!(fields.contains(&"readQueueNums".to_string()));
        assert!(fields.contains(&"order".to_string()));
        assert!(fields.contains(&"messageType".to_string()));
    }

    #[test]
    fn summarize_message_type_returns_unspecified_for_mixed_configs() {
        let snapshots = vec![
            snapshot("broker-a", "DefaultCluster", 8, 8, 6, false, "NORMAL"),
            snapshot("broker-b", "DefaultCluster", 8, 8, 6, false, "FIFO"),
        ];

        assert_eq!(summarize_message_type(&snapshots), Some("UNSPECIFIED".to_string()));
    }

    #[test]
    fn build_order_conf_is_sorted_and_stable() {
        let broker_names = HashSet::from(["broker-b".to_string(), "broker-a".to_string()]);

        assert_eq!(build_order_conf(&broker_names, 8), "broker-a:8;broker-b:8");
    }
}

fn map_route_view(topic: &str, route: &TopicRouteData) -> TopicRouteView {
    let mut brokers = route
        .broker_datas
        .iter()
        .map(|broker| {
            let mut addresses = broker
                .broker_addrs()
                .iter()
                .map(|(broker_id, address)| TopicBrokerAddressView {
                    broker_id: *broker_id as i64,
                    address: address.to_string(),
                })
                .collect::<Vec<_>>();
            addresses.sort_by_key(|left| left.broker_id);
            TopicRouteBrokerView {
                cluster_name: broker.cluster().to_string(),
                broker_name: broker.broker_name().to_string(),
                addresses,
            }
        })
        .collect::<Vec<_>>();
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    let mut queues = route
        .queue_datas
        .iter()
        .map(|queue| TopicRouteQueueView {
            broker_name: queue.broker_name().to_string(),
            read_queue_nums: queue.read_queue_nums(),
            write_queue_nums: queue.write_queue_nums(),
            perm: queue.perm() as i32,
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));

    TopicRouteView {
        topic: topic.to_string(),
        brokers,
        queues,
    }
}

fn map_status_view(topic: &str, stats: &TopicStatsTable) -> TopicStatusView {
    let mut offsets = stats
        .get_offset_table()
        .iter()
        .map(|(queue, offset)| TopicStatusOffsetView {
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

    TopicStatusView {
        topic: topic.to_string(),
        total_message_count: offsets
            .iter()
            .map(|item| (item.max_offset - item.min_offset).max(0))
            .sum(),
        queue_count: offsets.len(),
        offsets,
    }
}

#[cfg(test)]
mod tests {
    use super::TopicBrokerConfigSnapshot;
    use super::classify_topic;
    use super::cluster_targets_from_cluster_info;
    use super::normalize_message_type;
    use rocketmq_common::common::config::TopicConfig as RocketMqTopicConfig;
    use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use std::collections::HashMap;
    use std::collections::HashSet;

    #[test]
    fn classify_special_topics_matches_dashboard_rules() {
        let retry = classify_topic("%RETRY%group-a", None);
        let dlq = classify_topic("%DLQ%group-a", None);
        let system = classify_topic("TBW102", None);

        assert_eq!(retry.0, "RETRY");
        assert_eq!(dlq.0, "DLQ");
        assert_eq!(system.0, "SYSTEM");
        assert!(system.2);
    }

    #[test]
    fn classify_regular_topics_uses_message_type_attribute() {
        let mut config = RocketMqTopicConfig::new("TopicTest");
        config.attributes.insert("message.type".into(), "FIFO".into());
        let snapshots = vec![TopicBrokerConfigSnapshot {
            broker_name: "broker-a".to_string(),
            cluster_name: Some("cluster-a".to_string()),
            config,
        }];

        let (category, message_type, system_topic) = classify_topic("TopicTest", Some(&snapshots));

        assert_eq!(category, "FIFO");
        assert_eq!(message_type, "FIFO");
        assert!(!system_topic);
    }

    #[test]
    fn normalize_message_type_defaults_to_normal() {
        assert_eq!(normalize_message_type(None), "NORMAL");
        assert_eq!(normalize_message_type(Some("delay")), "DELAY");
        assert_eq!(normalize_message_type(Some("unknown")), "NORMAL");
    }

    #[test]
    fn cluster_targets_are_sorted_for_forms() {
        let mut broker_addr_table = HashMap::new();
        broker_addr_table.insert(
            "broker-a".into(),
            BrokerData::new("cluster-a".into(), "broker-a".into(), HashMap::new(), None),
        );
        let mut cluster_addr_table = HashMap::new();
        cluster_addr_table.insert(
            "cluster-a".into(),
            HashSet::from_iter(["broker-b".into(), "broker-a".into()]),
        );

        let items =
            cluster_targets_from_cluster_info(&ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table)));

        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].broker_names,
            vec!["broker-a".to_string(), "broker-b".to_string()]
        );
    }
}
