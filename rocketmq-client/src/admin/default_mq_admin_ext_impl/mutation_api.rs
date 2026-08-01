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

//! Direct implementation of the explicit mutation capability.
//!
//! This module deliberately talks to the concrete client APIs instead of
//! routing mutation-only builds through the scoped administration traits.

mod log_filter;

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rand::seq::IndexedRandom;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::attribute::attribute_parser::AttributeParser;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_protocol::protocol::admin::topic_offset::TopicOffset;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader;
use rocketmq_protocol::protocol::header::delete_topic_request_header::DeleteTopicRequestHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_info_request_header::GetTopicStatsInfoRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use crate::admin::mq_admin_mutation_ext::BrokerConfigPatchOutcome;
use crate::admin::mq_admin_mutation_ext::MQAdminMutationExt;
use crate::admin::mq_admin_mutation_ext::SubscriptionGroupConfigPatch;
use crate::admin::mq_admin_mutation_ext::SubscriptionGroupConfigPatchOutcome;
use crate::admin::mq_admin_mutation_ext::TopicConfigPatch;
use crate::admin::mq_admin_mutation_ext::TopicConfigPatchOutcome;

use super::DefaultMQAdminExtImpl;
use super::NAMESPACE_ORDER_TOPIC_CONFIG;

fn timestamp_to_java_long(operation: &'static str, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
    i64::try_from(timestamp)
        .map_err(|_| RocketMQError::illegal_argument(format!("{operation} timestamp exceeds Java long range")))
}

fn java_long_to_u64(operation: &'static str, field: &'static str, value: i64) -> rocketmq_error::RocketMQResult<u64> {
    u64::try_from(value).map_err(|_| {
        RocketMQError::illegal_argument(format!(
            "{operation} {field} is negative and cannot be represented as Rust u64"
        ))
    })
}

fn encode_topic_attributes(attributes: &HashMap<CheetahString, CheetahString>) -> Option<CheetahString> {
    if attributes.is_empty() {
        return None;
    }
    let serialized = AttributeParser::parse_to_string(
        &attributes
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>(),
    );
    (!serialized.is_empty()).then(|| serialized.into())
}

fn merge_order_conf_entries(existing: &str, value: &str) -> String {
    let mut entries = HashMap::new();
    for item in existing.split(';').filter(|item| !item.trim().is_empty()) {
        if let Some((broker_name, _)) = item.split_once(':') {
            entries.insert(broker_name.to_string(), item.to_string());
        }
    }
    if let Some((broker_name, _)) = value.split_once(':') {
        entries.insert(broker_name.to_string(), value.to_string());
    } else if !value.trim().is_empty() {
        entries.insert(value.to_string(), value.to_string());
    }
    let mut broker_names = entries.keys().cloned().collect::<Vec<_>>();
    broker_names.sort();
    broker_names
        .into_iter()
        .filter_map(|broker_name| entries.remove(&broker_name))
        .collect::<Vec<_>>()
        .join(";")
}

fn select_consumer_direct_connection(
    consumer_group: &CheetahString,
    consumer_connection: &ConsumerConnection,
    requested_client_id: Option<&CheetahString>,
) -> rocketmq_error::RocketMQResult<(CheetahString, CheetahString)> {
    let requested = requested_client_id.filter(|client_id| !client_id.is_empty());
    let connection = consumer_connection
        .get_connection_set()
        .iter()
        .find(|connection| {
            requested
                .map(|client_id| connection.get_client_id() == *client_id)
                .unwrap_or_else(|| !connection.get_client_id().is_empty())
        })
        .ok_or_else(|| {
            let message = requested
                .map(|client_id| {
                    format!(
                        "Client `{}` was not found in consumer group `{}`",
                        client_id, consumer_group
                    )
                })
                .unwrap_or_else(|| format!("NO CONSUMER for consumer group `{consumer_group}`"));
            RocketMQError::IllegalArgument(message)
        })?;
    Ok((connection.get_client_id(), connection.get_client_addr()))
}

impl DefaultMQAdminExtImpl {
    async fn mutation_reset_offset_on_broker(
        &self,
        broker_addr: CheetahString,
        queue_data: &QueueData,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let consume_stats = self
            .mq_client_api()?
            .get_consume_stats(
                &broker_addr,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: CheetahString::empty(),
                    topic_request_header: None,
                },
                self.remoting_timeout_millis()?,
            )
            .await?;
        let mut rollback_stats = Vec::new();
        let mut has_consumed = false;
        for (queue, offset_wrapper) in &consume_stats.offset_table {
            if queue.topic() == &topic {
                has_consumed = true;
                rollback_stats.push(
                    self.mutation_reset_queue_offset(
                        broker_addr.clone(),
                        consumer_group.clone(),
                        queue.clone(),
                        offset_wrapper,
                        timestamp,
                        force,
                    )
                    .await?,
                );
            }
        }
        if !has_consumed {
            let topic_status = self
                .mq_client_api()?
                .get_topic_stats_info(
                    &broker_addr,
                    GetTopicStatsInfoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    },
                    self.remoting_timeout_millis()?,
                )
                .await?;
            for queue_id in 0..queue_data.read_queue_nums() {
                let queue = MessageQueue::from_parts(topic.clone(), queue_data.broker_name().clone(), queue_id as i32);
                let mut offset_wrapper = OffsetWrapper::new();
                let topic_offset = topic_status
                    .get_offset_table()
                    .get(&queue)
                    .cloned()
                    .unwrap_or_else(TopicOffset::new);
                offset_wrapper.set_broker_offset(topic_offset.get_max_offset());
                offset_wrapper.set_consumer_offset(topic_offset.get_min_offset());
                rollback_stats.push(
                    self.mutation_reset_queue_offset(
                        broker_addr.clone(),
                        consumer_group.clone(),
                        queue,
                        &offset_wrapper,
                        timestamp,
                        force,
                    )
                    .await?,
                );
            }
        }
        Ok(rollback_stats)
    }

    async fn mutation_reset_queue_offset(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        queue: MessageQueue,
        offset_wrapper: &OffsetWrapper,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<RollbackStats> {
        let reset_offset = if timestamp == -1 {
            self.mq_client_api()?
                .get_max_offset(broker_addr.as_str(), &queue, self.remoting_timeout_millis()?)
                .await?
        } else {
            self.mq_client_api()?
                .search_offset_by_timestamp(
                    broker_addr.as_str(),
                    &queue,
                    timestamp,
                    rocketmq_model::common::boundary_type::BoundaryType::Lower,
                    self.remoting_timeout_millis()?,
                )
                .await?
        };
        let mut rollback_stats = RollbackStats {
            broker_name: queue.broker_name().clone(),
            queue_id: queue.queue_id() as i64,
            broker_offset: offset_wrapper.get_broker_offset(),
            consumer_offset: offset_wrapper.get_consumer_offset(),
            timestamp_offset: reset_offset,
            rollback_offset: offset_wrapper.get_consumer_offset(),
        };
        if force || reset_offset <= offset_wrapper.get_consumer_offset() {
            rollback_stats.rollback_offset = reset_offset;
            self.mq_client_api()?
                .update_consumer_offset(
                    &broker_addr,
                    UpdateConsumerOffsetRequestHeader {
                        consumer_group,
                        topic: queue.topic().clone(),
                        queue_id: queue.queue_id(),
                        commit_offset: reset_offset,
                        topic_request_header: None,
                    },
                    self.remoting_timeout_millis()?,
                )
                .await?;
        }
        Ok(rollback_stats)
    }
}

impl MQAdminMutationExt for DefaultMQAdminExtImpl {
    async fn begin_proxy_drain(
        &self,
        proxy_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody> {
        self.mq_client_api()?
            .begin_proxy_drain(&proxy_addr, operation_id, self.remoting_timeout_millis()?)
            .await
    }

    async fn cancel_proxy_drain(
        &self,
        proxy_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProxyDrainStateResponseBody> {
        self.mq_client_api()?
            .cancel_proxy_drain(&proxy_addr, operation_id, self.remoting_timeout_millis()?)
            .await
    }

    async fn broker_config_generation(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<u64> {
        let runtime = self
            .mq_client_api()?
            .get_broker_runtime_info(&broker_addr, self.remoting_timeout_millis()?)
            .await?;
        let generation = runtime.table.get("brokerConfigGeneration").ok_or_else(|| {
            rocketmq_error::RocketMQError::ResponseProcessFailed {
                operation: "broker_config_generation",
                reason: "broker runtime info does not contain brokerConfigGeneration".to_string(),
            }
        })?;
        generation
            .parse::<u64>()
            .ok()
            .filter(|generation| *generation > 0)
            .ok_or_else(|| rocketmq_error::RocketMQError::ResponseProcessFailed {
                operation: "broker_config_generation",
                reason: "brokerConfigGeneration is not a positive unsigned integer".to_string(),
            })
    }

    async fn patch_broker_config_if_generation(
        &self,
        broker_addr: CheetahString,
        expected_generation: u64,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerConfigPatchOutcome> {
        self.mq_client_api()?
            .update_broker_config_if_generation(
                &broker_addr,
                expected_generation,
                properties,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn patch_topic_config_if_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        expected_version: u64,
        patch: TopicConfigPatch,
    ) -> rocketmq_error::RocketMQResult<TopicConfigPatchOutcome> {
        self.mq_client_api()?
            .update_topic_config_if_version(
                &broker_addr,
                topic,
                expected_version,
                patch,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn patch_subscription_group_config_if_version(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        expected_version: u64,
        patch: SubscriptionGroupConfigPatch,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfigPatchOutcome> {
        self.mq_client_api()?
            .update_subscription_group_config_if_version(
                &broker_addr,
                group,
                expected_version,
                patch,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn set_broker_log_filter_ttl(
        &self,
        broker_addr: CheetahString,
        logger: CheetahString,
        level: CheetahString,
        ttl_seconds: u32,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let properties = log_filter::set_properties(&broker_addr, &logger, &level, ttl_seconds, &operation_id)?;
        self.mq_client_api()?
            .update_broker_config(&broker_addr, properties, self.remoting_timeout_millis()?)
            .await
    }

    async fn restore_broker_log_filter(
        &self,
        broker_addr: CheetahString,
        operation_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let properties = log_filter::restore_properties(&broker_addr, &operation_id)?;
        self.mq_client_api()?
            .update_broker_config(&broker_addr, properties, self.remoting_timeout_millis()?)
            .await
    }

    async fn upsert_topic_config(
        &self,
        broker_addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        let topic = config
            .topic_name
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("Topic name is required".into()))?;
        let request_header = CreateTopicRequestHeader {
            topic,
            default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            read_queue_nums: config.read_queue_nums as i32,
            write_queue_nums: config.write_queue_nums as i32,
            perm: config.perm as i32,
            topic_filter_type: CheetahString::from_static_str(config.topic_filter_type.as_str()),
            topic_sys_flag: Some(config.topic_sys_flag as i32),
            order: config.order,
            attributes: encode_topic_attributes(&config.attributes),
            force: Some(false),
            topic_request_header: None,
        };

        self.mq_client_api()?
            .update_or_create_topic(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn remove_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let cluster_info = MQAdminMutationExt::mutation_cluster_info(self).await?;
        let mut broker_addrs = HashSet::new();
        if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
            if let Some(broker_names) = cluster_addr_table.get(&cluster_name) {
                if let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() {
                    for broker_name in broker_names {
                        if let Some(broker_data) = broker_addr_table.get(broker_name) {
                            broker_addrs.extend(broker_data.broker_addrs().values().cloned());
                        }
                    }
                }
            }
        }
        MQAdminMutationExt::remove_topic_from_brokers(self, broker_addrs, topic_name.clone()).await?;
        let namesrv_addrs = MQAdminMutationExt::mutation_name_server_addresses(self)
            .await?
            .into_iter()
            .collect();
        MQAdminMutationExt::remove_topic_from_name_servers(self, namesrv_addrs, Some(cluster_name), topic_name).await
    }

    async fn reset_consumer_offset(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestamp", timestamp)?;
        let topic_route = MQAdminMutationExt::mutation_topic_route(self, topic.clone()).await?;
        let mut offset_table = HashMap::new();
        let timeout = self.remoting_timeout_millis()?;

        if let Some(route_data) = topic_route {
            for broker_data in &route_data.broker_datas {
                if cluster_name
                    .as_ref()
                    .is_some_and(|expected| broker_data.cluster() != expected)
                {
                    continue;
                }
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let offsets = self
                        .mq_client_api()?
                        .invoke_broker_to_reset_offset(
                            master_addr,
                            ResetOffsetRequestHeader {
                                topic: topic.clone(),
                                group: consumer_group.clone(),
                                queue_id: -1,
                                offset: Some(-1),
                                timestamp,
                                is_force: force,
                                topic_request_header: None,
                            },
                            timeout,
                        )
                        .await?;
                    for (queue, offset) in offsets {
                        offset_table.insert(queue, java_long_to_u64("resetOffsetByTimestamp", "offset", offset)?);
                    }
                }
            }
        }

        Ok(offset_table)
    }

    async fn upsert_subscription_group(
        &self,
        broker_addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .create_subscription_group(&broker_addr, &config, self.remoting_timeout_millis()?)
            .await
    }

    async fn remove_subscription_group(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_subscription_group(
                &broker_addr,
                group_name,
                remove_offset.unwrap_or(false),
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn configure_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .set_message_request_mode(
                &broker_addr,
                &topic,
                &consumer_group,
                mode,
                pop_work_group_size,
                timeout_millis,
            )
            .await
    }

    async fn consume_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        let timeout = self.remoting_timeout_millis()?;
        let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_group.as_str()));
        let selected_addr = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&retry_topic, timeout)
            .await?
            .and_then(|route| {
                route
                    .broker_datas
                    .choose(&mut rand::rng())
                    .and_then(|broker| broker.select_broker_addr())
            });
        let mut connection = ConsumerConnection::new();
        if let Some(broker_addr) = selected_addr {
            connection = self
                .mq_client_api()?
                .get_consumer_connection_list(broker_addr.as_str(), consumer_group.clone(), timeout)
                .await?;
        }
        if connection.get_connection_set().is_empty() {
            return Err(mq_client_err!(
                ResponseCode::ConsumerNotOnline,
                "Not found the consumer group connection"
            ));
        }

        let (resolved_client_id, client_addr) =
            select_consumer_direct_connection(&consumer_group, &connection, Some(&client_id))?;
        let message = MQAdminMutationExt::view_message_for_mutation(self, topic.clone(), message_id.clone()).await?;
        let request_header = ConsumeMessageDirectlyResultRequestHeader {
            consumer_group,
            client_id: Some(resolved_client_id),
            msg_id: Some(message_id),
            broker_name: (!message.broker_name().is_empty()).then(|| message.broker_name.clone()),
            topic: Some(topic),
            topic_sys_flag: None,
            group_sys_flag: None,
            topic_request_header: None,
        };

        self.mq_client_api()?
            .consume_message_directly(&client_addr, request_header, &message, timeout)
            .await
    }

    async fn clone_consumer_group_offset(
        &self,
        source_group: CheetahString,
        destination_group: CheetahString,
        topic: CheetahString,
        offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(source_group.as_str()));
        let route = MQAdminMutationExt::mutation_topic_route(self, retry_topic.clone())
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for retry topic: {retry_topic}")))?;
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        for broker_data in &route.broker_datas {
            if let Some(addr) = broker_data.select_broker_addr() {
                api.clone_group_offset(
                    &addr,
                    source_group.clone(),
                    destination_group.clone(),
                    topic.clone(),
                    offline,
                    timeout,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn mutation_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        self.mq_client_api()?
            .get_broker_cluster_info(self.remoting_timeout_millis()?)
            .await
    }

    async fn mutation_topic_route(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn mutation_topic_config(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_topic_config(&broker_addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn remove_topic_from_brokers(
        &self,
        broker_addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        for broker_addr in broker_addrs {
            api.delete_topic_in_broker(
                &broker_addr,
                DeleteTopicRequestHeader {
                    topic: topic.clone(),
                    topic_request_header: None,
                },
                timeout,
            )
            .await?;
        }
        Ok(())
    }

    async fn remove_topic_from_name_servers(
        &self,
        namesrv_addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = DeleteTopicFromNamesrvRequestHeader::new(topic, cluster_name);
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        for namesrv_addr in namesrv_addrs {
            api.delete_topic_in_nameserver(&namesrv_addr, request_header.clone(), timeout)
                .await?;
        }
        Ok(())
    }

    async fn mutation_name_server_addresses(&self) -> rocketmq_error::RocketMQResult<Vec<CheetahString>> {
        Ok(self.mq_client_api()?.get_name_server_address_list().to_vec())
    }

    async fn upsert_order_topic_config(
        &self,
        topic: CheetahString,
        value: CheetahString,
        cluster_wide: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        let namespace = CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG);
        if cluster_wide {
            return api.put_kvconfig_value(namespace, topic, value, timeout).await;
        }

        let existing = api
            .get_kvconfig_value(namespace.clone(), topic.clone(), timeout)
            .await?
            .unwrap_or_default();
        api.put_kvconfig_value(
            namespace,
            topic,
            merge_order_conf_entries(existing.as_str(), value.as_str()).into(),
            timeout,
        )
        .await
    }

    async fn reset_consumer_offset_legacy(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestampOld", timestamp)?;
        let mut route_topic = topic.clone();
        if !topic.is_empty()
            && (mix_all::is_lmq(Some(topic.as_str()))
                || topic.as_str() == format!("{}wheel_timer", TopicValidator::SYSTEM_TOPIC_PREFIX))
            && cluster_name.as_ref().is_some_and(|name| !name.is_empty())
        {
            if let Some(cluster_name) = cluster_name {
                route_topic = cluster_name;
            }
        }
        let topic_route_data = MQAdminMutationExt::mutation_topic_route(self, route_topic).await?;
        let mut rollback_stats_list = Vec::new();

        if let Some(route_data) = topic_route_data {
            let mut topic_route_map = HashMap::new();
            for queue_data in &route_data.queue_datas {
                topic_route_map.insert(queue_data.broker_name().to_string(), queue_data.clone());
            }
            for broker_data in &route_data.broker_datas {
                if let Some(addr) = broker_data.select_broker_addr() {
                    if let Some(queue_data) = topic_route_map.get(broker_data.broker_name().as_str()) {
                        let mut rollback_stats = self
                            .mutation_reset_offset_on_broker(
                                addr,
                                queue_data,
                                consumer_group.clone(),
                                topic.clone(),
                                timestamp,
                                force,
                            )
                            .await?;
                        rollback_stats_list.append(&mut rollback_stats);
                    }
                }
            }
        }

        Ok(rollback_stats_list)
    }

    async fn view_message_for_mutation(
        &self,
        topic: CheetahString,
        message_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        MessageDecoder::validate_message_id(message_id.as_str())
            .map_err(|error| rocketmq_error::RocketMQError::IllegalArgument(format!("Invalid message ID: {error}")))?;
        let decoded = MessageDecoder::decode_message_id(message_id.as_str()).map_err(|error| {
            rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {error}"))
        })?;
        let broker_addr = CheetahString::from_string(format!("{}:{}", decoded.address.ip(), decoded.address.port()));
        self.mq_client_api()?
            .view_message(
                &broker_addr,
                ViewMessageRequestHeader {
                    topic: Some(topic),
                    offset: decoded.offset,
                },
                self.remoting_timeout_millis()?,
            )
            .await
    }
}
