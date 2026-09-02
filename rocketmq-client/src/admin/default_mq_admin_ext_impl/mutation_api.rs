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
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::view_message_request_header::ViewMessageRequestHeader;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::route_facade::BrokerDataExt;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

use crate::admin::mq_admin_mutation_ext::BrokerConfigPatchOutcome;
use crate::admin::mq_admin_mutation_ext::BrokerMutationConfigState;
use crate::admin::mq_admin_mutation_ext::ConditionalConsumerOffsetOutcome;
use crate::admin::mq_admin_mutation_ext::MQAdminMutationExt;
use crate::admin::mq_admin_mutation_ext::MutationConsumerOffsetPreview;
use crate::admin::mq_admin_mutation_ext::MutationExpectedMessageRequestMode;
use crate::admin::mq_admin_mutation_ext::MutationExpectedState;
use crate::admin::mq_admin_mutation_ext::MutationMessageRequestMode;
use crate::admin::mq_admin_mutation_ext::MutationMessageRequestModeOutcome;
use crate::admin::mq_admin_mutation_ext::MutationStateCasOutcome;
use crate::admin::mq_admin_mutation_ext::MutationSubscriptionGroupConfig;
use crate::admin::mq_admin_mutation_ext::MutationSubscriptionGroupConfigState;
use crate::admin::mq_admin_mutation_ext::MutationTopicConfig;
use crate::admin::mq_admin_mutation_ext::MutationTopicConfigState;
use crate::admin::mq_admin_mutation_ext::SubscriptionGroupConfigPatch;
use crate::admin::mq_admin_mutation_ext::SubscriptionGroupConfigPatchOutcome;
use crate::admin::mq_admin_mutation_ext::TopicConfigPatch;
use crate::admin::mq_admin_mutation_ext::TopicConfigPatchOutcome;
use crate::admin::mq_admin_mutation_ext::TopicOffsetMutationFailureCode;
use crate::admin::mq_admin_mutation_ext::TopicOffsetMutationOutcome;
use crate::admin::mq_admin_mutation_ext::TopicOffsetMutationTargetOutcome;

use super::DefaultMQAdminExtImpl;
use super::NAMESPACE_ORDER_TOPIC_CONFIG;

const MAX_SUPERVISED_OFFSET_TARGETS: usize = 1_000;

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

fn offset_failure(broker_name: &str, queue_id: Option<i32>, error: &RocketMQError) -> TopicOffsetMutationTargetOutcome {
    TopicOffsetMutationTargetOutcome {
        broker_name: broker_name.to_owned(),
        queue_id,
        applied: false,
        offset: None,
        failure: Some(TopicOffsetMutationFailureCode::Unavailable),
        retryable: error.boundary_view().is_retryable(),
    }
}

fn invalid_offset(broker_name: &str, queue_id: i32) -> TopicOffsetMutationTargetOutcome {
    TopicOffsetMutationTargetOutcome {
        broker_name: broker_name.to_owned(),
        queue_id: Some(queue_id),
        applied: false,
        offset: None,
        failure: Some(TopicOffsetMutationFailureCode::InvalidData),
        retryable: false,
    }
}

fn applied_offset(broker_name: &str, queue_id: i32, offset: u64) -> TopicOffsetMutationTargetOutcome {
    TopicOffsetMutationTargetOutcome {
        broker_name: broker_name.to_owned(),
        queue_id: Some(queue_id),
        applied: true,
        offset: Some(offset),
        failure: None,
        retryable: false,
    }
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
            .get_consume_stats_for_mutation(
                &broker_addr,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: topic.clone(),
                    topic_list: None,
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

    async fn mutation_reset_offset_on_broker_detailed(
        &self,
        broker_addr: CheetahString,
        queue_data: &QueueData,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
        force: bool,
    ) -> Vec<TopicOffsetMutationTargetOutcome> {
        let broker_name = queue_data.broker_name().to_string();
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => return vec![offset_failure(&broker_name, None, &error)],
        };
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return vec![offset_failure(&broker_name, None, &error)],
        };
        let consume_stats = match api
            .get_consume_stats_for_mutation(
                &broker_addr,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: topic.clone(),
                    topic_list: None,
                    topic_request_header: None,
                },
                timeout,
            )
            .await
        {
            Ok(stats) => stats,
            Err(error) => return vec![offset_failure(&broker_name, None, &error)],
        };
        let consumed = consume_stats
            .offset_table
            .iter()
            .filter(|(queue, _)| queue.topic() == &topic)
            .map(|(queue, wrapper)| (queue.clone(), wrapper.clone()))
            .collect::<Vec<_>>();
        let queues = if consumed.is_empty() {
            let topic_status = match api
                .get_topic_stats_info(
                    &broker_addr,
                    GetTopicStatsInfoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    },
                    timeout,
                )
                .await
            {
                Ok(status) => status,
                Err(error) => return vec![offset_failure(&broker_name, None, &error)],
            };
            (0..queue_data.read_queue_nums())
                .map(|queue_id| {
                    let queue =
                        MessageQueue::from_parts(topic.clone(), queue_data.broker_name().clone(), queue_id as i32);
                    let topic_offset = topic_status
                        .get_offset_table()
                        .get(&queue)
                        .cloned()
                        .unwrap_or_else(TopicOffset::new);
                    let mut wrapper = OffsetWrapper::new();
                    wrapper.set_broker_offset(topic_offset.get_max_offset());
                    wrapper.set_consumer_offset(topic_offset.get_min_offset());
                    (queue, wrapper)
                })
                .collect()
        } else {
            consumed
        };
        let mut outcomes = Vec::with_capacity(queues.len());
        for (queue, wrapper) in queues {
            let queue_id = queue.queue_id();
            match self
                .mutation_reset_queue_offset(
                    broker_addr.clone(),
                    consumer_group.clone(),
                    queue,
                    &wrapper,
                    timestamp,
                    force,
                )
                .await
            {
                Ok(stats) => match u64::try_from(stats.rollback_offset) {
                    Ok(offset) => outcomes.push(applied_offset(&broker_name, queue_id, offset)),
                    Err(_) => outcomes.push(invalid_offset(&broker_name, queue_id)),
                },
                Err(error) => outcomes.push(offset_failure(&broker_name, Some(queue_id), &error)),
            }
        }
        outcomes
    }

    async fn mutation_offset_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: i64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        let topic_route = MQAdminMutationExt::mutation_topic_route(self, topic.clone()).await?;
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let mut targets = Vec::new();
        if let Some(route_data) = topic_route {
            let queue_data = route_data
                .queue_datas
                .iter()
                .map(|queue| (queue.broker_name().to_string(), queue))
                .collect::<HashMap<_, _>>();
            for broker in &route_data.broker_datas {
                if broker.cluster() != cluster_name {
                    continue;
                }
                let broker_name = broker.broker_name().to_string();
                let Some(master_addr) = broker.broker_addrs().get(&mix_all::MASTER_ID) else {
                    targets.push(TopicOffsetMutationTargetOutcome {
                        broker_name,
                        queue_id: None,
                        applied: false,
                        offset: None,
                        failure: Some(TopicOffsetMutationFailureCode::Unavailable),
                        retryable: true,
                    });
                    continue;
                };
                let current = api
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
                    .await;
                match current {
                    Ok(offsets) => {
                        for (queue, offset) in offsets {
                            match u64::try_from(offset) {
                                Ok(offset) => {
                                    targets.push(applied_offset(queue.broker_name().as_str(), queue.queue_id(), offset))
                                }
                                Err(_) => targets.push(invalid_offset(queue.broker_name().as_str(), queue.queue_id())),
                            }
                        }
                    }
                    Err(RocketMQError::BrokerOperationFailed { code, .. })
                        if ResponseCode::from(code) == ResponseCode::ConsumerNotOnline =>
                    {
                        if let (Some(addr), Some(queue)) = (broker.select_broker_addr(), queue_data.get(&broker_name)) {
                            targets.extend(
                                self.mutation_reset_offset_on_broker_detailed(
                                    addr,
                                    queue,
                                    consumer_group.clone(),
                                    topic.clone(),
                                    timestamp,
                                    force,
                                )
                                .await,
                            );
                        } else {
                            targets.push(TopicOffsetMutationTargetOutcome {
                                broker_name,
                                queue_id: None,
                                applied: false,
                                offset: None,
                                failure: Some(TopicOffsetMutationFailureCode::Unavailable),
                                retryable: true,
                            });
                        }
                    }
                    Err(error) => targets.push(offset_failure(&broker_name, None, &error)),
                }
            }
        }
        Ok(TopicOffsetMutationOutcome { targets })
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

    async fn mutation_topic_config_with_version(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<crate::admin::MutationTopicConfigVersioned> {
        self.mq_client_api()?
            .get_topic_config_with_version_for_mutation(&broker_addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn mutation_topic_config_state(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationTopicConfigState> {
        self.mq_client_api()?
            .get_topic_config_state_for_mutation(&broker_addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn replace_topic_config_if_state(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationTopicConfig,
    ) -> rocketmq_error::RocketMQResult<MutationStateCasOutcome> {
        self.mq_client_api()?
            .replace_topic_config_if_state(
                &broker_addr,
                topic,
                expected_state,
                replacement,
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

    async fn mutation_subscription_group_config_state(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MutationSubscriptionGroupConfigState> {
        self.mq_client_api()?
            .get_subscription_group_config_state_for_mutation(&broker_addr, group, self.remoting_timeout_millis()?)
            .await
    }

    async fn replace_subscription_group_config_if_state(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationSubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<MutationStateCasOutcome> {
        self.mq_client_api()?
            .replace_subscription_group_config_if_state(
                &broker_addr,
                group,
                expected_state,
                replacement,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn broker_mutation_config_state(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerMutationConfigState> {
        self.mq_client_api()?
            .get_broker_mutation_config_state(&broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn reset_consumer_offset_if_current(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        expected_offset: i64,
        new_offset: i64,
    ) -> rocketmq_error::RocketMQResult<ConditionalConsumerOffsetOutcome> {
        self.mq_client_api()?
            .reset_consumer_offset_if_current(
                &broker_addr,
                consumer_group,
                topic,
                queue_id,
                expected_offset,
                new_offset,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn preview_consumer_offset_reset_on_broker(
        &self,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        read_queue_nums: u32,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: i64,
    ) -> rocketmq_error::RocketMQResult<Vec<MutationConsumerOffsetPreview>> {
        if read_queue_nums as usize > MAX_SUPERVISED_OFFSET_TARGETS {
            return Err(RocketMQError::illegal_argument(
                "supervised offset preview exceeds 1000 queue targets",
            ));
        }
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let consume_stats = api
            .get_consume_stats_for_supervised_mutation(
                &broker_addr,
                &broker_name,
                read_queue_nums,
                GetConsumeStatsRequestHeader {
                    consumer_group: consumer_group.clone(),
                    topic: topic.clone(),
                    topic_list: None,
                    topic_request_header: None,
                },
                timeout,
            )
            .await?;
        if consume_stats.offset_table.values().any(|wrapper| {
            wrapper.get_consumer_offset() < -1 || wrapper.get_broker_offset() < 0 || wrapper.get_pull_offset() < -1
        }) || consume_stats.offset_table.keys().any(|queue| {
            queue.topic() != &topic
                || queue.broker_name() != &broker_name
                || queue.queue_id() < 0
                || queue.queue_id() >= read_queue_nums as i32
        }) {
            return Err(RocketMQError::response_process_failed(
                "preview_consumer_offset_reset_on_broker",
                "Broker returned consume-stats rows outside the exact Topic/Broker queue set",
            ));
        }
        let mut consumed = consume_stats
            .offset_table
            .into_iter()
            .map(|(queue, wrapper)| (queue, wrapper.get_consumer_offset()))
            .collect::<Vec<_>>();
        consumed.sort_by_key(|(queue, _)| queue.queue_id());
        if !consumed.is_empty()
            && (consumed.len() != read_queue_nums as usize
                || consumed
                    .iter()
                    .enumerate()
                    .any(|(queue_id, (queue, _))| queue.queue_id() != queue_id as i32))
        {
            return Err(RocketMQError::response_process_failed(
                "preview_consumer_offset_reset_on_broker",
                "Broker returned an incomplete consume-stats queue set",
            ));
        }
        let queues = if consumed.is_empty() {
            let stats = api
                .get_topic_stats_info(
                    &broker_addr,
                    GetTopicStatsInfoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    },
                    timeout,
                )
                .await?;
            (0..read_queue_nums)
                .map(|queue_id| {
                    let queue = MessageQueue::from_parts(topic.clone(), broker_name.clone(), queue_id as i32);
                    let current = stats
                        .get_offset_table()
                        .get(&queue)
                        .map(TopicOffset::get_min_offset)
                        .unwrap_or(0);
                    (queue, current)
                })
                .collect()
        } else {
            consumed
        };
        let mut preview = Vec::with_capacity(queues.len());
        for (queue, current_offset) in queues {
            let planned_offset = if timestamp == -1 {
                api.get_max_offset(broker_addr.as_str(), &queue, timeout).await?
            } else {
                api.search_offset_by_timestamp(
                    broker_addr.as_str(),
                    &queue,
                    timestamp,
                    rocketmq_model::common::boundary_type::BoundaryType::Lower,
                    timeout,
                )
                .await?
            };
            if current_offset < -1 || planned_offset < 0 {
                return Err(RocketMQError::response_process_failed(
                    "preview_consumer_offset_reset_on_broker",
                    "Broker returned an invalid consumer offset",
                ));
            }
            preview.push(MutationConsumerOffsetPreview {
                broker_name: broker_name.to_string(),
                queue_id: queue.queue_id(),
                current_offset,
                planned_offset,
            });
        }
        preview.sort_by_key(|row| row.queue_id);
        Ok(preview)
    }

    async fn mutation_consumer_offset(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
    ) -> rocketmq_error::RocketMQResult<i64> {
        if queue_id < 0 {
            return Err(RocketMQError::illegal_argument("queueId must be non-negative"));
        }
        self.mq_client_api()?
            .query_consumer_offset(
                broker_addr.as_str(),
                QueryConsumerOffsetRequestHeader::new(consumer_group, topic, queue_id),
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn mutation_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<MutationMessageRequestMode>> {
        self.mq_client_api()?
            .get_message_request_mode_for_mutation(&broker_addr, topic, consumer_group, self.remoting_timeout_millis()?)
            .await
    }

    async fn replace_message_request_mode_if_current(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        expected: MutationExpectedMessageRequestMode,
        replacement: MutationMessageRequestMode,
    ) -> rocketmq_error::RocketMQResult<MutationMessageRequestModeOutcome> {
        self.mq_client_api()?
            .replace_message_request_mode_if_current(
                &broker_addr,
                topic,
                consumer_group,
                expected,
                replacement,
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

    async fn reset_consumer_offset_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestampDetailed", timestamp)?;
        self.mutation_offset_detailed(cluster_name, topic, consumer_group, timestamp, force)
            .await
    }

    async fn skip_accumulated_message(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        consumer_group: CheetahString,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<usize> {
        let topic_route = MQAdminMutationExt::mutation_topic_route(self, topic.clone()).await?;
        let mut offset_table = HashMap::new();
        let timeout = self.remoting_timeout_millis()?;
        let current = async {
            if let Some(route_data) = &topic_route {
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
                                    timestamp: -1,
                                    is_force: force,
                                    topic_request_header: None,
                                },
                                timeout,
                            )
                            .await?;
                        for (queue, offset) in offsets {
                            offset_table.insert(queue, java_long_to_u64("skipAccumulatedMessage", "offset", offset)?);
                        }
                    }
                }
            }
            Ok::<_, RocketMQError>(offset_table.len())
        }
        .await;
        match current {
            Ok(count) => Ok(count),
            Err(RocketMQError::BrokerOperationFailed { code, .. })
                if ResponseCode::from(code) == ResponseCode::ConsumerNotOnline =>
            {
                let mut rollback_count = 0usize;
                if let Some(route_data) = topic_route {
                    let mut topic_route_map = HashMap::new();
                    for queue_data in &route_data.queue_datas {
                        topic_route_map.insert(queue_data.broker_name().to_string(), queue_data.clone());
                    }
                    for broker_data in &route_data.broker_datas {
                        if cluster_name
                            .as_ref()
                            .is_some_and(|expected| broker_data.cluster() != expected)
                        {
                            continue;
                        }
                        if let Some(addr) = broker_data.select_broker_addr() {
                            if let Some(queue_data) = topic_route_map.get(broker_data.broker_name().as_str()) {
                                rollback_count += self
                                    .mutation_reset_offset_on_broker(
                                        addr,
                                        queue_data,
                                        consumer_group.clone(),
                                        topic.clone(),
                                        -1,
                                        force,
                                    )
                                    .await?
                                    .len();
                            }
                        }
                    }
                }
                Ok(rollback_count)
            }
            Err(error) => Err(error),
        }
    }

    async fn skip_accumulated_message_detailed(
        &self,
        cluster_name: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<TopicOffsetMutationOutcome> {
        self.mutation_offset_detailed(cluster_name, topic, consumer_group, -1, force)
            .await
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

    async fn remove_subscription_groups(
        &self,
        broker_addr: CheetahString,
        group_names: Vec<CheetahString>,
        clean_offset: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_subscription_group_list(&broker_addr, group_names, clean_offset, self.remoting_timeout_millis()?)
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

    async fn remove_topics_from_broker(
        &self,
        broker_addr: CheetahString,
        topics: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_topic_in_broker_list(&broker_addr, topics, self.remoting_timeout_millis()?)
            .await
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

    async fn mutation_order_topic_config(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        self.mq_client_api()?
            .get_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                topic,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn delete_order_topic_config(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                topic,
                self.remoting_timeout_millis()?,
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

#[cfg(test)]
mod detailed_offset_tests {
    use std::collections::BTreeMap;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
    use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
    use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
    use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
    use rocketmq_protocol::RemotingCommand;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::ShutdownDeadline;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::test_support::SessionProcessor;
    use rocketmq_transport::test_support::SessionTransportServer;
    use rocketmq_transport::test_support::SessionTransportServerConfig;

    use crate::base::client_config::ClientConfig;
    use crate::factory::mq_client_instance::MQClientInstance;
    use crate::runtime::test_client_runtime;

    use super::*;

    #[derive(Default)]
    struct ScriptedRequestLedger {
        responses: Mutex<HashMap<i32, VecDeque<RemotingCommand>>>,
        counts: Mutex<HashMap<i32, usize>>,
        kv_reads: Mutex<Vec<(String, String)>>,
    }

    impl ScriptedRequestLedger {
        fn push(&self, code: RequestCode, response: RemotingCommand) {
            self.responses
                .lock()
                .expect("scripted responses")
                .entry(code.to_i32())
                .or_default()
                .push_back(response);
        }

        fn snapshot(&self) -> HashMap<i32, usize> {
            self.counts.lock().expect("request counts").clone()
        }

        fn kv_reads(&self) -> Vec<(String, String)> {
            self.kv_reads.lock().expect("KV reads").clone()
        }
    }

    impl SessionProcessor for ScriptedRequestLedger {
        fn process(
            &self,
            request: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = rocketmq_error::RocketMQResult<RemotingCommand>> + Send + '_>> {
            Box::pin(async move {
                if request.code() == RequestCode::GetKvConfig.to_i32() {
                    let header = request.decode_command_custom_header::<GetKVConfigRequestHeader>()?;
                    self.kv_reads
                        .lock()
                        .expect("KV reads")
                        .push((header.namespace.to_string(), header.key.to_string()));
                }
                *self
                    .counts
                    .lock()
                    .expect("request counts")
                    .entry(request.code())
                    .or_default() += 1;
                let response = self
                    .responses
                    .lock()
                    .expect("scripted responses")
                    .get_mut(&request.code())
                    .and_then(VecDeque::pop_front)
                    .ok_or_else(|| {
                        RocketMQError::illegal_argument(format!("unexpected request code {}", request.code()))
                    })?;
                Ok(response.set_opaque(request.opaque()))
            })
        }
    }

    struct ProductionAdminHarness {
        admin: DefaultMQAdminExtImpl,
        broker_addr: CheetahString,
        client_instance: Arc<MQClientInstance>,
        client_runtime: Arc<crate::runtime::ClientRuntime>,
        ledger: Arc<ScriptedRequestLedger>,
        server: Arc<SessionTransportServer>,
        server_runtime: RuntimeContext,
    }

    impl ProductionAdminHarness {
        async fn new(scope: &'static str) -> Self {
            let server_runtime = RuntimeContext::from_current(scope);
            let admission = Arc::new(AdmissionController::new(AdmissionLimits::default()));
            let ledger = Arc::new(ScriptedRequestLedger::default());
            let server = SessionTransportServer::bind(
                server_runtime.service_context("scripted-broker"),
                SessionTransportServerConfig::loopback(),
                Arc::clone(&ledger) as Arc<dyn SessionProcessor>,
                admission,
            )
            .await
            .expect("bind scripted broker");
            let broker_addr = CheetahString::from_string(server.local_addr().to_string());
            server.start().expect("start scripted broker");

            let client_runtime = test_client_runtime(scope);
            let mut client_config = ClientConfig::default();
            client_config.set_vip_channel_enabled(false);
            let client_instance = MQClientInstance::new_arc(
                client_config.clone(),
                0,
                CheetahString::from_string(format!("{scope}-client")),
                None,
                client_runtime.component("instance"),
                client_runtime.telemetry_handle().clone(),
                client_runtime.pool().request_future_holder(),
            );
            client_instance
                .get_mq_client_api_impl()
                .expect("client API")
                .start()
                .await
                .expect("start client API transport");

            let mut admin = DefaultMQAdminExtImpl::new(
                Arc::clone(&client_runtime),
                None,
                Duration::from_secs(5),
                client_config,
                CheetahString::from_string(format!("{scope}-admin")),
            );
            admin.client_instance = Some(Arc::clone(&client_instance));

            Self {
                admin,
                broker_addr,
                client_instance,
                client_runtime,
                ledger,
                server,
                server_runtime,
            }
        }

        async fn shutdown(self) {
            let Self {
                admin,
                client_instance,
                client_runtime,
                server,
                server_runtime,
                ..
            } = self;
            drop(admin);
            client_instance.shutdown().await;
            client_runtime
                .shutdown()
                .await
                .assert_no_task_leak()
                .expect("client runtime tasks drained");
            server
                .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
                .await
                .assert_no_task_leak()
                .expect("scripted broker tasks drained");
            server_runtime
                .shutdown_tasks(Duration::from_secs(5))
                .await
                .assert_no_task_leak()
                .expect("scripted broker runtime tasks drained");
        }
    }

    fn request_delta(before: &HashMap<i32, usize>, after: &HashMap<i32, usize>, code: RequestCode) -> usize {
        after.get(&code.to_i32()).copied().unwrap_or(0) - before.get(&code.to_i32()).copied().unwrap_or(0)
    }

    fn assert_request_delta(
        before: &HashMap<i32, usize>,
        after: &HashMap<i32, usize>,
        expected: &[(RequestCode, usize)],
    ) {
        let expected = expected
            .iter()
            .map(|(code, count)| (code.to_i32(), *count))
            .collect::<BTreeMap<_, _>>();
        let actual = after
            .iter()
            .filter_map(|(code, count)| {
                let delta = count - before.get(code).copied().unwrap_or(0);
                (delta != 0).then_some((*code, delta))
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
    }

    fn consume_stats_response(rows: Vec<MessageQueue>) -> RemotingCommand {
        let mut stats = ConsumeStats::new();
        for row in rows {
            stats.offset_table.insert(row, OffsetWrapper::new());
        }
        RemotingCommand::create_success_response_command()
            .set_body(stats.encode_java_compatible().expect("consume stats body"))
    }

    fn duplicate_consume_stats_response() -> RemotingCommand {
        let mut stats = ConsumeStats::new();
        stats
            .offset_table
            .insert(MessageQueue::from_parts("orders", "broker-a", 0), OffsetWrapper::new());
        let one = stats.to_java_compatible_json().expect("single row JSON");
        let body = one.strip_prefix("{\"offsetTable\":{").expect("offset table prefix");
        let (entry, suffix) = body.split_once("},\"consumeTps\":").expect("offset table suffix");
        RemotingCommand::create_success_response_command()
            .set_body(format!("{{\"offsetTable\":{{{entry},{entry}}},\"consumeTps\":{suffix}"))
    }

    fn response_with_offset_header(code: ResponseCode, offset: i64) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command_with_code_and_header(
            code,
            QueryConsumerOffsetResponseHeader { offset: Some(offset) },
        );
        response.make_custom_header_to_net();
        response
    }

    #[tokio::test]
    async fn production_admin_reads_the_exact_order_topic_value_without_merging() {
        let harness = ProductionAdminHarness::new("order-topic-exact-read-test").await;
        harness
            .client_instance
            .get_mq_client_api_impl()
            .expect("client API")
            .update_name_server_address_list_sync(harness.broker_addr.as_str());
        let exact = "broker-a:8;broker-z:4";
        let mut found = RemotingCommand::create_response_command_with_code_and_header(
            ResponseCode::Success,
            GetKVConfigResponseHeader::new(Some(exact.into())),
        );
        found.make_custom_header_to_net();
        harness.ledger.push(RequestCode::GetKvConfig, found);
        let before = harness.ledger.snapshot();
        let value =
            MQAdminMutationExt::mutation_order_topic_config(&harness.admin, CheetahString::from_static_str("orders"))
                .await
                .expect("exact order Topic read");
        assert_eq!(value.as_deref(), Some(exact));
        let after = harness.ledger.snapshot();
        assert_request_delta(&before, &after, &[(RequestCode::GetKvConfig, 1)]);
        assert_eq!(
            harness.ledger.kv_reads(),
            vec![(NAMESPACE_ORDER_TOPIC_CONFIG.to_owned(), "orders".to_owned())]
        );

        harness.ledger.push(
            RequestCode::GetKvConfig,
            RemotingCommand::create_response_command_with_code(ResponseCode::QueryNotFound),
        );
        let missing =
            MQAdminMutationExt::mutation_order_topic_config(&harness.admin, CheetahString::from_static_str("missing"))
                .await
                .expect("missing exact order Topic read");
        assert_eq!(missing, None);
        assert_eq!(
            harness.ledger.kv_reads(),
            vec![
                (NAMESPACE_ORDER_TOPIC_CONFIG.to_owned(), "orders".to_owned()),
                (NAMESPACE_ORDER_TOPIC_CONFIG.to_owned(), "missing".to_owned()),
            ]
        );
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn production_admin_preview_stops_after_real_remoting_rejects_consume_stats() {
        let harness = ProductionAdminHarness::new("supervised-preview-wire-test").await;
        let cases = [
            (
                "wrong topic",
                consume_stats_response(vec![MessageQueue::from_parts("wrong-topic", "broker-a", 0)]),
                1,
            ),
            (
                "wrong broker",
                consume_stats_response(vec![MessageQueue::from_parts("orders", "wrong-broker", 0)]),
                1,
            ),
            (
                "wrong queue id",
                consume_stats_response(vec![MessageQueue::from_parts("orders", "broker-a", 1)]),
                1,
            ),
            ("duplicate queue", duplicate_consume_stats_response(), 1),
            (
                "queue gap",
                consume_stats_response(vec![MessageQueue::from_parts("orders", "broker-a", 0)]),
                2,
            ),
            (
                "oversized body",
                RemotingCommand::create_success_response_command().set_body(vec![b' '; 1024 * 1024 + 1]),
                1,
            ),
        ];

        for (label, response, read_queue_nums) in cases {
            harness.ledger.push(RequestCode::GetConsumeStats, response);
            let before = harness.ledger.snapshot();
            let error = MQAdminMutationExt::preview_consumer_offset_reset_on_broker(
                &harness.admin,
                harness.broker_addr.clone(),
                CheetahString::from_static_str("broker-a"),
                read_queue_nums,
                CheetahString::from_static_str("orders-consumer"),
                CheetahString::from_static_str("orders"),
                123,
            )
            .await
            .expect_err(label);
            let rendered = error.to_string();
            assert!(!rendered.contains("wrong-topic"));
            assert!(!rendered.contains("wrong-broker"));
            let after = harness.ledger.snapshot();
            assert_request_delta(&before, &after, &[(RequestCode::GetConsumeStats, 1)]);
            assert_eq!(request_delta(&before, &after, RequestCode::GetTopicStatsInfo), 0);
            assert_eq!(request_delta(&before, &after, RequestCode::SearchOffsetByTimestamp), 0);
            assert_eq!(request_delta(&before, &after, RequestCode::GetMaxOffset), 0);
            assert_eq!(
                request_delta(&before, &after, RequestCode::UpdateConsumerOffsetConditional),
                0
            );
            assert_eq!(request_delta(&before, &after, RequestCode::QueryConsumerOffset), 0);
        }

        harness.ledger.push(
            RequestCode::GetConsumeStats,
            consume_stats_response(vec![MessageQueue::from_parts("orders", "broker-a", 0)]),
        );
        let mut search =
            RemotingCommand::create_success_response_command_with_header(SearchOffsetResponseHeader { offset: 5 });
        search.make_custom_header_to_net();
        harness.ledger.push(RequestCode::SearchOffsetByTimestamp, search);
        let before = harness.ledger.snapshot();
        let preview = MQAdminMutationExt::preview_consumer_offset_reset_on_broker(
            &harness.admin,
            harness.broker_addr.clone(),
            CheetahString::from_static_str("broker-a"),
            1,
            CheetahString::from_static_str("orders-consumer"),
            CheetahString::from_static_str("orders"),
            123,
        )
        .await
        .expect("valid search-offset preview");
        assert_eq!(preview.len(), 1);
        assert_eq!(preview[0].planned_offset, 5);
        let after = harness.ledger.snapshot();
        assert_request_delta(
            &before,
            &after,
            &[
                (RequestCode::GetConsumeStats, 1),
                (RequestCode::SearchOffsetByTimestamp, 1),
            ],
        );

        harness.ledger.push(
            RequestCode::GetConsumeStats,
            consume_stats_response(vec![MessageQueue::from_parts("orders", "broker-a", 0)]),
        );
        let mut maximum =
            RemotingCommand::create_success_response_command_with_header(GetMaxOffsetResponseHeader { offset: 9 });
        maximum.make_custom_header_to_net();
        harness.ledger.push(RequestCode::GetMaxOffset, maximum);
        let before = harness.ledger.snapshot();
        let preview = MQAdminMutationExt::preview_consumer_offset_reset_on_broker(
            &harness.admin,
            harness.broker_addr.clone(),
            CheetahString::from_static_str("broker-a"),
            1,
            CheetahString::from_static_str("orders-consumer"),
            CheetahString::from_static_str("orders"),
            -1,
        )
        .await
        .expect("valid max-offset preview");
        assert_eq!(preview[0].planned_offset, 9);
        let after = harness.ledger.snapshot();
        assert_request_delta(
            &before,
            &after,
            &[(RequestCode::GetConsumeStats, 1), (RequestCode::GetMaxOffset, 1)],
        );

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn production_admin_conditional_offset_rejects_invalid_real_remoting_responses_before_verify() {
        let harness = ProductionAdminHarness::new("conditional-offset-wire-test").await;
        let invalid = [
            response_with_offset_header(ResponseCode::SystemError, 3).set_remark("accessKey=secret at 10.0.0.9:10911"),
            RemotingCommand::create_success_response_command(),
            response_with_offset_header(ResponseCode::Success, 4),
            response_with_offset_header(ResponseCode::InvalidParameter, 7),
            response_with_offset_header(ResponseCode::Success, -2),
        ];

        for response in invalid {
            harness
                .ledger
                .push(RequestCode::UpdateConsumerOffsetConditional, response);
            let before = harness.ledger.snapshot();
            let error = MQAdminMutationExt::reset_consumer_offset_if_current(
                &harness.admin,
                harness.broker_addr.clone(),
                CheetahString::from_static_str("orders-consumer"),
                CheetahString::from_static_str("orders"),
                0,
                7,
                3,
            )
            .await
            .expect_err("invalid conditional response");
            let rendered = error.to_string();
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("10.0.0.9"));
            let after = harness.ledger.snapshot();
            assert_request_delta(&before, &after, &[(RequestCode::UpdateConsumerOffsetConditional, 1)]);
            assert_eq!(request_delta(&before, &after, RequestCode::QueryConsumerOffset), 0);
        }

        harness.ledger.push(
            RequestCode::UpdateConsumerOffsetConditional,
            response_with_offset_header(ResponseCode::Success, 3),
        );
        let before = harness.ledger.snapshot();
        let outcome = MQAdminMutationExt::reset_consumer_offset_if_current(
            &harness.admin,
            harness.broker_addr.clone(),
            CheetahString::from_static_str("orders-consumer"),
            CheetahString::from_static_str("orders"),
            0,
            7,
            3,
        )
        .await
        .expect("valid conditional response");
        assert!(outcome.applied);
        assert_eq!(outcome.actual_offset, 3);
        let after = harness.ledger.snapshot();
        assert_request_delta(&before, &after, &[(RequestCode::UpdateConsumerOffsetConditional, 1)]);

        harness.shutdown().await;
    }

    #[test]
    fn detailed_offset_outcomes_retain_applied_and_failed_queue_targets_in_order() {
        let unavailable = RocketMQError::IllegalArgument("safe test failure".into());
        let outcomes = [
            applied_offset("broker-a", 0, 41),
            offset_failure("broker-a", Some(1), &unavailable),
            invalid_offset("broker-b", 2),
        ];

        assert!(outcomes[0].applied);
        assert_eq!(outcomes[0].offset, Some(41));
        assert_eq!(outcomes[0].queue_id, Some(0));
        assert!(!outcomes[1].applied);
        assert_eq!(outcomes[1].queue_id, Some(1));
        assert_eq!(outcomes[1].failure, Some(TopicOffsetMutationFailureCode::Unavailable));
        assert!(!outcomes[2].applied);
        assert_eq!(outcomes[2].queue_id, Some(2));
        assert_eq!(outcomes[2].failure, Some(TopicOffsetMutationFailureCode::InvalidData));
        assert!(!outcomes[2].retryable);
    }
}
