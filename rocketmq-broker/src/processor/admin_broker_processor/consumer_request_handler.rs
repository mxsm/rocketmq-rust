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

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use crate::config::config_manager::ConfigManager;
use cheetah_string::CheetahString;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::mq_version::RocketMqVersion;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList;
use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_protocol::protocol::body::connection::Connection;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::message_request_mode_serialize_wrapper::MessageRequestModeSerializeWrapper;
use rocketmq_protocol::protocol::body::query_consume_time_span_body::QueryConsumeTimeSpanBody;
use rocketmq_protocol::protocol::body::query_correction_offset_body::QueryCorrectionOffsetBody;
use rocketmq_protocol::protocol::body::query_subscription_response_body::QuerySubscriptionResponseBody;
use rocketmq_protocol::protocol::body::queue_time_span::QueueTimeSpan;
use rocketmq_protocol::protocol::body::response::reset_offset_body::ResetOffsetBody;
use rocketmq_protocol::protocol::header::clone_group_offset_request_header::CloneGroupOffsetRequestHeader;
use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
use rocketmq_protocol::protocol::header::query_correction_offset_header::QueryCorrectionOffsetHeader;
use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_store::BrokerAdminStore;
use rocketmq_transport::api::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::ServerRequestCommand;
use rocketmq_transport::api::ServerRequestOutcome;
use tracing::warn;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;
use crate::client::net::broker_to_client::Broker2Client;

#[derive(Clone)]
pub(super) struct ConsumerRequestHandler {
    broker_to_client: Broker2Client,
}

impl ConsumerRequestHandler {
    pub fn new() -> Self {
        Self::new_with_factory(application_remoting_command_factory())
    }

    pub fn new_with_factory(command_factory: RemotingCommandFactory) -> Self {
        Self {
            broker_to_client: Broker2Client::new(command_factory),
        }
    }
}

impl ConsumerRequestHandler {
    pub async fn get_consumer_connection_list<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let request_header = request.decode_command_custom_header::<GetConsumerConnectionListRequestHeader>()?;
        let consumer_group_info = broker_runtime_inner
            .consumer_manager()
            .get_consumer_group_info(request_header.get_consumer_group());
        match consumer_group_info {
            Some(consumer_group_info) => {
                let mut body_data = ConsumerConnection::new();
                body_data.set_consume_from_where(consumer_group_info.get_consume_from_where());
                body_data.set_consume_type(consumer_group_info.get_consume_type());
                body_data.set_message_model(consumer_group_info.get_message_model());
                let subscription_table = body_data.get_subscription_table_mut();
                for (topic, subscription) in consumer_group_info.subscription_snapshot() {
                    subscription_table.insert(topic, subscription.as_ref().clone());
                }

                for channel_info in consumer_group_info.session_info_snapshot() {
                    let mut connection = Connection::new();
                    connection.set_client_id(channel_info.client_id().clone());
                    connection.set_language(channel_info.language());
                    connection.set_version(channel_info.version());
                    connection.set_client_addr(
                        channel_info
                            .remote_address()
                            .map(CheetahString::from_slice)
                            .unwrap_or_else(|| format!("session:{:?}", channel_info.session_id()).into()),
                    );
                    body_data.insert_connection(connection);
                }
                let body = body_data.encode()?;
                Ok(Some(RemotingCommand::create_success_response_command().set_body(body)))
            }
            None => Ok(Some(response.set_code(ResponseCode::ConsumerNotOnline).set_remark(
                format!("the consumer group[{}] not online", request_header.get_consumer_group()),
            ))),
        }
    }

    pub async fn get_consume_stats<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header =
            request.decode_required_header::<GetConsumeStatsRequestHeader>("decode consume-stats request header")?;
        let mut consume_stats = ConsumeStats::new();
        let topic_list = request_header.fetch_topic_list();
        let topic_list_provided = !topic_list.is_empty();
        let mut topics: HashSet<CheetahString> = topic_list.into_iter().collect();
        if !topic_list_provided && request_header.get_topic().is_empty() {
            topics = broker_runtime_inner
                .consumer_offset_manager()
                .which_topic_by_consumer(request_header.get_consumer_group());
        } else if !topic_list_provided {
            topics.insert(request_header.get_topic().clone());
        }
        for topic in topics.iter() {
            let topic_config = broker_runtime_inner.topic_config_manager().select_topic_config(topic);
            if topic_config.is_none() {
                warn!(
                    "AdminBrokerProcessor#getConsumeStats: topic config does not exist, topic={}",
                    topic
                );
                continue;
            }

            let mapping_detail = broker_runtime_inner
                .topic_queue_mapping_manager()
                .get_topic_queue_mapping(topic);

            let find_subscription_data = broker_runtime_inner
                .consumer_manager()
                .find_subscription_data(request_header.get_consumer_group(), topic);

            if !topic_list_provided
                && find_subscription_data.is_none()
                && broker_runtime_inner
                    .consumer_manager()
                    .find_subscription_data_count(request_header.get_consumer_group())
                    > 0
            {
                warn!(
                    "AdminBrokerProcessor#getConsumeStats: topic does not exist in consumer group's subscription, \
                     topic={}, consumer group={}",
                    topic,
                    request_header.get_consumer_group()
                );
                continue;
            }

            for i in 0..topic_config.unwrap().get_read_queue_nums() {
                let mut mq = MessageQueue::new();
                mq.set_topic(topic.to_string().into());
                mq.set_broker_name(broker_runtime_inner.broker_config().broker_name().clone());
                mq.set_queue_id(i as i32);

                let mut offset_wrapper = OffsetWrapper::new();

                let mut broker_offset = broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_max_offset_in_queue(topic, i as i32);
                if broker_offset < 0 {
                    broker_offset = 0;
                }

                let mut consumer_offset = broker_runtime_inner.consumer_offset_manager().query_offset(
                    request_header.get_consumer_group(),
                    topic,
                    i as i32,
                );

                if mapping_detail.is_none() && consumer_offset < 0 {
                    consumer_offset = 0;
                }

                let pull_offset = broker_runtime_inner.consumer_offset_manager().query_offset(
                    request_header.get_consumer_group(),
                    topic,
                    i as i32,
                );

                offset_wrapper.set_broker_offset(broker_offset);
                offset_wrapper.set_consumer_offset(consumer_offset);
                offset_wrapper.set_pull_offset(std::cmp::max(consumer_offset, pull_offset));

                let time_offset = consumer_offset - 1;
                if time_offset >= 0 {
                    let last_timestamp = broker_runtime_inner
                        .message_store()
                        .unwrap()
                        .get_message_store_timestamp(topic, i as i32, time_offset);
                    if last_timestamp > 0 {
                        offset_wrapper.set_last_timestamp(last_timestamp);
                    }
                }

                consume_stats.get_offset_table_mut().insert(mq, offset_wrapper);
            }

            let consume_tps = broker_runtime_inner
                .broker_stats_manager()
                .tps_group_get_nums(request_header.get_consumer_group(), topic);

            let new_consume_tps = consume_stats.get_consume_tps() + consume_tps;
            consume_stats.set_consume_tps(new_consume_tps);
        }
        let body = consume_stats.encode_java_compatible()?;
        Ok(Some(RemotingCommand::create_success_response_command().set_body(body)))
    }

    pub async fn get_broker_consume_stats<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetConsumeStatsInBrokerHeader>()?;
        let mut broker_consume_stats_list = Vec::new();
        let mut total_diff = 0i64;
        let mut total_inflight_diff = 0i64;

        for entry in broker_runtime_inner
            .subscription_group_manager()
            .subscription_group_table()
            .iter()
        {
            let group = entry.key().clone();
            let topics = broker_runtime_inner
                .consumer_offset_manager()
                .which_topic_by_consumer(&group);
            let mut consume_stats_list = Vec::new();

            for topic in topics {
                let Some(consume_stats) = self.build_broker_topic_consume_stats(
                    broker_runtime_inner,
                    &group,
                    &topic,
                    request_header.is_order,
                ) else {
                    continue;
                };

                total_diff += consume_stats.compute_total_diff();
                total_inflight_diff += consume_stats.compute_inflight_total_diff();
                consume_stats_list.push(consume_stats);
            }

            let mut group_stats = HashMap::new();
            group_stats.insert(group, consume_stats_list);
            broker_consume_stats_list.push(group_stats);
        }

        let body = ConsumeStatsList {
            consume_stats_list: broker_consume_stats_list,
            broker_addr: Some(broker_runtime_inner.get_broker_addr().clone()),
            total_diff,
            total_inflight_diff,
        };

        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(body.encode_java_compatible()?),
        ))
    }

    pub async fn query_correction_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<QueryCorrectionOffsetHeader>()?;
        let mut correction_offsets = broker_runtime_inner
            .consumer_offset_manager()
            .query_min_offset_in_all_group(&request_header.topic, request_header.filter_groups.as_ref());

        if let Some(compare_offsets) = broker_runtime_inner
            .consumer_offset_manager()
            .query_offsets(&request_header.compare_group, &request_header.topic)
        {
            for (queue_id, compare_offset) in compare_offsets {
                match correction_offsets.get_mut(&queue_id) {
                    Some(correction_offset) if *correction_offset > compare_offset => {
                        *correction_offset = i64::MAX;
                    }
                    Some(_) => {}
                    None => {
                        correction_offsets.insert(queue_id, i64::MAX);
                    }
                }
            }
        }

        Ok(Some(
            RemotingCommand::create_success_response_command()
                .set_body(QueryCorrectionOffsetBody { correction_offsets }.encode()?),
        ))
    }

    pub async fn consume_message_directly<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ConsumeMessageDirectlyResultRequestHeader>()?;
        let Some(client_id) = request_header
            .client_id
            .as_ref()
            .filter(|client_id| !client_id.is_empty())
        else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "clientId is missing",
            )));
        };
        let Some(msg_id) = request_header.msg_id.as_ref().filter(|msg_id| !msg_id.is_empty()) else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "msgId is missing",
            )));
        };

        request.ensure_ext_fields_initialized();
        request.add_ext_field("brokerName", broker_runtime_inner.broker_config().broker_name().clone());

        if let Some(topic) = request_header.topic.as_ref().filter(|topic| !topic.is_empty()) {
            if let Some(topic_config) = broker_runtime_inner
                .topic_config_manager()
                .get_topic_config(topic.as_str())
            {
                request.add_ext_field("topicSysFlag", topic_config.topic_sys_flag.to_string());
            }
        }

        if let Some(group_config) = broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(&request_header.consumer_group)
        {
            request.add_ext_field("groupSysFlag", group_config.group_sys_flag().to_string());
        }

        let message_id = match MessageDecoder::decode_message_id(msg_id.as_str()) {
            Ok(message_id) => message_id,
            Err(error) => {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invalid msgId: {error}"),
                )));
            }
        };

        let Some(select_result) = broker_runtime_inner
            .message_store()
            .unwrap()
            .select_one_message_by_offset(message_id.offset)
        else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "message not found by msgId",
            )));
        };
        let Some(body) = select_result.get_bytes() else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                "message bytes not available",
            )));
        };
        request.set_body_mut_ref(body);

        self.call_consumer(
            broker_runtime_inner,
            request.clone(),
            request_header.consumer_group.as_str(),
            client_id.as_str(),
        )
        .await
    }

    fn build_broker_topic_consume_stats<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        group: &cheetah_string::CheetahString,
        topic: &cheetah_string::CheetahString,
        is_order: bool,
    ) -> Option<ConsumeStats> {
        let topic_config = broker_runtime_inner.topic_config_manager().select_topic_config(topic);
        let Some(topic_config) = topic_config else {
            warn!(
                "AdminBrokerProcessor#fetchAllConsumeStatsInBroker: topic config does not exist, topic={}",
                topic
            );
            return None;
        };

        if is_order && !topic_config.order {
            return None;
        }

        let find_subscription_data = broker_runtime_inner
            .consumer_manager()
            .find_subscription_data(group, topic);
        if find_subscription_data.is_none()
            && broker_runtime_inner
                .consumer_manager()
                .find_subscription_data_count(group)
                > 0
        {
            warn!(
                "AdminBrokerProcessor#fetchAllConsumeStatsInBroker: topic does not exist in consumer group's \
                 subscription, topic={}, consumer group={}",
                topic, group
            );
            return None;
        }

        let message_store = broker_runtime_inner
            .message_store()
            .expect("message store should be initialized before broker consume stats");
        let mut consume_stats = ConsumeStats::new();
        for queue_id in 0..topic_config.write_queue_nums {
            let queue_id = queue_id as i32;
            let mut mq = MessageQueue::new();
            mq.set_topic(topic.clone());
            mq.set_broker_name(broker_runtime_inner.broker_config().broker_name().clone());
            mq.set_queue_id(queue_id);

            let mut offset_wrapper = OffsetWrapper::new();
            let mut broker_offset = message_store.get_max_offset_in_queue(topic, queue_id);
            if broker_offset < 0 {
                broker_offset = 0;
            }

            let mut consumer_offset = broker_runtime_inner
                .consumer_offset_manager()
                .query_offset(group, topic, queue_id);
            if consumer_offset < 0 {
                consumer_offset = 0;
            }

            offset_wrapper.set_broker_offset(broker_offset);
            offset_wrapper.set_consumer_offset(consumer_offset);

            let time_offset = consumer_offset - 1;
            if time_offset >= 0 {
                let last_timestamp = message_store.get_message_store_timestamp(topic, queue_id, time_offset);
                if last_timestamp > 0 {
                    offset_wrapper.set_last_timestamp(last_timestamp);
                }
            }

            consume_stats.get_offset_table_mut().insert(mq, offset_wrapper);
        }

        let consume_tps = broker_runtime_inner
            .broker_stats_manager()
            .tps_group_get_nums(group, topic);
        consume_stats.set_consume_tps(consume_stats.get_consume_tps() + consume_tps);
        Some(consume_stats)
    }

    pub async fn get_all_consumer_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let content = broker_runtime_inner.consumer_offset_manager().encode();
        if !content.is_empty() {
            Ok(Some(
                RemotingCommand::create_success_response_command().set_body(content),
            ))
        } else {
            Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No consumer offset in this broker"),
            ))
        }
    }

    pub async fn get_all_message_request_mode<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let Some(query_assignment_processor) = broker_runtime_inner.query_assignment_processor() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("query assignment processor is not initialized"),
            ));
        };

        let message_request_mode_map = query_assignment_processor
            .message_request_mode_manager()
            .message_request_mode_map()
            .lock()
            .clone();
        if message_request_mode_map.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No message request mode in this broker"),
            ));
        }

        let body = MessageRequestModeSerializeWrapper::from_inner(message_request_mode_map);
        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(body.encode()?),
        ))
    }

    pub async fn invoke_broker_to_reset_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ResetOffsetRequestHeader>()?;

        let response = if broker_runtime_inner.broker_config().use_server_side_reset_offset {
            self.reset_offset_inner(
                broker_runtime_inner,
                &request_header.topic,
                &request_header.group,
                request_header.queue_id,
                request_header.timestamp,
                request_header.offset,
            )
            .await
        } else {
            if request.language() == LanguageCode::CPP {
                self.broker_to_client
                    .reset_offset_for_c(
                        broker_runtime_inner,
                        &request_header.topic,
                        &request_header.group,
                        request_header.timestamp,
                        request_header.is_force,
                    )
                    .await
            } else {
                self.broker_to_client
                    .reset_offset(
                        broker_runtime_inner,
                        &request_header.topic,
                        &request_header.group,
                        request_header.timestamp,
                        request_header.is_force,
                    )
                    .await
            }
        };

        Ok(Some(response))
    }

    pub async fn invoke_broker_to_get_consumer_status<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetConsumerStatusRequestHeader>()?;
        Ok(Some(
            self.broker_to_client
                .get_consume_status(
                    broker_runtime_inner,
                    &request_header.topic,
                    &request_header.group,
                    request_header.client_addr.as_ref(),
                )
                .await,
        ))
    }

    pub async fn query_subscription_by_consumer<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<QuerySubscriptionByConsumerRequestHeader>()?;
        let response_body = QuerySubscriptionResponseBody {
            subscription_data: broker_runtime_inner
                .consumer_manager()
                .find_subscription_data(&request_header.group, &request_header.topic),
            group: request_header.group,
            topic: request_header.topic,
        };

        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(response_body.encode()?),
        ))
    }

    pub async fn query_consume_time_span<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let request_header = request.decode_command_custom_header::<QueryConsumeTimeSpanRequestHeader>()?;
        let topic = request_header.topic;
        let Some(topic_config) = broker_runtime_inner.topic_config_manager().select_topic_config(&topic) else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("topic[{topic}] not exist")),
            ));
        };

        let message_store = broker_runtime_inner
            .message_store()
            .expect("message store should be initialized before query consume time span");
        let mut time_span_set = Vec::with_capacity(topic_config.write_queue_nums as usize);
        for queue_id in 0..topic_config.write_queue_nums {
            let queue_id = queue_id as i32;
            let min_time = message_store.get_earliest_message_time(&topic, queue_id);
            let max_offset = message_store.get_max_offset_in_queue(&topic, queue_id);
            let max_time = if max_offset > 0 {
                message_store.get_message_store_timestamp(&topic, queue_id, max_offset - 1)
            } else {
                min_time
            };

            let consumer_offset =
                broker_runtime_inner
                    .consumer_offset_manager()
                    .query_offset(&request_header.group, &topic, queue_id);
            let consume_time = if consumer_offset > 0 {
                message_store.get_message_store_timestamp(&topic, queue_id, consumer_offset - 1)
            } else {
                min_time
            };

            let delay_time = if consumer_offset >= 0 && consumer_offset < max_offset {
                let next_time = message_store.get_message_store_timestamp(&topic, queue_id, consumer_offset);
                if next_time > 0 {
                    rocketmq_runtime::common::time_utils::current_millis() as i64 - next_time
                } else {
                    0
                }
            } else {
                0
            };

            let mut queue_time_span = QueueTimeSpan::default();
            queue_time_span.set_message_queue(MessageQueue::from_parts(
                topic.clone(),
                broker_runtime_inner.broker_config().broker_name().clone(),
                queue_id,
            ));
            queue_time_span.set_min_time_stamp(min_time);
            queue_time_span.set_max_time_stamp(max_time);
            queue_time_span.set_consume_time_stamp(consume_time);
            queue_time_span.set_delay_time(delay_time);
            time_span_set.push(queue_time_span);
        }

        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(
                QueryConsumeTimeSpanBody {
                    consume_time_span_set: time_span_set,
                }
                .encode()?,
            ),
        ))
    }

    pub async fn clone_group_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<CloneGroupOffsetRequestHeader>()?;
        let mut topics = HashSet::new();
        if let Some(topic) = request_header.topic.clone().filter(|topic| !topic.is_empty()) {
            topics.insert(topic);
        } else {
            topics = broker_runtime_inner
                .consumer_offset_manager()
                .which_topic_by_consumer(&request_header.src_group);
        }

        for topic in topics {
            if broker_runtime_inner
                .topic_config_manager()
                .select_topic_config(&topic)
                .is_none()
            {
                warn!("[cloneGroupOffset], topic config not exist, {}", topic);
                continue;
            }

            if !request_header.offline
                && broker_runtime_inner
                    .consumer_manager()
                    .find_subscription_data_count(&request_header.src_group)
                    > 0
                && broker_runtime_inner
                    .consumer_manager()
                    .find_subscription_data(&request_header.src_group, &topic)
                    .is_none()
            {
                warn!(
                    "AdminBrokerProcessor#cloneGroupOffset: topic does not exist in consumer group's subscription, \
                     topic={}, consumer group={}",
                    topic, request_header.src_group
                );
                continue;
            }

            broker_runtime_inner.consumer_offset_manager().clone_offset(
                &request_header.src_group,
                &request_header.dest_group,
                &topic,
            );
        }

        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn get_consumer_running_info<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = match request.decode_command_custom_header::<GetConsumerRunningInfoRequestHeader>() {
            Ok(header) => header,
            Err(e) => {
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("decode GetConsumerRunningInfoRequestHeader failed: {}", e),
                );
                return Ok(Some(response));
            }
        };

        self.call_consumer(
            broker_runtime_inner,
            request.clone(),
            request_header.consumer_group.as_str(),
            request_header.client_id.as_str(),
        )
        .await
    }

    async fn call_consumer<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        request: RemotingCommand,
        consumer_group: &str,
        client_id: &str,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = self
            .broker_to_client
            .command_factory()
            .create_java_default_error_response_command();

        let group: CheetahString = consumer_group.into();
        let Some((client_session_info, transport)) = broker_runtime_inner
            .consumer_manager()
            .session_registry()
            .find_transport(&group, client_id)
        else {
            response = response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!("The Consumer <{}> <{}> not online", consumer_group, client_id));
            return Ok(Some(response));
        };

        if client_session_info.version() < RocketMqVersion::V3_1_8_SNAPSHOT.ordinal() as i32 {
            response = response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The Consumer <{}> Version <{}> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
                client_id,
                RocketMqVersion::from_ordinal(client_session_info.version() as u32).name()
            ));
            return Ok(Some(response));
        }

        let command = match RequestCode::from(request.code()) {
            RequestCode::GetConsumerRunningInfo => ServerRequestCommand::GetConsumerRunningInfo {
                header: request.decode_command_custom_header::<GetConsumerRunningInfoRequestHeader>()?,
            },
            RequestCode::ConsumeMessageDirectly => {
                let header = request.decode_command_custom_header::<ConsumeMessageDirectlyResultRequestHeader>()?;
                let body = request.body().cloned().unwrap_or_default();
                ServerRequestCommand::ConsumeMessageDirectly { header, body }
            }
            code => {
                return Ok(Some(request_code_not_supported_with_factory_remark_and_opaque(
                    self.broker_to_client.command_factory(),
                    request.code(),
                    format!("broker-to-consumer request code {code:?} is not allowlisted"),
                    request.opaque(),
                )));
            }
        };

        match transport
            .request_sender()
            .request(command, Duration::from_millis(5_000))
            .await
        {
            Ok(ServerRequestOutcome::Responded(result)) => Ok(Some(result.into_command())),
            Ok(
                ServerRequestOutcome::QueueSaturated
                | ServerRequestOutcome::DeadlineExpired
                | ServerRequestOutcome::SessionClosed,
            ) => {
                let (code, remark) = consumer_request_rejection_response();
                response = response.set_code(code).set_remark(remark);
                Ok(Some(response))
            }
            Err(error) => {
                let (code, remark) = consumer_request_failure_response(error.code());
                response = response.set_code(code).set_remark(remark);
                Ok(Some(response))
            }
        }
    }

    async fn reset_offset_inner<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        topic: &cheetah_string::CheetahString,
        group: &cheetah_string::CheetahString,
        queue_id: i32,
        timestamp: i64,
        offset: Option<i64>,
    ) -> RemotingCommand {
        let response = self
            .broker_to_client
            .command_factory()
            .create_java_default_error_response_command();

        if broker_runtime_inner.message_store_config().broker_role == BrokerRole::Slave {
            return response
                .set_code(ResponseCode::SystemError)
                .set_remark("Can not reset offset in slave broker");
        }

        let Some(topic_config) = broker_runtime_inner.topic_config_manager().select_topic_config(topic) else {
            return response
                .set_code(ResponseCode::TopicNotExist)
                .set_remark(format!("Topic {} does not exist", topic));
        };

        if !broker_runtime_inner
            .subscription_group_manager()
            .contains_subscription_group(group)
        {
            return response
                .set_code(ResponseCode::SubscriptionGroupNotExist)
                .set_remark(format!("Group {} does not exist", group));
        }

        let mut queue_offset_map = std::collections::HashMap::new();
        if queue_id >= 0 {
            match self
                .resolve_reset_offset(broker_runtime_inner, topic, queue_id, timestamp, offset)
                .await
            {
                Ok(target_offset) => {
                    queue_offset_map.insert(queue_id, target_offset);
                }
                Err(response) => return response,
            }
        } else {
            for queue_index in 0..topic_config.read_queue_nums {
                match self
                    .resolve_reset_offset(broker_runtime_inner, topic, queue_index as i32, timestamp, None)
                    .await
                {
                    Ok(target_offset) => {
                        queue_offset_map.insert(queue_index as i32, target_offset);
                    }
                    Err(response) => return response,
                }
            }
        }

        if queue_offset_map.is_empty() {
            return response
                .set_code(ResponseCode::SystemError)
                .set_remark("No queues to reset.");
        }

        let broker_name = broker_runtime_inner.broker_config().broker_name().clone();
        let mut body = ResetOffsetBody::new();
        for (queue_index, target_offset) in queue_offset_map {
            broker_runtime_inner.consumer_offset_manager().assign_reset_offset(
                topic,
                group,
                queue_index,
                target_offset,
            );
            broker_runtime_inner
                .consumer_offset_manager()
                .clear_pull_offset(group, topic);
            broker_runtime_inner
                .pop_inflight_message_counter()
                .clear_in_flight_message_num(topic, group, queue_index);
            body.offset_table.insert(
                MessageQueue::from_parts(topic.clone(), broker_name.clone(), queue_index),
                target_offset,
            );
        }

        self.broker_to_client
            .command_factory()
            .create_success_response_command()
            .set_body(body.encode())
    }

    async fn resolve_reset_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        topic: &cheetah_string::CheetahString,
        queue_id: i32,
        timestamp: i64,
        offset: Option<i64>,
    ) -> Result<i64, RemotingCommand> {
        let mut response = self
            .broker_to_client
            .command_factory()
            .create_java_default_error_response_command();
        let message_store = broker_runtime_inner
            .message_store()
            .expect("message store should be initialized before admin request");

        if let Some(target_offset) = offset.filter(|value| *value != -1) {
            let min_offset = message_store.get_min_offset_in_queue(topic, queue_id);
            let max_offset = message_store.get_max_offset_in_queue(topic, queue_id);
            if (min_offset >= 0 && target_offset < min_offset) || target_offset > max_offset + 1 {
                response = response.set_code(ResponseCode::SystemError).set_remark(format!(
                    "Target offset {} not in consume queue range [{}-{}]",
                    target_offset, min_offset, max_offset
                ));
                return Err(response);
            }
            return Ok(target_offset);
        }

        let target_offset = if timestamp < 0 {
            message_store.get_max_offset_in_queue(topic, queue_id)
        } else {
            match message_store
                .get_offset_in_queue_by_time_async(topic, queue_id, timestamp)
                .await
            {
                Ok(offset) => offset,
                Err(error) => {
                    response = response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(format!("Resolve offset by timestamp failed: {error}"));
                    return Err(response);
                }
            }
        };
        Ok(target_offset)
    }
}

const fn consumer_request_rejection_response() -> (ResponseCode, &'static str) {
    (ResponseCode::SystemError, "consumer request rejected")
}

fn consumer_request_failure_response(code: rocketmq_error::ErrorCode) -> (ResponseCode, &'static str) {
    if code == rocketmq_error::TRANSPORT_REQUEST_TIMEOUT.code() {
        (ResponseCode::ConsumeMsgTimeout, "consumer request timed out")
    } else {
        (ResponseCode::SystemError, "consumer request failed")
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::SystemTime;

    use crate::config::broker_config::BrokerConfig;
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::admin::consume_stats_list::ConsumeStatsList;
    use rocketmq_protocol::protocol::body::message_request_mode_serialize_wrapper::MessageRequestModeSerializeWrapper;
    use rocketmq_protocol::protocol::body::query_consume_time_span_body::QueryConsumeTimeSpanBody;
    use rocketmq_protocol::protocol::body::query_correction_offset_body::QueryCorrectionOffsetBody;
    use rocketmq_protocol::protocol::body::query_subscription_response_body::QuerySubscriptionResponseBody;
    use rocketmq_protocol::protocol::body::response::reset_offset_body::ResetOffsetBody;
    use rocketmq_protocol::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
    use rocketmq_protocol::protocol::header::clone_group_offset_request_header::CloneGroupOffsetRequestHeader;
    use rocketmq_protocol::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
    use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
    use rocketmq_protocol::protocol::header::get_consume_stats_in_broker_header::GetConsumeStatsInBrokerHeader;
    use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
    use rocketmq_protocol::protocol::header::query_consume_time_span_request_header::QueryConsumeTimeSpanRequestHeader;
    use rocketmq_protocol::protocol::header::query_correction_offset_header::QueryCorrectionOffsetHeader;
    use rocketmq_protocol::protocol::header::query_subscription_by_consumer_request_header::QuerySubscriptionByConsumerRequestHeader;
    use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
    use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
    use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
    use rocketmq_protocol::protocol::RemotingDeserializable;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_store::BrokerAdminStore;
    use rocketmq_store::MessageStoreConfig;

    use super::ConsumerRequestHandler;
    use crate::broker_runtime::BrokerRuntime;

    #[test]
    fn consumer_request_transport_outcomes_keep_the_timeout_wire_split() {
        let (timeout_code, timeout_remark) =
            super::consumer_request_failure_response(rocketmq_error::TRANSPORT_REQUEST_TIMEOUT.code());
        assert_eq!(timeout_code as i32, 207);
        assert_eq!(timeout_remark, "consumer request timed out");

        let (failure_code, failure_remark) =
            super::consumer_request_failure_response(rocketmq_error::TRANSPORT_SESSION_FAILED.code());
        assert_eq!(failure_code as i32, 1);
        assert_eq!(failure_remark, "consumer request failed");

        let (rejection_code, rejection_remark) = super::consumer_request_rejection_response();
        assert_eq!(rejection_code as i32, 1);
        assert_eq!(rejection_remark, "consumer request rejected");
    }

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-consumer-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str) -> BrokerRuntime {
        new_test_runtime_with_server_side_reset(label, true).await
    }

    async fn new_test_runtime_with_server_side_reset(label: &str, use_server_side_reset_offset: bool) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            use_server_side_reset_offset,
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await.is_ok());
        runtime
    }

    fn test_command_factory(version: i32, serialize_type: SerializeType) -> RemotingCommandFactory {
        RemotingCommandFactory::new(RemotingCommandDefaults::new(version, serialize_type))
    }

    fn assert_owner_defaults(response: &RemotingCommand, version: i32, serialize_type: SerializeType) {
        assert_eq!(response.version(), version);
        assert_eq!(response.serialize_type(), serialize_type);
    }

    async fn put_test_message<MS: BrokerAdminStore>(
        admin: &mut crate::broker::broker_admin_runtime::BrokerAdminRuntime<MS>,
        topic: &str,
    ) -> String {
        let mut message = MessageExtBrokerInner::default();
        message.set_topic(CheetahString::from_string(topic.to_string()));
        message.set_body(Bytes::from_static(b"hello-admin"));
        message.message_ext_inner.queue_id = 0;
        message.message_ext_inner.born_host = "127.0.0.1:10000".parse::<SocketAddr>().expect("parse born host");
        message.message_ext_inner.store_host = "127.0.0.1:10911".parse::<SocketAddr>().expect("parse store host");

        let put_result = admin.put_message(message).await.expect("message store should exist");
        assert!(put_result.is_ok(), "put test message should succeed");
        put_result
            .append_message_result()
            .and_then(|append_result| append_result.get_message_id())
            .expect("put test message should return msg id")
    }

    #[tokio::test]
    async fn get_all_message_request_mode_returns_configured_modes() {
        let mut runtime = new_test_runtime("message-mode").await;
        let _ = runtime.init_processor_checked().expect("initialize request processors");
        let admin = runtime.admin_runtime_for_test();
        admin
            .query_assignment_processor()
            .expect("query assignment processor should be initialized")
            .message_request_mode_manager()
            .set_message_request_mode(
                CheetahString::from_static_str("topic-a"),
                CheetahString::from_static_str("group-a"),
                SetMessageRequestModeRequestBody {
                    topic: CheetahString::from_static_str("topic-a"),
                    consumer_group: CheetahString::from_static_str("group-a"),
                    ..Default::default()
                },
            );

        let handler = ConsumerRequestHandler::new();
        let mut request =
            RemotingCommand::create_request_command(RequestCode::GetAllMessageRequestMode, EmptyHeader {});
        let mut response = handler
            .get_all_message_request_mode(&admin, RequestCode::GetAllMessageRequestMode, &mut request)
            .await
            .expect("get all message request mode should succeed")
            .expect("get all message request mode should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = MessageRequestModeSerializeWrapper::decode(
            response
                .take_body()
                .expect("message request mode response should contain body")
                .as_ref(),
        )
        .expect("decode message request mode body");
        assert!(body
            .message_request_mode_map()
            .get(&CheetahString::from_static_str("topic-a"))
            .and_then(|group_map| group_map.get(&CheetahString::from_static_str("group-a")))
            .is_some());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn get_broker_consume_stats_returns_grouped_offsets() {
        let runtime = new_test_runtime("broker-consume-stats").await;
        let admin = runtime.admin_runtime_for_test();
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);
        let mut group_config =
            rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
                CheetahString::from_static_str("group-a"),
            );
        admin
            .subscription_group_manager()
            .update_subscription_group_config(&mut group_config);
        admin.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1"),
            &CheetahString::from_static_str("group-a"),
            &CheetahString::from_static_str("topic-a"),
            0,
            6,
        );

        let handler = ConsumerRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetBrokerConsumeStats,
            GetConsumeStatsInBrokerHeader { is_order: false },
        );
        request.make_custom_header_to_net();

        let mut response = handler
            .get_broker_consume_stats(&admin, RequestCode::GetBrokerConsumeStats, &mut request)
            .await
            .expect("get broker consume stats should succeed")
            .expect("get broker consume stats should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = ConsumeStatsList::decode(
            response
                .take_body()
                .expect("broker consume stats should contain body")
                .as_ref(),
        )
        .expect("decode broker consume stats body");
        let group_stats = body
            .consume_stats_list
            .iter()
            .find_map(|entry| entry.get(&CheetahString::from_static_str("group-a")));
        let group_stats = group_stats.expect("group stats should exist");
        assert_eq!(group_stats.len(), 1);
        assert_eq!(group_stats[0].offset_table.len(), 1);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn query_correction_offset_marks_queue_as_max_when_compare_group_is_ahead() {
        let runtime = new_test_runtime("query-correction").await;
        let admin = runtime.admin_runtime_for_test();
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);
        admin.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1"),
            &CheetahString::from_static_str("group-a"),
            &CheetahString::from_static_str("topic-a"),
            0,
            12,
        );
        admin.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1"),
            &CheetahString::from_static_str("group-b"),
            &CheetahString::from_static_str("topic-a"),
            0,
            8,
        );

        let handler = ConsumerRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::QueryCorrectionOffset,
            QueryCorrectionOffsetHeader {
                filter_groups: Some(CheetahString::from_static_str("group-b")),
                compare_group: CheetahString::from_static_str("group-b"),
                topic: CheetahString::from_static_str("topic-a"),
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let mut response = handler
            .query_correction_offset(&admin, RequestCode::QueryCorrectionOffset, &mut request)
            .await
            .expect("query correction offset should succeed")
            .expect("query correction offset should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = QueryCorrectionOffsetBody::decode(
            response
                .take_body()
                .expect("query correction offset should contain body")
                .as_ref(),
        )
        .expect("decode query correction offset body");
        assert_eq!(body.correction_offsets.get(&0), Some(&i64::MAX));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn consume_message_directly_returns_offline_error_for_known_message() {
        let runtime = new_test_runtime("consume-message-directly").await;
        let mut admin = runtime.admin_runtime_for_test();
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);
        let mut group_config =
            rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
                CheetahString::from_static_str("group-a"),
            );
        admin
            .subscription_group_manager()
            .update_subscription_group_config(&mut group_config);
        let msg_id = put_test_message(&mut admin, "topic-a").await;

        let handler = ConsumerRequestHandler::new_with_factory(test_command_factory(9343, SerializeType::ROCKETMQ));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ConsumeMessageDirectly,
            ConsumeMessageDirectlyResultRequestHeader {
                consumer_group: CheetahString::from_static_str("group-a"),
                client_id: Some(CheetahString::from_static_str("client-a")),
                msg_id: Some(CheetahString::from_string(msg_id)),
                broker_name: None,
                topic: Some(CheetahString::from_static_str("topic-a")),
                topic_sys_flag: None,
                group_sys_flag: None,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .consume_message_directly(&admin, RequestCode::ConsumeMessageDirectly, &mut request)
            .await
            .expect("consume message directly should succeed")
            .expect("consume message directly should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert_owner_defaults(&response, 9343, SerializeType::ROCKETMQ);
        assert!(response
            .remark()
            .expect("offline response should contain remark")
            .contains("not online"));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn invoke_broker_to_reset_offset_assigns_server_side_offset() {
        let runtime = new_test_runtime("reset-offset").await;
        let admin = runtime.admin_runtime_for_test();
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);
        let mut group_config =
            rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
                CheetahString::from_static_str("group-a"),
            );
        admin
            .subscription_group_manager()
            .update_subscription_group_config(&mut group_config);
        admin
            .consumer_offset_manager()
            .commit_offset("127.0.0.1:10911".into(), &group, &topic, 0, 9);

        let handler = ConsumerRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::InvokeBrokerToResetOffset,
            ResetOffsetRequestHeader {
                topic: topic.clone(),
                group: group.clone(),
                queue_id: 0,
                offset: None,
                timestamp: -1,
                is_force: true,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let mut response = handler
            .invoke_broker_to_reset_offset(&admin, RequestCode::InvokeBrokerToResetOffset, &mut request)
            .await
            .expect("invoke broker to reset offset should succeed")
            .expect("invoke broker to reset offset should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(admin
            .consumer_offset_manager()
            .has_offset_reset("group-a", "topic-a", 0));
        let body = ResetOffsetBody::decode(
            response
                .take_body()
                .expect("reset offset response should contain body")
                .as_ref(),
        )
        .expect("decode reset offset body");
        assert_eq!(body.offset_table.len(), 1);
        let reset_offset = *body
            .offset_table
            .values()
            .next()
            .expect("reset response should contain a queue offset");
        assert_eq!(
            admin.consumer_offset_manager().query_offset(&group, &topic, 0),
            reset_offset
        );
        assert_eq!(
            admin
                .consumer_offset_manager()
                .query_then_erase_reset_offset(&topic, &group, 0),
            Some(reset_offset)
        );
        assert_eq!(
            admin.consumer_offset_manager().query_offset(&group, &topic, 0),
            reset_offset
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn invoke_broker_to_get_consumer_status_returns_offline_group_error() {
        let runtime = new_test_runtime("consumer-status").await;
        let handler = ConsumerRequestHandler::new_with_factory(test_command_factory(9344, SerializeType::JSON));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::InvokeBrokerToGetConsumerStatus,
            GetConsumerStatusRequestHeader::new(
                CheetahString::from_static_str("topic-a"),
                CheetahString::from_static_str("group-a"),
            ),
        );
        request.make_custom_header_to_net();

        let response = handler
            .invoke_broker_to_get_consumer_status(
                &runtime.admin_runtime_for_test(),
                RequestCode::InvokeBrokerToGetConsumerStatus,
                &mut request,
            )
            .await
            .expect("invoke broker to get consumer status should succeed")
            .expect("invoke broker to get consumer status should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert_owner_defaults(&response, 9344, SerializeType::JSON);
        assert!(response
            .remark()
            .expect("offline response should contain remark")
            .contains("No Any Consumer online"));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn invoke_broker_to_reset_offset_preserves_factory_for_client_side_error() {
        let runtime = new_test_runtime_with_server_side_reset("client-reset-error", false).await;
        let handler = ConsumerRequestHandler::new_with_factory(test_command_factory(9345, SerializeType::JSON));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::InvokeBrokerToResetOffset,
            ResetOffsetRequestHeader {
                topic: CheetahString::from_static_str("missing-topic"),
                group: CheetahString::from_static_str("group-a"),
                queue_id: -1,
                offset: None,
                timestamp: -1,
                is_force: true,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .invoke_broker_to_reset_offset(
                &runtime.admin_runtime_for_test(),
                RequestCode::InvokeBrokerToResetOffset,
                &mut request,
            )
            .await
            .expect("invoke broker to reset offset should succeed")
            .expect("invoke broker to reset offset should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::TopicNotExist);
        assert_owner_defaults(&response, 9345, SerializeType::JSON);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn invoke_broker_to_reset_offset_preserves_factory_for_server_side_error() {
        let runtime = new_test_runtime("server-reset-error").await;
        let handler = ConsumerRequestHandler::new_with_factory(test_command_factory(9346, SerializeType::ROCKETMQ));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::InvokeBrokerToResetOffset,
            ResetOffsetRequestHeader {
                topic: CheetahString::from_static_str("missing-topic"),
                group: CheetahString::from_static_str("group-a"),
                queue_id: -1,
                offset: None,
                timestamp: -1,
                is_force: true,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .invoke_broker_to_reset_offset(
                &runtime.admin_runtime_for_test(),
                RequestCode::InvokeBrokerToResetOffset,
                &mut request,
            )
            .await
            .expect("invoke broker to reset offset should succeed")
            .expect("invoke broker to reset offset should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::TopicNotExist);
        assert_owner_defaults(&response, 9346, SerializeType::ROCKETMQ);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn query_subscription_by_consumer_returns_subscription_body() {
        let runtime = new_test_runtime("query-subscription").await;
        let admin = runtime.admin_runtime_for_test();
        let group = CheetahString::from_static_str("group-a");
        let topic = CheetahString::from_static_str("topic-a");
        let subscription = SubscriptionData {
            topic: topic.clone(),
            sub_string: CheetahString::from_static_str("*"),
            ..Default::default()
        };
        admin.consumer_manager().compensate_basic_consumer_info(
            &group,
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
        );
        admin
            .consumer_manager()
            .compensate_subscribe_data(&group, &topic, &subscription);

        let handler = ConsumerRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::QuerySubscriptionByConsumer,
            QuerySubscriptionByConsumerRequestHeader {
                group: CheetahString::from_static_str("group-a"),
                topic: CheetahString::from_static_str("topic-a"),
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let mut response = handler
            .query_subscription_by_consumer(&admin, RequestCode::QuerySubscriptionByConsumer, &mut request)
            .await
            .expect("query subscription by consumer should succeed")
            .expect("query subscription by consumer should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = QuerySubscriptionResponseBody::decode(
            response
                .take_body()
                .expect("query subscription response should contain body")
                .as_ref(),
        )
        .expect("decode query subscription response body");
        assert_eq!(body.group, "group-a");
        assert_eq!(body.topic, "topic-a");
        assert_eq!(
            body.subscription_data
                .expect("subscription data should be present")
                .topic,
            CheetahString::from_static_str("topic-a")
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn query_consume_time_span_returns_queue_metadata() {
        let runtime = new_test_runtime("consume-time-span").await;
        let admin = runtime.admin_runtime_for_test();
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);

        let handler = ConsumerRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::QueryConsumeTimeSpan,
            QueryConsumeTimeSpanRequestHeader {
                topic: CheetahString::from_static_str("topic-a"),
                group: CheetahString::from_static_str("group-a"),
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let mut response = handler
            .query_consume_time_span(&admin, RequestCode::QueryConsumeTimeSpan, &mut request)
            .await
            .expect("query consume time span should succeed")
            .expect("query consume time span should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = QueryConsumeTimeSpanBody::decode(
            response
                .take_body()
                .expect("query consume time span response should contain body")
                .as_ref(),
        )
        .expect("decode query consume time span body");
        assert_eq!(body.consume_time_span_set.len(), 1);
        assert_eq!(
            body.consume_time_span_set[0]
                .message_queue
                .as_ref()
                .expect("queue metadata should exist")
                .topic(),
            &CheetahString::from_static_str("topic-a")
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn clone_group_offset_copies_offsets_from_source_group() {
        let runtime = new_test_runtime("clone-offset").await;
        let admin = runtime.admin_runtime_for_test();
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);
        admin.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1"),
            &CheetahString::from_static_str("group-src"),
            &CheetahString::from_static_str("topic-a"),
            0,
            18,
        );

        let handler = ConsumerRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::CloneGroupOffset,
            CloneGroupOffsetRequestHeader {
                src_group: CheetahString::from_static_str("group-src"),
                dest_group: CheetahString::from_static_str("group-dest"),
                topic: Some(CheetahString::from_static_str("topic-a")),
                offline: true,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .clone_group_offset(&admin, RequestCode::CloneGroupOffset, &mut request)
            .await
            .expect("clone group offset should succeed")
            .expect("clone group offset should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(
            admin.consumer_offset_manager().query_offset(
                &CheetahString::from_static_str("group-dest"),
                &CheetahString::from_static_str("topic-a"),
                0,
            ),
            18
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
