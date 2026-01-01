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

use std::collections::HashSet;

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::body::connection::Connection;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::client::net::broker_to_client::Broker2Client;

#[derive(Clone)]
pub(super) struct ConsumerRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> ConsumerRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS: MessageStore> ConsumerRequestHandler<MS> {
    pub async fn get_consumer_connection_list(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<GetConsumerConnectionListRequestHeader>()?;
        let consumer_group_info = self
            .broker_runtime_inner
            .consumer_manager()
            .get_consumer_group_info(request_header.get_consumer_group());
        match consumer_group_info {
            Some(consumer_group_info) => {
                let mut body_data = ConsumerConnection::new();
                body_data.set_consume_from_where(consumer_group_info.get_consume_from_where());
                body_data.set_consume_type(consumer_group_info.get_consume_type());
                body_data.set_message_model(consumer_group_info.get_message_model());
                let subscription_table_consumer = consumer_group_info.get_subscription_table();
                let subscription_table = body_data.get_subscription_table_mut();
                for key_value in subscription_table_consumer.iter() {
                    subscription_table.insert(key_value.key().clone(), key_value.clone());
                }

                for channel_info in consumer_group_info.get_channel_info_table().iter() {
                    let mut connection = Connection::new();
                    connection.set_client_id(channel_info.client_id().clone());
                    connection.set_language(channel_info.language());
                    connection.set_version(channel_info.version());
                    connection.set_client_addr(channel_info.key().remote_address().to_string().into());
                    body_data.insert_connection(connection);
                }
                let body = body_data.encode()?;
                response.set_body_mut_ref(body);
                Ok(Some(response))
            }
            None => Ok(Some(response.set_code(ResponseCode::ConsumerNotOnline).set_remark(
                format!("the consumer group[{}] not online", request_header.get_consumer_group()),
            ))),
        }
    }

    pub async fn get_consume_stats(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetConsumeStatsRequestHeader>()
            .unwrap();
        let mut consume_stats = ConsumeStats::new();
        let mut topics = HashSet::new();
        if request_header.get_topic().is_empty() {
            topics = self
                .broker_runtime_inner
                .consumer_offset_manager()
                .which_topic_by_consumer(request_header.get_consumer_group());
        } else {
            topics.insert(request_header.get_topic().clone());
        }
        for topic in topics.iter() {
            let topic_config = self
                .broker_runtime_inner
                .topic_config_manager()
                .select_topic_config(topic);
            if topic_config.is_none() {
                warn!(
                    "AdminBrokerProcessor#getConsumeStats: topic config does not exist, topic={}",
                    topic
                );
                continue;
            }

            let mapping_detail = self
                .broker_runtime_inner
                .topic_queue_mapping_manager()
                .get_topic_queue_mapping(topic);

            let find_subscription_data = self
                .broker_runtime_inner
                .consumer_manager()
                .find_subscription_data(request_header.get_consumer_group(), topic);

            if find_subscription_data.is_none()
                && self
                    .broker_runtime_inner
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
                mq.set_broker_name(self.broker_runtime_inner.broker_config().broker_name().clone());
                mq.set_queue_id(i as i32);

                let mut offset_wrapper = OffsetWrapper::new();

                let mut broker_offset = self
                    .broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_max_offset_in_queue(topic, i as i32);
                if broker_offset < 0 {
                    broker_offset = 0;
                }

                let mut consumer_offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
                    request_header.get_consumer_group(),
                    topic,
                    i as i32,
                );

                if mapping_detail.is_none() && consumer_offset < 0 {
                    consumer_offset = 0;
                }

                let pull_offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
                    request_header.get_consumer_group(),
                    topic,
                    i as i32,
                );

                offset_wrapper.set_broker_offset(broker_offset);
                offset_wrapper.set_consumer_offset(consumer_offset);
                offset_wrapper.set_pull_offset(std::cmp::max(consumer_offset, pull_offset));

                let time_offset = consumer_offset - 1;
                if time_offset >= 0 {
                    let last_timestamp = self
                        .broker_runtime_inner
                        .message_store()
                        .unwrap()
                        .get_message_store_timestamp(topic, i as i32, time_offset);
                    if last_timestamp > 0 {
                        offset_wrapper.set_last_timestamp(last_timestamp);
                    }
                }

                consume_stats.get_offset_table_mut().insert(mq, offset_wrapper);
            }

            let consume_tps = self
                .broker_runtime_inner
                .broker_stats_manager()
                .tps_group_get_nums(request_header.get_consumer_group(), topic);

            let new_consume_tps = consume_stats.get_consume_tps() + consume_tps;
            consume_stats.set_consume_tps(new_consume_tps);
        }
        let body = consume_stats.encode().expect("consume stats encode failed");
        response.set_body_mut_ref(body);
        Ok(Some(response))
    }

    pub async fn get_all_consumer_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let content = self.broker_runtime_inner.consumer_offset_manager_mut().encode();
        if !content.is_empty() {
            response.set_body_mut_ref(content);
            Ok(Some(response))
        } else {
            Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No consumer offset in this broker"),
            ))
        }
    }

    pub async fn get_consumer_running_info(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = match request.decode_command_custom_header::<GetConsumerRunningInfoRequestHeader>() {
            Ok(header) => header,
            Err(e) => {
                let response = RemotingCommand::create_response_command()
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!("decode GetConsumerRunningInfoRequestHeader failed: {}", e));
                return Ok(Some(response));
            }
        };

        self.call_consumer(
            request.clone(),
            request_header.consumer_group.as_str(),
            request_header.client_id.as_str(),
        )
        .await
    }

    async fn call_consumer(
        &mut self,
        request: RemotingCommand,
        consumer_group: &str,
        client_id: &str,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();

        let client_channel_info = self
            .broker_runtime_inner
            .consumer_manager()
            .find_channel_by_client_id(consumer_group, client_id);

        if client_channel_info.is_none() {
            response = response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!("The Consumer <{}> <{}> not online", consumer_group, client_id));
            return Ok(Some(response));
        }

        let client_channel_info = client_channel_info.unwrap();

        if client_channel_info.version() < RocketMqVersion::V3_1_8_SNAPSHOT.ordinal() as i32 {
            response = response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The Consumer <{}> Version <{}> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
                client_id,
                RocketMqVersion::from_ordinal(client_channel_info.version() as u32).name()
            ));
            return Ok(Some(response));
        }

        let mut channel = client_channel_info.channel().clone();

        // Default timeout is 5000ms, same as Java implementation
        let timeout_millis = 5000u64;

        match Broker2Client.call_client(&mut channel, request, timeout_millis).await {
            Ok(result) => Ok(Some(result)),
            Err(e) => {
                let (code, error_type) = match &e {
                    rocketmq_error::RocketMQError::Network(
                        rocketmq_error::NetworkError::RequestTimeout { .. }
                        | rocketmq_error::NetworkError::ConnectionTimeout { .. },
                    ) => (ResponseCode::ConsumeMsgTimeout, "Timeout"),
                    _ => (ResponseCode::SystemError, "Exception"),
                };

                response = response.set_code(code).set_remark(format!(
                    "consumer <{}> <{}> {}: {:?}",
                    consumer_group, client_id, error_type, e
                ));
                Ok(Some(response))
            }
        }
    }
}
