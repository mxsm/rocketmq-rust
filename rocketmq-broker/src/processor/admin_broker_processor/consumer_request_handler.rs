/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashSet;

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_remoting::protocol::admin::offset_wrapper::OffsetWrapper;
use rocketmq_remoting::protocol::body::connection::Connection;
use rocketmq_remoting::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_remoting::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_store::log_file::MessageStore;
use tracing::warn;

use crate::processor::admin_broker_processor::Inner;

#[derive(Clone)]
pub(super) struct ConsumerRequestHandler {
    inner: Inner,
}

impl ConsumerRequestHandler {
    pub fn new(inner: Inner) -> Self {
        Self { inner }
    }
}

impl ConsumerRequestHandler {
    pub async fn get_consumer_connection_list(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetConsumerConnectionListRequestHeader>()
            .unwrap();
        let consumer_group_info = self
            .inner
            .consume_manager
            .get_consumer_group_info(request_header.get_consumer_group());
        match consumer_group_info {
            Some(consumer_group_info) => {
                let mut body_data = ConsumerConnection::new();
                body_data.set_consume_from_where(consumer_group_info.get_consume_from_where());
                body_data.set_consume_type(consumer_group_info.get_consume_type());
                body_data.set_message_model(consumer_group_info.get_message_model());
                let subscription_table =
                    consumer_group_info.get_subscription_table().read().clone();
                body_data
                    .get_subscription_table()
                    .extend(subscription_table);

                for (channel, info) in consumer_group_info.get_channel_info_table().read().iter() {
                    let mut connection = Connection::new();
                    connection.set_client_id(info.client_id().clone());
                    connection.set_language(info.language());
                    connection.set_version(info.version());
                    connection.set_client_addr(channel.remote_address().to_string());
                    body_data.get_connection_set().insert(connection);
                }
                let body = body_data.encode();
                response.set_body_mut_ref(body);
                Some(response)
            }
            None => Some(
                response
                    .set_code(ResponseCode::ConsumerNotOnline)
                    .set_remark(Some(format!(
                        "the consumer group[{}] not online",
                        request_header.get_consumer_group()
                    ))),
            ),
        }
    }

    pub async fn get_consume_stats(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<GetConsumeStatsRequestHeader>()
            .unwrap();
        let mut consume_stats = ConsumeStats::new();
        let mut topics = HashSet::new();
        if request_header.get_topic().is_empty() {
            topics = self
                .inner
                .consumer_offset_manager
                .which_topic_by_consumer(request_header.get_consumer_group());
        } else {
            topics.insert(request_header.get_topic().to_string());
        }
        for topic in topics.iter() {
            let topic_config = self.inner.topic_config_manager.select_topic_config(topic);
            if topic_config.is_none() {
                warn!(
                    "AdminBrokerProcessor#getConsumeStats: topic config does not exist, topic={}",
                    topic
                );
                continue;
            }

            let mapping_detail = self
                .inner
                .topic_queue_mapping_manager
                .get_topic_queue_mapping(topic);

            let find_subscription_data = self
                .inner
                .consume_manager
                .find_subscription_data(request_header.get_consumer_group(), topic);

            if find_subscription_data.is_none()
                && self
                    .inner
                    .consume_manager
                    .find_subscription_data_count(request_header.get_consumer_group())
                    > 0
            {
                warn!(
                    "AdminBrokerProcessor#getConsumeStats: topic does not exist in consumer \
                     group's subscription, topic={}, consumer group={}",
                    topic,
                    request_header.get_consumer_group()
                );
                continue;
            }

            for i in 0..topic_config.unwrap().get_read_queue_nums() {
                let mut mq = MessageQueue::new();
                mq.set_topic(topic.to_string().into());
                mq.set_broker_name(self.inner.broker_config.broker_name.clone().into());
                mq.set_queue_id(i as i32);

                let mut offset_wrapper = OffsetWrapper::new();

                let mut broker_offset = self
                    .inner
                    .default_message_store
                    .get_max_offset_in_queue(topic, i as i32);
                if broker_offset < 0 {
                    broker_offset = 0;
                }

                let mut consumer_offset = self.inner.consumer_offset_manager.query_offset(
                    request_header.get_consumer_group(),
                    topic,
                    i as i32,
                );

                if mapping_detail.is_none() && consumer_offset < 0 {
                    consumer_offset = 0;
                }

                let pull_offset = self.inner.consumer_offset_manager.query_offset(
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
                        .inner
                        .default_message_store
                        .get_message_store_timestamp(topic, i as i32, time_offset);
                    if last_timestamp > 0 {
                        offset_wrapper.set_last_timestamp(last_timestamp);
                    }
                }

                consume_stats.get_offset_table().insert(mq, offset_wrapper);
            }

            let consume_tps = self
                .inner
                .broker_stats_manager
                .tps_group_get_nums(request_header.get_consumer_group(), topic);

            let new_consume_tps = consume_stats.get_consume_tps() + consume_tps;
            consume_stats.set_consume_tps(new_consume_tps);
        }
        let body = consume_stats.encode();
        response.set_body_mut_ref(body);
        Some(response)
    }

    pub async fn get_all_consumer_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let mut response = RemotingCommand::create_response_command();
        let content = self.inner.consumer_offset_manager.encode();
        if !content.is_empty() {
            response.set_body_mut_ref(content);
            Some(response)
        } else {
            Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(Some("No consumer offset in this broker".to_string())),
            )
        }
    }
}
