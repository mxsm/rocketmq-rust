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

use cheetah_string::CheetahString;
use rocketmq_common::common::FAQUrl;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::batch_ack::BatchAck;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::log_file::MessageStore;

use crate::broker_error::BrokerError::BrokerRemotingError;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub struct AckMessageProcessor<MS> {
    topic_config_manager: TopicConfigManager,
    message_store: ArcMut<MS>,
}

impl<MS> AckMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(
        topic_config_manager: TopicConfigManager,
        message_store: ArcMut<MS>,
    ) -> AckMessageProcessor<MS> {
        AckMessageProcessor {
            topic_config_manager,
            message_store,
        }
    }

    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> crate::Result<Option<RemotingCommand>> {
        match request_code {
            RequestCode::AckMessage => self.process_ack(channel, ctx, request, true),
            RequestCode::BatchAckMessage => self.process_batch_ack(channel, ctx, request, true),
            _ => Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::MessageIllegal,
                    format!(
                        "request code not supported, request code: {:?}",
                        request_code
                    ),
                ),
            )),
        }
    }
}

impl<MS> AckMessageProcessor<MS>
where
    MS: MessageStore,
{
    fn process_ack(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> crate::Result<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<AckMessageRequestHeader>()
            .map_err(BrokerRemotingError)?;
        let topic_config = self
            .topic_config_manager
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::TopicNotExist,
                    format!(
                        "topic[{}] not exist, apply first please! {}",
                        request_header.topic,
                        FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                    ),
                ),
            ));
        }
        let topic_config = topic_config.unwrap();
        if request_header.queue_id >= topic_config.read_queue_nums as i32
            || request_header.queue_id < 0
        {
            let error_msg = format!(
                "queueId{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.read_queue_nums,
                channel.remote_address()
            );
            return Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::MessageIllegal,
                    error_msg,
                ),
            ));
        }
        let min_offset = self
            .message_store
            .get_min_offset_in_queue(&request_header.topic, request_header.queue_id);
        let max_offset = self
            .message_store
            .get_max_offset_in_queue(&request_header.topic, request_header.queue_id);
        if request_header.offset < min_offset || request_header.offset > max_offset {
            let error_msg = format!(
                "request offset not in queue offset range, request offset: {}, min offset: {}, \
                 max offset: {}",
                request_header.offset, min_offset, max_offset
            );
            return Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::NoMessage,
                    error_msg,
                ),
            ));
        }
        self.append_ack(Some(request_header), None, channel, None)
    }

    fn process_batch_ack(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> crate::Result<Option<RemotingCommand>> {
        unimplemented!("AckMessageProcessor process_ack")
    }

    fn append_ack(
        &mut self,
        _request_header: Option<AckMessageRequestHeader>,
        _batch_ack: Option<BatchAck>,
        _channel: Channel,
        _broker_name: Option<CheetahString>,
    ) -> crate::Result<Option<RemotingCommand>> {
        unimplemented!("AckMessageProcessor appendAck")
    }
}
