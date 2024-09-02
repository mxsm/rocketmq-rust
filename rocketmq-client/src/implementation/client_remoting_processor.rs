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
use std::net::SocketAddr;

use bytes::Bytes;
use rocketmq_common::common::compression::compressor_factory::CompressorFactory;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_remoting::Result;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::producer::request_future_holder::REQUEST_FUTURE_HOLDER;

#[derive(Clone)]
pub struct ClientRemotingProcessor;

impl RequestProcessor for ClientRemotingProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("process_request: {:?}", request_code);
        match request_code {
            RequestCode::PushReplyMessageToClient => self.receive_reply_message(ctx, request).await,
            _ => {
                info!("Unknown request code: {:?}", request_code);
                Ok(None)
            }
        }
    }
}

impl ClientRemotingProcessor {
    async fn receive_reply_message(
        &mut self,
        ctx: ConnectionHandlerContext,
        request: RemotingCommand,
    ) -> Result<Option<RemotingCommand>> {
        let receive_time = get_current_millis();
        let response = RemotingCommand::create_response_command();
        let request_header = request
            .decode_command_custom_header::<ReplyMessageRequestHeader>()
            .unwrap();

        let mut msg = MessageExt::default();
        msg.message.topic = request_header.topic.clone();
        msg.queue_id = request_header.queue_id;
        msg.store_timestamp = request_header.store_timestamp;
        if !request_header.born_host.is_empty() {
            match request_header.born_host.parse::<SocketAddr>() {
                Ok(value) => msg.born_host = value,
                Err(_) => {
                    warn!("parse born_host failed: {}", request_header.store_host);
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark(Some("parse born_host failed".to_string())),
                    ));
                }
            }
        }
        if !request_header.store_host.is_empty() {
            match request_header.store_host.parse::<SocketAddr>() {
                Ok(value) => msg.store_host = value,
                Err(_) => {
                    warn!("parse store_host failed: {}", request_header.store_host);
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark(Some("parse store_host failed".to_string())),
                    ));
                }
            }
        }
        let body = request.get_body();
        let sys_flag = request_header.sys_flag;

        if (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG {
            let de_result =
                CompressorFactory::get_compressor(MessageSysFlag::get_compression_type(sys_flag))
                    .decompress(body.unwrap())
                    .map(Bytes::from);
            if let Ok(decompressed) = de_result {
                msg.message.body = Some(decompressed);
            } else {
                warn!("err when uncompress constant");
                msg.message.body = body.cloned();
            }
        }
        msg.message.flag = request_header.flag;
        MessageAccessor::set_properties(
            &mut msg.message,
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref()),
        );
        MessageAccessor::put_property(
            &mut msg.message,
            MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME,
            receive_time.to_string().as_str(),
        );
        msg.born_timestamp = request_header.born_timestamp;
        msg.reconsume_times = request_header.reconsume_times.unwrap_or(0);
        debug!("Receive reply message: {:?}", msg);
        Self::process_reply_message(msg).await;
        Ok(Some(response))
    }

    async fn process_reply_message(reply_msg: MessageExt) {
        let correlation_id = reply_msg
            .message
            .get_property(MessageConst::PROPERTY_CORRELATION_ID)
            .unwrap_or("".to_string());
        if let Some(request_response_future) = REQUEST_FUTURE_HOLDER
            .get_request(correlation_id.as_str())
            .await
        {
            request_response_future.put_response_message(Some(Box::new(reply_msg)));
            if request_response_future.get_request_callback().is_some() {
                request_response_future.on_success();
            }
        } else {
            warn!(
                "receive reply message, but not matched any request, CorrelationId: {} , reply \
                 from host: {}",
                correlation_id, reply_msg.born_host
            )
        }
    }
}
