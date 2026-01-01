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

use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_common::common::compression::compressor_factory::CompressorFactory;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_remoting::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_remoting::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::factory::mq_client_instance::MQClientInstance;
use crate::producer::request_future_holder::REQUEST_FUTURE_HOLDER;

#[derive(Clone)]
pub struct ClientRemotingProcessor {
    pub(crate) client_instance: ArcMut<MQClientInstance>,
}

impl ClientRemotingProcessor {
    pub fn new(client_instance: ArcMut<MQClientInstance>) -> Self {
        Self { client_instance }
    }
}

impl RequestProcessor for ClientRemotingProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("process_request: {:?}", request_code);
        match request_code {
            RequestCode::CheckTransactionState => self.check_transaction_state(channel, ctx, request).await,
            RequestCode::ResetConsumerClientOffset => {
                unimplemented!("ResetConsumerClientOffset")
            }
            RequestCode::GetConsumerStatusFromClient => {
                unimplemented!("GetConsumerStatusFromClient")
            }
            RequestCode::GetConsumerRunningInfo => {
                unimplemented!("GetConsumerRunningInfo")
            }
            RequestCode::ConsumeMessageDirectly => self.consume_message_directly(channel, ctx, request).await,
            //RPC message handle code
            RequestCode::PushReplyMessageToClient => self.receive_reply_message(ctx, request).await,
            RequestCode::NotifyConsumerIdsChanged => self.notify_consumer_ids_changed(channel, ctx, request),

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
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
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
                            .set_remark("parse born_host failed"),
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
                            .set_remark("parse store_host failed"),
                    ));
                }
            }
        }
        let body = request.get_body();
        let sys_flag = request_header.sys_flag;

        if (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG {
            let de_result = CompressorFactory::get_compressor(MessageSysFlag::get_compression_type(sys_flag))
                .decompress(body.unwrap());
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
            CheetahString::from_static_str(MessageConst::PROPERTY_REPLY_MESSAGE_ARRIVE_TIME),
            CheetahString::from_string(receive_time.to_string()),
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
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap_or_default();
        if let Some(request_response_future) = REQUEST_FUTURE_HOLDER.get_request(correlation_id.as_str()).await {
            request_response_future.put_response_message(Some(Box::new(reply_msg)));
            if request_response_future.get_request_callback().is_some() {
                request_response_future.on_success();
            }
        } else {
            warn!(
                "receive reply message, but not matched any request, CorrelationId: {} , reply from host: {}",
                correlation_id, reply_msg.born_host
            )
        }
    }

    fn notify_consumer_ids_changed(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<NotifyConsumerIdsChangedRequestHeader>()
            .unwrap();

        info!(
            "receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",
            channel.remote_address(),
            request_header.consumer_group
        );

        self.client_instance.re_balance_immediately();

        Ok(None)
    }

    async fn check_transaction_state(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<CheckTransactionStateRequestHeader>()
            .unwrap();
        let message_ext = MessageDecoder::decode(request.get_body_mut().unwrap(), true, true, false, false, false);
        if let Some(mut message_ext) = message_ext {
            if let Some(ref namespace) = self.client_instance.client_config.get_namespace() {
                let topic = NamespaceUtil::without_namespace_with_namespace(
                    message_ext.get_topic(),
                    self.client_instance
                        .client_config
                        .get_namespace()
                        .unwrap_or_default()
                        .as_str(),
                );
                message_ext.set_topic(CheetahString::from_string(topic));
            }
            let transaction_id = message_ext.get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            ));
            if let Some(transaction_id) = transaction_id {
                if !transaction_id.is_empty() {
                    message_ext.set_transaction_id(transaction_id);
                }
            }
            let group =
                message_ext.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
            if let Some(group) = group {
                let producer = self.client_instance.select_producer(&group).await;
                if let Some(producer) = producer {
                    let addr = CheetahString::from_string(channel.remote_address().to_string());
                    producer.check_transaction_state(&addr, message_ext, request_header);
                } else {
                    warn!("checkTransactionState, pick producer group failed");
                }
            } else {
                warn!("checkTransactionState, pick producer group failed");
            }
        } else {
            warn!("checkTransactionState, decode message failed");
        };
        Ok(None)
    }

    async fn consume_message_directly(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ConsumeMessageDirectlyResultRequestHeader>()?;
        let body = request
            .get_body_mut()
            .ok_or(rocketmq_error::RocketMQError::IllegalArgument(
                "body is empty".to_string(),
            ))?;
        let msg = message_decoder::decode(body, true, true, false, false, false).ok_or(
            rocketmq_error::RocketMQError::IllegalArgument("decode message failed".to_string()),
        )?;

        let result = self
            .client_instance
            .consume_message_directly(msg, &request_header.consumer_group, request_header.broker_name.clone())
            .await;
        if let Some(result) = result {
            let body = result
                .encode()
                .map_err(|_| rocketmq_error::RocketMQError::IllegalArgument("encode result failed".to_string()))?;
            Ok(Some(RemotingCommand::create_response_command().set_body(body)))
        } else {
            warn!("consumeMessageDirectly, consume message failed");
            Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::SystemError).set_remark(format!(
                    "The Consumer Group <{}> not exist in this consumer",
                    request_header.consumer_group
                )),
            ))
        }
    }
}
