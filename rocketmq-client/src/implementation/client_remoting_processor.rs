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
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_remoting::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_remoting::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
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
            RequestCode::ResetConsumerClientOffset
            | RequestCode::GetConsumerStatusFromClient
            | RequestCode::GetConsumerRunningInfo => Ok(Some(Self::unsupported_client_callback_response(request_code))),
            RequestCode::ConsumeMessageDirectly => self.consume_message_directly(channel, ctx, request).await,
            //RPC message handle code
            RequestCode::PushReplyMessageToClient => self.receive_reply_message(ctx, request).await,
            RequestCode::NotifyUnsubscribeLite => self.notify_unsubscribe_lite(channel, request),
            RequestCode::NotifyConsumerIdsChanged => self.notify_consumer_ids_changed(channel, ctx, request),

            _ => {
                info!("Unknown request code: {:?}", request_code);
                Ok(None)
            }
        }
    }
}

impl ClientRemotingProcessor {
    fn unsupported_client_callback_response(request_code: RequestCode) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code_remark(
            ResponseCode::RequestCodeNotSupported,
            format!(
                "client callback request {:?}-{} is not implemented",
                request_code,
                request_code.to_i32()
            ),
        )
    }

    fn notify_unsubscribe_lite(
        &mut self,
        channel: Channel,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request.decode_command_custom_header::<NotifyUnsubscribeLiteRequestHeader>() {
            Ok(header) => {
                info!(
                    "receive broker's notify unsubscribe lite callback from {}; liteTopic={}, group={}, clientId={}",
                    channel.remote_address(),
                    header.lite_topic,
                    header.consumer_group,
                    header.client_id
                );
            }
            Err(error) => {
                warn!(
                    "decode notify unsubscribe lite callback header failed from {}; error={:?}",
                    channel.remote_address(),
                    error
                );
            }
        }
        Ok(None)
    }

    async fn receive_reply_message(
        &mut self,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let receive_time = current_millis();
        let response = RemotingCommand::create_response_command().set_opaque(request.opaque());
        let request_header = match request.decode_command_custom_header::<ReplyMessageRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!("decode reply message request header failed: {:?}", error);
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(format!("decode reply message request header failed: {error}")),
                ));
            }
        };

        let mut msg = MessageExt::default();
        msg.message.set_topic(request_header.topic.clone());
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
            let Some(body) = body else {
                warn!("compressed reply message body is missing");
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("compressed reply message body is missing"),
                ));
            };
            let de_result =
                CompressorFactory::get_compressor(MessageSysFlag::get_compression_type(sys_flag)).decompress(body);
            if let Ok(decompressed) = de_result {
                msg.message.set_body(Some(decompressed));
            } else {
                warn!("err when uncompress constant");
                msg.message.set_body(Some(body.clone()));
            }
        } else {
            msg.message.set_body(body.cloned());
        }
        msg.message.set_flag(request_header.flag);
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
        let correlation_id: CheetahString = reply_msg
            .message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .map(Into::into)
            .unwrap_or_default();
        if let Some(request_response_future) = REQUEST_FUTURE_HOLDER.get_request(correlation_id.as_str()).await {
            request_response_future.put_response_message(Some(Box::new(reply_msg)));
            REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
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
        let request_header = match request.decode_command_custom_header::<NotifyConsumerIdsChangedRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!(
                    "ignore malformed NotifyConsumerIdsChanged callback from {}: {}",
                    channel.remote_address(),
                    error
                );
                return Ok(None);
            }
        };

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
        let request_header = match request.decode_command_custom_header::<CheckTransactionStateRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!(
                    "ignore malformed CheckTransactionState callback from {}: {}",
                    channel.remote_address(),
                    error
                );
                return Ok(None);
            }
        };
        let Some(body) = request.get_body_mut() else {
            warn!(
                "ignore CheckTransactionState callback with missing body from {}",
                channel.remote_address()
            );
            return Ok(None);
        };
        let message_ext = MessageDecoder::decode(body, true, true, false, false, false);
        if let Some(mut message_ext) = message_ext {
            if let Some(ref namespace) = self.client_instance.client_config.get_namespace() {
                let topic = NamespaceUtil::without_namespace_with_namespace(
                    message_ext.topic(),
                    self.client_instance
                        .client_config
                        .get_namespace()
                        .unwrap_or_default()
                        .as_str(),
                );
                message_ext.set_topic(CheetahString::from_string(topic));
            }
            let transaction_id = message_ext.property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            ));
            if let Some(transaction_id) = transaction_id {
                if !transaction_id.is_empty() {
                    message_ext.set_transaction_id(transaction_id);
                }
            }
            let group = message_ext.property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use rocketmq_remoting::local::LocalRequestHarness;

    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::producer::request_response_future::RequestResponseFuture;

    fn valid_reply_message_header(correlation_id: Option<CheetahString>, sys_flag: i32) -> ReplyMessageRequestHeader {
        let properties = correlation_id.map(|correlation_id| {
            let mut properties = HashMap::new();
            properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
                correlation_id,
            );
            message_decoder::message_properties_to_string(&properties)
        });

        ReplyMessageRequestHeader {
            producer_group: CheetahString::from_static_str("reply-producer"),
            topic: CheetahString::from_static_str("reply-topic"),
            default_topic: CheetahString::from_static_str("TBW102"),
            default_topic_queue_nums: 1,
            queue_id: 0,
            sys_flag,
            born_timestamp: current_millis() as i64,
            flag: 0,
            properties,
            reconsume_times: None,
            unit_mode: Some(false),
            born_host: CheetahString::from_static_str("127.0.0.1:10000"),
            store_host: CheetahString::from_static_str("127.0.0.1:10911"),
            store_timestamp: current_millis() as i64,
            topic_request: None,
        }
    }

    #[tokio::test]
    async fn reply_message_processing_removes_completed_future() {
        let correlation_id = CheetahString::from_string(format!("reply-cleanup-{}", current_millis()));
        let request_future = std::sync::Arc::new(RequestResponseFuture::new(correlation_id.clone(), 30_000, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_future)
            .await;

        let mut reply_msg = MessageExt::default();
        MessageAccessor::put_property(
            &mut reply_msg.message,
            CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
            correlation_id.clone(),
        );

        ClientRemotingProcessor::process_reply_message(reply_msg).await;

        assert!(
            REQUEST_FUTURE_HOLDER
                .get_request(correlation_id.as_str())
                .await
                .is_none(),
            "completed reply future should be removed from the holder"
        );
    }

    #[tokio::test]
    async fn push_reply_message_with_malformed_header_returns_system_error() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "reply-malformed-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::PushReplyMessageToClient);

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("malformed reply callback should not fail")
            .expect("malformed reply callback should return an error response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("decode reply message request header failed")));
    }

    #[tokio::test]
    async fn push_reply_message_with_compressed_missing_body_returns_system_error() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "reply-missing-body-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::PushReplyMessageToClient,
            valid_reply_message_header(None, MessageSysFlag::COMPRESSED_FLAG),
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("missing body reply callback should not fail")
            .expect("missing body reply callback should return an error response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("compressed reply message body is missing")));
    }

    #[tokio::test]
    async fn push_reply_message_stores_response_and_removes_future() {
        let correlation_id = CheetahString::from_string(format!("reply-roundtrip-{}", current_millis()));
        let request_future = std::sync::Arc::new(RequestResponseFuture::new(correlation_id.clone(), 30_000, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_future.clone())
            .await;

        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "reply-roundtrip-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::PushReplyMessageToClient,
            valid_reply_message_header(Some(correlation_id.clone()), 0),
        )
        .set_body(Bytes::from_static(b"reply-body"));
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("reply callback should not fail")
            .expect("reply callback should return success response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(request_future.get_response_msg().is_some());
        assert!(
            REQUEST_FUTURE_HOLDER
                .get_request(correlation_id.as_str())
                .await
                .is_none(),
            "completed reply future should be removed from the holder"
        );
    }

    #[tokio::test]
    async fn notify_unsubscribe_lite_callback_is_explicit_oneway_noop() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "notify-lite-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::NotifyUnsubscribeLite);

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("notify unsubscribe lite callback should not fail");

        assert!(response.is_none(), "NotifyUnsubscribeLite is a one-way broker callback");
    }

    #[tokio::test]
    async fn notify_unsubscribe_lite_callback_with_valid_header_is_oneway_noop() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "notify-lite-valid-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::NotifyUnsubscribeLite,
            NotifyUnsubscribeLiteRequestHeader {
                lite_topic: CheetahString::from_static_str("lite-topic"),
                consumer_group: CheetahString::from_static_str("consumer-group"),
                client_id: CheetahString::from_static_str("client-id"),
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("notify unsubscribe lite callback should not fail");

        assert!(response.is_none(), "NotifyUnsubscribeLite is a one-way broker callback");
    }

    #[tokio::test]
    async fn notify_consumer_ids_changed_with_malformed_header_is_oneway_noop() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "notify-consumer-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::NotifyConsumerIdsChanged);

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("malformed notify callback should not fail");

        assert!(
            response.is_none(),
            "NotifyConsumerIdsChanged is a one-way broker callback"
        );
    }

    #[tokio::test]
    async fn check_transaction_state_with_malformed_header_is_oneway_noop() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "tx-malformed-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::CheckTransactionState);

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("malformed transaction callback should not fail");

        assert!(
            response.is_none(),
            "CheckTransactionState malformed broker callback should be one-way noop"
        );
    }

    #[tokio::test]
    async fn check_transaction_state_with_missing_body_is_oneway_noop() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "tx-missing-body-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::CheckTransactionState,
            CheckTransactionStateRequestHeader {
                topic: Some(CheetahString::from_static_str("tx-topic")),
                tran_state_table_offset: 0,
                commit_log_offset: 0,
                msg_id: Some(CheetahString::from_static_str("msg-id")),
                transaction_id: Some(CheetahString::from_static_str("tx-id")),
                offset_msg_id: None,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("missing body transaction callback should not fail");

        assert!(
            response.is_none(),
            "CheckTransactionState missing-body broker callback should be one-way noop"
        );
    }

    #[tokio::test]
    async fn client_callback_unimplemented_request_codes_return_unsupported() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "client-callback-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);

        for request_code in [
            RequestCode::ResetConsumerClientOffset,
            RequestCode::GetConsumerStatusFromClient,
            RequestCode::GetConsumerRunningInfo,
        ] {
            let harness = LocalRequestHarness::new()
                .await
                .expect("local remoting harness should start");
            let mut request = RemotingCommand::create_remoting_command(request_code);

            let response = processor
                .process_request(harness.channel(), harness.context(), &mut request)
                .await
                .expect("client callback processor should not fail")
                .expect("unsupported callback should return an explicit response");

            assert_eq!(
                ResponseCode::from(response.code()),
                ResponseCode::RequestCodeNotSupported
            );
            assert!(
                response
                    .remark()
                    .expect("unsupported callback should include a remark")
                    .contains(&request_code.to_i32().to_string()),
                "{request_code:?} unsupported response should name the request code"
            );
        }
    }
}
