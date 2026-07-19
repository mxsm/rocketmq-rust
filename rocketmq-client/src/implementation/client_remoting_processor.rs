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

use std::backtrace::Backtrace;
use std::collections::HashMap;
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
use rocketmq_remoting::protocol::body::response::get_consumer_status_body::GetConsumerStatusBody;
use rocketmq_remoting::protocol::body::response::reset_offset_body::ResetOffsetBody;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::header::consume_message_directly_result_request_header::ConsumeMessageDirectlyResultRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_running_info_request_header::GetConsumerRunningInfoRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_remoting::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_remoting::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
use rocketmq_remoting::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_remoting::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
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
            RequestCode::ResetConsumerClientOffset => self.reset_consumer_client_offset(channel, request).await,
            RequestCode::GetConsumerStatusFromClient => self.get_consumer_status_from_client(request).await,
            RequestCode::GetConsumerRunningInfo => self.get_consumer_running_info(request).await,
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

    fn reject_request(&self, _code: i32) -> RejectRequestResponse {
        (false, None)
    }
}

impl ClientRemotingProcessor {
    fn capture_rust_jstack() -> String {
        let current_thread = std::thread::current();
        let thread_name = current_thread.name().unwrap_or("<unnamed>");
        let thread_id = format!("{:?}", current_thread.id());
        let backtrace = Backtrace::force_capture();

        format!(
            "Rust client diagnostic stack (current request processor thread only)\n{:<40}TID: {} STATE: \
             RUNNING\n{:<40}{}\n",
            thread_name, thread_id, thread_name, backtrace
        )
    }

    fn transaction_check_topic(topic: &str, namespace: &CheetahString) -> CheetahString {
        CheetahString::from_string(NamespaceUtil::without_namespace_with_namespace(
            topic,
            namespace.as_str(),
        ))
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
            match MessageSysFlag::try_get_compression_type(sys_flag)
                .and_then(|compression_type| CompressorFactory::get_compressor(compression_type).decompress(body))
            {
                Ok(decompressed) => msg.message.set_body(Some(decompressed)),
                Err(err) => {
                    warn!("err when uncompress reply message body: {}", err);
                    msg.message.set_body(Some(body.clone()));
                }
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
        if let Some(request_response_future) = REQUEST_FUTURE_HOLDER
            .remove_request_and_get(correlation_id.as_str())
            .await
        {
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

    async fn reset_consumer_client_offset(
        &mut self,
        channel: Channel,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = match request.decode_command_custom_header::<ResetOffsetRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!(
                    "ignore malformed ResetConsumerClientOffset callback from {}: {}",
                    channel.remote_address(),
                    error
                );
                return Ok(None);
            }
        };
        let offset_table = match request.get_body() {
            Some(body) => {
                let Some(reset_body) = ResetOffsetBody::decode(body) else {
                    warn!(
                        "ignore ResetConsumerClientOffset callback with malformed body from {}; topic={}, group={}",
                        channel.remote_address(),
                        request_header.topic,
                        request_header.group
                    );
                    return Ok(None);
                };
                reset_body.offset_table
            }
            None => {
                debug!(
                    "ResetConsumerClientOffset callback has no body from {}; topic={}, group={}, using empty offset \
                     table like Java client",
                    channel.remote_address(),
                    request_header.topic,
                    request_header.group
                );
                HashMap::new()
            }
        };

        self.client_instance
            .reset_offset(&request_header.topic, &request_header.group, offset_table)
            .await;
        Ok(None)
    }

    async fn get_consumer_status_from_client(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command().set_opaque(request.opaque());
        let request_header = match request.decode_command_custom_header::<GetConsumerStatusRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!("decode GetConsumerStatusFromClient request header failed: {}", error);
                return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                    "decode GetConsumerStatusFromClient request header failed: {error}"
                ))));
            }
        };

        match self
            .client_instance
            .get_consumer_status(&request_header.topic, &request_header.group)
            .await
        {
            Some(message_queue_table) => {
                let mut body = GetConsumerStatusBody::new();
                body.message_queue_table = message_queue_table;
                Ok(Some(response.set_code(ResponseCode::Success).set_body(body.encode())))
            }
            None => {
                debug!(
                    "GetConsumerStatusFromClient missing consumer group {}, returning empty offset table like Java \
                     client",
                    request_header.group
                );
                Ok(Some(
                    response
                        .set_code(ResponseCode::Success)
                        .set_body(GetConsumerStatusBody::new().encode()),
                ))
            }
        }
    }

    async fn get_consumer_running_info(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command().set_opaque(request.opaque());
        let request_header = match request.decode_command_custom_header::<GetConsumerRunningInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!("decode GetConsumerRunningInfo request header failed: {}", error);
                return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                    "decode GetConsumerRunningInfo request header failed: {error}"
                ))));
            }
        };

        match self
            .client_instance
            .consumer_running_info(&request_header.consumer_group)
            .await
        {
            Some(mut consumer_running_info) => {
                if request_header.jstack_enable {
                    consumer_running_info.jstack = Some(Self::capture_rust_jstack());
                }
                let body = consumer_running_info.encode_java_compatible()?;
                Ok(Some(response.set_code(ResponseCode::Success).set_body(body)))
            }
            None => Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The Consumer Group <{}> not exist in this consumer",
                request_header.consumer_group
            )))),
        }
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
            if let Some(namespace) = self
                .client_instance
                .client_config
                .get_namespace()
                .filter(|namespace| !namespace.is_empty())
            {
                let topic = Self::transaction_check_topic(message_ext.topic(), &namespace);
                message_ext.set_topic(topic);
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
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command().set_opaque(request.opaque());
        let request_header = match request.decode_command_custom_header::<ConsumeMessageDirectlyResultRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                warn!(
                    "decode ConsumeMessageDirectly request header failed from {}: {}",
                    channel.remote_address(),
                    error
                );
                return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                    "decode ConsumeMessageDirectly request header failed: {error}"
                ))));
            }
        };
        let Some(body) = request.get_body_mut() else {
            warn!(
                "ConsumeMessageDirectly request body is empty from {}; group={}",
                channel.remote_address(),
                request_header.consumer_group
            );
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("consumeMessageDirectly request body is empty"),
            ));
        };
        let Some(msg) = message_decoder::decode(body, true, true, false, false, false) else {
            warn!(
                "decode ConsumeMessageDirectly message body failed from {}; group={}",
                channel.remote_address(),
                request_header.consumer_group
            );
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("decode consumeMessageDirectly message failed"),
            ));
        };

        let result = self
            .client_instance
            .consume_message_directly(msg, &request_header.consumer_group, request_header.broker_name.clone())
            .await;
        if let Some(result) = result {
            match result.encode() {
                Ok(body) => Ok(Some(response.set_code(ResponseCode::Success).set_body(body))),
                Err(error) => {
                    warn!(
                        "encode ConsumeMessageDirectly result failed from {}; group={}: {:?}",
                        channel.remote_address(),
                        request_header.consumer_group,
                        error
                    );
                    Ok(Some(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark("encode consumeMessageDirectly result failed"),
                    ))
                }
            }
        } else {
            warn!("consumeMessageDirectly, consume message failed");
            Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The Consumer Group <{}> not exist in this consumer",
                request_header.consumer_group
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_remoting::local::LocalRequestHarness;

    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
    use crate::consumer::consumer_impl::process_queue::ProcessQueue;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use crate::consumer::mq_consumer_inner::MQConsumerInnerImpl;
    use crate::consumer::store::offset_store::OffsetStore;
    use crate::consumer::store::read_offset_type::ReadOffsetType;
    use crate::producer::request_response_future::RequestResponseFuture;

    async fn client_with_push_consumer(
        group: CheetahString,
        client_id: &str,
    ) -> (ArcMut<MQClientInstance>, ArcMut<DefaultMQPushConsumerImpl>) {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, client_id, None);
        let consumer_config = ConsumerConfig {
            consumer_group: group.clone(),
            ..Default::default()
        };
        let mut consumer_impl = ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            consumer_config,
            None,
        ));
        let wrapper = consumer_impl.clone();
        consumer_impl.set_default_mqpush_consumer_impl(wrapper);
        consumer_impl.offset_store = Some(Arc::new(OffsetStore::new_test()));

        let registered = client_instance
            .mut_from_ref()
            .register_consumer(&group, MQConsumerInnerImpl::from_push(consumer_impl.clone()))
            .await;
        assert!(registered, "test consumer should register");

        (client_instance, consumer_impl)
    }

    #[tokio::test]
    async fn reject_request_matches_java_client_processor_contract() {
        let client_config = ClientConfig {
            namesrv_addr: None,
            ..Default::default()
        };
        let client_instance = MQClientInstance::new_arc(client_config, 0, "reject-request-test", None);
        let processor = ClientRemotingProcessor::new(client_instance);

        let (rejected, response) =
            RequestProcessor::reject_request(&processor, RequestCode::CheckTransactionState as i32);

        assert!(!rejected);
        assert!(response.is_none());
    }

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
    async fn reset_consumer_client_offset_updates_matching_consumer_offsets() {
        let topic = CheetahString::from_static_str("reset-topic");
        let group = CheetahString::from_static_str("reset-group");
        let (client_instance, mut consumer_impl) =
            client_with_push_consumer(group.clone(), "reset-consumer-offset-test").await;
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 0);
        let process_queue = std::sync::Arc::new(ProcessQueue::new());
        consumer_impl.set_consume_orderly(true);
        consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .write()
            .await
            .insert(mq.clone(), process_queue.clone());
        let mut reset_body = ResetOffsetBody::new();
        reset_body.offset_table.insert(mq.clone(), 123);
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ResetConsumerClientOffset,
            ResetOffsetRequestHeader {
                topic: topic.clone(),
                group: group.clone(),
                queue_id: -1,
                offset: None,
                timestamp: current_millis() as i64,
                is_force: true,
                topic_request_header: None,
            },
        )
        .set_body(reset_body.encode());
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("reset offset callback should not fail");

        assert!(
            response.is_none(),
            "ResetConsumerClientOffset is a one-way broker callback"
        );
        assert!(process_queue.is_dropped());
        assert!(!consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await
            .contains_key(&mq));
        let offset_store = consumer_impl
            .offset_store
            .as_ref()
            .expect("test consumer should have offset store");
        assert_eq!(offset_store.test_persisted_offset(&mq), Some(123));
        assert_eq!(offset_store.read_offset(&mq, ReadOffsetType::ReadFromMemory).await, -1);
    }

    #[tokio::test]
    async fn reset_consumer_client_offset_with_missing_body_uses_empty_offset_table_like_java() {
        let topic = CheetahString::from_static_str("reset-missing-body-topic");
        let group = CheetahString::from_static_str("reset-missing-body-group");
        let (client_instance, _) = client_with_push_consumer(group.clone(), "reset-missing-body-test").await;
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ResetConsumerClientOffset,
            ResetOffsetRequestHeader {
                topic,
                group,
                queue_id: -1,
                offset: None,
                timestamp: current_millis() as i64,
                is_force: true,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("missing-body reset offset callback should not fail");

        assert!(
            response.is_none(),
            "missing reset body should still be a one-way Java-compatible callback"
        );
    }

    #[tokio::test]
    async fn get_consumer_status_from_client_returns_offset_table_body() {
        let topic = CheetahString::from_static_str("status-topic");
        let group = CheetahString::from_static_str("status-group");
        let (client_instance, consumer_impl) = client_with_push_consumer(group.clone(), "consumer-status-test").await;
        let mq = MessageQueue::from_parts(topic.clone(), "broker-a", 1);
        consumer_impl
            .offset_store
            .as_ref()
            .expect("test consumer should have offset store")
            .update_offset(&mq, 456, false)
            .await;

        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerStatusFromClient,
            GetConsumerStatusRequestHeader {
                topic,
                group,
                client_addr: None,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("consumer status callback should not fail")
            .expect("consumer status callback should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("consumer status response should have body");
        let body = GetConsumerStatusBody::decode(body).expect("consumer status body should decode");
        assert_eq!(body.message_queue_table.get(&mq), Some(&456));
    }

    #[tokio::test]
    async fn get_consumer_status_from_client_missing_group_returns_empty_success_like_java() {
        let client_instance =
            MQClientInstance::new_arc(ClientConfig::default(), 0, "consumer-status-missing-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerStatusFromClient,
            GetConsumerStatusRequestHeader {
                topic: CheetahString::from_static_str("missing-status-topic"),
                group: CheetahString::from_static_str("missing-status-group"),
                client_addr: None,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("missing-group consumer status callback should not fail")
            .expect("missing-group consumer status callback should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("consumer status response should have body");
        let body = GetConsumerStatusBody::decode(body).expect("consumer status body should decode");
        assert!(body.message_queue_table.is_empty());
    }

    #[tokio::test]
    async fn consume_message_directly_with_malformed_header_returns_system_error() {
        let client_instance =
            MQClientInstance::new_arc(ClientConfig::default(), 0, "consume-direct-malformed-header-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::ConsumeMessageDirectly);

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("malformed consume-direct callback should not fail")
            .expect("malformed consume-direct callback should return an error response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("decode ConsumeMessageDirectly request header failed")));
    }

    #[tokio::test]
    async fn consume_message_directly_with_missing_body_returns_system_error() {
        let client_instance =
            MQClientInstance::new_arc(ClientConfig::default(), 0, "consume-direct-missing-body-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ConsumeMessageDirectly,
            ConsumeMessageDirectlyResultRequestHeader {
                consumer_group: CheetahString::from_static_str("missing-body-group"),
                ..Default::default()
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("missing-body consume-direct callback should not fail")
            .expect("missing-body consume-direct callback should return an error response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("request body is empty")));
    }

    #[tokio::test]
    async fn consume_message_directly_with_malformed_body_returns_system_error() {
        let client_instance =
            MQClientInstance::new_arc(ClientConfig::default(), 0, "consume-direct-malformed-body-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ConsumeMessageDirectly,
            ConsumeMessageDirectlyResultRequestHeader {
                consumer_group: CheetahString::from_static_str("malformed-body-group"),
                ..Default::default()
            },
        )
        .set_body(Bytes::from_static(b"not-a-rocketmq-message"));
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("malformed-body consume-direct callback should not fail")
            .expect("malformed-body consume-direct callback should return an error response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("decode consumeMessageDirectly message failed")));
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
    async fn push_reply_message_with_unknown_compression_type_keeps_raw_body() {
        let correlation_id = CheetahString::from_string(format!("reply-unknown-compression-{}", current_millis()));
        let request_future = std::sync::Arc::new(RequestResponseFuture::new(correlation_id.clone(), 30_000, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_future.clone())
            .await;

        let client_instance =
            MQClientInstance::new_arc(ClientConfig::default(), 0, "reply-unknown-compression-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::PushReplyMessageToClient,
            valid_reply_message_header(
                Some(correlation_id.clone()),
                MessageSysFlag::COMPRESSED_FLAG | (0x7 << 8),
            ),
        )
        .set_body(Bytes::from_static(b"raw-reply-body"));
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("unknown compression type should not fail request processing")
            .expect("reply callback should return success response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let response_msg = request_future
            .get_response_msg()
            .expect("reply future should receive response");
        assert_eq!(
            response_msg.get_body().map(Bytes::as_ref),
            Some(b"raw-reply-body".as_slice())
        );
        assert!(
            REQUEST_FUTURE_HOLDER
                .get_request(correlation_id.as_str())
                .await
                .is_none(),
            "completed reply future should be removed from the holder"
        );
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

    #[test]
    fn transaction_check_topic_strips_only_matching_namespace_like_java() {
        let namespace = CheetahString::from_static_str("ns-a");

        assert_eq!(
            ClientRemotingProcessor::transaction_check_topic("ns-a%tx-topic", &namespace),
            "tx-topic"
        );
        assert_eq!(
            ClientRemotingProcessor::transaction_check_topic("ns-b%tx-topic", &namespace),
            "ns-b%tx-topic"
        );
        assert_eq!(
            ClientRemotingProcessor::transaction_check_topic("tx-topic", &namespace),
            "tx-topic"
        );
    }

    #[tokio::test]
    async fn get_consumer_running_info_returns_consumer_snapshot_body() {
        let group = CheetahString::from_static_str("running-info-group");
        let (client_instance, _consumer_impl) = client_with_push_consumer(group.clone(), "running-info-client").await;
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerRunningInfo,
            GetConsumerRunningInfoRequestHeader {
                consumer_group: group.clone(),
                client_id: CheetahString::from_static_str("running-info-client"),
                jstack_enable: true,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("consumer running info callback should not fail")
            .expect("consumer running info callback should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("consumer running info should have body");
        let info = rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::decode(body)
            .expect("consumer running info should decode");
        assert_eq!(
            info.properties
                .get(rocketmq_remoting::protocol::body::consumer_running_info::ConsumerRunningInfo::PROP_CONSUME_TYPE)
                .map(String::as_str),
            Some("CONSUME_PASSIVELY")
        );
        let jstack = info.jstack.expect("jstack should be captured when requested");
        assert!(jstack.contains("Rust client diagnostic stack"));
        assert!(jstack.contains("TID:"));
        assert!(!jstack.contains("thread stack capture is not available"));
    }

    #[tokio::test]
    async fn get_consumer_running_info_missing_group_returns_system_error() {
        let client_instance = MQClientInstance::new_arc(ClientConfig::default(), 0, "running-info-missing-test", None);
        let mut processor = ClientRemotingProcessor::new(client_instance);
        let harness = LocalRequestHarness::new()
            .await
            .expect("local remoting harness should start");
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerRunningInfo,
            GetConsumerRunningInfoRequestHeader {
                consumer_group: CheetahString::from_static_str("missing-running-info-group"),
                client_id: CheetahString::from_static_str("running-info-client"),
                jstack_enable: false,
                rpc_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = processor
            .process_request(harness.channel(), harness.context(), &mut request)
            .await
            .expect("missing-group running info callback should not fail")
            .expect("missing-group running info callback should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("not exist in this consumer")));
    }
}
