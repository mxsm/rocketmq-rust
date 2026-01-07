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

use cheetah_string::CheetahString;
use rocketmq_common::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::stats::stats_type::StatsType;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::processor::send_message_processor::Inner;
use crate::transaction::transactional_message_service::TransactionalMessageService;

/// Processes reply messages in the Request-Reply pattern.
///
/// This processor handles the server-side logic of sending reply messages back to
/// requesting clients. It validates the reply message, pushes it to the target client,
/// and optionally stores it in the message store.
///
/// # Type Parameters
///
/// - `MS`: Message store implementation
/// - `TS`: Transactional message service implementation
///
/// # Thread Safety
///
/// This processor is designed to be used in a multi-threaded async environment.
/// All shared state access is properly synchronized through the broker runtime.
///
/// # Example Flow
///
/// ```text
/// Producer (send request)
///        ↓
///  Broker (store request message → Consumer consumes)
///        ↓
///  Consumer (process & send reply message to Broker)
///        ↓
///  Broker → ReplyMessageProcessor.handle()
///       ↓
///  Broker  push reply message to original Producer ((Optional) Store)
///        ↓
///  Producer (receive reply in callback or future)
///                                                      
///                                              
/// ```
pub struct ReplyMessageProcessor<MS: MessageStore, TS> {
    inner: Inner<MS, TS>,
}

impl<MS, TS> RequestProcessor for ReplyMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("ReplyMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::SendReplyMessage | RequestCode::SendReplyMessageV2 => {
                self.process_request_inner(channel, ctx, request_code, request).await
            }
            _ => {
                warn!(
                    "ReplyMessageProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("ReplyMessageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    pub fn new(
        transactional_message_service: ArcMut<TS>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        Self {
            inner: Inner {
                send_message_hook_vec: ArcMut::new(Vec::new()),
                consume_message_hook_vec: ArcMut::new(Vec::new()),
                transactional_message_service,
                broker_to_client: Default::default(),
                broker_runtime_inner,
            },
        }
    }
}
impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_header = parse_request_header(request)?;
        let mut mqtrace_context = self
            .inner
            .build_msg_context(&channel, &ctx, &mut request_header, request);
        self.inner.execute_send_message_hook_before(&mqtrace_context);

        let mut response = self
            .process_reply_message_request(&ctx, &channel, request, &mut mqtrace_context, request_header)
            .await;

        self.inner
            .execute_send_message_hook_after(Some(&mut response), &mut mqtrace_context);
        Ok(Some(response))
    }

    async fn process_reply_message_request(
        &mut self,
        ctx: &ConnectionHandlerContext,
        channel: &Channel,
        request: &mut RemotingCommand,
        send_message_context: &mut SendMessageContext,
        request_header: SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command().set_opaque(request.opaque());

        // Cache broker config values to reduce lock contention
        let (region_id, trace_on, start_timstamp, store_reply_message_enable) = {
            let config = self.inner.broker_runtime_inner.broker_config();
            (
                config.region_id.clone(),
                config.trace_on,
                config.start_accept_send_request_time_stamp as u64,
                config.store_reply_message_enable,
            )
        };

        response
            .add_ext_field(MessageConst::PROPERTY_MSG_REGION, region_id.as_str())
            .add_ext_field(MessageConst::PROPERTY_TRACE_SWITCH, trace_on.to_string());

        if get_current_millis() < start_timstamp {
            return response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!("broker unable to service, until, {start_timstamp}"));
        }
        response.set_code_mut(-1);
        self.inner
            .msg_check(channel, ctx, request, &request_header, &mut response)
            .await;
        if response.code() != -1 {
            return response;
        }
        let mut queue_id_int = request_header.queue_id;
        let topic_config = match self
            .inner
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(request_header.topic())
        {
            Some(config) => config,
            None => {
                warn!("Topic {} not found", request_header.topic());
                return response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("Topic {} does not exist", request_header.topic()));
            }
        };
        if queue_id_int < 0 {
            queue_id_int = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        // Build message inner with extracted helper
        let mut msg_inner = self.build_msg_inner(channel, request, &request_header, queue_id_int);

        let mut push_reply_result = self
            .push_reply_message(channel, ctx, &request_header, &mut msg_inner)
            .await;

        // Update properties_string after msg_inner properties are modified
        msg_inner.properties_string = MessageDecoder::message_properties_to_string(msg_inner.get_properties());

        let mut response_header = SendMessageResponseHeader::default();
        Self::handle_push_reply_result(
            &mut push_reply_result,
            &mut response,
            &mut response_header,
            queue_id_int,
        );

        // Use cached config value
        if store_reply_message_enable {
            let Some(store) = self.inner.broker_runtime_inner.message_store_mut().as_mut() else {
                const STORE_ERROR: &str = "message store not available";
                warn!("process reply message, {}", STORE_ERROR);
                return response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(STORE_ERROR.to_string());
            };

            let put_message_result = store.put_message(msg_inner).await;
            self.handle_put_message_result(
                put_message_result,
                request,
                &mut response_header,
                send_message_context,
                queue_id_int,
                TopicMessageType::Normal,
                request_header.topic(),
            );
        }
        response.set_command_custom_header(response_header)
    }

    // Build MessageExtBrokerInner to improve readability
    fn build_msg_inner(
        &self,
        channel: &Channel,
        request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
        queue_id_int: i32,
    ) -> MessageExtBrokerInner {
        let mut msg_inner = MessageExtBrokerInner::default();
        msg_inner.set_topic(request_header.topic().to_owned());
        msg_inner.message_ext_inner.queue_id = queue_id_int;
        if let Some(body) = request.body() {
            msg_inner.set_body(body.clone());
        }
        msg_inner.set_flag(request_header.flag);
        MessageAccessor::set_properties(
            &mut msg_inner,
            MessageDecoder::string_to_message_properties(request_header.properties.as_ref()),
        );
        msg_inner.properties_string = request_header.properties.clone().unwrap_or_default();
        msg_inner.message_ext_inner.born_timestamp = request_header.born_timestamp;
        msg_inner.message_ext_inner.born_host = channel.remote_address();
        msg_inner.message_ext_inner.store_host = self.inner.broker_runtime_inner.store_host();
        msg_inner.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);
        msg_inner
    }

    fn handle_put_message_result(
        &mut self,
        put_message_result: PutMessageResult,
        request: &RemotingCommand,
        response_header: &mut SendMessageResponseHeader,
        send_message_context: &mut SendMessageContext,
        queue_id_int: i32,
        _message_type: TopicMessageType,
        topic: &str,
    ) {
        let put_ok = match put_message_result.put_message_status() {
            PutMessageStatus::PutOk
            | PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable => true,
            PutMessageStatus::ServiceNotAvailable => {
                warn!(
                    "service not available now. It may be caused by one of the following reasons: the broker's disk \
                     is full, messages are put to the slave, message store has been shut down, etc."
                );
                false
            }
            PutMessageStatus::CreateMappedFileFailed => {
                warn!("create mapped file failed, remoting_server is busy or broken.");
                false
            }
            PutMessageStatus::MessageIllegal => {
                let max_size = self
                    .inner
                    .broker_runtime_inner
                    .message_store()
                    .map(|store| store.get_message_store_config().max_message_size)
                    .unwrap_or(4 * 1024 * 1024); // Default 4MB
                warn!(
                    "the message is illegal, maybe msg body or properties length not matched. msg body length limit \
                     {}B.",
                    max_size
                );
                false
            }
            PutMessageStatus::PropertiesSizeExceeded => {
                warn!("the message is illegal, maybe msg properties length limit 32KB.");
                false
            }
            PutMessageStatus::OsPageCacheBusy => {
                warn!("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                false
            }
            PutMessageStatus::UnknownError => {
                warn!("UNKNOWN_ERROR");
                false
            }
            _ => {
                warn!("UNKNOWN_ERROR DEFAULT");
                false
            }
        };
        let owner = request
            .get_ext_fields()
            .unwrap()
            .get(BrokerStatsManager::COMMERCIAL_OWNER)
            .cloned();
        // Cache config values (extract immediately to release lock)
        let (commercial_size_per_msg, commercial_base_count) = {
            let config = self.inner.broker_runtime_inner.broker_config();
            (config.commercial_size_per_msg, config.commercial_base_count)
        };

        if put_ok {
            // Cache append_message_result to avoid repeated unwrap
            let append_result = put_message_result.append_message_result().unwrap();
            let stats_manager = self.inner.broker_runtime_inner.broker_stats_manager();

            stats_manager.inc_topic_put_nums(topic, append_result.msg_num, 1);
            stats_manager.inc_topic_put_size(topic, append_result.wrote_bytes);
            stats_manager.inc_broker_put_nums(topic, append_result.msg_num);

            response_header.set_msg_id(append_result.msg_id.clone().unwrap_or_default());
            response_header.set_queue_id(queue_id_int);
            response_header.set_queue_offset(append_result.logics_offset);

            if self.inner.has_send_message_hook() {
                let msg_id = response_header.msg_id().clone();
                let queue_id = Some(response_header.queue_id());
                let queue_offset = Some(response_header.queue_offset());
                send_message_context.msg_id = msg_id;
                send_message_context.queue_id = queue_id;
                send_message_context.queue_offset = queue_offset;

                let wrote_size = append_result.wrote_bytes;
                let commercial_msg_num = (wrote_size as f64 / commercial_size_per_msg as f64).ceil() as i32;
                let inc_value = commercial_msg_num * commercial_base_count;
                send_message_context.commercial_send_stats = StatsType::SendSuccess;
                send_message_context.commercial_send_times = inc_value;
                send_message_context.commercial_send_size = wrote_size;
                send_message_context.commercial_owner = owner.unwrap_or_default();
            }
        } else if self.inner.has_send_message_hook() {
            let wrote_size = request.get_body().map_or(0, |body| body.len());
            let inc_value = (wrote_size as f64 / commercial_size_per_msg as f64).ceil() as i32;
            send_message_context.commercial_send_stats = StatsType::SendFailure;
            send_message_context.commercial_send_times = inc_value;
            send_message_context.commercial_send_size = wrote_size as i32;
            send_message_context.commercial_owner = owner.unwrap_or_default();
        }
    }

    fn handle_push_reply_result(
        push_reply_result: &mut PushReplyResult,
        response: &mut RemotingCommand,
        response_header: &mut SendMessageResponseHeader,
        queue_id_int: i32,
    ) {
        if !push_reply_result.success {
            response.set_code_mut(ResponseCode::SystemError);
            response.set_remark_mut(push_reply_result.remark.clone());
        } else {
            response.set_code_mut(ResponseCode::Success);
            //response.set_remark_mut(None);
            response_header.set_msg_id("0");
            response_header.set_queue_id(queue_id_int);
            response_header.set_queue_offset(0);
        }
    }

    async fn push_reply_message<M: MessageTrait>(
        &mut self,
        channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        request_header: &SendMessageRequestHeader,
        msg: &mut M,
    ) -> PushReplyResult {
        let sender_id = msg.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
        ));

        let Some(sender_id) = sender_id else {
            warn!(
                "{} is null, can not reply message",
                MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT
            );
            return PushReplyResult::failure(format!(
                "{} is null, can not reply message",
                MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT
            ));
        };

        let Some(mut reply_channel) = self
            .inner
            .broker_runtime_inner
            .producer_manager()
            .find_channel(sender_id.as_str())
        else {
            // Format once for both logging and error return
            warn!(
                "push reply message fail, channel of <{}> not found. Topic: {}, QueueId: {}",
                sender_id,
                request_header.topic(),
                request_header.queue_id
            );
            return PushReplyResult::failure(format!("push reply message fail, channel of <{}> not found", sender_id));
        };

        // Add PROPERTY_PUSH_REPLY_TIME to message properties BEFORE building header
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_PUSH_REPLY_TIME),
            CheetahString::from_string(get_current_millis().to_string()),
        );

        // Build reply message request header with properties (including PROPERTY_PUSH_REPLY_TIME)
        let reply_message_request_header = self.build_reply_request_header(channel, request_header, msg);
        let mut command = RemotingCommand::create_request_command(
            RequestCode::PushReplyMessageToClient,
            reply_message_request_header,
        );
        if let Some(body) = msg.get_body().cloned() {
            command.set_body_mut_ref(body);
        }

        match self
            .inner
            .broker_to_client
            .call_client(&mut reply_channel, command, 3000)
            .await
        {
            Ok(response) if response.code() == ResponseCode::Success as i32 => PushReplyResult::success(),
            Ok(response) => {
                let code = response.code();
                let remark = response.remark().map(|r| r.as_str()).unwrap_or("unknown error");
                warn!(
                    "push reply message to <{}> return fail, code: {}, remark: {}. Topic: {}, QueueId: {}, \
                     CorrelationId: {:?}",
                    sender_id,
                    code,
                    remark,
                    request_header.topic(),
                    request_header.queue_id,
                    msg.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
                );
                // Reuse extracted values to avoid duplicate format
                PushReplyResult::failure(format!(
                    "push reply message to {} failed, code: {}, remark: {}",
                    sender_id, code, remark
                ))
            }
            Err(error) => {
                warn!(
                    "push reply message to <{}> failed: {}. Channel: {:?}, Topic: {}, QueueId: {}",
                    sender_id,
                    error,
                    reply_channel.remote_address(),
                    request_header.topic(),
                    request_header.queue_id
                );
                // Use compact error message to reduce allocation
                PushReplyResult::failure(format!("push reply message to {} failed", sender_id))
            }
        }
    }

    // Build ReplyMessageRequestHeader with message properties
    fn build_reply_request_header<M: MessageTrait>(
        &self,
        channel: &Channel,
        request_header: &SendMessageRequestHeader,
        msg: &M,
    ) -> ReplyMessageRequestHeader {
        // Use message properties directly (PROPERTY_PUSH_REPLY_TIME already added)
        let properties_string = MessageDecoder::message_properties_to_string(msg.get_properties());

        // Cache addresses to avoid repeated .to_string() calls
        let born_host = CheetahString::from_string(channel.remote_address().to_string());
        let store_host = CheetahString::from_string(self.inner.broker_runtime_inner.store_host().to_string());

        ReplyMessageRequestHeader {
            born_host,
            store_host,
            store_timestamp: get_current_millis() as i64,
            producer_group: request_header.producer_group.clone(),
            topic: request_header.topic.clone(),
            default_topic: request_header.default_topic.clone(),
            default_topic_queue_nums: request_header.default_topic_queue_nums,
            queue_id: request_header.queue_id,
            sys_flag: request_header.sys_flag,
            born_timestamp: request_header.born_timestamp,
            flag: request_header.flag,
            properties: Some(properties_string),
            reconsume_times: request_header.reconsume_times,
            unit_mode: request_header.unit_mode,
            ..Default::default()
        }
    }
}

fn parse_request_header(request: &RemotingCommand) -> rocketmq_error::RocketMQResult<SendMessageRequestHeader> {
    let request_code = RequestCode::from(request.code());
    let mut request_header_v2 = None;
    if RequestCode::SendReplyMessageV2 == request_code || RequestCode::SendReplyMessage == request_code {
        request_header_v2 = request
            .decode_command_custom_header::<SendMessageRequestHeaderV2>()
            .ok();
    }

    match request_header_v2 {
        Some(header) => Ok(SendMessageRequestHeaderV2::create_send_message_request_header_v1(
            &header,
        )),
        None => request.decode_command_custom_header::<SendMessageRequestHeader>(),
    }
}

/// Extracts correlation ID from message properties with backward compatibility.
///
/// Supports both new (`PROPERTY_CORRELATION_ID`) and legacy (`REPLY_CORRELATION_ID`)
/// property names for compatibility with older clients.
///
/// # Arguments
///
/// * `msg` - Message containing properties to extract from
///
/// # Returns
///
/// Correlation ID if found, `None` otherwise
fn get_correlation_id_with_fallback<M: MessageTrait>(msg: &M) -> Option<CheetahString> {
    msg.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
        .or_else(|| {
            // Fallback to old property name for backward compatibility
            msg.get_property(&CheetahString::from_static_str("REPLY_CORRELATION_ID"))
        })
}

#[derive(Debug, Clone)]
struct PushReplyResult {
    success: bool,
    remark: String,
}

impl PushReplyResult {
    fn success() -> Self {
        Self {
            success: true,
            remark: String::new(),
        }
    }

    fn failure(remark: impl Into<String>) -> Self {
        Self {
            success: false,
            remark: remark.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_correlation_id_with_fallback_new_property() {
        let mut msg = MessageExtBrokerInner::default();
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
            CheetahString::from_static_str("test-correlation-123"),
        );

        let result = get_correlation_id_with_fallback(&msg);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_str(), "test-correlation-123");
    }

    #[test]
    fn test_get_correlation_id_with_fallback_legacy_property() {
        let mut msg = MessageExtBrokerInner::default();
        msg.put_property(
            CheetahString::from_static_str("REPLY_CORRELATION_ID"),
            CheetahString::from_static_str("legacy-correlation-456"),
        );

        let result = get_correlation_id_with_fallback(&msg);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_str(), "legacy-correlation-456");
    }

    #[test]
    fn test_get_correlation_id_prefers_new_over_legacy() {
        let mut msg = MessageExtBrokerInner::default();
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
            CheetahString::from_static_str("new-id-123"),
        );
        msg.put_property(
            CheetahString::from_static_str("REPLY_CORRELATION_ID"),
            CheetahString::from_static_str("old-id-456"),
        );

        let result = get_correlation_id_with_fallback(&msg);
        assert!(result.is_some());
        // Should prefer new property name
        assert_eq!(result.unwrap().as_str(), "new-id-123");
    }

    #[test]
    fn test_get_correlation_id_returns_none_when_missing() {
        let msg = MessageExtBrokerInner::default();
        let result = get_correlation_id_with_fallback(&msg);
        assert!(result.is_none());
    }

    #[test]
    fn test_push_reply_result_success() {
        let result = PushReplyResult::success();
        assert!(result.success);
        assert!(result.remark.is_empty());
    }

    #[test]
    fn test_push_reply_result_failure() {
        let result = PushReplyResult::failure("test error message");
        assert!(!result.success);
        assert_eq!(result.remark, "test error message");
    }

    #[test]
    fn test_push_reply_result_failure_with_string() {
        let error = String::from("dynamic error");
        let result = PushReplyResult::failure(error);
        assert!(!result.success);
        assert_eq!(result.remark, "dynamic error");
    }
}
