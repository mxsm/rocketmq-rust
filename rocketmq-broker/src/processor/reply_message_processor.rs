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
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_model::common::message::message_accessor::MessageAccessor;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_protocol::protocol::header::reply_message_request_header::ReplyMessageRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerMasterAddressStore;
use rocketmq_store::BrokerStatsManager;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::PutMessageResult;
use rocketmq_store::PutMessageStatus;
use rocketmq_store::StatsType;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreError;
use rocketmq_transport::api::command_from_error_with_factory_and_opaque;
use rocketmq_transport::api::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestControlView;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ServerRequestCommand;
use rocketmq_transport::api::ServerRequestOutcome;
use rocketmq_transport::api::ServerRequestSender;
use rocketmq_transport::api::TransportError;

use crate::client::manager::producer_manager::ProducerReplySessionRegistry;
use tracing::info;
use tracing::warn;

use crate::mqtrace::send_message_context::SendMessageContext;
use crate::processor::response_assembly::BrokerResponseParts;
use crate::processor::send_message_processor::capability::SendMessageProcessorContext;
use crate::processor::send_message_processor::structured_store::append_message_with_control_reply;
use crate::processor::send_message_processor::structured_store::StoreHookCompletion;
use crate::processor::send_message_processor::structured_store::StructuredStoreReply;
use crate::processor::send_message_processor::structured_store::StructuredStoreReplyError;
use crate::processor::send_message_processor::Inner;
use crate::transaction::transactional_message_service::TransactionalMessageService;

const PUSH_REPLY_MESSAGE_TO_CLIENT_TIMEOUT_MILLIS: u64 = 10_000;

pub(crate) async fn append_reply_message_with_control_reply<S, M, B>(
    control: RequestControlView,
    store: &mut S,
    message: M,
    build_response: B,
) -> Result<StructuredStoreReply, StructuredStoreReplyError>
where
    S: MessageAppender<M>,
    M: Send,
    B: FnOnce(Result<S::Receipt, StoreError>) -> (RemotingCommand, StoreHookCompletion),
{
    append_message_with_control_reply(control, store, message, build_response).await
}

fn add_reply_response_metadata(response: &mut RemotingCommand, region_id: &str, trace_on: bool) {
    response
        .add_ext_field(MessageConst::PROPERTY_MSG_REGION, region_id)
        .add_ext_field(MessageConst::PROPERTY_TRACE_SWITCH, trace_on.to_string());
}

fn push_reply_call_failed_remark(sender_id: &str) -> String {
    format!("push reply message to {sender_id}fail.")
}

enum ReplyPushPortError {
    SessionNotFound,
    Rejected,
    Call {
        source: TransportError,
    },
    #[cfg(test)]
    TestCall,
}

trait ReplyPushPort {
    type Target: Send;

    fn acquire(&mut self, sender_id: &str) -> Result<Self::Target, ReplyPushPortError>;

    async fn push(
        &mut self,
        target: Self::Target,
        header: ReplyMessageRequestHeader,
        body: Option<Bytes>,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, ReplyPushPortError>;
}

struct BrokerReplyPushPort {
    sessions: ProducerReplySessionRegistry,
}

impl ReplyPushPort for BrokerReplyPushPort {
    type Target = ServerRequestSender;

    fn acquire(&mut self, sender_id: &str) -> Result<Self::Target, ReplyPushPortError> {
        self.sessions
            .find_request_sender(sender_id)
            .ok_or(ReplyPushPortError::SessionNotFound)
    }

    async fn push(
        &mut self,
        sender: Self::Target,
        header: ReplyMessageRequestHeader,
        body: Option<Bytes>,
        timeout_millis: u64,
    ) -> Result<RemotingCommand, ReplyPushPortError> {
        match sender
            .request(
                ServerRequestCommand::PushReplyMessageToClient { header, body },
                Duration::from_millis(timeout_millis),
            )
            .await
        {
            Ok(ServerRequestOutcome::Responded(response)) => Ok(response.into_command()),
            Ok(_) => Err(ReplyPushPortError::Rejected),
            Err(source) => Err(ReplyPushPortError::Call { source }),
        }
    }
}

fn apply_reply_store_result(
    put_message_result: &PutMessageResult,
    response_header: &mut SendMessageResponseHeader,
    queue_id: i32,
    max_message_size: i32,
) -> bool {
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
            warn!(
                "the message is illegal, maybe msg body or properties length not matched. msg body length limit \
                 {}B.",
                max_message_size
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
    if let (true, Some(append_result)) = (put_ok, put_message_result.append_message_result()) {
        response_header.set_msg_id(append_result.msg_id.clone().unwrap_or_default());
        response_header.set_queue_id(queue_id);
        response_header.set_queue_offset(append_result.logics_offset);
    }
    put_ok
}

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
///        |
///        v
/// Broker (store request message -> Consumer consumes)
///        |
///        v
/// Consumer (process and send reply message to Broker)
///        |
///        v
/// Broker -> ReplyMessageProcessor.handle()
///        |
///        v
/// Broker pushes reply message to original Producer (optional store)
///        |
///        v
/// Producer (receive reply in callback or future)
/// ```
pub struct ReplyMessageProcessor<MS: BrokerWriteStore, TS> {
    inner: Arc<Inner<MS, TS>>,
}

struct ReplyCompletionFacts {
    owner: Option<CheetahString>,
    body_len: usize,
}

impl ReplyCompletionFacts {
    fn capture(request: &RemotingCommand) -> Self {
        Self {
            owner: request
                .get_ext_fields()
                .and_then(|fields| fields.get(BrokerStatsManager::COMMERCIAL_OWNER))
                .cloned(),
            body_len: request.get_body().map_or(0, |body| body.len()),
        }
    }
}

impl<MS: BrokerWriteStore, TS> Clone for ReplyMessageProcessor<MS, TS> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<MS, TS> RequestProcessor for ReplyMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore + 'static,
    TS: TransactionalMessageService + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

fn reply_request_peer(origin: &RequestOrigin) -> rocketmq_error::RocketMQResult<SocketAddr> {
    match origin {
        RequestOrigin::Network { peer } => Ok(peer.address()),
        RequestOrigin::Embedded { .. } => Err(rocketmq_error::RocketMQError::illegal_argument(
            "ReplyMessage requires a trusted network origin for the persisted born host",
        )),
        _ => Err(rocketmq_error::RocketMQError::invariant_violated(
            "ReplyMessage received an unrecognized request origin",
        )),
    }
}

impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original = request.original_identity();
        let inbound_peer = reply_request_peer(request.origin())?;
        let result = self
            .process_request(
                inbound_peer,
                request.control().clone(),
                original.original_code(),
                original.original_opaque(),
                request.command_mut(),
            )
            .await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.kind() == rocketmq_error::ErrorKind::RequestHeaderError => {
                BrokerResponseParts::from_command(command_from_error_with_factory_and_opaque(
                    &self.inner.context.command_factory,
                    &error,
                    original.original_opaque(),
                ))?
                .into_handler_outcome()
            }
            Err(error) => Err(error),
        }
    }
}

impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    pub fn new(transactional_message_service: Arc<TS>, context: Arc<SendMessageProcessorContext<MS>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                send_message_hook_vec: Arc::new(Vec::new()),
                consume_message_hook_vec: Arc::new(Vec::new()),
                transactional_message_service,
                context,
            }),
        }
    }
}
impl<MS, TS> ReplyMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    async fn process_request(
        &self,
        inbound_peer: SocketAddr,
        control: RequestControlView,
        original_code: i32,
        original_opaque: i32,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let request_code = RequestCode::from(original_code);
        info!("ReplyMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::SendReplyMessage | RequestCode::SendReplyMessageV2 => {
                self.process_reply_message(inbound_peer, control, original_opaque, request)
                    .await
            }
            _ => BrokerResponseParts::from_command(request_code_not_supported_with_factory_remark_and_opaque(
                &self.inner.context.command_factory,
                original_code,
                format!("ReplyMessageProcessor request code {original_code} not supported"),
                original_opaque,
            ))?
            .into_handler_outcome(),
        }
    }

    async fn process_reply_message(
        &self,
        inbound_peer: SocketAddr,
        control: RequestControlView,
        original_opaque: i32,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let mut request_header = parse_request_header(request)?;
        let request_properties = MessageDecoder::string_to_message_properties(request_header.properties.as_ref());
        let mut send_message_context = self
            .inner
            .build_msg_context_at(inbound_peer, &mut request_header, request, request_properties)
            .0;
        self.inner.execute_send_message_hook_before(&send_message_context);

        let mut response = self
            .inner
            .context
            .command_factory
            .create_success_response_command()
            .set_opaque(original_opaque);
        let (region_id, trace_on, start_timestamp, store_reply_message_enable) = {
            let policy = self.inner.context.policy.snapshot();
            (
                policy.region_id.clone(),
                policy.trace_on,
                policy.start_accept_send_request_time_stamp as u64,
                policy.store_reply_message_enable,
            )
        };
        add_reply_response_metadata(&mut response, region_id.as_str(), trace_on);
        if current_millis() < start_timestamp {
            response = response
                .set_code(ResponseCode::SystemError)
                .set_remark(format!("broker unable to service, until, {start_timestamp}"));
            return self.finish_reply(response, send_message_context);
        }
        response.set_code_mut(-1);
        self.inner
            .msg_check_at(inbound_peer, request, &request_header, &mut response)
            .await;
        if response.code() != -1 {
            return self.finish_reply(response, send_message_context);
        }
        let topic_config = match self.inner.context.topics.select_topic_config(request_header.topic()) {
            Some(config) => config,
            None => {
                return self.finish_reply(
                    response
                        .set_code(ResponseCode::TopicNotExist)
                        .set_remark(format!("Topic {} does not exist", request_header.topic())),
                    send_message_context,
                );
            }
        };
        let mut queue_id = request_header.queue_id;
        if queue_id < 0 {
            queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }
        let mut message = self.build_msg_inner(inbound_peer, request, &request_header, queue_id);
        let completion_facts = ReplyCompletionFacts::capture(request);
        let store_host = self.inner.context.policy.snapshot().store_host;
        // Outbound reply delivery uses the session-owned request capability;
        // the inbound request never carries that authority.
        let mut push_port = BrokerReplyPushPort {
            sessions: self.inner.context.producer_reply_sessions.clone(),
        };
        let mut push_result =
            push_reply_message(&mut push_port, inbound_peer, store_host, &request_header, &mut message).await;
        message.properties_string = MessageDecoder::message_properties_to_string(message.get_properties());
        let mut response_header = SendMessageResponseHeader::default();
        Self::handle_push_reply_result(&mut push_result, &mut response, &mut response_header, queue_id);

        if !store_reply_message_enable {
            response = response.set_command_custom_header(response_header);
            return self.finish_reply(response, send_message_context);
        }

        let mut store = self.inner.context.store.clone();
        let processor = self;
        let reply = append_reply_message_with_control_reply(control, &mut store, message, move |result| match result {
            Ok(receipt) => {
                processor.handle_put_message_result(
                    receipt.result(),
                    completion_facts,
                    &mut response_header,
                    &mut send_message_context,
                    queue_id,
                    TopicMessageType::Normal,
                    request_header.topic(),
                );
                response = response.set_command_custom_header(response_header);
                processor
                    .inner
                    .execute_send_message_hook_after(Some(&mut response), &mut send_message_context);
                (response, StoreHookCompletion::BeforeReply)
            }
            Err(_) => {
                response = response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("message store not available");
                processor
                    .inner
                    .execute_send_message_hook_after(Some(&mut response), &mut send_message_context);
                (response, StoreHookCompletion::BeforeReply)
            }
        })
        .await
        .map_err(|error| rocketmq_error::RocketMQError::internal("reply-message-store", error))?;
        let (outcome, _) = reply.into_parts();
        Ok(outcome)
    }

    fn finish_reply(
        &self,
        mut response: RemotingCommand,
        mut send_message_context: SendMessageContext,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.inner
            .execute_send_message_hook_after(Some(&mut response), &mut send_message_context);
        BrokerResponseParts::from_command(response)?.into_handler_outcome()
    }

    // Build MessageExtBrokerInner to improve readability
    fn build_msg_inner(
        &self,
        inbound_peer: SocketAddr,
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
        msg_inner.message_ext_inner.born_host = inbound_peer;
        msg_inner.message_ext_inner.store_host = self.inner.context.policy.snapshot().store_host;
        msg_inner.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);
        msg_inner
    }

    fn handle_put_message_result(
        &self,
        put_message_result: &PutMessageResult,
        completion_facts: ReplyCompletionFacts,
        response_header: &mut SendMessageResponseHeader,
        send_message_context: &mut SendMessageContext,
        queue_id_int: i32,
        _message_type: TopicMessageType,
        topic: &str,
    ) {
        let put_ok = apply_reply_store_result(
            put_message_result,
            response_header,
            queue_id_int,
            self.inner.context.policy.snapshot().max_message_size,
        );
        let (commercial_size_per_msg, commercial_base_count) = {
            let policy = self.inner.context.policy.snapshot();
            (policy.commercial_size_per_msg, policy.commercial_base_count)
        };

        if put_ok {
            // Cache append_message_result to avoid repeated unwrap
            let append_result = put_message_result.append_message_result().unwrap();
            let stats_manager = &self.inner.context.broker_stats_manager;

            stats_manager.inc_topic_put_nums(topic, append_result.msg_num, 1);
            stats_manager.inc_topic_put_size(topic, append_result.wrote_bytes);
            stats_manager.inc_broker_put_nums(topic, append_result.msg_num);

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
                send_message_context.commercial_owner = completion_facts.owner.unwrap_or_default();
            }
        } else if self.inner.has_send_message_hook() {
            let wrote_size = completion_facts.body_len;
            let inc_value = (wrote_size as f64 / commercial_size_per_msg as f64).ceil() as i32;
            send_message_context.commercial_send_stats = StatsType::SendFailure;
            send_message_context.commercial_send_times = inc_value;
            send_message_context.commercial_send_size = wrote_size as i32;
            send_message_context.commercial_owner = completion_facts.owner.unwrap_or_default();
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
}

async fn push_reply_message<P: ReplyPushPort, M: MessageTrait>(
    port: &mut P,
    inbound_peer: SocketAddr,
    store_host: SocketAddr,
    request_header: &SendMessageRequestHeader,
    msg: &mut M,
) -> PushReplyResult {
    let sender_id = msg.property(&CheetahString::from_static_str(
        MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
    ));

    let Some(sender_id) = sender_id else {
        warn!(
            "{} is null, can not reply message",
            MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT
        );
        return PushReplyResult::failure(format!(
            "reply message properties[{}] is null",
            MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT
        ));
    };

    let target = match port.acquire(sender_id.as_str()) {
        Ok(target) => target,
        Err(ReplyPushPortError::SessionNotFound) => {
            warn!("typed reply push target session was not found");
            return PushReplyResult::failure(format!(
                "push reply message fail, session of <{}> not found.",
                sender_id
            ));
        }
        Err(ReplyPushPortError::Rejected) => {
            return PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()));
        }
        Err(ReplyPushPortError::Call { .. }) => {
            return PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()));
        }
        #[cfg(test)]
        Err(ReplyPushPortError::TestCall) => {
            return PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()));
        }
    };

    // Add PROPERTY_PUSH_REPLY_TIME to message properties BEFORE building header
    msg.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_PUSH_REPLY_TIME),
        CheetahString::from_string(current_millis().to_string()),
    );

    // Build reply message request header with properties (including PROPERTY_PUSH_REPLY_TIME)
    let reply_message_request_header = build_reply_request_header(inbound_peer, store_host, request_header, msg);
    match port
        .push(
            target,
            reply_message_request_header,
            msg.get_body().cloned(),
            PUSH_REPLY_MESSAGE_TO_CLIENT_TIMEOUT_MILLIS,
        )
        .await
    {
        Ok(response) if response.code() == ResponseCode::Success as i32 => PushReplyResult::success(),
        Ok(response) => {
            let code = response.code();
            warn!(code, "typed reply push returned a failure response");
            // Reuse extracted values to avoid duplicate format
            PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()))
        }
        Err(ReplyPushPortError::SessionNotFound) => {
            PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()))
        }
        Err(ReplyPushPortError::Rejected) => {
            PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()))
        }
        Err(ReplyPushPortError::Call { source }) => {
            warn!(code = ?source.code(), condition = ?source.condition(), "typed reply push failed");
            // Use compact error message to reduce allocation
            PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()))
        }
        #[cfg(test)]
        Err(ReplyPushPortError::TestCall) => {
            PushReplyResult::failure(push_reply_call_failed_remark(sender_id.as_str()))
        }
    }
}

// Build ReplyMessageRequestHeader with message properties
fn build_reply_request_header<M: MessageTrait>(
    inbound_peer: SocketAddr,
    store_host: SocketAddr,
    request_header: &SendMessageRequestHeader,
    msg: &M,
) -> ReplyMessageRequestHeader {
    // Use message properties directly (PROPERTY_PUSH_REPLY_TIME already added)
    let properties_string = MessageDecoder::message_properties_to_string(msg.get_properties());

    // Cache addresses to avoid repeated .to_string() calls
    let born_host = CheetahString::from_string(inbound_peer.to_string());
    let store_host = CheetahString::from_string(store_host.to_string());

    ReplyMessageRequestHeader {
        born_host,
        store_host,
        store_timestamp: current_millis() as i64,
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

fn parse_request_header(request: &RemotingCommand) -> rocketmq_error::RocketMQResult<SendMessageRequestHeader> {
    let request_code = RequestCode::from(request.code());
    let mut request_header_v2 = None;
    if RequestCode::SendReplyMessageV2 == request_code || RequestCode::SendReplyMessage == request_code {
        request_header_v2 = request
            .decode_command_custom_header_fast::<SendMessageRequestHeaderV2>()
            .ok();
    }

    match request_header_v2 {
        Some(header) => Ok(SendMessageRequestHeaderV2::create_send_message_request_header_v1(
            &header,
        )),
        None => request.decode_command_custom_header_fast::<SendMessageRequestHeader>(),
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
    msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
        .or_else(|| {
            // Fallback to old property name for backward compatibility
            msg.property(&CheetahString::from_static_str("REPLY_CORRELATION_ID"))
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
    use std::future::Future;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_store::StorePorts;

    use super::add_reply_response_metadata;
    use super::get_correlation_id_with_fallback;
    use super::push_reply_call_failed_remark;
    use super::PushReplyResult;
    use super::PUSH_REPLY_MESSAGE_TO_CLIENT_TIMEOUT_MILLIS;

    #[test]
    fn reply_shared_seam_accepts_an_arc_held_leaf() {
        type TransactionService =
            crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService<
                StorePorts,
            >;

        fn call_shared<'a>(
            leaf: &'a Arc<super::ReplyMessageProcessor<StorePorts, TransactionService>>,
            request: &'a mut super::RemotingRequest,
        ) -> impl Future<Output = rocketmq_error::RocketMQResult<super::HandlerOutcome>> + 'a {
            leaf.process_shared(request)
        }

        let _ = call_shared;
    }

    #[test]
    fn reply_response_keeps_region_and_trace_fields() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let mut response = RemotingCommand::create_success_response_command().set_serialize_type(serialize_type);
            add_reply_response_metadata(&mut response, "region-b", false);
            let mut encoded = bytes::BytesMut::new();

            response
                .try_fast_header_encode(&mut encoded)
                .expect("reply response should encode");
            let decoded = RemotingCommand::decode(&mut encoded)
                .expect("reply response should decode")
                .expect("reply response frame should be complete");

            let fields = decoded.ext_fields().expect("reply response ext fields");
            assert_eq!(
                fields.get(MessageConst::PROPERTY_MSG_REGION).map(CheetahString::as_str),
                Some("region-b")
            );
            assert_eq!(
                fields
                    .get(MessageConst::PROPERTY_TRACE_SWITCH)
                    .map(CheetahString::as_str),
                Some("false")
            );
        }
    }

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

    #[test]
    fn push_reply_message_to_client_timeout_matches_java_broker2client() {
        assert_eq!(PUSH_REPLY_MESSAGE_TO_CLIENT_TIMEOUT_MILLIS, 10_000);
    }

    #[test]
    fn push_reply_call_failed_remark_matches_java_semantics() {
        assert_eq!(
            push_reply_call_failed_remark("client-a"),
            "push reply message to client-afail."
        );
    }
}

#[cfg(test)]
#[path = "../../tests/unit/processor/reply_message/structured_store.rs"]
mod structured_store_tests;
