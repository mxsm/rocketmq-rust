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
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use crate::config::broker_config::BrokerConfig;
use crate::send_message_constants::apply_topic_delivery_properties;
use crate::send_message_constants::has_valid_compaction_key;
use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rand::RngExt;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::common::message::message_accessor::MessageAccessor;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_model::common::message::message_enum::MessageType;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_model::common::mq_version::RocketMqVersion;
use rocketmq_model::common::producer::HandleV1;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::common::FAQUrl;
use rocketmq_model::common::TopicFilterType;
use rocketmq_model::common::TopicSysFlag;
use rocketmq_model::common::TopicSysFlag::build_sys_flag;
use rocketmq_model::utils::cleanup_policy_utils;
use rocketmq_model::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::RemotingSysResponseCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::code::response_code::ResponseCode::SystemError;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::common::message::message_decoder::message_properties_to_string;
use rocketmq_protocol::common::message::message_decoder::string_to_message_properties;
use rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::parse_request_header;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_runtime::common::time_utils;
use rocketmq_runtime::common::util_all;
use rocketmq_store::store_append_receipt;
use rocketmq_store::BrokerMasterAddressStore;
use rocketmq_store::BrokerStatsManager;
use rocketmq_store::BrokerWriteStore;
use rocketmq_store::PutMessageResult;
use rocketmq_store::PutMessageStatus;
use rocketmq_store::StatsType;
use rocketmq_store::StoreAppendReceipt;
use rocketmq_store::StoreHealthSnapshot;
use rocketmq_store::SyncFlushRuntimeInfo;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::StoreHealth;
use rocketmq_transport::api::error_response as remoting_error_response;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestId;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::ResponseObservation;
use tracing::debug;
use tracing::info;
use tracing::warn;
use tracing::Instrument;

use crate::mqtrace::consume_message_context::ConsumeMessageContext;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::mqtrace::send_message_hook::SendMessageHook;
use crate::processor::response_assembly::BrokerResponseParts;
use crate::send_message_constants::error_messages;
use crate::send_message_constants::message_limits;
use crate::send_message_constants::queue_config;
use crate::send_message_constants::retry_config;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::transaction::transactional_message_service::TransactionalMessageService;

pub(crate) mod capability;
mod message_builder;
pub(super) mod structured_store;

use capability::SendMessagePolicy;
use capability::SendMessageProcessorContext;
use message_builder::clear_reserved_properties;
use message_builder::enrich_parsed_send_message_request_properties;
use message_builder::recall_handle_topic_and_timestamp;
use message_builder::should_create_uniq_key;
use structured_store::append_message_with_control_reply;
use structured_store::await_store;
use structured_store::StoreAwaitControl;
use structured_store::StoreAwaitStopped;
use structured_store::StoreHookCompletion;

pub struct SendMessageProcessor<MS: BrokerWriteStore, TS> {
    inner: Arc<Inner<MS, TS>>,
    after_hooks: Arc<Mutex<HashMap<RequestId, SendMessageContext>>>,
}

struct SendCompletionFacts {
    opaque: i32,
    body_len: i32,
    owner: Option<CheetahString>,
    auth_type: Option<CheetahString>,
    owner_parent: Option<CheetahString>,
    owner_self: Option<CheetahString>,
}

impl SendCompletionFacts {
    fn capture(request: &RemotingCommand) -> Self {
        let binding = HashMap::new();
        let ext_fields = request.ext_fields().unwrap_or(&binding);
        Self {
            opaque: request.opaque(),
            body_len: request.body().as_ref().map_or(0, |body| body.len() as i32),
            owner: ext_fields.get(BrokerStatsManager::COMMERCIAL_OWNER).cloned(),
            auth_type: ext_fields.get(BrokerStatsManager::ACCOUNT_AUTH_TYPE).cloned(),
            owner_parent: ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_PARENT).cloned(),
            owner_self: ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_SELF).cloned(),
        }
    }
}

struct ParsedSendRequest {
    header: SendMessageRequestHeader,
    properties: HashMap<CheetahString, CheetahString>,
}

fn add_send_response_metadata(response: &mut RemotingCommand, region_id: CheetahString, trace_on: bool) {
    response.add_ext_field(MessageConst::PROPERTY_MSG_REGION, region_id);
    response.add_ext_field(
        MessageConst::PROPERTY_TRACE_SWITCH,
        CheetahString::from_static_str(if trace_on { "true" } else { "false" }),
    );
}

impl<MS: BrokerWriteStore, TS> Clone for SendMessageProcessor<MS, TS> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            after_hooks: Arc::clone(&self.after_hooks),
        }
    }
}

impl<MS, TS> SendMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    pub(crate) fn rejection_response(&self) -> Option<RemotingCommand> {
        let policy = self.inner.context.policy.snapshot();
        let enable_slave_acting_master = policy.enable_slave_acting_master;
        let broker_role = policy.broker_role;
        if !enable_slave_acting_master && broker_role == BrokerRole::Slave {
            return Some(
                self.inner
                    .context
                    .command_factory
                    .create_response_command_with_code_remark(
                        ResponseCode::SlaveNotAvailable,
                        "The broker is slave mode, not allowed to accept message",
                    ),
            );
        }
        let snapshot = match self.inner.context.store.health_snapshot() {
            Ok(snapshot) => snapshot,
            Err(_) => {
                return Some(
                    self.inner
                        .context
                        .command_factory
                        .create_response_command_with_code_remark(
                            ResponseCode::SystemBusy,
                            "store_backpressure reason=store_shutdown",
                        ),
                );
            }
        };
        if let Some(remark) = store_health_reject_remark(policy.as_ref(), snapshot) {
            return Some(
                self.inner
                    .context
                    .command_factory
                    .create_response_command_with_code_remark(ResponseCode::SystemBusy, remark),
            );
        }

        None
    }
}

impl<MS, TS> RequestProcessor for SendMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore + 'static,
    TS: TransactionalMessageService + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }

    fn observe_response(&self, observation: ResponseObservation) {
        let Some(observation) = observation.write_projection() else {
            return;
        };
        let Some(mut context) = self.after_hooks.lock().remove(&observation.request_id()) else {
            return;
        };
        self.inner.execute_send_message_hook_after(None, &mut context);
    }
}

fn send_request_peer(origin: &RequestOrigin) -> rocketmq_error::RocketMQResult<SocketAddr> {
    match origin {
        RequestOrigin::Network { peer } => Ok(peer.address()),
        RequestOrigin::Embedded { .. } => Err(RocketMQError::illegal_argument(
            "SendMessage requires a trusted network origin for the persisted born host",
        )),
        _ => Err(RocketMQError::invariant_violated(
            "SendMessage received an unrecognized request origin",
        )),
    }
}

impl<MS, TS> SendMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original = request.original_identity();
        let origin = request.origin().clone();
        let control = request.control().clone();
        let (receive_span, parsed_request) = self.receive_span_and_request(request.command());
        let result = self
            .process_request(
                origin,
                control,
                original.request_id(),
                original.original_code(),
                original.original_opaque(),
                original.is_one_way(),
                request.command_mut(),
                parsed_request,
            )
            .instrument(receive_span)
            .await;
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.descriptor() == &rocketmq_error::PROTOCOL_HEADER_INVALID => {
                let context = error.context();
                let view = PublicErrorView::try_new(error.descriptor(), &context)
                    .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
                BrokerResponseParts::from_command(remoting_error_response(
                    view,
                    RemotingErrorTarget::Reply {
                        factory: &self.inner.context.command_factory,
                        opaque: original.original_opaque(),
                    },
                ))?
                .into_handler_outcome()
            }
            Err(error) => Err(error),
        }
    }

    async fn process_request(
        &self,
        origin: RequestOrigin,
        control: rocketmq_transport::api::RequestControlView,
        request_id: RequestId,
        original_code: i32,
        original_opaque: i32,
        original_oneway: bool,
        request: &mut RemotingCommand,
        parsed_request: Option<ParsedSendRequest>,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let request_code = RequestCode::from(original_code);
        debug!("SendMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::SendMessage | RequestCode::SendMessageV2 | RequestCode::SendBatchMessage => {
                let inbound_peer = send_request_peer(&origin)?;
                self.process_send_message(
                    inbound_peer,
                    control,
                    request_id,
                    original_oneway,
                    request_code,
                    request,
                    parsed_request,
                )
                .await
            }
            RequestCode::ConsumerSendMsgBack => {
                let result = self.inner.consumer_send_msg_back(request).await;
                crate::processor::response_assembly::immediate_outcome_from_command_result(
                    &self.inner.context.command_factory,
                    result,
                    original_opaque,
                    "SendMessageProcessor consumer send-back completed without a response",
                )
            }
            _ => BrokerResponseParts::from_command(remoting_error_response(
                PublicErrorView::descriptor_only(&rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED),
                RemotingErrorTarget::Reply {
                    factory: &self.inner.context.command_factory,
                    opaque: original_opaque,
                },
            ))?
            .into_handler_outcome(),
        }
    }

    async fn process_send_message(
        &self,
        inbound_peer: SocketAddr,
        control: rocketmq_transport::api::RequestControlView,
        request_id: RequestId,
        original_oneway: bool,
        request_code: RequestCode,
        request: &mut RemotingCommand,
        parsed_request: Option<ParsedSendRequest>,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let ParsedSendRequest {
            header: mut request_header,
            properties: parsed_properties,
        } = match parsed_request {
            Some(parsed_request) => parsed_request,
            None => {
                let header = parse_request_header(request, request_code)?;
                let properties = string_to_message_properties(header.properties.as_ref());
                ParsedSendRequest { header, properties }
            }
        };
        let mapping_context = self
            .inner
            .context
            .topics
            .build_topic_queue_mapping_context(&request_header, true);
        if let Some(response) = TopicQueueMappingManager::rewrite_request_for_static_topic(
            &self.inner.context.command_factory,
            &mut request_header,
            &mapping_context,
        ) {
            return BrokerResponseParts::from_command(response)?.into_handler_outcome();
        }

        let (send_message_context, mut request_properties) =
            self.inner
                .build_msg_context_at(inbound_peer, &mut request_header, request, parsed_properties);
        self.inner.execute_send_message_hook_before(&send_message_context);
        clear_reserved_properties(&mut request_header, &mut request_properties);

        if request_header.is_batch() {
            self.send_batch_message(
                inbound_peer,
                control,
                request_id,
                original_oneway,
                request,
                send_message_context,
                request_header,
                request_properties,
                mapping_context,
            )
            .await
        } else {
            self.send_message(
                inbound_peer,
                control,
                request_id,
                original_oneway,
                request,
                send_message_context,
                request_header,
                request_properties,
                mapping_context,
            )
            .await
        }
    }

    fn receive_span_and_request(&self, request: &RemotingCommand) -> (tracing::Span, Option<ParsedSendRequest>) {
        let span = rocketmq_observability::trace::broker::receive_send_span(
            &self.inner.context.telemetry,
            request.code(),
            request.opaque(),
        );
        let request_code = RequestCode::from(request.code());
        let parsed_request = matches!(
            request_code,
            RequestCode::SendMessage | RequestCode::SendMessageV2 | RequestCode::SendBatchMessage
        )
        .then(|| {
            parse_request_header(request, request_code).ok().map(|header| {
                let properties = string_to_message_properties(header.properties.as_ref());
                ParsedSendRequest { header, properties }
            })
        })
        .flatten();
        #[cfg(feature = "otel-traces")]
        {
            if let Some(parsed_request) = parsed_request.as_ref() {
                if let Err(error) = rocketmq_observability::set_span_parent_from_properties_with_handle(
                    &self.inner.context.telemetry,
                    &span,
                    &parsed_request.properties,
                ) {
                    rocketmq_observability::record_span_parent_assignment_error(
                        &self.inner.context.telemetry,
                        "broker.receive_send",
                        error,
                    );
                }
                rocketmq_observability::trace::record_message_properties_with_handle(
                    &self.inner.context.telemetry,
                    &span,
                    &parsed_request.properties,
                    request.body().map(|body| body.len()),
                );
            }
        }
        (span, parsed_request)
    }
}

// Shared send-message operations.
impl<MS, TS> SendMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    pub fn has_send_message_hook(&self) -> bool {
        has_registered_send_message_hooks(&self.inner.send_message_hook_vec)
    }
}

impl<MS, TS> SendMessageProcessor<MS, TS>
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
            after_hooks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the leaf preserves the existing send request state while replacing only transport ownership"
    )]
    async fn send_message(
        &self,
        inbound_peer: SocketAddr,
        control: rocketmq_transport::api::RequestControlView,
        request_id: RequestId,
        original_oneway: bool,
        request: &mut RemotingCommand,
        mut send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        request_properties: HashMap<CheetahString, CheetahString>,
        mut mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let mut response = self.pre_send_at(inbound_peer, request, &request_header).await;
        if response.code() != -1 {
            return BrokerResponseParts::from_command(response)?.into_handler_outcome();
        }

        let mut topic_config = self
            .inner
            .context
            .topics
            .select_topic_config(request_header.topic())
            .ok_or_else(|| RocketMQError::TopicNotExist {
                topic: request_header.topic().to_string(),
            })?;
        let mut queue_id = request_header.queue_id;
        if queue_id < 0 {
            queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        let mut message_ext = MessageExtBrokerInner::default();
        message_ext
            .message_ext_inner
            .message
            .set_topic(request_header.topic().clone());
        message_ext.message_ext_inner.queue_id = queue_id;
        let mut properties = request_properties;
        if !self
            .handle_retry_and_dlq(
                &request_header,
                &mut response,
                request,
                &mut message_ext.message_ext_inner,
                &mut topic_config,
                &mut properties,
            )
            .await
        {
            return BrokerResponseParts::from_command(response)?.into_handler_outcome();
        }
        apply_topic_delivery_properties(&topic_config, request_header.topic(), &mut properties, &mut queue_id);
        message_ext.message_ext_inner.queue_id = queue_id;
        message_ext.message_ext_inner.message.set_body(request.body().cloned());
        message_ext.message_ext_inner.message.set_flag(request_header.flag);
        if should_create_uniq_key(&properties) {
            properties.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from_string(MessageClientIDSetter::create_uniq_id()),
            );
        }
        let tra_flag = properties
            .get(MessageConst::PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value.parse().unwrap_or(false));
        message_ext.message_ext_inner.message.set_properties(properties);
        if cleanup_policy_utils::get_delete_policy(Some(&topic_config)) == CleanupPolicy::COMPACTION
            && !has_valid_compaction_key(message_ext.message_ext_inner.message.properties().as_map())
        {
            return BrokerResponseParts::from_command(
                response
                    .set_code(ResponseCode::MessageIllegal)
                    .set_remark("Required message key is missing"),
            )?
            .into_handler_outcome();
        }
        message_ext.tags_code = MessageExtBrokerInner::tags_string2tags_code(
            &topic_config.topic_filter_type,
            message_ext.tags().unwrap_or_default().as_str(),
        );
        message_ext.message_ext_inner.born_timestamp = request_header.born_timestamp;
        message_ext.message_ext_inner.born_host = inbound_peer;
        message_ext.message_ext_inner.store_host = self.inner.context.policy.snapshot().store_host;
        message_ext.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);
        message_ext
            .message_ext_inner
            .message
            .properties_mut()
            .as_map_mut()
            .insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER),
                self.inner.context.policy.snapshot().broker_cluster_name.clone(),
            );
        message_ext.properties_string =
            MessageDecoder::message_properties_to_string(message_ext.message_ext_inner.message.properties().as_map());

        let transactional = if tra_flag
            && !(message_ext.reconsume_times() > 0 && message_ext.message_ext_inner.message.delay_time_level() > 0)
        {
            let policy = self.inner.context.policy.snapshot();
            if policy.reject_transaction_message {
                return BrokerResponseParts::from_command(response.set_code(ResponseCode::NoPermission).set_remark(
                    format!(
                        "the broker[{}] sending transaction message is forbidden",
                        policy.broker_ip
                    ),
                ))?
                .into_handler_outcome();
            }
            true
        } else {
            false
        };

        let start = Instant::now();
        let topic = message_ext.topic().clone();
        let topic_message_type = crate::metrics::broker_metrics_manager::get_message_type(&request_header);
        let transaction_id = MessageClientIDSetter::get_uniq_id(&message_ext.message_ext_inner.message);
        let recall_handle = self.build_recall_handle(&message_ext);
        let completion_facts = SendCompletionFacts::capture(request);
        if transactional {
            let mut store = TransactionalMessageAppender::new(self.inner.transactional_message_service.as_ref());
            let result = match await_store(StoreAwaitControl::Request(control), store.append_message(message_ext))
                .await
                .map_err(|StoreAwaitStopped| RocketMQError::invariant_violated("message store await stopped"))?
            {
                Ok(result) => result,
                Err(error) => {
                    let response = map_store_api_error(error).apply_to(response);
                    return BrokerResponseParts::from_command(response)?.into_handler_outcome();
                }
            };
            let (max_phy_offset, flushed_where) = self
                .inner
                .context
                .store
                .append_progress()
                .map_err(|_| message_store_not_initialized())?;
            let completion = self.handle_put_message_result(
                store_append_receipt(result, max_phy_offset, flushed_where),
                &mut response,
                completion_facts,
                topic.as_str(),
                transaction_id,
                recall_handle,
                &mut send_message_context,
                queue_id,
                start,
                &mut mapping_context,
                topic_message_type,
                MessageType::NormalMsg,
            );
            return self.finish_send_message_response(
                request_id,
                original_oneway,
                completion,
                response,
                send_message_context,
            );
        }

        let mut store = self.inner.context.store.clone();
        let processor = self;
        let reply = append_message_with_control_reply(control, &mut store, message_ext, move |result| match result {
            Ok(receipt) => {
                let completion = processor.handle_put_message_result(
                    receipt,
                    &mut response,
                    completion_facts,
                    topic.as_str(),
                    transaction_id,
                    recall_handle,
                    &mut send_message_context,
                    queue_id,
                    start,
                    &mut mapping_context,
                    topic_message_type,
                    MessageType::NormalMsg,
                );
                processor.prepare_send_store_reply(
                    request_id,
                    original_oneway,
                    completion,
                    response,
                    send_message_context,
                )
            }
            Err(error) => {
                let view = error
                    .public_view()
                    .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
                (
                    remoting_error_response(view, RemotingErrorTarget::Existing(response)),
                    StoreHookCompletion::NoAfterHook,
                )
            }
        })
        .await
        .map_err(|error| RocketMQError::internal("send-message-store", error))?;
        let (outcome, _) = reply.into_parts();
        Ok(outcome)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the leaf preserves the existing batch request state while replacing only transport ownership"
    )]
    async fn send_batch_message(
        &self,
        inbound_peer: SocketAddr,
        control: rocketmq_transport::api::RequestControlView,
        request_id: RequestId,
        original_oneway: bool,
        request: &mut RemotingCommand,
        mut send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        request_properties: HashMap<CheetahString, CheetahString>,
        mut mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let mut response = self.pre_send_at(inbound_peer, request, &request_header).await;
        if response.code() != -1 {
            return BrokerResponseParts::from_command(response)?.into_handler_outcome();
        }
        let topic_config = self
            .inner
            .context
            .topics
            .select_topic_config(request_header.topic())
            .ok_or_else(|| RocketMQError::TopicNotExist {
                topic: request_header.topic().to_string(),
            })?;
        let mut queue_id = request_header.queue_id;
        if queue_id < 0 {
            queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }
        if request_header.topic.len() > message_limits::MAX_TOPIC_LENGTH {
            return BrokerResponseParts::from_command(response.set_code(ResponseCode::MessageIllegal).set_remark(
                format!("message topic length too long {}", request_header.topic().len()),
            ))?
            .into_handler_outcome();
        }
        if !request_header.topic.is_empty() && request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return BrokerResponseParts::from_command(response.set_code(ResponseCode::MessageIllegal).set_remark(
                format!("batch request does not support retry group  {}", request_header.topic()),
            ))?
            .into_handler_outcome();
        }

        let mut message_ext = MessageExtBrokerInner::default();
        message_ext
            .message_ext_inner
            .message
            .set_topic(request_header.topic().clone());
        message_ext.message_ext_inner.queue_id = queue_id;
        let mut sys_flag = request_header.sys_flag;
        if TopicFilterType::MultiTag == topic_config.topic_filter_type {
            sys_flag |= MessageSysFlag::MULTI_TAGS_FLAG;
        }
        message_ext.message_ext_inner.sys_flag = sys_flag;
        message_ext.message_ext_inner.message.set_flag(request_header.flag);
        message_ext.message_ext_inner.message.set_properties(request_properties);
        message_ext.message_ext_inner.message.set_body(request.body().cloned());
        message_ext.message_ext_inner.born_timestamp = request_header.born_timestamp;
        message_ext.message_ext_inner.born_host = inbound_peer;
        message_ext.message_ext_inner.store_host = self.inner.context.policy.snapshot().store_host;
        message_ext.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);
        message_ext.message_ext_inner.message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER),
            self.inner.context.policy.snapshot().broker_cluster_name.clone(),
        );

        let mut batch_message = MessageExtBatch {
            message_ext_broker_inner: message_ext,
            is_inner_batch: false,
            encoded_buff: None,
        };
        let mut is_inner_batch = false;
        let mut response_header = SendMessageResponseHeader::default();
        let batch_uniq_id =
            MessageClientIDSetter::get_uniq_id(&batch_message.message_ext_broker_inner.message_ext_inner.message);
        if batch_uniq_id.is_some() && QueueTypeUtils::is_batch_cq_arc_mut(Some(&topic_config)) {
            let sys_flag = batch_message.message_ext_broker_inner.message_ext_inner.sys_flag;
            batch_message.message_ext_broker_inner.message_ext_inner.sys_flag =
                sys_flag | MessageSysFlag::NEED_UNWRAP_FLAG | MessageSysFlag::INNER_BATCH_FLAG;
            batch_message.is_inner_batch = true;
            let inner_num = MessageDecoder::count_inner_msg_num(
                batch_message
                    .message_ext_broker_inner
                    .message_ext_inner
                    .message
                    .get_body()
                    .cloned(),
            );
            batch_message
                .message_ext_broker_inner
                .message_ext_inner
                .message
                .put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_INNER_NUM),
                    CheetahString::from_string(inner_num.to_string()),
                );
            batch_message.message_ext_broker_inner.properties_string = message_properties_to_string(
                batch_message
                    .message_ext_broker_inner
                    .message_ext_inner
                    .message
                    .properties()
                    .as_map(),
            );
            response_header.set_batch_uniq_id(batch_uniq_id);
            is_inner_batch = true;
        }
        let start = Instant::now();
        let transaction_id =
            MessageClientIDSetter::get_uniq_id(&batch_message.message_ext_broker_inner.message_ext_inner.message);
        let topic = batch_message.message_ext_broker_inner.message_ext_inner.topic().clone();
        let topic_message_type = crate::metrics::broker_metrics_manager::get_message_type(&request_header);
        let completion_facts = SendCompletionFacts::capture(request);
        let processor = self;
        let reply = if is_inner_batch {
            let mut store = self.inner.context.store.clone();
            append_message_with_control_reply(
                control,
                &mut store,
                batch_message.message_ext_broker_inner,
                move |result| match result {
                    Ok(receipt) => {
                        let completion = processor.handle_put_message_result(
                            receipt,
                            &mut response,
                            completion_facts,
                            topic.as_str(),
                            transaction_id,
                            None,
                            &mut send_message_context,
                            queue_id,
                            start,
                            &mut mapping_context,
                            topic_message_type,
                            MessageType::NormalMsg,
                        );
                        processor.prepare_send_store_reply(
                            request_id,
                            original_oneway,
                            completion,
                            response,
                            send_message_context,
                        )
                    }
                    Err(error) => {
                        let view = error
                            .public_view()
                            .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
                        (
                            remoting_error_response(view, RemotingErrorTarget::Existing(response)),
                            StoreHookCompletion::NoAfterHook,
                        )
                    }
                },
            )
            .await
        } else {
            let mut store = self.inner.context.store.clone();
            append_message_with_control_reply(control, &mut store, batch_message, move |result| match result {
                Ok(receipt) => {
                    let completion = processor.handle_put_message_result(
                        receipt,
                        &mut response,
                        completion_facts,
                        topic.as_str(),
                        transaction_id,
                        None,
                        &mut send_message_context,
                        queue_id,
                        start,
                        &mut mapping_context,
                        topic_message_type,
                        MessageType::NormalMsg,
                    );
                    processor.prepare_send_store_reply(
                        request_id,
                        original_oneway,
                        completion,
                        response,
                        send_message_context,
                    )
                }
                Err(error) => {
                    let view = error
                        .public_view()
                        .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
                    (
                        remoting_error_response(view, RemotingErrorTarget::Existing(response)),
                        StoreHookCompletion::NoAfterHook,
                    )
                }
            })
            .await
        }
        .map_err(|error| RocketMQError::internal("send-batch-message-store", error))?;
        let (outcome, _) = reply.into_parts();
        Ok(outcome)
    }

    fn finish_send_message_response(
        &self,
        request_id: RequestId,
        original_oneway: bool,
        result: (Option<RemotingCommand>, bool),
        response: RemotingCommand,
        mut send_message_context: SendMessageContext,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let (mut response, after_canonical_write) = match result {
            (Some(response), _) => (response, false),
            (None, after_canonical_write) => (response, after_canonical_write),
        };
        if after_canonical_write && !original_oneway {
            self.inner
                .update_send_message_context_from_response(&response, &mut send_message_context);
            self.after_hooks.lock().insert(request_id, send_message_context);
        } else {
            self.inner
                .execute_send_message_hook_after(Some(&mut response), &mut send_message_context);
        }
        BrokerResponseParts::from_command(response)?.into_handler_outcome()
    }

    fn prepare_send_store_reply(
        &self,
        request_id: RequestId,
        original_oneway: bool,
        result: (Option<RemotingCommand>, bool),
        response: RemotingCommand,
        mut send_message_context: SendMessageContext,
    ) -> (RemotingCommand, StoreHookCompletion) {
        let (mut response, after_canonical_write) = match result {
            (Some(response), _) => (response, false),
            (None, after_canonical_write) => (response, after_canonical_write),
        };
        if after_canonical_write && !original_oneway {
            self.inner
                .update_send_message_context_from_response(&response, &mut send_message_context);
            self.after_hooks.lock().insert(request_id, send_message_context);
            (response, StoreHookCompletion::AfterCanonicalWrite)
        } else {
            self.inner
                .execute_send_message_hook_after(Some(&mut response), &mut send_message_context);
            (response, StoreHookCompletion::BeforeReply)
        }
    }
}

fn map_put_status_to_response(status: PutMessageStatus, response: &mut RemotingCommand) -> bool {
    match status {
        PutMessageStatus::PutOk => {
            response.set_code_ref(RemotingSysResponseCode::Success);
            true
        }
        PutMessageStatus::FlushDiskTimeout => {
            response.set_code_ref(ResponseCode::FlushDiskTimeout);
            true
        }
        PutMessageStatus::FlushSlaveTimeout => {
            response.set_code_ref(ResponseCode::FlushSlaveTimeout);
            true
        }
        PutMessageStatus::SlaveNotAvailable => {
            response.set_code_ref(ResponseCode::SlaveNotAvailable);
            true
        }
        PutMessageStatus::ServiceNotAvailable => {
            response
                .set_code_mut(ResponseCode::ServiceNotAvailable)
                .set_remark_mut(error_messages::SERVICE_NOT_AVAILABLE);
            false
        }
        PutMessageStatus::CreateMappedFileFailed => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut(error_messages::MAPPED_FILE_CREATE_FAILED);
            false
        }
        PutMessageStatus::MessageIllegal | PutMessageStatus::PropertiesSizeExceeded => {
            response
                .set_code_mut(ResponseCode::MessageIllegal)
                .set_remark_mut(error_messages::MESSAGE_ILLEGAL);
            false
        }
        PutMessageStatus::OsPageCacheBusy => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut(error_messages::OS_PAGE_CACHE_BUSY);
            false
        }
        PutMessageStatus::UnknownError => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut("UNKNOWN_ERROR");
            false
        }
        PutMessageStatus::InSyncReplicasNotEnough => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut(error_messages::IN_SYNC_REPLICAS_NOT_ENOUGH);
            false
        }
        PutMessageStatus::LmqConsumeQueueNumExceeded => {
            response
                .set_code_mut(ResponseCode::LmqQuotaExceeded)
                .set_remark_mut(error_messages::LMQ_QUEUE_NUM_EXCEEDED);
            false
        }
        PutMessageStatus::WheelTimerFlowControl => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut(error_messages::TIMER_FLOW_CONTROL);
            false
        }
        PutMessageStatus::WheelTimerMsgIllegal => {
            response
                .set_code_mut(ResponseCode::MessageIllegal)
                .set_remark_mut(error_messages::TIMER_MSG_ILLEGAL);
            false
        }
        PutMessageStatus::WheelTimerNotEnable => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut(error_messages::TIMER_NOT_ENABLED);
            false
        }
        PutMessageStatus::PutToRemoteBrokerFail => {
            response
                .set_code_mut(RemotingSysResponseCode::SystemError)
                .set_remark_mut("UNKNOWN_ERROR DEFAULT");
            false
        }
    }
}

impl<MS, TS> SendMessageProcessor<MS, TS>
where
    MS: BrokerWriteStore + BrokerMasterAddressStore,
    TS: TransactionalMessageService,
{
    /// Update broker statistics for successful message send
    #[inline]
    fn update_broker_stats_on_success(
        &self,
        topic: &str,
        queue_id: i32,
        append_receipt: &StoreAppendReceipt,
        begin_time_millis: Instant,
        topic_message_type: &TopicMessageType,
    ) {
        let append_result = append_receipt
            .result()
            .append_message_result()
            .expect("append result must exist for successful send");
        let stats = &self.inner.context.broker_stats_manager;
        if TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == topic {
            stats.inc_queue_put_nums(topic, queue_id, append_result.msg_num, 1);
            stats.inc_queue_put_size(topic, queue_id, append_result.wrote_bytes);
        }

        stats.inc_topic_put_nums(topic, append_result.msg_num, 1);
        stats.inc_topic_put_size(topic, append_result.wrote_bytes);
        stats.inc_broker_put_nums(topic, append_result.msg_num);
        let latency_millis = begin_time_millis.elapsed().as_millis();
        let latency_ms_for_stats = latency_millis.min(i32::MAX as u128) as i32;
        stats.inc_topic_put_latency(topic, queue_id, latency_ms_for_stats);

        if let Some(metrics) = self.inner.context.broker_metrics_manager.as_ref() {
            let msg_num = u64::try_from(append_result.msg_num.max(0)).unwrap_or_default();
            let bytes = u64::try_from(append_result.wrote_bytes.max(0)).unwrap_or_default();
            let message_size = bytes / msg_num.max(1);
            let is_system = TopicValidator::is_system_topic(topic);
            metrics.record_messages_in_success(topic, topic_message_type, msg_num, bytes, message_size, is_system);
            metrics.record_send_message_latency(topic, latency_millis.min(u64::MAX as u128) as u64);
        }
    }

    /// Update send message context for hooks
    fn update_send_context_on_success(
        &self,
        send_message_context: &mut SendMessageContext,
        response_header: &SendMessageResponseHeader,
        append_receipt: &StoreAppendReceipt,
        owner: Option<CheetahString>,
        auth_type: Option<CheetahString>,
        owner_parent: Option<CheetahString>,
        owner_self: Option<CheetahString>,
    ) {
        let policy = self.inner.context.policy.snapshot();
        let commercial_size_per_msg = policy.commercial_size_per_msg;
        let commercial_base_count = policy.commercial_base_count;

        send_message_context.msg_id = response_header.msg_id().clone();
        send_message_context.queue_id = Some(response_header.queue_id());
        send_message_context.queue_offset = Some(response_header.queue_offset());

        let append_result = append_receipt
            .result()
            .append_message_result()
            .expect("append result must exist for successful send");
        let commercial_msg_num = (append_result.wrote_bytes as f64 / commercial_size_per_msg as f64).ceil() as i32;
        let inc_value = commercial_msg_num * commercial_base_count;

        send_message_context.commercial_send_stats = StatsType::SendSuccess;
        send_message_context.commercial_send_times = inc_value;
        send_message_context.commercial_send_size = append_result.wrote_bytes;
        send_message_context.commercial_owner = owner.unwrap_or_default();

        send_message_context.send_stat = StatsType::SendSuccess;
        send_message_context.commercial_send_msg_num = commercial_msg_num;
        send_message_context.account_auth_type = auth_type.unwrap_or_default();
        send_message_context.account_owner_parent = owner_parent.unwrap_or_default();
        send_message_context.account_owner_self = owner_self.unwrap_or_default();
        send_message_context.send_msg_size = append_result.wrote_bytes;
        send_message_context.send_msg_num = append_result.msg_num;
    }

    /// Update send message context for failure case
    fn update_send_context_on_failure(
        &self,
        send_message_context: &mut SendMessageContext,
        append_receipt: &StoreAppendReceipt,
        request_body_len: i32,
        owner: Option<CheetahString>,
        auth_type: Option<CheetahString>,
        owner_parent: Option<CheetahString>,
        owner_self: Option<CheetahString>,
    ) {
        let commercial_size_per_msg = self.inner.context.policy.snapshot().commercial_size_per_msg;

        let msg_num = append_receipt
            .result()
            .append_message_result()
            .map_or(1, |result| result.msg_num)
            .max(1);
        let commercial_msg_num = (request_body_len as f64 / commercial_size_per_msg as f64).ceil() as i32;

        send_message_context.commercial_send_stats = StatsType::SendFailure;
        send_message_context.commercial_send_times = commercial_msg_num;
        send_message_context.commercial_send_size = request_body_len;
        send_message_context.commercial_owner = owner.unwrap_or_default();

        send_message_context.send_stat = StatsType::SendFailure;
        send_message_context.commercial_send_msg_num = commercial_msg_num;
        send_message_context.account_auth_type = auth_type.unwrap_or_default();
        send_message_context.account_owner_parent = owner_parent.unwrap_or_default();
        send_message_context.account_owner_self = owner_self.unwrap_or_default();
        send_message_context.send_msg_size = request_body_len;
        send_message_context.send_msg_num = msg_num;
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "existing send result protocol context is tracked by the lint debt registry"
    )]
    fn handle_put_message_result(
        &self,
        append_receipt: StoreAppendReceipt,
        response: &mut RemotingCommand,
        completion_facts: SendCompletionFacts,
        topic: &str,
        transaction_id: Option<CheetahString>,
        recall_handle: Option<CheetahString>,
        send_message_context: &mut SendMessageContext,
        queue_id_int: i32,
        begin_time_millis: Instant,
        mapping_context: &mut TopicQueueMappingContext,
        topic_message_type: TopicMessageType,
        _message_type: MessageType,
    ) -> (Option<RemotingCommand>, bool) {
        let send_ok = map_put_status_to_response(append_receipt.result().put_message_status(), response);

        let has_send_message_hook = self.has_send_message_hook();

        if send_ok {
            self.update_broker_stats_on_success(
                topic,
                queue_id_int,
                &append_receipt,
                begin_time_millis,
                &topic_message_type,
            );

            {
                let response_header = response
                    .read_custom_header_mut::<SendMessageResponseHeader>()
                    .expect("SendMessageResponseHeader must exist");

                set_success_response_header(
                    response_header,
                    &append_receipt,
                    queue_id_int,
                    transaction_id.clone(),
                    recall_handle.clone(),
                );

                let rewrite_result = rewrite_response_for_static_topic(
                    &self.inner.context.command_factory,
                    response_header,
                    mapping_context,
                );
                if rewrite_result.is_some() {
                    return (rewrite_result, false);
                }

                if has_send_message_hook {
                    self.update_send_context_on_success(
                        send_message_context,
                        response_header,
                        &append_receipt,
                        completion_facts.owner,
                        completion_facts.auth_type,
                        completion_facts.owner_parent,
                        completion_facts.owner_self,
                    );
                }
            }

            response.set_opaque_mut(completion_facts.opaque);
            (None, true)
        } else {
            if has_send_message_hook {
                self.update_send_context_on_failure(
                    send_message_context,
                    &append_receipt,
                    completion_facts.body_len,
                    completion_facts.owner,
                    completion_facts.auth_type,
                    completion_facts.owner_parent,
                    completion_facts.owner_self,
                );
            }
            (None, false)
        }
    }

    fn build_recall_handle(&self, message: &MessageExtBrokerInner) -> Option<CheetahString> {
        let policy = self.inner.context.policy.snapshot();
        let max_delay_sec = if policy.timer_store_mode == rocketmq_store_api::TimerStoreMode::ExtendedTimeline {
            u64::from(policy.timer_maximum_horizon_days).saturating_mul(86_400)
        } else {
            policy.timer_max_delay_sec
        };
        let (real_topic, timestamp) =
            recall_handle_topic_and_timestamp(message, max_delay_sec, policy.timer_precision_ms)?;
        if real_topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return None;
        }

        let uniq_id = MessageClientIDSetter::get_uniq_id(&message.message_ext_inner.message)?;
        let broker_name = policy.broker_name.clone();

        Some(CheetahString::from_string(HandleV1::build_handle(
            real_topic,
            broker_name,
            timestamp.to_string(),
            uniq_id,
        )))
    }

    async fn pre_send_at(
        &self,
        inbound_peer: SocketAddr,
        request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = self
            .inner
            .context
            .command_factory
            .create_success_response_command_with_header(SendMessageResponseHeader::default());
        let policy = self.inner.context.policy.snapshot();
        // set opaque
        response.with_opaque(request.opaque());
        add_send_response_metadata(&mut response, policy.region_id.clone(), policy.trace_on);
        let start_timestamp = policy.start_accept_send_request_time_stamp;
        let store_now = self.inner.context.store.now().unwrap_or_default();
        if store_now < (start_timestamp as u64) {
            response = response
                .set_code(RemotingSysResponseCode::SystemError)
                .set_remark(format!(
                    "broker unable to service, until {}",
                    util_all::time_millis_to_human_string2(start_timestamp)
                ));
            return response;
        }
        response = response.set_code(-1);
        self.inner
            .msg_check_at(inbound_peer, request, request_header, &mut response)
            .await;
        response
    }

    async fn handle_retry_and_dlq(
        &self,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
        request: &RemotingCommand,
        msg: &mut MessageExt,
        topic_config: &mut Arc<rocketmq_model::common::config::TopicConfig>,
        properties: &mut HashMap<CheetahString, CheetahString>,
    ) -> bool {
        let mut new_topic = request_header.topic();
        if !new_topic.is_empty() && new_topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            let group_name = CheetahString::from_string(KeyBuilder::parse_group(new_topic.as_str()));
            let subscription_group_config = self
                .inner
                .context
                .subscription_groups
                .find_subscription_group_config(group_name.as_ref());
            if subscription_group_config.is_none() {
                response
                    .with_code(ResponseCode::SubscriptionNotExist)
                    .with_remark(format!(
                        "subscription group not exist, {}  {}",
                        group_name.as_str(),
                        FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                    ));
                return false;
            }
            // SAFETY: subscription_group_config existence checked above
            let subscription_group_config = subscription_group_config.unwrap();

            let mut max_reconsume_times = subscription_group_config.retry_max_times();
            if request.rocketmq_version() >= RocketMqVersion::V3_4_9 {
                if let Some(times) = request_header.max_reconsume_times {
                    max_reconsume_times = times;
                }
            }
            let reconsume_times = request_header.reconsume_times.unwrap_or(0);
            let mut send_retry_message_to_dead_letter_queue_directly = false;
            if self
                .inner
                .context
                .rebalance_locks
                .is_lock_all_expired(group_name.as_str())
            {
                info!(
                    "Group has unexpired lock record, which show it is ordered message, send it to DLQ right now \
                     group={}, topic={}, reconsumeTimes={}, maxReconsumeTimes={}.",
                    group_name, new_topic, reconsume_times, max_reconsume_times
                );
                send_retry_message_to_dead_letter_queue_directly = true;
            }
            if reconsume_times > max_reconsume_times || send_retry_message_to_dead_letter_queue_directly {
                properties.insert(
                    CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL),
                    CheetahString::from_static_str("-1"),
                );
                let topic_ = CheetahString::from_string(mix_all::get_dlq_topic(group_name.as_str()));
                new_topic = &topic_;
                let queue_id_int = self.inner.random_queue_id(retry_config::DLQ_NUMS_PER_GROUP) as i32;
                let new_topic_config = self
                    .inner
                    .context
                    .topics
                    .create_topic_in_send_message_back(
                        new_topic,
                        retry_config::DLQ_NUMS_PER_GROUP as i32,
                        PermName::PERM_WRITE | PermName::PERM_READ,
                        false,
                        0,
                    )
                    .await;
                msg.message.set_topic(new_topic.clone());
                msg.queue_id = queue_id_int;
                msg.message.set_delay_time_level(0);
                if new_topic_config.is_none() {
                    response
                        .with_code(ResponseCode::SystemError)
                        .with_remark(format!("topic {new_topic} not exist, apply DLQ failed"));
                    return false;
                }
                // SAFETY: new_topic_config existence checked above
                *topic_config = new_topic_config.unwrap();
            }
        }

        let mut sys_flag = request_header.sys_flag;
        if TopicFilterType::MultiTag == topic_config.topic_filter_type {
            sys_flag |= MessageSysFlag::MULTI_TAGS_FLAG;
        }
        msg.sys_flag = sys_flag;
        true
    }
}

#[inline]
fn has_registered_send_message_hooks(hooks: &[Box<dyn SendMessageHook>]) -> bool {
    !hooks.is_empty()
}

trait SendBackpressurePolicy {
    fn sync_flush_backlog_reject_depth(&self) -> u64;
    fn sync_flush_backlog_reject_wait_millis(&self) -> u64;
    fn ha_pending_reject_count(&self) -> u64;
    fn ha_pending_reject_wait_millis(&self) -> u64;
    fn reput_lag_reject_bytes(&self) -> i64;
}

impl SendBackpressurePolicy for BrokerConfig {
    fn sync_flush_backlog_reject_depth(&self) -> u64 {
        self.sync_flush_backlog_reject_depth
    }

    fn sync_flush_backlog_reject_wait_millis(&self) -> u64 {
        self.sync_flush_backlog_reject_wait_millis
    }

    fn ha_pending_reject_count(&self) -> u64 {
        self.ha_pending_reject_count
    }

    fn ha_pending_reject_wait_millis(&self) -> u64 {
        self.ha_pending_reject_wait_millis
    }

    fn reput_lag_reject_bytes(&self) -> i64 {
        self.reput_lag_reject_bytes
    }
}

impl SendBackpressurePolicy for SendMessagePolicy {
    fn sync_flush_backlog_reject_depth(&self) -> u64 {
        self.sync_flush_backlog_reject_depth
    }

    fn sync_flush_backlog_reject_wait_millis(&self) -> u64 {
        self.sync_flush_backlog_reject_wait_millis
    }

    fn ha_pending_reject_count(&self) -> u64 {
        self.ha_pending_reject_count
    }

    fn ha_pending_reject_wait_millis(&self) -> u64 {
        self.ha_pending_reject_wait_millis
    }

    fn reput_lag_reject_bytes(&self) -> i64 {
        self.reput_lag_reject_bytes
    }
}

fn sync_flush_backlog_reject_remark(
    policy: &impl SendBackpressurePolicy,
    runtime_info: SyncFlushRuntimeInfo,
) -> Option<String> {
    let reject_depth = policy.sync_flush_backlog_reject_depth();
    let reject_wait_millis = policy.sync_flush_backlog_reject_wait_millis();
    let depth_exceeded = reject_depth > 0 && runtime_info.queue_depth >= reject_depth;
    let wait_exceeded = reject_wait_millis > 0 && runtime_info.oldest_wait_millis >= reject_wait_millis;

    (depth_exceeded || wait_exceeded).then(|| {
        format!(
            "The broker sync flush backlog is busy, queueDepth={}, oldestWaitMillis={}, rejectDepth={}, \
             rejectWaitMillis={}",
            runtime_info.queue_depth, runtime_info.oldest_wait_millis, reject_depth, reject_wait_millis
        )
    })
}

struct TransactionalMessageAppender<'a, TS> {
    service: &'a TS,
}

impl<'a, TS> TransactionalMessageAppender<'a, TS> {
    fn new(service: &'a TS) -> Self {
        Self { service }
    }
}

impl<TS> MessageAppender<MessageExtBrokerInner> for TransactionalMessageAppender<'_, TS>
where
    TS: TransactionalMessageService,
{
    type Receipt = PutMessageResult;

    async fn append_message(
        &mut self,
        message: MessageExtBrokerInner,
    ) -> Result<Self::Receipt, rocketmq_store_api::StoreError> {
        Ok(self.service.prepare_message(message).await)
    }
}

/// Applies the production Send store receipt to the wire response header.
/// Both command completion and the route-neutral structured leaf use this
/// mapping, so transaction and recall fields cannot drift between paths.
#[inline]
fn set_success_response_header(
    response_header: &mut SendMessageResponseHeader,
    append_receipt: &StoreAppendReceipt,
    queue_id: i32,
    transaction_id: Option<CheetahString>,
    recall_handle: Option<CheetahString>,
) {
    let append_result = append_receipt
        .result()
        .append_message_result()
        .expect("append result must exist for successful send");
    response_header.set_msg_id(
        append_result
            .get_message_id()
            .expect("message_id must exist for successful send"),
    );
    response_header.set_queue_id(queue_id);
    response_header.set_queue_offset(append_result.logics_offset);
    response_header.set_transaction_id(transaction_id);
    response_header.set_recall_handle(recall_handle);
}

fn append_message_with_store<'a, S, M>(
    store: &'a mut S,
    message: M,
) -> impl Future<Output = Result<Result<S::Receipt, rocketmq_store_api::StoreError>, StoreAwaitStopped>> + Send + 'a
where
    S: MessageAppender<M> + 'a,
    M: Send + 'a,
{
    await_store(StoreAwaitControl::Legacy, store.append_message(message))
}

fn map_legacy_store_wait_stopped(_: StoreAwaitStopped) -> RocketMQError {
    RocketMQError::invariant_violated("legacy message store await cannot observe request cancellation")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreApiRetryDisposition {
    SwitchBroker,
    Immediate,
    AfterBackoff,
    Never,
}

impl StoreApiRetryDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SwitchBroker => "switch_broker",
            Self::Immediate => "immediate",
            Self::AfterBackoff => "after_backoff",
            Self::Never => "never",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StoreApiErrorProjection {
    response_code: i32,
    public_message: &'static str,
    retry: StoreApiRetryDisposition,
}

impl StoreApiErrorProjection {
    fn apply_to(self, response: RemotingCommand) -> RemotingCommand {
        debug!(
            response_code = self.response_code,
            retry = self.retry.as_str(),
            "mapped canonical storage failure to legacy send response"
        );
        response.set_code(self.response_code).set_remark(self.public_message)
    }
}

fn map_store_api_error(error: rocketmq_store_api::StoreError) -> StoreApiErrorProjection {
    use rocketmq_error::STORAGE_BACKEND_UNAVAILABLE;
    use rocketmq_error::STORAGE_MAPPED_FILE_NOT_FOUND;
    use rocketmq_error::STORAGE_OPERATION_TIMED_OUT;
    use rocketmq_error::STORAGE_OPERATION_UNSUPPORTED;
    use rocketmq_store_api::StoreOperation;

    let descriptor = error.descriptor();
    let retry = if (descriptor == &STORAGE_BACKEND_UNAVAILABLE && error.operation() == StoreOperation::Append)
        || (descriptor == &STORAGE_MAPPED_FILE_NOT_FOUND && error.operation() == StoreOperation::Append)
        || descriptor == &STORAGE_OPERATION_UNSUPPORTED
    {
        StoreApiRetryDisposition::SwitchBroker
    } else if descriptor == &STORAGE_MAPPED_FILE_NOT_FOUND {
        StoreApiRetryDisposition::Immediate
    } else if descriptor == &STORAGE_OPERATION_TIMED_OUT {
        StoreApiRetryDisposition::AfterBackoff
    } else {
        StoreApiRetryDisposition::Never
    };

    // This is the single intentional source-termination boundary. The broker
    // carries only the descriptor-owned remoting code and fixed public message.
    // Retry disposition preserves the legacy send decision independently from
    // catalog RecoveryHint metadata. Private detail and source are dropped here.
    StoreApiErrorProjection {
        response_code: descriptor.projection().remoting().code.as_i32(),
        public_message: descriptor.public_message(),
        retry,
    }
}

fn store_health_reject_remark_from<P, S>(policy: &P, store: &S) -> Option<String>
where
    P: SendBackpressurePolicy,
    S: StoreHealth<Snapshot = StoreHealthSnapshot>,
{
    store_health_reject_remark(policy, store.health_snapshot())
}

fn store_health_reject_remark(policy: &impl SendBackpressurePolicy, snapshot: StoreHealthSnapshot) -> Option<String> {
    if snapshot.shutdown {
        return Some("store_backpressure reason=store_shutdown".to_string());
    }
    if !snapshot.writable {
        let error_code = snapshot
            .last_error
            .map_or("unknown", |descriptor| descriptor.code().as_str());
        return Some(format!(
            "store_backpressure reason=store_not_writeable, lastFlushErrorCode={error_code}"
        ));
    }
    if snapshot.page_cache_busy {
        return Some("store_backpressure reason=page_cache_busy".to_string());
    }
    if snapshot.transient_pool_deficient {
        return Some("store_backpressure reason=transient_store_pool_deficient".to_string());
    }
    let flush_backlog = SyncFlushRuntimeInfo {
        queue_depth: snapshot.flush_backlog.queue_depth,
        oldest_wait_millis: snapshot.flush_backlog.oldest_wait_millis,
        ..SyncFlushRuntimeInfo::default()
    };
    if let Some(remark) = sync_flush_backlog_reject_remark(policy, flush_backlog) {
        return Some(format!("store_backpressure reason=sync_flush_backlog, {remark}"));
    }

    let ha_count_threshold = policy.ha_pending_reject_count();
    let ha_wait_threshold = policy.ha_pending_reject_wait_millis();
    let ha_count_exceeded = ha_count_threshold > 0 && snapshot.replication_pending_count >= ha_count_threshold;
    let ha_wait_exceeded = ha_wait_threshold > 0 && snapshot.replication_oldest_wait_millis >= ha_wait_threshold;
    if ha_count_exceeded || ha_wait_exceeded {
        return Some(format!(
            "store_backpressure reason=ha_pending, pendingCount={}, oldestWaitMillis={}, rejectCount={}, \
             rejectWaitMillis={}",
            snapshot.replication_pending_count,
            snapshot.replication_oldest_wait_millis,
            ha_count_threshold,
            ha_wait_threshold
        ));
    }

    let reput_lag_threshold = policy.reput_lag_reject_bytes();
    if reput_lag_threshold > 0 && snapshot.dispatch_behind_bytes >= reput_lag_threshold {
        return Some(format!(
            "store_backpressure reason=reput_lag, dispatchBehindBytes={}, rejectBytes={}",
            snapshot.dispatch_behind_bytes, reput_lag_threshold
        ));
    }

    None
}

pub(crate) struct Inner<MS, TS>
where
    MS: BrokerWriteStore,
{
    pub(crate) send_message_hook_vec: Arc<Vec<Box<dyn SendMessageHook>>>,
    pub(crate) consume_message_hook_vec: Arc<Vec<Box<dyn ConsumeMessageHook>>>,
    pub(crate) transactional_message_service: Arc<TS>,
    pub(crate) context: Arc<SendMessageProcessorContext<MS>>,
}

impl<MS, TS> Inner<MS, TS>
where
    MS: BrokerWriteStore,
    TS: TransactionalMessageService,
{
    #[inline]
    pub fn has_send_message_hook(&self) -> bool {
        has_registered_send_message_hooks(&self.send_message_hook_vec)
    }

    #[inline]
    pub fn has_consume_message_hook(&self) -> bool {
        !self.consume_message_hook_vec.is_empty()
    }

    pub(crate) fn execute_send_message_hook_before(&self, context: &SendMessageContext) {
        for hook in self.send_message_hook_vec.iter() {
            hook.send_message_before(context);
        }
    }

    pub(crate) fn execute_consume_message_hook_after<'a>(&self, context: &mut ConsumeMessageContext<'a>) {
        for hook in self.consume_message_hook_vec.iter() {
            hook.consume_message_after(context);
        }
    }

    pub(crate) fn execute_send_message_hook_after(
        &self,
        response: Option<&mut RemotingCommand>,
        context: &mut SendMessageContext,
    ) {
        if let Some(response) = response {
            self.update_send_message_context_from_response(response, context);
        }

        for hook in self.send_message_hook_vec.iter() {
            hook.send_message_after(context);
        }
    }

    fn update_send_message_context_from_response(&self, response: &RemotingCommand, context: &mut SendMessageContext) {
        if let Ok(header) = response.decode_command_custom_header::<SendMessageResponseHeader>() {
            context.msg_id = header.msg_id().clone();
            context.queue_id = Some(header.queue_id());
            context.queue_offset = Some(header.queue_offset());
            context.code = response.code();
            context.error_msg = response.remark().cloned().unwrap_or_default();
        }
    }

    pub(crate) async fn consumer_send_msg_back(
        &self,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>
    where
        MS: BrokerMasterAddressStore,
    {
        let request_header = request.decode_command_custom_header::<ConsumerSendMsgBackRequestHeader>()?;
        let policy = self.context.policy.snapshot();
        if policy.broker_id != mix_all::MASTER_ID {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("no master available along with {}", policy.broker_ip),
                ),
            ));
        }
        let subscription_group_config = self
            .context
            .subscription_groups
            .find_subscription_group_config(&request_header.group);
        if subscription_group_config.is_none() {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SubscriptionNotExist,
                    format!(
                        "subscription group not exist, {} {}",
                        request_header.group,
                        FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                    ),
                ),
            ));
        }

        if !PermName::is_writeable(policy.broker_permission.get()) {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::NoPermission,
                    format!(
                        "the broker[{}-{}] sending message is forbidden",
                        policy.broker_name, policy.broker_ip
                    ),
                ),
            ));
        }

        // SAFETY: subscription_group_config existence checked above
        let subscription_group_config = subscription_group_config.unwrap();

        // Early return: no retry queues configured
        if subscription_group_config.retry_queue_nums() <= 0 {
            return Ok(Some(no_retry_queue_response(&self.context.command_factory)));
        }
        let mut new_topic = CheetahString::from_string(mix_all::get_retry_topic(request_header.group.as_str()));
        let mut queue_id_int = rand::rng().random_range(0..subscription_group_config.retry_queue_nums());
        let topic_sys_flag = if request_header.unit_mode {
            TopicSysFlag::build_sys_flag(false, true)
        } else {
            0
        };
        let topic_config = self
            .context
            .topics
            .create_topic_in_send_message_back(
                &new_topic,
                subscription_group_config.retry_queue_nums(),
                PermName::PERM_WRITE | PermName::PERM_READ,
                false,
                topic_sys_flag,
            )
            .await;
        if topic_config.is_none() {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("topic {new_topic} not exist"),
                ),
            ));
        }
        // SAFETY: topic_config existence checked above
        let topic_config = topic_config.unwrap();

        // Early return: topic not writable
        if !PermName::is_writeable(topic_config.perm) {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::NoPermission,
                    format!("the topic[{new_topic}] sending message is forbidden"),
                ),
            ));
        }
        // Early return: message not found
        let msg_ext: Option<MessageExt> = self
            .context
            .store
            .look_message_by_offset(request_header.offset)
            .map_err(|_| message_store_not_initialized())?;
        let Some(mut msg_ext) = msg_ext else {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("look message by offset failed, the offset is {}", request_header.offset),
                ),
            ));
        };
        #[cfg(feature = "otel-traces")]
        {
            rocketmq_observability::trace::record_current_message_properties_with_handle(
                &self.context.telemetry,
                msg_ext.get_properties(),
                msg_ext.get_body().map(|body| body.len()),
            );
        }

        let retry_topic = msg_ext.property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC));
        if retry_topic.is_none() {
            let topic = msg_ext.topic().clone();
            MessageAccessor::put_property(
                &mut msg_ext,
                CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
                topic,
            );
        }
        msg_ext.set_wait_store_msg_ok(false);
        let mut delay_level = request_header.delay_level;
        let mut max_reconsume_times = subscription_group_config.retry_max_times();
        if request.rocketmq_version() >= RocketMqVersion::V3_4_9 {
            if let Some(num) = request_header.max_reconsume_times {
                max_reconsume_times = num;
            }
        }

        //judge DLQ
        let is_dlq = if msg_ext.reconsume_times >= max_reconsume_times || delay_level < 0 {
            new_topic = CheetahString::from_string(mix_all::get_dlq_topic(&request_header.group));
            queue_id_int = 0;
            let topic_config_inner = self
                .context
                .topics
                .create_topic_in_send_message_back(
                    &new_topic,
                    retry_config::DLQ_NUMS_PER_GROUP as i32,
                    PermName::PERM_WRITE | PermName::PERM_READ,
                    false,
                    0,
                )
                .await;
            if topic_config_inner.is_none() {
                return Ok(Some(
                    self.context.command_factory.create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        format!("topic {new_topic} not exist"),
                    ),
                ));
            }
            msg_ext.set_delay_time_level(0);
            true
        } else {
            if 0 == delay_level {
                delay_level = retry_config::DEFAULT_RETRY_DELAY_LEVEL + msg_ext.reconsume_times();
            }
            msg_ext.set_delay_time_level(delay_level);
            false
        };
        let mut msg_inner = MessageExtBrokerInner::default();
        msg_inner.set_topic(new_topic);
        if let Some(body) = msg_ext.get_body() {
            msg_inner.set_body(body.clone());
        }
        msg_inner.set_flag(msg_ext.get_flag());
        MessageAccessor::set_properties(&mut msg_inner, msg_ext.get_properties().clone());
        msg_inner.properties_string = message_properties_to_string(msg_ext.get_properties());
        msg_inner.tags_code =
            MessageExtBrokerInner::tags_string_to_tags_code(msg_ext.tags().unwrap_or_default().as_str());
        msg_inner.message_ext_inner.queue_id = queue_id_int;
        msg_inner.message_ext_inner.sys_flag = msg_ext.sys_flag;
        msg_inner.message_ext_inner.born_timestamp = msg_ext.born_timestamp;
        msg_inner.message_ext_inner.born_host = msg_ext.born_host;
        msg_inner.message_ext_inner.store_host = policy.store_host;
        msg_inner.message_ext_inner.reconsume_times = msg_ext.reconsume_times + 1;

        let origin_msg_id = if let Some(id) = MessageAccessor::get_origin_message_id(&msg_ext) {
            id
        } else {
            msg_ext.msg_id.clone()
        };
        MessageAccessor::set_origin_message_id(&mut msg_inner, origin_msg_id);
        msg_inner.properties_string = message_properties_to_string(msg_ext.get_properties());

        let inner_topic = msg_inner.get_topic().clone();
        let put_message_result = await_store(StoreAwaitControl::Legacy, self.context.store.put_message(msg_inner))
            .await
            .map_err(map_legacy_store_wait_stopped)?
            .map_err(|_| message_store_not_initialized())?;
        let commercial_owner = request
            .get_ext_fields()
            .and_then(|value| value.get(BrokerStatsManager::COMMERCIAL_OWNER).cloned());
        let (response, succeeded) = match put_message_result.put_message_status() {
            PutMessageStatus::PutOk => {
                if is_dlq {
                    if let Some(metrics) = self.context.broker_metrics_manager.as_ref() {
                        metrics.inc_send_to_dlq_messages(inner_topic.as_str(), request_header.group.as_str(), 1);
                    }
                }
                (self.context.command_factory.create_success_response_command(), true)
            }

            _ => (
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    put_message_result.put_message_status().to_string(),
                ),
                false,
            ),
        };
        #[cfg(feature = "otel-traces")]
        {
            let event = if is_dlq {
                "RocketMQ CONSUMER DLQ"
            } else {
                "RocketMQ CONSUMER RETRY"
            };
            let status = if succeeded { "success" } else { "failure" };
            rocketmq_observability::add_current_span_event_with_status(&self.context.telemetry, event, status);
        }

        if self.has_consume_message_hook() && request_header.origin_msg_id.is_some_and(|ref id| !id.is_empty()) {
            let namespace = CheetahString::from_string(NamespaceUtil::get_namespace_from_resource(
                request_header.group.as_str(),
            ));
            let origin_topic = request_header.origin_topic.as_ref().unwrap_or(&request_header.group);

            let account_auth_type = request
                .get_ext_fields()
                .and_then(|value| value.get(BrokerStatsManager::ACCOUNT_AUTH_TYPE));
            let account_owner_parent = request
                .get_ext_fields()
                .and_then(|value| value.get(BrokerStatsManager::ACCOUNT_OWNER_PARENT));
            let account_owner_self = request
                .get_ext_fields()
                .and_then(|value| value.get(BrokerStatsManager::ACCOUNT_OWNER_SELF));

            let mut context = ConsumeMessageContext {
                namespace: &namespace,
                topic: origin_topic,
                consumer_group: &request_header.group,
                queue_id: None,
                client_host: None,
                store_host: None,
                message_ids: None,
                body_length: 0,
                success: succeeded,
                status: None,
                topic_config: None,
                account_auth_type,
                account_owner_parent,
                account_owner_self,
                rcv_msg_num: 1,
                rcv_msg_size: 0,
                rcv_stat: if is_dlq {
                    StatsType::SendBackToDlq
                } else {
                    StatsType::SendBack
                },
                commercial_rcv_msg_num: if succeeded { 1 } else { 0 },
                commercial_owner: commercial_owner.as_ref(),
                commercial_rcv_stats: StatsType::SendBack,
                commercial_rcv_times: 1,
                commercial_rcv_size: 0,
            };
            self.execute_consume_message_hook_after(&mut context);
        }

        Ok(Some(response))
    }

    pub(crate) fn build_msg_context_at(
        &self,
        inbound_peer: SocketAddr,
        request_header: &mut SendMessageRequestHeader,
        request: &RemotingCommand,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> (SendMessageContext, HashMap<CheetahString, CheetahString>) {
        let namespace = NamespaceUtil::get_namespace_from_resource(request_header.topic.as_str());
        let policy = self.context.policy.snapshot();
        let region_id = policy.region_id.to_string();

        let mut send_message_context = SendMessageContext {
            namespace: CheetahString::from_string(namespace),
            producer_group: request_header.producer_group.clone(),
            ..Default::default()
        };
        send_message_context.topic(request_header.topic.clone());
        send_message_context.body_length(request.body().as_ref().map_or_else(|| 0, |b| b.len() as i32));
        send_message_context.msg_props(request_header.properties.clone().unwrap_or_default());
        send_message_context.born_host(CheetahString::from_string(inbound_peer.to_string()));
        send_message_context.broker_addr(policy.broker_addr.clone());
        send_message_context.queue_id(Some(request_header.queue_id));
        send_message_context.broker_region_id(CheetahString::from_string(region_id.clone()));
        send_message_context.born_time_stamp(request_header.born_timestamp);
        send_message_context.request_time_stamp(time_utils::current_millis() as i64);

        if let Some(owner) = request.ext_fields() {
            if let Some(value) = owner.get(BrokerStatsManager::COMMERCIAL_OWNER) {
                send_message_context.commercial_owner(value.clone());
            }
        }
        let properties = enrich_parsed_send_message_request_properties(
            request_header,
            properties,
            region_id.as_str(),
            policy.trace_on,
        );

        if let Some(unique_key) = properties.get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) {
            send_message_context.msg_unique_key = CheetahString::from_slice(unique_key);
        } else {
            send_message_context.msg_unique_key = CheetahString::empty();
        }

        if properties.contains_key(MessageConst::PROPERTY_SHARDING_KEY) {
            send_message_context.msg_type = MessageType::OrderMsg;
        } else {
            send_message_context.msg_type = MessageType::NormalMsg;
        }
        (send_message_context, properties)
    }

    pub(crate) async fn msg_check_at(
        &self,
        inbound_peer: SocketAddr,
        request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
    ) where
        MS: BrokerMasterAddressStore,
    {
        //check broker permission
        let policy = self.context.policy.snapshot();
        if broker_send_permission_denied(policy.broker_permission.get()) {
            response.with_code(ResponseCode::NoPermission);
            response.with_remark(format!(
                "the broker[{}] sending message is forbidden",
                policy.broker_ip.clone()
            ));
            return;
        }

        //check Topic
        let result = TopicValidator::validate_topic(request_header.topic.as_str());
        if !result.valid() {
            response.with_code(SystemError);
            response.with_remark(result.take_remark());
            return;
        }

        if TopicValidator::is_not_allowed_send_topic(request_header.topic.as_str()) {
            response.with_code(ResponseCode::NoPermission);
            response.with_remark(format!(
                "Sending message to topic[{}] is forbidden.",
                request_header.topic.as_str()
            ));
            return;
        }

        if let Some(remark) = message_body_limit_violation(request, policy.max_message_size) {
            response.with_code(ResponseCode::MessageIllegal);
            response.with_remark(remark);
            return;
        }
        let mut topic_config = self.context.topics.select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            let mut topic_sys_flag = 0;
            if request_header.unit_mode.unwrap_or(false) {
                if request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
                    topic_sys_flag = build_sys_flag(false, true);
                } else {
                    topic_sys_flag = build_sys_flag(true, false);
                }
            }
            warn!(
                "the topic {} not exist, producer: {}",
                request_header.topic(),
                inbound_peer,
            );
            topic_config = self
                .context
                .topics
                .create_topic_in_send_message(
                    &request_header.topic,
                    &request_header.default_topic,
                    inbound_peer,
                    request_header.default_topic_queue_nums,
                    topic_sys_flag,
                )
                .await;

            if topic_config.is_none() && request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
                topic_config = self
                    .context
                    .topics
                    .create_topic_in_send_message_back(
                        request_header.topic.as_ref(),
                        1,
                        PermName::PERM_WRITE | PermName::PERM_READ,
                        false,
                        topic_sys_flag,
                    )
                    .await;
            }

            if topic_config.is_none() {
                response.with_code(ResponseCode::TopicNotExist);
                response.with_remark(format!(
                    "topic[{}] not exist, apply first please!",
                    request_header.topic.as_str()
                ));
                return;
            }
        }

        let queue_id_int = request_header.queue_id;
        // SAFETY: topic_config existence checked above
        let topic_config_inner = topic_config.as_ref().unwrap();
        let id_valid = topic_config_inner
            .write_queue_nums
            .max(topic_config_inner.read_queue_nums);
        if queue_id_int >= id_valid as i32 {
            response.with_code(ResponseCode::SystemError);
            response.with_remark(format!(
                "request queueId[{}] is illegal, {:?} Producer: {}",
                queue_id_int, topic_config_inner, inbound_peer
            ));
        }
    }

    #[inline]
    pub(crate) fn random_queue_id(&self, write_queue_nums: u32) -> u32 {
        rand::rng().random_range(0..=queue_config::RANDOM_QUEUE_RANGE) % write_queue_nums
    }
}

fn rewrite_response_for_static_topic(
    command_factory: &RemotingCommandFactory,
    response_header: &mut SendMessageResponseHeader,
    mapping_context: &TopicQueueMappingContext,
) -> Option<RemotingCommand> {
    // Early return: no mapping detail
    let mapping_detail = mapping_context.mapping_detail.as_ref()?;

    // Early return: no leader item
    let Some(mapping_item) = mapping_context.leader_item.as_ref() else {
        return Some(command_factory.create_response_command_with_code_remark(
            ResponseCode::NotLeaderForQueue,
            format!(
                "{}-{:?} does not exit in request process of current broker {:?}",
                mapping_context.topic.as_str(),
                mapping_context.global_id,
                mapping_detail.topic_queue_mapping_info.bname.as_ref()
            ),
        ));
    };

    let static_logic_offset = mapping_item.compute_static_queue_offset_loosely(response_header.queue_offset());

    response_header.set_queue_id(mapping_context.global_id.unwrap());
    response_header.set_queue_offset(static_logic_offset);
    None
}

fn message_body_limit_violation(request: &RemotingCommand, configured_max_message_size: i32) -> Option<String> {
    let body_size = request.body().map_or(0, |body| body.len());
    let max_message_size = usize::try_from(configured_max_message_size).unwrap_or_default();
    (body_size > max_message_size)
        .then(|| format!("message body size {body_size} exceeds the configured maximum {max_message_size} bytes"))
}

fn broker_send_permission_denied(broker_permission: u32) -> bool {
    !PermName::is_writeable(broker_permission)
}

fn message_store_not_initialized() -> RocketMQError {
    RocketMQError::not_initialized("message_store")
}

fn no_retry_queue_response(command_factory: &RemotingCommandFactory) -> RemotingCommand {
    command_factory.create_success_response_command()
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::sync::Arc;

    use std::collections::HashMap;
    use std::future::Future;

    use crate::config::broker_config::BrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::constant::PermName;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::RemotingSysResponseCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
    use rocketmq_protocol::protocol::SerializeType;
    use rocketmq_store::store_append_receipt;
    use rocketmq_store::FlushBacklog;
    use rocketmq_store::PutMessageResult;
    use rocketmq_store::PutMessageStatus;
    use rocketmq_store::StoreAppendReceipt;
    use rocketmq_store::StoreHealthSnapshot;
    use rocketmq_store::StorePorts;
    use rocketmq_store::SyncFlushRuntimeInfo;

    use crate::mqtrace::send_message_context::SendMessageContext;
    use crate::mqtrace::send_message_hook::SendMessageHook;
    use crate::send_message_constants::error_messages;

    use super::add_send_response_metadata;
    use super::append_message_with_store;
    use super::broker_send_permission_denied;
    use super::has_registered_send_message_hooks;
    use super::has_valid_compaction_key;
    use super::map_put_status_to_response;
    use super::map_store_api_error;
    use super::message_body_limit_violation;
    use super::message_store_not_initialized;
    use super::no_retry_queue_response;
    use super::store_health_reject_remark;
    use super::store_health_reject_remark_from;
    use super::sync_flush_backlog_reject_remark;
    use super::StoreApiRetryDisposition;
    use crate::send_message_constants::apply_topic_delivery_properties;

    #[test]
    fn send_shared_seam_accepts_an_arc_held_leaf() {
        type TransactionService =
            crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService<
                StorePorts,
            >;

        fn call_shared<'a>(
            leaf: &'a Arc<super::SendMessageProcessor<StorePorts, TransactionService>>,
            request: &'a mut super::RemotingRequest,
        ) -> impl Future<Output = rocketmq_error::RocketMQResult<super::HandlerOutcome>> + 'a {
            leaf.process_shared(request)
        }

        let _ = call_shared;
    }

    #[test]
    fn zero_retry_queue_reply_is_a_response_on_both_wire_formats() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let mut response =
                no_retry_queue_response(&application_remoting_command_factory()).set_serialize_type(serialize_type);
            assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
            assert!(response.is_response_type());

            let mut encoded = bytes::BytesMut::new();
            response
                .try_fast_header_encode(&mut encoded)
                .expect("zero-retry response should encode");
            let decoded = RemotingCommand::decode(&mut encoded)
                .expect("zero-retry response should decode")
                .expect("encoded response should contain one frame");
            assert_eq!(ResponseCode::from(decoded.code()), ResponseCode::Success);
            assert!(decoded.is_response_type());
        }
    }

    #[test]
    fn send_response_keeps_region_and_trace_fields() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let mut response = application_remoting_command_factory()
                .create_success_response_command_with_header(SendMessageResponseHeader::new(
                    CheetahString::from_static_str("msg-id"),
                    0,
                    0,
                    None,
                    None,
                    None,
                ))
                .set_serialize_type(serialize_type);
            add_send_response_metadata(&mut response, CheetahString::from_static_str("region-a"), true);
            let mut encoded = bytes::BytesMut::new();

            response
                .try_fast_header_encode(&mut encoded)
                .expect("send response should encode");
            let decoded = RemotingCommand::decode(&mut encoded)
                .expect("send response should decode")
                .expect("send response frame should be complete");

            let fields = decoded.ext_fields().expect("send response ext fields");
            assert_eq!(
                fields.get(MessageConst::PROPERTY_MSG_REGION).map(CheetahString::as_str),
                Some("region-a")
            );
            assert_eq!(
                fields
                    .get(MessageConst::PROPERTY_TRACE_SWITCH)
                    .map(CheetahString::as_str),
                Some("true")
            );
        }
    }

    #[test]
    fn broker_receive_spans_bind_remote_parent_before_instrumentation() {
        let source = include_str!("send_message_processor.rs").replace("\r\n", "\n");
        let production = source
            .split_once("#[cfg(test)]\nmod tests")
            .map(|(production, _)| production)
            .expect("SendMessageProcessor production section");
        let parent_with_handle = concat!("set_span_parent_from_properties", "_with_handle");
        let late_parent_with_handle = concat!("set_current_span_parent_from_properties", "_with_handle");
        let record_with_handle = concat!("record_message_properties", "_with_handle");
        let late_record_with_handle = concat!("record_current_message_properties", "_with_handle");

        assert_eq!(
            production
                .matches("receive_span_and_request(request.command())")
                .count(),
            1
        );
        assert_eq!(
            production
                .matches("parse_request_header(request, request_code)")
                .count(),
            2
        );
        assert_eq!(
            production
                .matches("decode_command_custom_header::<SendMessageResponseHeader>()")
                .count(),
            1
        );
        assert_eq!(production.matches(parent_with_handle).count(), 1);
        assert_eq!(production.matches(late_parent_with_handle).count(), 0);
        assert_eq!(production.matches(record_with_handle).count(), 1);
        assert_eq!(production.matches(late_record_with_handle).count(), 1);
        assert!(!production.contains(concat!("set_current_span_parent_from_", "properties(")));
        assert!(!production.contains(concat!("record_current_message_", "properties(")));
        assert_eq!(production.matches("trace::broker::receive_send_span").count(), 1);
        assert!(!production.contains("name = \"RocketMQ BROKER RECEIVE_SEND\""));
    }

    struct CapabilityStore {
        health: StoreHealthSnapshot,
        receipt: Option<StoreAppendReceipt>,
    }

    impl rocketmq_store_api::MessageAppender<()> for CapabilityStore {
        type Receipt = StoreAppendReceipt;

        fn append_message(
            &mut self,
            (): (),
        ) -> impl Future<Output = Result<Self::Receipt, rocketmq_store_api::StoreError>> + Send {
            let result = self.receipt.take().ok_or_else(|| {
                rocketmq_store_api::StoreError::new(
                    &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
                    rocketmq_store_api::StoreOperation::Append,
                )
            });
            std::future::ready(result)
        }
    }

    impl rocketmq_store_api::StoreHealth for CapabilityStore {
        type Snapshot = StoreHealthSnapshot;

        fn health_snapshot(&self) -> Self::Snapshot {
            self.health.clone()
        }
    }

    struct NoopSendMessageHook;

    impl SendMessageHook for NoopSendMessageHook {
        fn hook_name(&self) -> &'static str {
            "noop"
        }

        fn send_message_before(&self, _context: &SendMessageContext) {}

        fn send_message_after(&self, _context: &SendMessageContext) {}
    }

    #[test]
    fn has_registered_send_message_hooks_matches_vector_presence() {
        let empty_hooks: Vec<Box<dyn SendMessageHook>> = Vec::new();
        assert!(!has_registered_send_message_hooks(&empty_hooks));

        let hooks: Vec<Box<dyn SendMessageHook>> = vec![Box::new(NoopSendMessageHook)];
        assert!(has_registered_send_message_hooks(&hooks));
    }

    #[test]
    fn priority_message_maps_to_bounded_priority_queue() {
        let mut topic = rocketmq_model::common::config::TopicConfig::with_queues("priority-topic", 8, 8);
        topic.attributes.insert("message.type".into(), "PRIORITY".into());
        let mut properties = HashMap::from([(
            CheetahString::from_static_str(MessageConst::PROPERTY_PRIORITY),
            CheetahString::from_static_str("99"),
        )]);
        let mut queue_id = 0;

        apply_topic_delivery_properties(&topic, &"priority-topic".into(), &mut properties, &mut queue_id);

        assert_eq!(queue_id, 7);
        assert_eq!(
            properties
                .get(MessageConst::PROPERTY_PRIORITY)
                .map(CheetahString::as_str),
            Some("99")
        );
    }

    #[test]
    fn priority_property_is_applied_without_topic_metadata_gating() {
        let topic = rocketmq_model::common::config::TopicConfig::with_queues("normal-topic", 8, 8);
        let mut properties = HashMap::from([(
            CheetahString::from_static_str(MessageConst::PROPERTY_PRIORITY),
            CheetahString::from_static_str("3"),
        )]);
        let mut queue_id = 2;

        apply_topic_delivery_properties(&topic, &"normal-topic".into(), &mut properties, &mut queue_id);

        assert_eq!(queue_id, 3);
        assert_eq!(
            properties
                .get(MessageConst::PROPERTY_PRIORITY)
                .map(CheetahString::as_str),
            Some("3")
        );
    }

    #[test]
    fn lite_message_adds_internal_multi_dispatch_queue() {
        let topic = rocketmq_model::common::config::TopicConfig::with_queues("parent", 8, 8);
        let mut properties = HashMap::from([(
            CheetahString::from_static_str(MessageConst::PROPERTY_LITE_TOPIC),
            CheetahString::from_static_str("child"),
        )]);
        let mut queue_id = 0;

        apply_topic_delivery_properties(&topic, &"parent".into(), &mut properties, &mut queue_id);

        assert_eq!(
            properties
                .get(MessageConst::PROPERTY_INNER_MULTI_DISPATCH)
                .map(CheetahString::as_str),
            Some("%LMQ%$parent$child")
        );
    }

    #[test]
    fn message_store_not_initialized_uses_not_initialized_kind() {
        let error = message_store_not_initialized();

        assert_eq!(error.descriptor(), &rocketmq_error::CORE_LIFECYCLE_NOT_INITIALIZED);
    }

    #[test]
    fn single_and_batch_requests_share_the_configured_message_body_limit() {
        let max_message_size = 1024;

        for request_code in [RequestCode::SendMessage, RequestCode::SendBatchMessage] {
            let exact =
                RemotingCommand::create_remoting_command(request_code).set_body(vec![0_u8; max_message_size as usize]);
            let oversized =
                RemotingCommand::create_remoting_command(request_code)
                    .set_body(vec![0_u8; max_message_size as usize + 1]);

            assert!(message_body_limit_violation(&exact, max_message_size).is_none());
            assert!(message_body_limit_violation(&oversized, max_message_size).is_some());
        }
    }

    #[test]
    fn broker_write_permission_rejects_every_send_request_shape() {
        for request_code in [
            RequestCode::SendMessage,
            RequestCode::SendMessageV2,
            RequestCode::SendBatchMessage,
        ] {
            assert!(
                broker_send_permission_denied(PermName::PERM_READ),
                "{request_code:?} must be rejected by the shared pre-send check"
            );
            assert!(
                !broker_send_permission_denied(PermName::PERM_READ | PermName::PERM_WRITE),
                "{request_code:?} must be accepted when write permission is present"
            );
        }
    }

    #[test]
    fn compaction_message_requires_non_blank_key() {
        let key = CheetahString::from_static_str(MessageConst::PROPERTY_KEYS);

        assert!(!has_valid_compaction_key(&HashMap::new()));
        for invalid in ["", " ", "\t\r\n"] {
            assert!(!has_valid_compaction_key(&HashMap::from([(
                key.clone(),
                CheetahString::from_string(invalid.to_string()),
            )])));
        }
        assert!(has_valid_compaction_key(&HashMap::from([(
            key,
            CheetahString::from_static_str("business-key"),
        )])));
    }

    #[test]
    fn store_api_error_every_descriptor_has_one_fixed_legacy_boundary_mapping() {
        use rocketmq_error::RemotingResponseCode;

        let cases = [
            (
                &rocketmq_error::STORAGE_LIFECYCLE_NOT_STARTED,
                RemotingResponseCode::SystemError,
                "Storage service is not started",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
                RemotingResponseCode::SystemError,
                "Storage backend is unavailable",
                StoreApiRetryDisposition::SwitchBroker,
            ),
            (
                &rocketmq_error::STORAGE_REQUEST_INVALID,
                RemotingResponseCode::InvalidParameter,
                "Storage request is invalid",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_MAPPED_FILE_NOT_FOUND,
                RemotingResponseCode::QueryNotFound,
                "Mapped file was not found",
                StoreApiRetryDisposition::SwitchBroker,
            ),
            (
                &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED,
                RemotingResponseCode::SystemError,
                "Storage capacity is exhausted",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_READ_FAILED,
                RemotingResponseCode::SystemError,
                "Storage read failed",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_WRITE_FAILED,
                RemotingResponseCode::SystemError,
                "Storage write failed",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_IO_FAILED,
                RemotingResponseCode::SystemError,
                "Storage I/O operation failed",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_STATE_CORRUPTED,
                RemotingResponseCode::SystemError,
                "Storage state is corrupted",
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_OPERATION_TIMED_OUT,
                RemotingResponseCode::SystemBusy,
                "Storage operation timed out",
                StoreApiRetryDisposition::AfterBackoff,
            ),
            (
                &rocketmq_error::STORAGE_OPERATION_UNSUPPORTED,
                RemotingResponseCode::RequestCodeNotSupported,
                "Storage operation is unsupported",
                StoreApiRetryDisposition::SwitchBroker,
            ),
            (
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                RemotingResponseCode::SystemError,
                "Internal storage failure",
                StoreApiRetryDisposition::Never,
            ),
        ];

        for (descriptor, expected_code, expected_message, expected_retry) in cases {
            let mapped = map_store_api_error(
                rocketmq_store_api::StoreError::new(descriptor, rocketmq_store_api::StoreOperation::Append)
                    .with_detail("backend-secret")
                    .with_source(std::io::Error::other("source-secret")),
            );

            assert_eq!(mapped.retry, expected_retry, "descriptor {}", descriptor.code());
            let response = mapped.apply_to(RemotingCommand::create_success_response_command());
            assert_eq!(
                response.code(),
                expected_code.as_i32(),
                "descriptor {}",
                descriptor.code()
            );
            assert_eq!(
                response.remark().map(|remark| remark.as_str()),
                Some(expected_message),
                "descriptor {}",
                descriptor.code()
            );
            let remark = response.remark().expect("storage response has a fixed public remark");
            assert!(!remark.contains("backend-secret"));
            assert!(!remark.contains("source-secret"));
        }
    }

    #[test]
    fn store_api_error_retry_mapping_preserves_operation_sensitive_legacy_policy() {
        use rocketmq_store_api::StoreOperation;

        let cases = [
            (
                &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
                StoreOperation::Append,
                StoreApiRetryDisposition::SwitchBroker,
            ),
            (
                &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
                StoreOperation::Read,
                StoreApiRetryDisposition::Never,
            ),
            (
                &rocketmq_error::STORAGE_MAPPED_FILE_NOT_FOUND,
                StoreOperation::Append,
                StoreApiRetryDisposition::SwitchBroker,
            ),
            (
                &rocketmq_error::STORAGE_MAPPED_FILE_NOT_FOUND,
                StoreOperation::Read,
                StoreApiRetryDisposition::Immediate,
            ),
            (
                &rocketmq_error::STORAGE_OPERATION_UNSUPPORTED,
                StoreOperation::Admin,
                StoreApiRetryDisposition::SwitchBroker,
            ),
            (
                &rocketmq_error::STORAGE_OPERATION_TIMED_OUT,
                StoreOperation::Flush,
                StoreApiRetryDisposition::AfterBackoff,
            ),
            (
                &rocketmq_error::STORAGE_WRITE_FAILED,
                StoreOperation::Append,
                StoreApiRetryDisposition::Never,
            ),
        ];

        for (descriptor, operation, expected_retry) in cases {
            let mapped = map_store_api_error(rocketmq_store_api::StoreError::new(descriptor, operation));
            assert_eq!(mapped.retry, expected_retry, "{} / {operation:?}", descriptor.code());
        }
    }

    #[test]
    fn store_api_error_legacy_adapter_terminates_the_typed_source_chain() {
        #[derive(Debug)]
        struct StoreSource;

        impl std::fmt::Display for StoreSource {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("private source")
            }
        }

        impl std::error::Error for StoreSource {}

        let store_error = rocketmq_store_api::StoreError::new(
            &rocketmq_error::STORAGE_WRITE_FAILED,
            rocketmq_store_api::StoreOperation::Append,
        )
        .with_source(StoreSource);
        assert!(store_error
            .source()
            .and_then(|source| source.downcast_ref::<StoreSource>())
            .is_some());

        let response = map_store_api_error(store_error).apply_to(RemotingCommand::create_success_response_command());
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("Storage write failed")
        );
        assert!(!response
            .remark()
            .expect("fixed public remark")
            .contains("private source"));
    }

    #[tokio::test]
    async fn append_seam_depends_only_on_message_appender() {
        let expected = store_append_receipt(PutMessageResult::new_default(PutMessageStatus::PutOk), 20, 20);
        let mut store = CapabilityStore {
            health: StoreHealthSnapshot::default(),
            receipt: Some(expected),
        };

        let actual = append_message_with_store(&mut store, ())
            .await
            .expect("legacy append await remains active")
            .expect("append succeeds");

        assert_eq!(PutMessageStatus::PutOk, actual.result().put_message_status());
        assert_eq!(20, actual.appended_watermark());
        assert_eq!(20, actual.durable_watermark());
    }

    #[test]
    fn reject_seam_depends_only_on_store_health() {
        let store = CapabilityStore {
            health: StoreHealthSnapshot {
                writable: false,
                last_error: Some(&rocketmq_error::STORAGE_WRITE_FAILED),
                ..StoreHealthSnapshot::default()
            },
            receipt: None,
        };

        let remark =
            store_health_reject_remark_from(&BrokerConfig::default(), &store).expect("non-writable store rejects");

        assert!(remark.contains("reason=store_not_writeable"));
        assert!(remark.contains("lastFlushErrorCode=storage.write.failed"));
    }

    #[test]
    fn legacy_append_status_preserves_every_processor_output() {
        let cases = [
            (PutMessageStatus::PutOk, RemotingSysResponseCode::Success as i32, None),
            (
                PutMessageStatus::FlushDiskTimeout,
                ResponseCode::FlushDiskTimeout as i32,
                None,
            ),
            (
                PutMessageStatus::FlushSlaveTimeout,
                ResponseCode::FlushSlaveTimeout as i32,
                None,
            ),
            (
                PutMessageStatus::SlaveNotAvailable,
                ResponseCode::SlaveNotAvailable as i32,
                None,
            ),
            (
                PutMessageStatus::ServiceNotAvailable,
                ResponseCode::ServiceNotAvailable as i32,
                Some(error_messages::SERVICE_NOT_AVAILABLE),
            ),
            (
                PutMessageStatus::CreateMappedFileFailed,
                RemotingSysResponseCode::SystemError as i32,
                Some(error_messages::MAPPED_FILE_CREATE_FAILED),
            ),
            (
                PutMessageStatus::MessageIllegal,
                ResponseCode::MessageIllegal as i32,
                Some(error_messages::MESSAGE_ILLEGAL),
            ),
            (
                PutMessageStatus::PropertiesSizeExceeded,
                ResponseCode::MessageIllegal as i32,
                Some(error_messages::MESSAGE_ILLEGAL),
            ),
            (
                PutMessageStatus::OsPageCacheBusy,
                RemotingSysResponseCode::SystemError as i32,
                Some(error_messages::OS_PAGE_CACHE_BUSY),
            ),
            (
                PutMessageStatus::UnknownError,
                RemotingSysResponseCode::SystemError as i32,
                Some("UNKNOWN_ERROR"),
            ),
            (
                PutMessageStatus::InSyncReplicasNotEnough,
                RemotingSysResponseCode::SystemError as i32,
                Some(error_messages::IN_SYNC_REPLICAS_NOT_ENOUGH),
            ),
            (
                PutMessageStatus::PutToRemoteBrokerFail,
                RemotingSysResponseCode::SystemError as i32,
                Some("UNKNOWN_ERROR DEFAULT"),
            ),
            (
                PutMessageStatus::LmqConsumeQueueNumExceeded,
                ResponseCode::LmqQuotaExceeded as i32,
                Some(error_messages::LMQ_QUEUE_NUM_EXCEEDED),
            ),
            (
                PutMessageStatus::WheelTimerFlowControl,
                RemotingSysResponseCode::SystemError as i32,
                Some(error_messages::TIMER_FLOW_CONTROL),
            ),
            (
                PutMessageStatus::WheelTimerMsgIllegal,
                ResponseCode::MessageIllegal as i32,
                Some(error_messages::TIMER_MSG_ILLEGAL),
            ),
            (
                PutMessageStatus::WheelTimerNotEnable,
                RemotingSysResponseCode::SystemError as i32,
                Some(error_messages::TIMER_NOT_ENABLED),
            ),
        ];

        for (legacy, expected_code, expected_remark) in cases {
            let mut response = RemotingCommand::create_success_response_command();

            map_put_status_to_response(legacy, &mut response);

            assert_eq!(expected_code, response.code(), "legacy status {legacy:?}");
            assert_eq!(
                expected_remark,
                response.remark().map(AsRef::as_ref),
                "legacy status {legacy:?}"
            );
        }
    }

    #[test]
    fn sync_flush_backlog_reject_remark_is_disabled_by_default() {
        let runtime_info = SyncFlushRuntimeInfo {
            queue_depth: 64,
            oldest_wait_millis: 10_000,
            ..SyncFlushRuntimeInfo::default()
        };

        assert!(sync_flush_backlog_reject_remark(&BrokerConfig::default(), runtime_info).is_none());
    }

    #[test]
    fn sync_flush_backlog_reject_remark_matches_depth_threshold() {
        let broker_config = BrokerConfig {
            sync_flush_backlog_reject_depth: 8,
            ..BrokerConfig::default()
        };
        let runtime_info = SyncFlushRuntimeInfo {
            queue_depth: 8,
            ..SyncFlushRuntimeInfo::default()
        };

        let remark = sync_flush_backlog_reject_remark(&broker_config, runtime_info).expect("depth should reject");
        assert!(remark.contains("queueDepth=8"));
        assert!(remark.contains("rejectDepth=8"));
    }

    #[test]
    fn sync_flush_backlog_reject_remark_matches_oldest_wait_threshold() {
        let broker_config = BrokerConfig {
            sync_flush_backlog_reject_wait_millis: 250,
            ..BrokerConfig::default()
        };
        let runtime_info = SyncFlushRuntimeInfo {
            oldest_wait_millis: 250,
            ..SyncFlushRuntimeInfo::default()
        };

        let remark = sync_flush_backlog_reject_remark(&broker_config, runtime_info).expect("wait should reject");
        assert!(remark.contains("oldestWaitMillis=250"));
        assert!(remark.contains("rejectWaitMillis=250"));
    }

    #[test]
    fn store_health_reject_remark_does_not_reject_optional_reasons_by_default() {
        let snapshot = StoreHealthSnapshot {
            flush_backlog: FlushBacklog {
                queue_depth: 64,
                oldest_wait_millis: 10_000,
            },
            dispatch_behind_bytes: 1024 * 1024,
            replication_pending_count: 64,
            replication_oldest_wait_millis: 10_000,
            ..StoreHealthSnapshot::default()
        };

        assert!(store_health_reject_remark(&BrokerConfig::default(), snapshot).is_none());
    }

    #[test]
    fn store_health_reject_remark_reports_page_cache_busy() {
        let snapshot = StoreHealthSnapshot {
            page_cache_busy: true,
            ..StoreHealthSnapshot::default()
        };

        let remark = store_health_reject_remark(&BrokerConfig::default(), snapshot).expect("page cache should reject");
        assert!(remark.contains("reason=page_cache_busy"));
    }

    #[test]
    fn store_health_reject_remark_reports_transient_pool_deficient() {
        let snapshot = StoreHealthSnapshot {
            transient_pool_deficient: true,
            ..StoreHealthSnapshot::default()
        };

        let remark =
            store_health_reject_remark(&BrokerConfig::default(), snapshot).expect("transient pool should reject");
        assert!(remark.contains("reason=transient_store_pool_deficient"));
    }

    #[test]
    fn store_health_reject_remark_reports_sync_flush_backlog() {
        let broker_config = BrokerConfig {
            sync_flush_backlog_reject_depth: 8,
            ..BrokerConfig::default()
        };
        let snapshot = StoreHealthSnapshot {
            flush_backlog: FlushBacklog {
                queue_depth: 8,
                ..FlushBacklog::default()
            },
            ..StoreHealthSnapshot::default()
        };

        let remark = store_health_reject_remark(&broker_config, snapshot).expect("sync flush should reject");
        assert!(remark.contains("reason=sync_flush_backlog"));
        assert!(remark.contains("queueDepth=8"));
    }

    #[test]
    fn store_health_reject_remark_reports_ha_pending() {
        let broker_config = BrokerConfig {
            ha_pending_reject_count: 4,
            ..BrokerConfig::default()
        };
        let snapshot = StoreHealthSnapshot {
            replication_pending_count: 4,
            replication_oldest_wait_millis: 250,
            ..StoreHealthSnapshot::default()
        };

        let remark = store_health_reject_remark(&broker_config, snapshot).expect("HA pending should reject");
        assert!(remark.contains("reason=ha_pending"));
        assert!(remark.contains("pendingCount=4"));
        assert!(remark.contains("oldestWaitMillis=250"));
    }

    #[test]
    fn store_health_reject_remark_reports_reput_lag() {
        let broker_config = BrokerConfig {
            reput_lag_reject_bytes: 1024,
            ..BrokerConfig::default()
        };
        let snapshot = StoreHealthSnapshot {
            dispatch_behind_bytes: 1024,
            ..StoreHealthSnapshot::default()
        };

        let remark = store_health_reject_remark(&broker_config, snapshot).expect("Reput lag should reject");
        assert!(remark.contains("reason=reput_lag"));
        assert!(remark.contains("dispatchBehindBytes=1024"));
    }

    #[test]
    fn store_health_reject_remark_reports_store_shutdown() {
        let snapshot = StoreHealthSnapshot {
            shutdown: true,
            ..StoreHealthSnapshot::default()
        };

        let remark = store_health_reject_remark(&BrokerConfig::default(), snapshot).expect("shutdown should reject");
        assert!(remark.contains("reason=store_shutdown"));
    }

    #[test]
    fn store_health_reject_remark_reports_typed_flush_failure() {
        for descriptor in [
            &rocketmq_error::STORAGE_READ_FAILED,
            &rocketmq_error::STORAGE_WRITE_FAILED,
            &rocketmq_error::STORAGE_IO_FAILED,
            &rocketmq_error::STORAGE_STATE_CORRUPTED,
        ] {
            let snapshot = StoreHealthSnapshot {
                writable: false,
                last_error: Some(descriptor),
                ..StoreHealthSnapshot::default()
            };

            let remark =
                store_health_reject_remark(&BrokerConfig::default(), snapshot).expect("flush failure should reject");
            assert!(remark.contains("reason=store_not_writeable"));
            assert!(
                remark.contains(&format!("lastFlushErrorCode={}", descriptor.code())),
                "canonical descriptor {}",
                descriptor.code()
            );
            assert!(!remark.contains("backend detail"));
        }
    }
}
