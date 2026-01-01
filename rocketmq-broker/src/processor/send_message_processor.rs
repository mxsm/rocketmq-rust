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
use std::net::SocketAddr;
use std::time::Instant;

use cheetah_string::CheetahString;
use rand::Rng;
use rocketmq_common::common::attribute::cleanup_policy::CleanupPolicy;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_batch::MessageExtBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::common::TopicFilterType;
use rocketmq_common::common::TopicSysFlag;
use rocketmq_common::common::TopicSysFlag::build_sys_flag;
use rocketmq_common::utils::message_utils;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_common::utils::util_all;
use rocketmq_common::CleanupPolicyUtils;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::MessageDecoder::message_properties_to_string;
use rocketmq_common::MessageDecoder::string_to_message_properties;
use rocketmq_common::TimeUtils;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode::SystemError;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::parse_request_header;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use rocketmq_store::stats::stats_type::StatsType;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::client::net::broker_to_client::Broker2Client;
use crate::mqtrace::consume_message_context::ConsumeMessageContext;
use crate::mqtrace::consume_message_hook::ConsumeMessageHook;
use crate::mqtrace::send_message_context::SendMessageContext;
use crate::mqtrace::send_message_hook::SendMessageHook;
use crate::send_message_constants::error_messages;
use crate::send_message_constants::message_limits;
use crate::send_message_constants::queue_config;
use crate::send_message_constants::retry_config;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;
use crate::transaction::transactional_message_service::TransactionalMessageService;

pub struct SendMessageProcessor<MS: MessageStore, TS> {
    inner: ArcMut<Inner<MS, TS>>,
    store_host: SocketAddr,
}

impl<MS, TS> RequestProcessor for SendMessageProcessor<MS, TS>
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
        debug!("SendMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::SendMessage
            | RequestCode::SendMessageV2
            | RequestCode::SendBatchMessage
            | RequestCode::ConsumerSendMsgBack => self.process_request_inner(channel, ctx, request_code, request).await,
            _ => {
                warn!("SendMessageProcessor received unknown request code: {:?}", request_code);
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }

    fn reject_request(&self, _code: i32) -> RejectRequestResponse {
        let enable_slave_acting_master = self
            .inner
            .broker_runtime_inner
            .broker_config()
            .enable_slave_acting_master;
        let broker_role = self.inner.broker_runtime_inner.message_store_config().broker_role;
        if !enable_slave_acting_master && broker_role == BrokerRole::Slave {
            return (
                true,
                Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SlaveNotAvailable,
                    "The broker is slave mode, not allowed to accept message",
                )),
            );
        }
        let message_store = self.inner.broker_runtime_inner.message_store_unchecked();
        if message_store.is_os_page_cache_busy() || message_store.is_transient_store_pool_deficient() {
            return (
                true,
                Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemBusy,
                    "The broker message store is busy, please try again later",
                )),
            );
        }

        (false, None)
    }
}

// RequestProcessor implementation
impl<MS, TS> SendMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    pub fn has_send_message_hook(&self) -> bool {
        self.inner.send_message_hook_vec.is_empty()
    }

    fn clear_reserved_properties(request_header: &mut SendMessageRequestHeader) {
        let properties = request_header.properties.take();
        if let Some(value) = properties {
            let delete_properties = message_utils::delete_property(value.as_str(), MessageConst::PROPERTY_POP_CK);
            request_header.properties = Some(CheetahString::from_string(delete_properties));
        }
    }

    pub async fn process_request_inner(
        &mut self,
        channel: Channel,
        mut ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::ConsumerSendMsgBack => self.inner.consumer_send_msg_back(&channel, &ctx, request).await,
            _ => {
                let mut request_header = parse_request_header(request, request_code)?;
                let mapping_context = self
                    .inner
                    .broker_runtime_inner
                    .topic_queue_mapping_manager()
                    .build_topic_queue_mapping_context(&request_header, true);
                let rewrite_result =
                    TopicQueueMappingManager::rewrite_request_for_static_topic(&mut request_header, &mapping_context);
                if rewrite_result.is_some() {
                    return Ok(rewrite_result);
                }

                let send_message_context = self
                    .inner
                    .build_msg_context(&channel, &ctx, &mut request_header, request);
                self.inner.execute_send_message_hook_before(&send_message_context);
                SendMessageProcessor::<MS, TS>::clear_reserved_properties(&mut request_header);

                let execute_send_message_hook_after = {
                    let inner = self.inner.clone();
                    move |ctx: &mut SendMessageContext, cmd: &mut RemotingCommand| {
                        inner.execute_send_message_hook_after(Some(cmd), ctx)
                    }
                };

                if request_header.is_batch() {
                    //handle batch message
                    self.send_batch_message(
                        &channel,
                        &mut ctx,
                        request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        execute_send_message_hook_after,
                    )
                    .await
                } else {
                    //handle single message
                    self.send_message(
                        &channel,
                        &mut ctx,
                        request,
                        send_message_context,
                        request_header,
                        mapping_context,
                        execute_send_message_hook_after,
                    )
                    .await
                }
            }
        }
    }
}

impl<MS, TS> SendMessageProcessor<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    pub fn new(
        transactional_message_service: ArcMut<TS>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        let store_host = broker_runtime_inner.store_host();
        Self {
            inner: ArcMut::new(Inner {
                send_message_hook_vec: ArcMut::new(Vec::new()),
                consume_message_hook_vec: ArcMut::new(Vec::new()),
                transactional_message_service,
                broker_to_client: Default::default(),
                broker_runtime_inner,
            }),
            store_host,
        }
    }

    async fn send_batch_message<F>(
        &mut self,
        channel: &Channel,
        ctx: &mut ConnectionHandlerContext,
        request: &mut RemotingCommand,
        mut send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mut mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>
    where
        F: Fn(&mut SendMessageContext, &mut RemotingCommand),
    {
        let mut response = self.pre_send(channel, ctx, request, &request_header).await;
        if response.code() != -1 {
            return Ok(Some(response));
        }
        let topic_config = self
            .inner
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(request_header.topic())
            .ok_or_else(|| RocketMQError::TopicNotExist {
                topic: request_header.topic().to_string(),
            })?;
        let mut queue_id = request_header.queue_id;
        if queue_id < 0 {
            queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        if request_header.topic.len() > message_limits::MAX_TOPIC_LENGTH {
            return Ok(Some(response.set_code(ResponseCode::MessageIllegal).set_remark(
                format!("message topic length too long {}", request_header.topic().len()),
            )));
        }

        if !request_header.topic.is_empty() && request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            return Ok(Some(response.set_code(ResponseCode::MessageIllegal).set_remark(
                format!("batch request does not support retry group  {}", request_header.topic()),
            )));
        }
        let mut message_ext = MessageExtBrokerInner::default();
        message_ext.message_ext_inner.message.topic = request_header.topic().clone();
        message_ext.message_ext_inner.queue_id = queue_id;
        let mut sys_flag = request_header.sys_flag;
        if TopicFilterType::MultiTag == topic_config.topic_filter_type {
            sys_flag |= MessageSysFlag::MULTI_TAGS_FLAG;
        }
        message_ext.message_ext_inner.sys_flag = sys_flag;
        message_ext.message_ext_inner.message.flag = request_header.flag;
        message_ext
            .message_ext_inner
            .message
            .set_properties(string_to_message_properties(request_header.properties.as_ref()));
        message_ext.message_ext_inner.message.body = request.body().cloned();
        message_ext.message_ext_inner.born_timestamp = request_header.born_timestamp;
        message_ext.message_ext_inner.born_host = channel.remote_address();
        message_ext.message_ext_inner.store_host = self.store_host;
        message_ext.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);
        let cluster_name = self
            .inner
            .broker_runtime_inner
            .broker_config()
            .broker_identity
            .broker_cluster_name
            .clone();
        message_ext.message_ext_inner.message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER),
            cluster_name,
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
                    .body
                    .clone(),
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
                    .properties(),
            );
            response_header.set_batch_uniq_id(batch_uniq_id);
            is_inner_batch = true;
        }
        let start = Instant::now();
        let transaction_id =
            MessageClientIDSetter::get_uniq_id(&batch_message.message_ext_broker_inner.message_ext_inner.message);
        let topic = batch_message.message_ext_broker_inner.message_ext_inner.topic().clone();
        let put_message_result = if is_inner_batch {
            self.inner
                .broker_runtime_inner
                .message_store_unchecked_mut()
                .put_message(batch_message.message_ext_broker_inner)
                .await
        } else {
            self.inner
                .broker_runtime_inner
                .message_store_unchecked_mut()
                .put_messages(batch_message)
                .await
        };
        let result = self
            .handle_put_message_result(
                put_message_result,
                &mut response,
                request,
                topic.as_str(),
                transaction_id,
                &mut send_message_context,
                ctx,
                queue_id,
                start,
                &mut mapping_context,
                MessageType::NormalMsg,
            )
            .await;
        send_message_callback(&mut send_message_context, &mut response);
        if result.1 {
            Ok(None)
        } else if result.0.is_some() {
            Ok(result.0)
        } else {
            Ok(Some(response))
        }
    }

    async fn send_message<F>(
        &mut self,
        channel: &Channel,
        ctx: &mut ConnectionHandlerContext,
        request: &mut RemotingCommand,
        mut send_message_context: SendMessageContext,
        request_header: SendMessageRequestHeader,
        mut mapping_context: TopicQueueMappingContext,
        send_message_callback: F,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>>
    where
        F: Fn(&mut SendMessageContext, &mut RemotingCommand),
    {
        let mut response = self.pre_send(channel, ctx, request, &request_header).await;
        if response.code() != -1 {
            return Ok(Some(response));
        }

        let mut topic_config = self
            .inner
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(request_header.topic())
            .ok_or_else(|| RocketMQError::TopicNotExist {
                topic: request_header.topic().to_string(),
            })?;
        let mut queue_id = request_header.queue_id;
        if queue_id < 0 {
            queue_id = self.inner.random_queue_id(topic_config.write_queue_nums) as i32;
        }

        let mut message_ext = MessageExtBrokerInner::default();
        message_ext.message_ext_inner.message.topic = request_header.topic().clone();
        message_ext.message_ext_inner.queue_id = queue_id;
        let mut ori_props = MessageDecoder::string_to_message_properties(request_header.properties.as_ref());
        if !self
            .handle_retry_and_dlq(
                &request_header,
                &mut response,
                request,
                &mut message_ext.message_ext_inner,
                &mut topic_config,
                &mut ori_props,
            )
            .await
        {
            return Ok(Some(response));
        }
        message_ext.message_ext_inner.message.body = request.body().cloned();
        message_ext.message_ext_inner.message.flag = request_header.flag;

        let uniq_key = ori_props.get(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if !uniq_key.is_some_and(|uniq_key_inner| uniq_key_inner.is_empty()) {
            ori_props.insert(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from_string(MessageClientIDSetter::create_uniq_id()),
            );
        }

        let tra_flag = ori_props
            .get(MessageConst::PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|tra_flag_inner| tra_flag_inner.parse().unwrap_or(false));
        message_ext.message_ext_inner.message.properties = ori_props;
        let cleanup_policy = CleanupPolicyUtils::get_delete_policy(Some(&topic_config));

        if cleanup_policy == CleanupPolicy::COMPACTION {
            if let Some(value) = message_ext
                .message_ext_inner
                .message
                .properties
                .get(MessageConst::PROPERTY_KEYS)
            {
                if value.trim().is_empty() {
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::MessageIllegal)
                            .set_remark("Required message key is missing"),
                    ));
                }
            }
        }
        message_ext.tags_code = MessageExtBrokerInner::tags_string2tags_code(
            &topic_config.topic_filter_type,
            message_ext.get_tags().unwrap_or_default().as_str(),
        );

        message_ext.message_ext_inner.born_timestamp = request_header.born_timestamp;
        message_ext.message_ext_inner.born_host = channel.remote_address();
        message_ext.message_ext_inner.store_host = self.store_host;
        message_ext.message_ext_inner.reconsume_times = request_header.reconsume_times.unwrap_or(0);

        message_ext.message_ext_inner.message.properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_CLUSTER),
            self.inner
                .broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_cluster_name
                .clone(),
        );

        message_ext.properties_string =
            MessageDecoder::message_properties_to_string(&message_ext.message_ext_inner.message.properties);
        let send_transaction_prepare_message = if tra_flag
            && !(message_ext.reconsume_times() > 0 && message_ext.message_ext_inner.message.get_delay_time_level() > 0)
        {
            if self
                .inner
                .broker_runtime_inner
                .broker_config()
                .reject_transaction_message
            {
                return Ok(Some(response.set_code(ResponseCode::NoPermission).set_remark(format!(
                    "the broker[{}] sending transaction message is forbidden",
                    self.inner.broker_runtime_inner.broker_config().broker_ip1
                ))));
            }
            true
        } else {
            false
        };

        let start = Instant::now();
        let topic = message_ext.topic().clone();
        let transaction_id = MessageClientIDSetter::get_uniq_id(&message_ext.message_ext_inner.message);
        let put_message_result = if send_transaction_prepare_message {
            self.inner
                .transactional_message_service
                .prepare_message(message_ext)
                .await
        } else {
            self.inner
                .broker_runtime_inner
                .message_store_unchecked_mut()
                .put_message(message_ext)
                .await
        };
        let result = self
            .handle_put_message_result(
                put_message_result,
                &mut response,
                request,
                topic.as_str(),
                transaction_id,
                &mut send_message_context,
                ctx,
                queue_id,
                start,
                &mut mapping_context,
                MessageType::NormalMsg,
            )
            .await;
        send_message_callback(&mut send_message_context, &mut response);
        if result.1 {
            Ok(None)
        } else if result.0.is_some() {
            Ok(result.0)
        } else {
            Ok(Some(response))
        }
    }

    /// Map PutMessageStatus to response code
    #[inline]
    fn map_put_status_to_response_code(
        &self,
        status: rocketmq_store::base::message_status_enum::PutMessageStatus,
        response: &mut RemotingCommand,
    ) -> bool {
        use rocketmq_store::base::message_status_enum::PutMessageStatus;

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
                    .set_code_mut(RemotingSysResponseCode::SystemError)
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
            _ => {
                response
                    .set_code_mut(RemotingSysResponseCode::SystemError)
                    .set_remark_mut("UNKNOWN_ERROR DEFAULT");
                false
            }
        }
    }

    /// Update broker statistics for successful message send
    #[inline]
    fn update_broker_stats_on_success(
        &self,
        topic: &str,
        queue_id: i32,
        put_message_result: &PutMessageResult,
        begin_time_millis: Instant,
    ) {
        // SAFETY: When send_ok is true, append_message_result must exist
        let result = put_message_result
            .append_message_result()
            .expect("append_message_result must exist for successful send");

        if TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == topic {
            self.inner
                .broker_runtime_inner
                .broker_stats_manager()
                .inc_queue_put_nums(topic, queue_id, result.msg_num, 1);
            self.inner
                .broker_runtime_inner
                .broker_stats_manager()
                .inc_queue_put_size(topic, queue_id, result.wrote_bytes);
        }

        self.inner
            .broker_runtime_inner
            .broker_stats_manager()
            .inc_topic_put_nums(topic, result.msg_num, 1);
        self.inner
            .broker_runtime_inner
            .broker_stats_manager()
            .inc_topic_put_size(topic, result.wrote_bytes);
        self.inner
            .broker_runtime_inner
            .broker_stats_manager()
            .inc_broker_put_nums(topic, result.msg_num);
        self.inner
            .broker_runtime_inner
            .broker_stats_manager()
            .inc_topic_put_latency(topic, queue_id, begin_time_millis.elapsed().as_millis() as i32);
    }

    /// Set response header for successful message send
    #[inline]
    fn set_success_response_header(
        &self,
        response_header: &mut SendMessageResponseHeader,
        put_message_result: &PutMessageResult,
        queue_id: i32,
        transaction_id: Option<CheetahString>,
    ) {
        // SAFETY: When send_ok is true, append_message_result must exist
        let result = put_message_result
            .append_message_result()
            .expect("append_message_result must exist for successful send");

        response_header.set_msg_id(
            result
                .get_message_id()
                .expect("message_id must exist for successful send"),
        );
        response_header.set_queue_id(queue_id);
        response_header.set_queue_offset(result.logics_offset);
        response_header.set_transaction_id(transaction_id);
    }

    /// Update send message context for hooks
    fn update_send_context_on_success(
        &self,
        send_message_context: &mut SendMessageContext,
        response_header: &SendMessageResponseHeader,
        put_message_result: &PutMessageResult,
        owner: Option<CheetahString>,
        auth_type: Option<CheetahString>,
        owner_parent: Option<CheetahString>,
        owner_self: Option<CheetahString>,
    ) {
        // SAFETY: When send_ok is true, append_message_result must exist
        let result = put_message_result
            .append_message_result()
            .expect("append_message_result must exist for successful send");

        let commercial_size_per_msg = self.inner.broker_runtime_inner.broker_config().commercial_size_per_msg;
        let commercial_base_count = self.inner.broker_runtime_inner.broker_config().commercial_base_count;

        send_message_context.msg_id = response_header.msg_id().clone();
        send_message_context.queue_id = Some(response_header.queue_id());
        send_message_context.queue_offset = Some(response_header.queue_offset());

        let commercial_msg_num = (result.wrote_bytes as f64 / commercial_size_per_msg as f64).ceil() as i32;
        let inc_value = commercial_msg_num * commercial_base_count;

        send_message_context.commercial_send_stats = StatsType::SendSuccess;
        send_message_context.commercial_send_times = inc_value;
        send_message_context.commercial_send_size = result.wrote_bytes;
        send_message_context.commercial_owner = owner.unwrap_or_default();

        send_message_context.send_stat = StatsType::SendSuccess;
        send_message_context.commercial_send_msg_num = commercial_msg_num;
        send_message_context.account_auth_type = auth_type.unwrap_or_default();
        send_message_context.account_owner_parent = owner_parent.unwrap_or_default();
        send_message_context.account_owner_self = owner_self.unwrap_or_default();
        send_message_context.send_msg_size = result.wrote_bytes;
        send_message_context.send_msg_num = result.msg_num;
    }

    /// Update send message context for failure case
    fn update_send_context_on_failure(
        &self,
        send_message_context: &mut SendMessageContext,
        put_message_result: &PutMessageResult,
        request_body_len: i32,
        owner: Option<CheetahString>,
        auth_type: Option<CheetahString>,
        owner_parent: Option<CheetahString>,
        owner_self: Option<CheetahString>,
    ) {
        let commercial_size_per_msg = self.inner.broker_runtime_inner.broker_config().commercial_size_per_msg;

        let msg_num = put_message_result
            .append_message_result()
            .map_or(1, |inner| inner.msg_num)
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

    async fn handle_put_message_result(
        &self,
        put_message_result: PutMessageResult,
        response: &mut RemotingCommand,
        request: &RemotingCommand,
        topic: &str,
        transaction_id: Option<CheetahString>,
        send_message_context: &mut SendMessageContext,
        ctx: &mut ConnectionHandlerContext,
        queue_id_int: i32,
        begin_time_millis: Instant,
        mapping_context: &mut TopicQueueMappingContext,
        _message_type: MessageType,
    ) -> (Option<RemotingCommand>, bool) {
        let send_ok = self.map_put_status_to_response_code(put_message_result.put_message_status(), response);

        let binding = HashMap::new();
        let ext_fields = request.ext_fields().unwrap_or(&binding);
        let owner = ext_fields.get(BrokerStatsManager::COMMERCIAL_OWNER).cloned();
        let auth_type = ext_fields.get(BrokerStatsManager::ACCOUNT_AUTH_TYPE).cloned();
        let owner_parent = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_PARENT).cloned();
        let owner_self = ext_fields.get(BrokerStatsManager::ACCOUNT_OWNER_SELF).cloned();

        if send_ok {
            self.update_broker_stats_on_success(topic, queue_id_int, &put_message_result, begin_time_millis);

            {
                let response_header = response
                    .read_custom_header_mut::<SendMessageResponseHeader>()
                    .expect("SendMessageResponseHeader must exist");

                self.set_success_response_header(
                    response_header,
                    &put_message_result,
                    queue_id_int,
                    transaction_id.clone(),
                );

                let rewrite_result = rewrite_response_for_static_topic(response_header, mapping_context);
                if rewrite_result.is_some() {
                    return (rewrite_result, false);
                }
            }

            response.set_opaque_mut(request.opaque());
            ctx.write_response_ref(response).await;

            if self.has_send_message_hook() {
                let response_header = response
                    .read_custom_header_mut::<SendMessageResponseHeader>()
                    .expect("SendMessageResponseHeader must exist");

                self.update_send_context_on_success(
                    send_message_context,
                    response_header,
                    &put_message_result,
                    owner,
                    auth_type,
                    owner_parent,
                    owner_self,
                );
            }
            (None, true)
        } else {
            if self.has_send_message_hook() {
                let request_body_len = request.body().as_ref().map_or(0, |body| body.len() as i32);
                self.update_send_context_on_failure(
                    send_message_context,
                    &put_message_result,
                    request_body_len,
                    owner,
                    auth_type,
                    owner_parent,
                    owner_self,
                );
            }
            (None, false)
        }
    }

    pub async fn pre_send(
        &mut self,
        channel: &Channel,
        ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
    ) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command_with_header(SendMessageResponseHeader::default());
        // set opaque
        response.with_opaque(request.opaque());
        response.add_ext_field(
            MessageConst::PROPERTY_MSG_REGION,
            self.inner.broker_runtime_inner.broker_config().region_id(),
        );
        response.add_ext_field(
            MessageConst::PROPERTY_TRACE_SWITCH,
            CheetahString::from_static_str(if self.inner.broker_runtime_inner.broker_config().trace_on {
                "true"
            } else {
                "false"
            }),
        );
        let start_timestamp = self
            .inner
            .broker_runtime_inner
            .broker_config()
            .start_accept_send_request_time_stamp;
        if self.inner.broker_runtime_inner.message_store_unchecked().now() < (start_timestamp as u64) {
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
            .msg_check(channel, ctx, request, request_header, &mut response)
            .await;
        response
    }

    async fn handle_retry_and_dlq(
        &mut self,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
        request: &RemotingCommand,
        msg: &mut MessageExt,
        topic_config: &mut ArcMut<rocketmq_common::common::config::TopicConfig>,
        properties: &mut HashMap<CheetahString, CheetahString>,
    ) -> bool {
        let mut new_topic = request_header.topic();
        if !new_topic.is_empty() && new_topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
            let group_name = CheetahString::from_string(KeyBuilder::parse_group(new_topic.as_str()));
            let subscription_group_config = self
                .inner
                .broker_runtime_inner
                .subscription_group_manager()
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
                .broker_runtime_inner
                .rebalance_lock_manager()
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
                    CheetahString::from_string("-1".to_string()),
                );
                let topic_ = CheetahString::from_string(mix_all::get_dlq_topic(group_name.as_str()));
                new_topic = &topic_;
                let queue_id_int = self.inner.random_queue_id(retry_config::DLQ_NUMS_PER_GROUP) as i32;
                let new_topic_config = self
                    .inner
                    .broker_runtime_inner
                    .topic_config_manager_mut()
                    .create_topic_in_send_message_back_method(
                        new_topic,
                        retry_config::DLQ_NUMS_PER_GROUP as i32,
                        PermName::PERM_WRITE | PermName::PERM_READ,
                        false,
                        0,
                    )
                    .await;
                // can optimize
                msg.message.topic = CheetahString::from_string(new_topic.to_string());
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

pub(crate) struct Inner<MS, TS>
where
    MS: MessageStore,
{
    pub(crate) send_message_hook_vec: ArcMut<Vec<Box<dyn SendMessageHook>>>,
    pub(crate) consume_message_hook_vec: ArcMut<Vec<Box<dyn ConsumeMessageHook>>>,
    pub(crate) broker_to_client: Broker2Client,
    pub(crate) transactional_message_service: ArcMut<TS>,
    pub(crate) broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS, TS> Inner<MS, TS>
where
    MS: MessageStore,
    TS: TransactionalMessageService,
{
    #[inline]
    pub fn has_send_message_hook(&self) -> bool {
        self.send_message_hook_vec.is_empty()
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
        for hook in self.send_message_hook_vec.iter() {
            if let Some(ref response) = response {
                if let Ok(ref header) = response.decode_command_custom_header::<SendMessageResponseHeader>() {
                    context.msg_id = header.msg_id().clone();
                    context.queue_id = Some(header.queue_id());
                    context.queue_offset = Some(header.queue_offset());
                    context.code = response.code();
                    context.error_msg = response.remark().cloned().unwrap_or_default();
                }
            }

            hook.send_message_after(context);
        }
    }

    pub(crate) async fn consumer_send_msg_back(
        &mut self,
        _channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        request: &RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ConsumerSendMsgBackRequestHeader>()?;
        if self.broker_runtime_inner.broker_config().broker_identity.broker_id != mix_all::MASTER_ID {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!(
                    "no master available along with {}",
                    self.broker_runtime_inner.broker_config().broker_ip1
                ),
            )));
        }
        let subscription_group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(&request_header.group);
        if subscription_group_config.is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SubscriptionNotExist,
                format!(
                    "subscription group not exist, {} {}",
                    request_header.group,
                    FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                ),
            )));
        }

        if !PermName::is_writeable(self.broker_runtime_inner.broker_config().broker_permission) {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                format!(
                    "the broker[{}-{}] sending message is forbidden",
                    self.broker_runtime_inner.broker_config().broker_identity.broker_name,
                    self.broker_runtime_inner.broker_config().broker_ip1
                ),
            )));
        }

        // SAFETY: subscription_group_config existence checked above
        let subscription_group_config = subscription_group_config.unwrap();

        // Early return: no retry queues configured
        if subscription_group_config.retry_queue_nums() <= 0 {
            return Ok(Some(RemotingCommand::create_remoting_command(ResponseCode::Success)));
        }
        let mut new_topic = CheetahString::from_string(mix_all::get_retry_topic(request_header.group.as_str()));
        let mut queue_id_int = rand::rng().random_range(0..subscription_group_config.retry_queue_nums());
        let topic_sys_flag = if request_header.unit_mode {
            TopicSysFlag::build_sys_flag(false, true)
        } else {
            0
        };
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager_mut()
            .create_topic_in_send_message_back_method(
                &new_topic,
                subscription_group_config.retry_queue_nums(),
                PermName::PERM_WRITE | PermName::PERM_READ,
                false,
                topic_sys_flag,
            )
            .await;
        if topic_config.is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("topic {new_topic} not exist"),
            )));
        }
        // SAFETY: topic_config existence checked above
        let topic_config = topic_config.unwrap();

        // Early return: topic not writable
        if !PermName::is_writeable(topic_config.perm) {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                format!("the topic[{new_topic}] sending message is forbidden"),
            )));
        }
        // Early return: message not found
        let message_store = self
            .broker_runtime_inner
            .message_store()
            .ok_or_else(|| RocketMQError::Internal("Message store not initialized".to_string()))?;

        let msg_ext: Option<MessageExt> = message_store.look_message_by_offset(request_header.offset);
        let Some(mut msg_ext) = msg_ext else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("look message by offset failed, the offset is {}", request_header.offset),
            )));
        };
        let retry_topic = msg_ext.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC));
        if retry_topic.is_none() {
            let topic = msg_ext.get_topic().clone();
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
                .broker_runtime_inner
                .topic_config_manager_mut()
                .create_topic_in_send_message_back_method(
                    &new_topic,
                    retry_config::DLQ_NUMS_PER_GROUP as i32,
                    PermName::PERM_WRITE | PermName::PERM_READ,
                    false,
                    0,
                )
                .await;
            if topic_config_inner.is_none() {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("topic {new_topic} not exist"),
                )));
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
            MessageExtBrokerInner::tags_string_to_tags_code(msg_ext.get_tags().unwrap_or_default().as_str());
        msg_inner.message_ext_inner.queue_id = queue_id_int;
        msg_inner.message_ext_inner.sys_flag = msg_ext.sys_flag;
        msg_inner.message_ext_inner.born_timestamp = msg_ext.born_timestamp;
        msg_inner.message_ext_inner.born_host = msg_ext.born_host;
        msg_inner.message_ext_inner.store_host = self.broker_runtime_inner.store_host();
        msg_inner.message_ext_inner.reconsume_times = msg_ext.reconsume_times + 1;

        let origin_msg_id = if let Some(id) = MessageAccessor::get_origin_message_id(&msg_ext) {
            id
        } else {
            msg_ext.msg_id.clone()
        };
        MessageAccessor::set_origin_message_id(&mut msg_inner, origin_msg_id);
        msg_inner.properties_string = message_properties_to_string(msg_ext.get_properties());

        let inner_topic = msg_inner.get_topic().clone();
        let message_store = self
            .broker_runtime_inner
            .message_store_mut()
            .as_mut()
            .ok_or_else(|| RocketMQError::Internal("Message store not initialized".to_string()))?;
        let put_message_result = message_store.put_message(msg_inner).await;
        let commercial_owner = request
            .get_ext_fields()
            .and_then(|value| value.get(BrokerStatsManager::COMMERCIAL_OWNER).cloned());
        let (response, succeeded) = match put_message_result.put_message_status() {
            PutMessageStatus::PutOk => {
                let mut _back_topic = msg_ext.get_topic().clone();
                let correct_topic =
                    msg_ext.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC));
                if let Some(topic) = correct_topic {
                    _back_topic = topic;
                }

                if TopicValidator::RMQ_SYS_SCHEDULE_TOPIC == inner_topic {
                    //TODO: implement this
                }

                if is_dlq {
                    // TODO: implement this
                }
                (RemotingCommand::create_response_command(), true)
            }

            _ => (
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    put_message_result.put_message_status().to_string(),
                ),
                false,
            ),
        };
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

    pub(crate) fn build_msg_context(
        &self,
        channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        request_header: &mut SendMessageRequestHeader,
        request: &RemotingCommand,
    ) -> SendMessageContext {
        let namespace = NamespaceUtil::get_namespace_from_resource(request_header.topic.as_str());
        let broker_config = self.broker_runtime_inner.broker_config();
        let region_id = broker_config.region_id().to_string();

        let mut send_message_context = SendMessageContext {
            namespace: CheetahString::from_string(namespace),
            producer_group: request_header.producer_group.clone(),
            ..Default::default()
        };
        send_message_context.topic(request_header.topic.clone());
        send_message_context.body_length(request.body().as_ref().map_or_else(|| 0, |b| b.len() as i32));
        send_message_context.msg_props(request_header.properties.clone().unwrap_or_default());
        send_message_context.born_host(CheetahString::from_string(channel.remote_address().to_string()));
        send_message_context.broker_addr(CheetahString::from_string(broker_config.get_broker_addr()));
        send_message_context.queue_id(Some(request_header.queue_id));
        send_message_context.broker_region_id(CheetahString::from_string(region_id.clone()));
        send_message_context.born_time_stamp(request_header.born_timestamp);
        send_message_context.request_time_stamp(TimeUtils::get_current_millis() as i64);

        if let Some(owner) = request.ext_fields() {
            if let Some(value) = owner.get(BrokerStatsManager::COMMERCIAL_OWNER) {
                send_message_context.commercial_owner(value.clone());
            }
        }
        let mut properties = MessageDecoder::string_to_message_properties(request_header.properties.as_ref());
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION),
            CheetahString::from_string(region_id),
        );
        properties.insert(
            CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_SWITCH),
            CheetahString::from_string(self.broker_runtime_inner.broker_config().trace_on.to_string()),
        );
        request_header.properties = Some(MessageDecoder::message_properties_to_string(&properties));

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
        send_message_context
    }

    pub(crate) async fn msg_check(
        &mut self,
        channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        _request: &RemotingCommand,
        request_header: &SendMessageRequestHeader,
        response: &mut RemotingCommand,
    ) {
        //check broker permission
        if !PermName::is_writeable(self.broker_runtime_inner.broker_config().broker_permission())
            && self
                .broker_runtime_inner
                .topic_config_manager()
                .is_order_topic(request_header.topic.as_str())
        {
            response.with_code(ResponseCode::NoPermission);
            response.with_remark(format!(
                "the broker[{}] sending message is forbidden",
                self.broker_runtime_inner.broker_config().broker_ip1.clone()
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
        let mut topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic);
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
                channel.remote_address(),
            );
            topic_config = self
                .broker_runtime_inner
                .topic_config_manager_mut()
                .create_topic_in_send_message_method(
                    &request_header.topic,
                    &request_header.default_topic,
                    channel.remote_address(),
                    request_header.default_topic_queue_nums,
                    topic_sys_flag,
                )
                .await;

            if topic_config.is_none() && request_header.topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) {
                topic_config = self
                    .broker_runtime_inner
                    .topic_config_manager_mut()
                    .create_topic_in_send_message_back_method(
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
                queue_id_int,
                topic_config_inner,
                channel.remote_address()
            ));
        }
    }

    #[inline]
    pub(crate) fn random_queue_id(&self, write_queue_nums: u32) -> u32 {
        rand::rng().random_range(0..=queue_config::RANDOM_QUEUE_RANGE) % write_queue_nums
    }
}

fn rewrite_response_for_static_topic(
    response_header: &mut SendMessageResponseHeader,
    mapping_context: &TopicQueueMappingContext,
) -> Option<RemotingCommand> {
    // Early return: no mapping detail
    let mapping_detail = mapping_context.mapping_detail.as_ref()?;

    // Early return: no leader item
    let Some(mapping_item) = mapping_context.leader_item.as_ref() else {
        return Some(RemotingCommand::create_response_command_with_code_remark(
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
