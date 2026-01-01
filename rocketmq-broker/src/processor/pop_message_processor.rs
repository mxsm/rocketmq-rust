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

#![allow(unused_variables)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rand::Rng;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::consume_init_mode::ConsumeInitMode;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::base::select_result::SelectMappedBufferResult;
use rocketmq_store::filter::MessageFilter;
use rocketmq_store::pop::batch_ack_msg::BatchAckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use rocketmq_store::pop::AckMessage;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::filter::expression_message_filter::ExpressionMessageFilter;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingService;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;
use crate::processor::processor_service::pop_buffer_merge_service::PopBufferMergeService;

const BORN_TIME: &str = "bornTime";

pub struct PopMessageProcessor<MS: MessageStore> {
    ck_message_number: AtomicI64,
    pop_long_polling_service: ArcMut<PopLongPollingService<MS, PopMessageProcessor<MS>>>,
    pop_buffer_merge_service: ArcMut<PopBufferMergeService<MS>>,
    queue_lock_manager: QueueLockManager,
    revive_topic: CheetahString,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> PopMessageProcessor<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let revive_topic = CheetahString::from_string(PopAckConstants::build_cluster_revive_topic(
            broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_cluster_name
                .as_str(),
        ));
        let queue_lock_manager = QueueLockManager::new();
        PopMessageProcessor {
            ck_message_number: Default::default(),
            pop_long_polling_service: ArcMut::new(PopLongPollingService::new(broker_runtime_inner.clone(), false)),
            pop_buffer_merge_service: ArcMut::new(PopBufferMergeService::new(
                revive_topic.clone(),
                queue_lock_manager.clone(),
                broker_runtime_inner.clone(),
            )),

            queue_lock_manager,
            revive_topic,

            broker_runtime_inner,
        }
    }

    pub fn new_arc_mut(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> ArcMut<Self> {
        let revive_topic = CheetahString::from_string(PopAckConstants::build_cluster_revive_topic(
            broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_cluster_name
                .as_str(),
        ));
        let queue_lock_manager = QueueLockManager::new();
        let processor = PopMessageProcessor {
            ck_message_number: Default::default(),
            pop_long_polling_service: ArcMut::new(PopLongPollingService::new(broker_runtime_inner.clone(), false)),
            pop_buffer_merge_service: ArcMut::new(PopBufferMergeService::new(
                revive_topic.clone(),
                queue_lock_manager.clone(),
                broker_runtime_inner.clone(),
            )),

            queue_lock_manager,
            revive_topic,

            broker_runtime_inner,
        };
        let mut processor_inner = ArcMut::new(processor);
        let cloned = processor_inner.clone();
        processor_inner.pop_long_polling_service.set_processor(cloned);
        processor_inner
    }

    pub fn start(&mut self) {
        PopLongPollingService::start(self.pop_long_polling_service.clone());
        PopBufferMergeService::start(self.pop_buffer_merge_service.clone());
        self.queue_lock_manager.start();
    }
}

impl<MS> RequestProcessor for PopMessageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        self._process_request(channel, ctx, request_code, request).await
    }
}

impl<MS> PopMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub async fn _process_request(
        &mut self,
        mut channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let begin_time_mills = get_current_millis();
        request.add_ext_field_if_not_exist(CheetahString::from_static_str(BORN_TIME), begin_time_mills.to_string());

        if request
            .get_ext_fields()
            .and_then(|fields| fields.get(BORN_TIME).cloned())
            .is_none_or(|old| old == "0")
        {
            request.add_ext_field(CheetahString::from_static_str(BORN_TIME), begin_time_mills.to_string());
        }
        let opaque = request.opaque();
        let request_header = request.decode_command_custom_header::<PopMessageRequestHeader>()?;

        self.broker_runtime_inner
            .consumer_manager()
            .compensate_basic_consumer_info(
                &request_header.consumer_group,
                ConsumeType::ConsumePop,
                MessageModel::Clustering,
            );

        if request_header.is_timeout_too_much() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::PollingTimeout,
                format!(
                    "the broker[{}] pop message is timeout too much",
                    self.broker_runtime_inner.broker_config().broker_ip1
                ),
            )));
        }

        if !PermName::is_readable(self.broker_runtime_inner.broker_config().broker_permission) {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                format!(
                    "the broker[{}] pop message is forbidden",
                    self.broker_runtime_inner.broker_config().broker_ip1
                ),
            )));
        }

        if request_header.max_msg_nums > 32 {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!(
                    "the broker[{}] pop message's num is greater than 32",
                    self.broker_runtime_inner.broker_config().broker_ip1
                ),
            )));
        }

        if !self.broker_runtime_inner.message_store_config().timer_wheel_enable {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!(
                    "the broker[{}] pop message is forbidden because timerWheelEnable is false",
                    self.broker_runtime_inner.broker_config().broker_ip1
                ),
            )));
        }
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::TopicNotExist,
                format!(
                    "topic[{}] not exist, apply first please! {}",
                    request_header.topic,
                    FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                ),
            )));
        }
        let topic_config = topic_config.unwrap();
        if !PermName::is_readable(topic_config.perm) {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                format!("the topic[{}] peeking message is forbidden", request_header.topic),
            )));
        }
        if request_header.queue_id >= topic_config.read_queue_nums as i32 {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!(
                    "the queueId[{}] is illegal, topic[{}]  topicConfig readQueueNums[{}] consumer[{}]",
                    request_header.queue_id,
                    request_header.topic,
                    topic_config.read_queue_nums,
                    channel.remote_address()
                ),
            )));
        }
        let subscription_group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(&request_header.consumer_group);
        if subscription_group_config.is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SubscriptionGroupNotExist,
                format!(
                    "the consumer group[{}] not online, apply first please! {}",
                    request_header.consumer_group,
                    FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                ),
            )));
        }
        let subscription_group_config = subscription_group_config.unwrap();
        if !subscription_group_config.consume_enable() {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoPermission,
                format!(
                    "the consumer group[{}], not permitted to consume",
                    request_header.consumer_group
                ),
            )));
        }

        let exp = request_header.exp.as_ref();

        let (subscription_data, message_filter) = if exp.is_some() && !exp.unwrap().is_empty() {
            let subscription_data = match FilterAPI::build(
                &request_header.topic,
                &request_header.exp.clone().unwrap_or_default(),
                request_header.exp_type.clone(),
            ) {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        "Parse the consumer's subscription[{:?}] error, group: {}",
                        request_header.exp, request_header.consumer_group
                    );
                    return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    )));
                }
            };
            self.broker_runtime_inner.consumer_manager().compensate_subscribe_data(
                &request_header.consumer_group,
                &request_header.topic,
                &subscription_data,
            );
            let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
                &request_header.topic,
                &request_header.consumer_group,
                self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
            ));
            let retry_subscription_data = match FilterAPI::build(
                &retry_topic,
                &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                request_header.exp_type.clone(),
            ) {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        "Parse the consumer's subscription[{:?}] error, group: {}",
                        request_header.exp, request_header.consumer_group
                    );
                    return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    )));
                }
            };
            self.broker_runtime_inner.consumer_manager().compensate_subscribe_data(
                &request_header.consumer_group,
                &retry_topic,
                &retry_subscription_data,
            );
            let message_filter = if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
                let consumer_filter_data = ConsumerFilterManager::build(
                    request_header.consumer_group.clone(),
                    request_header.topic.clone(),
                    request_header.exp.clone(),
                    request_header.exp_type.clone(),
                    get_current_millis(),
                );
                if consumer_filter_data.is_none() {
                    warn!(
                        "Parse the consumer's subscription[{:?}] failed, group: {}",
                        request_header.exp, request_header.consumer_group
                    );
                    return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    )));
                }
                let consumer_filter_data = consumer_filter_data.unwrap();
                let message_filter: Box<dyn MessageFilter> = Box::new(ExpressionMessageFilter::new(
                    Some(subscription_data.clone()),
                    Some(consumer_filter_data),
                    Arc::new(self.broker_runtime_inner.consumer_filter_manager().clone()),
                ));
                Some(message_filter)
            } else {
                None
            };
            (subscription_data, message_filter)
        } else {
            let subscription_data = match FilterAPI::build(
                &request_header.topic,
                &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                Some(CheetahString::from_static_str(ExpressionType::TAG)),
            ) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    )));
                }
            };
            self.broker_runtime_inner.consumer_manager().compensate_subscribe_data(
                &request_header.consumer_group,
                &request_header.topic,
                &subscription_data,
            );
            let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
                &request_header.topic,
                &request_header.consumer_group,
                self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
            ));
            let retry_subscription_data = match FilterAPI::build(
                &retry_topic,
                &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                Some(CheetahString::from_static_str(ExpressionType::TAG)),
            ) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    )));
                }
            };
            self.broker_runtime_inner.consumer_manager().compensate_subscribe_data(
                &request_header.consumer_group,
                &retry_topic,
                &retry_subscription_data,
            );
            (subscription_data, None)
        };

        let revive_qid = if request_header.order.unwrap_or(false) {
            POP_ORDER_REVIVE_QUEUE
        } else {
            let revive_queue_num = self.broker_runtime_inner.broker_config().revive_queue_num as i64;
            let ck_num = self.ck_message_number.fetch_add(1, Ordering::AcqRel);
            ((ck_num % revive_queue_num + revive_queue_num) % revive_queue_num) as i32
        };
        let mut get_message_result =
            ArcMut::new(GetMessageResult::new_result_size(request_header.max_msg_nums as usize));

        // Due to the design of the fields startOffsetInfo, msgOffsetInfo, and orderCountInfo,
        // a single POP request could only invoke the popMsgFromQueue method once
        // for either a normal topic or a retry topic's queue. Retry topics v1 and v2 are
        // considered the same type because they share the same retry flag in previous fields.
        // Therefore, needRetryV1 is designed as a subset of needRetry, and within a single request,
        // only one type of retry topic is able to call popMsgFromQueue.
        //Determine whether to pull a message from the retry queue using a random number and a set
        // probability
        let randomq = rand::rng().random_range(0..100);
        let need_retry = randomq < self.broker_runtime_inner.broker_config().pop_from_retry_probability;
        let mut need_retry_v1 = false;
        if self.broker_runtime_inner.broker_config().enable_retry_topic_v2
            && self
                .broker_runtime_inner
                .broker_config()
                .retrieve_message_from_pop_retry_topic_v1
        {
            need_retry_v1 = randomq % 2 == 0;
        }
        let mut start_offset_info = String::with_capacity(64);
        let mut msg_offset_info = String::with_capacity(64);
        let mut order_count_info = if request_header.order.is_some() {
            String::with_capacity(64)
        } else {
            String::new()
        };
        let pop_time = get_current_millis();

        let message_filter = message_filter.map(Arc::new);
        let mut rest_num = 0; // remaining number of messages to be fetched
        if need_retry && !request_header.order.unwrap_or(false) {
            rest_num = if need_retry_v1 {
                let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic_v1(
                    &request_header.topic,
                    &request_header.consumer_group,
                ));

                self.pop_msg_from_topic_by_name(
                    &retry_topic,
                    true,
                    get_message_result.clone(),
                    &request_header,
                    revive_qid,
                    channel.clone(),
                    pop_time,
                    message_filter.clone(),
                    &mut start_offset_info,
                    &mut msg_offset_info,
                    &mut order_count_info,
                    randomq,
                    rest_num,
                )
                .await
            } else {
                let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
                    &request_header.topic,
                    &request_header.consumer_group,
                    self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
                ));
                self.pop_msg_from_topic_by_name(
                    &retry_topic,
                    true,
                    get_message_result.clone(),
                    &request_header,
                    revive_qid,
                    channel.clone(),
                    pop_time,
                    message_filter.clone(),
                    &mut start_offset_info,
                    &mut msg_offset_info,
                    &mut order_count_info,
                    randomq,
                    rest_num,
                )
                .await
            };
        }
        //request_header.queue_id < 0 means read all queue
        rest_num = if request_header.queue_id < 0 {
            // read all queue
            self.pop_msg_from_topic(
                &topic_config,
                false,
                get_message_result.clone(),
                &request_header,
                revive_qid,
                channel.clone(),
                pop_time,
                message_filter.clone(),
                &mut start_offset_info,
                &mut msg_offset_info,
                &mut order_count_info,
                randomq,
                rest_num,
            )
            .await
        } else {
            self.pop_msg_from_queue(
                &topic_config.topic_name.clone().unwrap_or_default(),
                &request_header.attempt_id.clone().unwrap_or_default(),
                false,
                get_message_result.clone(),
                &request_header,
                request_header.queue_id,
                rest_num,
                revive_qid,
                channel.clone(),
                pop_time,
                message_filter.clone(),
                &mut start_offset_info,
                &mut msg_offset_info,
                &mut order_count_info,
            )
            .await
        };
        // if not full , fetch retry again
        if !need_retry
            && get_message_result.message_mapped_list().len() < request_header.max_msg_nums as usize
            && !request_header.order.unwrap_or(false)
        {
            rest_num = if need_retry_v1 {
                let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic_v1(
                    &request_header.topic,
                    &request_header.consumer_group,
                ));

                self.pop_msg_from_topic_by_name(
                    &retry_topic,
                    true,
                    get_message_result.clone(),
                    &request_header,
                    revive_qid,
                    channel.clone(),
                    pop_time,
                    message_filter.clone(),
                    &mut start_offset_info,
                    &mut msg_offset_info,
                    &mut order_count_info,
                    randomq,
                    rest_num,
                )
                .await
            } else {
                let retry_topic = CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
                    &request_header.topic,
                    &request_header.consumer_group,
                    self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
                ));
                self.pop_msg_from_topic_by_name(
                    &retry_topic,
                    true,
                    get_message_result.clone(),
                    &request_header,
                    revive_qid,
                    channel.clone(),
                    pop_time,
                    message_filter.clone(),
                    &mut start_offset_info,
                    &mut msg_offset_info,
                    &mut order_count_info,
                    randomq,
                    rest_num,
                )
                .await
            };
        }
        let mut final_response = RemotingCommand::create_response_command();
        final_response.set_opaque_mut(opaque);
        if !get_message_result.message_mapped_list().is_empty() {
            get_message_result.set_status(Some(GetMessageStatus::Found));
            if rest_num > 0 {
                // all queue pop can not notify specified queue pop, and vice versa
                self.pop_long_polling_service.notify_message_arriving(
                    &request_header.topic,
                    request_header.queue_id,
                    &request_header.consumer_group,
                    None,
                    0,
                    None,
                    None,
                );
            }
        } else {
            let polling_result = self.pop_long_polling_service.polling(
                ctx.clone(),
                request,
                PollingHeader::new_from_pop_message_request_header(&request_header),
                Some(subscription_data),
                message_filter,
            );
            match polling_result {
                PollingResult::PollingSuc => {
                    if rest_num > 0 {
                        self.pop_long_polling_service.notify_message_arriving(
                            &request_header.topic,
                            request_header.queue_id,
                            &request_header.consumer_group,
                            None,
                            0,
                            None,
                            None,
                        );
                    }
                    return Ok(None);
                }
                PollingResult::PollingFull => {
                    final_response.set_code_ref(ResponseCode::PollingFull);
                }
                _ => {
                    final_response.set_code_ref(ResponseCode::PollingTimeout);
                }
            }
            get_message_result.set_status(Some(GetMessageStatus::NoMessageInQueue));
        }
        let response_header = PopMessageResponseHeader {
            pop_time,
            invisible_time: request_header.invisible_time,
            revive_qid: revive_qid as u32,
            rest_num: rest_num as u64,
            start_offset_info: Some(CheetahString::from_string(start_offset_info)),
            msg_offset_info: Some(CheetahString::from_string(msg_offset_info)),
            order_count_info: if order_count_info.is_empty() {
                None
            } else {
                Some(CheetahString::from_string(order_count_info))
            },
        };
        final_response.set_remark_mut(get_message_result.status().unwrap().to_string());

        match ResponseCode::from(final_response.code()) {
            ResponseCode::Success => {
                if self.broker_runtime_inner.broker_config().transfer_msg_by_heap {
                    if let Some(bytes) = self.read_get_message_result(
                        &get_message_result,
                        &request_header.consumer_group,
                        &request_header.topic,
                        request_header.queue_id,
                    ) {
                        final_response.set_body_mut_ref(bytes);
                    }
                    final_response.set_command_custom_header_ref(response_header);
                    Ok(Some(final_response))
                } else {
                    //zero copy transfer
                    if let Some(header_bytes) =
                        final_response.encode_header_with_body_length(get_message_result.buffer_total_size() as usize)
                    {
                        channel.connection_mut().send_bytes(header_bytes).await?;
                    }
                    for select_result in get_message_result.message_mapped_list_mut() {
                        if let Some(message) = select_result.bytes.take() {
                            channel.connection_mut().send_bytes(message).await?;
                        }
                    }

                    Ok(None)
                }
            }
            _ => Ok(Some(final_response)),
        }
    }

    async fn pop_msg_from_topic(
        &self,
        topic_config: &TopicConfig,
        is_retry: bool,
        get_message_result: ArcMut<GetMessageResult>,
        request_header: &PopMessageRequestHeader,
        revive_qid: i32,
        channel: Channel,
        pop_time: u64,
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
        start_offset_info: &mut String,
        msg_offset_info: &mut String,
        order_count_info: &mut str,
        random_q: i32,
        mut rest_num: i64,
    ) -> i64 {
        for index in 0..topic_config.read_queue_nums {
            let queue_id = (random_q + index as i32) % topic_config.read_queue_nums as i32;
            rest_num = self
                .pop_msg_from_queue(
                    &topic_config.topic_name.clone().unwrap_or_default(),
                    &request_header.consumer_group,
                    is_retry,
                    get_message_result.clone(),
                    request_header,
                    queue_id,
                    rest_num,
                    revive_qid,
                    channel.clone(),
                    pop_time,
                    message_filter.clone(),
                    start_offset_info,
                    msg_offset_info,
                    order_count_info,
                )
                .await;
        }
        rest_num
    }

    async fn pop_msg_from_topic_by_name(
        &self,
        topic: &CheetahString,
        is_retry: bool,
        get_message_result: ArcMut<GetMessageResult>,
        request_header: &PopMessageRequestHeader,
        revive_qid: i32,
        channel: Channel,
        pop_time: u64,
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
        start_offset_info: &mut String,
        msg_offset_info: &mut String,
        order_count_info: &mut str,
        random_q: i32,
        rest_num: i64,
    ) -> i64 {
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(topic);
        if topic_config.is_none() {
            return rest_num;
        }
        self.pop_msg_from_topic(
            &topic_config.unwrap(),
            is_retry,
            get_message_result,
            request_header,
            revive_qid,
            channel,
            pop_time,
            message_filter,
            start_offset_info,
            msg_offset_info,
            order_count_info,
            random_q,
            rest_num,
        )
        .await
    }

    async fn pop_msg_from_queue(
        &self,
        topic: &CheetahString,
        attempt_id: &CheetahString,
        is_retry: bool,
        mut get_message_result: ArcMut<GetMessageResult>,
        request_header: &PopMessageRequestHeader,
        queue_id: i32,
        rest_num: i64,
        revive_qid: i32,
        channel: Channel,
        pop_time: u64,
        message_filter: Option<Arc<Box<dyn MessageFilter>>>,
        start_offset_info: &mut String,
        msg_offset_info: &mut String,
        order_count_info: &mut str,
    ) -> i64 {
        let lock_key = CheetahString::from_string(format!(
            "{}{}{}{}{}",
            topic,
            PopAckConstants::SPLIT,
            request_header.consumer_group,
            PopAckConstants::SPLIT,
            queue_id
        ));
        let offset = self
            .get_pop_offset(
                topic,
                &request_header.consumer_group,
                queue_id,
                request_header.init_mode,
                false,
                &lock_key,
                false,
            )
            .await;
        if !self.queue_lock_manager().try_lock_with_key(lock_key.clone()).await {
            let message_store = match self.broker_runtime_inner.message_store() {
                Some(store) => store,
                None => {
                    warn!(
                        "Message store not initialized, topic={}, group={}",
                        topic, request_header.consumer_group
                    );
                    return rest_num;
                }
            };
            return message_store.get_max_offset_in_queue(topic, queue_id) - offset + rest_num;
        }
        if self.is_pop_should_stop(topic, &request_header.consumer_group, queue_id) {
            let message_store = match self.broker_runtime_inner.message_store() {
                Some(store) => store,
                None => {
                    warn!(
                        "Message store not initialized, topic={}, group={}",
                        topic, request_header.consumer_group
                    );
                    return rest_num;
                }
            };
            let result = message_store.get_max_offset_in_queue(topic, queue_id) - offset + rest_num;
            self.queue_lock_manager().unlock_with_key(lock_key.clone()).await;
            return result;
        }
        let offset = self
            .get_pop_offset(
                topic,
                &request_header.consumer_group,
                queue_id,
                request_header.init_mode,
                true,
                &lock_key,
                true,
            )
            .await;
        let is_order = request_header.order.unwrap_or(false);
        if is_order {
            if self.broker_runtime_inner.consumer_order_info_manager().check_block(
                attempt_id,
                topic,
                &request_header.consumer_group,
                queue_id,
                request_header.invisible_time,
            ) {
                self.queue_lock_manager().unlock_with_key(lock_key.clone()).await;
                return rest_num;
            }
            self.broker_runtime_inner
                .pop_inflight_message_counter()
                .clear_in_flight_message_num(topic, &request_header.consumer_group, queue_id);
        }

        if get_message_result.message_mapped_list().len() >= request_header.max_msg_nums as usize {
            let message_store = match self.broker_runtime_inner.message_store() {
                Some(store) => store,
                None => {
                    warn!(
                        "Message store not initialized, topic={}, group={}",
                        topic, request_header.consumer_group
                    );
                    self.queue_lock_manager().unlock_with_key(lock_key.clone()).await;
                    return rest_num;
                }
            };
            let result = message_store.get_max_offset_in_queue(topic, queue_id) - offset + rest_num;
            self.queue_lock_manager().unlock_with_key(lock_key.clone()).await;
            return result;
        }
        let message_store = match self.broker_runtime_inner.message_store() {
            Some(store) => store,
            None => {
                warn!(
                    "Message store not initialized, topic={}, group={}",
                    topic, request_header.consumer_group
                );
                self.queue_lock_manager().unlock_with_key(lock_key.clone()).await;
                return rest_num;
            }
        };
        let get_message_result_inner = message_store
            .get_message(
                &request_header.consumer_group,
                topic,
                queue_id,
                offset,
                request_header.max_msg_nums as i32 - get_message_result.message_mapped_list().len() as i32,
                message_filter.clone(),
            )
            .await;
        let atomic_rest_num = AtomicI64::new(rest_num);
        let atomic_offset = AtomicI64::new(offset);
        let final_offset = offset;

        let result = match get_message_result_inner {
            None => None,
            Some(value) => {
                if value.status().is_none() {
                    Some(value)
                } else {
                    match value.status().unwrap() {
                        GetMessageStatus::OffsetFoundNull
                        | GetMessageStatus::OffsetOverflowBadly
                        | GetMessageStatus::OffsetTooSmall => {
                            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                                channel.remote_address().to_string().into(),
                                &request_header.consumer_group,
                                topic,
                                queue_id,
                                value.next_begin_offset(),
                            );
                            atomic_offset.store(value.next_begin_offset(), Ordering::Release);
                            match self.broker_runtime_inner.message_store() {
                                Some(store) => {
                                    store
                                        .get_message(
                                            &request_header.consumer_group,
                                            topic,
                                            queue_id,
                                            offset,
                                            request_header.max_msg_nums as i32
                                                - get_message_result.message_mapped_list().len() as i32,
                                            message_filter,
                                        )
                                        .await
                                }
                                None => {
                                    warn!(
                                        "Message store not initialized during offset adjustment, topic={}, group={}",
                                        topic, request_header.consumer_group
                                    );
                                    Some(value)
                                }
                            }
                        }
                        _ => Some(value),
                    }
                }
            }
        };
        match result {
            None => {
                let message_store = match self.broker_runtime_inner.message_store() {
                    Some(store) => store,
                    None => {
                        warn!(
                            "Message store not initialized, topic={}, group={}",
                            topic, request_header.consumer_group
                        );
                        return atomic_rest_num.load(Ordering::Acquire);
                    }
                };
                let num = message_store.get_max_offset_in_queue(topic, queue_id)
                    - atomic_offset.load(Ordering::Acquire)
                    + atomic_rest_num.load(Ordering::Acquire);
                atomic_rest_num.store(num, Ordering::Release);
            }
            Some(result_inner) => {
                if !result_inner.message_mapped_list().is_empty() {
                    if is_order {
                        self.broker_runtime_inner.consumer_order_info_manager().update(
                            request_header.attempt_id.clone().unwrap_or_default(),
                            is_retry,
                            topic,
                            &request_header.consumer_group,
                            queue_id,
                            pop_time,
                            request_header.invisible_time,
                            result_inner.message_queue_offset().clone(),
                            order_count_info,
                        );
                        self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                            channel.remote_address().to_string().into(),
                            &request_header.consumer_group,
                            topic,
                            queue_id,
                            final_offset,
                        );
                    } else if !self
                        .append_check_point(
                            request_header,
                            topic,
                            revive_qid,
                            queue_id,
                            final_offset,
                            &result_inner,
                            pop_time,
                            self.broker_runtime_inner.broker_config().broker_name(),
                        )
                        .await
                    {
                        self.queue_lock_manager().unlock_with_key(lock_key).await;
                        return atomic_rest_num.load(Ordering::Acquire) + result_inner.message_count() as i64;
                    }
                    ExtraInfoUtil::build_start_offset_info(start_offset_info, topic, queue_id, final_offset);
                    ExtraInfoUtil::build_msg_offset_info(
                        msg_offset_info,
                        topic,
                        queue_id,
                        result_inner.message_queue_offset().as_slice(),
                    );
                    if self.broker_runtime_inner.broker_config().enable_pop_log {
                        info!(
                            topic = %topic,
                            group = %request_header.consumer_group,
                            queue_id = queue_id,
                            msg_count = result_inner.message_count(),
                            start_offset = final_offset,
                            "PopMessage succeeded"
                        );
                    }
                } else if let Some(status) = result_inner.status() {
                    if matches!(
                        status,
                        GetMessageStatus::NoMatchedMessage
                            | GetMessageStatus::OffsetFoundNull
                            | GetMessageStatus::MessageWasRemoving
                            | GetMessageStatus::NoMatchedLogicQueue
                    ) && result_inner.next_begin_offset() > -1
                    {
                        if is_order {
                            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                                channel.remote_address().to_string().into(),
                                &request_header.consumer_group,
                                topic,
                                queue_id,
                                result_inner.next_begin_offset(),
                            );
                        } else {
                            self.pop_buffer_merge_service
                                .mut_from_ref()
                                .add_ck_mock(
                                    request_header.consumer_group.clone(),
                                    topic.clone(),
                                    queue_id,
                                    final_offset as u64,
                                    request_header.invisible_time,
                                    pop_time,
                                    revive_qid,
                                    result_inner.next_begin_offset() as u64,
                                    self.broker_runtime_inner.broker_config().broker_name().clone(),
                                )
                                .await;
                        }
                    }
                }

                atomic_rest_num.fetch_add(
                    result_inner.max_offset() - result_inner.next_begin_offset(),
                    Ordering::AcqRel,
                );
                let broker_name = self.broker_runtime_inner.broker_config().broker_name().as_str();
                let message_count = result_inner.message_count();
                for maped_buffer in result_inner.message_mapped_vec() {
                    if self
                        .broker_runtime_inner
                        .broker_config()
                        .pop_response_return_actual_retry_topic
                        || !is_retry
                    {
                        get_message_result.add_message_inner(maped_buffer);
                    } else {
                        let mut bytes = maped_buffer.get_bytes().unwrap_or_default();
                        let message_ext_list = message_decoder::decodes_batch(&mut bytes, true, false);
                        //maped_buffer.release();
                        for mut message_ext in message_ext_list {
                            let ck_info = ExtraInfoUtil::build_extra_info_with_offset(
                                final_offset,
                                pop_time as i64,
                                request_header.invisible_time as i64,
                                revive_qid,
                                message_ext.get_topic(),
                                broker_name,
                                message_ext.queue_id(),
                                message_ext.queue_offset(),
                            );
                            message_ext
                                .message
                                .properties
                                .entry(CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
                                .or_insert(ck_info.into());

                            message_ext.set_topic(request_header.topic.clone());
                            message_ext.set_store_size(0);

                            let encode = message_decoder::encode(&message_ext, false).unwrap();
                            let tmp_result = SelectMappedBufferResult {
                                start_offset: maped_buffer.start_offset,
                                size: encode.len() as i32,
                                bytes: Some(encode),
                                mapped_file: None,
                                is_in_cache: true,
                            };
                            get_message_result.add_message_inner(tmp_result);
                        }
                    }
                }
                self.broker_runtime_inner
                    .pop_inflight_message_counter()
                    .increment_in_flight_message_num(
                        topic,
                        &request_header.consumer_group,
                        queue_id,
                        message_count as i64,
                    );
            }
        }

        self.queue_lock_manager().unlock_with_key(lock_key).await;
        atomic_rest_num.load(Ordering::Acquire)
    }

    /// Appends a checkpoint for a pop message. And wait ack.
    ///
    /// This function creates a `PopCheckPoint` and adds it to the `PopBufferMergeService`.
    /// It ensures that the checkpoint is added correctly and handles retries if necessary.
    ///
    /// # Arguments
    ///
    /// * `request_header` - The header of the pop message request.
    /// * `topic` - The topic of the message.
    /// * `revive_qid` - The revive queue ID.
    /// * `queue_id` - The queue ID.
    /// * `offset` - The offset of the message. This is the start offset of the message.
    /// * `get_message_tmp_result` - The result of the get message operation.
    /// * `pop_time` - The time the message was popped.
    /// * `broker_name` - The name of the broker.
    ///
    /// # Returns
    ///
    /// * `bool` - Returns `true` if the checkpoint was added successfully, `false` otherwise.
    async fn append_check_point(
        &self,
        request_header: &PopMessageRequestHeader,
        topic: &str,
        revive_qid: i32,
        queue_id: i32,
        offset: i64,
        get_message_tmp_result: &GetMessageResult,
        pop_time: u64,
        broker_name: &CheetahString,
    ) -> bool {
        let mut ck = PopCheckPoint {
            start_offset: offset,
            pop_time: pop_time as i64,
            invisible_time: request_header.invisible_time as i64,
            bit_map: 0,
            num: get_message_tmp_result.message_mapped_list().len() as u8,
            queue_id,
            topic: topic.into(),
            cid: request_header.consumer_group.clone(),
            broker_name: Some(broker_name.clone()),
            ..Default::default()
        };
        for msg_queue_offset in get_message_tmp_result.message_queue_offset() {
            ck.add_diff(((*msg_queue_offset) as i64 - offset) as i32);
        }
        let pop_buffer_merge_service_ref_mut = self.pop_buffer_merge_service.mut_from_ref();

        match pop_buffer_merge_service_ref_mut
            .add_ck(&ck, revive_qid, -1, get_message_tmp_result.next_begin_offset())
            .await
        {
            Ok(_) => true,
            Err(_) => pop_buffer_merge_service_ref_mut
                .add_ck_just_offset(ck, revive_qid, -1, get_message_tmp_result.next_begin_offset())
                .await
                .is_ok(),
        }
    }

    fn is_pop_should_stop(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32) -> bool {
        let broker_config = self.broker_runtime_inner.broker_config();
        broker_config.enable_pop_message_threshold
            && self
                .broker_runtime_inner
                .pop_inflight_message_counter()
                .get_group_pop_in_flight_message_num(topic, group, queue_id)
                > broker_config.pop_inflight_message_threshold
    }

    async fn get_pop_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        init_mode: i32,
        init: bool,
        lock_key: &CheetahString,
        check_reset_offset: bool,
    ) -> i64 {
        let mut offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(group, topic, queue_id);
        if offset < 0 {
            offset = self.get_init_offset(topic, group, queue_id, init_mode, init);
        }

        if check_reset_offset {
            if let Some(reset_offset) = self.reset_pop_offset(topic, group, queue_id) {
                return reset_offset;
            }
        }

        let buffer_offset = self.pop_buffer_merge_service.get_latest_offset(lock_key).await;
        if buffer_offset < 0 {
            offset
        } else {
            std::cmp::max(buffer_offset, offset)
        }
    }

    fn get_init_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        init_mode: i32,
        init: bool,
    ) -> i64 {
        let mut offset;
        if init_mode == ConsumeInitMode::MIN || topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            offset = self
                .broker_runtime_inner
                .message_store()
                .unwrap()
                .get_min_offset_in_queue(topic, queue_id);
        } else if self
            .broker_runtime_inner
            .broker_config()
            .init_pop_offset_by_check_msg_in_mem
            && self
                .broker_runtime_inner
                .message_store()
                .unwrap()
                .get_min_offset_in_queue(topic, queue_id)
                <= 0
            && self
                .broker_runtime_inner
                .message_store()
                .unwrap()
                .check_in_mem_by_consume_offset(topic, queue_id, 0, 1)
        {
            offset = 0;
        } else {
            offset = self
                .broker_runtime_inner
                .message_store()
                .unwrap()
                .get_max_offset_in_queue(topic, queue_id)
                - 1;
            if offset < 0 {
                offset = 0;
            }
        }

        // whichever initMode
        if init {
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                "getPopOffset".into(),
                group,
                topic,
                queue_id,
                offset,
            );
        }
        offset
    }

    fn reset_pop_offset(&self, topic: &CheetahString, group: &CheetahString, queue_id: i32) -> Option<i64> {
        let lock_key = CheetahString::from_string(format!(
            "{}{}{}{}{}",
            topic,
            PopAckConstants::SPLIT,
            group,
            PopAckConstants::SPLIT,
            queue_id
        ));
        let reset_offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_then_erase_reset_offset(group, topic, queue_id);
        if let Some(value) = &reset_offset {
            self.broker_runtime_inner
                .consumer_order_info_manager()
                .clear_block(topic, group, queue_id);
            self.pop_buffer_merge_service.clear_offset_queue(&lock_key);
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                "ResetPopOffset".into(),
                group,
                topic,
                queue_id,
                *value,
            )
        }
        reset_offset
    }

    pub fn queue_lock_manager(&self) -> &QueueLockManager {
        &self.queue_lock_manager
    }

    /// Get the pop long polling service
    ///
    /// # Returns
    /// A reference to the PopLongPollingService instance
    #[inline]
    pub fn pop_long_polling_service(&self) -> Option<&ArcMut<PopLongPollingService<MS, PopMessageProcessor<MS>>>> {
        Some(&self.pop_long_polling_service)
    }

    pub fn notify_message_arriving(&self, topic: &CheetahString, queue_id: i32, cid: &CheetahString) {
        self.pop_long_polling_service
            .notify_message_arriving(topic, queue_id, cid, None, 0, None, None);
    }

    pub fn notify_message_arriving_full(
        &self,
        topic: CheetahString,
        queue_id: i32,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        self.pop_long_polling_service
            .notify_message_arriving_with_retry_topic_full(
                topic,
                queue_id,
                tags_code,
                msg_store_time,
                filter_bit_map,
                properties,
            );
    }

    fn read_get_message_result(
        &self,
        get_message_result: &GetMessageResult,
        _group: &str,
        _topic: &str,
        _queue_id: i32,
    ) -> Option<Bytes> {
        let mut bytes_mut = BytesMut::with_capacity(get_message_result.buffer_total_size() as usize);
        for msg in get_message_result.message_mapped_list() {
            let data = &msg.mapped_file.as_ref().unwrap().get_mapped_file()
                [msg.start_offset as usize..(msg.start_offset + msg.size as u64) as usize];
            bytes_mut.extend_from_slice(data);
        }
        Some(bytes_mut.freeze())
    }

    pub fn shutdown(&mut self) {
        self.pop_long_polling_service.shutdown();
        self.queue_lock_manager.shutdown();
    }
}

impl<MS: MessageStore> PopMessageProcessor<MS> {
    pub fn gen_ack_unique_id(ack_msg: &dyn AckMessage) -> String {
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}",
            ack_msg.topic(),
            PopAckConstants::SPLIT,
            ack_msg.queue_id(),
            PopAckConstants::SPLIT,
            ack_msg.ack_offset(),
            PopAckConstants::SPLIT,
            ack_msg.consumer_group(),
            PopAckConstants::SPLIT,
            ack_msg.pop_time(),
            PopAckConstants::SPLIT,
            ack_msg.broker_name(),
            PopAckConstants::SPLIT,
            PopAckConstants::ACK_TAG
        )
    }

    pub fn gen_batch_ack_unique_id(batch_ack_msg: &BatchAckMsg) -> String {
        format!(
            "{}{}{}{}{:?}{}{}{}{}{}{}",
            batch_ack_msg.ack_msg.topic,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_msg.queue_id,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_offset_list,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_msg.consumer_group,
            PopAckConstants::SPLIT,
            batch_ack_msg.ack_msg.pop_time,
            PopAckConstants::SPLIT,
            PopAckConstants::BATCH_ACK_TAG
        )
    }

    pub fn gen_ck_unique_id(ck: &PopCheckPoint) -> String {
        format!(
            "{}{}{}{}{}{}{}{}{}{}{}{}{}",
            ck.topic,
            PopAckConstants::SPLIT,
            ck.queue_id,
            PopAckConstants::SPLIT,
            ck.start_offset,
            PopAckConstants::SPLIT,
            ck.cid,
            PopAckConstants::SPLIT,
            ck.pop_time,
            PopAckConstants::SPLIT,
            ck.broker_name.as_ref().map_or("null".to_string(), |x| x.to_string()),
            PopAckConstants::SPLIT,
            PopAckConstants::CK_TAG
        )
    }

    pub fn pop_buffer_merge_service(&self) -> &ArcMut<PopBufferMergeService<MS>> {
        &self.pop_buffer_merge_service
    }

    pub fn pop_buffer_merge_service_mut(&mut self) -> &mut ArcMut<PopBufferMergeService<MS>> {
        &mut self.pop_buffer_merge_service
    }

    pub fn build_ck_msg(
        store_host: SocketAddr,
        ck: &PopCheckPoint,
        revive_qid: i32,
        revive_topic: CheetahString,
    ) -> MessageExtBrokerInner {
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(revive_topic);
        msg.set_body(Bytes::from(ck.serialize_json().unwrap()));
        msg.message_ext_inner.queue_id = revive_qid;
        msg.set_tags(CheetahString::from_static_str(PopAckConstants::CK_TAG));
        msg.message_ext_inner.born_timestamp = get_current_millis() as i64;
        msg.message_ext_inner.born_host = store_host;
        msg.message_ext_inner.store_host = store_host;
        msg.set_delay_time_ms((ck.get_revive_time() - PopAckConstants::ACK_TIME_INTERVAL) as u64);
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_string(PopMessageProcessor::<MS>::gen_ck_unique_id(ck)),
        );
        msg.properties_string = message_decoder::message_properties_to_string(msg.get_properties());
        msg
    }
}

struct TimedLock {
    lock: AtomicBool,
    lock_time: AtomicU64,
}

impl TimedLock {
    pub fn new() -> Self {
        TimedLock {
            lock: AtomicBool::new(false),
            lock_time: AtomicU64::new(get_current_millis()),
        }
    }

    pub fn try_lock(&self) -> bool {
        match self
            .lock
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => {
                self.lock_time.store(get_current_millis(), Ordering::Relaxed);
                true
            }
            Err(_) => false,
        }
    }

    pub fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }

    pub fn is_locked(&self) -> bool {
        self.lock.load(Ordering::Acquire)
    }

    pub fn get_lock_time(&self) -> u64 {
        self.lock_time.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct QueueLockManager {
    expired_local_cache: Arc<RwLock<HashMap<CheetahString, TimedLock>>>,
    shutdown: Arc<Notify>,
}

impl QueueLockManager {
    pub fn new() -> Self {
        QueueLockManager {
            expired_local_cache: Arc::new(RwLock::new(HashMap::with_capacity(4096))),
            shutdown: Arc::new(Notify::new()),
        }
    }

    #[inline]
    pub fn build_lock_key(topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> String {
        format!(
            "{}{}{}{}{}",
            topic,
            PopAckConstants::SPLIT,
            consumer_group,
            PopAckConstants::SPLIT,
            queue_id
        )
    }

    pub async fn try_lock(&self, topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> bool {
        let key = Self::build_lock_key(topic, consumer_group, queue_id);
        self.try_lock_with_key(CheetahString::from_string(key)).await
    }

    pub async fn try_lock_with_key(&self, key: CheetahString) -> bool {
        let cache = self.expired_local_cache.read().await;
        if let Some(lock) = cache.get(&key) {
            return lock.try_lock();
        }
        drop(cache);
        let mut cache = self.expired_local_cache.write().await;
        let lock = cache.entry(key).or_insert(TimedLock::new());
        lock.try_lock()
    }

    pub async fn unlock(&self, topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) {
        let key = Self::build_lock_key(topic, consumer_group, queue_id);
        self.unlock_with_key(CheetahString::from_string(key)).await;
    }

    pub async fn unlock_with_key(&self, key: CheetahString) {
        let cache = self.expired_local_cache.read().await;
        if let Some(lock) = cache.get(&key) {
            lock.unlock();
        }
    }

    pub async fn clean_unused_locks(&self, used_expire_millis: u64) -> usize {
        let mut cache = self.expired_local_cache.write().await;
        let count = cache.len();
        cache.retain(|_, lock| get_current_millis() - lock.get_lock_time() <= used_expire_millis);
        count
    }

    pub fn start(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = this.shutdown.notified() => {
                        break;
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {}
                }
                let count = this.clean_unused_locks(60000).await;
                info!("QueueLockSize={}", count);
            }
        });
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_store::message_store::local_file_message_store::LocalFileMessageStore;
    use rocketmq_store::pop::ack_msg::AckMsg;

    use super::*;

    #[test]
    fn gen_ack_unique_id_formats_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let result = PopMessageProcessor::<LocalFileMessageStore>::gen_ack_unique_id(&ack_msg);
        let expected = "test_topic@1@123@test_group@789@test_broker@ack";
        assert_eq!(result, expected);
    }

    #[test]
    fn gen_batch_ack_unique_id_formats_correctly() {
        let ack_msg = AckMsg {
            ack_offset: 123,
            start_offset: 456,
            consumer_group: CheetahString::from_static_str("test_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            pop_time: 789,
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let batch_ack_msg = BatchAckMsg {
            ack_msg,
            ack_offset_list: vec![1, 2, 3],
        };
        let result = PopMessageProcessor::<LocalFileMessageStore>::gen_batch_ack_unique_id(&batch_ack_msg);
        let expected = "test_topic@1@[1, 2, 3]@test_group@789@bAck";
        assert_eq!(result, expected);
    }

    #[test]
    fn gen_ck_unique_id_formats_correctly() {
        let ck = PopCheckPoint {
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            start_offset: 456,
            cid: CheetahString::from("test_cid"),
            revive_offset: 0,
            pop_time: 789,
            invisible_time: 0,
            bit_map: 0,
            broker_name: Some(CheetahString::from("test_broker")),
            num: 0,
            queue_offset_diff: vec![],
            re_put_times: None,
        };
        let result = PopMessageProcessor::<LocalFileMessageStore>::gen_ck_unique_id(&ck);
        let expected = "test_topic@1@456@test_cid@789@test_broker@ck";
        assert_eq!(result, expected);
    }

    #[test]
    fn new_timed_lock_is_unlocked() {
        let lock = TimedLock::new();
        assert!(!lock.is_locked());
    }

    #[test]
    fn try_lock_locks_successfully() {
        let lock = TimedLock::new();
        assert!(lock.try_lock());
        assert!(lock.is_locked());
    }

    #[test]
    fn try_lock_fails_when_already_locked() {
        let lock = TimedLock::new();
        lock.try_lock();
        assert!(!lock.try_lock());
    }

    #[test]
    fn unlock_unlocks_successfully() {
        let lock = TimedLock::new();
        lock.try_lock();
        lock.unlock();
        assert!(!lock.is_locked());
    }

    #[test]
    fn get_lock_time_returns_correct_time() {
        let lock = TimedLock::new();
        let initial_time = lock.get_lock_time();
        lock.try_lock();
        let lock_time = lock.get_lock_time();
        assert!(lock_time >= initial_time);
    }

    #[tokio::test]
    async fn new_queue_lock_manager_has_empty_cache() {
        let manager = QueueLockManager::new();
        let cache = manager.expired_local_cache.read().await;
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn build_lock_key_formats_correctly() {
        let topic = CheetahString::from_static_str("test_topic");
        let consumer_group = CheetahString::from_static_str("test_group");
        let queue_id = 1;
        let key = QueueLockManager::build_lock_key(&topic, &consumer_group, queue_id);
        let expected = "test_topic@test_group@1";
        assert_eq!(key, expected);
    }

    #[tokio::test]
    async fn try_lock_locks_successfully1() {
        let manager = QueueLockManager::new();
        let topic = CheetahString::from_static_str("test_topic");
        let consumer_group = CheetahString::from_static_str("test_group");
        let queue_id = 1;
        assert!(manager.try_lock(&topic, &consumer_group, queue_id).await);
    }

    #[tokio::test]
    async fn try_lock_fails_when_already_locked1() {
        let manager = QueueLockManager::new();
        let topic = CheetahString::from_static_str("test_topic");
        let consumer_group = CheetahString::from_static_str("test_group");
        let queue_id = 1;
        manager.try_lock(&topic, &consumer_group, queue_id).await;
        assert!(!manager.try_lock(&topic, &consumer_group, queue_id).await);
    }

    #[tokio::test]
    async fn unlock_unlocks_successfully1() {
        let manager = QueueLockManager::new();
        let topic = CheetahString::from_static_str("test_topic");
        let consumer_group = CheetahString::from_static_str("test_group");
        let queue_id = 1;
        manager.try_lock(&topic, &consumer_group, queue_id).await;
        manager.unlock(&topic, &consumer_group, queue_id).await;
        assert!(manager.try_lock(&topic, &consumer_group, queue_id).await);
    }

    #[tokio::test]
    async fn clean_unused_locks_removes_expired_locks() {
        let manager = QueueLockManager::new();
        let topic = CheetahString::from_static_str("test_topic");
        let consumer_group = CheetahString::from_static_str("test_group");
        let queue_id = 1;
        manager.try_lock(&topic, &consumer_group, queue_id).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let removed_count = manager.clean_unused_locks(5).await;
        assert_eq!(removed_count, 1);
        let removed_count = manager.clean_unused_locks(15).await;
        assert_eq!(removed_count, 0);
    }
}
