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

use std::cmp::Ordering;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Weak;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use futures::future::join_all;
use rocketmq_error::PublicErrorView;
use rocketmq_error::RocketMQResult;
use rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED;
use rocketmq_model::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::pop_ack_constants::PopAckConstants;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::body::batch_ack::BatchAck;
use rocketmq_protocol::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
use rocketmq_protocol::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_protocol::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::AckMessage;
use rocketmq_store::AckMsg;
use rocketmq_store::BatchAckMsg;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_store::PutMessageResult;
use rocketmq_store::PutMessageStatus;
use rocketmq_transport::api::error_response;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::long_polling::pop_deferred::index::PopFanoutCursor;
use crate::long_polling::pop_deferred::service::PopDeferredService;
use crate::long_polling::pop_deferred::service::PopPendingArrivalOutcome;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::processor::pop_inflight_message_counter::PopInflightMessageCounter;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pop_message_processor::QueueLockManager;
use crate::processor::processor_service::pop_buffer_merge_service::PopBufferMergeService;
use crate::processor::processor_service::pop_revive_service::PopReviveService;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone)]
pub(crate) struct AckMessagePolicy {
    revive_topic: CheetahString,
    store_host: SocketAddr,
}

impl AckMessagePolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig, store_host: SocketAddr) -> Self {
        Self {
            revive_topic: CheetahString::from_string(PopAckConstants::build_cluster_revive_topic(
                broker_config.broker_identity.broker_cluster_name.as_str(),
            )),
            store_host,
        }
    }

    pub(crate) fn revive_topic(&self) -> &CheetahString {
        &self.revive_topic
    }
}

pub(crate) struct AckMessageStoreCapability<MS: BrokerReadWriteStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: BrokerReadWriteStore> AckMessageStoreCapability<MS> {
    pub(crate) fn new(escape_bridge: &Arc<EscapeBridge<MS>>) -> Self {
        Self {
            escape_bridge: Arc::downgrade(escape_bridge),
        }
    }

    fn queue_offsets(&self, topic: &CheetahString, queue_id: i32) -> Result<(i64, i64), MessageStoreUnavailable> {
        let bridge = self.escape_bridge.upgrade().ok_or(MessageStoreUnavailable)?;
        Ok((
            bridge.get_min_offset_from_local_store(topic, queue_id)?,
            bridge.get_max_offset_from_local_store(topic, queue_id)?,
        ))
    }

    async fn put_message(&self, message: MessageExtBrokerInner) -> Result<PutMessageResult, MessageStoreUnavailable> {
        Ok(self
            .escape_bridge
            .upgrade()
            .ok_or(MessageStoreUnavailable)?
            .put_message_to_specific_queue(message)
            .await)
    }
}

pub(crate) struct AckMessageOrderCapability {
    manager: Weak<ConsumerOrderInfoManager>,
}

impl AckMessageOrderCapability {
    pub(crate) fn new(manager: &Arc<ConsumerOrderInfoManager>) -> Self {
        Self {
            manager: Arc::downgrade(manager),
        }
    }

    fn commit_and_next(
        &self,
        consume_group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: u64,
        pop_time: u64,
    ) -> Option<i64> {
        self.manager
            .upgrade()
            .map(|manager| manager.commit_and_next(consume_group, topic, queue_id, queue_offset, pop_time))
    }

    fn check_block(
        &self,
        attempt_id: &CheetahString,
        consume_group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        invisible_time: u64,
    ) -> Option<bool> {
        self.manager
            .upgrade()
            .map(|manager| manager.check_block(attempt_id, consume_group, topic, queue_id, invisible_time))
    }
}

pub(crate) struct AckMessageOffsetCapability<MS: BrokerReadWriteStore> {
    manager: Weak<ConsumerOffsetManager<MS>>,
}

impl<MS: BrokerReadWriteStore> AckMessageOffsetCapability<MS> {
    pub(crate) fn new(manager: &Arc<ConsumerOffsetManager<MS>>) -> Self {
        Self {
            manager: Arc::downgrade(manager),
        }
    }

    fn query_offset(&self, group: &CheetahString, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        self.manager
            .upgrade()
            .map(|manager| manager.query_offset(group, topic, queue_id))
    }

    fn has_offset_reset(&self, group: &str, topic: &str, queue_id: i32) -> Option<bool> {
        self.manager
            .upgrade()
            .map(|manager| manager.has_offset_reset(group, topic, queue_id))
    }

    fn commit_offset(
        &self,
        client_host: CheetahString,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
    ) -> bool {
        let Some(manager) = self.manager.upgrade() else {
            return false;
        };
        manager.commit_offset(client_host, group, topic, queue_id, offset);
        true
    }
}

pub(crate) struct AckMessagePopCapability<MS: BrokerReadWriteStore> {
    merge_service: Weak<PopBufferMergeService<MS>>,
    notification_service: Weak<PopDeferredService>,
    queue_lock_manager: QueueLockManager,
}

impl<MS: BrokerReadWriteStore> AckMessagePopCapability<MS> {
    pub(crate) fn new(processor: &Arc<PopMessageProcessor<MS>>) -> Self {
        Self {
            merge_service: Arc::downgrade(processor.pop_buffer_merge_service()),
            notification_service: processor.pop_deferred_service().map(Arc::downgrade).unwrap_or_default(),
            queue_lock_manager: processor.queue_lock_manager().clone(),
        }
    }

    fn add_ack(&self, revive_queue_id: i32, ack: &dyn AckMessage) -> bool {
        self.merge_service
            .upgrade()
            .is_some_and(|service| service.add_ack(revive_queue_id, ack))
    }

    fn notify_message_arriving(&self, topic: &CheetahString, queue_id: i32, consumer_group: &CheetahString) -> bool {
        let Some(service) = self.notification_service.upgrade() else {
            return false;
        };
        let _ = consumer_group;
        matches!(
            service.latch_arrival(
                topic,
                queue_id,
                None,
                current_millis() as i64,
                None,
                None,
                PopFanoutCursor::new(),
            ),
            Ok(PopPendingArrivalOutcome::Latched)
        )
    }
}

pub(crate) struct AckMessageProcessorContext<MS: BrokerReadWriteStore> {
    command_factory: RemotingCommandFactory,
    policy: AckMessagePolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    consumer_offset: AckMessageOffsetCapability<MS>,
    consumer_order: AckMessageOrderCapability,
    message_store: AckMessageStoreCapability<MS>,
    inflight_counter: PopInflightMessageCounter,
    pop: AckMessagePopCapability<MS>,
    pop_revive_services: Vec<Arc<PopReviveService<MS>>>,
}

impl<MS: BrokerReadWriteStore> AckMessageProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor lists the complete narrow acknowledgment capability boundary"
    )]
    pub(crate) fn new(
        policy: AckMessagePolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        consumer_offset: AckMessageOffsetCapability<MS>,
        consumer_order: AckMessageOrderCapability,
        message_store: AckMessageStoreCapability<MS>,
        inflight_counter: PopInflightMessageCounter,
        pop: AckMessagePopCapability<MS>,
        pop_revive_services: Vec<Arc<PopReviveService<MS>>>,
    ) -> Self {
        Self {
            command_factory: application_remoting_command_factory(),
            policy,
            topic_config_manager,
            consumer_offset,
            consumer_order,
            message_store,
            inflight_counter,
            pop,
            pop_revive_services,
        }
    }

    pub(crate) fn with_command_factory(mut self, command_factory: RemotingCommandFactory) -> Self {
        self.command_factory = command_factory;
        self
    }
}

pub struct AckMessageProcessor<MS: BrokerReadWriteStore> {
    context: AckMessageProcessorContext<MS>,
}

impl<MS> RequestProcessor for AckMessageProcessor<MS>
where
    MS: BrokerReadWriteStore + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl<MS: BrokerReadWriteStore> AckMessageProcessor<MS> {
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let command_factory = self.context.command_factory;
        let request_source = request_origin_label(request.origin());
        let result = self.process_command(request.command_mut(), &request_source).await;
        crate::processor::response_assembly::immediate_outcome_from_command_result(
            &command_factory,
            result,
            original_opaque,
            "AckMessageProcessor command dispatch completed without a response",
        )
    }
}

fn request_origin_label(origin: &RequestOrigin) -> CheetahString {
    match origin {
        RequestOrigin::Network { peer } => CheetahString::from_string(peer.address().to_string()),
        RequestOrigin::Embedded { .. } => CheetahString::from_static_str("embedded"),
        _ => CheetahString::from_static_str("unrecognized-origin"),
    }
}

impl<MS> AckMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    /// processor business contract; the typed origin is reduced to a diagnostic/offset source label.
    async fn process_command(
        &self,
        request: &mut RemotingCommand,
        request_source: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("AckMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::AckMessage | RequestCode::BatchAckMessage => {
                self.process_command_inner(request_code, request, request_source).await
            }
            _ => {
                warn!("AckMessageProcessor received unknown request code: {:?}", request_code);
                Ok(Some(error_response(
                    PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
                    RemotingErrorTarget::Reply {
                        factory: &self.context.command_factory,
                        opaque: request.opaque(),
                    },
                )))
            }
        }
    }
}

impl<MS> AckMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    pub(crate) fn new(context: AckMessageProcessorContext<MS>) -> AckMessageProcessor<MS> {
        AckMessageProcessor { context }
    }

    async fn process_command_inner(
        &self,
        request_code: RequestCode,
        request: &mut RemotingCommand,
        request_source: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::AckMessage => self.process_ack(request, request_source).await,
            RequestCode::BatchAckMessage => self.process_batch_ack(request, request_source).await,
            _ => {
                error!(
                    "AckMessageProcessor failed to process RequestCode: {}, consumer: {} ",
                    request_code.to_i32(),
                    request_source
                );
                Ok(Some(
                    self.context.command_factory.create_response_command_with_code_remark(
                        ResponseCode::MessageIllegal,
                        format!("AckMessageProcessor failed to process RequestCode: {request_code:?}",),
                    ),
                ))
            }
        }
    }

    pub fn start(&self) {
        for pop_revive_service in self.context.pop_revive_services.iter() {
            PopReviveService::start(pop_revive_service.clone());
        }
    }

    pub fn set_pop_revive_service_status(&self, status: bool) {
        for pop_revive_service in &self.context.pop_revive_services {
            pop_revive_service.set_should_run_pop_revive(status);
        }
    }

    pub fn pop_revive_metrics(&self) -> Vec<(i32, i64, i64)> {
        self.context
            .pop_revive_services
            .iter()
            .map(|service| {
                (
                    service.queue_id(),
                    service.get_revive_behind_messages(),
                    service.get_revive_behind_millis(),
                )
            })
            .collect()
    }
}

impl<MS> AckMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    async fn process_ack(
        &self,
        request: &mut RemotingCommand,
        request_source: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<AckMessageRequestHeader>()?;
        let topic_config = self
            .context
            .topic_config_manager
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            error!(
                "topic[{}] not exist, consumer: {},apply first please! {}",
                request_header.topic,
                request_source,
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            );
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
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
        if request_header.queue_id >= topic_config.read_queue_nums as i32 || request_header.queue_id < 0 {
            let error_msg = format!(
                "queueId{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id, request_header.topic, topic_config.read_queue_nums, request_source
            );
            warn!("{}", error_msg);

            return Ok(Some(
                self.context
                    .command_factory
                    .create_response_command_with_code_remark(ResponseCode::MessageIllegal, error_msg),
            ));
        }
        let Ok((min_offset, max_offset)) = self
            .context
            .message_store
            .queue_offsets(&request_header.topic, request_header.queue_id)
        else {
            return Ok(Some(
                self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::ServiceNotAvailable,
                    "message store is not available",
                ),
            ));
        };
        if request_header.offset < min_offset || request_header.offset > max_offset {
            let error_msg = format!(
                "request offset not in queue offset range, request offset: {}, min offset: {}, max offset: {}",
                request_header.offset, min_offset, max_offset
            );
            warn!("{}", error_msg);

            return Ok(Some(
                self.context
                    .command_factory
                    .create_response_command_with_code_remark(ResponseCode::NoMessage, error_msg),
            ));
        }
        let mut response = self.context.command_factory.create_success_response_command();
        self.append_ack(Some(request_header), &mut response, None, request_source, None)
            .await?;
        Ok(Some(response))
    }

    async fn process_batch_ack(
        &self,
        request: &mut RemotingCommand,
        request_source: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if request.get_body().is_none() {
            return Ok(Some(
                self.context
                    .command_factory
                    .create_response_command_with_code(ResponseCode::NoMessage),
            ));
        }
        let req_body = BatchAckMessageRequestBody::decode(request.get_body().unwrap())?;
        if req_body.acks.is_empty() {
            return Ok(Some(
                self.context
                    .command_factory
                    .create_response_command_with_code(ResponseCode::NoMessage),
            ));
        }
        let mut response = self.context.command_factory.create_success_response_command();
        let broker_name = &req_body.broker_name;
        for ack in req_body.acks {
            self.append_ack(None, &mut response, Some(ack), request_source, Some(broker_name))
                .await?;
        }
        Ok(Some(response))
    }

    async fn append_ack(
        &self,
        request_header: Option<AckMessageRequestHeader>,
        response: &mut RemotingCommand,
        batch_ack: Option<BatchAck>,
        request_source: &CheetahString,
        broker_name: Option<&CheetahString>,
    ) -> RocketMQResult<()> {
        //handle single ack
        let (
            consume_group,
            topic,
            qid,
            r_qid,
            start_offset,
            ack_offset,
            pop_time,
            invisible_time,
            ack_count,
            mut ack_msg,
            broker_name,
        ) = if let Some(request_header) = request_header {
            let extra_info = ExtraInfoUtil::split(request_header.extra_info.as_str());
            let broker_name = ExtraInfoUtil::get_broker_name(extra_info.as_slice())?;
            let consume_group = request_header.consumer_group;
            let topic = request_header.topic;
            let qid = request_header.queue_id;
            let r_qid = ExtraInfoUtil::get_revive_qid(extra_info.as_slice())?;
            let start_offset = ExtraInfoUtil::get_ck_queue_offset(extra_info.as_slice())?;
            let ack_offset = request_header.offset;
            let pop_time = ExtraInfoUtil::get_pop_time(extra_info.as_slice())?;
            let invisible_time = ExtraInfoUtil::get_invisible_time(extra_info.as_slice())?;
            if r_qid == POP_ORDER_REVIVE_QUEUE {
                self.ack_orderly(
                    topic,
                    consume_group,
                    qid,
                    ack_offset,
                    pop_time,
                    invisible_time,
                    request_source,
                    response,
                )
                .await;
                return Ok(());
            }
            let ack = AckMsg::default();
            let ack_count = 1;
            (
                consume_group,
                topic,
                qid,
                r_qid,
                start_offset,
                ack_offset,
                pop_time,
                invisible_time,
                ack_count,
                Box::new(ack) as Box<dyn AckMessage + Send>,
                CheetahString::from(broker_name),
            )
        } else {
            //handle batch ack
            let batch_ack = batch_ack.unwrap();
            let consumer_group = batch_ack.consumer_group;
            let topic = CheetahString::from(ExtraInfoUtil::get_real_topic_with_retry(
                batch_ack.topic.as_str(),
                consumer_group.as_str(),
                batch_ack.retry.as_str(),
            )?);
            let qid = batch_ack.queue_id;
            let r_qid = batch_ack.revive_queue_id;
            let start_offset = batch_ack.start_offset;
            let akc_offset = -1;
            let pop_time = batch_ack.pop_time;
            let invisible_time = batch_ack.invisible_time;
            let Ok((min_offset, max_offset)) = self.context.message_store.queue_offsets(&topic, qid) else {
                response.set_code_ref(ResponseCode::ServiceNotAvailable);
                response.set_remark_mut("message store is not available");
                return Ok(());
            };
            if min_offset == -1 || max_offset == -1 {
                //error!("Illegal topic or queue found when batch ack {:?}", batch_ack);
                return Ok(());
            }

            let mut batch_ack_msg = BatchAckMsg::default();

            let bit_set = &batch_ack.bit_set.0;
            for i in bit_set.iter_ones() {
                if i == usize::MAX {
                    break;
                }
                let offset = batch_ack.start_offset + i as i64;
                if offset < min_offset || offset > max_offset {
                    continue;
                }
                if r_qid == POP_ORDER_REVIVE_QUEUE {
                    self.ack_orderly(
                        topic.clone(),
                        consumer_group.clone(),
                        qid,
                        offset,
                        pop_time,
                        invisible_time,
                        request_source,
                        response,
                    )
                    .await;
                } else {
                    batch_ack_msg.ack_offset_list.push(offset);
                }
            }
            if r_qid == POP_ORDER_REVIVE_QUEUE || batch_ack_msg.ack_offset_list.is_empty() {
                return Ok(());
            }
            let ack_count = batch_ack_msg.ack_offset_list.len();
            (
                consumer_group,
                topic,
                qid,
                r_qid,
                start_offset,
                akc_offset,
                pop_time,
                invisible_time,
                ack_count,
                Box::new(batch_ack_msg) as Box<dyn AckMessage + Send>,
                broker_name.unwrap().clone(),
            )
        };

        //this.brokerController.getBrokerStatsManager().incBrokerAckNums(ackCount);
        //this.brokerController.getBrokerStatsManager().incGroupAckNums(consumeGroup,topic,
        // ackCount);
        ack_msg.set_consumer_group(consume_group.clone());
        ack_msg.set_topic(topic.clone());
        ack_msg.set_queue_id(qid);
        ack_msg.set_start_offset(start_offset);
        ack_msg.set_ack_offset(ack_offset);
        ack_msg.set_pop_time(pop_time);
        ack_msg.set_broker_name(broker_name);
        if self.context.pop.add_ack(r_qid, ack_msg.as_ref()) {
            self.context.inflight_counter.decrement_in_flight_message_num(
                &topic,
                &consume_group,
                pop_time,
                qid,
                ack_count as i64,
            );
            return Ok(());
        }
        let mut inner = MessageExtBrokerInner::default();
        inner.set_topic(self.context.policy.revive_topic.clone());
        inner.message_ext_inner.queue_id = qid;
        if let Some(batch_ack) = ack_msg.as_any().downcast_ref::<BatchAckMsg>() {
            inner.set_body(batch_ack.encode()?.into());
            inner.set_tags(CheetahString::from_static_str(PopAckConstants::BATCH_ACK_TAG));
            inner.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from(PopMessageProcessor::<MS>::gen_batch_ack_unique_id(batch_ack)),
            );
        } else if let Some(ack_msg) = ack_msg.as_any().downcast_ref::<AckMsg>() {
            inner.set_body(ack_msg.encode()?.into());
            inner.set_tags(CheetahString::from_static_str(PopAckConstants::ACK_TAG));
            inner.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                CheetahString::from(PopMessageProcessor::<MS>::gen_ack_unique_id(ack_msg as &dyn AckMessage)),
            );
        }
        inner.message_ext_inner.born_timestamp = current_millis() as i64;
        inner.message_ext_inner.store_host = self.context.policy.store_host;
        inner.message_ext_inner.born_host = self.context.policy.store_host;
        inner.set_delay_time_ms((pop_time + invisible_time) as u64);
        inner.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from(PopMessageProcessor::<MS>::gen_ack_unique_id(ack_msg.as_ref())),
        );
        inner.properties_string = MessageDecoder::message_properties_to_string(inner.get_properties());
        let put_message_result = match self.context.message_store.put_message(inner).await {
            Ok(result) => result,
            Err(MessageStoreUnavailable) => {
                response.set_code_ref(ResponseCode::ServiceNotAvailable);
                response.set_remark_mut("message store is not available");
                return Ok(());
            }
        };
        if !matches!(
            put_message_result.put_message_status(),
            PutMessageStatus::PutOk
                | PutMessageStatus::FlushDiskTimeout
                | PutMessageStatus::FlushSlaveTimeout
                | PutMessageStatus::SlaveNotAvailable
        ) {
            error!("put ack msg error:{:?}", put_message_result.put_message_status());
        }
        self.context.inflight_counter.decrement_in_flight_message_num(
            &topic,
            &consume_group,
            pop_time,
            qid,
            ack_count as i64,
        );
        Ok(())
    }

    async fn ack_orderly(
        &self,
        topic: CheetahString,
        consume_group: CheetahString,
        q_id: i32,
        ack_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        request_source: &CheetahString,
        response: &mut RemotingCommand,
    ) {
        let lock_key = CheetahString::from_string(QueueLockManager::build_lock_key(&topic, &consume_group, q_id));
        let Some(old_offset) = self.context.consumer_offset.query_offset(&consume_group, &topic, q_id) else {
            response.set_code_ref(ResponseCode::ServiceNotAvailable);
            response.set_remark_mut("consumer offset manager is not available");
            return;
        };
        if old_offset > ack_offset {
            return;
        }
        while !self
            .context
            .pop
            .queue_lock_manager
            .try_lock_with_key(lock_key.clone())
            .await
        {
            //nothing to do
        }
        let Some(old_offset) = self.context.consumer_offset.query_offset(&consume_group, &topic, q_id) else {
            response.set_code_ref(ResponseCode::ServiceNotAvailable);
            response.set_remark_mut("consumer offset manager is not available");
            self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
            return;
        };
        if old_offset > ack_offset {
            self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
            return;
        }
        let Some(next_offset) = self.context.consumer_order.commit_and_next(
            &consume_group,
            &topic,
            q_id,
            ack_offset as u64,
            pop_time as u64,
        ) else {
            response.set_code_ref(ResponseCode::ServiceNotAvailable);
            response.set_remark_mut("consumer order manager is not available");
            self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
            return;
        };
        match next_offset.cmp(&-1) {
            Ordering::Less => {}
            Ordering::Equal => {
                let error_info = format!(
                    "offset is illegal, key:{}, old:{}, commit:{}, next:{}, {}",
                    lock_key, old_offset, ack_offset, next_offset, request_source
                );
                response.set_code_ref(ResponseCode::MessageIllegal);
                response.set_remark_mut(error_info);
                self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
                return;
            }
            Ordering::Greater => {
                let Some(has_offset_reset) =
                    self.context
                        .consumer_offset
                        .has_offset_reset(consume_group.as_str(), topic.as_str(), q_id)
                else {
                    response.set_code_ref(ResponseCode::ServiceNotAvailable);
                    response.set_remark_mut("consumer offset manager is not available");
                    self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
                    return;
                };
                if !has_offset_reset
                    && !self.context.consumer_offset.commit_offset(
                        request_source.clone(),
                        &consume_group,
                        &topic,
                        q_id,
                        next_offset,
                    )
                {
                    response.set_code_ref(ResponseCode::ServiceNotAvailable);
                    response.set_remark_mut("consumer offset manager is not available");
                    self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
                    return;
                }

                match self.context.consumer_order.check_block(
                    &CheetahString::empty(),
                    &consume_group,
                    &topic,
                    q_id,
                    invisible_time as u64,
                ) {
                    Some(false) => {
                        self.context.pop.notify_message_arriving(&topic, q_id, &consume_group);
                    }
                    Some(true) => {}
                    None => {
                        response.set_code_ref(ResponseCode::ServiceNotAvailable);
                        response.set_remark_mut("consumer order manager is not available");
                        self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
                        return;
                    }
                }
            }
        }
        self.context.pop.queue_lock_manager.unlock_with_key(lock_key).await;
        self.context
            .inflight_counter
            .decrement_in_flight_message_num(&topic, &consume_group, pop_time, q_id, 1);
    }

    pub async fn shutdown(&self) {
        join_all(
            self.context
                .pop_revive_services
                .iter()
                .map(|pop_revive_service| pop_revive_service.shutdown()),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;
    use rocketmq_error::RocketMQResult;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::StorePorts;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::AuthorizedCommandDispatcher;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::ResponseBodyKind;
    use rocketmq_transport::api::TransportError;
    use rocketmq_transport::api::TransportSecurity;
    use rocketmq_transport::test_support::EmbeddedRequestHarness;

    struct TestLeafProcessor<P> {
        processor: Arc<tokio::sync::Mutex<P>>,
    }

    impl<P> Clone for TestLeafProcessor<P> {
        fn clone(&self) -> Self {
            Self {
                processor: Arc::clone(&self.processor),
            }
        }
    }

    impl<P> TestLeafProcessor<P> {
        fn new(processor: P) -> Self {
            Self {
                processor: Arc::new(tokio::sync::Mutex::new(processor)),
            }
        }
    }

    impl<P> RequestProcessor for TestLeafProcessor<P>
    where
        P: RequestProcessor + Send,
    {
        async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
            Box::pin(self.processor.lock().await.process(request)).await
        }
    }

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    async fn dispatch_embedded<P>(
        processor: P,
        command: RemotingCommand,
    ) -> Result<EmbeddedDispatchOutcome, TransportError>
    where
        P: RequestProcessor + Send + 'static,
    {
        let owner = RuntimeOwner::plan(RuntimeConfig::server_default("ack-message-test"))
            .expect("runtime configuration is valid")
            .build()
            .expect("AckMessage test runtime");
        let context = owner.root_context().component("ack-message-test.request");
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            TestLeafProcessor::new(processor),
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarness::new(
            dispatcher,
            context.task_group().clone(),
            Principal::new("ack-message-test"),
        );
        let outcome = harness.dispatch(None, command).await;

        drop(harness);
        drop(context);
        assert!(owner.shutdown_tasks().await.is_healthy());
        assert!(owner.shutdown_background().is_healthy());
        outcome
    }

    fn test_processor() -> AckMessageProcessor<StorePorts> {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = MessageStoreConfig::default();
        let topic_config_manager = Arc::new(TopicConfigManager::new(
            broker_config.as_ref(),
            &message_store_config,
            true,
            None,
        ));
        let store_host = "127.0.0.1:10911".parse().expect("valid store host");
        let context = AckMessageProcessorContext::new(
            AckMessagePolicy::from_config(broker_config.as_ref(), store_host),
            topic_config_manager,
            AckMessageOffsetCapability { manager: Weak::new() },
            AckMessageOrderCapability { manager: Weak::new() },
            AckMessageStoreCapability {
                escape_bridge: Weak::new(),
            },
            PopInflightMessageCounter::new(Arc::new(AtomicU64::new(0))),
            AckMessagePopCapability {
                merge_service: Weak::new(),
                notification_service: Weak::new(),
                queue_lock_manager: QueueLockManager::new(),
            },
            Vec::new(),
        );
        AckMessageProcessor::new(context)
    }

    #[test]
    fn ack_message_policy_captures_only_required_startup_values() {
        let mut broker_config = BrokerConfig::default();
        broker_config.broker_identity.broker_cluster_name = CheetahString::from_static_str("cluster-a");
        let store_host = "127.0.0.1:10911".parse().expect("valid store host");

        let policy = AckMessagePolicy::from_config(&broker_config, store_host);

        assert_eq!(
            policy.revive_topic,
            PopAckConstants::build_cluster_revive_topic("cluster-a")
        );
        assert_eq!(policy.store_host, store_host);
    }

    #[tokio::test]
    async fn ack_message_weak_capabilities_fail_closed_after_provider_shutdown() {
        let store = AckMessageStoreCapability::<StorePorts> {
            escape_bridge: Weak::new(),
        };
        let order = AckMessageOrderCapability { manager: Weak::new() };
        let offset = AckMessageOffsetCapability::<StorePorts> { manager: Weak::new() };
        let pop = AckMessagePopCapability::<StorePorts> {
            merge_service: Weak::new(),
            notification_service: Weak::new(),
            queue_lock_manager: QueueLockManager::new(),
        };
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        assert!(store.queue_offsets(&topic, 0).is_err());
        assert!(store.put_message(MessageExtBrokerInner::default()).await.is_err());
        assert_eq!(offset.query_offset(&group, &topic, 0), None);
        assert_eq!(offset.has_offset_reset(group.as_str(), topic.as_str(), 0), None);
        assert!(!offset.commit_offset(CheetahString::empty(), &group, &topic, 0, 1));
        assert!(!pop.add_ack(0, &AckMsg::default()));
        assert!(!pop.notify_message_arriving(&topic, 0, &group));
        assert_eq!(order.commit_and_next(&group, &topic, 0, 0, 0), None);
        assert_eq!(order.check_block(&CheetahString::empty(), &group, &topic, 0, 1), None);
    }

    #[tokio::test]
    async fn embedded_unknown_request_returns_a_reply_response() {
        let outcome = dispatch_embedded(
            test_processor(),
            RemotingCommand::create_remoting_command(-98_451).set_opaque(317),
        )
        .await
        .expect("embedded AckMessage response");
        let EmbeddedDispatchOutcome::Reply(response) = outcome else {
            panic!("AckMessage unknown request must return a reply response");
        };

        assert_eq!(response.response_code(), ResponseCode::RequestCodeNotSupported as i32);
        assert_eq!(response.body_kind(), ResponseBodyKind::Empty);
    }
}
