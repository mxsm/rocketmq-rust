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

use cheetah_string::CheetahString;
use rocketmq_common::common::key_builder::POP_ORDER_REVIVE_QUEUE;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::batch_ack::BatchAck;
use rocketmq_remoting::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::batch_ack_msg::BatchAckMsg;
use rocketmq_store::pop::AckMessage;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::processor_service::pop_revive_service::PopReviveService;

pub struct AckMessageProcessor<MS: MessageStore> {
    pop_message_processor: ArcMut<PopMessageProcessor<MS>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    revive_topic: CheetahString,
    pop_revive_services: Vec<ArcMut<PopReviveService<MS>>>,
}

impl<MS> RequestProcessor for AckMessageProcessor<MS>
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
        info!("AckMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::AckMessage | RequestCode::BatchAckMessage => {
                self.process_request_inner(channel, ctx, request_code, request).await
            }
            _ => {
                warn!("AckMessageProcessor received unknown request code: {:?}", request_code);
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("AckMessageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS> AckMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
        pop_message_processor: ArcMut<PopMessageProcessor<MS>>,
    ) -> AckMessageProcessor<MS> {
        let revive_topic = CheetahString::from_string(PopAckConstants::build_cluster_revive_topic(
            broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_cluster_name
                .as_str(),
        ));
        let mut pop_revive_services = vec![];
        let is_run_pop_revive = broker_runtime_inner.broker_config().broker_identity.broker_id == MASTER_ID;

        // each PopReviveService handles one revive topic's revive queue
        for i in 0..broker_runtime_inner.broker_config().revive_queue_num {
            let mut pop_revive_service =
                PopReviveService::new(revive_topic.clone(), i as i32, broker_runtime_inner.clone());
            pop_revive_service.set_should_run_pop_revive(is_run_pop_revive);
            pop_revive_services.push(ArcMut::new(pop_revive_service));
        }
        AckMessageProcessor {
            pop_message_processor,
            broker_runtime_inner,
            revive_topic,
            pop_revive_services,
        }
    }

    pub async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::AckMessage => self.process_ack(channel, ctx, request, true).await,
            RequestCode::BatchAckMessage => self.process_batch_ack(channel, ctx, request, true).await,
            _ => {
                error!(
                    "AckMessageProcessor failed to process RequestCode: {}, consumer: {} ",
                    request_code.to_i32(),
                    channel.remote_address()
                );
                Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::MessageIllegal,
                    format!("AckMessageProcessor failed to process RequestCode: {request_code:?}",),
                )))
            }
        }
    }

    pub fn start(&mut self) {
        for pop_revive_service in self.pop_revive_services.iter() {
            PopReviveService::start(pop_revive_service.clone());
        }
    }

    pub fn set_pop_revive_service_status(&mut self, status: bool) {
        for pop_revive_service in self.pop_revive_services.iter_mut() {
            pop_revive_service.set_should_run_pop_revive(status);
        }
    }
}

impl<MS> AckMessageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_ack(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<AckMessageRequestHeader>()?;
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            error!(
                "topic[{}] not exist, consumer: {},apply first please! {}",
                request_header.topic,
                channel.remote_address(),
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            );
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
        if request_header.queue_id >= topic_config.read_queue_nums as i32 || request_header.queue_id < 0 {
            let error_msg = format!(
                "queueId{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.read_queue_nums,
                channel.remote_address()
            );
            warn!("{}", error_msg);

            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::MessageIllegal,
                error_msg,
            )));
        }
        let message_store_inner = self.broker_runtime_inner.message_store().unwrap();
        let min_offset = message_store_inner.get_min_offset_in_queue(&request_header.topic, request_header.queue_id);
        let max_offset = message_store_inner.get_max_offset_in_queue(&request_header.topic, request_header.queue_id);
        if request_header.offset < min_offset || request_header.offset > max_offset {
            let error_msg = format!(
                "request offset not in queue offset range, request offset: {}, min offset: {}, max offset: {}",
                request_header.offset, min_offset, max_offset
            );
            warn!("{}", error_msg);

            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoMessage,
                error_msg,
            )));
        }
        let mut response = RemotingCommand::create_response_command();
        self.append_ack(Some(request_header), &mut response, None, &channel, None)
            .await?;
        Ok(Some(response))
    }

    async fn process_batch_ack(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if request.get_body().is_none() {
            return Ok(Some(RemotingCommand::create_response_command_with_code(
                ResponseCode::NoMessage,
            )));
        }
        let req_body = BatchAckMessageRequestBody::decode(request.get_body().unwrap())?;
        if req_body.acks.is_empty() {
            return Ok(Some(RemotingCommand::create_response_command_with_code(
                ResponseCode::NoMessage,
            )));
        }
        let mut response = RemotingCommand::create_response_command();
        let broker_name = &req_body.broker_name;
        for ack in req_body.acks {
            self.append_ack(None, &mut response, Some(ack), &_channel, Some(broker_name))
                .await?;
        }
        Ok(Some(response))
    }

    async fn append_ack(
        &mut self,
        request_header: Option<AckMessageRequestHeader>,
        response: &mut RemotingCommand,
        batch_ack: Option<BatchAck>,
        channel: &Channel,
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
                    channel,
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
            let message_store = self.broker_runtime_inner.message_store().unwrap();
            let min_offset = message_store.get_min_offset_in_queue(&topic, qid);
            let max_offset = message_store.get_max_offset_in_queue(&topic, qid);
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
                        channel,
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
        if self
            .pop_message_processor
            .pop_buffer_merge_service_mut()
            .add_ack(r_qid, ack_msg.as_ref())
        {
            self.broker_runtime_inner
                .pop_inflight_message_counter()
                .decrement_in_flight_message_num(&topic, &consume_group, pop_time, qid, ack_count as i64);
            return Ok(());
        }
        let mut inner = MessageExtBrokerInner::default();
        inner.set_topic(self.revive_topic.clone());
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
        inner.message_ext_inner.born_timestamp = get_current_millis() as i64;
        inner.message_ext_inner.store_host = self.broker_runtime_inner.store_host();
        inner.message_ext_inner.born_host = self.broker_runtime_inner.store_host();
        inner.set_delay_time_ms((pop_time + invisible_time) as u64);
        inner.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from(PopMessageProcessor::<MS>::gen_ack_unique_id(ack_msg.as_ref())),
        );
        inner.properties_string = message_decoder::message_properties_to_string(inner.get_properties());
        let put_message_result = self
            .broker_runtime_inner
            .escape_bridge_mut()
            .put_message_to_specific_queue(inner)
            .await;
        if !matches!(
            put_message_result.put_message_status(),
            PutMessageStatus::PutOk
                | PutMessageStatus::FlushDiskTimeout
                | PutMessageStatus::FlushSlaveTimeout
                | PutMessageStatus::SlaveNotAvailable
        ) {
            error!("put ack msg error:{:?}", put_message_result.put_message_status());
        }
        self.broker_runtime_inner
            .pop_inflight_message_counter()
            .decrement_in_flight_message_num(&topic, &consume_group, pop_time, qid, ack_count as i64);
        Ok(())
    }

    async fn ack_orderly(
        &mut self,
        topic: CheetahString,
        consume_group: CheetahString,
        q_id: i32,
        ack_offset: i64,
        pop_time: i64,
        invisible_time: i64,
        channel: &Channel,
        response: &mut RemotingCommand,
    ) {
        let lock_key = CheetahString::from_string(format!(
            "{}{}{}{}{}",
            &topic,
            PopAckConstants::SPLIT,
            &consume_group,
            PopAckConstants::SPLIT,
            q_id
        ));
        let old_offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(&consume_group, &topic, q_id);
        if old_offset > ack_offset {
            return;
        }
        while !self
            .pop_message_processor
            .queue_lock_manager()
            .try_lock_with_key(lock_key.clone())
            .await
        {
            //nothing to do
        }
        let old_offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(&consume_group, &topic, q_id);
        if old_offset > ack_offset {
            return;
        }
        let next_offset = self.broker_runtime_inner.consumer_order_info_manager().commit_and_next(
            &consume_group,
            &topic,
            q_id,
            ack_offset as u64,
            pop_time as u64,
        );
        match next_offset.cmp(&-1) {
            Ordering::Less => {}
            Ordering::Equal => {
                let error_info = format!(
                    "offset is illegal, key:{}, old:{}, commit:{}, next:{}, {}",
                    lock_key,
                    old_offset,
                    ack_offset,
                    next_offset,
                    channel.remote_address()
                );
                response.set_code_ref(ResponseCode::MessageIllegal);
                response.set_remark_mut(error_info);
                self.pop_message_processor
                    .queue_lock_manager()
                    .unlock_with_key(lock_key)
                    .await;
                return;
            }
            Ordering::Greater => {
                if !self.broker_runtime_inner.consumer_offset_manager().has_offset_reset(
                    consume_group.as_str(),
                    topic.as_str(),
                    q_id,
                ) {
                    self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                        channel.remote_address().to_string().into(),
                        &consume_group,
                        &topic,
                        q_id,
                        next_offset,
                    );
                }

                if !self.broker_runtime_inner.consumer_order_info_manager().check_block(
                    &CheetahString::empty(),
                    &consume_group,
                    &topic,
                    q_id,
                    invisible_time as u64,
                ) {
                    self.pop_message_processor
                        .notify_message_arriving(&topic, q_id, &consume_group);
                }
            }
        }
        self.pop_message_processor
            .queue_lock_manager()
            .unlock_with_key(lock_key)
            .await;
        self.broker_runtime_inner
            .pop_inflight_message_counter()
            .decrement_in_flight_message_num(&topic, &consume_group, pop_time, q_id, 1);
    }

    pub fn shutdown(&mut self) {
        for pop_revive_service in self.pop_revive_services.iter_mut() {
            pop_revive_service.shutdown();
        }
    }
}
