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

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::processor::pop_message_processor::PopMessageProcessor;

pub struct ChangeInvisibleTimeProcessor<MS: MessageStore> {
    revive_topic: CheetahString,
    pop_message_processor: ArcMut<PopMessageProcessor<MS>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RequestProcessor for ChangeInvisibleTimeProcessor<MS>
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
        info!("ChangeInvisibleTimeProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::ChangeMessageInvisibleTime => self.process_request_(channel, ctx, request_code, request).await,
            _ => {
                warn!(
                    "ChangeInvisibleTimeProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!(
                        "ChangeInvisibleTimeProcessor request code {} not supported",
                        request.code()
                    ),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS: MessageStore> ChangeInvisibleTimeProcessor<MS> {
    pub fn new(
        pop_message_processor: ArcMut<PopMessageProcessor<MS>>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        let revive_topic = PopAckConstants::build_cluster_revive_topic(
            broker_runtime_inner
                .broker_config()
                .broker_identity
                .broker_cluster_name
                .as_str(),
        );
        ChangeInvisibleTimeProcessor {
            revive_topic: CheetahString::from_string(revive_topic),
            pop_message_processor,
            broker_runtime_inner,
        }
    }
}

impl<MS> ChangeInvisibleTimeProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request_(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_inner(channel, ctx, request, true).await
    }

    pub async fn process_request_inner(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ChangeInvisibleTimeRequestHeader>()?;
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            error!(
                "The topic {} not exist, consumer: {} ",
                request_header.topic,
                channel.remote_address()
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
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.read_queue_nums,
                channel.remote_address()
            );

            error!("{}", error_info);

            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::MessageIllegal,
                error_info,
            )));
        }
        let mix_offset = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .get_min_offset_in_queue(&request_header.topic, request_header.queue_id);
        let max_offset = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .get_max_offset_in_queue(&request_header.topic, request_header.queue_id);
        if request_header.offset < mix_offset || request_header.offset > max_offset {
            let info = format!(
                "request offset[{}] not in queue offset range[{}-{}], topic:[{}] consumer:[{}]",
                request_header.offset,
                mix_offset,
                max_offset,
                request_header.topic,
                channel.remote_address()
            );

            info!("{}", info);

            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NoMessage,
                info,
            )));
        }
        let extra_info = ExtraInfoUtil::split(&request_header.extra_info);
        if ExtraInfoUtil::is_order(extra_info.as_slice()) {
            return self
                .process_change_invisible_time_for_order(&request_header, extra_info.as_slice())
                .await;
        }
        // add new ck
        let now = get_current_millis();
        let revive_qid = ExtraInfoUtil::get_revive_qid(extra_info.as_slice())?;
        let ck_result = self
            .append_check_point(
                &request_header,
                revive_qid,
                request_header.queue_id,
                request_header.offset,
                now,
                CheetahString::from_string(ExtraInfoUtil::get_broker_name(extra_info.as_slice())?),
            )
            .await?;
        match ck_result.put_message_status() {
            PutMessageStatus::PutOk
            | PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable => {}
            _ => {
                error!("change Invisible, put new ck error: {}", ck_result.put_message_status());
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("append check point error, status: {:?}", ck_result.put_message_status()),
                )));
            }
        }
        if let Err(e) = self.ack_origin(&request_header, extra_info.as_slice()).await {
            error!(
                "change Invisible, put ack msg error: {}, {}",
                request_header.extra_info, e
            );
        }
        let response_header = ChangeInvisibleTimeResponseHeader {
            pop_time: now,
            revive_qid,
            invisible_time: request_header.invisible_time,
        };
        Ok(Some(RemotingCommand::create_response_command_with_header(
            response_header,
        )))
    }

    async fn ack_origin(
        &mut self,
        request_header: &ChangeInvisibleTimeRequestHeader,
        extra_info: &[String],
    ) -> rocketmq_error::RocketMQResult<()> {
        let ack_msg = AckMsg {
            ack_offset: request_header.offset,
            start_offset: ExtraInfoUtil::get_ck_queue_offset(extra_info)?,
            consumer_group: request_header.consumer_group.clone(),
            topic: request_header.topic.clone(),
            queue_id: request_header.queue_id,
            pop_time: ExtraInfoUtil::get_pop_time(extra_info)?,
            broker_name: CheetahString::from_string(ExtraInfoUtil::get_broker_name(extra_info)?),
        };

        let rq_id = ExtraInfoUtil::get_revive_qid(extra_info)?;
        self.broker_runtime_inner.broker_stats_manager().inc_broker_ack_nums(1);
        self.broker_runtime_inner.broker_stats_manager().inc_group_ack_nums(
            request_header.consumer_group.as_str(),
            request_header.topic.as_str(),
            1,
        );
        if self
            .pop_message_processor
            .pop_buffer_merge_service_mut()
            .add_ack(rq_id, &ack_msg)
        {
            return Ok(());
        }
        let mut inner = MessageExtBrokerInner::default();
        inner.set_topic(self.revive_topic.clone());
        inner.set_body(Bytes::from(ack_msg.encode()?));
        inner.message_ext_inner.queue_id = rq_id;
        inner.set_tags(CheetahString::from_static_str(PopAckConstants::ACK_TAG));
        inner.message_ext_inner.born_timestamp = get_current_millis() as i64;
        inner.message_ext_inner.born_host = self.broker_runtime_inner.store_host();
        inner.message_ext_inner.store_host = self.broker_runtime_inner.store_host();
        let deliver_time_ms = ExtraInfoUtil::get_pop_time(extra_info)? + ExtraInfoUtil::get_invisible_time(extra_info)?;
        inner.set_delay_time_ms(deliver_time_ms as u64);
        inner.message_ext_inner.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from(PopMessageProcessor::<MS>::gen_ack_unique_id(&ack_msg)),
        );
        inner.properties_string = message_decoder::message_properties_to_string(inner.get_properties());
        let result = self
            .broker_runtime_inner
            .escape_bridge_mut()
            .put_message_to_specific_queue(inner)
            .await;
        match result.put_message_status() {
            PutMessageStatus::PutOk
            | PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable
            | PutMessageStatus::ServiceNotAvailable => {}
            _ => {
                error!("change Invisible, put ack msg error: {}", result.put_message_status());
            }
        }
        //PopMetricsManager.incPopReviveAckPutCount(ackMsg,
        // putMessageResult.getPutMessageStatus());
        Ok(())
    }

    async fn append_check_point(
        &mut self,
        request_header: &ChangeInvisibleTimeRequestHeader,
        revive_qid: i32,
        queue_id: i32,
        offset: i64,
        pop_time: u64,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<PutMessageResult> {
        let mut ck = PopCheckPoint {
            bit_map: 0,
            num: 1,
            pop_time: pop_time as i64,
            invisible_time: request_header.invisible_time,
            start_offset: offset,
            cid: request_header.consumer_group.clone(),
            topic: request_header.topic.clone(),
            queue_id,
            broker_name: Some(broker_name),
            ..Default::default()
        };

        ck.add_diff(0);

        let mut inner = MessageExtBrokerInner::default();
        inner.set_topic(self.revive_topic.clone());
        inner.set_body(Bytes::from(ck.encode()?));
        inner.message_ext_inner.queue_id = revive_qid;
        inner.set_tags(CheetahString::from_static_str(PopAckConstants::ACK_TAG));
        inner.message_ext_inner.born_timestamp = get_current_millis() as i64;
        inner.message_ext_inner.born_host = self.broker_runtime_inner.store_host();
        inner.message_ext_inner.store_host = self.broker_runtime_inner.store_host();
        let deliver_time_ms = ck.get_revive_time() - PopAckConstants::ACK_TIME_INTERVAL;
        let deliver_time_ms = if deliver_time_ms > 0 { deliver_time_ms as u64 } else { 0 };
        inner.set_delay_time_ms(deliver_time_ms);
        inner.message_ext_inner.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from(PopMessageProcessor::<MS>::gen_ck_unique_id(&ck)),
        );
        inner.properties_string = message_decoder::message_properties_to_string(inner.get_properties());
        let put_message_result = self
            .broker_runtime_inner
            .escape_bridge_mut()
            .put_message_to_specific_queue(inner)
            .await;
        if self.broker_runtime_inner.broker_config().enable_pop_log {
            info!(
                "change Invisible , appendCheckPoint, topic {}, queueId {},reviveId {}, cid {}, startOffset {}, rt \
                 {}, result {}",
                request_header.topic,
                queue_id,
                revive_qid,
                request_header.consumer_group,
                offset,
                ck.get_revive_time(),
                put_message_result.put_message_status()
            )
        }
        Ok(put_message_result)
    }

    async fn process_change_invisible_time_for_order(
        &mut self,
        request_header: &ChangeInvisibleTimeRequestHeader,
        extra_info: &[String],
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let pop_time = ExtraInfoUtil::get_pop_time(extra_info)?;
        let old_offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
            &request_header.consumer_group,
            &request_header.topic,
            request_header.queue_id,
        );
        if old_offset > request_header.offset {
            return Ok(Some(RemotingCommand::create_response_command()));
        }
        while !self
            .pop_message_processor
            .queue_lock_manager()
            .try_lock(
                &request_header.consumer_group,
                &request_header.topic,
                request_header.queue_id,
            )
            .await
        {}
        let old_offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
            &request_header.consumer_group,
            &request_header.topic,
            request_header.queue_id,
        );
        if old_offset > request_header.offset {
            return Ok(Some(RemotingCommand::create_response_command()));
        }
        let next_visible_time = get_current_millis() + request_header.invisible_time as u64;
        self.broker_runtime_inner
            .consumer_order_info_manager()
            .update_next_visible_time(
                &request_header.topic,
                &request_header.consumer_group,
                request_header.queue_id,
                request_header.offset as u64,
                pop_time as u64,
                next_visible_time,
            );
        let revive_qid = ExtraInfoUtil::get_revive_qid(extra_info)?;
        let response_header = ChangeInvisibleTimeResponseHeader {
            pop_time: pop_time as u64,
            revive_qid,
            invisible_time: (next_visible_time as i64) - pop_time,
        };
        self.pop_message_processor
            .queue_lock_manager()
            .unlock(
                &request_header.consumer_group,
                &request_header.topic,
                request_header.queue_id,
            )
            .await;
        Ok(Some(RemotingCommand::create_response_command_with_header(
            response_header,
        )))
    }
}
