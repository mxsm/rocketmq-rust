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
use std::sync::Weak;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::error_response;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::failover::escape_bridge::EscapeBridge;
use crate::failover::escape_bridge::MessageStoreUnavailable;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetQueryCapability;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pop_message_processor::QueueLockManager;
use crate::processor::processor_service::pop_buffer_merge_service::PopBufferMergeService;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

#[derive(Clone)]
pub(crate) struct ChangeInvisibleTimePolicy {
    revive_topic: CheetahString,
    store_host: SocketAddr,
    enable_pop_log: bool,
}

impl ChangeInvisibleTimePolicy {
    pub(crate) fn from_config(broker_config: &BrokerConfig, store_host: SocketAddr) -> Self {
        Self {
            revive_topic: CheetahString::from_string(PopAckConstants::build_cluster_revive_topic(
                broker_config.broker_identity.broker_cluster_name.as_str(),
            )),
            store_host,
            enable_pop_log: broker_config.enable_pop_log,
        }
    }
}

pub(crate) struct ChangeInvisibleTimeStoreCapability<MS: MessageStore> {
    escape_bridge: Weak<EscapeBridge<MS>>,
}

impl<MS: MessageStore> ChangeInvisibleTimeStoreCapability<MS> {
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

pub(crate) struct ChangeInvisibleTimePopCapability<MS: MessageStore> {
    merge_service: Weak<PopBufferMergeService<MS>>,
}

impl<MS: MessageStore> ChangeInvisibleTimePopCapability<MS> {
    pub(crate) fn new(merge_service: &Arc<PopBufferMergeService<MS>>) -> Self {
        Self {
            merge_service: Arc::downgrade(merge_service),
        }
    }

    fn add_ack(&self, revive_qid: i32, ack_msg: &AckMsg) -> bool {
        self.merge_service
            .upgrade()
            .is_some_and(|service| service.add_ack(revive_qid, ack_msg))
    }
}

pub(crate) struct ChangeInvisibleTimeOrderCapability {
    manager: Weak<ConsumerOrderInfoManager>,
}

impl ChangeInvisibleTimeOrderCapability {
    pub(crate) fn new(manager: &Arc<ConsumerOrderInfoManager>) -> Self {
        Self {
            manager: Arc::downgrade(manager),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "arguments preserve ConsumerOrderInfoManager's protocol update coordinates"
    )]
    fn update_next_visible_time(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        queue_offset: u64,
        pop_time: u64,
        next_visible_time: u64,
    ) -> bool {
        let Some(manager) = self.manager.upgrade() else {
            return false;
        };
        manager.update_next_visible_time(topic, group, queue_id, queue_offset, pop_time, next_visible_time);
        true
    }
}

pub(crate) struct ChangeInvisibleTimeProcessorContext<MS: MessageStore> {
    policy: ChangeInvisibleTimePolicy,
    topic_config_manager: Arc<TopicConfigManager>,
    consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
    consumer_order_info: ChangeInvisibleTimeOrderCapability,
    broker_stats_manager: Arc<BrokerStatsManager>,
    message_store: ChangeInvisibleTimeStoreCapability<MS>,
    pop_buffer: ChangeInvisibleTimePopCapability<MS>,
    queue_lock_manager: QueueLockManager,
}

impl<MS: MessageStore> ChangeInvisibleTimeProcessorContext<MS> {
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor lists the complete narrow change-invisible-time capability boundary"
    )]
    pub(crate) fn new(
        policy: ChangeInvisibleTimePolicy,
        topic_config_manager: Arc<TopicConfigManager>,
        consumer_offset_query: ConsumerOffsetQueryCapability<MS>,
        consumer_order_info: ChangeInvisibleTimeOrderCapability,
        broker_stats_manager: Arc<BrokerStatsManager>,
        message_store: ChangeInvisibleTimeStoreCapability<MS>,
        pop_buffer: ChangeInvisibleTimePopCapability<MS>,
        queue_lock_manager: QueueLockManager,
    ) -> Self {
        Self {
            policy,
            topic_config_manager,
            consumer_offset_query,
            consumer_order_info,
            broker_stats_manager,
            message_store,
            pop_buffer,
            queue_lock_manager,
        }
    }
}

pub struct ChangeInvisibleTimeProcessor<MS: MessageStore> {
    context: ChangeInvisibleTimeProcessorContext<MS>,
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
                let response = error_response::request_code_not_supported_with_remark_and_opaque(
                    request.code(),
                    format!(
                        "ChangeInvisibleTimeProcessor request code {} not supported",
                        request.code()
                    ),
                    request.opaque(),
                );
                Ok(Some(response))
            }
        }
    }
}

impl<MS: MessageStore> ChangeInvisibleTimeProcessor<MS> {
    pub(crate) fn new(context: ChangeInvisibleTimeProcessorContext<MS>) -> Self {
        Self { context }
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
            .context
            .topic_config_manager
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
        let Ok((mix_offset, max_offset)) = self
            .context
            .message_store
            .queue_offsets(&request_header.topic, request_header.queue_id)
        else {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ServiceNotAvailable,
                "message store is not available",
            )));
        };
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
        let now = current_millis();
        let revive_qid = ExtraInfoUtil::get_revive_qid(extra_info.as_slice())?;
        let ck_result = match self
            .append_check_point(
                &request_header,
                revive_qid,
                request_header.queue_id,
                request_header.offset,
                now,
                CheetahString::from_string(ExtraInfoUtil::get_broker_name(extra_info.as_slice())?),
            )
            .await?
        {
            Some(result) => result,
            None => {
                return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::ServiceNotAvailable,
                    "message store is not available",
                )));
            }
        };
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
        self.context.broker_stats_manager.inc_broker_ack_nums(1);
        self.context.broker_stats_manager.inc_group_ack_nums(
            request_header.consumer_group.as_str(),
            request_header.topic.as_str(),
            1,
        );
        if self.context.pop_buffer.add_ack(rq_id, &ack_msg) {
            return Ok(());
        }
        let mut inner = MessageExtBrokerInner::default();
        inner.set_topic(self.context.policy.revive_topic.clone());
        inner.set_body(Bytes::from(ack_msg.encode()?));
        inner.message_ext_inner.queue_id = rq_id;
        inner.set_tags(CheetahString::from_static_str(PopAckConstants::ACK_TAG));
        inner.message_ext_inner.born_timestamp = current_millis() as i64;
        inner.message_ext_inner.born_host = self.context.policy.store_host;
        inner.message_ext_inner.store_host = self.context.policy.store_host;
        let deliver_time_ms = ExtraInfoUtil::get_pop_time(extra_info)? + ExtraInfoUtil::get_invisible_time(extra_info)?;
        inner.set_delay_time_ms(deliver_time_ms as u64);
        inner.message_ext_inner.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from(PopMessageProcessor::<MS>::gen_ack_unique_id(&ack_msg)),
        );
        inner.properties_string = message_decoder::message_properties_to_string(inner.get_properties());
        let result = match self.context.message_store.put_message(inner).await {
            Ok(result) => result,
            Err(MessageStoreUnavailable) => return Ok(()),
        };
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
    ) -> rocketmq_error::RocketMQResult<Option<PutMessageResult>> {
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
        inner.set_topic(self.context.policy.revive_topic.clone());
        inner.set_body(Bytes::from(ck.encode()?));
        inner.message_ext_inner.queue_id = revive_qid;
        inner.set_tags(CheetahString::from_static_str(PopAckConstants::ACK_TAG));
        inner.message_ext_inner.born_timestamp = current_millis() as i64;
        inner.message_ext_inner.born_host = self.context.policy.store_host;
        inner.message_ext_inner.store_host = self.context.policy.store_host;
        let deliver_time_ms = ck.get_revive_time() - PopAckConstants::ACK_TIME_INTERVAL;
        let deliver_time_ms = if deliver_time_ms > 0 { deliver_time_ms as u64 } else { 0 };
        inner.set_delay_time_ms(deliver_time_ms);
        inner.message_ext_inner.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from(PopMessageProcessor::<MS>::gen_ck_unique_id(&ck)),
        );
        inner.properties_string = message_decoder::message_properties_to_string(inner.get_properties());
        let Ok(put_message_result) = self.context.message_store.put_message(inner).await else {
            return Ok(None);
        };
        if self.context.policy.enable_pop_log {
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
        Ok(Some(put_message_result))
    }

    async fn process_change_invisible_time_for_order(
        &mut self,
        request_header: &ChangeInvisibleTimeRequestHeader,
        extra_info: &[String],
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let pop_time = ExtraInfoUtil::get_pop_time(extra_info)?;
        let old_offset = self.context.consumer_offset_query.query_offset(
            &request_header.consumer_group,
            &request_header.topic,
            request_header.queue_id,
        );
        if old_offset > request_header.offset {
            return Ok(Some(RemotingCommand::create_response_command()));
        }
        while !self
            .context
            .queue_lock_manager
            .try_lock(
                &request_header.consumer_group,
                &request_header.topic,
                request_header.queue_id,
            )
            .await
        {}
        let old_offset = self.context.consumer_offset_query.query_offset(
            &request_header.consumer_group,
            &request_header.topic,
            request_header.queue_id,
        );
        if old_offset > request_header.offset {
            return Ok(Some(RemotingCommand::create_response_command()));
        }
        let next_visible_time = current_millis() + request_header.invisible_time as u64;
        let updated = self.context.consumer_order_info.update_next_visible_time(
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
        self.context
            .queue_lock_manager
            .unlock(
                &request_header.consumer_group,
                &request_header.topic,
                request_header.queue_id,
            )
            .await;
        if !updated {
            return Ok(Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::ServiceNotAvailable,
                "consumer order info is not available",
            )));
        }
        Ok(Some(RemotingCommand::create_response_command_with_header(
            response_header,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_store::message_store::GenericMessageStore;

    #[test]
    fn change_invisible_time_policy_captures_only_required_startup_values() {
        let mut broker_config = BrokerConfig::default();
        broker_config.broker_identity.broker_cluster_name = CheetahString::from_static_str("cluster-a");
        broker_config.enable_pop_log = true;
        let store_host = "127.0.0.1:10911".parse().expect("valid store host");

        let policy = ChangeInvisibleTimePolicy::from_config(&broker_config, store_host);

        assert_eq!(
            policy.revive_topic,
            PopAckConstants::build_cluster_revive_topic("cluster-a")
        );
        assert_eq!(policy.store_host, store_host);
        assert!(policy.enable_pop_log);
    }

    #[tokio::test]
    async fn change_invisible_time_weak_capabilities_fail_closed_after_provider_shutdown() {
        let store = ChangeInvisibleTimeStoreCapability::<GenericMessageStore> {
            escape_bridge: Weak::new(),
        };
        let pop_buffer = ChangeInvisibleTimePopCapability::<GenericMessageStore> {
            merge_service: Weak::new(),
        };
        let order = ChangeInvisibleTimeOrderCapability { manager: Weak::new() };
        let topic = CheetahString::from_static_str("topic-a");
        let group = CheetahString::from_static_str("group-a");

        assert!(store.queue_offsets(&topic, 0).is_err());
        assert!(store.put_message(MessageExtBrokerInner::default()).await.is_err());
        assert!(!pop_buffer.add_ack(0, &AckMsg::default()));
        assert!(!order.update_next_visible_time(&topic, &group, 0, 0, 0, 1));
    }

    #[test]
    fn change_invisible_time_source_uses_only_explicit_capabilities() {
        let source = include_str!("change_invisible_time_processor.rs");

        assert!(!source.contains(concat!("rocketmq_rust::", "ArcMut")));
        assert!(!source.contains(concat!("BrokerRuntime", "Inner")));
        assert!(!source.contains(concat!("pop_message_processor", ": Arc<")));
        assert!(source.contains("ChangeInvisibleTimeProcessorContext<MS>"));
        assert!(source.contains("Weak<EscapeBridge<MS>>"));
        assert!(source.contains("Weak<PopBufferMergeService<MS>>"));
        assert!(source.contains("Weak<ConsumerOrderInfoManager>"));
        assert!(source.contains("queue_lock_manager: QueueLockManager"));
    }
}
