/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::remoting_error::RemotingError::RemotingCommandError;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tracing::error;
use tracing::info;

use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::offset::manager::consumer_order_info_manager::ConsumerOrderInfoManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub struct ChangeInvisibleTimeProcessor<MS> {
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: TopicConfigManager,
    message_store: ArcMut<MS>,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
    broker_stats_manager: Arc<BrokerStatsManager>,
}

impl<MS> ChangeInvisibleTimeProcessor<MS> {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: TopicConfigManager,
        message_store: ArcMut<MS>,
        consumer_offset_manager: Arc<ConsumerOffsetManager>,
        consumer_order_info_manager: Arc<ConsumerOrderInfoManager>,
        broker_stats_manager: Arc<BrokerStatsManager>,
    ) -> Self {
        ChangeInvisibleTimeProcessor {
            broker_config,
            topic_config_manager,
            message_store,
            consumer_offset_manager,
            consumer_order_info_manager,
            broker_stats_manager,
        }
    }
}

impl<MS> ChangeInvisibleTimeProcessor<MS>
where
    MS: MessageStore,
{
    pub async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: RemotingCommand,
    ) -> crate::Result<Option<RemotingCommand>> {
        self.process_request_inner(channel, ctx, request, true)
            .await
    }

    pub async fn process_request_inner(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: RemotingCommand,
        _broker_allow_suspend: bool,
    ) -> crate::Result<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<ChangeInvisibleTimeRequestHeader>()
            .map_err(|e| RemotingCommandError(e.to_string()))?;
        let topic_config = self
            .topic_config_manager
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            error!(
                "The topic {} not exist, consumer: {} ",
                request_header.topic,
                channel.remote_address()
            );

            return Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
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
        if request_header.queue_id >= topic_config.read_queue_nums as i32
            || request_header.queue_id < 0
        {
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.read_queue_nums,
                channel.remote_address()
            );

            error!("{}", error_info);

            return Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::MessageIllegal,
                    error_info,
                ),
            ));
        }
        let mix_offset = self
            .message_store
            .get_min_offset_in_queue(&request_header.topic, request_header.queue_id);
        let max_offset = self
            .message_store
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

            return Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::NoMessage,
                    info,
                ),
            ));
        }
        let extra_info = ExtraInfoUtil::split(&request_header.extra_info)?;
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
                now,
                CheetahString::from_string(ExtraInfoUtil::get_broker_name(extra_info.as_slice())?),
            )
            .await;
        match ck_result.put_message_status() {
            PutMessageStatus::PutOk
            | PutMessageStatus::FlushDiskTimeout
            | PutMessageStatus::FlushSlaveTimeout
            | PutMessageStatus::SlaveNotAvailable => {}
            _ => {
                error!(
                    "change Invisible, put new ck error: {}",
                    ck_result.put_message_status()
                );
                return Ok(Some(
                    RemotingCommand::create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        format!(
                            "append check point error, status: {:?}",
                            ck_result.put_message_status()
                        ),
                    ),
                ));
            }
        }
        if let Err(e) = self
            .ack_origin(&request_header, extra_info.as_slice())
            .await
        {
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
        _request_header: &ChangeInvisibleTimeRequestHeader,
        _extra_info: &[String],
    ) -> crate::Result<()> {
        unimplemented!("ChangeInvisibleTimeProcessor ack_origin")
    }

    async fn append_check_point(
        &mut self,
        _request_header: &ChangeInvisibleTimeRequestHeader,
        _revive_qid: i32,
        _pop_time: u64,
        _broker_name: CheetahString,
    ) -> PutMessageResult {
        unimplemented!("ChangeInvisibleTimeProcessor append_check_point")
    }

    async fn process_change_invisible_time_for_order(
        &mut self,
        _request_header: &ChangeInvisibleTimeRequestHeader,
        _extra_info: &[String],
    ) -> crate::Result<Option<RemotingCommand>> {
        unimplemented!("ChangeInvisibleTimeProcessor process_change_invisible_time_for_order")
    }
}
