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
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rand::Rng;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_remoting::protocol::header::notification_response_header::NotificationResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::long_polling::long_polling_service::pop_long_polling_service::PopLongPollingService;
use crate::long_polling::polling_header::PollingHeader;
use crate::long_polling::polling_result::PollingResult;

pub struct NotificationProcessor<MS> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    pop_long_polling_service: PopLongPollingService<MS, NotificationProcessor<MS>>,
}

impl<MS: MessageStore> NotificationProcessor<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> ArcMut<Self> {
        let mut this = ArcMut::new(Self {
            broker_runtime_inner: broker_runtime_inner.clone(),
            pop_long_polling_service: PopLongPollingService::new(broker_runtime_inner, true),
        });
        let this_clone = this.clone();
        this.pop_long_polling_service.set_processor(this_clone);
        this
    }

    pub fn start(&mut self) {
        warn!("NotificationProcessor started unimplemented, need to implement it");
    }

    pub fn shutdown(&mut self) {
        warn!("NotificationProcessor shutdown unimplemented, need to implement it");
    }

    pub fn notify_message_arriving_simple(&self, topic: &CheetahString, queue_id: i32) {
        self.pop_long_polling_service
            .notify_message_arriving_with_retry_topic(topic, queue_id);
    }

    #[allow(unused_variables)]
    pub fn notify_message_arriving(
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

    async fn has_msg_from_topic_name(
        &self,
        topic_name: &CheetahString,
        random_q: i32,
        request_header: &NotificationRequestHeader,
    ) -> bool {
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(topic_name);
        self.has_msg_from_topic(topic_config.as_ref(), random_q, request_header)
            .await
    }

    async fn has_msg_from_topic(
        &self,
        topic_config: Option<&TopicConfig>,
        random_q: i32,
        request_header: &NotificationRequestHeader,
    ) -> bool {
        if let Some(tc) = topic_config {
            for i in 0..tc.read_queue_nums {
                let queue_id = (random_q as u32 + i) % tc.read_queue_nums;
                if self
                    .has_msg_from_queue(
                        tc.topic_name.as_ref().unwrap(),
                        request_header,
                        queue_id as i32,
                    )
                    .await
                {
                    return true;
                }
            }
        }
        false
    }

    async fn has_msg_from_queue(
        &self,
        target_topic: &str,
        request_header: &NotificationRequestHeader,
        queue_id: i32,
    ) -> bool {
        if request_header.order
            && self
                .broker_runtime_inner
                .consumer_order_info_manager()
                .check_block(
                    request_header.attempt_id.as_ref().unwrap(),
                    &request_header.topic,
                    &request_header.consumer_group,
                    queue_id,
                    0,
                )
        {
            return false;
        }

        let offset = self
            .get_pop_offset(
                &target_topic.to_string().into(),
                &request_header.consumer_group,
                queue_id,
            )
            .await;
        let rest_num = self
            .broker_runtime_inner
            .message_store_unchecked()
            .get_max_offset_in_queue(&target_topic.to_string().into(), queue_id)
            - offset;
        rest_num > 0
    }

    async fn get_pop_offset(
        &self,
        topic: &CheetahString,
        cid: &CheetahString,
        queue_id: i32,
    ) -> i64 {
        let mut offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(cid, topic, queue_id);
        if offset < 0 {
            offset = self
                .broker_runtime_inner
                .message_store_unchecked()
                .get_min_offset_in_queue(topic, queue_id);
        }
        let buffer_offset = self
            .broker_runtime_inner
            .pop_message_processor_unchecked()
            .pop_buffer_merge_service()
            .get_latest_offset_full(topic, cid, queue_id)
            .await;
        if buffer_offset < 0 {
            offset
        } else {
            buffer_offset.max(offset)
        }
    }
}

impl<MS> RequestProcessor for NotificationProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        mut request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        const BORN_TIME: &str = "bornTime";
        let now = get_current_millis();
        request.add_ext_field_if_not_exist(BORN_TIME, now.to_string());
        if request
            .ext_fields()
            .unwrap()
            .get(BORN_TIME)
            .map(|v| v == "0")
            .unwrap_or(false)
        {
            request.add_ext_field(BORN_TIME, now.to_string());
        }
        let channel = ctx.channel();

        let mut response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<NotificationRequestHeader>()?;

        if !PermName::is_readable(
            self.broker_runtime_inner
                .broker_config()
                .broker_permission(),
        ) {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "the broker[{}] peeking message is forbidden",
                self.broker_runtime_inner.broker_config().broker_ip1()
            ));
            return Ok(Some(response));
        }

        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(&request_header.topic);
        if topic_config.is_none() {
            error!(
                "The topic {} not exist, consumer: {}",
                request_header.topic,
                channel.remote_address()
            );
            response.set_code_ref(ResponseCode::TopicNotExist);
            response.set_remark_mut(format!(
                "topic[{}] not exist, apply first please! {}",
                request_header.topic,
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            ));
            return Ok(Some(response));
        }
        let topic_config = topic_config.unwrap();

        if !PermName::is_readable(topic_config.perm) {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "the topic[{}] peeking message is forbidden",
                request_header.topic
            ));
            return Ok(Some(response));
        }

        if request_header.queue_id >= topic_config.get_read_queue_nums() as i32 {
            let error_info = format!(
                "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{:?}]",
                request_header.queue_id,
                request_header.topic,
                topic_config.get_read_queue_nums(),
                channel.remote_address()
            );
            warn!("{}", error_info);
            response.set_code_ref(ResponseCode::SystemError);
            response.set_remark_mut(&error_info);
            return Ok(Some(response));
        }

        let subscription_group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(&request_header.consumer_group);
        if subscription_group_config.is_none() {
            response.set_code_ref(ResponseCode::SubscriptionGroupNotExist);
            response.set_remark_mut(format!(
                "subscription group [{}] does not exist, {}",
                request_header.consumer_group,
                FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
            ));
            return Ok(Some(response));
        }
        let subscription_group_config = subscription_group_config.unwrap();

        if !subscription_group_config.consume_enable() {
            response.set_code_ref(ResponseCode::NoPermission);
            response.set_remark_mut(format!(
                "subscription group no permission, {}",
                request_header.consumer_group
            ));
            return Ok(Some(response));
        }

        let random_q = rand::rng().random_range(0..100);
        let mut has_msg = false;
        let need_retry = random_q % 5 == 0;
        let broker_config = self.broker_runtime_inner.broker_config();

        if need_retry {
            let retry_topic = KeyBuilder::build_pop_retry_topic(
                request_header.topic.as_str(),
                request_header.consumer_group.as_str(),
                broker_config.enable_retry_topic_v2,
            )
            .into();
            has_msg = self
                .has_msg_from_topic_name(&retry_topic, random_q, &request_header)
                .await;
            if !has_msg
                && broker_config.enable_retry_topic_v2
                && broker_config.retrieve_message_from_pop_retry_topic_v1
            {
                let retry_topic_v1 = KeyBuilder::build_pop_retry_topic_v1(
                    request_header.topic.as_str(),
                    request_header.consumer_group.as_str(),
                )
                .into();
                has_msg = self
                    .has_msg_from_topic_name(&retry_topic_v1, random_q, &request_header)
                    .await;
            }
        }
        if !has_msg {
            if request_header.queue_id < 0 {
                has_msg = self
                    .has_msg_from_topic(Some(&topic_config), random_q, &request_header)
                    .await;
            } else {
                let queue_id = request_header.queue_id;
                has_msg = self
                    .has_msg_from_queue(
                        topic_config.topic_name.as_ref().unwrap(),
                        &request_header,
                        queue_id,
                    )
                    .await;
            }
            if !need_retry && !has_msg {
                let retry_topic = KeyBuilder::build_pop_retry_topic(
                    request_header.topic.as_str(),
                    request_header.consumer_group.as_str(),
                    broker_config.enable_retry_topic_v2,
                )
                .into();
                has_msg = self
                    .has_msg_from_topic_name(&retry_topic, random_q, &request_header)
                    .await;
                if !has_msg
                    && broker_config.enable_retry_topic_v2
                    && broker_config.retrieve_message_from_pop_retry_topic_v1
                {
                    let retry_topic_v1 = KeyBuilder::build_pop_retry_topic_v1(
                        request_header.topic.as_str(),
                        request_header.consumer_group.as_str(),
                    )
                    .into();
                    has_msg = self
                        .has_msg_from_topic_name(&retry_topic_v1, random_q, &request_header)
                        .await;
                }
            }
        }

        if !has_msg
            && self.pop_long_polling_service.polling(
                ctx,
                request,
                PollingHeader::new_from_notification_request_header(&request_header),
                None,
                None,
            ) == PollingResult::PollingSuc
        {
            return Ok(None);
        }

        response.set_code_ref(ResponseCode::Success);
        response.set_command_custom_header_ref(NotificationResponseHeader { has_msg });

        Ok(Some(response))
    }
}
