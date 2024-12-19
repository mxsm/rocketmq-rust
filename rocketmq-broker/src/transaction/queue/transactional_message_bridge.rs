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
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::log_file::MAX_PULL_MSG_SIZE;
use rocketmq_store::stats::broker_stats_manager::BrokerStatsManager;
use tokio::sync::Mutex;
use tracing::error;

use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;

pub struct TransactionalMessageBridge<MS> {
    pub(crate) op_queue_map: Arc<Mutex<HashMap<i32, MessageQueue>>>,
    pub(crate) message_store: ArcMut<MS>,
    pub(crate) store_host: SocketAddr,
    pub(crate) broker_stats_manager: Arc<BrokerStatsManager>,
    pub(crate) consumer_offset_manager: ConsumerOffsetManager,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) topic_config_manager: TopicConfigManager,
}

impl<MS> TransactionalMessageBridge<MS>
where
    MS: MessageStore,
{
    pub fn new(
        message_store: ArcMut<MS>,
        broker_stats_manager: Arc<BrokerStatsManager>,
        consumer_offset_manager: ConsumerOffsetManager,
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: TopicConfigManager,
    ) -> Self {
        let store_host = format!("{}:{}", broker_config.broker_ip1, broker_config.listen_port)
            .parse::<SocketAddr>()
            .expect("parse store host failed");
        Self {
            op_queue_map: Arc::new(Mutex::new(HashMap::new())),
            message_store,
            store_host,
            broker_stats_manager,
            consumer_offset_manager,
            broker_config,
            topic_config_manager,
        }
    }
}

impl<MS> TransactionalMessageBridge<MS>
where
    MS: MessageStore,
{
    pub(crate) fn fetch_consume_offset(&self, mq: &MessageQueue) -> i64 {
        let group =
            CheetahString::from_static_str(TransactionalMessageUtil::build_consumer_group());
        let topic = mq.get_topic_cs();
        let queue_id = mq.get_queue_id();
        let mut offset = self
            .consumer_offset_manager
            .query_offset(&group, topic, queue_id);
        if offset == -1 {
            offset = self.message_store.get_min_offset_in_queue(topic, queue_id);
        }
        offset
    }

    pub fn fetch_message_queues(&mut self, topic: &CheetahString) -> HashSet<MessageQueue> {
        let mut message_queues = HashSet::new();
        let topic_config = self.select_topic_config(topic);
        let broker_name = self.broker_config.broker_name.clone();
        if let Some(topic_config) = topic_config {
            for i in 0..topic_config.read_queue_nums {
                let mq = MessageQueue::from_parts(topic, broker_name.as_str(), i as i32);
                message_queues.insert(mq);
            }
        }
        message_queues
    }

    pub fn update_consume_offset(&self, mq: &MessageQueue, offset: i64) {
        self.consumer_offset_manager.commit_offset(
            self.store_host,
            &CheetahString::from_static_str(TransactionalMessageUtil::build_consumer_group()),
            mq.get_topic_cs(),
            mq.get_queue_id(),
            offset,
        );
    }

    pub async fn get_half_message(
        &self,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Option<PullResult> {
        self.get_message(
            &CheetahString::from_static_str(TransactionalMessageUtil::build_consumer_group()),
            &CheetahString::from_static_str(TransactionalMessageUtil::build_half_topic()),
            queue_id,
            offset,
            nums,
            None,
        )
        .await
    }

    pub async fn get_op_message(
        &self,
        queue_id: i32,
        offset: i64,
        nums: i32,
    ) -> Option<PullResult> {
        self.get_message(
            &CheetahString::from_static_str(TransactionalMessageUtil::build_consumer_group()),
            &CheetahString::from_static_str(TransactionalMessageUtil::build_op_topic()),
            queue_id,
            offset,
            nums,
            None,
        )
        .await
    }

    async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
        _sub: Option<SubscriptionData>,
    ) -> Option<PullResult> {
        let get_message_result = self
            .message_store
            .get_message(
                group,
                topic,
                queue_id,
                offset,
                nums,
                MAX_PULL_MSG_SIZE,
                None,
            )
            .await;

        if let Some(get_message_result) = get_message_result {
            let (pull_status, msg_found_list) = match get_message_result.status().unwrap() {
                GetMessageStatus::Found => {
                    let msg_list = Self::decode_msg_list(&get_message_result);
                    (PullStatus::Found, Some(msg_list))
                }
                GetMessageStatus::NoMatchedMessage => (PullStatus::NoMatchedMsg, None),
                GetMessageStatus::OffsetOverflowOne | GetMessageStatus::NoMessageInQueue => {
                    (PullStatus::NoNewMsg, None)
                }
                GetMessageStatus::MessageWasRemoving
                | GetMessageStatus::OffsetFoundNull
                | GetMessageStatus::OffsetOverflowBadly
                | GetMessageStatus::OffsetTooSmall
                | GetMessageStatus::NoMatchedLogicQueue => (PullStatus::OffsetIllegal, None),

                GetMessageStatus::OffsetReset => (PullStatus::NoNewMsg, None),
            };

            Some(PullResult {
                pull_status,
                next_begin_offset: get_message_result.next_begin_offset() as u64,
                min_offset: get_message_result.min_offset() as u64,
                max_offset: get_message_result.max_offset() as u64,
                msg_found_list: Some(
                    msg_found_list
                        .unwrap_or_default()
                        .into_iter()
                        .map(ArcMut::new)
                        .collect(),
                ),
            })
        } else {
            error!(
                "Get message from store return null. topic={}, groupId={}, requestOffset={}",
                topic, group, offset
            );
            None
        }
    }

    fn decode_msg_list(get_message_result: &GetMessageResult) -> Vec<MessageClientExt> {
        let mut found_list = Vec::new();
        for bb in get_message_result.message_mapped_list() {
            let data = &bb.mapped_file.as_ref().unwrap().get_mapped_file()
                [bb.start_offset as usize..(bb.start_offset + bb.size as u64) as usize];
            let mut bytes = Bytes::copy_from_slice(data);
            let msg_ext = message_decoder::decode_client(&mut bytes, true, false, false, false);
            if let Some(msg_ext) = msg_ext {
                found_list.push(msg_ext);
            }
        }
        found_list
    }

    pub fn select_topic_config(&mut self, topic: &CheetahString) -> Option<TopicConfig> {
        let mut topic_config = self.topic_config_manager.select_topic_config(topic);
        if topic_config.is_none() {
            topic_config = self
                .topic_config_manager
                .create_topic_in_send_message_back_method(
                    topic,
                    1,
                    PermName::PERM_WRITE | PermName::PERM_READ,
                    false,
                    0,
                );
        }
        topic_config
    }

    pub async fn put_half_message(
        &mut self,
        mut message: MessageExtBrokerInner,
    ) -> PutMessageResult {
        Self::parse_half_message_inner(&mut message);
        self.message_store.put_message(message).await
    }

    pub fn parse_half_message_inner(message: &mut MessageExtBrokerInner) {
        let uniq_id = message.get_user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ));
        if let Some(uniq_id) = uniq_id {
            MessageAccessor::put_property(
                message,
                CheetahString::from_static_str(TransactionalMessageUtil::TRANSACTION_ID),
                uniq_id,
            );
        }
        let topic = message.get_topic().clone();
        MessageAccessor::put_property(
            message,
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
            topic,
        );
        MessageAccessor::put_property(
            message,
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID),
            CheetahString::from_string(message.message_ext_inner.queue_id.to_string()),
        );
        message.message_ext_inner.sys_flag = MessageSysFlag::reset_transaction_value(
            message.message_ext_inner.sys_flag,
            MessageSysFlag::TRANSACTION_NOT_TYPE,
        );
        message.set_topic(CheetahString::from_static_str(
            TransactionalMessageUtil::build_half_topic(),
        ));
        message.message_ext_inner.queue_id = 0;
        let properties_to_string =
            message_decoder::message_properties_to_string(message.get_properties());
        message.properties_string = properties_to_string;
    }

    pub fn renew_immunity_half_message_inner(msg_ext: &MessageExt) -> MessageExtBrokerInner {
        let mut message_inner = Self::renew_half_message_inner(msg_ext);
        let queue_offset_from_prepare = msg_ext.get_user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
        ));
        if let Some(queue_offset_from_prepare) = queue_offset_from_prepare {
            MessageAccessor::put_property(
                &mut message_inner,
                CheetahString::from_static_str(
                    MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                ),
                queue_offset_from_prepare,
            );
        } else {
            MessageAccessor::put_property(
                &mut message_inner,
                CheetahString::from_static_str(
                    MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                ),
                CheetahString::from_string(msg_ext.queue_offset.to_string()),
            );
        }
        let properties_to_string =
            message_decoder::message_properties_to_string(message_inner.get_properties());
        message_inner.properties_string = properties_to_string;
        message_inner
    }

    #[inline]
    pub fn renew_half_message_inner(msg_ext: &MessageExt) -> MessageExtBrokerInner {
        let mut inner = MessageExtBrokerInner {
            message_ext_inner: msg_ext.clone(),
            properties_string: message_decoder::message_properties_to_string(
                msg_ext.get_properties(),
            ),
            tags_code: MessageExtBrokerInner::tags_string_to_tags_code(
                msg_ext.get_tags().unwrap_or_default().as_str(),
            ),
            encoded_buff: None,
            encode_completed: false,
            version: Default::default(),
        };
        inner.set_wait_store_msg_ok(false);
        MessageAccessor::set_properties(&mut inner, msg_ext.get_properties().clone());
        inner
    }

    fn make_op_message_inner(
        &self,
        message: &Message,
        message_queue: &MessageQueue,
    ) -> MessageExtBrokerInner {
        let mut msg_inner = MessageExtBrokerInner::default();
        msg_inner.set_topic(message.get_topic().to_owned());
        msg_inner.set_body(message.get_body().expect("message body is empty").clone());
        msg_inner.message_ext_inner.queue_id = message_queue.get_queue_id();
        msg_inner.set_tags(message.get_tags().unwrap_or_default());
        msg_inner.tags_code = MessageExtBrokerInner::tags_string_to_tags_code(
            message.get_tags().unwrap_or_default().as_str(),
        );
        msg_inner.message_ext_inner.sys_flag = 0;
        MessageAccessor::set_properties(&mut msg_inner, message.get_properties().clone());
        msg_inner.properties_string =
            MessageDecoder::message_properties_to_string(msg_inner.get_properties());

        msg_inner.message_ext_inner.born_timestamp = get_current_millis() as i64;
        msg_inner.message_ext_inner.born_host = self.store_host;
        msg_inner.message_ext_inner.store_host = self.store_host;
        msg_inner.set_wait_store_msg_ok(false);
        MessageClientIDSetter::set_uniq_id(&mut msg_inner);
        msg_inner
    }

    pub fn look_message_by_offset(&self, offset: i64) -> Option<MessageExt> {
        self.message_store.look_message_by_offset(offset)
    }

    pub async fn write_op(&self, queue_id: i32, message: Message) -> bool {
        let mut op_queue_map = self.op_queue_map.lock().await;
        let op_queue = op_queue_map.entry(queue_id).or_insert_with(|| {
            get_op_queue_by_half(queue_id, self.broker_config.broker_name.clone())
        });
        let inner = self.make_op_message_inner(&message, op_queue);
        let result = self.put_message_return_result(inner).await;
        result.put_message_status() == PutMessageStatus::PutOk
    }

    pub async fn put_message_return_result(
        &self,
        message_inner: MessageExtBrokerInner,
    ) -> PutMessageResult {
        let result = self
            .message_store
            .mut_from_ref()
            .put_message(message_inner)
            .await;
        if result.put_message_status() == PutMessageStatus::PutOk {
            //nothing to do
        }
        result
    }
}

#[inline]
fn get_op_queue_by_half(queue_id: i32, broker_name: CheetahString) -> MessageQueue {
    MessageQueue::from_parts(
        TransactionalMessageUtil::build_op_topic(),
        broker_name,
        queue_id,
    )
}
