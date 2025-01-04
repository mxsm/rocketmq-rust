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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::common::TopicFilterType;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::AppendMessageStatus;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use tracing::error;
use tracing::info;

use crate::failover::escape_bridge::EscapeBridge;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetManager;
use crate::topic::manager::topic_config_manager::TopicConfigManager;

pub struct PopReviveService<MS> {
    queue_id: i32,
    revive_topic: CheetahString,
    current_revive_message_timestamp: i64,
    should_run_pop_revive: bool,
    inflight_revive_request_map: Arc<parking_lot::Mutex<BTreeMap<PopCheckPoint, (i64, bool)>>>,
    revive_offset: i64,
    ck_rewrite_intervals_in_seconds: [i32; 17],
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: TopicConfigManager,
    consumer_offset_manager: Arc<ConsumerOffsetManager>,
    escape_bridge: ArcMut<EscapeBridge<MS>>,
    message_store: ArcMut<MS>,
    should_start_time: Arc<AtomicU64>,
}
impl<MS: MessageStore> PopReviveService<MS> {
    pub fn new(
        revive_topic: CheetahString,
        queue_id: i32,
        revive_offset: i64,
        broker_config: Arc<BrokerConfig>,
        topic_config_manager: TopicConfigManager,
        consumer_offset_manager: Arc<ConsumerOffsetManager>,
        escape_bridge: ArcMut<EscapeBridge<MS>>,
        message_store: ArcMut<MS>,
        should_start_time: Arc<AtomicU64>,
    ) -> Self {
        Self {
            queue_id,
            revive_topic,
            current_revive_message_timestamp: -1,
            should_run_pop_revive: false,
            inflight_revive_request_map: Arc::new(Default::default()),
            revive_offset,
            ck_rewrite_intervals_in_seconds: [
                10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ],
            broker_config,
            topic_config_manager,
            consumer_offset_manager,
            escape_bridge,
            message_store,
            should_start_time,
        }
    }

    async fn revive_retry(
        &mut self,
        pop_check_point: &PopCheckPoint,
        message_ext: &MessageExt,
    ) -> bool {
        let mut msg_inner = MessageExtBrokerInner::default();
        if pop_check_point
            .topic
            .starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
        {
            msg_inner.set_topic(CheetahString::from_string(
                KeyBuilder::build_pop_retry_topic(
                    pop_check_point.topic.as_str(),
                    pop_check_point.cid.as_str(),
                    self.broker_config.enable_retry_topic_v2,
                ),
            ));
        } else {
            msg_inner.set_topic(pop_check_point.topic.clone());
        }
        if let Some(bytes) = message_ext.body() {
            msg_inner.set_body(bytes);
        }
        msg_inner.message_ext_inner.queue_id = 0;
        if let Some(tags) = message_ext.get_tags() {
            msg_inner.set_tags(tags);
        } else {
            MessageAccessor::set_properties(&mut msg_inner, HashMap::new());
        }
        msg_inner.message_ext_inner.born_timestamp = message_ext.born_timestamp;
        msg_inner.set_flag(message_ext.get_flag());
        msg_inner.message_ext_inner.sys_flag = message_ext.sys_flag;
        msg_inner.message_ext_inner.born_host = message_ext.born_host;
        msg_inner.message_ext_inner.store_host = message_ext.store_host;
        msg_inner.message_ext_inner.reconsume_times = message_ext.reconsume_times + 1;
        MessageAccessor::set_properties(&mut msg_inner, message_ext.properties().clone());
        if message_ext.reconsume_times() == 0
            || msg_inner
                .get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_FIRST_POP_TIME,
                ))
                .is_none()
        {
            msg_inner.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                CheetahString::from(pop_check_point.pop_time.to_string()),
            );
        }
        msg_inner.properties_string =
            message_decoder::message_properties_to_string(msg_inner.get_properties());
        self.add_retry_topic_if_not_exist(msg_inner.get_topic(), &pop_check_point.cid);
        let put_message_result = self
            .escape_bridge
            .put_message_to_specific_queue(msg_inner)
            .await;
        if put_message_result.append_message_result().is_none()
            || put_message_result.append_message_result().unwrap().status
                != AppendMessageStatus::PutOk
        {
            return false;
        }
        true
    }

    pub fn add_retry_topic_if_not_exist(
        &mut self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) {
        if let Some(_topic_config) = self.topic_config_manager.select_topic_config(topic) {
            return;
        }
        let mut topic_config = TopicConfig::new(topic.clone());
        topic_config.read_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.write_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.topic_filter_type = TopicFilterType::SingleTag;
        topic_config.perm = 6;
        topic_config.topic_sys_flag = 0;
        self.topic_config_manager
            .update_topic_config(&mut topic_config);
        self.init_pop_retry_offset(topic, consumer_group);
    }

    fn init_pop_retry_offset(&mut self, topic: &CheetahString, consumer_group: &CheetahString) {
        let offset = self
            .consumer_offset_manager
            .query_offset(consumer_group, topic, 0);
        if offset < 0 {
            self.consumer_offset_manager.commit_offset(
                "initPopRetryOffset".into(),
                consumer_group,
                topic,
                0,
                0,
            );
        }
    }

    pub(crate) async fn get_revive_message(
        &self,
        offset: i64,
        queue_id: i32,
    ) -> Option<Vec<ArcMut<MessageExt>>> {
        let pull_result = self
            .get_message(
                &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                &self.revive_topic,
                queue_id,
                offset,
                32,
                true,
            )
            .await?;
        if reach_tail(&pull_result, offset) {
            //nothing to do
        } else if pull_result.pull_status == PullStatus::OffsetIllegal
            || pull_result.pull_status == PullStatus::NoMatchedMsg
        {
            if !self.should_run_pop_revive {
                return None;
            }
            self.consumer_offset_manager.commit_offset(
                CheetahString::from_static_str(PopAckConstants::LOCAL_HOST),
                &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                &self.revive_topic,
                queue_id,
                pull_result.next_begin_offset as i64 - 1,
            );
        }
        pull_result.msg_found_list
    }

    pub async fn get_message(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        nums: i32,
        de_compress_body: bool,
    ) -> Option<PullResult> {
        let get_message_result = self
            .message_store
            .get_message(
                group,
                topic,
                queue_id,
                offset,
                nums,
                128 * 1024 * 1024,
                None,
            )
            .await;
        if let Some(get_message_result_) = get_message_result {
            let next_begin_offset = get_message_result_.next_begin_offset();
            let min_offset = get_message_result_.min_offset();
            let max_offset = get_message_result_.max_offset();
            let pull_status = match get_message_result_.status().unwrap() {
                GetMessageStatus::Found => {
                    let found_list = decode_msg_list(get_message_result_, de_compress_body);
                    (PullStatus::Found, Some(found_list))
                }
                GetMessageStatus::NoMatchedMessage => (PullStatus::NoMatchedMsg, None),

                GetMessageStatus::NoMessageInQueue | GetMessageStatus::OffsetReset => {
                    (PullStatus::NoNewMsg, None)
                }

                GetMessageStatus::MessageWasRemoving
                | GetMessageStatus::NoMatchedLogicQueue
                | GetMessageStatus::OffsetFoundNull
                | GetMessageStatus::OffsetOverflowBadly
                | GetMessageStatus::OffsetTooSmall
                | GetMessageStatus::OffsetOverflowOne => (PullStatus::OffsetIllegal, None),
            };
            Some(PullResult::new(
                pull_status.0,
                next_begin_offset as u64,
                min_offset as u64,
                max_offset as u64,
                pull_status.1,
            ))
        } else {
            let max_queue_offset = self.message_store.get_max_offset_in_queue(topic, queue_id);
            if max_queue_offset > offset {
                error!(
                    "get message from store return null. topic={}, groupId={}, requestOffset={}, \
                     maxQueueOffset={}",
                    topic, group, offset, max_queue_offset
                );
            }
            None
        }
    }

    pub fn start(mut this: ArcMut<Self>) {
        tokio::spawn(async move {
            let mut slow = 1;
            loop {
                if get_current_millis() < this.should_start_time.load(Relaxed) {
                    info!(
                        "PopReviveService Ready to run after {}",
                        this.should_start_time.load(Relaxed)
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    this.broker_config.revive_interval,
                ))
                .await;
                if !this.should_run_pop_revive {
                    info!(
                        "skip start revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );
                    continue;
                }
                if !this
                    .message_store
                    .get_message_store_config()
                    .timer_wheel_enable
                {
                    info!("skip revive topic because timerWheelEnable is false");
                    continue;
                }
                info!(
                    "start revive topic={}, reviveQueueId={}",
                    this.revive_topic, this.queue_id
                );
                let mut consume_revive_obj = ConsumeReviveObj::new();
                this.consume_revive_message(&mut consume_revive_obj);
                if !this.should_run_pop_revive {
                    info!(
                        "slave skip scan, revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );
                    continue;
                }
                this.merge_and_revive(&mut consume_revive_obj);
                let mut delay = 0;
                if let Some(ref sort_list) = consume_revive_obj.sort_list {
                    if !sort_list.is_empty() {
                        delay =
                            (get_current_millis() - (sort_list[0].get_revive_time() as u64)) / 1000;
                        this.current_revive_message_timestamp = sort_list[0].get_revive_time();
                        slow = 1;
                    }
                } else {
                    this.current_revive_message_timestamp = get_current_millis() as i64;
                }
                info!(
                    "reviveQueueId={}, revive finish,old offset is {}, new offset is {}, \
                     ckDelay={}  ",
                    this.queue_id,
                    consume_revive_obj.old_offset,
                    consume_revive_obj.new_offset,
                    delay
                );

                if consume_revive_obj.sort_list.is_none()
                    || consume_revive_obj.sort_list.as_ref().unwrap().is_empty()
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        slow * this.broker_config.revive_interval,
                    ))
                    .await;
                    if slow < this.broker_config.revive_max_slow {
                        slow += 1;
                    }
                }
            }
        });
    }

    fn consume_revive_message(&self, _consume_revive_obj: &mut ConsumeReviveObj) {
        unimplemented!("consume_revive_message")
    }
    fn merge_and_revive(&self, _consume_revive_obj: &mut ConsumeReviveObj) {
        unimplemented!("consume_revive_message")
    }
}

fn reach_tail(pull_result: &PullResult, offset: i64) -> bool {
    *pull_result.pull_status() == PullStatus::NoNewMsg
        || *pull_result.pull_status() == PullStatus::OffsetIllegal
            && offset == pull_result.max_offset as i64
}

fn decode_msg_list(
    get_message_result: GetMessageResult,
    de_compress_body: bool,
) -> Vec<ArcMut<MessageExt>> {
    let mut found_list = Vec::new();
    for bb in get_message_result.message_mapped_list() {
        let data = &bb.mapped_file.as_ref().unwrap().get_mapped_file()
            [bb.start_offset as usize..(bb.start_offset + bb.size as u64) as usize];
        let mut bytes = Bytes::copy_from_slice(data);
        let msg_ext =
            message_decoder::decode(&mut bytes, true, de_compress_body, false, false, false);
        if let Some(msg_ext) = msg_ext {
            found_list.push(ArcMut::new(msg_ext));
        }
    }
    found_list
}

struct ConsumeReviveObj {
    map: HashMap<CheetahString, PopCheckPoint>,
    sort_list: Option<Vec<PopCheckPoint>>,
    old_offset: i64,
    end_time: i64,
    new_offset: i64,
}

impl ConsumeReviveObj {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            sort_list: None,
            old_offset: 0,
            end_time: 0,
            new_offset: 0,
        }
    }

    fn gen_sort_list(&mut self) -> &Vec<PopCheckPoint> {
        if self.sort_list.is_none() {
            let mut list: Vec<PopCheckPoint> = self.map.values().cloned().collect();
            list.sort_by_key(|ck| ck.revive_offset);
            self.sort_list = Some(list);
        }
        self.sort_list.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_store::pop::pop_check_point::PopCheckPoint;

    use super::*;

    #[test]
    fn new_initializes_correctly() {
        let obj = ConsumeReviveObj::new();
        assert!(obj.map.is_empty());
        assert!(obj.sort_list.is_none());
        assert_eq!(obj.old_offset, 0);
        assert_eq!(obj.end_time, 0);
        assert_eq!(obj.new_offset, 0);
    }

    #[test]
    fn gen_sort_list_creates_sorted_list() {
        let mut obj = ConsumeReviveObj::new();
        let ck1 = PopCheckPoint {
            revive_offset: 10,
            ..Default::default()
        };
        let ck2 = PopCheckPoint {
            revive_offset: 5,
            ..Default::default()
        };
        obj.map.insert(CheetahString::from("key1"), ck1.clone());
        obj.map.insert(CheetahString::from("key2"), ck2.clone());

        let sorted_list = obj.gen_sort_list();
        assert_eq!(sorted_list.len(), 2);
        assert_eq!(sorted_list[0].revive_offset, 5);
        assert_eq!(sorted_list[1].revive_offset, 10);
    }

    #[test]
    fn gen_sort_list_returns_existing_list_if_already_sorted() {
        let mut obj = ConsumeReviveObj::new();
        let ck1 = PopCheckPoint {
            revive_offset: 10,
            ..Default::default()
        };
        let ck2 = PopCheckPoint {
            revive_offset: 5,
            ..Default::default()
        };
        obj.map.insert(CheetahString::from("key1"), ck1.clone());
        obj.map.insert(CheetahString::from("key2"), ck2.clone());

        let _ = obj.gen_sort_list();
        let sorted_list = obj.gen_sort_list();
        assert_eq!(sorted_list.len(), 2);
        assert_eq!(sorted_list[0].revive_offset, 5);
        assert_eq!(sorted_list[1].revive_offset, 10);
    }
}
