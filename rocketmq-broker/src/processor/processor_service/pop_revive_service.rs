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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures::future::join_all;
use futures::FutureExt;
use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
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
use rocketmq_common::utils::data_converter::DataConverter;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::AppendMessageStatus;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::batch_ack_msg::BatchAckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use rocketmq_store::pop::AckMessage;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::processor::pop_message_processor::PopMessageProcessor;

pub struct PopReviveService<MS: MessageStore> {
    ck_rewrite_intervals_in_seconds: [i32; 17],
    queue_id: i32,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    revive_topic: CheetahString,
    current_revive_message_timestamp: i64,
    should_run_pop_revive: bool,
    inflight_revive_request_map: Arc<tokio::sync::Mutex<BTreeMap<PopCheckPoint, (i64, bool)>>>,
    revive_offset: i64,
    shutdown: Arc<AtomicBool>,
}
impl<MS: MessageStore> PopReviveService<MS> {
    pub fn new(
        revive_topic: CheetahString,
        queue_id: i32,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        let revive_offset = broker_runtime_inner.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
            &revive_topic,
            queue_id,
        );
        Self {
            queue_id,
            broker_runtime_inner,
            revive_topic,
            current_revive_message_timestamp: -1,
            should_run_pop_revive: false,
            inflight_revive_request_map: Arc::new(Default::default()),
            revive_offset,
            ck_rewrite_intervals_in_seconds: [
                10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ],
            shutdown: Arc::new(AtomicBool::new(false)),
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
                    self.broker_runtime_inner
                        .broker_config()
                        .enable_retry_topic_v2,
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
            .broker_runtime_inner
            .escape_bridge_mut()
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
        if let Some(_topic_config) = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(topic)
        {
            return;
        }
        let mut topic_config = ArcMut::new(TopicConfig::new(topic.clone()));
        topic_config.read_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.write_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.topic_filter_type = TopicFilterType::SingleTag;
        topic_config.perm = 6;
        topic_config.topic_sys_flag = 0;
        self.broker_runtime_inner
            .topic_config_manager_mut()
            .update_topic_config(topic_config.clone());
        self.init_pop_retry_offset(topic, consumer_group);
    }

    fn init_pop_retry_offset(&mut self, topic: &CheetahString, consumer_group: &CheetahString) {
        let offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(consumer_group, topic, 0);
        if offset < 0 {
            self.broker_runtime_inner
                .consumer_offset_manager()
                .commit_offset("initPopRetryOffset".into(), consumer_group, topic, 0, 0);
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
        } else if *pull_result.pull_status() == PullStatus::OffsetIllegal
            || *pull_result.pull_status() == PullStatus::NoMatchedMsg
        {
            if !self.should_run_pop_revive {
                return None;
            }
            self.broker_runtime_inner
                .consumer_offset_manager()
                .commit_offset(
                    CheetahString::from_static_str(PopAckConstants::LOCAL_HOST),
                    &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                    &self.revive_topic,
                    queue_id,
                    pull_result.next_begin_offset() as i64 - 1,
                );
        }
        pull_result.msg_found_list().clone()
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
            .broker_runtime_inner
            .message_store()
            .as_ref()
            .unwrap()
            .get_message(
                group, topic, queue_id, offset, nums, //    128 * 1024 * 1024,
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
            let max_queue_offset = self
                .broker_runtime_inner
                .message_store()
                .as_ref()
                .unwrap()
                .get_max_offset_in_queue(topic, queue_id);
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
                if this.shutdown.load(Relaxed) {
                    break;
                }

                if get_current_millis()
                    < this.broker_runtime_inner.should_start_time().load(Relaxed)
                {
                    info!(
                        "PopReviveService Ready to run after {}",
                        this.broker_runtime_inner.should_start_time().load(Relaxed)
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    this.broker_runtime_inner.broker_config().revive_interval,
                ))
                .await;
                if !this.should_run_pop_revive {
                    /*                    info!(
                        "skip start revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );*/
                    continue;
                }
                if !this
                    .broker_runtime_inner
                    .message_store()
                    .as_ref()
                    .unwrap()
                    .get_message_store_config()
                    .timer_wheel_enable
                {
                    info!("skip revive topic because timerWheelEnable is false");
                    continue;
                }
                if this.broker_runtime_inner.broker_config().enable_pop_log {
                    info!(
                        "start revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );
                }
                let mut consume_revive_obj = ConsumeReviveObj::new();
                this.consume_revive_message(&mut consume_revive_obj).await;
                if !this.should_run_pop_revive {
                    info!(
                        "slave skip scan, revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );
                    continue;
                }

                if let Err(e) = Self::merge_and_revive(this.clone(), &mut consume_revive_obj).await
                {
                    error!("reviveQueueId={}, revive error:{}", this.queue_id, e);
                    continue;
                }
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
                if this.broker_runtime_inner.broker_config().enable_pop_log {
                    info!(
                        "reviveQueueId={}, revive finish,old offset is {}, new offset is {}, \
                         ckDelay={}  ",
                        this.queue_id,
                        consume_revive_obj.old_offset,
                        consume_revive_obj.new_offset,
                        delay
                    );
                }

                if consume_revive_obj.sort_list.is_none()
                    || consume_revive_obj.sort_list.as_ref().unwrap().is_empty()
                {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        slow * this.broker_runtime_inner.broker_config().revive_interval,
                    ))
                    .await;
                    if slow < this.broker_runtime_inner.broker_config().revive_max_slow {
                        slow += 1;
                    }
                }
            }
        });
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    async fn consume_revive_message(&self, consume_revive_obj: &mut ConsumeReviveObj) {
        let map = &mut consume_revive_obj.map;
        let mut mock_point_map = HashMap::new();
        let _start_scan_time = get_current_millis();
        let mut end_time = 0;
        let consume_offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(
                &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                &self.revive_topic,
                self.queue_id,
            );
        let old_offset = self.revive_offset.max(consume_offset);
        consume_revive_obj.old_offset = old_offset;
        if self.broker_runtime_inner.broker_config().enable_pop_log {
            info!(
                "reviveQueueId={}, old offset is {}",
                self.queue_id, old_offset
            );
        }
        let mut offset = old_offset + 1;
        let mut no_msg_count = 0;
        let mut first_rt = 0;

        loop {
            if !self.should_run_pop_revive {
                info!(
                    "slave skip scan, revive topic={}, reviveQueueId={}",
                    self.revive_topic, self.queue_id
                );
                break;
            }

            let message_exts = self.get_revive_message(offset, self.queue_id).await;
            if message_exts.is_none() || message_exts.as_ref().unwrap().is_empty() {
                let old = end_time;
                let timer_delay = self
                    .broker_runtime_inner
                    .message_store()
                    .as_ref()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap() // may be need to unwrap
                    .get_dequeue_behind();
                let commit_log_delay = self
                    .broker_runtime_inner
                    .message_store()
                    .as_ref()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap() // may be need to unwrap
                    .get_enqueue_behind();
                if end_time != 0
                    && get_current_millis() - end_time > (3 * PopAckConstants::SECOND) as u64
                    && timer_delay <= 0
                    && commit_log_delay <= 0
                {
                    end_time = get_current_millis();
                }
                if self.broker_runtime_inner.broker_config().enable_pop_log {
                    info!(
                        "reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new \
                         endTime {}, timerDelay={}, commitLogDelay={}",
                        self.queue_id, offset, old, end_time, timer_delay, commit_log_delay
                    );
                }

                if end_time - first_rt
                    > (PopAckConstants::ACK_TIME_INTERVAL + PopAckConstants::SECOND) as u64
                {
                    break;
                }
                no_msg_count += 1;
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                if no_msg_count * 100 > 4 * PopAckConstants::SECOND {
                    break;
                } else {
                    continue;
                }
            } else {
                no_msg_count = 0;
            }

            if get_current_millis() > self.broker_runtime_inner.broker_config().revive_scan_time {
                info!("reviveQueueId={}, scan timeout", self.queue_id);
                break;
            }
            let size = message_exts.as_ref().unwrap().len();
            for message_ext in message_exts.unwrap() {
                if PopAckConstants::CK_TAG == message_ext.get_tags().unwrap_or_default() {
                    // let raw = String::from_utf8(message_ext.get_body().to_vec()).unwrap();
                    if self.broker_runtime_inner.broker_config().enable_pop_log {
                        info!(
                            "reviveQueueId={},find ck, offset:{}",
                            message_ext.queue_id, message_ext.queue_offset
                        );
                    }

                    let mut point: PopCheckPoint =
                        SerdeJsonUtils::from_json_bytes(message_ext.get_body().unwrap()).unwrap();
                    if point.topic.is_empty() || point.cid.is_empty() {
                        continue;
                    }
                    map.insert(
                        format!(
                            "{}{}{}{}{}{}",
                            point.topic,
                            point.cid,
                            point.queue_id,
                            point.start_offset,
                            point.pop_time,
                            point.broker_name.clone().unwrap_or_default()
                        )
                        .into(),
                        point.clone(),
                    );
                    //  PopMetricsManager::inc_pop_revive_ck_get_count(&point, self.queue_id);
                    point.revive_offset = message_ext.queue_offset;
                    if first_rt == 0 {
                        first_rt = point.get_revive_time() as u64;
                    }
                } else if PopAckConstants::ACK_TAG == message_ext.get_tags().unwrap_or_default() {
                    let ack_msg: AckMsg =
                        SerdeJsonUtils::from_json_bytes(message_ext.get_body().unwrap()).unwrap();
                    // PopMetricsManager::inc_pop_revive_ack_get_count(&ack_msg, self.queue_id);
                    let merge_key = CheetahString::from_string(format!(
                        "{}{}{}{}{}{}",
                        ack_msg.topic,
                        ack_msg.consumer_group,
                        ack_msg.queue_id,
                        ack_msg.start_offset,
                        ack_msg.pop_time,
                        ack_msg.broker_name
                    ));
                    if let Some(point) = map.get_mut(&merge_key) {
                        let index_of_ack = point.index_of_ack(ack_msg.ack_offset());
                        if index_of_ack > -1 {
                            point.bit_map =
                                DataConverter::set_bit(point.bit_map, index_of_ack as usize, true);
                        } else {
                            error!("invalid ack index, {}, {}", ack_msg, point);
                        }
                    } else {
                        if !self
                            .broker_runtime_inner
                            .broker_config()
                            .enable_skip_long_awaiting_ack
                        {
                            continue;
                        }
                        if self.mock_ck_for_ack(
                            &message_ext,
                            &ack_msg,
                            &merge_key,
                            &mut mock_point_map,
                        ) && first_rt == 0
                        {
                            first_rt =
                                mock_point_map.get(&merge_key).unwrap().get_revive_time() as u64;
                        }
                    }
                } else if PopAckConstants::BATCH_ACK_TAG
                    == message_ext.get_tags().unwrap_or_default()
                {
                    //let raw = String::from_utf8(message_ext.get_body().to_vec()).unwrap();
                    if self.broker_runtime_inner.broker_config().enable_pop_log {
                        info!(
                            "reviveQueueId={}, find batch ack, offset:{},",
                            message_ext.queue_id, message_ext.queue_offset,
                        );
                    }
                    let b_ack_msg: BatchAckMsg =
                        SerdeJsonUtils::from_json_bytes(message_ext.get_body().unwrap()).unwrap();
                    // PopMetricsManager::inc_pop_revive_ack_get_count(&b_ack_msg, self.queue_id);
                    let merge_key = CheetahString::from_string(format!(
                        "{}{}{}{}{}{}",
                        b_ack_msg.ack_msg.topic,
                        b_ack_msg.ack_msg.consumer_group,
                        b_ack_msg.ack_msg.queue_id,
                        b_ack_msg.ack_msg.start_offset,
                        b_ack_msg.ack_msg.pop_time,
                        b_ack_msg.ack_msg.broker_name
                    ));
                    if let Some(point) = map.get_mut(&merge_key) {
                        for ack_offset in &b_ack_msg.ack_offset_list {
                            let index_of_ack = point.index_of_ack(*ack_offset);
                            if index_of_ack > -1 {
                                point.bit_map = DataConverter::set_bit(
                                    point.bit_map,
                                    index_of_ack as usize,
                                    true,
                                );
                            } else {
                                error!("invalid batch ack index, {}, {}", b_ack_msg, point);
                            }
                        }
                    } else {
                        if !self
                            .broker_runtime_inner
                            .broker_config()
                            .enable_skip_long_awaiting_ack
                        {
                            continue;
                        }
                        if self.mock_ck_for_ack(
                            &message_ext,
                            &b_ack_msg,
                            &merge_key,
                            &mut mock_point_map,
                        ) && first_rt == 0
                        {
                            first_rt =
                                mock_point_map.get(&merge_key).unwrap().get_revive_time() as u64;
                        }
                    }
                }
                let deliver_time = message_ext.get_deliver_time_ms();
                if deliver_time > end_time {
                    end_time = deliver_time;
                }
            }
            offset += size as i64;
        }
        consume_revive_obj.map.extend(mock_point_map);
        consume_revive_obj.end_time = end_time as i64;
    }

    fn mock_ck_for_ack(
        &self,
        message_ext: &MessageExt,
        ack_msg: &(impl AckMessage + std::fmt::Debug),
        merge_key: &CheetahString,
        mock_point_map: &mut HashMap<CheetahString, PopCheckPoint>,
    ) -> bool {
        let now = get_current_millis();
        let ack_wait_time = now - message_ext.get_deliver_time_ms();
        let revive_ack_wait_ms = self.broker_runtime_inner.broker_config().revive_ack_wait_ms;
        if ack_wait_time > revive_ack_wait_ms {
            // will use the revive_offset of pop_check_point to commit offset in merge_and_revive
            let mock_point = self.create_mock_ck_for_ack(ack_msg, message_ext.queue_offset());
            warn!(
                "ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{:?}, \
                 mockCk:{:?}",
                revive_ack_wait_ms, merge_key, ack_msg, mock_point
            );
            mock_point_map.insert(merge_key.clone(), mock_point);
            return true;
        }
        false
    }

    fn create_mock_ck_for_ack(
        &self,
        ack_msg: &impl AckMessage,
        revive_offset: i64,
    ) -> PopCheckPoint {
        let mut point = PopCheckPoint::default();
        point.set_start_offset(ack_msg.start_offset());
        point.set_pop_time(ack_msg.pop_time());
        point.set_queue_id(ack_msg.queue_id());
        point.set_cid(ack_msg.consumer_group().clone());
        point.set_topic(ack_msg.topic().clone());
        point.set_num(0);
        point.set_bit_map(0);
        point.set_revive_offset(revive_offset);
        point.set_broker_name(Some(ack_msg.broker_name().clone()));
        point
    }

    async fn merge_and_revive(
        mut this: ArcMut<Self>,
        consume_revive_obj: &mut ConsumeReviveObj,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut new_offset = consume_revive_obj.old_offset;
        let end_time = consume_revive_obj.end_time;
        let sort_list = consume_revive_obj.gen_sort_list();
        if this.broker_runtime_inner.broker_config().enable_pop_log {
            info!(
                "reviveQueueId={}, ck listSize={}",
                &this.queue_id,
                sort_list.len()
            );
        }
        if !sort_list.is_empty() {
            let last = sort_list.last().unwrap();
            info!(
                "reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={}; last ck, \
                 startOffset={}, reviveOffset={}",
                this.queue_id,
                sort_list[0].start_offset,
                sort_list[0].revive_offset,
                last.start_offset,
                last.revive_offset
            );
        }

        for pop_check_point in sort_list {
            if !this.should_run_pop_revive {
                info!(
                    "slave skip ck process, revive topic={}, reviveQueueId={}",
                    this.revive_topic, this.queue_id
                );
                break;
            }
            if end_time - pop_check_point.get_revive_time()
                <= (PopAckConstants::ACK_TIME_INTERVAL + PopAckConstants::SECOND)
            {
                break;
            }

            let normal_topic = CheetahString::from_string(KeyBuilder::parse_normal_topic(
                pop_check_point.topic.as_str(),
                pop_check_point.cid.as_str(),
            ));
            if this
                .broker_runtime_inner
                .topic_config_manager()
                .select_topic_config(&normal_topic)
                .is_none()
            {
                info!(
                    "reviveQueueId={}, can not get normal topic {}, then continue",
                    this.queue_id, pop_check_point.topic
                );
                new_offset = pop_check_point.revive_offset;
                continue;
            }
            if this
                .broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(&pop_check_point.cid)
                .is_none()
            {
                info!(
                    "reviveQueueId={}, can not get cid {}, then continue",
                    this.queue_id, pop_check_point.cid
                );
                new_offset = pop_check_point.revive_offset;
                continue;
            }

            // may be need to optimize
            let mut remove = vec![];
            let length = this.inflight_revive_request_map.lock().await.len();
            while length - remove.len() > 3 {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let mut inflight_map = this.inflight_revive_request_map.lock().await;
                let entry = inflight_map.first_entry().unwrap();
                let pair = entry.get();
                let old_ck = entry.key();
                if !pair.1 && (get_current_millis() - pair.0 as u64 > 30 * 1000) {
                    this.re_put_ck(old_ck, pair).await;
                    remove.push(old_ck.clone());
                    info!(
                        "stay too long, remove from reviveRequestMap, {}, {:?}, {}, {}",
                        pop_check_point.topic,
                        pop_check_point.broker_name,
                        pop_check_point.queue_id,
                        pop_check_point.start_offset
                    );
                }
            }
            let mut inflight_revive_request_map = this.inflight_revive_request_map.lock().await;
            for ck in remove {
                inflight_revive_request_map.remove(&ck);
            }
            // may be need to optimize
            Self::revive_msg_from_ck(this.clone(), pop_check_point).await;

            new_offset = pop_check_point.revive_offset;
        }
        if new_offset > consume_revive_obj.old_offset {
            if !this.should_run_pop_revive {
                println!(
                    "slave skip commit, revive topic={}, reviveQueueId={}",
                    this.revive_topic, this.queue_id
                );
                return Ok(());
            }
            this.broker_runtime_inner
                .consumer_offset_manager()
                .commit_offset(
                    CheetahString::from_static_str(PopAckConstants::LOCAL_HOST),
                    &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                    &this.revive_topic,
                    this.queue_id,
                    new_offset,
                );
        }
        this.revive_offset = new_offset;
        consume_revive_obj.new_offset = new_offset;
        Ok(())
    }

    async fn re_put_ck(&self, old_ck: &PopCheckPoint, pair: &(i64, bool)) {
        let re_put_times = old_ck.parse_re_put_times();
        if re_put_times >= self.ck_rewrite_intervals_in_seconds.len() as i32
            && self
                .broker_runtime_inner
                .broker_config()
                .skip_when_ck_re_put_reach_max_times
        {
            info!(
                "rePut CK reach max times, drop it. {}, {}, {:?}, {}-{}, {}, {}, {}",
                old_ck.get_topic(),
                old_ck.get_cid(),
                old_ck.get_broker_name(),
                old_ck.get_queue_id(),
                pair.0,
                old_ck.get_pop_time(),
                old_ck.get_invisible_time(),
                re_put_times
            );
            return;
        }

        let mut new_ck = PopCheckPoint::default();
        new_ck.set_bit_map(0);
        new_ck.set_num(1);
        new_ck.set_pop_time(old_ck.get_pop_time());
        new_ck.set_invisible_time(old_ck.get_invisible_time());
        new_ck.set_start_offset(pair.0);
        new_ck.set_cid(old_ck.get_cid().clone());
        new_ck.set_topic(old_ck.get_topic().clone());
        new_ck.set_queue_id(old_ck.get_queue_id());
        new_ck.set_broker_name(old_ck.get_broker_name().cloned());
        new_ck.add_diff(0);
        new_ck.set_re_put_times(Some(CheetahString::from_string(
            (re_put_times + 1).to_string(),
        )));

        if old_ck.get_revive_time() <= get_current_millis() as i64 {
            let interval_index =
                if re_put_times >= self.ck_rewrite_intervals_in_seconds.len() as i32 {
                    self.ck_rewrite_intervals_in_seconds.len() - 1
                } else {
                    re_put_times as usize
                };
            new_ck.set_invisible_time(
                old_ck.get_invisible_time()
                    + (self.ck_rewrite_intervals_in_seconds[interval_index] * 1000) as i64,
            );
        }

        let ck_msg = PopMessageProcessor::<MS>::build_ck_msg(
            self.broker_runtime_inner.store_host(),
            &new_ck,
            self.queue_id,
            self.revive_topic.clone(),
        );
        self.broker_runtime_inner
            .message_store()
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .put_message(ck_msg)
            .await;
    }

    async fn revive_msg_from_ck(this: ArcMut<Self>, pop_check_point: &PopCheckPoint) {
        if !this.should_run_pop_revive {
            info!(
                "slave skip retry, revive topic={}, reviveQueueId={}",
                this.revive_topic, this.queue_id
            );
            return;
        }
        let now = get_current_millis() as i64;
        this.inflight_revive_request_map
            .lock()
            .await
            .insert(pop_check_point.clone(), (now, false));
        let mut future_list = Vec::with_capacity(pop_check_point.get_num() as usize);

        for j in 0..pop_check_point.get_num() {
            if DataConverter::get_bit(pop_check_point.get_bit_map(), j as usize) {
                continue;
            }
            let msg_offset = pop_check_point.ack_offset_by_index(j);

            let mut this_inner = this.clone();

            let future = async move {
                let rst = this_inner
                    .get_biz_message(pop_check_point, msg_offset)
                    .await;
                let message = rst.0;
                if message.is_none() {
                    info!(
                        "reviveQueueId={}, can not get biz msg, topic:{}, qid:{}, offset:{}, \
                         brokerName:{:?}, info:{}, retry:{}, then continue",
                        this_inner.queue_id,
                        pop_check_point.get_topic(),
                        pop_check_point.get_queue_id(),
                        msg_offset,
                        pop_check_point.get_broker_name(),
                        &rst.1,
                        rst.2
                    );
                    return (msg_offset, !rst.2);
                }
                let result = this_inner
                    .revive_retry(pop_check_point, &message.unwrap())
                    .await;
                (msg_offset, result)
            }
            .boxed();
            future_list.push(future);
        }

        let results = join_all(future_list).await;

        for pair in &results {
            if !pair.1 {
                this.re_put_ck(pop_check_point, pair).await;
            }
        }

        {
            let mut inflight = this.inflight_revive_request_map.lock().await;
            if let Some(entry) = inflight.get_mut(pop_check_point) {
                entry.1 = true;
            }
            let keys: Vec<_> = inflight.keys().cloned().collect();
            for old_ck in keys {
                if let Some(pair) = inflight.get(&old_ck) {
                    if pair.1 {
                        this.broker_runtime_inner
                            .consumer_offset_manager()
                            .commit_offset(
                                CheetahString::from_static_str(PopAckConstants::LOCAL_HOST),
                                &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                                &this.revive_topic,
                                this.queue_id,
                                old_ck.get_revive_offset(),
                            );
                        inflight.remove(&old_ck);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    pub fn set_should_run_pop_revive(&mut self, should_run_pop_revive: bool) {
        self.should_run_pop_revive = should_run_pop_revive;
    }

    pub async fn get_biz_message(
        &self,
        pop_check_point: &PopCheckPoint,
        offset: i64,
    ) -> (Option<MessageExt>, String, bool) {
        let topic = pop_check_point.get_topic();
        let queue_id = pop_check_point.get_queue_id();
        let broker_name = pop_check_point.get_broker_name().unwrap();
        self.broker_runtime_inner
            .escape_bridge()
            .get_message_async(topic, offset, queue_id, broker_name, false)
            .await
    }

    pub fn get_revive_behind_millis(&self) -> i64 {
        if self.current_revive_message_timestamp <= 0 {
            return 0;
        }
        let max_offset = self
            .broker_runtime_inner
            .message_store_unchecked()
            .get_max_offset_in_queue(&self.revive_topic, self.queue_id);
        if max_offset - self.revive_offset > 1 {
            let now = get_current_millis() as i64;
            return std::cmp::max(0, now - self.current_revive_message_timestamp);
        }
        0
    }

    pub fn get_revive_behind_messages(&self) -> i64 {
        if self.current_revive_message_timestamp <= 0 {
            return 0;
        }
        let max_offset = self
            .broker_runtime_inner
            .message_store_unchecked()
            .get_max_offset_in_queue(&self.revive_topic, self.queue_id);
        let diff = max_offset - self.revive_offset;
        std::cmp::max(0, diff)
    }
}

fn reach_tail(pull_result: &PullResult, offset: i64) -> bool {
    *pull_result.pull_status() == PullStatus::NoNewMsg
        || *pull_result.pull_status() == PullStatus::OffsetIllegal
            && offset == pull_result.max_offset() as i64
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
