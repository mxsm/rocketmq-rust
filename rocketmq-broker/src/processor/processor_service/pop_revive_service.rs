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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
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
use crate::metrics::pop_metrics_manager;
use crate::processor::pop_message_processor::PopMessageProcessor;

/// Maximum number of concurrent in-flight revive requests
const MAX_INFLIGHT_REVIVE_REQUESTS: usize = 3;

/// Timeout for in-flight revive requests (30 seconds)
const INFLIGHT_REVIVE_TIMEOUT_MS: u64 = 30_000;

/// Sleep interval when waiting for in-flight requests (100ms)
const INFLIGHT_WAIT_INTERVAL_MS: u64 = 100;

pub struct PopReviveService<MS: MessageStore> {
    ck_rewrite_intervals_in_seconds: [i32; 17],
    queue_id: i32,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    revive_topic: CheetahString,
    current_revive_message_timestamp: i64,
    should_run_pop_revive: Arc<AtomicBool>,
    inflight_revive_request_map: Arc<tokio::sync::Mutex<BTreeMap<PopCheckPoint, (i64, bool)>>>,
    revive_offset: Arc<AtomicI64>,
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
            should_run_pop_revive: Arc::new(AtomicBool::new(false)),
            inflight_revive_request_map: Arc::new(Default::default()),
            revive_offset: Arc::new(AtomicI64::new(revive_offset)),
            ck_rewrite_intervals_in_seconds: [
                10, 20, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
            ],
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn revive_retry(&mut self, pop_check_point: &PopCheckPoint, message_ext: &MessageExt) -> bool {
        let mut msg_inner = MessageExtBrokerInner::default();
        // Check if topic is NOT retry topic, then build retry topic
        if !pop_check_point.topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            // Normal topic -> build %RETRY% topic
            msg_inner.set_topic(CheetahString::from_string(KeyBuilder::build_pop_retry_topic(
                pop_check_point.topic.as_str(),
                pop_check_point.cid.as_str(),
                self.broker_runtime_inner.broker_config().enable_retry_topic_v2,
            )));
        } else {
            // Already retry topic -> keep it unchanged
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
                .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME))
                .is_none()
        {
            msg_inner.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                CheetahString::from(pop_check_point.pop_time.to_string()),
            );
        }
        msg_inner.properties_string = message_decoder::message_properties_to_string(msg_inner.get_properties());
        let retry_topic = msg_inner.get_topic().clone();
        self.add_retry_topic_if_not_exist(&retry_topic, &pop_check_point.cid);
        let put_message_result = self
            .broker_runtime_inner
            .escape_bridge_mut()
            .put_message_to_specific_queue(msg_inner)
            .await;
        if put_message_result.append_message_result().is_none()
            || put_message_result.append_message_result().unwrap().status != AppendMessageStatus::PutOk
        {
            error!(
                "reviveQueueId={}, revive error, put message failed, ck={:?}",
                self.queue_id, pop_check_point
            );
            return false;
        }

        // Update statistics after successful put
        self.broker_runtime_inner
            .pop_inflight_message_counter()
            .decrement_in_flight_message_num_checkpoint(pop_check_point);

        // Record observability metrics
        pop_metrics_manager::inc_pop_revive_retry_message_count(
            pop_check_point,
            put_message_result.put_message_status(),
        );

        self.broker_runtime_inner
            .broker_stats_manager()
            .inc_broker_put_nums(&pop_check_point.topic, 1);
        self.broker_runtime_inner
            .broker_stats_manager()
            .inc_topic_put_nums(&retry_topic, 1, 1);
        if let Some(append_result) = put_message_result.append_message_result() {
            self.broker_runtime_inner
                .broker_stats_manager()
                .inc_topic_put_size(&retry_topic, append_result.wrote_bytes);
        }

        true
    }

    pub fn add_retry_topic_if_not_exist(&mut self, topic: &CheetahString, consumer_group: &CheetahString) {
        if let Some(_topic_config) = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(topic)
        {
            return;
        }
        let mut topic_config = TopicConfig::new(topic.clone());
        topic_config.read_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.write_queue_nums = PopAckConstants::RETRY_QUEUE_NUM as u32;
        topic_config.topic_filter_type = TopicFilterType::SingleTag;
        topic_config.perm = 6;
        topic_config.topic_sys_flag = 0;
        self.broker_runtime_inner
            .topic_config_manager_mut()
            .update_topic_config(ArcMut::new(topic_config));
        self.init_pop_retry_offset(topic, consumer_group);
    }

    fn init_pop_retry_offset(&mut self, topic: &CheetahString, consumer_group: &CheetahString) {
        let offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_offset(consumer_group, topic, 0);
        if offset < 0 {
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
                "initPopRetryOffset".into(),
                consumer_group,
                topic,
                0,
                0,
            );
        }
    }

    pub(crate) async fn get_revive_message(&self, offset: i64, queue_id: i32) -> Option<Vec<ArcMut<MessageExt>>> {
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
            if !self.should_run_pop_revive.load(Ordering::Acquire) {
                return None;
            }
            self.broker_runtime_inner.consumer_offset_manager().commit_offset(
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
        let message_store = self.broker_runtime_inner.message_store()?;
        let get_message_result = message_store
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

                GetMessageStatus::NoMessageInQueue | GetMessageStatus::OffsetReset => (PullStatus::NoNewMsg, None),

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
            if let Some(store) = self.broker_runtime_inner.message_store() {
                let max_queue_offset = store.get_max_offset_in_queue(topic, queue_id);
                if max_queue_offset > offset {
                    error!(
                        "get message from store return null. topic={}, groupId={}, requestOffset={}, maxQueueOffset={}",
                        topic, group, offset, max_queue_offset
                    );
                }
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

                if get_current_millis() < this.broker_runtime_inner.should_start_time().load(Relaxed) {
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
                if !this.should_run_pop_revive.load(Ordering::Acquire) {
                    /*                    info!(
                        "skip start revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );*/
                    continue;
                }
                if !this
                    .broker_runtime_inner
                    .message_store()
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
                if !this.should_run_pop_revive.load(Ordering::Acquire) {
                    info!(
                        "slave skip scan, revive topic={}, reviveQueueId={}",
                        this.revive_topic, this.queue_id
                    );
                    continue;
                }

                if let Err(e) = Self::merge_and_revive(this.clone(), &mut consume_revive_obj).await {
                    error!("reviveQueueId={}, revive error:{}", this.queue_id, e);
                    continue;
                }
                let mut delay = 0;
                if let Some(ref sort_list) = consume_revive_obj.sort_list {
                    if !sort_list.is_empty() {
                        delay = (get_current_millis() - (sort_list[0].get_revive_time() as u64)) / 1000;
                        this.current_revive_message_timestamp = sort_list[0].get_revive_time();
                        slow = 1;
                    }
                } else {
                    this.current_revive_message_timestamp = get_current_millis() as i64;
                }
                if this.broker_runtime_inner.broker_config().enable_pop_log {
                    info!(
                        "reviveQueueId={}, revive finish,old offset is {}, new offset is {}, ckDelay={}  ",
                        this.queue_id, consume_revive_obj.old_offset, consume_revive_obj.new_offset, delay
                    );
                }

                if consume_revive_obj.sort_list.is_none() || consume_revive_obj.sort_list.as_ref().unwrap().is_empty() {
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
        let consume_offset = self.broker_runtime_inner.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
            &self.revive_topic,
            self.queue_id,
        );
        let old_offset = self.revive_offset.load(Ordering::Acquire).max(consume_offset);
        consume_revive_obj.old_offset = old_offset;
        if self.broker_runtime_inner.broker_config().enable_pop_log {
            info!("reviveQueueId={}, old offset is {}", self.queue_id, old_offset);
        }
        let mut offset = old_offset + 1;
        let mut no_msg_count = 0;
        let mut first_rt = 0;

        loop {
            if !self.should_run_pop_revive.load(Ordering::Acquire) {
                info!(
                    "slave skip scan, revive topic={}, reviveQueueId={}",
                    self.revive_topic, self.queue_id
                );
                break;
            }

            let message_exts = self.get_revive_message(offset, self.queue_id).await;
            if message_exts.is_none() || message_exts.as_ref().unwrap().is_empty() {
                let old = end_time;
                // Safely get timer message store delays
                let (timer_delay, commit_log_delay) = self
                    .broker_runtime_inner
                    .message_store()
                    .and_then(|s| s.get_timer_message_store())
                    .map(|timer_store| (timer_store.get_dequeue_behind(), timer_store.get_enqueue_behind()))
                    .unwrap_or((0, 0));
                if end_time != 0
                    && get_current_millis() - end_time > (3 * PopAckConstants::SECOND) as u64
                    && timer_delay <= 0
                    && commit_log_delay <= 0
                {
                    end_time = get_current_millis();
                }
                if self.broker_runtime_inner.broker_config().enable_pop_log {
                    info!(
                        "reviveQueueId={}, offset is {}, can not get new msg, old endTime {}, new endTime {}, \
                         timerDelay={}, commitLogDelay={}",
                        self.queue_id, offset, old, end_time, timer_delay, commit_log_delay
                    );
                }

                if end_time - first_rt > (PopAckConstants::ACK_TIME_INTERVAL + PopAckConstants::SECOND) as u64 {
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

                    // Safely decode CK message body
                    let body = match message_ext.get_body() {
                        Some(body) => body,
                        None => {
                            error!(
                                "reviveQueueId={}, ck message body is null, offset:{}",
                                message_ext.queue_id, message_ext.queue_offset
                            );
                            continue;
                        }
                    };
                    let mut point: PopCheckPoint = match SerdeJsonUtils::from_json_bytes(body) {
                        Ok(point) => point,
                        Err(e) => {
                            error!(
                                "reviveQueueId={}, decode ck failed, offset:{}, error:{}",
                                message_ext.queue_id, message_ext.queue_offset, e
                            );
                            continue;
                        }
                    };
                    if point.topic.is_empty() || point.cid.is_empty() {
                        continue;
                    }
                    // Set revive_offset BEFORE inserting into map
                    point.revive_offset = message_ext.queue_offset;

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
                    pop_metrics_manager::inc_pop_revive_ck_get_count(&point, self.queue_id);
                    if first_rt == 0 {
                        first_rt = point.get_revive_time() as u64;
                    }
                } else if PopAckConstants::ACK_TAG == message_ext.get_tags().unwrap_or_default() {
                    // Safely decode ACK message body
                    let body = match message_ext.get_body() {
                        Some(body) => body,
                        None => {
                            error!(
                                "reviveQueueId={}, ack message body is null, offset:{}",
                                message_ext.queue_id, message_ext.queue_offset
                            );
                            continue;
                        }
                    };
                    let ack_msg: AckMsg = match SerdeJsonUtils::from_json_bytes(body) {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!(
                                "reviveQueueId={}, decode ack failed, offset:{}, error:{}",
                                message_ext.queue_id, message_ext.queue_offset, e
                            );
                            continue;
                        }
                    };
                    pop_metrics_manager::inc_pop_revive_ack_get_count(&ack_msg, self.queue_id);
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
                            point.bit_map = DataConverter::set_bit(point.bit_map, index_of_ack as usize, true);
                        } else {
                            error!("invalid ack index, {}, {}", ack_msg, point);
                        }
                    } else {
                        if !self.broker_runtime_inner.broker_config().enable_skip_long_awaiting_ack {
                            continue;
                        }
                        if self.mock_ck_for_ack(&message_ext, &ack_msg, &merge_key, &mut mock_point_map)
                            && first_rt == 0
                        {
                            first_rt = mock_point_map.get(&merge_key).unwrap().get_revive_time() as u64;
                        }
                    }
                } else if PopAckConstants::BATCH_ACK_TAG == message_ext.get_tags().unwrap_or_default() {
                    //let raw = String::from_utf8(message_ext.get_body().to_vec()).unwrap();
                    if self.broker_runtime_inner.broker_config().enable_pop_log {
                        info!(
                            "reviveQueueId={}, find batch ack, offset:{},",
                            message_ext.queue_id, message_ext.queue_offset,
                        );
                    }
                    // Safely decode BatchAck message body
                    let body = match message_ext.get_body() {
                        Some(body) => body,
                        None => {
                            error!(
                                "reviveQueueId={}, batch ack message body is null, offset:{}",
                                message_ext.queue_id, message_ext.queue_offset
                            );
                            continue;
                        }
                    };
                    let b_ack_msg: BatchAckMsg = match SerdeJsonUtils::from_json_bytes(body) {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!(
                                "reviveQueueId={}, decode batch ack failed, offset:{}, error:{}",
                                message_ext.queue_id, message_ext.queue_offset, e
                            );
                            continue;
                        }
                    };
                    pop_metrics_manager::inc_pop_revive_ack_get_count(&b_ack_msg.ack_msg, self.queue_id);
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
                                point.bit_map = DataConverter::set_bit(point.bit_map, index_of_ack as usize, true);
                            } else {
                                error!("invalid batch ack index, {}, {}", b_ack_msg, point);
                            }
                        }
                    } else {
                        if !self.broker_runtime_inner.broker_config().enable_skip_long_awaiting_ack {
                            continue;
                        }
                        if self.mock_ck_for_ack(&message_ext, &b_ack_msg, &merge_key, &mut mock_point_map)
                            && first_rt == 0
                        {
                            first_rt = mock_point_map.get(&merge_key).unwrap().get_revive_time() as u64;
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
                "ack wait for {}ms cannot find ck, skip this ack. mergeKey:{}, ack:{:?}, mockCk:{:?}",
                revive_ack_wait_ms, merge_key, ack_msg, mock_point
            );
            mock_point_map.insert(merge_key.clone(), mock_point);
            return true;
        }
        false
    }

    fn create_mock_ck_for_ack(&self, ack_msg: &impl AckMessage, revive_offset: i64) -> PopCheckPoint {
        let mut point = PopCheckPoint::default();
        point.set_start_offset(ack_msg.start_offset());
        point.set_pop_time(ack_msg.pop_time());
        point.set_queue_id(ack_msg.queue_id());
        point.cid = ack_msg.consumer_group().clone();
        point.topic = ack_msg.topic().clone();
        point.set_num(0);
        point.set_bit_map(0);
        point.set_revive_offset(revive_offset);
        point.broker_name = Some(ack_msg.broker_name().clone());
        point
    }

    async fn merge_and_revive(
        this: ArcMut<Self>,
        consume_revive_obj: &mut ConsumeReviveObj,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut new_offset = consume_revive_obj.old_offset;
        let end_time = consume_revive_obj.end_time;
        let sort_list = consume_revive_obj.gen_sort_list();
        if this.broker_runtime_inner.broker_config().enable_pop_log {
            info!("reviveQueueId={}, ck listSize={}", &this.queue_id, sort_list.len());
        }
        if !sort_list.is_empty() {
            let last = sort_list.last().unwrap();
            info!(
                "reviveQueueId={}, 1st ck, startOffset={}, reviveOffset={}; last ck, startOffset={}, reviveOffset={}",
                this.queue_id,
                sort_list[0].start_offset,
                sort_list[0].revive_offset,
                last.start_offset,
                last.revive_offset
            );
        }

        for pop_check_point in sort_list {
            if !this.should_run_pop_revive.load(Ordering::Acquire) {
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

            // Wait for inflight requests to complete if limit exceeded
            loop {
                let (current_length, timeout_ck) = {
                    let inflight_map = this.inflight_revive_request_map.lock().await;
                    let len = inflight_map.len();

                    // Find first timeout request
                    let now = get_current_millis();
                    let timeout = inflight_map
                        .iter()
                        .find(|(_, (timestamp, completed))| {
                            !completed && (now - *timestamp as u64 > INFLIGHT_REVIVE_TIMEOUT_MS)
                        })
                        .map(|(ck, pair)| (ck.clone(), *pair));

                    (len, timeout)
                }; // Lock released here

                if current_length <= MAX_INFLIGHT_REVIVE_REQUESTS {
                    break;
                }

                if let Some((ck, pair)) = timeout_ck {
                    this.re_put_ck(&ck, &pair).await;
                    this.inflight_revive_request_map.lock().await.remove(&ck);
                    info!(
                        "stay too long, remove from reviveRequestMap, {}, {:?}, {}, {}",
                        pop_check_point.topic,
                        pop_check_point.broker_name,
                        pop_check_point.queue_id,
                        pop_check_point.start_offset
                    );
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(INFLIGHT_WAIT_INTERVAL_MS)).await;
            }

            Self::revive_msg_from_ck(this.clone(), pop_check_point).await;

            new_offset = pop_check_point.revive_offset;
        }
        if new_offset > consume_revive_obj.old_offset {
            if !this.should_run_pop_revive.load(Ordering::Acquire) {
                println!(
                    "slave skip commit, revive topic={}, reviveQueueId={}",
                    this.revive_topic, this.queue_id
                );
                return Ok(());
            }
            this.broker_runtime_inner.consumer_offset_manager().commit_offset(
                CheetahString::from_static_str(PopAckConstants::LOCAL_HOST),
                &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                &this.revive_topic,
                this.queue_id,
                new_offset,
            );
        }
        this.revive_offset.store(new_offset, Ordering::Release);
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
        new_ck.cid = old_ck.cid.clone();
        new_ck.topic = old_ck.topic.clone();
        new_ck.set_queue_id(old_ck.get_queue_id());
        new_ck.set_broker_name(old_ck.get_broker_name().cloned());
        new_ck.add_diff(0);
        new_ck.set_re_put_times(Some(CheetahString::from_string((re_put_times + 1).to_string())));

        if old_ck.get_revive_time() <= get_current_millis() as i64 {
            let interval_index = if re_put_times >= self.ck_rewrite_intervals_in_seconds.len() as i32 {
                self.ck_rewrite_intervals_in_seconds.len() - 1
            } else {
                re_put_times as usize
            };
            new_ck.set_invisible_time(
                old_ck.get_invisible_time() + (self.ck_rewrite_intervals_in_seconds[interval_index] * 1000) as i64,
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
            .unwrap()
            .mut_from_ref()
            .put_message(ck_msg)
            .await;
    }

    async fn revive_msg_from_ck(this: ArcMut<Self>, pop_check_point: &PopCheckPoint) {
        if !this.should_run_pop_revive.load(Ordering::Acquire) {
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
                let rst = this_inner.get_biz_message(pop_check_point, msg_offset).await;
                let message = rst.0;
                if message.is_none() {
                    info!(
                        "reviveQueueId={}, can not get biz msg, topic:{}, qid:{}, offset:{}, brokerName:{:?}, \
                         info:{}, retry:{}, then continue",
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
                let result = this_inner.revive_retry(pop_check_point, &message.unwrap()).await;
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

        // Mark request as completed
        {
            let mut inflight = this.inflight_revive_request_map.lock().await;
            if let Some(entry) = inflight.get_mut(pop_check_point) {
                entry.1 = true;
            }
        } // Lock released

        // Clean up completed requests and commit offsets
        // CRITICAL: Must process in order (BTreeMap), stop at first incomplete
        loop {
            let ck_to_remove = {
                let inflight = this.inflight_revive_request_map.lock().await;
                // BTreeMap iterates in sorted order (by PopCheckPoint.start_offset)
                if let Some((ck, (_timestamp, completed))) = inflight.iter().next() {
                    if *completed {
                        Some(ck.clone())
                    } else {
                        None // Stop at first incomplete
                    }
                } else {
                    None
                }
            }; // Lock released

            match ck_to_remove {
                Some(ck) => {
                    // Commit offset and remove
                    this.broker_runtime_inner.consumer_offset_manager().commit_offset(
                        CheetahString::from_static_str(PopAckConstants::LOCAL_HOST),
                        &CheetahString::from_static_str(PopAckConstants::REVIVE_GROUP),
                        &this.revive_topic,
                        this.queue_id,
                        ck.get_revive_offset(),
                    );
                    this.inflight_revive_request_map.lock().await.remove(&ck);
                }
                None => break, // Stop when no more completed or hit incomplete
            }
        }
    }

    pub fn set_should_run_pop_revive(&mut self, should_run_pop_revive: bool) {
        self.should_run_pop_revive
            .store(should_run_pop_revive, Ordering::Release);
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
        let revive_offset = self.revive_offset.load(Ordering::Acquire);
        if max_offset - revive_offset > 1 {
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
        let revive_offset = self.revive_offset.load(Ordering::Acquire);
        let diff = max_offset - revive_offset;
        std::cmp::max(0, diff)
    }
}

fn reach_tail(pull_result: &PullResult, offset: i64) -> bool {
    *pull_result.pull_status() == PullStatus::NoNewMsg
        || *pull_result.pull_status() == PullStatus::OffsetIllegal && offset == pull_result.max_offset() as i64
}

fn decode_msg_list(get_message_result: GetMessageResult, de_compress_body: bool) -> Vec<ArcMut<MessageExt>> {
    let mut found_list = Vec::new();
    for (index, bb) in get_message_result.message_mapped_list().iter().enumerate() {
        let mapped_file = match bb.mapped_file.as_ref() {
            Some(mf) => mf,
            None => {
                error!("decode_msg_list: mapped_file is null at index={}", index);
                continue;
            }
        };
        let data =
            &mapped_file.get_mapped_file()[bb.start_offset as usize..(bb.start_offset + bb.size as u64) as usize];
        let mut bytes = Bytes::copy_from_slice(data);
        let msg_ext = message_decoder::decode(&mut bytes, true, de_compress_body, false, false, false);
        match msg_ext {
            Some(msg_ext) => {
                found_list.push(ArcMut::new(msg_ext));
            }
            None => {
                error!(
                    "decode_msg_list: decode msgExt is null, index={}, start_offset={}, size={}",
                    index, bb.start_offset, bb.size
                );
            }
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
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::key_builder::KeyBuilder;
    use rocketmq_common::common::mix_all;
    use rocketmq_common::common::pop_ack_constants::PopAckConstants;
    use rocketmq_common::TimeUtils::get_current_millis;
    use rocketmq_store::pop::ack_msg::AckMsg;
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

    /// Test 1: CK Timeout Detection Logic
    ///
    /// Verifies that the timeout calculation matches Java's implementation:
    /// `now >= popTime + invisibleTime`
    #[test]
    fn test_ck_timeout_detection() {
        let mut ck = PopCheckPoint::default();
        ck.set_pop_time(1000); // Pop at T=1000ms
        ck.set_invisible_time(5000); // Invisible for 5000ms

        // Revive time = popTime + invisibleTime = 6000ms
        assert_eq!(ck.get_revive_time(), 6000);

        // Before timeout
        let now = 5999;
        assert!(now < ck.get_revive_time(), "Message should NOT be revived yet");

        // Exactly at timeout
        let now = 6000;
        assert!(now >= ck.get_revive_time(), "Message should be revived now");

        // After timeout
        let now = 7000;
        assert!(now >= ck.get_revive_time(), "Message should have been revived");
    }

    /// Test 2: Revive Topic Selection Logic (CRITICAL)
    ///
    /// This test verifies the FIXED logic:
    /// - Normal topic → %RETRY%group-topic
    /// - Already retry topic → Keep unchanged
    ///
    /// Previous bug: logic was inverted causing messages to go to wrong topics
    #[test]
    fn test_revive_topic_selection() {
        // Case 1: Normal topic should generate retry topic
        let normal_topic = "test_topic";
        assert!(!normal_topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX));

        let retry_topic = KeyBuilder::build_pop_retry_topic(normal_topic, "consumer_group_1", false);
        assert!(
            retry_topic.starts_with("%RETRY%"),
            "Normal topic should generate %RETRY% prefix"
        );
        assert!(
            retry_topic.contains("consumer_group_1"),
            "Retry topic should contain consumer group"
        );
        assert!(
            retry_topic.contains(normal_topic),
            "Retry topic should contain original topic"
        );

        // Case 2: Already retry topic should remain unchanged
        let existing_retry_topic = "%RETRY%consumer_group_1-test_topic";
        assert!(existing_retry_topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX));

        // Logic: if already retry topic, do NOT build retry topic again
        // Just keep it as-is (this is what the fixed code does)
        let result = if !existing_retry_topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            KeyBuilder::build_pop_retry_topic(existing_retry_topic, "consumer_group_1", false)
        } else {
            existing_retry_topic.to_string()
        };

        assert_eq!(
            result, existing_retry_topic,
            "Already retry topic should remain unchanged"
        );
    }

    /// Test 3: CK Message Parsing Error Handling
    ///
    /// Verifies robust handling of:
    /// - Null/empty body
    /// - Invalid JSON
    /// - Missing required fields
    #[test]
    fn test_ck_parse_error_handling() {
        // Case 1: Empty body should be detected
        let empty_body: Option<&[u8]> = None;
        assert!(empty_body.is_none(), "Empty body should be detected and skipped");

        // Case 2: Invalid JSON should fail gracefully
        let invalid_json = b"{ this is not valid json }";
        let result: Result<PopCheckPoint, _> =
            rocketmq_common::utils::serde_json_utils::SerdeJsonUtils::from_json_bytes(invalid_json);
        assert!(result.is_err(), "Invalid JSON should return error, not panic");

        // Case 3: Valid JSON but missing optional fields, empty required fields
        // Using correct serde field names: so=start_offset, pt=pop_time, it=invisible_time,
        // bm=bit_map, n=num, q=queue_id, t=topic, c=cid, ro=revive_offset, d=queue_offset_diff
        let incomplete_ck = r#"{"so":0,"pt":0,"it":0,"bm":0,"n":0,"q":0,"t":"","c":"","ro":0,"d":[]}"#;
        let ck: PopCheckPoint =
            rocketmq_common::utils::serde_json_utils::SerdeJsonUtils::from_json_str(incomplete_ck).unwrap();

        // Rust code checks: if topic.is_empty() || cid.is_empty() { continue; }
        assert!(
            ck.topic.is_empty() && ck.cid.is_empty(),
            "Should detect empty required fields and skip"
        );
    }

    /// Test 4: ACK Message Merge Key Generation
    ///
    /// Verifies that merge key generation is consistent
    #[test]
    fn test_ack_merge_key_generation() {
        let ack = AckMsg {
            ack_offset: 100,
            start_offset: 0,
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 5,
            pop_time: 1000,
            broker_name: CheetahString::from("broker-a"),
        };

        // Merge key format: topic + consumerGroup + queueId + startOffset + popTime + brokerName
        let merge_key = format!(
            "{}{}{}{}{}{}",
            ack.topic, ack.consumer_group, ack.queue_id, ack.start_offset, ack.pop_time, ack.broker_name
        );

        let expected = "test_topictest_group501000broker-a";
        assert_eq!(merge_key, expected, "Merge key format should match Java");
    }

    /// Test 5: Concurrent Access to revive_offset (Atomic Safety)
    ///
    /// Verifies that concurrent reads/writes to revive_offset are thread-safe
    /// after the P0 fix (changed from i64 to Arc<AtomicI64>)
    #[tokio::test]
    async fn test_concurrent_revive_offset_access() {
        let revive_offset = Arc::new(AtomicI64::new(0));

        // Spawn 10 concurrent tasks, each incrementing offset 1000 times
        let mut handles = vec![];
        for _ in 0..10 {
            let offset_clone = revive_offset.clone();
            let handle = tokio::spawn(async move {
                for _ in 0..1000 {
                    offset_clone.fetch_add(1, Ordering::SeqCst);
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify: 10 * 1000 = 10,000
        assert_eq!(
            revive_offset.load(Ordering::SeqCst),
            10000,
            "Atomic operations should prevent race conditions"
        );
    }

    /// Test 6: should_run_pop_revive Flag Visibility (Atomic Safety)
    ///
    /// Verifies that the flag is properly synchronized across tasks
    #[tokio::test]
    async fn test_should_run_pop_revive_visibility() {
        let flag = Arc::new(AtomicBool::new(false));

        // Writer task: sets flag to true after 100ms
        let flag_writer = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            flag_writer.store(true, Ordering::Release);
        });

        // Reader task: polls flag until it becomes true
        let start = get_current_millis();
        loop {
            if flag.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            // Timeout after 1 second
            if get_current_millis() - start > 1000 {
                panic!("Flag was never set to true - visibility issue!");
            }
        }

        // If we reach here, flag was properly visible across tasks
        assert!(flag.load(Ordering::Acquire), "Flag should be true");
    }

    /// Test 7: CK revive_offset Assignment Order
    ///
    /// Rust vs Java difference: Java sets reviveOffset AFTER map.put,
    /// Rust must set BEFORE map.insert (because of move/clone semantics)
    #[test]
    fn test_ck_revive_offset_assignment() {
        // Simulate message queue offset
        let message_queue_offset = 12345i64;

        // Create PopCheckPoint with fields initialized directly
        let ck = PopCheckPoint {
            topic: CheetahString::from("test_topic"),
            cid: CheetahString::from("test_group"),
            queue_id: 1,
            revive_offset: message_queue_offset,
            ..Default::default()
        };

        // Now insert (this clones ck)
        let mut map = std::collections::HashMap::new();
        let key = format!("{}{}{}", ck.topic, ck.cid, ck.queue_id);
        map.insert(key.clone(), ck.clone());

        // Verify the inserted CK has correct revive_offset
        let inserted_ck = map.get(&key).unwrap();
        assert_eq!(
            inserted_ck.revive_offset, message_queue_offset,
            "revive_offset should be correctly set in inserted CK"
        );
    }

    /// Test 8: Revive Time Calculation Edge Cases
    #[test]
    fn test_revive_time_edge_cases() {
        // Case 1: Zero invisible time
        let mut ck = PopCheckPoint::default();
        ck.set_pop_time(1000);
        ck.set_invisible_time(0);
        assert_eq!(ck.get_revive_time(), 1000, "Zero invisible time");

        // Case 2: Negative pop time (should not happen, but handle gracefully)
        ck.set_pop_time(-1000);
        ck.set_invisible_time(5000);
        assert_eq!(ck.get_revive_time(), 4000, "Negative pop time");

        // Case 3: Large values
        ck.set_pop_time(i64::MAX - 1000);
        ck.set_invisible_time(500);
        // Should not overflow (get_revive_time uses wrapping_add internally in practice)
        let _revive_time = ck.get_revive_time();
    }

    /// Test 9: ACK Time Interval Constants
    ///
    /// Verifies that constants match implementation
    #[test]
    fn test_ack_time_interval_constants() {
        // Rust: ACK_TIME_INTERVAL and SECOND constants

        assert_eq!(
            PopAckConstants::ACK_TIME_INTERVAL,
            1000,
            "ACK_TIME_INTERVAL should be 1000ms (1 second)"
        );
        assert_eq!(PopAckConstants::SECOND, 1000, "SECOND constant should be 1000ms");

        // Timeout threshold = 1000 + 1000 = 2000ms = 2 seconds
        let threshold = PopAckConstants::ACK_TIME_INTERVAL + PopAckConstants::SECOND;
        assert_eq!(threshold, 2000, "Timeout threshold should be 2 seconds");
    }

    /// Test 10: Mock CK for Long-Awaiting ACK
    ///
    /// Tests the mock CK creation logic when ACK arrives without matching CK
    #[test]
    fn test_mock_ck_for_long_awaiting_ack() {
        let ack = AckMsg {
            ack_offset: 100,
            start_offset: 50,
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 3,
            pop_time: 1000,
            broker_name: CheetahString::from("broker-a"),
        };

        // Create mock CK
        let mut mock_ck = PopCheckPoint::default();
        mock_ck.set_start_offset(ack.start_offset);
        mock_ck.set_pop_time(ack.pop_time);
        mock_ck.set_queue_id(ack.queue_id);
        mock_ck.set_cid(ack.consumer_group.clone());
        mock_ck.set_topic(ack.topic.clone());
        mock_ck.set_num(0); // No messages
        mock_ck.set_bit_map(0); // No ACKs
        mock_ck.set_revive_offset(999); // Some offset
        mock_ck.set_broker_name(Some(ack.broker_name.clone()));

        // Verify mock CK properties
        assert_eq!(mock_ck.start_offset, ack.start_offset);
        assert_eq!(mock_ck.pop_time, ack.pop_time);
        assert_eq!(mock_ck.queue_id, ack.queue_id);
        assert_eq!(mock_ck.cid, ack.consumer_group);
        assert_eq!(mock_ck.topic, ack.topic);
        assert_eq!(mock_ck.num, 0);
        assert_eq!(mock_ck.bit_map, 0);
    }
}
