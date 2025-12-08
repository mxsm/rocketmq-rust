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
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::running::running_stats::RunningStats;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::FileUtils;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::queue::consume_queue_store::ConsumeQueueStoreTrait;
use rocketmq_store::queue::local_file_consume_queue_store::ConsumeQueueStore;
use rocketmq_store::store_path_config_helper::get_delay_offset_store_path;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::schedule::delay_offset_serialize_wrapper::DelayOffsetSerializeWrapper;

// Constants
const FIRST_DELAY_TIME: u64 = 1000;
const DELAY_FOR_A_WHILE: u64 = 100;
const DELAY_FOR_A_PERIOD: u64 = 10000;
const WAIT_FOR_SHUTDOWN: u64 = 5000;
const DELAY_FOR_A_SLEEP: u64 = 10;

pub type DeliverPendingTable<MS> = Arc<DashMap<i32, Arc<Mutex<VecDeque<PutResultProcess<MS>>>>>>;

/// `ScheduleMessageService` is the core service in RocketMQ specifically designed to manage and
/// deliver delayed messages (scheduled messages). It supports the consumption of messages after a
/// specified time through predefined delay levels, making it suitable for scenarios such as order
/// timeouts and triggering scheduled tasks.
///
/// ### Core Features
///
/// #### 1. Storage Management of Delayed Messages
///
/// **Receiving Delayed Messages**:
/// When a producer sends a delayed message, the Broker temporarily stores the message in a special
/// internal Topic (`SCHEDULE_TOPIC_XXXX`) instead of directly writing it to the target Topic. Each
/// delay level corresponds to a queue (e.g., Queue 0 of `SCHEDULE_TOPIC_XXXX` corresponds to a 1 -
/// second delay).
///
/// **Storage Structure**:
/// - The original Topic and queue information of the message are stored in the message attributes.
/// - The delay time is specified by the `delayTimeLevel` parameter (e.g., Level 3 corresponds to a
///   10 - second delay).
///
/// #### 2. Periodic Scanning and Message Redelivery
///
/// **Scheduled Task Scheduling**:
/// An independent scheduled task is created for each delay level to periodically scan the messages
/// in the corresponding queue.
///
/// **Delay Time Check**:
/// When the preset delay time of a message arrives, it is retrieved from `SCHEDULE_TOPIC_XXXX` and
/// redelivered to the queue of the original target Topic.
///
/// **Consumption Visibility**:
/// After delivery, the message can be normally pulled by the consumer, ensuring the delay takes
/// effect.
///
/// #### 3. Exception Recovery and Consistency Assurance
///
/// **Crash Recovery**:
/// When the Broker restarts, it recovers the unprocessed delayed messages to avoid message loss.
///
/// **Duplicate Delivery Prevention**:
/// Duplicate delivery is prevented through the unique message key (`uniqKey`) and the storage
/// offset (`commitLogOffset`).
pub struct ScheduleMessageService<MS: MessageStore> {
    delay_level_table: ArcMut<BTreeMap<i32 /* level */, i64 /* delay timeMillis */>>,
    offset_table: ArcMut<DashMap<i32, i64>>,
    started: AtomicBool,
    max_delay_level: AtomicI32,
    data_version: ArcMut<DataVersion>,
    enable_async_deliver: bool,
    deliver_pending_table: DeliverPendingTable<MS>,
    broker_controller: ArcMut<BrokerRuntimeInner<MS>>,
    version_change_counter: AtomicI64,
}

impl<MS: MessageStore> ScheduleMessageService<MS> {
    pub fn new(broker_controller: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let enable_async_deliver = broker_controller
            .message_store_config()
            .enable_schedule_async_deliver;

        Self {
            delay_level_table: ArcMut::new(BTreeMap::new()),
            offset_table: ArcMut::new(DashMap::new()),
            started: AtomicBool::new(false),
            max_delay_level: AtomicI32::new(0),
            data_version: ArcMut::new(DataVersion::new()),
            enable_async_deliver,
            deliver_pending_table: Arc::new(DashMap::new()),
            broker_controller,
            version_change_counter: AtomicI64::new(0),
        }
    }

    pub fn build_running_stats(&self, stats: &mut HashMap<String, String>) {
        for entry in self.offset_table.iter() {
            let delay_level = entry.key();
            let delay_offset = entry.value();

            let queue_id = delay_level_to_queue_id(*delay_level);
            let max_offset = self
                .broker_controller
                .message_store()
                .unwrap()
                .get_max_offset_in_queue(
                    &CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                    queue_id,
                );

            let value = format!("{delay_offset},{max_offset}");
            let key = format!(
                "{}_{}",
                RunningStats::ScheduleMessageOffset.as_str(),
                delay_level
            );
            stats.insert(key, value);
        }
    }

    fn update_offset(&self, delay_level: i32, offset: i64) {
        self.offset_table.insert(delay_level, offset);

        let version_counter = self.version_change_counter.fetch_add(1, Ordering::SeqCst);
        if version_counter
            % self
                .broker_controller
                .broker_config()
                .delay_offset_update_version_step as i64
            == 0
        {
            let state_machine_version = self
                .broker_controller
                .message_store_unchecked()
                .get_state_machine_version();

            let data_version = self.data_version.mut_from_ref();
            data_version.next_version_with(state_machine_version);
        }
    }

    pub fn compute_deliver_timestamp(&self, delay_level: i32, store_timestamp: i64) -> i64 {
        if let Some(time) = self.delay_level_table.get(&delay_level) {
            *time + store_timestamp
        } else {
            store_timestamp + 1000
        }
    }

    pub fn start(this: ArcMut<Self>) -> Result<(), Box<dyn std::error::Error>> {
        if this
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(false)
        {
            // maybe need to optimize
            this.load();

            for (level, _time_delay) in this.delay_level_table.iter() {
                let offset = {
                    this.offset_table
                        .get(level)
                        .map_or(0, |key_value| *key_value.value())
                };

                if this.enable_async_deliver {
                    let level_copy = *level;
                    let service = this.clone();

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(FIRST_DELAY_TIME)).await;
                        let task = HandlePutResultTask::new(level_copy, service);
                        task.run().await;
                    });
                }

                let level_copy = *level;
                let offset_copy = offset;
                let service = this.clone();

                tokio::spawn(async move {
                    let task =
                        DeliverDelayedMessageTimerTask::new(level_copy, offset_copy, service);
                    tokio::time::sleep(Duration::from_millis(FIRST_DELAY_TIME)).await;
                    task.run().await;
                });
            }
            let service = this.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(
                    service
                        .broker_controller
                        .message_store_config()
                        .flush_delay_offset_interval,
                ));
                tokio::time::sleep(Duration::from_millis(10000)).await;
                loop {
                    interval.tick().await;
                    service.persist();
                }
            });
        }

        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.stop();
    }

    pub fn stop(&mut self) -> bool {
        if self
            .started
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            self.persist();
        }

        true
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }

    pub fn get_max_delay_level(&self) -> i32 {
        self.max_delay_level.load(Ordering::Relaxed)
    }

    pub fn get_data_version(&self) -> DataVersion {
        self.data_version.as_ref().clone()
    }

    pub fn set_data_version(&self, data_version: DataVersion) {
        let current = self.data_version.mut_from_ref();
        *current = data_version;
    }

    fn load_super(&self) -> Result<bool, Box<dyn std::error::Error>> {
        // Mock implementation for the parent class load method
        Ok(true)
    }

    pub fn load_when_sync_delay_offset(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let result = self.load_super()?;
        let parse_result = self.parse_delay_level();
        Ok(result && parse_result)
    }

    /// Corrects delay offsets based on actual consume queue state.
    ///
    /// Ensures all offsets are within valid bounds for their respective consume queues.
    /// If an offset is out of bounds, it's corrected and logged.
    ///
    /// # Returns
    ///
    /// `true` if corrections were successful, `false` otherwise
    pub fn correct_delay_offset(&self) -> bool {
        let topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        for delay_level in self.delay_level_table.keys() {
            let queue_id = delay_level_to_queue_id(*delay_level);
            let cq = self
                .broker_controller
                .message_store_unchecked()
                .get_queue_store()
                .downcast_ref::<ConsumeQueueStore>()
                .expect("Failed to downcast to ConsumeQueueStore")
                .find_or_create_consume_queue(&topic, queue_id);

            if let Some(current_delay_offset) = self.offset_table.get(delay_level) {
                let mut correct_delay_offset = *current_delay_offset;
                let cq_min_offset = cq.get_min_offset_in_queue();
                let cq_max_offset = cq.get_max_offset_in_queue();

                if *current_delay_offset < cq_min_offset {
                    correct_delay_offset = cq_min_offset;
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, \
                         queueId={}",
                        *current_delay_offset,
                        cq_min_offset,
                        cq_max_offset,
                        cq.get_queue_id()
                    );
                }

                if *current_delay_offset > cq_max_offset {
                    correct_delay_offset = cq_max_offset;
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, \
                         queueId={}",
                        *current_delay_offset,
                        cq_min_offset,
                        cq_max_offset,
                        cq.get_queue_id()
                    );
                }

                if correct_delay_offset != *current_delay_offset {
                    error!(
                        "correct delay offset [ delayLevel {} ] from {} to {}",
                        delay_level, *current_delay_offset, correct_delay_offset
                    );
                    self.offset_table.insert(*delay_level, correct_delay_offset);
                }
            }
        }

        true
    }

    pub fn parse_delay_level(&self) -> bool {
        let mut time_unit_table = HashMap::new();
        time_unit_table.insert("s", 1000);
        time_unit_table.insert("m", 1000 * 60);
        time_unit_table.insert("h", 1000 * 60 * 60);
        time_unit_table.insert("d", 1000 * 60 * 60 * 24);

        let level_string = self
            .broker_controller
            .message_store_config()
            .message_delay_level
            .as_str();

        let level_array: Vec<&str> = level_string.split(' ').collect();

        let delay_level_table = self.delay_level_table.mut_from_ref();
        let mut max_delay_level = 0;

        for (i, value) in level_array.iter().enumerate() {
            let ch = value.chars().last().unwrap().to_string();
            let tu = time_unit_table
                .get(&ch.as_str())
                .ok_or(format!("Unknown time unit: {ch}"));
            if tu.is_err() {
                return false;
            }
            let tu = *tu.unwrap();

            let level = i as i32 + 1;
            if level > max_delay_level {
                max_delay_level = level;
            }

            let num_str = &value[0..value.len() - 1];
            let num = num_str.parse::<i64>();
            if num.is_err() {
                return false;
            }
            let num = num.unwrap();
            let delay_time_millis = tu * num;

            delay_level_table.insert(level, delay_time_millis);

            if self.enable_async_deliver {
                self.deliver_pending_table
                    .insert(level, Arc::new(Mutex::new(VecDeque::new())));
            }
        }

        self.max_delay_level
            .store(max_delay_level, Ordering::Relaxed);
        true
    }

    fn message_time_up(&self, msg_ext: MessageExt) -> MessageExtBrokerInner {
        let mut inner = MessageExtBrokerInner::default();
        let sys_flag = msg_ext.sys_flag();
        let born_timestamp = msg_ext.born_timestamp();
        let born_host = msg_ext.born_host();
        let store_host = msg_ext.store_host();
        let reconsume_times = msg_ext.reconsume_times();
        let message = msg_ext.message;
        if let Some(body) = message.body {
            inner.set_body(body);
        }
        inner.set_flag(message.flag);
        MessageAccessor::set_properties(&mut inner, message.properties);
        let topic_filter_type = message_single::parse_topic_filter_type(inner.sys_flag());
        let tags_code = MessageExtBrokerInner::tags_string2tags_code(
            &topic_filter_type,
            inner.get_tags().as_ref().unwrap_or(&CheetahString::empty()),
        );
        inner.tags_code = tags_code;
        inner.properties_string =
            MessageDecoder::message_properties_to_string(inner.get_properties());
        inner.message_ext_inner.sys_flag = sys_flag;
        inner.message_ext_inner.born_timestamp = born_timestamp;
        inner.message_ext_inner.born_host = born_host;
        inner.message_ext_inner.store_host = store_host;
        inner.message_ext_inner.reconsume_times = reconsume_times;
        inner.set_wait_store_msg_ok(false);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELIVER_MS);
        MessageAccessor::clear_property(&mut inner, MessageConst::PROPERTY_TIMER_DELAY_SEC);
        let topic = inner.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_REAL_TOPIC,
        ));
        if let Some(topic) = topic {
            inner.set_topic(topic);
        }
        let queue_id = inner.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_REAL_QUEUE_ID,
        ));
        if let Some(queue_id) = queue_id {
            inner.message_ext_inner.queue_id = queue_id.parse::<i32>().unwrap();
        }

        inner
    }

    /// Gets a copy of the current offset table.
    ///
    /// # Returns
    ///
    /// A HashMap containing all delay levels and their current offsets
    pub fn get_offset_table(&self) -> HashMap<i32, i64> {
        self.offset_table
            .as_ref()
            .iter()
            .fold(HashMap::new(), |mut acc, item| {
                acc.insert(*item.key(), *item.value());
                acc
            })
    }
}

impl<MS: MessageStore> ConfigManager for ScheduleMessageService<MS> {
    fn load(&self) -> bool {
        let result = {
            let file_name = self.config_file_path();
            let result = FileUtils::file_to_string(file_name.as_str());
            match result {
                Ok(ref content) => {
                    if content.is_empty() {
                        warn!("load bak config file");
                        self.load_bak()
                    } else {
                        self.decode(content);
                        info!("load Config file: {} -----OK", file_name);
                        true
                    }
                }
                Err(_) => self.load_bak(),
            }
        };
        let parse_result = self.parse_delay_level();
        let correct_result = self.correct_delay_offset();

        result && parse_result && correct_result
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&self) -> String {
        get_delay_offset_store_path(
            self.broker_controller
                .broker_config()
                .store_path_root_dir
                .as_str(),
        )
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        let delay_offset_serialize_wrapper = DelayOffsetSerializeWrapper::new(
            Some(self.get_offset_table()),
            Some(self.get_data_version()),
        );
        if pretty_format {
            delay_offset_serialize_wrapper
                .serialize_json_pretty()
                .expect("Failed to encode pretty")
        } else {
            delay_offset_serialize_wrapper
                .serialize_json()
                .expect("Failed to encode pretty")
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let delay_offset_serialize_wrapper =
            SerdeJsonUtils::from_json_str::<DelayOffsetSerializeWrapper>(json_string).unwrap();

        if let Some(offset_table_value) = delay_offset_serialize_wrapper.offset_table() {
            self.offset_table
                .mut_from_ref()
                .extend(offset_table_value.clone());
        }
        if let Some(data_version) = delay_offset_serialize_wrapper.data_version() {
            let current = self.data_version.mut_from_ref();
            current.assign_new_one(data_version);
        }
    }
}

/// Task for delivering delayed messages when their time is up
pub struct DeliverDelayedMessageTimerTask<MS: MessageStore> {
    /// The delay level for this task
    delay_level: i32,

    /// The offset in the consume queue to start from
    offset: i64,

    /// Reference to the parent service
    schedule_service: ArcMut<ScheduleMessageService<MS>>,
}

impl<MS: MessageStore> DeliverDelayedMessageTimerTask<MS> {
    /// Create a new timer task for delivering delayed messages
    pub fn new(
        delay_level: i32,
        offset: i64,
        schedule_service: ArcMut<ScheduleMessageService<MS>>,
    ) -> Self {
        Self {
            delay_level,
            offset,
            schedule_service,
        }
    }

    /// Execute the task
    pub async fn run(&self) {
        if !self.schedule_service.is_started() {
            return;
        }

        match self.execute_on_time_up().await {
            Ok(_) => {}
            Err(e) => {
                error!("ScheduleMessageService, executeOnTimeUp exception: {}", e);
                self.schedule_next_timer_task(self.offset, DELAY_FOR_A_PERIOD);
            }
        }
    }

    /// Correct the delivery timestamp if it's too far in the future
    fn correct_deliver_timestamp(&self, now: i64, deliver_timestamp: i64) -> i64 {
        let delay_time = *self
            .schedule_service
            .delay_level_table
            .get(&self.delay_level)
            .unwrap();
        let max_timestamp = now + delay_time;

        if deliver_timestamp > max_timestamp {
            now
        } else {
            deliver_timestamp
        }
    }

    /// Execute when the scheduled time is up for messages
    async fn execute_on_time_up(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get the consume queue for this delay level
        let queue_id = delay_level_to_queue_id(self.delay_level);
        let cq = self
            .schedule_service
            .broker_controller
            .message_store_unchecked()
            .get_consume_queue(
                &CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                queue_id,
            );
        if cq.is_none() {
            self.schedule_next_timer_task(self.offset, DELAY_FOR_A_WHILE);
            return Ok(());
        }
        let cq = cq.unwrap();
        // Get an iterator to the consume queue starting at the current offset
        let mut buffer_cq = match cq.iterate_from(self.offset) {
            Some(iter) => iter,
            None => {
                let reset_offset = if cq.get_min_offset_in_queue() > self.offset {
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                        self.offset,
                        cq.get_min_offset_in_queue(),
                        cq.get_queue_id()
                    );
                    cq.get_min_offset_in_queue()
                } else if cq.get_max_offset_in_queue() < self.offset {
                    error!(
                        "schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                        self.offset,
                        cq.get_max_offset_in_queue(),
                        cq.get_queue_id()
                    );
                    cq.get_max_offset_in_queue()
                } else {
                    self.offset
                };

                self.schedule_next_timer_task(reset_offset, DELAY_FOR_A_WHILE);
                return Ok(());
            }
        };

        let mut next_offset = self.offset;

        // Process each message in the consume queue
        while self.schedule_service.is_started() {
            let cq_unit = if let Some(unit) = buffer_cq.next() {
                unit
            } else {
                break;
            };
            let offset_py = cq_unit.pos;
            let size_py = cq_unit.size;
            let mut tags_code = cq_unit.tags_code;

            // Handle invalid tags code
            if !cq_unit.is_tags_code_valid() {
                error!(
                    "[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, \
                     sizePy={}",
                    tags_code, offset_py, size_py
                );

                let msg_store_time = self
                    .schedule_service
                    .broker_controller
                    .message_store_unchecked()
                    .get_commit_log()
                    .pickup_store_timestamp(offset_py, size_py);

                tags_code = self
                    .schedule_service
                    .compute_deliver_timestamp(self.delay_level, msg_store_time);
            }

            // Check if it's time to deliver the message
            let now = get_current_millis() as i64;
            let deliver_timestamp = self.correct_deliver_timestamp(now, tags_code);

            let curr_offset = cq_unit.queue_offset;
            assert_eq!(cq_unit.batch_num, 1);
            next_offset = curr_offset + cq_unit.batch_num as i64;

            let countdown = deliver_timestamp - now;
            if countdown > 0 {
                self.schedule_next_timer_task(curr_offset, DELAY_FOR_A_WHILE);
                self.schedule_service
                    .update_offset(self.delay_level, curr_offset);
                return Ok(());
            }

            // Look up the actual message
            let msg_ext = match self
                .schedule_service
                .broker_controller
                .message_store_unchecked()
                .look_message_by_offset_with_size(offset_py, size_py)
            {
                Some(msg) => msg,
                None => continue,
            };
            let msg_id = msg_ext.msg_id().clone();
            // Process the message for delivery
            let msg_inner = self.schedule_service.message_time_up(msg_ext);

            // Check for transaction half messages which should be discarded
            if msg_inner.get_topic() == TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC {
                error!(
                    "[BUG] the real topic of schedule msg is {}, discard the msg. msg={:?}",
                    msg_inner.get_topic(),
                    msg_inner
                );
                continue;
            }

            // Deliver the message
            let deliver_suc = if self.schedule_service.enable_async_deliver {
                self.async_deliver(msg_inner, msg_id, curr_offset, offset_py, size_py)
                    .await?
            } else {
                self.sync_deliver(msg_inner, msg_id, curr_offset, offset_py, size_py)
                    .await?
            };

            if !deliver_suc {
                self.schedule_next_timer_task(next_offset, DELAY_FOR_A_WHILE);
                return Ok(());
            }
        }

        // Release the iterator
        buffer_cq.release();

        // Schedule the next task
        self.schedule_next_timer_task(next_offset, DELAY_FOR_A_WHILE);

        Ok(())
    }

    /// Schedule the next timer task
    fn schedule_next_timer_task(&self, offset: i64, delay: u64) {
        let schedule_service = self.schedule_service.clone();
        let delay_level = self.delay_level;

        // Schedule the next task after the specified delay
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let task = DeliverDelayedMessageTimerTask::new(delay_level, offset, schedule_service);
            task.run().await;
        });
    }

    /// Deliver a message synchronously
    async fn sync_deliver(
        &self,
        msg_inner: MessageExtBrokerInner,
        msg_id: CheetahString,
        offset: i64,
        offset_py: i64,
        size_py: i32,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut result_process = self
            .deliver_message(msg_inner, msg_id, offset, offset_py, size_py, false)
            .await?;

        // Wait for the result
        let result = result_process.get();
        let send_status = result.put_message_status() == PutMessageStatus::PutOk;

        if send_status {
            self.schedule_service
                .update_offset(self.delay_level, result_process.get_next_offset());
        }

        Ok(send_status)
    }

    /// Deliver a message asynchronously
    async fn async_deliver(
        &self,
        msg_inner: MessageExtBrokerInner,
        msg_id: CheetahString,
        offset: i64,
        offset_py: i64,
        size_py: i32,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let processes_queue_table = self
            .schedule_service
            .deliver_pending_table
            .get(&self.delay_level)
            .unwrap();

        // Flow Control
        let mut processes_queue = processes_queue_table.lock().await;
        let current_pending_num = processes_queue.len();
        let max_pending_limit = self
            .schedule_service
            .broker_controller
            .message_store_config()
            .schedule_async_deliver_max_pending_limit;

        if current_pending_num > max_pending_limit {
            warn!(
                "Asynchronous deliver triggers flow control, currentPendingNum={}, \
                 maxPendingLimit={}",
                current_pending_num, max_pending_limit
            );
            return Ok(false);
        }

        // Check if we're blocked
        if let Some(first_process) = processes_queue.front() {
            if first_process.need2_blocked() {
                warn!("Asynchronous deliver block. info={}", first_process);
                return Ok(false);
            }
        }

        // Deliver the message
        let result_process = self
            .deliver_message(msg_inner, msg_id, offset, offset_py, size_py, true)
            .await?;

        // Add to pending queue
        processes_queue.push_back(result_process);

        Ok(true)
    }

    /// Deliver a message and return a process to track the result
    async fn deliver_message(
        &self,
        msg_inner: MessageExtBrokerInner,
        msg_id: CheetahString,
        offset: i64,
        offset_py: i64,
        size_py: i32,
        auto_resend: bool,
    ) -> Result<PutResultProcess<MS>, Box<dyn std::error::Error + Send + Sync>> {
        // Create a channel for the async result

        let topic = msg_inner.get_topic().clone();
        // Send the message asynchronously
        let result = self
            .schedule_service
            .broker_controller
            .mut_from_ref()
            .escape_bridge_mut()
            .async_put_message(msg_inner)
            .await;

        // Create and return the process tracking object
        let result_process =
            PutResultProcess::new(ArcMut::clone(&self.schedule_service.broker_controller))
                .set_topic(topic)
                .set_delay_level(self.delay_level)
                .set_offset(offset)
                .set_physic_offset(offset_py)
                .set_physic_size(size_py)
                .set_msg_id(msg_id.to_string())
                .set_auto_resend(auto_resend)
                .set_put_message_result(result)
                .then_process()
                .await;

        Ok(result_process)
    }
}

/// Process for handling the result of putting a message
pub struct PutResultProcess<MS: MessageStore> {
    topic: CheetahString,
    offset: i64,
    physic_offset: i64,
    physic_size: i32,
    delay_level: i32,
    msg_id: CheetahString,
    auto_resend: bool,
    put_message_result: Option<PutMessageResult>,
    resend_count: AtomicI32,
    status: ArcMut<ProcessStatus>,
    broker_controller: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> PutResultProcess<MS> {
    /// Create a new PutResultProcess instance
    pub fn new(broker_controller: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            topic: CheetahString::empty(),
            offset: 0,
            physic_offset: 0,
            physic_size: 0,
            delay_level: 0,
            msg_id: CheetahString::empty(),
            auto_resend: false,
            put_message_result: None,
            resend_count: AtomicI32::new(0),
            status: ArcMut::new(ProcessStatus::Running),
            broker_controller,
        }
    }

    /// Set the topic for this process
    pub fn set_topic(mut self, topic: impl Into<CheetahString>) -> Self {
        self.topic = topic.into();
        self
    }

    /// Set the offset for this process
    pub fn set_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// Set the physical offset for this process
    pub fn set_physic_offset(mut self, physic_offset: i64) -> Self {
        self.physic_offset = physic_offset;
        self
    }

    /// Set the physical size for this process
    pub fn set_physic_size(mut self, physic_size: i32) -> Self {
        self.physic_size = physic_size;
        self
    }

    /// Set the delay level for this process
    pub fn set_delay_level(mut self, delay_level: i32) -> Self {
        self.delay_level = delay_level;
        self
    }

    /// Set the message ID for this process
    pub fn set_msg_id(mut self, msg_id: impl Into<CheetahString>) -> Self {
        self.msg_id = msg_id.into();
        self
    }

    /// Set whether to automatically resend on failure
    pub fn set_auto_resend(mut self, auto_resend: bool) -> Self {
        self.auto_resend = auto_resend;
        self
    }

    /// Set the future for this process
    pub fn set_put_message_result(mut self, put_result: PutMessageResult) -> Self {
        self.put_message_result = Some(put_result);
        self
    }

    /// Get the topic
    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    /// Get the offset
    pub fn get_offset(&self) -> i64 {
        self.offset
    }

    /// Get the next offset
    pub fn get_next_offset(&self) -> i64 {
        self.offset + 1
    }

    /// Get the physical offset
    pub fn get_physic_offset(&self) -> i64 {
        self.physic_offset
    }

    /// Get the physical size
    pub fn get_physic_size(&self) -> i32 {
        self.physic_size
    }

    /// Get the delay level
    pub fn get_delay_level(&self) -> i32 {
        self.delay_level
    }

    /// Get the message ID
    pub fn get_msg_id(&self) -> &str {
        &self.msg_id
    }

    /// Check if auto-resend is enabled
    pub fn is_auto_resend(&self) -> bool {
        self.auto_resend
    }

    /// Get the resend count
    pub fn get_resend_count(&self) -> i32 {
        self.resend_count.load(Ordering::Relaxed)
    }

    /// Handle the processing after completing the future
    pub async fn then_process(self) -> Self {
        // Create a clone of self that will be captured in the async closure
        if let Some(put_message_result) = &self.put_message_result {
            self.handle_result(put_message_result);
        }

        self
        /*let this = Arc::new(self);
        let this_clone = Arc::clone(&this);

        // Handle the future completion
        tokio::spawn(async move {
            if let Some(mut future) = this_clone.future.clone() {
                match future.await {
                    Ok(result) => {
                        this_clone.handle_result(result);
                    }
                    Err(e) => {
                        error!(
                            "ScheduleMessageService put message exceptionally, info: {}",
                            this_clone,
                        );
                        this_clone.on_exception();
                    }
                }
            }
        });

        // Unwrap the Arc to return self
        // This is safe because we're the only owner at this point
        match Arc::try_unwrap(this) {
            Ok(this) => this,
            Err(_) => panic!("Failed to unwrap Arc in then_process"),
        }*/
    }

    /// Handle the result of a put operation
    fn handle_result(&self, result: &PutMessageResult) {
        if result.put_message_status() == PutMessageStatus::PutOk {
            self.on_success(result);
        } else {
            self.on_exception();
        }
    }

    /// Handle a successful put operation
    pub fn on_success(&self, result: &PutMessageResult) {
        *self.status.mut_from_ref() = ProcessStatus::Success;

        if self
            .broker_controller
            .message_store_config()
            .enable_schedule_message_stats
            && !result.remote_put()
        {
            /*// Update stats in broker controller
            let broker_stats_manager = self.broker_controller.get_broker_stats_manager();
            broker_stats_manager.inc_queue_get_nums(
                MixAll::SCHEDULE_CONSUMER_GROUP,
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                self.delay_level - 1,
                result.get_append_message_result().get_msg_num(),
            );

            broker_stats_manager.inc_queue_get_size(
                MixAll::SCHEDULE_CONSUMER_GROUP,
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                self.delay_level - 1,
                result.get_append_message_result().get_wrote_bytes(),
            );

            broker_stats_manager.inc_group_get_nums(
                MixAll::SCHEDULE_CONSUMER_GROUP,
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                result.get_append_message_result().get_msg_num(),
            );

            broker_stats_manager.inc_group_get_size(
                MixAll::SCHEDULE_CONSUMER_GROUP,
                TopicValidator::RMQ_SYS_SCHEDULE_TOPIC,
                result.get_append_message_result().get_wrote_bytes(),
            );

            // Update metrics
            let attributes = BrokerMetricsManager::new_attributes_builder()
                .put(LABEL_TOPIC, TopicValidator::RMQ_SYS_SCHEDULE_TOPIC)
                .put(LABEL_CONSUMER_GROUP, MixAll::SCHEDULE_CONSUMER_GROUP)
                .put(LABEL_IS_SYSTEM, true)
                .build();

            BrokerMetricsManager::messages_out_total().add(
                result.get_append_message_result().get_msg_num() as i64,
                &attributes,
            );

            BrokerMetricsManager::throughput_out_total().add(
                result.get_append_message_result().get_wrote_bytes() as i64,
                &attributes,
            );

            // Update topic stats
            broker_stats_manager.inc_topic_put_nums(
                &self.topic,
                result.get_append_message_result().get_msg_num(),
                1,
            );

            broker_stats_manager.inc_topic_put_size(
                &self.topic,
                result.get_append_message_result().get_wrote_bytes(),
            );

            broker_stats_manager.inc_broker_put_nums(
                &self.topic,
                result.get_append_message_result().get_msg_num(),
            );

            // Update message in metrics
            let attributes = BrokerMetricsManager::new_attributes_builder()
                .put(LABEL_TOPIC, &self.topic)
                .put(
                    LABEL_MESSAGE_TYPE,
                    TopicMessageType::Delay.get_metrics_value(),
                )
                .put(
                    LABEL_IS_SYSTEM,
                    TopicValidator::is_system_topic(&self.topic),
                )
                .build();

            BrokerMetricsManager::messages_in_total().add(
                result.get_append_message_result().get_msg_num() as i64,
                &attributes,
            );

            BrokerMetricsManager::throughput_in_total().add(
                result.get_append_message_result().get_wrote_bytes() as i64,
                &attributes,
            );

            BrokerMetricsManager::message_size().record(
                result.get_append_message_result().get_wrote_bytes() as f64
                    / result.get_append_message_result().get_msg_num() as f64,
                &attributes,
            );*/
        }
    }

    /// Handle an exception during processing
    pub fn on_exception(&self) {
        warn!("ScheduleMessageService onException, info: {}", self);

        let status_guard = self.status.mut_from_ref();
        *status_guard = if self.auto_resend {
            ProcessStatus::Exception
        } else {
            ProcessStatus::Skip
        };
    }

    /// Get the current processing status
    pub fn get_status(&self) -> ProcessStatus {
        *self.status.as_ref()
    }

    /// Get the result
    pub fn get(&mut self) -> PutMessageResult {
        match self.put_message_result.take() {
            None => PutMessageResult::new_default(PutMessageStatus::UnknownError),
            Some(value) => value,
        }
    }

    /// Resend the message
    pub async fn do_resend(&self) {
        info!("Resend message, info: {}", self);

        // Gradually increase the resend interval
        let sleep_time = std::cmp::min(
            (self.resend_count.fetch_add(1, Ordering::SeqCst) + 1) * 100,
            60 * 1000,
        );
        tokio::time::sleep(Duration::from_millis(sleep_time as u64)).await;

        // Look up the message and resend it
        match self
            .broker_controller
            .message_store_unchecked()
            .look_message_by_offset_with_size(self.physic_offset, self.physic_size)
        {
            Some(msg_ext) => {
                // Convert to inner message
                let schedule_service = self.broker_controller.schedule_message_service();
                let msg_inner = schedule_service.message_time_up(msg_ext);

                // Try to put the message
                let result = self
                    .broker_controller
                    .mut_from_ref()
                    .escape_bridge_mut()
                    .put_message(msg_inner)
                    .await;

                self.handle_result(&result);
            }
            None => {
                warn!(
                    "ScheduleMessageService resend not found message. info: {}",
                    self
                );
                *self.status.mut_from_ref() = if self.need2_skip() {
                    ProcessStatus::Skip
                } else {
                    ProcessStatus::Exception
                };
            }
        }
    }

    /// Check if processing needs to be blocked
    pub fn need2_blocked(&self) -> bool {
        let max_resend_num2_blocked = self
            .broker_controller
            .message_store_config()
            .schedule_async_deliver_max_resend_num2_blocked;

        self.resend_count.load(Ordering::Relaxed) > max_resend_num2_blocked as i32
    }

    /// Check if processing needs to be skipped
    pub fn need2_skip(&self) -> bool {
        let max_resend_num2_blocked = self
            .broker_controller
            .message_store_config()
            .schedule_async_deliver_max_resend_num2_blocked;

        self.resend_count.load(Ordering::Relaxed) > (max_resend_num2_blocked * 2) as i32
    }
}

impl<MS: MessageStore> Display for PutResultProcess<MS> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PutResultProcess{{topic='{}', offset={}, physicOffset={}, physicSize={}, \
             delayLevel={}, msgId='{}', autoResend={}, resendCount={}, status={}}}",
            self.topic,
            self.offset,
            self.physic_offset,
            self.physic_size,
            self.delay_level,
            self.msg_id,
            self.auto_resend,
            self.resend_count.load(Ordering::Relaxed),
            self.status.as_ref(),
        )
    }
}

/// Represents the status of a message processing operation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessStatus {
    /// In process, the processing result has not yet been returned
    Running,

    /// Put message success
    Success,

    /// Put message exception. When auto_resend is true, the message will be resent
    Exception,

    /// Skip put message. When the message cannot be looked up, the message will be skipped
    Skip,
}

impl Display for ProcessStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ProcessStatus::Running => write!(f, "RUNNING"),
            ProcessStatus::Success => write!(f, "SUCCESS"),
            ProcessStatus::Exception => write!(f, "EXCEPTION"),
            ProcessStatus::Skip => write!(f, "SKIP"),
        }
    }
}

/// Task for handling results of asynchronous message puts
pub struct HandlePutResultTask<MS: MessageStore> {
    /// Delay level this task is handling
    delay_level: i32,

    /// Reference to the parent service
    schedule_service: ArcMut<ScheduleMessageService<MS>>,
}

impl<MS: MessageStore> HandlePutResultTask<MS> {
    /// Create a new task for handling put results at the specified delay level
    pub fn new(delay_level: i32, schedule_service: ArcMut<ScheduleMessageService<MS>>) -> Self {
        Self {
            delay_level,
            schedule_service,
        }
    }

    /// Execute the task to process pending results
    pub async fn run(&self) {
        // Get the pending queue for this delay level
        let pending_queue_guard = match self
            .schedule_service
            .deliver_pending_table
            .get(&self.delay_level)
        {
            Some(queue) => queue,
            None => {
                // If queue doesn't exist, schedule next task and return
                self.schedule_next_task();
                return;
            }
        };
        let mut pending_queue_guard = pending_queue_guard.lock().await;
        // Process each result in the queue
        while let Some(process) = pending_queue_guard.front_mut() {
            match process.get_status() {
                ProcessStatus::Success => {
                    // Update offset and remove the process from queue
                    self.schedule_service
                        .update_offset(self.delay_level, process.get_next_offset());
                    let _ = pending_queue_guard.pop_front();
                }
                ProcessStatus::Running => {
                    // If any process is still running, schedule next task and return
                    self.schedule_next_task();
                    return;
                }
                ProcessStatus::Exception => {
                    // If service is stopped, don't continue processing
                    if !self.schedule_service.is_started() {
                        warn!("HandlePutResultTask shutdown, info={}", process);
                        return;
                    }

                    // Otherwise log warning and try resending
                    warn!("putResultProcess error, info={}", process);
                    process.do_resend().await;
                    break;
                }
                ProcessStatus::Skip => {
                    // Log and remove skipped processes
                    warn!("putResultProcess skip, info={}", process);
                    let _ = pending_queue_guard.pop_front();
                    break;
                }
            }
        }
        // Schedule the next execution of this task
        self.schedule_next_task();
    }

    /// Schedule the next execution of the handler task
    fn schedule_next_task(&self) {
        // Only schedule if the service is still running
        if self.schedule_service.is_started() {
            let delay_level = self.delay_level;
            let schedule_service = ArcMut::clone(&self.schedule_service);

            // Schedule after a short delay
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(DELAY_FOR_A_SLEEP)).await; // DELAY_FOR_A_SLEEP
                let task = HandlePutResultTask::new(delay_level, schedule_service);
                task.run().await;
            });
        }
    }
}

#[inline]
pub fn queue_id_to_delay_level(queue_id: i32) -> i32 {
    queue_id + 1
}

#[inline]
pub fn delay_level_to_queue_id(delay_level: i32) -> i32 {
    delay_level - 1
}
