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
use std::thread;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::running::running_stats::RunningStats;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStoreInner;
use rocketmq_store::log_file::MessageStore;
use rocketmq_store::store_path_config_helper::get_delay_offset_store_path;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task;
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

pub struct ScheduleMessageService<MS> {
    delay_level_table: RwLock<BTreeMap<i32, i64>>,
    offset_table: RwLock<HashMap<i32, i64>>,
    started: AtomicBool,
    max_delay_level: AtomicI32,
    data_version: RwLock<DataVersion>,
    enable_async_deliver: bool,

    deliver_pending_table: RwLock<HashMap<i32, Arc<Mutex<VecDeque<PutResultProcess<MS>>>>>>,
    broker_controller: ArcMut<BrokerRuntimeInner<MS>>,
    version_change_counter: AtomicI64,
}

impl<MS: MessageStore> ScheduleMessageService<MS> {
    pub fn new(broker_controller: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let enable_async_deliver = broker_controller
            .message_store_config()
            .enable_schedule_async_deliver;

        Self {
            delay_level_table: RwLock::new(BTreeMap::new()),
            offset_table: RwLock::new(HashMap::new()),
            started: AtomicBool::new(false),
            max_delay_level: AtomicI32::new(0),
            data_version: RwLock::new(DataVersion::new()),
            enable_async_deliver,
            deliver_pending_table: RwLock::new(HashMap::new()),
            broker_controller,
            version_change_counter: AtomicI64::new(0),
        }
    }

    pub fn queue_id_to_delay_level(queue_id: i32) -> i32 {
        queue_id + 1
    }

    pub fn delay_level_to_queue_id(delay_level: i32) -> i32 {
        delay_level - 1
    }

    pub async fn build_running_stats(&self, stats: &mut HashMap<String, String>) {
        let offset_table = self.offset_table.read();

        for (delay_level, delay_offset) in offset_table.iter() {
            let queue_id = Self::delay_level_to_queue_id(*delay_level);
            let max_offset = self
                .broker_controller
                .message_store()
                .as_ref()
                .unwrap()
                .get_max_offset_in_queue(
                    &CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                    queue_id,
                );

            let value = format!("{},{}", delay_offset, max_offset);
            let key = format!(
                "{}_{}",
                RunningStats::ScheduleMessageOffset.as_str(),
                delay_level
            );
            stats.insert(key, value);
        }
    }

    async fn update_offset(&self, delay_level: i32, offset: i64) {
        {
            let mut offset_table = self.offset_table.write();
            offset_table.insert(delay_level, offset);
        }

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

            let mut data_version = self.data_version.write();
            data_version.next_version_with(state_machine_version);
        }
    }

    pub fn compute_deliver_timestamp(&self, delay_level: i32, store_timestamp: i64) -> i64 {
        let delay_level_table = self.delay_level_table.read();
        if let Some(time) = delay_level_table.get(&delay_level) {
            time + store_timestamp
        } else {
            store_timestamp + 1000
        }
    }

    pub fn start(this: ArcMut<Self>) -> Result<(), Box<dyn std::error::Error>> {
        if this
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            // maybe need to optimize
            this.load();

            let delay_level_table = this.delay_level_table.read();
            for (level, time_delay) in delay_level_table.iter() {
                let level = *level;
                let time_delay = *time_delay;

                let mut offset = {
                    let offset_table = this.offset_table.read();
                    offset_table.get(&level).cloned().unwrap_or(0)
                };

                if this.enable_async_deliver {
                    let level_copy = level;
                    let service = this.clone();

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(FIRST_DELAY_TIME)).await;
                        let task = HandlePutResultTask::new(level_copy, service);
                        task.run().await;
                    });
                }

                let level_copy = level;
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
            let deliver_pending_table = self.deliver_pending_table.read().unwrap();
            for (i, queue) in deliver_pending_table.iter() {
                warn!(
                    "deliverPendingTable level: {}, size: {}",
                    i,
                    queue.lock().unwrap().len()
                );
            }

            if let Err(e) = self.persist() {
                error!("Failed to persist during shutdown: {}", e);
            }
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
        self.data_version.read().clone()
    }

    pub fn set_data_version(&self, data_version: DataVersion) {
        let mut current = self.data_version.write();
        *current = data_version;
    }

    fn load_super(&self) -> Result<bool, Box<dyn std::error::Error>> {
        // Mock implementation for the parent class load method
        Ok(true)
    }

    pub fn load_when_sync_delay_offset(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let result = self.load_super()?;
        let parse_result = self.parse_delay_level()?;
        Ok(result && parse_result)
    }

    pub fn correct_delay_offset(&self) -> bool {
        let delay_level_table = self.delay_level_table.read();
        let mut offset_table = self.offset_table.write();
        let topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
        for delay_level in delay_level_table.keys() {
            let queue_id = Self::delay_level_to_queue_id(*delay_level);
            let cq = self
                .broker_controller
                .message_store_unchecked()
                .get_queue_store()
                .find_or_create_consume_queue(&topic, queue_id);

            if let Some(current_delay_offset) = offset_table.get(delay_level) {
                if cq.is_none() {
                    continue;
                }
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
                    offset_table.insert(*delay_level, correct_delay_offset);
                }
            }
        }

        true
    }

    pub async fn parse_delay_level(&self) -> bool {
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

        let handle = Handle::current();

        let mut delay_level_table = self.delay_level_table.write();
        let mut max_delay_level = 0;

        for (i, value) in level_array.iter().enumerate() {
            let ch = value.chars().last().unwrap().to_string();
            let tu = time_unit_table
                .get(&ch.as_str())
                .ok_or(format!("Unknown time unit: {}", ch));
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
                let mut deliver_pending_table = self.deliver_pending_table.write();
                deliver_pending_table.insert(level, Arc::new(Mutex::new(VecDeque::new())));
            }
        }

        self.max_delay_level
            .store(max_delay_level, Ordering::Relaxed);
        true
    }

    fn message_time_up(&self, msg_ext: &MessageExt) -> MessageExtBrokerInner {
        // Mock implementation of converting a MessageExt to MessageExtBrokerInner
        unimplemented!(" messageTimeUp not implemented")
    }

    pub fn get_offset_table(&self) -> HashMap<i32, i64> {
        self.offset_table.read().clone()
    }
}

impl<MS: MessageStore> ConfigManager for ScheduleMessageService<MS> {
    fn load(&self) -> bool {
        let result = ConfigManager::load(self);
        let handle = Handle::current();
        let handle_result = thread::spawn(move || {
            handle.block_on(async {
                let parse_result = self.parse_delay_level().await;
                let correct_result = self.correct_delay_offset().await;
                result && parse_result && correct_result
            })
        })
        .join()
        .unwrap_or(false);

        result && handle_result
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
                .to_json_pretty()
                .expect("Failed to encode pretty")
        } else {
            delay_offset_serialize_wrapper
                .to_json()
                .expect("Failed to encode pretty")
        }
    }

    fn decode(&self, json_string: &str) {
        if json_string.is_empty() {
            return;
        }
        let delay_offset_serialize_wrapper =
            SerdeJsonUtils::from_json_str::<DelayOffsetSerializeWrapper>(json_string).unwrap();

        let mut current_offset_table = self.offset_table.write();
        if let Some(offset_table) = delay_offset_serialize_wrapper.offset_table() {
            current_offset_table.extend(offset_table);
        }
        if let Some(data_version) = delay_offset_serialize_wrapper.data_version() {
            let mut current = self.data_version.write();
            current.assign_new_one(data_version);
        }
    }
}

/// Task for delivering delayed messages when their time is up
pub struct DeliverDelayedMessageTimerTask<MS> {
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
                self.schedule_next_timer_task(self.offset, DELAY_FOR_A_PERIOD)
                    .await;
            }
        }
    }

    /// Correct the delivery timestamp if it's too far in the future
    fn correct_deliver_timestamp(&self, now: i64, deliver_timestamp: i64) -> i64 {
        let delay_time = self.schedule_service.get_delay_level_time(self.delay_level);
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
        let queue_id = ScheduleMessageService::delay_level_to_queue_id(self.delay_level);
        let cq = self
            .schedule_service
            .broker_controller
            .message_store_unchecked()
            .get_consume_queue(
                &CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC),
                queue_id,
            );

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

                self.schedule_next_timer_task(reset_offset, DELAY_FOR_A_WHILE)
                    .await;
                return Ok(());
            }
        };

        let mut next_offset = self.offset;

        // Process each message in the consume queue
        while buffer_cq.has_next() && self.schedule_service.is_started() {
            let cq_unit = buffer_cq.next();
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
                    .get_message_store()
                    .get_commit_log()
                    .pickup_store_timestamp(offset_py, size_py)?;

                tags_code = self
                    .schedule_service
                    .compute_deliver_timestamp(self.delay_level, msg_store_time);
            }

            // Check if it's time to deliver the message
            let now = get_current_millis() as i64;
            let deliver_timestamp = self.correct_deliver_timestamp(now, tags_code);

            let curr_offset = cq_unit.queue_offset;
            assert_eq!(cq_unit.get_batch_num(), 1);
            next_offset = curr_offset + cq_unit.get_batch_num();

            let countdown = deliver_timestamp - now;
            if countdown > 0 {
                self.schedule_next_timer_task(curr_offset, DELAY_FOR_A_WHILE)
                    .await;
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

            // Process the message for delivery
            let msg_inner = self.schedule_service.message_time_up(&msg_ext)?;

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
                self.async_deliver(
                    &msg_inner,
                    msg_ext.msg_id(),
                    curr_offset,
                    offset_py,
                    size_py,
                )
                .await?
            } else {
                self.sync_deliver(
                    &msg_inner,
                    msg_ext.msg_id(),
                    curr_offset,
                    offset_py,
                    size_py,
                )
                .await?
            };

            if !deliver_suc {
                self.schedule_next_timer_task(next_offset, DELAY_FOR_A_WHILE)
                    .await;
                return Ok(());
            }
        }

        // Release the iterator
        buffer_cq.release();

        // Schedule the next task
        self.schedule_next_timer_task(next_offset, DELAY_FOR_A_WHILE)
            .await;

        Ok(())
    }

    /// Schedule the next timer task
    async fn schedule_next_timer_task(&self, offset: i64, delay: u64) {
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
        msg_inner: &MessageExtBrokerInner,
        msg_id: &str,
        offset: i64,
        offset_py: i64,
        size_py: i32,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut result_process = self
            .deliver_message(msg_inner, msg_id, offset, offset_py, size_py, false)
            .await?;

        // Wait for the result
        let result = result_process.get().await;
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
        msg_inner: &MessageExtBrokerInner,
        msg_id: &str,
        offset: i64,
        offset_py: i64,
        size_py: i32,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let processes_queue = self
            .schedule_service
            .get_deliver_pending_queue(self.delay_level)?;

        // Flow Control
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
        if let Some(first_process) = processes_queue.peek() {
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
        processes_queue.push(result_process);

        Ok(true)
    }

    /// Deliver a message and return a process to track the result
    async fn deliver_message(
        &self,
        msg_inner: &MessageExtBrokerInner,
        msg_id: &str,
        offset: i64,
        offset_py: i64,
        size_py: i32,
        auto_resend: bool,
    ) -> Result<PutResultProcess<MS>, Box<dyn std::error::Error + Send + Sync>> {
        // Create a channel for the async result
        let (tx, rx) = oneshot::channel();

        // Send the message asynchronously
        self.schedule_service
            .broker_controller
            .get_escape_bridge()
            .async_put_message(msg_inner.clone(), tx);

        // Create and return the process tracking object
        let result_process =
            PutResultProcess::new(Arc::clone(&self.schedule_service.broker_controller))
                .set_topic(msg_inner.get_topic().to_string())
                .set_delay_level(self.delay_level)
                .set_offset(offset)
                .set_physic_offset(offset_py)
                .set_physic_size(size_py)
                .set_msg_id(msg_id.to_string())
                .set_auto_resend(auto_resend)
                .set_future(rx)
                .then_process()
                .await;

        Ok(result_process)
    }
}

/// Process for handling the result of putting a message
pub struct PutResultProcess<MS> {
    topic: String,
    offset: i64,
    physic_offset: i64,
    physic_size: i32,
    delay_level: i32,
    msg_id: String,
    auto_resend: bool,
    future: Option<oneshot::Receiver<PutMessageResult>>,
    result_sender: Option<oneshot::Sender<PutMessageResult>>,

    resend_count: AtomicI32,
    status: std::sync::RwLock<ProcessStatus>,
    broker_controller: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> PutResultProcess<MS> {
    /// Create a new PutResultProcess instance
    pub fn new(broker_controller: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            topic: String::new(),
            offset: 0,
            physic_offset: 0,
            physic_size: 0,
            delay_level: 0,
            msg_id: String::new(),
            auto_resend: false,
            future: Some(rx),
            result_sender: Some(tx),
            resend_count: AtomicI32::new(0),
            status: std::sync::RwLock::new(ProcessStatus::Running),
            broker_controller,
        }
    }

    /// Set the topic for this process
    pub fn set_topic(mut self, topic: impl Into<String>) -> Self {
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
    pub fn set_msg_id(mut self, msg_id: impl Into<String>) -> Self {
        self.msg_id = msg_id.into();
        self
    }

    /// Set whether to automatically resend on failure
    pub fn set_auto_resend(mut self, auto_resend: bool) -> Self {
        self.auto_resend = auto_resend;
        self
    }

    /// Set the future for this process
    pub fn set_future(mut self, put_result: PutMessageResult) -> Self {
        if let Some(sender) = self.result_sender.take() {
            let _ = sender.send(put_result);
        }
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
        let this = Arc::new(self);
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
        }
    }

    /// Handle the result of a put operation
    fn handle_result(&self, result: PutMessageResult) {
        if result.put_message_status() == PutMessageStatus::PutOk {
            self.on_success(result);
        } else {
            warn!(
                "ScheduleMessageService put message failed. info: {}.",
                result
            );
            self.on_exception();
        }
    }

    /// Handle a successful put operation
    pub fn on_success(&self, result: PutMessageResult) {
        *self.status.write().unwrap() = ProcessStatus::Success;

        if self
            .broker_controller
            .get_message_store()
            .get_message_store_config()
            .is_enable_schedule_message_stats()
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

        let mut status_guard = self.status.write().unwrap();
        *status_guard = if self.auto_resend {
            ProcessStatus::Exception
        } else {
            ProcessStatus::Skip
        };
    }

    /// Get the current processing status
    pub fn get_status(&self) -> ProcessStatus {
        *self.status.read().unwrap()
    }

    /// Get the result from the future
    pub async fn get(&mut self) -> PutMessageResult {
        if let Some(future) = self.future.take() {
            match future.await {
                Ok(result) => result,
                Err(_) => PutMessageResult::new_default(PutMessageStatus::UnknownError),
            }
        } else {
            PutMessageResult::new_default(PutMessageStatus::UnknownError)
        }
    }

    /// Resend the message
    pub fn do_resend(&self) {
        info!("Resend message, info: {}", self);

        // Gradually increase the resend interval
        let sleep_time = std::cmp::min(
            self.resend_count.fetch_add(1, Ordering::SeqCst) * 100,
            60 * 1000,
        );
        //  thread::sleep(Duration::from_millis(sleep_time as u64));

        // Look up the message and resend it
        match self
            .broker_controller
            .get_message_store()
            .look_message_by_offset(self.physic_offset, self.physic_size)
        {
            Some(msg_ext) => {
                // Convert to inner message
                let schedule_service = self.broker_controller.get_schedule_message_service();
                let msg_inner = schedule_service.message_time_up(&msg_ext);

                // Try to put the message
                match self
                    .broker_controller
                    .get_escape_bridge()
                    .put_message(&msg_inner)
                {
                    Ok(result) => {
                        self.handle_result(result);
                        if result.put_message_status() == PutMessageStatus::PutOk {
                            info!("Resend message success, info: {}", self);
                        }
                    }
                    Err(e) => {
                        *self.status.write().unwrap() = ProcessStatus::Exception;
                        error!("Resend message error, info: {}", self, e);
                    }
                }
            }
            None => {
                warn!(
                    "ScheduleMessageService resend not found message. info: {}",
                    self
                );
                *self.status.write().unwrap() = if self.need2_skip() {
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
            .get_message_store()
            .get_message_store_config()
            .get_schedule_async_deliver_max_resend_num2_blocked();

        self.resend_count.load(Ordering::Relaxed) > max_resend_num2_blocked
    }

    /// Check if processing needs to be skipped
    pub fn need2_skip(&self) -> bool {
        let max_resend_num2_blocked = self
            .broker_controller
            .get_message_store()
            .get_message_store_config()
            .get_schedule_async_deliver_max_resend_num2_blocked();

        self.resend_count.load(Ordering::Relaxed) > (max_resend_num2_blocked * 2)
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
            *self.status.read().unwrap(),
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
pub struct HandlePutResultTask<MS> {
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
            .read()
            .get(&self.delay_level)
            .cloned()
        {
            Some(queue) => queue,
            None => {
                // If queue doesn't exist, schedule next task and return
                self.schedule_next_task().await;
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
                    self.schedule_next_task().await;
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
                    process.do_resend();
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
        self.schedule_next_task().await;
    }

    /// Schedule the next execution of the handler task
    async fn schedule_next_task(&self) {
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
