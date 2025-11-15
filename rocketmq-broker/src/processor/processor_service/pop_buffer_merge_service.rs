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
#![allow(unused_variables)]

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::key_builder::KeyBuilder;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::pop_ack_constants::PopAckConstants;
use rocketmq_common::utils::data_converter::DataConverter;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::pop::ack_msg::AckMsg;
use rocketmq_store::pop::batch_ack_msg::BatchAckMsg;
use rocketmq_store::pop::pop_check_point::PopCheckPoint;
use rocketmq_store::pop::AckMessage;
use tokio::select;
use tokio::sync::Notify;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::processor::pop_message_processor::PopMessageProcessor;
use crate::processor::pop_message_processor::QueueLockManager;

pub(crate) struct PopBufferMergeService<MS: MessageStore> {
    buffer: DashMap<CheetahString /* mergeKey */, ArcMut<PopCheckPointWrapper>>,
    commit_offsets: DashMap<
        CheetahString, /* topic@cid@queueId */
        QueueWithTime<ArcMut<PopCheckPointWrapper>>,
    >,
    serving: AtomicBool,
    counter: AtomicI32,
    scan_times: u64,
    revive_topic: CheetahString,
    queue_lock_manager: QueueLockManager,
    interval: u64,
    minute5: u64,
    count_of_minute1: u64,
    count_of_second1: u64,
    count_of_second30: u64,
    batch_ack_index_list: Vec<u8>,
    master: AtomicBool,
    shutdown: Arc<Notify>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> PopBufferMergeService<MS> {
    pub fn new(
        revive_topic: CheetahString,
        queue_lock_manager: QueueLockManager,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        let interval = 5;
        Self {
            buffer: DashMap::with_capacity(1024 * 16),
            commit_offsets: DashMap::with_capacity(1024),
            serving: AtomicBool::new(true),
            counter: AtomicI32::new(0),
            scan_times: 0,
            revive_topic,
            queue_lock_manager,
            interval,
            minute5: 5 * 60 * 1000,
            count_of_minute1: 60 * 1000 / interval,
            count_of_second1: 1000 / interval,
            count_of_second30: 30 * 1000 / interval,
            batch_ack_index_list: Vec::with_capacity(32),
            master: AtomicBool::new(false),
            shutdown: Arc::new(Notify::new()),
            broker_runtime_inner,
        }
    }
}

impl<MS: MessageStore> PopBufferMergeService<MS> {
    /// Adds a checkpoint to the buffer
    pub async fn add_ck(
        &self,
        point: &PopCheckPoint,
        revive_queue_id: i32,
        revive_queue_offset: i64,
        next_begin_offset: i64,
    ) -> bool {
        // Check if buffer merge is enabled
        let broker_config = self.broker_runtime_inner.broker_config();
        if !broker_config.enable_pop_buffer_merge {
            return false;
        }

        // Check if service is active
        if !self.serving.load(Ordering::Acquire) {
            return false;
        }

        // Check timeout condition
        let now = get_current_millis() as i64;

        if point.get_revive_time() - now < broker_config.pop_ck_stay_buffer_time_out as i64 + 1500 {
            if broker_config.enable_pop_log {
                warn!("[PopBuffer]add ck, timeout, {:?}, {}", point, now);
            }
            return false;
        }

        // Check buffer size
        if self.counter.load(Ordering::Acquire) as i64 > broker_config.pop_ck_max_buffer_size {
            warn!(
                "[PopBuffer]add ck, max size, {:?}, {}",
                point,
                self.counter.load(Ordering::Acquire)
            );
            return false;
        }

        // Create wrapper
        let point_wrapper = ArcMut::new(PopCheckPointWrapper::new(
            revive_queue_id,
            revive_queue_offset,
            Arc::new(point.clone()),
            next_begin_offset,
        ));

        // Check if queue is valid
        if !self.check_queue_ok(&point_wrapper).await {
            return false;
        }

        // Check for merge key conflict
        let merge_key = point_wrapper.get_merge_key();
        if self.buffer.contains_key(merge_key) {
            warn!(
                "[PopBuffer]mergeKey conflict when add ck. ck:{:?}, mergeKey:{}",
                point_wrapper, merge_key
            );
            return false;
        }

        // Add to offset queue
        self.put_offset_queue(point_wrapper.clone()).await;

        if broker_config.enable_pop_log {
            info!("[PopBuffer]add ck, {:?}", point_wrapper);
        }
        // Add to buffer
        self.buffer.insert(merge_key.clone(), point_wrapper);
        self.counter.fetch_add(1, Ordering::AcqRel);

        true
    }

    // Helper methods
    async fn check_queue_ok(&self, point_wrapper: &PopCheckPointWrapper) -> bool {
        let queue = self.commit_offsets.get(point_wrapper.get_lock_key());
        match queue {
            None => true,
            Some(value) => {
                value.get().lock().await.len()
                    < self
                        .broker_runtime_inner
                        .broker_config()
                        .pop_ck_offset_max_queue_size as usize
            }
        }
    }

    pub async fn add_ck_just_offset(
        &self,
        point: PopCheckPoint,
        revive_queue_id: i32,
        revive_queue_offset: i64,
        next_begin_offset: i64,
    ) -> bool {
        // Create a new wrapper with justOffset flag set to true
        let point_wrapper = ArcMut::new(PopCheckPointWrapper::new_with_offset(
            revive_queue_id,
            revive_queue_offset,
            Arc::new(point),
            next_begin_offset,
            true,
        ));

        // Check for merge key conflict in the buffer
        let merge_key = point_wrapper.get_merge_key();
        if self.buffer.contains_key(merge_key) {
            // Log warning about merge key conflict
            warn!(
                "[PopBuffer]mergeKey conflict when add ckJustOffset. ck:{:?}, mergeKey:{}",
                point_wrapper, merge_key
            );
            return false;
        }

        // Put checkpoint to store with condition: !checkQueueOk
        let should_run_in_current = !self.check_queue_ok(&point_wrapper).await;
        self.put_ck_to_store(point_wrapper.as_ref(), should_run_in_current)
            .await;

        // Add to offset queue
        self.put_offset_queue(point_wrapper.clone()).await;
        // Log if enabled
        if self.broker_runtime_inner.broker_config().enable_pop_log {
            info!("[PopBuffer]add ck just offset, {:?}", point_wrapper);
        }
        // Add to buffer and increment counter
        self.buffer.insert(merge_key.clone(), point_wrapper);
        self.counter.fetch_add(1, Ordering::AcqRel);
        true
    }

    pub fn add_ack(&mut self, revive_qid: i32, ack_msg: &dyn AckMessage) -> bool {
        if !self
            .broker_runtime_inner
            .broker_config()
            .enable_pop_buffer_merge
        {
            return false;
        }
        if !self.serving.load(Ordering::Acquire) {
            return false;
        }
        let point_wrapper = match self.buffer.get(&CheetahString::from_string(format!(
            "{}{}{}{}{}{}",
            ack_msg.topic(),
            ack_msg.consumer_group(),
            ack_msg.queue_id(),
            ack_msg.start_offset(),
            ack_msg.pop_time(),
            ack_msg.broker_name()
        ))) {
            Some(wrapper) => wrapper,
            None => {
                if self.broker_runtime_inner.broker_config().enable_pop_log {
                    warn!(
                        "[PopBuffer]add ack fail, rqId={}, no ck, {}",
                        revive_qid, ack_msg
                    );
                }
                return false;
            }
        };
        if point_wrapper.is_just_offset() {
            return false;
        }
        let point = point_wrapper.get_ck();
        let now = get_current_millis();
        if (point.get_revive_time() as u64 - now)
            < self
                .broker_runtime_inner
                .broker_config()
                .pop_ck_stay_buffer_time_out
                + 1500
        {
            if self.broker_runtime_inner.broker_config().enable_pop_log {
                warn!(
                    "[PopBuffer]add ack fail, rqId={}, almost timeout for revive, {:?}, {}, {}",
                    revive_qid,
                    point_wrapper.value(),
                    ack_msg,
                    now
                );
            }
            return false;
        }
        if now - point.pop_time as u64
            > self
                .broker_runtime_inner
                .broker_config()
                .pop_ck_stay_buffer_time_out
                - 1500
        {
            if self.broker_runtime_inner.broker_config().enable_pop_log {
                warn!(
                    "[PopBuffer]add ack fail, rqId={}, timeout for revive, {:?}, {}, {}",
                    revive_qid,
                    point_wrapper.value(),
                    ack_msg,
                    now
                );
            }
            return false;
        }

        if let Some(batch_ack_msg) = ack_msg.as_any().downcast_ref::<BatchAckMsg>() {
            for ack_offset in &batch_ack_msg.ack_offset_list {
                let index_of_ack = point.index_of_ack(*ack_offset);
                if index_of_ack > -1 {
                    Self::mark_bit_cas(point_wrapper.get_bits(), index_of_ack as usize);
                } else {
                    error!(
                        "[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}",
                        revive_qid, ack_msg, point
                    );
                }
            }
        } else {
            let index_of_ack = point.index_of_ack(ack_msg.ack_offset());
            if index_of_ack > -1 {
                Self::mark_bit_cas(point_wrapper.get_bits(), index_of_ack as usize);
            } else {
                error!(
                    "[PopBuffer]Invalid index of ack, reviveQid={}, {}, {}",
                    revive_qid, ack_msg, point
                );
                return true;
            }
        }
        true
    }

    #[inline]
    pub async fn get_latest_offset(&self, lock_key: &CheetahString) -> i64 {
        let queue = self.commit_offsets.get(lock_key);
        if let Some(queue) = queue {
            let queue = queue.get().lock().await;
            if let Some(queue) = queue.back() {
                queue.get_next_begin_offset()
            } else {
                -1
            }
        } else {
            -1
        }
    }

    pub async fn get_latest_offset_full(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
    ) -> i64 {
        self.get_latest_offset(&CheetahString::from_string(KeyBuilder::build_polling_key(
            topic, group, queue_id,
        )))
        .await
    }

    pub fn clear_offset_queue(&self, lock_key: &CheetahString) {
        self.commit_offsets.remove(lock_key);
    }

    fn mark_bit_cas(set_bits: &AtomicI32, index: usize) {
        loop {
            let bits = set_bits.load(Ordering::Relaxed);
            if DataConverter::get_bit(bits, index) {
                break;
            }
            let new_bits = DataConverter::set_bit(bits, index, true);
            if let Ok(value) =
                set_bits.compare_exchange(bits, new_bits, Ordering::Acquire, Ordering::Relaxed)
            {
                if value == bits {
                    break;
                }
            }
        }
    }

    fn is_should_running(&self) -> bool {
        if self
            .broker_runtime_inner
            .broker_config()
            .enable_slave_acting_master
        {
            return true;
        }
        self.master.store(
            self.broker_runtime_inner.broker_config().broker_role != BrokerRole::Slave,
            Ordering::Release,
        );
        self.master.load(Ordering::Acquire)
    }

    async fn scan(&mut self) {
        let start_time = Instant::now();
        let mut count = 0;
        let mut count_ck = 0;
        let pop_ck_stay_buffer_time_out = self
            .broker_runtime_inner
            .broker_config()
            .pop_ck_stay_buffer_time_out as i64;
        let pop_ck_stay_buffer_time = self
            .broker_runtime_inner
            .broker_config()
            .pop_ck_stay_buffer_time as i64;
        let mut remove_keys = HashSet::new();
        for entry in self.buffer.iter() {
            let point_wrapper = entry.value();

            // just process offset(already stored at pull thread), or buffer ck(not stored and ack
            // finish)
            if point_wrapper.is_just_offset() && point_wrapper.is_ck_stored()
                || is_ck_done(point_wrapper)
                || is_ck_done_for_finish(point_wrapper) && point_wrapper.is_ck_stored()
            {
                self.counter.fetch_sub(1, Ordering::AcqRel);
                remove_keys.insert(entry.key().clone());
                continue;
            }
            let point = point_wrapper.get_ck();
            let now = get_current_millis();

            // remove CheckPoint or not
            let mut remove_ck = !self.serving.load(Ordering::Acquire);
            // ck will be timeout
            if (point.get_revive_time() - now as i64) < pop_ck_stay_buffer_time_out {
                remove_ck = true;
            }
            // the time stayed is too long
            if (now as i64 - point.get_revive_time()) > pop_ck_stay_buffer_time {
                remove_ck = true;
            }

            // double check
            if is_ck_done(point_wrapper) {
                continue;
            } else if point_wrapper.is_just_offset() {
                // just offset should be in store.
                if point_wrapper.get_revive_queue_offset() < 0 {
                    self.put_ck_to_store(point_wrapper, false).await;
                    count_ck += 1;
                }
                continue;
            } else if remove_ck {
                // put buffer ak to store
                if point_wrapper.get_revive_queue_offset() < 0 {
                    self.put_ck_to_store(point_wrapper, false).await;
                    count_ck += 1;
                }
                if !point_wrapper.is_ck_stored() {
                    continue;
                }
                if self
                    .broker_runtime_inner
                    .broker_config()
                    .enable_pop_batch_ack
                {
                    // Before removing the CheckPoint from memory, store the messages that have been
                    // Acked in it as Ack messages on disk.
                    for i in 0..point.num {
                        // Traverse each bit of the message bit code table in CheckPoint to check
                        // whether it has been Acked and not stored on disk.
                        if DataConverter::get_bit(
                            point_wrapper.get_bits().load(Ordering::Relaxed),
                            i as usize,
                        ) && !DataConverter::get_bit(
                            point_wrapper.get_to_store_bits().load(Ordering::Relaxed),
                            i as usize,
                        ) {
                            self.batch_ack_index_list.push(i);
                        }
                    }
                    if !self.batch_ack_index_list.is_empty()
                        && self
                            .put_batch_ack_to_store(point_wrapper, &self.batch_ack_index_list)
                            .await
                    {
                        count += self.batch_ack_index_list.len();
                        for index in &self.batch_ack_index_list {
                            Self::mark_bit_cas(point_wrapper.get_to_store_bits(), *index as usize);
                        }
                    }

                    self.batch_ack_index_list.clear();
                } else {
                    for i in 0..point.num {
                        if DataConverter::get_bit(
                            point_wrapper.get_bits().load(Ordering::Relaxed),
                            i as usize,
                        ) && !DataConverter::get_bit(
                            point_wrapper.get_to_store_bits().load(Ordering::Relaxed),
                            i as usize,
                        ) && self.put_ack_to_store(point_wrapper, i).await
                        {
                            count += 1;
                            Self::mark_bit_cas(point_wrapper.get_to_store_bits(), i as usize);
                        }
                    }
                }
                if is_ck_done_for_finish(point_wrapper) && point_wrapper.is_ck_stored() {
                    remove_keys.insert(entry.key().clone());
                    self.counter.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
        // Remove keys
        for key in remove_keys {
            self.buffer.remove(&key);
        }
        //Scan the completed CheckPoints and submit message consumption progress for them.
        let offset_buffer_size = self.scan_commit_offset().await;
        let eclipse = start_time.elapsed().as_millis() as i64;
        if eclipse
            > self
                .broker_runtime_inner
                .broker_config()
                .pop_ck_stay_buffer_time_out as i64
                - 1000
        {
            self.serving.store(false, Ordering::Release);
        } else if self.scan_times % self.count_of_second1 == 0 {
            info!(
                "[PopBuffer]scan, PopBufferEclipse={}, PopBufferToStoreAck={}, \
                 PopBufferToStoreCk={}, PopBufferSize={}, PopBufferOffsetSize={}",
                eclipse,
                count,
                count_ck,
                self.counter.load(Ordering::Acquire),
                offset_buffer_size
            );
        }

        self.scan_times += 1;

        if self.scan_times >= self.count_of_minute1 {
            self.counter
                .store(self.buffer.len() as i32, Ordering::Release);
            self.scan_times = 0;
        }
    }
    fn scan_garbage(&mut self) {
        self.commit_offsets.retain(|key, value| {
            let key_array: Vec<&str> = key.split(PopAckConstants::SPLIT).collect();
            if key_array.is_empty() || key_array.len() != 3 {
                return true;
            }
            let topic = key_array[0];
            let cid = key_array[1];
            if self
                .broker_runtime_inner
                .topic_config_manager()
                .select_topic_config(&topic.into())
                .is_none()
            {
                return false;
            }

            if self
                .broker_runtime_inner
                .subscription_group_manager()
                .contains_subscription_group(&cid.into())
            {
                return false;
            }
            if get_current_millis() - value.get_time() > self.minute5 {
                return false;
            }
            true
        });
    }

    pub async fn get_offset_total_size(&self) -> usize {
        let mut count = 0;
        for entry in self.commit_offsets.iter() {
            let queue = entry.value();
            let guard = queue.get().lock().await;
            count += guard.len();
        }
        count
    }

    pub fn start(this: ArcMut<Self>) {
        tokio::spawn(async move {
            let interval = this.interval * 200 * 5;

            loop {
                select! {
                    _ = this.shutdown.notified() => {
                        break;
                    }
                    _ = async {
                        if !this.is_should_running() {
                            //slave
                            tokio::time::sleep(tokio::time::Duration::from_millis(interval)).await;
                            this.buffer.clear();
                            this.commit_offsets.clear();
                            return;
                        }
                        this.mut_from_ref().scan().await;
                        if this.scan_times % this.count_of_second30 == 0 {
                            this.mut_from_ref().scan_garbage();
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(this.interval)).await;
                        if !this.serving.load(Ordering::Acquire) && this.buffer.is_empty()  && this.get_offset_total_size().await == 0 {
                            this.serving.store(true,Ordering::Release);
                        }
                    } =>{}

                }
            }
            this.serving.store(false, Ordering::Release);
            tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
            if !this.is_should_running() {
                return;
            }
            while !this.buffer.is_empty() || this.get_offset_total_size().await > 0 {
                this.mut_from_ref().scan().await;
            }
        });
    }

    async fn scan_commit_offset(&self) -> usize {
        let mut count = 0;

        for entry in self.commit_offsets.iter() {
            let queue_ = entry.value();
            let mut queue = queue_.get().lock().await;
            loop {
                let point_wrapper = queue.front_mut();
                if point_wrapper.is_none() {
                    break;
                }
                // 1. just offset & stored, not processed by scan
                // 2. ck is buffer(acked)
                // 3. ck is buffer(not all acked), all ak are stored and ck is stored
                let point_wrapper = point_wrapper.unwrap();
                if point_wrapper.is_just_offset() && point_wrapper.is_ck_stored()
                    || is_ck_done(point_wrapper)
                    || is_ck_done_for_finish(point_wrapper) && point_wrapper.is_ck_stored()
                {
                    if self.commit_offset(point_wrapper.as_ref()).await {
                        queue.pop_front();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            let qs = queue.len();
            count += qs;
        }
        count
    }

    async fn commit_offset(&self, point_wrapper: &PopCheckPointWrapper) -> bool {
        if point_wrapper.next_begin_offset < 0 {
            return true;
        }
        let pop_check_point = point_wrapper.get_ck();
        let lock_key = point_wrapper.get_lock_key();
        if !self
            .queue_lock_manager
            .try_lock_with_key(lock_key.clone())
            .await
        {
            return false;
        }
        let consumer_offset_manager = self.broker_runtime_inner.consumer_offset_manager();
        let offset = consumer_offset_manager.query_offset(
            &pop_check_point.cid,
            &pop_check_point.topic,
            pop_check_point.queue_id,
        );
        if point_wrapper.next_begin_offset > offset {
            info!("Commit offset, {}, {}", point_wrapper, offset);
        } else {
            warn!(
                "Commit offset, consumer offset less than store, {}, {}",
                point_wrapper, offset
            )
        }
        consumer_offset_manager.commit_offset(
            "PopBufferMergeService".into(),
            &pop_check_point.cid,
            &pop_check_point.topic,
            pop_check_point.queue_id,
            point_wrapper.next_begin_offset,
        );
        self.queue_lock_manager
            .unlock_with_key(lock_key.clone())
            .await;
        true
    }

    pub async fn add_ck_mock(
        &mut self,
        group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        start_offset: u64,
        invisible_time: u64,
        pop_time: u64,
        revive_queue_id: i32,
        next_begin_offset: u64,
        broker_name: CheetahString,
    ) {
        let ck = PopCheckPoint {
            pop_time: pop_time as i64,
            invisible_time: invisible_time as i64,
            start_offset: start_offset as i64,
            topic,
            cid: group,
            queue_id,
            broker_name: Some(broker_name),
            ..Default::default()
        };

        let point_wrapper = PopCheckPointWrapper::new_with_offset(
            revive_queue_id,
            i64::MAX,
            Arc::new(ck),
            next_begin_offset as i64,
            true,
        );
        point_wrapper.set_ck_stored(true);

        self.put_offset_queue(ArcMut::new(point_wrapper)).await;
    }
    async fn put_offset_queue(&self, point_wrapper: ArcMut<PopCheckPointWrapper>) -> bool {
        let mut queue = self
            .commit_offsets
            .entry(point_wrapper.lock_key.clone())
            .or_insert(QueueWithTime::new());
        queue.set_time(point_wrapper.get_ck().pop_time as u64);
        let mut guard = queue.get().lock().await;
        guard.push_back(point_wrapper);
        true
    }

    async fn put_ck_to_store(&self, point_wrapper: &PopCheckPointWrapper, flag: bool) {
        if point_wrapper.get_revive_queue_offset() >= 0 {
            return;
        }
        let msg_inner = PopMessageProcessor::<MS>::build_ck_msg(
            self.broker_runtime_inner.store_host(),
            point_wrapper.get_ck(),
            point_wrapper.revive_queue_id,
            self.revive_topic.clone(),
        );
        let put_message_result = self
            .broker_runtime_inner
            .mut_from_ref()
            .escape_bridge_mut()
            .put_message_to_specific_queue(msg_inner)
            .await;
        if !matches!(
            put_message_result.put_message_status(),
            PutMessageStatus::PutOk
                | PutMessageStatus::FlushDiskTimeout
                | PutMessageStatus::FlushSlaveTimeout
                | PutMessageStatus::SlaveNotAvailable
        ) {
            return;
        }
        point_wrapper.set_ck_stored(true);
        if put_message_result.remote_put() {
            point_wrapper.set_revive_queue_offset(0);
        } else {
            point_wrapper.set_revive_queue_offset(
                put_message_result
                    .append_message_result()
                    .unwrap()
                    .logics_offset,
            );
        }
    }

    async fn put_batch_ack_to_store(
        &self,
        point_wrapper: &PopCheckPointWrapper,
        index_list: &[u8],
    ) -> bool {
        let point = point_wrapper.get_ck();
        let ack_msg = AckMsg {
            start_offset: point.start_offset,
            consumer_group: point.cid.clone(),
            topic: point.topic.clone(),
            queue_id: point.queue_id,
            pop_time: point.pop_time,
            ..Default::default()
        };
        let mut batch_ack_msg = BatchAckMsg {
            ack_offset_list: Vec::with_capacity(index_list.len()),
            ack_msg,
        };
        for index in index_list {
            batch_ack_msg
                .ack_offset_list
                .push(point.ack_offset_by_index(*index));
        }
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(self.revive_topic.clone());
        msg.set_body(Bytes::from(batch_ack_msg.serialize_json().unwrap()));
        msg.message_ext_inner.queue_id = point_wrapper.revive_queue_id;
        msg.set_tags(CheetahString::from_static_str(
            PopAckConstants::BATCH_ACK_TAG,
        ));
        msg.message_ext_inner.born_timestamp = get_current_millis() as i64;
        msg.message_ext_inner.born_host = self.broker_runtime_inner.store_host();
        msg.message_ext_inner.store_host = self.broker_runtime_inner.store_host();
        msg.set_delay_time_ms(point.get_revive_time() as u64);
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_string(PopMessageProcessor::<MS>::gen_batch_ack_unique_id(
                &batch_ack_msg,
            )),
        );
        msg.properties_string = message_decoder::message_properties_to_string(msg.get_properties());

        let put_message_result = self
            .broker_runtime_inner
            .mut_from_ref()
            .escape_bridge_mut()
            .put_message_to_specific_queue(msg)
            .await;
        matches!(
            put_message_result.put_message_status(),
            PutMessageStatus::PutOk
                | PutMessageStatus::FlushDiskTimeout
                | PutMessageStatus::FlushSlaveTimeout
                | PutMessageStatus::SlaveNotAvailable
        )
    }
    async fn put_ack_to_store(&self, point_wrapper: &PopCheckPointWrapper, index: u8) -> bool {
        let point = point_wrapper.get_ck();
        let ack_msg = AckMsg {
            ack_offset: point.ack_offset_by_index(index),
            start_offset: point.start_offset,
            consumer_group: point.cid.clone(),
            topic: point.topic.clone(),
            queue_id: point.queue_id,
            pop_time: point.pop_time,
            ..Default::default()
        };
        let mut msg = MessageExtBrokerInner::default();
        msg.set_topic(self.revive_topic.clone());
        msg.set_body(Bytes::from(ack_msg.serialize_json().unwrap()));
        msg.message_ext_inner.queue_id = point_wrapper.revive_queue_id;
        msg.set_tags(CheetahString::from_static_str(PopAckConstants::ACK_TAG));
        msg.message_ext_inner.born_timestamp = get_current_millis() as i64;
        msg.message_ext_inner.born_host = self.broker_runtime_inner.store_host();
        msg.message_ext_inner.store_host = self.broker_runtime_inner.store_host();
        msg.set_delay_time_ms(point.get_revive_time() as u64);
        msg.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            CheetahString::from_string(PopMessageProcessor::<MS>::gen_ack_unique_id(&ack_msg)),
        );
        msg.properties_string = message_decoder::message_properties_to_string(msg.get_properties());

        let put_message_result = self
            .broker_runtime_inner
            .mut_from_ref()
            .escape_bridge_mut()
            .put_message_to_specific_queue(msg)
            .await;
        matches!(
            put_message_result.put_message_status(),
            PutMessageStatus::PutOk
                | PutMessageStatus::FlushDiskTimeout
                | PutMessageStatus::FlushSlaveTimeout
                | PutMessageStatus::SlaveNotAvailable
        )
    }
}

fn is_ck_done(point_wrapper: &PopCheckPointWrapper) -> bool {
    let num = point_wrapper.ck.num;
    for i in 0..num {
        if !DataConverter::get_bit(point_wrapper.get_bits().load(Ordering::Relaxed), i as usize) {
            return false;
        }
    }
    true
}

fn is_ck_done_for_finish(point_wrapper: &PopCheckPointWrapper) -> bool {
    let num = point_wrapper.ck.num;
    let bits = point_wrapper.bits.load(Ordering::Acquire)
        ^ point_wrapper.to_store_bits.load(Ordering::Acquire);
    for i in 0..num {
        if !DataConverter::get_bit(bits, i as usize) {
            return false;
        }
    }
    true
}
pub struct QueueWithTime<T> {
    queue: Arc<tokio::sync::Mutex<VecDeque<T>>>,
    time: u64,
}

impl<T> QueueWithTime<T> {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            time: get_current_millis(),
        }
    }

    pub fn set_time(&mut self, pop_time: u64) {
        self.time = pop_time;
    }

    pub fn get_time(&self) -> u64 {
        self.time
    }

    pub fn get(&self) -> &Arc<tokio::sync::Mutex<VecDeque<T>>> {
        &self.queue
    }
}

#[derive(Debug)]
pub struct PopCheckPointWrapper {
    revive_queue_id: i32,
    // -1: not stored, >=0: stored, Long.MAX: storing.
    revive_queue_offset: AtomicI32,
    ck: Arc<PopCheckPoint>,
    // bit for concurrent
    bits: AtomicI32,
    // bit for stored buffer ak
    to_store_bits: AtomicI32,
    next_begin_offset: i64,
    lock_key: CheetahString,
    merge_key: CheetahString,
    just_offset: bool,
    ck_stored: AtomicBool,
}

impl std::fmt::Display for PopCheckPointWrapper {
    /// Formats the PopCheckPointWrapper for display
    ///
    /// Outputs the wrapper's fields in a compact format showing:
    /// - revive queue ID (rq)
    /// - revive queue offset (rqo)
    /// - checkpoint reference (ck)
    /// - bits for tracking acknowledged messages
    /// - store bits for tracking persisted acknowledgments
    /// - next begin offset (nbo)
    /// - checkpoint stored status (cks)
    /// - just offset flag (jo)
    ///
    /// # Arguments
    /// * `f` - The formatter to write to
    ///
    /// # Returns
    /// A formatting result indicating success or failure
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CkWrap{{rq={}, rqo={}, ck={:?}, bits={}, sBits={}, nbo={}, cks={}, jo={}}}",
            self.revive_queue_id,
            self.revive_queue_offset
                .load(std::sync::atomic::Ordering::Relaxed),
            self.ck,
            self.bits.load(std::sync::atomic::Ordering::Relaxed),
            self.to_store_bits
                .load(std::sync::atomic::Ordering::Relaxed),
            self.next_begin_offset,
            self.ck_stored.load(std::sync::atomic::Ordering::Relaxed),
            self.just_offset
        )
    }
}

impl PopCheckPointWrapper {
    pub fn new(
        revive_queue_id: i32,
        revive_queue_offset: i64,
        ck: Arc<PopCheckPoint>,
        next_begin_offset: i64,
    ) -> Self {
        let lock_key = format!(
            "{}{}{}{}{}",
            ck.topic,
            PopAckConstants::SPLIT,
            ck.cid,
            PopAckConstants::SPLIT,
            ck.queue_id
        );
        let merge_key = format!(
            "{}{}{}{}{}{}",
            ck.topic,
            ck.cid,
            ck.queue_id,
            ck.start_offset,
            ck.pop_time,
            ck.broker_name.clone().unwrap_or_default()
        );
        Self {
            revive_queue_id,
            revive_queue_offset: AtomicI32::new(revive_queue_offset as i32),
            ck,
            bits: AtomicI32::new(0),
            to_store_bits: AtomicI32::new(0),
            next_begin_offset,
            lock_key: CheetahString::from(lock_key),
            merge_key: CheetahString::from(merge_key),
            just_offset: false,
            ck_stored: AtomicBool::new(false),
        }
    }

    pub fn new_with_offset(
        revive_queue_id: i32,
        revive_queue_offset: i64,
        ck: Arc<PopCheckPoint>,
        next_begin_offset: i64,
        just_offset: bool,
    ) -> Self {
        let lock_key = format!(
            "{}{}{}{}{}",
            ck.topic,
            PopAckConstants::SPLIT,
            ck.cid,
            PopAckConstants::SPLIT,
            ck.queue_id
        );
        let merge_key = format!(
            "{}{}{}{}{}{}",
            ck.topic,
            ck.cid,
            ck.queue_id,
            ck.start_offset,
            ck.pop_time,
            ck.broker_name.clone().unwrap_or_default()
        );
        Self {
            revive_queue_id,
            revive_queue_offset: AtomicI32::new(revive_queue_offset as i32),
            ck,
            bits: AtomicI32::new(0),
            to_store_bits: AtomicI32::new(0),
            next_begin_offset,
            lock_key: CheetahString::from(lock_key),
            merge_key: CheetahString::from(merge_key),
            just_offset,
            ck_stored: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn get_revive_queue_id(&self) -> i32 {
        self.revive_queue_id
    }

    #[inline]
    pub fn get_revive_queue_offset(&self) -> i64 {
        self.revive_queue_offset.load(Ordering::SeqCst) as i64
    }

    #[inline]
    pub fn is_ck_stored(&self) -> bool {
        self.ck_stored.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_revive_queue_offset(&self, revive_queue_offset: i64) {
        self.revive_queue_offset
            .store(revive_queue_offset as i32, Ordering::SeqCst);
    }

    #[inline]
    pub fn get_ck(&self) -> &Arc<PopCheckPoint> {
        &self.ck
    }

    #[inline]
    pub fn get_bits(&self) -> &AtomicI32 {
        &self.bits
    }

    #[inline]
    pub fn get_to_store_bits(&self) -> &AtomicI32 {
        &self.to_store_bits
    }

    #[inline]
    pub fn get_next_begin_offset(&self) -> i64 {
        self.next_begin_offset
    }

    #[inline]
    pub fn get_lock_key(&self) -> &CheetahString {
        &self.lock_key
    }

    #[inline]
    pub fn get_merge_key(&self) -> &CheetahString {
        &self.merge_key
    }

    #[inline]
    pub fn is_just_offset(&self) -> bool {
        self.just_offset
    }

    #[inline]
    pub fn set_ck_stored(&self, ck_stored: bool) {
        self.ck_stored.store(ck_stored, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_store::pop::pop_check_point::PopCheckPoint;

    use super::*;

    #[test]
    fn new_creates_instance_correctly() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck.clone(), 200);
        assert_eq!(wrapper.get_revive_queue_id(), 1);
        assert_eq!(wrapper.get_revive_queue_offset(), 100);
        assert_eq!(wrapper.get_next_begin_offset(), 200);
        assert_eq!(wrapper.get_ck(), &ck);
    }

    #[test]
    fn new_with_offset_creates_instance_correctly() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new_with_offset(1, 100, ck.clone(), 200, true);
        assert_eq!(wrapper.get_revive_queue_id(), 1);
        assert_eq!(wrapper.get_revive_queue_offset(), 100);
        assert_eq!(wrapper.get_next_begin_offset(), 200);
        assert_eq!(wrapper.get_ck(), &ck);
        assert!(wrapper.is_just_offset());
    }

    #[test]
    fn set_revive_queue_offset_updates_offset() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck, 200);
        wrapper.set_revive_queue_offset(300);
        assert_eq!(wrapper.get_revive_queue_offset(), 300);
    }

    #[test]
    fn set_ck_stored_updates_flag() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck, 200);
        wrapper.set_ck_stored(true);
        assert!(wrapper.is_ck_stored());
    }

    #[test]
    fn display_formats_correctly() {
        let ck = Arc::new(PopCheckPoint::default());
        let wrapper = PopCheckPointWrapper::new(1, 100, ck, 200);
        let display = format!("{}", wrapper);
        assert!(display.contains("CkWrap{rq=1, rqo=100"));
    }

    #[tokio::test]
    async fn queue_with_time_initializes_correctly() {
        let queue_with_time: QueueWithTime<i32> = QueueWithTime::new();
        let guard = queue_with_time.get().lock().await;
        assert!(guard.is_empty());
        assert!(queue_with_time.get_time() > 0);
    }

    #[test]
    fn set_time_updates_time() {
        let mut queue_with_time: QueueWithTime<i32> = QueueWithTime::new();
        queue_with_time.set_time(123456789);
        assert_eq!(queue_with_time.get_time(), 123456789);
    }

    #[tokio::test]
    async fn get_queue_mut_returns_mutable_reference() {
        let queue_with_time: QueueWithTime<i32> = QueueWithTime::new();
        let mut guard = queue_with_time.get().lock().await;
        guard.push_back(1);
        assert_eq!(guard.len(), 1);
    }
}
