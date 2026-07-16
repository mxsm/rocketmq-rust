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

#![allow(unused_variables)]

use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use futures_util::future::join_all;
use rocketmq_common::common::attribute::cq_type::CQType;
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::queue_type_utils::QueueTypeUtils;
use rocketmq_rust::ArcMut;
use rocketmq_store_local::consume_queue::root::clamp_consume_queue_offset;
use rocketmq_store_local::consume_queue::root::find_or_create_consume_queue as drive_find_or_create_consume_queue;
use rocketmq_store_local::consume_queue::root::ConsumeQueueStoreRoot;
use tokio::sync::Semaphore;
use tracing::error;
use tracing::info;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::message_store::MessageStore;
use crate::config::message_store_config::MessageStoreConfig;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::queue::batch_consume_queue::BatchConsumeQueue;
use crate::queue::consume_queue::ConsumeQueueTrait;
use crate::queue::consume_queue_store::ConsumeQueueStoreTrait;
use crate::queue::queue_offset_operator::QueueOffsetOperator;
use crate::queue::single_consume_queue::ConsumeQueue;
use crate::queue::single_consume_queue::CQ_STORE_UNIT_SIZE;
use crate::queue::ArcConsumeQueue;
use crate::queue::ConsumeQueueTable;
use crate::queue::CqUnit;
use crate::store_path_config_helper::get_store_path_batch_consume_queue;
use crate::store_path_config_helper::get_store_path_consume_queue;

#[derive(Clone)]
pub struct ConsumeQueueStore {
    inner: ConsumeQueueStoreRoot<ArcMut<Inner>>,
}

struct ConsumeQueueRecoveryResult {
    topic: CheetahString,
    queue_id: i32,
    cq_type: CQType,
    elapsed_ms: u128,
    mapped_file_count_before: usize,
    mapped_file_count_after: usize,
    min_logic_offset_before: i64,
    min_logic_offset_after: i64,
    max_logic_offset_before: i64,
    max_logic_offset_after: i64,
    max_physic_offset_before: i64,
    max_physic_offset_after: i64,
    success: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConsumeQueueRecoveryFailure {
    pub topic: CheetahString,
    pub queue_id: i32,
    pub cq_type: CQType,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConsumeQueueRecoverySummary {
    pub queue_count: usize,
    pub success_count: usize,
    pub failure_count: usize,
    pub total_elapsed_ms: u128,
    pub failures: Vec<ConsumeQueueRecoveryFailure>,
}

impl ConsumeQueueRecoverySummary {
    fn from_results(queue_count: usize, total_elapsed_ms: u128, results: &[ConsumeQueueRecoveryResult]) -> Self {
        let failures = results
            .iter()
            .filter(|result| !result.success)
            .map(|result| ConsumeQueueRecoveryFailure {
                topic: result.topic.clone(),
                queue_id: result.queue_id,
                cq_type: result.cq_type,
                error: result.error.clone().unwrap_or_else(|| "unknown".to_string()),
            })
            .collect::<Vec<_>>();
        Self {
            queue_count,
            success_count: results.len().saturating_sub(failures.len()),
            failure_count: failures.len(),
            total_elapsed_ms,
            failures,
        }
    }

    pub fn success(queue_count: usize, total_elapsed_ms: u128) -> Self {
        Self {
            queue_count,
            success_count: queue_count,
            failure_count: 0,
            total_elapsed_ms,
            failures: Vec::new(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.failure_count == 0
    }

    pub fn failure_description(&self) -> String {
        if self.failures.is_empty() {
            return "none".to_string();
        }
        self.failures
            .iter()
            .map(|failure| {
                format!(
                    "{}:{}:{:?}:{}",
                    failure.topic, failure.queue_id, failure.cq_type, failure.error
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

struct Inner {
    pub(crate) message_store: Option<ArcMut<LocalFileMessageStore>>,
    pub(crate) message_store_config: Arc<MessageStoreConfig>,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) queue_offset_operator: QueueOffsetOperator,
    pub(crate) consume_queue_table: Arc<ConsumeQueueTable>,
}

impl Inner {
    fn put_message_position_info_wrapper(&self, consume_queue: &mut dyn ConsumeQueueTrait, request: &DispatchRequest) {
        consume_queue.put_message_position_info_wrapper(request)
    }
}

impl ConsumeQueueStore {
    fn snapshot_consume_queues(&self) -> Vec<ArcConsumeQueue> {
        self.inner
            .consume_queue_table
            .lock()
            .values()
            .flat_map(|queues| queues.values().cloned())
            .collect()
    }

    async fn recover_one_consume_queue(consume_queue: ArcConsumeQueue) -> ConsumeQueueRecoveryResult {
        let topic = consume_queue.get_topic().clone();
        let queue_id = consume_queue.get_queue_id();
        let cq_type = consume_queue.get_cq_type();
        match crate::runtime::spawn_io("local-file-cq-recover", move || {
            Self::recover_one_consume_queue_blocking(consume_queue)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => ConsumeQueueRecoveryResult {
                topic,
                queue_id,
                cq_type,
                elapsed_ms: 0,
                mapped_file_count_before: 0,
                mapped_file_count_after: 0,
                min_logic_offset_before: 0,
                min_logic_offset_after: 0,
                max_logic_offset_before: 0,
                max_logic_offset_after: 0,
                max_physic_offset_before: 0,
                max_physic_offset_after: 0,
                success: false,
                error: Some(error.to_string()),
            },
        }
    }

    fn recover_one_consume_queue_blocking(mut consume_queue: ArcConsumeQueue) -> ConsumeQueueRecoveryResult {
        let topic = consume_queue.get_topic().clone();
        let queue_id = consume_queue.get_queue_id();
        let cq_type = consume_queue.get_cq_type();
        let mapped_file_count_before = consume_queue.get_mapped_file_count();
        let min_logic_offset_before = consume_queue.get_min_logic_offset();
        let max_logic_offset_before = consume_queue.get_max_offset_in_queue();
        let max_physic_offset_before = consume_queue.get_max_physic_offset();

        let started = Instant::now();
        let recover_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consume_queue.recover()));
        let elapsed_ms = started.elapsed().as_millis();

        let mapped_file_count_after = consume_queue.get_mapped_file_count();
        let min_logic_offset_after = consume_queue.get_min_logic_offset();
        let max_logic_offset_after = consume_queue.get_max_offset_in_queue();
        let max_physic_offset_after = consume_queue.get_max_physic_offset();
        let error = recover_result.err().map(Self::panic_payload_to_string);

        ConsumeQueueRecoveryResult {
            topic,
            queue_id,
            cq_type,
            elapsed_ms,
            mapped_file_count_before,
            mapped_file_count_after,
            min_logic_offset_before,
            min_logic_offset_after,
            max_logic_offset_before,
            max_logic_offset_after,
            max_physic_offset_before,
            max_physic_offset_after,
            success: error.is_none(),
            error,
        }
    }

    fn panic_payload_to_string(payload: Box<dyn Any + Send>) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "panic while recovering consume queue".to_string()
        }
    }

    pub async fn recover_concurrently_with_summary(&self, parallelism: usize) -> ConsumeQueueRecoverySummary {
        let total_started = Instant::now();
        let queues = self.snapshot_consume_queues();
        if queues.is_empty() {
            info!("recover local file consume queue concurrently skipped, no consume queues loaded");
            return ConsumeQueueRecoverySummary::success(0, total_started.elapsed().as_millis());
        }

        let queue_count = queues.len();
        let parallelism = parallelism.max(1).min(queue_count);
        info!(
            "recover local file consume queue concurrently start, queues={}, parallelism={}",
            queue_count, parallelism
        );
        let semaphore = Arc::new(Semaphore::new(parallelism));
        let futures = queues.into_iter().map(|consume_queue| {
            let semaphore = semaphore.clone();
            async move {
                let topic = consume_queue.get_topic().clone();
                let queue_id = consume_queue.get_queue_id();
                let cq_type = consume_queue.get_cq_type();
                match semaphore.acquire_owned().await {
                    Ok(_permit) => Self::recover_one_consume_queue(consume_queue).await,
                    Err(error) => ConsumeQueueRecoveryResult {
                        topic,
                        queue_id,
                        cq_type,
                        elapsed_ms: 0,
                        mapped_file_count_before: 0,
                        mapped_file_count_after: 0,
                        min_logic_offset_before: 0,
                        min_logic_offset_after: 0,
                        max_logic_offset_before: 0,
                        max_logic_offset_after: 0,
                        max_physic_offset_before: 0,
                        max_physic_offset_after: 0,
                        success: false,
                        error: Some(format!("failed to acquire recovery permit: {error}")),
                    },
                }
            }
        });

        let results = join_all(futures).await;
        Self::log_recovery_results(&results);
        let summary =
            ConsumeQueueRecoverySummary::from_results(queue_count, total_started.elapsed().as_millis(), &results);
        if summary.is_success() {
            info!(
                "recover local file consume queue concurrently summary OK, queues={}, success={}, cost={}ms",
                summary.queue_count, summary.success_count, summary.total_elapsed_ms
            );
        } else {
            error!(
                "recover local file consume queue concurrently summary failed, queues={}, success={}, failed={}, \
                 cost={}ms, failures={}",
                summary.queue_count,
                summary.success_count,
                summary.failure_count,
                summary.total_elapsed_ms,
                summary.failure_description()
            );
        }
        summary
    }

    fn log_recovery_results(results: &[ConsumeQueueRecoveryResult]) {
        for result in results {
            if result.success {
                info!(
                    "recover local file consume queue concurrently OK, topic={}, queueId={}, cqType={:?}, cost={}ms, \
                     mappedFiles={} -> {}, minLogicOffset={} -> {}, maxLogicOffset={} -> {}, maxPhysicOffset={} -> {}",
                    result.topic,
                    result.queue_id,
                    result.cq_type,
                    result.elapsed_ms,
                    result.mapped_file_count_before,
                    result.mapped_file_count_after,
                    result.min_logic_offset_before,
                    result.min_logic_offset_after,
                    result.max_logic_offset_before,
                    result.max_logic_offset_after,
                    result.max_physic_offset_before,
                    result.max_physic_offset_after
                );
            } else {
                error!(
                    "recover local file consume queue concurrently failed, topic={}, queueId={}, cqType={:?}, \
                     cost={}ms, mappedFiles={} -> {}, minLogicOffset={} -> {}, maxLogicOffset={} -> {}, \
                     maxPhysicOffset={} -> {}, error={}",
                    result.topic,
                    result.queue_id,
                    result.cq_type,
                    result.elapsed_ms,
                    result.mapped_file_count_before,
                    result.mapped_file_count_after,
                    result.min_logic_offset_before,
                    result.min_logic_offset_after,
                    result.max_logic_offset_before,
                    result.max_logic_offset_after,
                    result.max_physic_offset_before,
                    result.max_physic_offset_after,
                    result.error.as_deref().unwrap_or("unknown")
                );
            }
        }
    }

    pub fn clean_expired_sync(&self, min_commit_log_offset: i64) {
        // Collect queues to remove
        let mut queues_to_destroy = Vec::new();
        let mut topics_to_remove = Vec::new();

        {
            let mut consume_queue_table = self.inner.consume_queue_table.lock();
            let topics: Vec<CheetahString> = consume_queue_table.keys().cloned().collect();

            for topic in topics {
                // Skip system topics
                if TopicValidator::is_system_topic(&topic) {
                    continue;
                }

                if let Some(queue_table) = consume_queue_table.get_mut(&topic) {
                    let queue_ids: Vec<i32> = queue_table.keys().cloned().collect();
                    let mut queues_to_remove = Vec::new();

                    for queue_id in queue_ids {
                        if let Some(consume_queue) = queue_table.get(&queue_id) {
                            let max_cl_offset_in_queue = consume_queue.get_max_physic_offset();
                            let message_total_in_queue = consume_queue.get_message_total_in_queue();
                            let queue_trimmed_to_empty =
                                message_total_in_queue <= 0 && consume_queue.get_min_offset_in_queue() > 0;

                            if max_cl_offset_in_queue == -1 {
                                tracing::warn!(
                                    "maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} \
                                     minLogicOffset={}.",
                                    consume_queue.get_topic(),
                                    consume_queue.get_queue_id(),
                                    consume_queue.get_max_physic_offset(),
                                    consume_queue.get_min_logic_offset()
                                );
                            } else if max_cl_offset_in_queue < min_commit_log_offset || queue_trimmed_to_empty {
                                tracing::info!(
                                    "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: \
                                     {} maxCLOffsetInConsumeQueue: {} messageTotalInQueue: {} minOffsetInQueue: {}",
                                    topic,
                                    queue_id,
                                    min_commit_log_offset,
                                    max_cl_offset_in_queue,
                                    message_total_in_queue,
                                    consume_queue.get_min_offset_in_queue()
                                );

                                queues_to_remove.push(queue_id);
                            }
                        }
                    }

                    // Remove expired queues and collect for destruction
                    for queue_id in queues_to_remove {
                        if let Some(consume_queue) = queue_table.remove(&queue_id) {
                            queues_to_destroy.push((topic.clone(), queue_id, consume_queue));
                        }
                    }

                    // If all queues removed, mark topic for removal
                    if queue_table.is_empty() {
                        topics_to_remove.push(topic.clone());
                    }
                }
            }

            // Remove empty topics
            for topic in &topics_to_remove {
                consume_queue_table.remove(topic);
                tracing::info!("cleanExpiredConsumerQueue: {},topic destroyed", topic);
            }
        }

        // Now destroy queues and remove from offset table (outside the lock)
        for (topic, queue_id, mut consume_queue) in queues_to_destroy {
            self.inner.queue_offset_operator.remove(&topic, queue_id);
            consume_queue.destroy();
        }
    }

    fn find_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<ArcConsumeQueue> {
        let table = self.inner.consume_queue_table.lock();
        let queue_table = table.get(topic)?;
        queue_table.get(&queue_id).cloned()
    }

    fn encode_cq_unit(cq_unit: &CqUnit) -> Bytes {
        if !cq_unit.native_buffer.is_empty() {
            return Bytes::copy_from_slice(&cq_unit.native_buffer);
        }
        let mut bytes = BytesMut::with_capacity(CQ_STORE_UNIT_SIZE as usize);
        bytes.put_i64(cq_unit.pos);
        bytes.put_i32(cq_unit.size);
        bytes.put_i64(cq_unit.tags_code);
        bytes.freeze()
    }

    #[inline]
    pub fn new(message_store_config: Arc<MessageStoreConfig>, broker_config: Arc<BrokerConfig>) -> Self {
        Self {
            inner: ConsumeQueueStoreRoot::new(ArcMut::new(Inner {
                message_store: None,
                message_store_config,
                broker_config,
                queue_offset_operator: Default::default(),
                consume_queue_table: Arc::new(Default::default()),
            })),
        }
    }

    pub fn set_message_store(&mut self, message_store: ArcMut<LocalFileMessageStore>) {
        self.inner.message_store = Some(message_store);
    }

    pub fn check_self(&self, consume_queue: &dyn ConsumeQueueTrait) {
        let life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        life_cycle.check_self();
    }
}

#[allow(unused_variables)]
impl ConsumeQueueStoreTrait for ConsumeQueueStore {
    fn start(&self) {
        //nothing to do
        info!("consume queue store start");
    }

    fn load(&mut self) -> bool {
        self.load_consume_queues(
            get_store_path_consume_queue(self.inner.message_store_config.store_path_root_dir.as_str()).as_str(),
            CQType::SimpleCQ,
        ) & self.load_consume_queues(
            get_store_path_batch_consume_queue(self.inner.message_store_config.store_path_root_dir.as_str()).as_str(),
            CQType::BatchCQ,
        )
    }

    fn load_after_destroy(&self) -> bool {
        true
    }

    async fn recover(&self) {
        let mut mutex = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in mutex.values_mut() {
            for consume_queue in consume_queue_table.values() {
                let queue_id = consume_queue.get_queue_id();
                let topic = consume_queue.get_topic();
                let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
                file_queue_life_cycle.recover();
            }
        }
    }

    async fn recover_concurrently(&self) -> bool {
        self.recover_concurrently_with_parallelism(
            self.inner
                .message_store_config
                .effective_local_file_consume_queue_recovery_parallelism(),
        )
        .await
    }

    async fn recover_concurrently_with_parallelism(&self, parallelism: usize) -> bool {
        self.recover_concurrently_with_summary(parallelism).await.is_success()
    }

    fn shutdown(&self) -> bool {
        true
    }

    fn destroy(&self) {
        let mutex = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in mutex.values() {
            for consume_queue in consume_queue_table.values() {
                let queue_id = consume_queue.get_queue_id();
                let topic = consume_queue.get_topic();
                let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
                file_queue_life_cycle.destroy();
            }
        }
    }

    fn destroy_queue(&self, consume_queue: &dyn ConsumeQueueTrait) {
        let mut file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.destroy();
    }

    fn flush(&self, consume_queue: &dyn ConsumeQueueTrait, flush_least_pages: i32) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.flush(flush_least_pages)
    }

    async fn clean_expired(&self, min_commit_log_offset: i64) {
        self.clean_expired_sync(min_commit_log_offset);
    }

    fn check_self(&self) {
        let consume_queue_table = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in consume_queue_table.values() {
            for consume_queue in consume_queue_table.values() {
                let consume_queue = &**consume_queue.as_ref();
                ConsumeQueueStore::check_self(self, consume_queue);
            }
        }
    }

    fn delete_expired_file(&self, consume_queue: &dyn ConsumeQueueTrait, min_commit_log_pos: i64) -> i32 {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.delete_expired_file(min_commit_log_pos)
    }

    fn is_first_file_available(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.is_first_file_available()
    }

    fn is_first_file_exist(&self, consume_queue: &dyn ConsumeQueueTrait) -> bool {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.is_first_file_exist()
    }

    fn roll_next_file(&self, consume_queue: &dyn ConsumeQueueTrait, offset: i64) -> i64 {
        let file_queue_life_cycle = self.get_life_cycle(consume_queue.get_topic(), consume_queue.get_queue_id());
        file_queue_life_cycle.roll_next_file(offset)
    }

    fn truncate_dirty(&self, offset_to_truncate: i64) {
        let cloned = self.inner.consume_queue_table.lock().clone();
        for consume_queue_table in cloned.values() {
            for logic in consume_queue_table.values() {
                let topic = logic.get_topic();
                let queue_id = logic.get_queue_id();
                self.truncate_dirty_logic_files(topic, queue_id, offset_to_truncate);
            }
        }
    }

    fn put_message_position_info_wrapper_with_cq(
        &self,
        consume_queue: &mut dyn ConsumeQueueTrait,
        request: &DispatchRequest,
    ) {
        self.inner.put_message_position_info_wrapper(consume_queue, request);
    }

    fn put_message_position_info_wrapper(&self, request: &DispatchRequest) {
        let mut cq = self.find_or_create_consume_queue(request.topic.as_ref(), request.queue_id);
        self.put_message_position_info_wrapper_with_cq(cq.as_mut().as_mut(), request);
    }

    async fn range_query(&self, topic: &CheetahString, queue_id: i32, start_index: i64, num: i32) -> Vec<Bytes> {
        let Some(consume_queue) = self.find_consume_queue(topic, queue_id) else {
            return Vec::new();
        };
        let Some(iter) = consume_queue.iterate_from_with_count(start_index, num) else {
            return Vec::new();
        };

        iter.take(num.max(0) as usize)
            .map(|cq_unit| Self::encode_cq_unit(&cq_unit))
            .collect()
    }

    async fn get(&self, topic: &CheetahString, queue_id: i32, start_index: i64) -> Bytes {
        self.find_consume_queue(topic, queue_id)
            .and_then(|consume_queue| consume_queue.get(start_index))
            .map(|cq_unit| Self::encode_cq_unit(&cq_unit))
            .unwrap_or_default()
    }

    fn get_consume_queue_table(&self) -> Arc<ConsumeQueueTable> {
        self.inner.consume_queue_table.clone()
    }

    fn assign_queue_offset(&self, msg: &mut MessageExtBrokerInner) {
        let consume_queue = self.find_or_create_consume_queue(msg.get_topic(), msg.queue_id());
        consume_queue.assign_queue_offset(&self.inner.queue_offset_operator, msg);
    }

    fn increase_queue_offset(&self, msg: &MessageExtBrokerInner, message_num: i16) {
        let consume_queue = self.find_or_create_consume_queue(msg.get_topic(), msg.queue_id());
        consume_queue.increase_queue_offset(&self.inner.queue_offset_operator, msg, message_num);
    }

    fn increase_lmq_offset(&self, queue_key: &str, message_num: i16) {
        let queue_key = CheetahString::from(queue_key);
        self.inner
            .queue_offset_operator
            .increase_queue_offset(queue_key.clone(), message_num);
        self.inner
            .queue_offset_operator
            .increase_lmq_offset(&queue_key, message_num);
    }

    fn get_lmq_queue_offset(&self, queue_key: &str) -> i64 {
        let queue_key = CheetahString::from(queue_key);
        let lmq_offset = self.inner.queue_offset_operator.get_lmq_offset(&queue_key);
        if lmq_offset > 0 {
            lmq_offset
        } else {
            self.inner.queue_offset_operator.current_queue_offset(&queue_key)
        }
    }

    fn get_lmq_num(&self) -> i32 {
        self.inner.queue_offset_operator.get_lmq_num()
    }

    fn is_lmq_exist(&self, lmq_topic: &str) -> bool {
        self.inner.queue_offset_operator.is_lmq_exist(lmq_topic)
    }

    fn recover_offset_table(&mut self, min_phy_offset: i64) {
        let mut cq_offset_table = HashMap::with_capacity(1024);
        let mut bcq_offset_table = HashMap::with_capacity(1024);
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter_mut() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let key = CheetahString::from_string(format!(
                    "{}-{}",
                    consume_queue.get_topic(),
                    consume_queue.get_queue_id()
                ));
                let max_offset_in_queue = consume_queue.get_max_offset_in_queue();
                if consume_queue.get_cq_type() == CQType::SimpleCQ {
                    cq_offset_table.insert(key, max_offset_in_queue);
                } else {
                    bcq_offset_table.insert(key, max_offset_in_queue);
                }
                self.correct_min_offset(&***consume_queue, min_phy_offset)
            }
        }
        if self.inner.message_store_config.duplication_enable || self.inner.broker_config.enable_controller_mode {
            // In duplication/controller mode, use LMQ offset table
            self.inner
                .queue_offset_operator
                .set_lmq_topic_queue_table(cq_offset_table);
        } else {
            self.set_topic_queue_table(cq_offset_table);
            self.set_batch_topic_queue_table(bcq_offset_table);
        }
    }

    fn set_topic_queue_table(&mut self, topic_queue_table: HashMap<CheetahString, i64>) {
        self.inner
            .queue_offset_operator
            .set_topic_queue_table(topic_queue_table.clone());
        self.inner
            .queue_offset_operator
            .set_lmq_topic_queue_table(topic_queue_table);
    }

    fn remove_topic_queue_table(&mut self, topic: &CheetahString, queue_id: i32) {
        self.inner.queue_offset_operator.remove(topic, queue_id);
    }

    fn get_topic_queue_table(&self) -> HashMap<CheetahString, i64> {
        self.inner.queue_offset_operator.get_topic_queue_table()
    }

    fn get_max_phy_offset_in_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        let mut max_physic_offset = -1i64;
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let max_physic_offset_in_consume_queue = consume_queue.get_max_physic_offset();
                if max_physic_offset_in_consume_queue > max_physic_offset {
                    max_physic_offset = max_physic_offset_in_consume_queue;
                }
            }
        }
        Some(max_physic_offset)
    }

    fn get_max_offset(&self, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        Some(
            self.inner
                .queue_offset_operator
                .current_queue_offset(&format!("{topic}-{queue_id}").into()),
        )
    }

    fn get_max_phy_offset_in_consume_queue_global(&self) -> i64 {
        let mut max_physic_offset = -1i64;
        for (topic, consume_queue_table) in self.inner.consume_queue_table.lock().iter() {
            for (queue_id, consume_queue) in consume_queue_table.iter() {
                let max_physic_offset_in_consume_queue = consume_queue.get_max_physic_offset();
                if max_physic_offset_in_consume_queue > max_physic_offset {
                    max_physic_offset = max_physic_offset_in_consume_queue;
                }
            }
        }
        max_physic_offset
    }

    fn get_min_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let queue = self.find_or_create_consume_queue(topic, queue_id);
        queue.get_min_offset_in_queue()
    }

    fn get_max_offset_in_queue(&self, topic: &CheetahString, queue_id: i32) -> i64 {
        let queue = self.find_or_create_consume_queue(topic, queue_id);
        queue.get_max_offset_in_queue()
    }

    fn get_offset_in_queue_by_time(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        timestamp: i64,
        boundary_type: BoundaryType,
    ) -> i64 {
        let logic = self.find_or_create_consume_queue(topic, queue_id);
        let result_offset = logic.get_offset_in_queue_by_time_with_boundary(timestamp, boundary_type);
        clamp_consume_queue_offset(
            result_offset,
            logic.get_min_offset_in_queue(),
            logic.get_max_offset_in_queue(),
        )
    }

    fn find_or_create_consume_queue(&self, topic: &CheetahString, queue_id: i32) -> ArcConsumeQueue {
        drive_find_or_create_consume_queue(
            || {
                self.inner
                    .consume_queue_table
                    .lock()
                    .get(topic)
                    .and_then(|topic_map| topic_map.get(&queue_id).cloned())
            },
            || {
                let message_store = self
                    .inner
                    .message_store
                    .as_ref()
                    .expect("MessageStore must be set before creating consume queues");
                let topic_config = message_store.get_topic_config(topic);
                let cq_type = QueueTypeUtils::get_cq_type_arc_mut(topic_config.as_ref());
                let new_queue: ArcConsumeQueue = match cq_type {
                    CQType::SimpleCQ | CQType::RocksDBCQ => {
                        let consume_queue = ConsumeQueue::new(
                            topic.clone(),
                            queue_id,
                            CheetahString::from_string(get_store_path_consume_queue(
                                self.inner.message_store_config.store_path_root_dir.as_str(),
                            )),
                            self.inner.message_store_config.get_mapped_file_size_consume_queue(),
                            self.inner.message_store.clone().unwrap(),
                        );
                        ArcMut::new(Box::new(consume_queue))
                    }
                    CQType::BatchCQ => {
                        let consume_queue = BatchConsumeQueue::new(
                            topic.clone(),
                            queue_id,
                            CheetahString::from_string(get_store_path_batch_consume_queue(
                                self.inner.message_store_config.store_path_root_dir.as_str(),
                            )),
                            self.inner.message_store_config.mapper_file_size_batch_consume_queue,
                            None,
                            self.inner.message_store_config.clone(),
                        );
                        ArcMut::new(Box::new(consume_queue))
                    }
                };
                new_queue
            },
            |new_queue| {
                self.inner
                    .consume_queue_table
                    .lock()
                    .entry(topic.clone())
                    .or_default()
                    .entry(queue_id)
                    .or_insert(new_queue)
                    .clone()
            },
        )
    }

    fn find_consume_queue_map(&self, topic: &CheetahString) -> Option<HashMap<i32, ArcConsumeQueue>> {
        self.inner.consume_queue_table.lock().get(topic).cloned()
    }

    fn get_total_size(&self) -> i64 {
        let mut total_size = 0;
        for consume_queue_table in self.inner.consume_queue_table.lock().values() {
            for consume_queue in consume_queue_table.values() {
                total_size += consume_queue.get_total_size();
            }
        }
        total_size
    }

    fn get_store_time(&self, cq_unit: &CqUnit) -> i64 {
        match self.inner.message_store.as_ref() {
            Some(ms) => ms.get_commit_log().pickup_store_timestamp(cq_unit.pos, cq_unit.size),
            None => {
                error!("Message store is not set in ConsumeQueueStore");
                -1
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ConsumeQueueStore {
    #[inline]
    pub fn correct_min_offset(&self, consume_queue: &dyn ConsumeQueueTrait, min_commit_log_offset: i64) {
        consume_queue.correct_min_offset(min_commit_log_offset)
    }

    #[inline]
    pub fn set_batch_topic_queue_table(&self, batch_topic_queue_table: HashMap<CheetahString, i64>) {
        self.inner
            .queue_offset_operator
            .set_batch_topic_queue_table(batch_topic_queue_table)
    }

    /// Load consume queues from the given directory path for a specific queue type
    ///
    /// # Arguments
    ///
    /// * `store_path` - Base path where queue directories are stored
    /// * `cq_type` - Type of consume queue to load (SimpleCQ or BatchCQ)
    ///
    /// # Returns
    ///
    /// Returns true if loading was successful, false otherwise
    fn load_consume_queues(&mut self, store_path: &str, cq_type: CQType) -> bool {
        let dir_logic = Path::new(store_path);

        // Check if directory exists
        if !dir_logic.exists() || !dir_logic.is_dir() {
            info!("Directory {} doesn't exist or is not a directory", store_path);
            return true; // Return true as this is not an error case
        }

        // Iterate through topic directories
        match fs::read_dir(dir_logic) {
            Ok(topic_entries) => {
                for topic_dir in topic_entries.flatten() {
                    let topic_path = topic_dir.path();
                    if !topic_path.is_dir() {
                        continue;
                    }

                    // Get topic name from directory name
                    let topic = match topic_path.file_name().and_then(|n| n.to_str()) {
                        Some(name) => CheetahString::from_string(name.to_string()),
                        None => continue,
                    };

                    // Iterate through queue ID directories
                    match fs::read_dir(topic_path) {
                        Ok(queue_id_entries) => {
                            for queue_id_dir in queue_id_entries.flatten() {
                                if !queue_id_dir.path().is_dir() {
                                    continue;
                                }

                                // Parse queue ID from directory name
                                let os_string = queue_id_dir.file_name();
                                let queue_id_op = os_string.to_str();
                                let queue_id_str = match queue_id_op {
                                    Some(name) => name,
                                    None => continue,
                                };

                                let queue_id = match i32::from_str(queue_id_str) {
                                    Ok(id) => id,
                                    Err(_) => {
                                        // Skip non-numeric directory names
                                        continue;
                                    }
                                };

                                // Verify queue type matches expected type
                                if !self.queue_type_should_be(&topic, cq_type) {
                                    return false;
                                }

                                // Create consume queue based on type
                                let logic =
                                    self.create_consume_queue_by_type(&topic, queue_id, cq_type, store_path.into());

                                // Store the queue in memory table
                                self.put_consume_queue(topic.clone(), queue_id, logic);

                                // Load the queue data
                                if !self.load_logic(&topic, queue_id) {
                                    return false;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to read queue ID directories for topic {}: {}", topic, e);
                            return false;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to read topic directories: {e}");
                return false;
            }
        }

        info!("load {cq_type} all over, OK");
        true
    }

    #[inline]
    fn load_logic(&mut self, topic: &CheetahString, queue_id: i32) -> bool {
        let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
        file_queue_life_cycle.load()
    }

    #[inline]
    fn put_consume_queue(
        &self,
        topic: CheetahString,
        queue_id: i32,
        consume_queue: ArcMut<Box<dyn ConsumeQueueTrait>>,
    ) {
        let mut consume_queue_table = self.inner.consume_queue_table.lock();
        let topic_table = consume_queue_table.entry(topic).or_default();
        topic_table.insert(queue_id, consume_queue);
    }

    #[inline]
    fn queue_type_should_be(&self, topic: &CheetahString, cq_type: CQType) -> bool {
        let Some(message_store) = self.inner.message_store.as_ref() else {
            error!("MessageStore must be set before loading consume queues for topic {topic}");
            return false;
        };

        let topic_config = message_store.get_topic_config(topic);
        let act = QueueTypeUtils::get_cq_type_arc_mut(topic_config.as_ref());
        if self.is_rocksdb_cq_compat(act, cq_type) {
            return true;
        }
        if act != cq_type {
            error!("The queue type of topic: {topic} should be {cq_type:?}, but is {act:?}");
            return false;
        }

        true
    }

    #[inline]
    fn is_rocksdb_cq_compat(&self, actual: CQType, expected: CQType) -> bool {
        self.inner.message_store_config.is_enable_rocksdb_store()
            && actual == CQType::RocksDBCQ
            && expected == CQType::SimpleCQ
    }

    #[inline]
    fn truncate_dirty_logic_files(&self, topic: &CheetahString, queue_id: i32, phy_offset: i64) {
        let mut file_queue_life_cycle = self.get_life_cycle(topic, queue_id);
        file_queue_life_cycle.truncate_dirty_logic_files(phy_offset);
    }
    #[inline]
    fn create_consume_queue_by_type(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        cq_type: CQType,
        store_path: CheetahString,
    ) -> ArcMut<Box<dyn ConsumeQueueTrait>> {
        match cq_type {
            CQType::SimpleCQ | CQType::RocksDBCQ => {
                let consume_queue = ConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    store_path,
                    self.inner.message_store_config.get_mapped_file_size_consume_queue(),
                    self.inner.message_store.clone().unwrap(),
                );
                ArcMut::new(Box::new(consume_queue))
            }
            CQType::BatchCQ => {
                let consume_queue = BatchConsumeQueue::new(
                    topic.clone(),
                    queue_id,
                    store_path,
                    self.inner.message_store_config.mapper_file_size_batch_consume_queue,
                    None,
                    self.inner.message_store_config.clone(),
                );
                ArcMut::new(Box::new(consume_queue))
            }
        }
    }

    #[inline]
    fn get_life_cycle(&self, topic: &CheetahString, queue_id: i32) -> ArcConsumeQueue {
        self.find_or_create_consume_queue(topic, queue_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Buf;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_common::common::attribute::Attribute;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::TopicAttributes::TopicAttributes;
    use rocketmq_rust::ArcMut;
    use tempfile::tempdir;

    use super::*;
    use crate::base::store_enum::StoreType;
    use crate::base::swappable::Swappable;
    use crate::filter::MessageFilter;
    use crate::queue::batch_consume_queue;
    use crate::queue::FileQueueLifeCycle;

    struct TrackingQueueState {
        recover_count: AtomicUsize,
        active_recoveries: Arc<AtomicUsize>,
        max_active_recoveries: Arc<AtomicUsize>,
        delay: Duration,
        panic_on_recover: bool,
        recovered_max_physic_offset: i64,
        recovered_max_offset_in_queue: i64,
        recovered_min_logic_offset: i64,
    }

    impl TrackingQueueState {
        fn new(
            queue_id: i32,
            delay: Duration,
            panic_on_recover: bool,
            active_recoveries: Arc<AtomicUsize>,
            max_active_recoveries: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                recover_count: AtomicUsize::new(0),
                active_recoveries,
                max_active_recoveries,
                delay,
                panic_on_recover,
                recovered_max_physic_offset: 1_000 + i64::from(queue_id),
                recovered_max_offset_in_queue: 100 + i64::from(queue_id),
                recovered_min_logic_offset: i64::from(queue_id),
            }
        }
    }

    struct ActiveRecoveryGuard {
        state: Arc<TrackingQueueState>,
    }

    impl ActiveRecoveryGuard {
        fn enter(state: Arc<TrackingQueueState>) -> Self {
            let active = state.active_recoveries.fetch_add(1, Ordering::SeqCst) + 1;
            state.max_active_recoveries.fetch_max(active, Ordering::SeqCst);
            Self { state }
        }
    }

    impl Drop for ActiveRecoveryGuard {
        fn drop(&mut self) {
            self.state.active_recoveries.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct TrackingConsumeQueue {
        topic: CheetahString,
        queue_id: i32,
        mapped_file_count: usize,
        max_physic_offset: i64,
        max_offset_in_queue: i64,
        min_logic_offset: i64,
        state: Arc<TrackingQueueState>,
    }

    impl TrackingConsumeQueue {
        fn new(topic: CheetahString, queue_id: i32, state: Arc<TrackingQueueState>) -> Self {
            Self {
                topic,
                queue_id,
                mapped_file_count: (queue_id as usize) + 1,
                max_physic_offset: -1,
                max_offset_in_queue: 0,
                min_logic_offset: 0,
                state,
            }
        }
    }

    impl Swappable for TrackingConsumeQueue {
        fn swap_map(&self, _reserve_num: i32, _force_swap_interval_ms: i64, _normal_swap_interval_ms: i64) {}

        fn clean_swapped_map(&self, _force_clean_swap_interval_ms: i64) {}
    }

    impl FileQueueLifeCycle for TrackingConsumeQueue {
        fn load(&mut self) -> bool {
            true
        }

        fn recover(&mut self) {
            let _guard = ActiveRecoveryGuard::enter(self.state.clone());
            if !self.state.delay.is_zero() {
                std::thread::sleep(self.state.delay);
            }
            self.state.recover_count.fetch_add(1, Ordering::SeqCst);
            if self.state.panic_on_recover {
                panic!("tracking queue recovery failed");
            }
            self.max_physic_offset = self.state.recovered_max_physic_offset;
            self.max_offset_in_queue = self.state.recovered_max_offset_in_queue;
            self.min_logic_offset = self.state.recovered_min_logic_offset;
        }

        fn check_self(&self) {}

        fn flush(&self, _flush_least_pages: i32) -> bool {
            true
        }

        fn destroy(&mut self) {}

        fn truncate_dirty_logic_files(&mut self, _max_commit_log_pos: i64) {}

        fn delete_expired_file(&self, _min_commit_log_pos: i64) -> i32 {
            0
        }

        fn roll_next_file(&self, next_begin_offset: i64) -> i64 {
            next_begin_offset
        }

        fn is_first_file_available(&self) -> bool {
            true
        }

        fn is_first_file_exist(&self) -> bool {
            true
        }
    }

    impl ConsumeQueueTrait for TrackingConsumeQueue {
        fn get_topic(&self) -> &CheetahString {
            &self.topic
        }

        fn get_queue_id(&self) -> i32 {
            self.queue_id
        }

        fn iterate_from(&self, _start_index: i64) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
            None
        }

        fn iterate_from_with_count(
            &self,
            _start_index: i64,
            _count: i32,
        ) -> Option<Box<dyn Iterator<Item = CqUnit> + Send + '_>> {
            None
        }

        fn get(&self, _index: i64) -> Option<CqUnit> {
            None
        }

        fn get_cq_unit_and_store_time(&self, _index: i64) -> Option<(CqUnit, i64)> {
            None
        }

        fn get_earliest_unit_and_store_time(&self) -> Option<(CqUnit, i64)> {
            None
        }

        fn get_earliest_unit(&self) -> Option<CqUnit> {
            None
        }

        fn get_latest_unit(&self) -> Option<CqUnit> {
            None
        }

        fn get_last_offset(&self) -> i64 {
            self.max_physic_offset
        }

        fn get_min_offset_in_queue(&self) -> i64 {
            self.min_logic_offset
        }

        fn get_max_offset_in_queue(&self) -> i64 {
            self.max_offset_in_queue
        }

        fn get_message_total_in_queue(&self) -> i64 {
            self.max_offset_in_queue.saturating_sub(self.min_logic_offset)
        }

        fn get_offset_in_queue_by_time(&self, _timestamp: i64) -> i64 {
            0
        }

        fn get_offset_in_queue_by_time_with_boundary(&self, _timestamp: i64, _boundary_type: BoundaryType) -> i64 {
            0
        }

        fn get_max_physic_offset(&self) -> i64 {
            self.max_physic_offset
        }

        fn get_min_logic_offset(&self) -> i64 {
            self.min_logic_offset
        }

        fn get_cq_type(&self) -> CQType {
            CQType::SimpleCQ
        }

        fn get_total_size(&self) -> i64 {
            (self.mapped_file_count * 1024) as i64
        }

        fn get_mapped_file_count(&self) -> usize {
            self.mapped_file_count
        }

        fn get_unit_size(&self) -> i32 {
            CQ_STORE_UNIT_SIZE
        }

        fn correct_min_offset(&self, _min_commit_log_offset: i64) {}

        fn put_message_position_info_wrapper(&mut self, _request: &DispatchRequest) {}

        fn assign_queue_offset(&self, _queue_offset_assigner: &QueueOffsetOperator, _msg: &mut MessageExtBrokerInner) {}

        fn increase_queue_offset(
            &self,
            _queue_offset_assigner: &QueueOffsetOperator,
            _msg: &MessageExtBrokerInner,
            _message_num: i16,
        ) {
        }

        fn estimate_message_count(&self, _from: i64, _to: i64, _filter: &dyn MessageFilter) -> i64 {
            0
        }
    }

    fn tracking_store(
        queue_count: i32,
        delay: Duration,
        panic_queue_id: Option<i32>,
    ) -> (ConsumeQueueStore, Vec<Arc<TrackingQueueState>>) {
        let store = ConsumeQueueStore::new(
            Arc::new(MessageStoreConfig::default()),
            Arc::new(BrokerConfig::default()),
        );
        let mut states = Vec::new();
        let active_recoveries = Arc::new(AtomicUsize::new(0));
        let max_active_recoveries = Arc::new(AtomicUsize::new(0));
        for queue_id in 0..queue_count {
            let topic = CheetahString::from_string(format!("TrackingTopic{}", queue_id % 2));
            let state = Arc::new(TrackingQueueState::new(
                queue_id,
                delay,
                panic_queue_id == Some(queue_id),
                active_recoveries.clone(),
                max_active_recoveries.clone(),
            ));
            let queue: Box<dyn ConsumeQueueTrait> =
                Box::new(TrackingConsumeQueue::new(topic.clone(), queue_id, state.clone()));
            store.put_consume_queue(topic, queue_id, ArcMut::new(queue));
            states.push(state);
        }
        (store, states)
    }

    fn recovery_offsets(store: &ConsumeQueueStore) -> Vec<(String, i32, i64, i64, i64)> {
        let mut offsets = store
            .inner
            .consume_queue_table
            .lock()
            .iter()
            .flat_map(|(topic, queues)| {
                queues.values().map(|queue| {
                    (
                        topic.to_string(),
                        queue.get_queue_id(),
                        queue.get_min_logic_offset(),
                        queue.get_max_offset_in_queue(),
                        queue.get_max_physic_offset(),
                    )
                })
            })
            .collect::<Vec<_>>();
        offsets.sort();
        offsets
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_concurrently_with_parallelism_bounds_topic_queue_recovery() {
        let (store, states) = tracking_store(4, Duration::from_millis(30), None);

        assert!(store.recover_concurrently_with_parallelism(2).await);

        let recovered = states
            .iter()
            .map(|state| state.recover_count.load(Ordering::SeqCst))
            .sum::<usize>();
        let max_active = states
            .iter()
            .map(|state| state.max_active_recoveries.load(Ordering::SeqCst))
            .max()
            .unwrap_or_default();
        assert_eq!(recovered, 4);
        assert!(
            max_active <= 2,
            "max active recoveries should be bounded by parallelism"
        );
        assert!(max_active > 1, "recovery should run more than one queue concurrently");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_concurrently_with_summary_reports_successes() {
        let (store, _) = tracking_store(3, Duration::ZERO, None);

        let summary = store.recover_concurrently_with_summary(2).await;

        assert!(summary.is_success());
        assert_eq!(summary.queue_count, 3);
        assert_eq!(summary.success_count, 3);
        assert_eq!(summary.failure_count, 0);
        assert!(summary.failures.is_empty());
        assert_eq!(summary.failure_description(), "none");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_concurrently_with_summary_reports_failed_queue_identity() {
        let (store, states) = tracking_store(3, Duration::ZERO, Some(1));

        let summary = store.recover_concurrently_with_summary(2).await;

        assert!(!summary.is_success());
        assert_eq!(summary.queue_count, 3);
        assert_eq!(summary.success_count, 2);
        assert_eq!(summary.failure_count, 1);
        assert_eq!(states[1].recover_count.load(Ordering::SeqCst), 1);
        let failure = summary.failures.first().expect("failed queue");
        assert_eq!(failure.topic, CheetahString::from_static_str("TrackingTopic1"));
        assert_eq!(failure.queue_id, 1);
        assert_eq!(failure.cq_type, CQType::SimpleCQ);
        assert!(failure.error.contains("tracking queue recovery failed"));
        assert!(summary.failure_description().contains("TrackingTopic1:1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_concurrently_matches_serial_queue_offsets() {
        let (serial_store, _) = tracking_store(3, Duration::ZERO, None);
        let (concurrent_store, _) = tracking_store(3, Duration::ZERO, None);

        serial_store.recover().await;
        assert!(concurrent_store.recover_concurrently_with_parallelism(2).await);

        assert_eq!(recovery_offsets(&serial_store), recovery_offsets(&concurrent_store));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recover_concurrently_returns_false_when_queue_recovery_panics() {
        let (store, states) = tracking_store(3, Duration::ZERO, Some(1));

        assert!(!store.recover_concurrently_with_parallelism(2).await);
        assert_eq!(states[1].recover_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn load_consume_queues_returns_false_when_topic_queue_type_mismatches_directory() {
        let root = tempdir().expect("tempdir");
        let topic = CheetahString::from_static_str("MismatchTopic");
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().to_string().into(),
            ..MessageStoreConfig::default()
        });
        let broker_config = Arc::new(BrokerConfig::default());
        let topic_config_table = Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new());
        topic_config_table.insert(topic.clone(), ArcMut::new(TopicConfig::new(topic.clone())));
        let mut message_store = ArcMut::new(LocalFileMessageStore::new(
            message_store_config.clone(),
            broker_config.clone(),
            topic_config_table,
            None,
            false,
        ));
        let message_store_clone = message_store.clone();
        message_store.set_message_store_arc(message_store_clone);

        let batch_store_path = get_store_path_batch_consume_queue(message_store_config.store_path_root_dir.as_str());
        fs::create_dir_all(Path::new(&batch_store_path).join(topic.as_str()).join("0"))
            .expect("batch consume queue directory");
        let mut store = ConsumeQueueStore::new(message_store_config, broker_config);
        store.set_message_store(message_store);

        assert!(!store.load_consume_queues(&batch_store_path, CQType::BatchCQ));
    }

    #[test]
    fn rocksdb_cq_loads_from_simple_cq_path_in_compat_mode() {
        let root = tempdir().expect("tempdir");
        let topic = CheetahString::from_static_str("RocksCompatTopic");
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().to_string().into(),
            store_type: StoreType::RocksDB,
            ..MessageStoreConfig::default()
        });
        let broker_config = Arc::new(BrokerConfig::default());
        let topic_config_table = Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new());
        let mut topic_config = TopicConfig::new(topic.clone());
        topic_config.attributes.insert(
            TopicAttributes::queue_type_attribute().name().clone(),
            CQType::RocksDBCQ.to_string().into(),
        );
        topic_config_table.insert(topic.clone(), ArcMut::new(topic_config));
        let mut message_store = ArcMut::new(LocalFileMessageStore::new(
            message_store_config.clone(),
            broker_config.clone(),
            topic_config_table,
            None,
            false,
        ));
        let message_store_clone = message_store.clone();
        message_store.set_message_store_arc(message_store_clone);

        let simple_store_path = get_store_path_consume_queue(message_store_config.store_path_root_dir.as_str());
        fs::create_dir_all(Path::new(&simple_store_path).join(topic.as_str()).join("0"))
            .expect("simple consume queue directory");
        let mut store = ConsumeQueueStore::new(message_store_config, broker_config);
        store.set_message_store(message_store);

        assert!(store.load_consume_queues(&simple_store_path, CQType::SimpleCQ));
        let queue = store
            .find_consume_queue(&topic, 0)
            .expect("rocksdb compatibility queue should load");
        assert_eq!(queue.get_cq_type(), CQType::SimpleCQ);
    }

    #[test]
    fn create_consume_queue_by_type_maps_rocksdb_cq_to_simple_cq() {
        let root = tempdir().expect("tempdir");
        let topic = CheetahString::from_static_str("RocksCreateTopic");
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().to_string().into(),
            store_type: StoreType::RocksDB,
            ..MessageStoreConfig::default()
        });
        let broker_config = Arc::new(BrokerConfig::default());
        let topic_config_table = Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new());
        let mut message_store = ArcMut::new(LocalFileMessageStore::new(
            message_store_config.clone(),
            broker_config.clone(),
            topic_config_table,
            None,
            false,
        ));
        let message_store_clone = message_store.clone();
        message_store.set_message_store_arc(message_store_clone);
        let mut store = ConsumeQueueStore::new(message_store_config, broker_config);
        store.set_message_store(message_store);

        let queue = store.create_consume_queue_by_type(
            &topic,
            0,
            CQType::RocksDBCQ,
            CheetahString::from_string(root.path().join("compat-cq").to_string_lossy().to_string()),
        );

        assert_eq!(queue.get_cq_type(), CQType::SimpleCQ);
    }

    #[test]
    fn batch_consume_queue_store_get_returns_full_batch_unit() {
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let broker_config = Arc::new(BrokerConfig::default());
        let store = ConsumeQueueStore::new(message_store_config.clone(), broker_config);
        let root = tempdir().expect("tempdir");
        let store_path = CheetahString::from_string(root.path().join("batch-cq").to_string_lossy().to_string());
        let topic = CheetahString::from_static_str("BatchTopic");
        let queue_id = 1;

        let mut batch_queue = BatchConsumeQueue::new(
            topic.clone(),
            queue_id,
            store_path,
            (batch_consume_queue::CQ_STORE_UNIT_SIZE * 2) as usize,
            None,
            message_store_config,
        );
        batch_queue.put_message_position_info_wrapper(&DispatchRequest {
            topic: topic.clone(),
            queue_id,
            commit_log_offset: 100,
            msg_size: 32,
            tags_code: 7,
            store_timestamp: 1_000,
            msg_base_offset: 0,
            batch_size: 3,
            success: true,
            ..DispatchRequest::default()
        });

        store
            .inner
            .consume_queue_table
            .lock()
            .entry(topic.clone())
            .or_default()
            .insert(queue_id, ArcMut::new(Box::new(batch_queue)));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let bytes = runtime.block_on(store.get(&topic, queue_id, 0));

        assert_eq!(bytes.len(), batch_consume_queue::CQ_STORE_UNIT_SIZE as usize);
        let mut bytes = bytes;
        assert_eq!(bytes.get_i64(), 100);
        assert_eq!(bytes.get_i32(), 32);
        assert_eq!(bytes.get_i64(), 7);
        assert_eq!(bytes.get_i64(), 1_000);
        assert_eq!(bytes.get_i64(), 0);
        assert_eq!(bytes.get_i16(), 3);
    }
}
