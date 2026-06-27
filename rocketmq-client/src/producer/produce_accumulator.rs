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

use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_rust::ArcMut;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;

use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientTrackedTaskHandle;

const GUARD_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

type BatchMap = Arc<DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>>;

#[derive(Debug, Clone, Serialize)]
pub struct ProduceAccumulatorGuardLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
    pub guard_metrics: ProduceAccumulatorGuardMetricsSnapshot,
    pub healthy: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ProduceAccumulatorGuardMetricsSnapshot {
    pub sync: BatchGuardMetricsSnapshot,
    pub async_send: BatchGuardMetricsSnapshot,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct BatchGuardMetricsSnapshot {
    pub wakeup_count: u64,
    pub flush_count: u64,
    pub idle_wakeup_count: u64,
}

#[derive(Default)]
struct BatchGuardMetrics {
    wakeup_count: AtomicU64,
    flush_count: AtomicU64,
    idle_wakeup_count: AtomicU64,
}

impl BatchGuardMetrics {
    fn record_wakeup(&self) {
        self.wakeup_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_idle(&self) {
        self.idle_wakeup_count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> BatchGuardMetricsSnapshot {
        BatchGuardMetricsSnapshot {
            wakeup_count: self.wakeup_count.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            idle_wakeup_count: self.idle_wakeup_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub struct ProduceAccumulator {
    total_hold_size: usize,
    hold_size: usize,
    hold_ms: u32,
    guard_thread_for_sync_send: GuardForSyncSendService,
    guard_thread_for_async_send: GuardForAsyncSendService,
    currently_hold_size: Arc<AtomicU64>,
    instance_name: String,
    sync_send_batchs: Arc<DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>>,
    async_send_batchs: Arc<DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>>,
}

impl ProduceAccumulator {
    pub fn new(instance_name: &str) -> Self {
        Self {
            total_hold_size: 1024 * 1024 * 32,
            hold_size: 1024 * 32,
            hold_ms: 10,
            instance_name: instance_name.to_string(),
            guard_thread_for_async_send: GuardForAsyncSendService::new(instance_name),
            guard_thread_for_sync_send: GuardForSyncSendService::new(instance_name),
            ..Default::default()
        }
    }
}

impl ProduceAccumulator {
    pub fn batch_max_delay_ms(&self) -> u32 {
        self.hold_ms
    }

    pub fn set_batch_max_delay_ms(&mut self, hold_ms: u32) -> rocketmq_error::RocketMQResult<()> {
        if hold_ms == 0 || hold_ms > 30_000 {
            return Err(crate::mq_client_err!(format!(
                "batchMaxDelayMs expect between 1ms and 30s, but get {hold_ms}!"
            )));
        }
        self.hold_ms = hold_ms;
        Ok(())
    }

    pub fn batch_max_bytes(&self) -> usize {
        self.hold_size
    }

    pub fn set_batch_max_bytes(&mut self, hold_size: u64) -> rocketmq_error::RocketMQResult<()> {
        if hold_size == 0 || hold_size > 2 * 1024 * 1024 {
            return Err(crate::mq_client_err!(format!(
                "batchMaxBytes expect between 1B and 2MB, but get {hold_size}!"
            )));
        }
        self.hold_size = hold_size as usize;
        Ok(())
    }

    pub fn total_batch_max_bytes(&self) -> usize {
        self.total_hold_size
    }

    pub fn set_total_batch_max_bytes(&mut self, total_hold_size: u64) -> rocketmq_error::RocketMQResult<()> {
        if total_hold_size == 0 {
            return Err(crate::mq_client_err!(format!(
                "totalBatchMaxBytes must bigger then 0, but get {total_hold_size}!"
            )));
        }
        self.total_hold_size = total_hold_size as usize;
        Ok(())
    }

    pub fn start(&mut self) {
        self.guard_thread_for_sync_send
            .start(self.sync_send_batchs.clone(), self.hold_ms);
        self.guard_thread_for_async_send.start(
            self.async_send_batchs.clone(),
            self.currently_hold_size.clone(),
            self.hold_size,
            self.hold_ms,
        );
    }
    pub fn shutdown(&mut self) {
        self.guard_thread_for_sync_send.shutdown();
        self.guard_thread_for_async_send.shutdown();
        self.release_pending_batches_on_shutdown();
    }

    pub async fn shutdown_async(&mut self) {
        self.guard_thread_for_sync_send.shutdown_async().await;
        self.guard_thread_for_async_send.shutdown_async().await;
        self.release_pending_batches_on_shutdown_async().await;
    }

    fn guard_task_count(&self) -> usize {
        self.guard_thread_for_sync_send.task_count() + self.guard_thread_for_async_send.task_count()
    }

    pub fn guard_metrics_snapshot(&self) -> ProduceAccumulatorGuardMetricsSnapshot {
        ProduceAccumulatorGuardMetricsSnapshot {
            sync: self.guard_thread_for_sync_send.metrics_snapshot(),
            async_send: self.guard_thread_for_async_send.metrics_snapshot(),
        }
    }

    pub(crate) fn try_add_message<T: MessageTrait>(&self, message: &T) -> bool {
        let body_size = message.get_body().map_or(0, |body| body.len()) as u64;
        if body_size == 0 {
            return true;
        }
        self.currently_hold_size
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let next = current.checked_add(body_size)?;
                (next <= self.total_hold_size as u64).then_some(next)
            })
            .is_ok()
    }

    fn release_hold_size(&self, size: u64) {
        release_hold_size(&self.currently_hold_size, size);
    }

    fn release_pending_batches_on_shutdown(&self) {
        let error_message = "ProduceAccumulator is shutdown";
        let sync_batches = self
            .sync_send_batchs
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        self.sync_send_batchs.clear();
        for batch in sync_batches {
            match batch.try_lock() {
                Ok(mut batch_guard) => {
                    close_pending_batch(&mut batch_guard, &self.currently_hold_size, error_message, false);
                }
                Err(_) => tracing::warn!("skip locked sync batch during ProduceAccumulator shutdown"),
            }
        }

        let async_batches = self
            .async_send_batchs
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        self.async_send_batchs.clear();
        for batch in async_batches {
            match batch.try_lock() {
                Ok(mut batch_guard) => {
                    close_pending_batch(&mut batch_guard, &self.currently_hold_size, error_message, true);
                }
                Err(_) => tracing::warn!("skip locked async batch during ProduceAccumulator shutdown"),
            }
        }
    }

    async fn release_pending_batches_on_shutdown_async(&self) {
        let error_message = "ProduceAccumulator is shutdown";
        let sync_batches = self
            .sync_send_batchs
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        self.sync_send_batchs.clear();
        for batch in sync_batches {
            let mut batch_guard = batch.lock().await;
            close_pending_batch(&mut batch_guard, &self.currently_hold_size, error_message, false);
        }

        let async_batches = self
            .async_send_batchs
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        self.async_send_batchs.clear();
        for batch in async_batches {
            let mut batch_guard = batch.lock().await;
            close_pending_batch(&mut batch_guard, &self.currently_hold_size, error_message, true);
        }
    }

    pub(crate) async fn send<M: MessageTrait + Send + Sync + 'static>(
        &mut self,
        message: M,
        mq: Option<MessageQueue>,
        default_mq_producer: DefaultMQProducer,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>> {
        let partition_key = AggregateKey::new_from_message_queue(&message, mq);

        let batch = self
            .get_or_create_sync_send_batch(partition_key.clone(), &default_mq_producer)
            .await;

        let reserved_size = message.get_body().map_or(0, |body| body.len()) as u64;

        // Lock the batch for exclusive access and add message
        let add_outcome = {
            let mut batch_guard = batch.lock().await;
            batch_guard.add(message, None, self.hold_size, self.hold_ms as u64)?
        }; // batch_guard dropped here

        // Check if add failed (batch closed)
        let Some(add_outcome) = add_outcome else {
            // Batch is closed, cannot retry because message is already consumed
            self.sync_send_batchs.remove(&partition_key);
            self.release_hold_size(reserved_size);
            return Err(crate::mq_client_err!("Batch is closed, cannot add message"));
        };

        let msg_index = add_outcome.index;
        let notify = add_outcome.notify;
        if add_outcome.should_flush {
            let batch_to_send = self.sync_send_batchs.remove(&partition_key).map(|(_, v)| v);
            if let Some(batch_arc) = batch_to_send {
                self.send_batch_sync(batch_arc).await?;
            }
        }

        // Wait for batch to be ready and sent
        loop {
            // Check if batch is closed (sent)
            let (state, should_send) = {
                let batch_guard = batch.lock().await;
                let state = batch_guard.state();
                let should_send = batch_guard.ready_to_send(self.hold_size, self.hold_ms as u64);
                (state, should_send)
            };

            if state == BatchState::Closed {
                // Batch has been sent, get result
                let (error, result) = {
                    let batch_guard = batch.lock().await;
                    (
                        batch_guard.send_error.clone(),
                        batch_guard
                            .send_results
                            .as_ref()
                            .and_then(|results| results.get(msg_index).cloned()),
                    )
                };

                if let Some(error) = error {
                    return Err(crate::mq_client_err!(error));
                }
                return Ok(result);
            }

            if state == BatchState::Open && should_send {
                // Try to remove and send the batch
                let batch_to_send = self.sync_send_batchs.remove(&partition_key).map(|(_, v)| v);

                if let Some(batch_arc) = batch_to_send {
                    // Send the batch (without holding the lock)
                    self.send_batch_sync(batch_arc).await?;
                    // Continue to wait loop to get result
                    continue;
                }
            }

            // Wait for notification instead of polling
            notify.notified().await;
        }
    }

    pub(crate) async fn send_callback<M>(
        &mut self,
        message: M,
        mq: Option<MessageQueue>,
        send_callback: Option<ArcSendCallback>,
        default_mq_producer: DefaultMQProducer,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync + 'static,
    {
        let partition_key = AggregateKey::new_from_message_queue(&message, mq);

        let batch = self
            .get_or_create_async_send_batch(partition_key.clone(), &default_mq_producer)
            .await;

        let reserved_size = message.get_body().map_or(0, |body| body.len()) as u64;

        // Lock the batch for exclusive access
        let add_outcome = {
            let mut batch_guard = batch.lock().await;
            batch_guard.add(message, send_callback, self.hold_size, self.hold_ms as u64)?
        }; // batch_guard dropped here

        // Try to add message to batch
        match add_outcome {
            Some(add_outcome) => {
                // Message added successfully, check if ready to send
                if add_outcome.should_flush {
                    // Remove batch from map
                    let batch_to_send = self.async_send_batchs.remove(&partition_key).map(|(_, v)| v);

                    if let Some(batch_arc) = batch_to_send {
                        // Send the batch (without holding the lock)
                        self.send_batch_async(batch_arc).await?;
                    }
                }
                Ok(())
            }
            None => {
                // Batch is closed, remove it and return error
                self.async_send_batchs.remove(&partition_key);
                self.release_hold_size(reserved_size);
                Err(crate::mq_client_err!("Batch is closed, please retry"))
            }
        }
    }

    async fn get_or_create_sync_send_batch(
        &self,
        aggregate_key: AggregateKey,
        default_mq_producer: &DefaultMQProducer,
    ) -> Arc<Mutex<MessageAccumulation>> {
        match self.sync_send_batchs.entry(aggregate_key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let accumulation =
                    MessageAccumulation::new(aggregate_key.clone(), ArcMut::new(default_mq_producer.clone()));
                let create_time = accumulation.create_time;
                let deadline_ms = accumulation.deadline_ms(self.hold_ms as u64);
                let batch = Arc::new(Mutex::new(accumulation));
                entry.insert(batch.clone());
                self.guard_thread_for_sync_send
                    .schedule_batch(aggregate_key, create_time, deadline_ms);
                batch
            }
        }
    }

    async fn get_or_create_async_send_batch(
        &self,
        aggregate_key: AggregateKey,
        default_mq_producer: &DefaultMQProducer,
    ) -> Arc<Mutex<MessageAccumulation>> {
        match self.async_send_batchs.entry(aggregate_key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let accumulation =
                    MessageAccumulation::new(aggregate_key.clone(), ArcMut::new(default_mq_producer.clone()));
                let create_time = accumulation.create_time;
                let deadline_ms = accumulation.deadline_ms(self.hold_ms as u64);
                let batch = Arc::new(Mutex::new(accumulation));
                entry.insert(batch.clone());
                self.guard_thread_for_async_send
                    .schedule_batch(aggregate_key, create_time, deadline_ms);
                batch
            }
        }
    }

    /// Send a batch synchronously (extracted to avoid holding lock across await)
    async fn send_batch_sync(&self, batch: Arc<Mutex<MessageAccumulation>>) -> rocketmq_error::RocketMQResult<()> {
        // Extract all data from the batch without holding the lock across await
        let (messages, mq, mut producer, total_size, count, notify, aggregate_key, keys) = {
            let mut batch_guard = batch.lock().await;
            if !batch_guard.try_mark_closing() {
                return Ok(());
            }
            let notify = batch_guard.completion_notify.clone();

            if batch_guard.messages.is_empty() {
                let error = crate::mq_client_err!("No messages to send");
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                return Err(crate::mq_client_err!("No messages to send"));
            }

            let total_size = batch_guard.messages_size.load(Ordering::Acquire) as u64;
            let messages = std::mem::take(&mut batch_guard.messages);
            let mq = batch_guard.aggregate_key.mq.clone();
            let producer = batch_guard.default_mq_producer.clone();
            let count = batch_guard.count;
            let aggregate_key = batch_guard.aggregate_key.clone();
            let keys = batch_guard.keys.clone();

            (messages, mq, producer, total_size, count, notify, aggregate_key, keys)
        }; // Lock released here

        let batch_msg = match build_message_batch(messages, &aggregate_key, &keys) {
            Ok(batch_msg) => batch_msg,
            Err(error) => {
                self.release_hold_size(total_size);
                let mut batch_guard = batch.lock().await;
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                return Err(error);
            }
        };

        // Send without holding any locks
        let send_result = match producer.send_direct(batch_msg, mq, None).await {
            Ok(result) => result,
            Err(error) => {
                self.release_hold_size(total_size);
                let mut batch_guard = batch.lock().await;
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                return Err(error);
            }
        };

        // Decrement currently_hold_size
        self.release_hold_size(total_size);

        // Store results in batch for waiting threads to retrieve
        if let Some(result) = send_result {
            let send_results = match split_send_results(&result, count) {
                Ok(results) => results,
                Err(error) => {
                    let mut batch_guard = batch.lock().await;
                    batch_guard.send_error = Some(error.to_string());
                    batch_guard.mark_closed();
                    notify.notify_waiters();
                    return Err(error);
                }
            };
            let mut batch_guard = batch.lock().await;
            batch_guard.send_results = Some(send_results);
            batch_guard.mark_closed();
        } else {
            let batch_guard = batch.lock().await;
            batch_guard.mark_closed();
        }

        // Notify all waiting threads
        notify.notify_waiters();

        Ok(())
    }

    /// Send a batch asynchronously (extracted to avoid holding lock across await)
    async fn send_batch_async(&self, batch: Arc<Mutex<MessageAccumulation>>) -> rocketmq_error::RocketMQResult<()> {
        // Extract all data from the batch without holding the lock across await
        let (messages, mq, mut producer, total_size, callbacks, aggregate_key, keys, notify) = {
            let mut batch_guard = batch.lock().await;
            if !batch_guard.try_mark_closing() {
                return Ok(());
            }
            let notify = batch_guard.completion_notify.clone();

            if batch_guard.messages.is_empty() {
                let error = crate::mq_client_err!("No messages to send");
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                return Err(error);
            }

            let total_size = batch_guard.messages_size.load(Ordering::Acquire) as u64;
            let messages = std::mem::take(&mut batch_guard.messages);
            let callbacks = std::mem::take(&mut batch_guard.send_callbacks);
            let mq = batch_guard.aggregate_key.mq.clone();
            let producer = batch_guard.default_mq_producer.clone();
            let aggregate_key = batch_guard.aggregate_key.clone();
            let keys = batch_guard.keys.clone();

            (
                messages,
                mq,
                producer,
                total_size,
                callbacks,
                aggregate_key,
                keys,
                notify,
            )
        }; // Lock released here

        let batch_msg = match build_message_batch(messages, &aggregate_key, &keys) {
            Ok(batch_msg) => batch_msg,
            Err(error) => {
                self.release_hold_size(total_size);
                let mut batch_guard = batch.lock().await;
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                for callback in &callbacks {
                    callback.on_exception(&error);
                }
                return Err(error);
            }
        };

        let currently_hold_size = self.currently_hold_size.clone();
        let callback_currently_hold_size = currently_hold_size.clone();
        let callbacks = Arc::new(callbacks);
        let callbacks_for_send_error = callbacks.clone();

        // Create combined callback
        let combined_callback = move |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
            release_hold_size(&callback_currently_hold_size, total_size);
            // Invoke all registered callbacks
            if let Some(result) = result {
                match split_send_results(result, callbacks.len()) {
                    Ok(send_results) => {
                        for (callback, result) in callbacks.iter().zip(send_results.iter()) {
                            callback.on_success(result);
                        }
                    }
                    Err(error) => {
                        for callback in callbacks.iter() {
                            callback.on_exception(&error);
                        }
                    }
                }
            } else if let Some(error) = error {
                for callback in callbacks.iter() {
                    callback.on_exception(error);
                }
            }
        };

        // Send without holding any locks
        let send_result = producer
            .send_direct(batch_msg, mq, Some(Arc::new(combined_callback)))
            .await;

        if let Err(error) = &send_result {
            release_hold_size(&currently_hold_size, total_size);
            let mut batch_guard = batch.lock().await;
            batch_guard.send_error = Some(error.to_string());
            batch_guard.mark_closed();
            notify.notify_waiters();
            for callback in callbacks_for_send_error.iter() {
                callback.on_exception(error);
            }
        } else {
            let batch_guard = batch.lock().await;
            batch_guard.mark_closed();
            notify.notify_waiters();
        }

        send_result.map(|_| ())
    }
}

fn release_hold_size(currently_hold_size: &AtomicU64, size: u64) {
    if size == 0 {
        return;
    }
    let _ = currently_hold_size.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(size))
    });
}

fn close_pending_batch(
    batch: &mut MessageAccumulation,
    currently_hold_size: &AtomicU64,
    error_message: &str,
    notify_callbacks: bool,
) {
    if !batch.try_mark_closing() {
        return;
    }

    let total_size = batch.messages_size.load(Ordering::Acquire) as u64;
    release_hold_size(currently_hold_size, total_size);
    batch.send_error = Some(error_message.to_string());
    batch.mark_closed();
    batch.completion_notify.notify_waiters();

    if notify_callbacks {
        let error = crate::mq_client_err!(error_message.to_string());
        for callback in &batch.send_callbacks {
            callback.on_exception(&error);
        }
    }
}

fn split_send_results(send_result: &SendResult, count: usize) -> rocketmq_error::RocketMQResult<Vec<SendResult>> {
    let Some(msg_id) = send_result.msg_id.as_ref() else {
        return Err(crate::mq_client_err!("sendResult is illegal"));
    };

    if !msg_id.as_str().contains(',') {
        return Ok(vec![send_result.clone(); count]);
    }

    let Some(offset_msg_id) = send_result.offset_msg_id.as_ref() else {
        return Err(crate::mq_client_err!("sendResult is illegal"));
    };

    let msg_ids = msg_id.as_str().split(',').collect::<Vec<_>>();
    let offset_msg_ids = offset_msg_id.split(',').collect::<Vec<_>>();
    if msg_ids.len() != count || offset_msg_ids.len() != count {
        return Err(crate::mq_client_err!("sendResult is illegal"));
    }

    Ok((0..count)
        .map(|index| {
            let mut result = send_result.clone();
            result.msg_id = Some(CheetahString::from_slice(msg_ids[index]));
            result.offset_msg_id = Some(offset_msg_ids[index].to_string());
            result.queue_offset += index as u64;
            result
        })
        .collect())
}

fn build_message_batch(
    messages: Vec<Box<dyn MessageTrait + Send + Sync + 'static>>,
    aggregate_key: &AggregateKey,
    keys: &HashSet<String>,
) -> rocketmq_error::RocketMQResult<MessageBatch> {
    let mut concrete_messages = Vec::with_capacity(messages.len());
    for boxed_msg in messages {
        if let Some(msg) = boxed_msg.as_any().downcast_ref::<Message>() {
            concrete_messages.push(msg.clone());
        } else {
            let mut msg = Message::default();
            msg.set_topic(boxed_msg.topic().clone());
            if let Some(body) = boxed_msg.get_body() {
                msg.set_body(Some(body.clone()));
            }
            msg.set_flag(boxed_msg.get_flag());
            msg.set_properties(boxed_msg.get_properties().clone());
            concrete_messages.push(msg);
        }
    }

    let mut batch = MessageBatch::generate_from_messages(concrete_messages)?;
    batch.set_topic(aggregate_key.topic.clone());
    batch.set_wait_store_msg_ok(aggregate_key.wait_store_msg_ok);
    if let Some(tag) = aggregate_key.tag.as_ref() {
        batch.set_tags(tag.clone());
    }
    if !keys.is_empty() {
        let mut sorted_keys = keys.iter().map(String::as_str).collect::<Vec<_>>();
        sorted_keys.sort_unstable();
        batch.set_keys(CheetahString::from_string(
            sorted_keys.join(MessageConst::KEY_SEPARATOR),
        ));
    }
    Ok(batch)
}

#[doc(hidden)]
pub async fn run_produce_accumulator_guard_lifecycle_probe() -> ProduceAccumulatorGuardLifecycleProbe {
    let mut accumulator = ProduceAccumulator::new("produce_accumulator_guard_probe");
    accumulator.start();

    let mut task_count_before_shutdown = accumulator.guard_task_count();
    for _ in 0..100 {
        if task_count_before_shutdown == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        task_count_before_shutdown = accumulator.guard_task_count();
    }

    let shutdown_started_at = Instant::now();
    accumulator.shutdown_async().await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = accumulator.guard_task_count();
    let guard_metrics = accumulator.guard_metrics_snapshot();
    let healthy = task_count_before_shutdown == 2 && task_count_after_shutdown == 0;

    ProduceAccumulatorGuardLifecycleProbe {
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
        guard_metrics,
        healthy,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::Ordering;

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn try_add_message_counts_body_size() {
        let accumulator = ProduceAccumulator::new("test");
        let message = Message::builder()
            .topic("test-topic")
            .body_slice(b"hello")
            .build_unchecked();

        assert!(accumulator.try_add_message(&message));
        assert_eq!(accumulator.currently_hold_size.load(Ordering::Acquire), 5);
    }

    #[test]
    fn try_add_message_allows_missing_body_without_accounting() {
        let accumulator = ProduceAccumulator::new("test");
        let mut message = Message::builder()
            .topic("test-topic")
            .body_slice(b"hello")
            .build_unchecked();
        assert_eq!(message.take_body().as_deref(), Some(&b"hello"[..]));

        assert!(accumulator.try_add_message(&message));
        assert_eq!(accumulator.currently_hold_size.load(Ordering::Acquire), 0);
    }

    #[test]
    fn try_add_message_rejects_when_total_hold_size_reaches_limit_like_java() {
        let mut accumulator = ProduceAccumulator::new("test");
        accumulator.set_total_batch_max_bytes(5).unwrap();
        let message = Message::builder()
            .topic("test-topic")
            .body_slice(b"hello")
            .build_unchecked();

        assert!(accumulator.try_add_message(&message));
        assert_eq!(accumulator.currently_hold_size.load(Ordering::Acquire), 5);
        assert!(!accumulator.try_add_message(&message));
    }

    #[test]
    fn try_add_message_concurrent_capacity_reservation_does_not_exceed_limit() {
        let mut accumulator = ProduceAccumulator::new("test");
        accumulator.set_total_batch_max_bytes(64).unwrap();
        let accumulator = Arc::new(accumulator);
        let successes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let message = Message::builder()
            .topic("test-topic")
            .body_slice(b"x")
            .build_unchecked();

        let threads = (0..16)
            .map(|_| {
                let accumulator = accumulator.clone();
                let successes = successes.clone();
                let message = message.clone();
                std::thread::spawn(move || {
                    for _ in 0..16 {
                        if accumulator.try_add_message(&message) {
                            successes.fetch_add(1, Ordering::AcqRel);
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().expect("capacity reservation thread should finish");
        }

        assert!(successes.load(Ordering::Acquire) <= 64);
        assert!(accumulator.currently_hold_size.load(Ordering::Acquire) <= 64);
    }

    #[test]
    fn message_accumulation_ready_to_send_requires_size_over_hold_size_like_java() {
        let producer = DefaultMQProducer::default();
        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let mut accumulation = MessageAccumulation::new(aggregate_key, ArcMut::new(producer));
        let message = Message::builder()
            .topic("test-topic")
            .body_slice(b"hello")
            .build_unchecked();

        assert!(accumulation.add(message, None, 5, 30_000).unwrap().is_some());

        assert!(!accumulation.ready_to_send(5, 30_000));
        assert!(accumulation.ready_to_send(4, 30_000));
    }

    #[test]
    fn message_accumulation_add_returns_index_and_flush_decision() {
        let producer = DefaultMQProducer::default();
        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let mut accumulation = MessageAccumulation::new(aggregate_key, ArcMut::new(producer));
        let first = Message::builder()
            .topic("test-topic")
            .body_slice(b"abc")
            .build_unchecked();
        let second = Message::builder()
            .topic("test-topic")
            .body_slice(b"def")
            .build_unchecked();

        let first_outcome = accumulation
            .add(first, None, 5, 30_000)
            .unwrap()
            .expect("first message should be added");
        let second_outcome = accumulation
            .add(second, None, 5, 30_000)
            .unwrap()
            .expect("second message should be added");

        assert_eq!(first_outcome.index, 0);
        assert!(!first_outcome.should_flush);
        assert_eq!(second_outcome.index, 1);
        assert!(second_outcome.should_flush);
    }

    #[test]
    fn message_accumulation_closing_state_is_claimed_once() {
        let producer = DefaultMQProducer::default();
        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let accumulation = MessageAccumulation::new(aggregate_key, ArcMut::new(producer));

        assert_eq!(accumulation.state(), BatchState::Open);
        assert!(accumulation.try_mark_closing());
        assert_eq!(accumulation.state(), BatchState::Closing);
        assert!(!accumulation.try_mark_closing());
        accumulation.mark_closed();
        assert_eq!(accumulation.state(), BatchState::Closed);
    }

    #[test]
    fn close_pending_batch_does_not_release_batch_already_claimed_for_send() {
        let producer = DefaultMQProducer::default();
        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let mut accumulation = MessageAccumulation::new(aggregate_key, ArcMut::new(producer));
        let message = Message::builder()
            .topic("test-topic")
            .body_slice(b"hello")
            .build_unchecked();
        assert!(accumulation.add(message, None, usize::MAX, 30_000).unwrap().is_some());
        assert!(accumulation.try_mark_closing());

        let currently_hold_size = AtomicU64::new(5);
        close_pending_batch(
            &mut accumulation,
            &currently_hold_size,
            "ProduceAccumulator is shutdown",
            false,
        );

        assert_eq!(accumulation.state(), BatchState::Closing);
        assert_eq!(currently_hold_size.load(Ordering::Acquire), 5);
    }

    #[test]
    fn message_accumulation_batch_sets_java_accumulator_keys_and_tags() {
        let producer = DefaultMQProducer::default();
        let aggregate_key = AggregateKey::new(
            CheetahString::from("test-topic"),
            None,
            true,
            Some(CheetahString::from("TagA")),
        );
        let mut accumulation = MessageAccumulation::new(aggregate_key, ArcMut::new(producer));
        let first = Message::builder()
            .topic("test-topic")
            .tags("TagA")
            .keys(vec!["key-b".to_string(), "key-a".to_string()])
            .body_slice(b"hello")
            .build_unchecked();
        let second = Message::builder()
            .topic("test-topic")
            .tags("TagA")
            .keys(vec!["key-c".to_string(), "key-a".to_string()])
            .body_slice(b"world")
            .build_unchecked();

        assert!(accumulation.add(first, None, usize::MAX, 30_000).unwrap().is_some());
        assert!(accumulation.add(second, None, usize::MAX, 30_000).unwrap().is_some());

        let messages = std::mem::take(&mut accumulation.messages);
        let batch = build_message_batch(messages, &accumulation.aggregate_key, &accumulation.keys).unwrap();

        assert_eq!(batch.topic().as_str(), "test-topic");
        assert!(batch.is_wait_store_msg_ok());
        assert_eq!(batch.tags().as_deref(), Some("TagA"));
        assert_eq!(batch.get_keys().as_deref(), Some("key-a key-b key-c"));
    }

    #[test]
    fn batch_config_setters_match_java_validation() {
        let mut accumulator = ProduceAccumulator::new("test");

        accumulator.set_batch_max_delay_ms(1).unwrap();
        accumulator.set_batch_max_bytes(1).unwrap();
        accumulator.set_total_batch_max_bytes(1).unwrap();

        assert_eq!(accumulator.batch_max_delay_ms(), 1);
        assert_eq!(accumulator.batch_max_bytes(), 1);
        assert_eq!(accumulator.total_batch_max_bytes(), 1);
        assert!(accumulator.set_batch_max_delay_ms(0).is_err());
        assert!(accumulator.set_batch_max_delay_ms(30_001).is_err());
        assert!(accumulator.set_batch_max_bytes(0).is_err());
        assert!(accumulator.set_batch_max_bytes(2 * 1024 * 1024 + 1).is_err());
        assert!(accumulator.set_total_batch_max_bytes(0).is_err());
    }

    #[test]
    fn start_without_tokio_runtime_does_not_spawn_panic() {
        let mut accumulator = ProduceAccumulator::new("accumulator-no-runtime-test");

        accumulator.start();
        assert!(accumulator.guard_thread_for_sync_send.task_handle.is_some());
        assert!(accumulator.guard_thread_for_async_send.task_handle.is_some());

        accumulator.shutdown();
        assert!(accumulator.guard_thread_for_sync_send.task_handle.is_none());
        assert!(accumulator.guard_thread_for_async_send.task_handle.is_none());
        std::thread::sleep(Duration::from_millis(20));
    }

    #[tokio::test]
    async fn shutdown_async_stops_guard_tasks() {
        let mut accumulator = ProduceAccumulator::new("accumulator-async-shutdown-test");

        accumulator.start();
        assert!(accumulator.guard_thread_for_sync_send.task_handle.is_some());
        assert!(accumulator.guard_thread_for_async_send.task_handle.is_some());

        accumulator.shutdown_async().await;

        assert!(accumulator.guard_thread_for_sync_send.task_handle.is_none());
        assert!(accumulator.guard_thread_for_async_send.task_handle.is_none());
    }

    #[tokio::test]
    async fn shutdown_async_releases_pending_async_batch_hold_size_and_callbacks() {
        let accumulator = ProduceAccumulator::new("accumulator-shutdown-release-test");
        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let mut accumulation =
            MessageAccumulation::new(aggregate_key.clone(), ArcMut::new(DefaultMQProducer::default()));
        let callback_invoked = Arc::new(AtomicBool::new(false));
        let callback_invoked_for_callback = callback_invoked.clone();
        let callback: ArcSendCallback = Arc::new(
            move |_result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
                assert!(error.is_some());
                callback_invoked_for_callback.store(true, Ordering::Release);
            },
        );
        let message = Message::builder()
            .topic("test-topic")
            .body_slice(b"hello")
            .build_unchecked();
        assert!(accumulation
            .add(message, Some(callback), usize::MAX, 30_000)
            .unwrap()
            .is_some());
        accumulator.currently_hold_size.store(5, Ordering::Release);
        accumulator
            .async_send_batchs
            .insert(aggregate_key, Arc::new(Mutex::new(accumulation)));

        accumulator.release_pending_batches_on_shutdown_async().await;

        assert_eq!(accumulator.currently_hold_size.load(Ordering::Acquire), 0);
        assert!(callback_invoked.load(Ordering::Acquire));
        assert!(accumulator.async_send_batchs.is_empty());
    }

    #[tokio::test]
    async fn sync_guard_notifies_batch_at_deadline() {
        let mut accumulator = ProduceAccumulator::new("accumulator-sync-deadline-test");
        accumulator.set_batch_max_delay_ms(100).unwrap();
        accumulator.start();

        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let batch = accumulator
            .get_or_create_sync_send_batch(aggregate_key, &DefaultMQProducer::default())
            .await;
        let notify = {
            let mut batch_guard = batch.lock().await;
            let message = Message::builder()
                .topic("test-topic")
                .body_slice(b"hello")
                .build_unchecked();
            assert!(batch_guard.add(message, None, usize::MAX, 100).unwrap().is_some());
            batch_guard.completion_notify.clone()
        };

        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("sync guard should notify when the batch deadline expires");

        let metrics = accumulator.guard_metrics_snapshot();
        assert!(metrics.sync.wakeup_count >= 1);
        assert_eq!(metrics.sync.flush_count, 1);

        accumulator.shutdown_async().await;
    }

    #[tokio::test]
    async fn guard_deadline_queue_lazily_ignores_removed_batch() {
        let mut accumulator = ProduceAccumulator::new("accumulator-stale-deadline-test");
        accumulator.set_batch_max_delay_ms(30).unwrap();
        accumulator.start();

        let aggregate_key = AggregateKey::new(CheetahString::from("test-topic"), None, true, None);
        let batch = accumulator
            .get_or_create_sync_send_batch(aggregate_key.clone(), &DefaultMQProducer::default())
            .await;
        {
            let mut batch_guard = batch.lock().await;
            let message = Message::builder()
                .topic("test-topic")
                .body_slice(b"hello")
                .build_unchecked();
            assert!(batch_guard.add(message, None, usize::MAX, 30).unwrap().is_some());
        }
        accumulator.sync_send_batchs.remove(&aggregate_key);

        tokio::time::sleep(Duration::from_millis(150)).await;

        let metrics = accumulator.guard_metrics_snapshot();
        assert!(metrics.sync.wakeup_count >= 1);
        assert_eq!(metrics.sync.flush_count, 0);
        assert!(metrics.sync.idle_wakeup_count >= 1);

        accumulator.shutdown_async().await;
    }

    #[tokio::test]
    async fn produce_accumulator_guard_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_produce_accumulator_guard_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 2, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
    }

    #[tokio::test]
    async fn guard_task_shutdown_waits_for_worker_completion() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();
        let task = spawn_guard_task("rocketmq-client-batch-guard-test", shutdown_tx, async move {
            let _ = shutdown_rx.changed().await;
            completed_in_task.store(true, Ordering::Release);
        })
        .expect("test task should spawn");

        assert!(task.shutdown_async(Duration::from_secs(1)).await);
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn guard_task_shutdown_aborts_after_timeout() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let task = spawn_guard_task("rocketmq-client-batch-guard-test", shutdown_tx, async move {
            let _drop_flag = DropFlag(dropped_in_task);
            pending::<()>().await;
        })
        .expect("test task should spawn");

        assert!(!task.shutdown_async(Duration::from_millis(20)).await);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn split_send_results_matches_java_batch_success_shape() {
        let send_result = SendResult::new(
            crate::producer::send_status::SendStatus::SendOk,
            Some(CheetahString::from("msg-a,msg-b")),
            Some("offset-a,offset-b".to_string()),
            None,
            17,
        );

        let split = split_send_results(&send_result, 2).expect("valid per-message batch result");

        assert_eq!(split.len(), 2);
        assert_eq!(split[0].msg_id.as_ref().unwrap().as_str(), "msg-a");
        assert_eq!(split[0].offset_msg_id.as_deref(), Some("offset-a"));
        assert_eq!(split[0].queue_offset, 17);
        assert_eq!(split[1].msg_id.as_ref().unwrap().as_str(), "msg-b");
        assert_eq!(split[1].offset_msg_id.as_deref(), Some("offset-b"));
        assert_eq!(split[1].queue_offset, 18);
    }

    #[test]
    fn split_send_results_keeps_original_when_ids_are_not_per_message_like_java() {
        let send_result = SendResult::new(
            crate::producer::send_status::SendStatus::SendOk,
            Some(CheetahString::from("single-msg-id")),
            Some("offset-a,offset-b".to_string()),
            None,
            17,
        );

        let split = split_send_results(&send_result, 2).expect("single msg id keeps Java batch-queue result");

        assert_eq!(split.len(), 2);
        assert_eq!(split[0].msg_id.as_ref().unwrap().as_str(), "single-msg-id");
        assert_eq!(split[0].offset_msg_id.as_deref(), Some("offset-a,offset-b"));
        assert_eq!(split[0].queue_offset, 17);
        assert_eq!(split[1].msg_id.as_ref().unwrap().as_str(), "single-msg-id");
        assert_eq!(split[1].offset_msg_id.as_deref(), Some("offset-a,offset-b"));
        assert_eq!(split[1].queue_offset, 17);
    }

    #[test]
    fn split_send_results_rejects_mismatched_comma_msg_ids_like_java() {
        let send_result = SendResult::new(
            crate::producer::send_status::SendStatus::SendOk,
            Some(CheetahString::from("msg-a,msg-b")),
            Some("offset-a".to_string()),
            None,
            17,
        );

        let error = split_send_results(&send_result, 2).expect_err("mismatched per-message ids are illegal");

        assert!(error.to_string().contains("sendResult is illegal"));
    }

    #[test]
    fn split_send_results_rejects_missing_offset_msg_id_for_comma_msg_ids_like_java() {
        let send_result = SendResult::new(
            crate::producer::send_status::SendStatus::SendOk,
            Some(CheetahString::from("msg-a,msg-b")),
            None,
            None,
            17,
        );

        let error = split_send_results(&send_result, 2).expect_err("per-message msg id requires offset msg id");

        assert!(error.to_string().contains("sendResult is illegal"));
    }

    #[test]
    fn split_send_results_rejects_missing_msg_id_with_typed_error() {
        let send_result = SendResult::new(
            crate::producer::send_status::SendStatus::SendOk,
            None,
            Some("offset-a,offset-b".to_string()),
            None,
            17,
        );

        let error = split_send_results(&send_result, 2).expect_err("missing msg id is illegal");

        assert!(error.to_string().contains("sendResult is illegal"));
    }
}

#[derive(Clone, Debug, Default)]
pub struct AggregateKey {
    pub topic: CheetahString,
    pub mq: Option<MessageQueue>,
    pub wait_store_msg_ok: bool,
    pub tag: Option<CheetahString>,
}

impl AggregateKey {
    pub fn new_from_message<M: MessageTrait>(message: &M) -> Self {
        Self {
            topic: message.topic().clone(),
            mq: None,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.tags(),
        }
    }

    pub fn new_from_message_queue<M: MessageTrait>(message: &M, mq: Option<MessageQueue>) -> Self {
        Self {
            topic: message.topic().clone(),
            mq,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.tags(),
        }
    }

    pub fn new(
        topic: CheetahString,
        mq: Option<MessageQueue>,
        wait_store_msg_ok: bool,
        tag: Option<CheetahString>,
    ) -> Self {
        Self {
            topic,
            mq,
            wait_store_msg_ok,
            tag,
        }
    }
}

impl PartialEq for AggregateKey {
    fn eq(&self, other: &Self) -> bool {
        self.wait_store_msg_ok == other.wait_store_msg_ok
            && self.topic == other.topic
            && self.mq == other.mq
            && self.tag == other.tag
    }
}

impl Eq for AggregateKey {}

impl Hash for AggregateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.mq.hash(state);
        self.wait_store_msg_ok.hash(state);
        self.tag.hash(state);
    }
}

struct MessageAccumulation {
    default_mq_producer: ArcMut<DefaultMQProducer>,
    messages: Vec<Box<dyn MessageTrait + Send + Sync + 'static>>,
    send_callbacks: Vec<ArcSendCallback>,
    keys: HashSet<String>,
    state: AtomicU8,
    send_results: Option<Vec<SendResult>>, // Stores results for sync send
    send_error: Option<String>,
    aggregate_key: AggregateKey,
    messages_size: Arc<AtomicI32>,
    count: usize,
    create_time: u64,
    completion_notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct AddOutcome {
    index: usize,
    should_flush: bool,
    notify: Arc<tokio::sync::Notify>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchState {
    Open = 0,
    Closing = 1,
    Closed = 2,
}

impl BatchState {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Open,
            1 => Self::Closing,
            _ => Self::Closed,
        }
    }
}

#[derive(Clone)]
struct GuardScheduleCommand {
    aggregate_key: AggregateKey,
    create_time: u64,
    deadline_ms: u64,
}

#[derive(Clone, Eq)]
struct GuardDeadline {
    aggregate_key: AggregateKey,
    create_time: u64,
    deadline_ms: u64,
    sequence: u64,
}

impl PartialEq for GuardDeadline {
    fn eq(&self, other: &Self) -> bool {
        self.deadline_ms == other.deadline_ms
            && self.sequence == other.sequence
            && self.create_time == other.create_time
            && self.aggregate_key == other.aggregate_key
    }
}

impl Ord for GuardDeadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .deadline_ms
            .cmp(&self.deadline_ms)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for GuardDeadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl MessageAccumulation {
    pub fn new(aggregate_key: AggregateKey, default_mq_producer: ArcMut<DefaultMQProducer>) -> Self {
        Self {
            default_mq_producer,
            messages: vec![],
            send_callbacks: vec![],
            keys: HashSet::new(),
            state: AtomicU8::new(BatchState::Open as u8),
            send_results: None,
            send_error: None,
            aggregate_key,
            messages_size: Arc::new(AtomicI32::new(0)),
            count: 0,
            create_time: current_millis(),
            completion_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn deadline_ms(&self, hold_ms: u64) -> u64 {
        self.create_time.saturating_add(hold_ms)
    }

    fn state(&self) -> BatchState {
        BatchState::from_u8(self.state.load(Ordering::Acquire))
    }

    fn try_mark_closing(&self) -> bool {
        self.state
            .compare_exchange(
                BatchState::Open as u8,
                BatchState::Closing as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn mark_closed(&self) {
        self.state.store(BatchState::Closed as u8, Ordering::Release);
    }

    /// Check if batch is ready to send based on size or time thresholds
    fn ready_to_send(&self, hold_size: usize, hold_ms: u64) -> bool {
        // Condition 1: Size threshold
        let current_size = self.messages_size.load(Ordering::Acquire);
        if current_size > hold_size as i32 {
            return true;
        }

        // Condition 2: Time threshold
        let elapsed = current_millis() - self.create_time;
        if elapsed >= hold_ms {
            return true;
        }

        false
    }

    pub fn add<M: MessageTrait + Send + Sync + 'static>(
        &mut self,
        msg: M,
        send_callback: Option<ArcSendCallback>,
        hold_size: usize,
        hold_ms: u64,
    ) -> rocketmq_error::RocketMQResult<Option<AddOutcome>> {
        // Check if batch is already closed
        if self.state() != BatchState::Open {
            return Ok(None);
        }

        // Calculate message body size
        let body_size = msg.get_body().map(|b| b.len()).unwrap_or(0) as i32;
        if let Some(keys) = msg.get_keys_ref() {
            self.keys.extend(
                keys.as_str()
                    .split(MessageConst::KEY_SEPARATOR)
                    .filter(|key| !key.is_empty())
                    .map(ToOwned::to_owned),
            );
        }

        // Add message to batch
        self.messages.push(Box::new(msg));

        // Add callback if provided
        if let Some(callback) = send_callback {
            self.send_callbacks.push(callback);
        }

        // Update message size counter
        if body_size > 0 {
            self.messages_size.fetch_add(body_size, Ordering::AcqRel);
        }

        // Increment count
        let index = self.count;
        self.count += 1;

        Ok(Some(AddOutcome {
            index,
            should_flush: self.ready_to_send(hold_size, hold_ms),
            notify: self.completion_notify.clone(),
        }))
    }
}

#[derive(Default)]
struct GuardForSyncSendService {
    service_name: String,
    stopped: Arc<AtomicBool>,
    task_handle: Option<GuardTaskHandle>,
    schedule_tx: Option<mpsc::UnboundedSender<GuardScheduleCommand>>,
    metrics: Arc<BatchGuardMetrics>,
}

enum GuardTaskHandle {
    Tracked {
        shutdown_tx: watch::Sender<bool>,
        handle: ClientTrackedTaskHandle,
    },
}

impl GuardTaskHandle {
    fn is_finished(&self) -> bool {
        match self {
            Self::Tracked { handle, .. } => handle.task_count() == 0,
        }
    }

    async fn shutdown_async(self, timeout: Duration) -> bool {
        match self {
            Self::Tracked { shutdown_tx, handle } => {
                let _ = shutdown_tx.send(true);
                let report = handle.shutdown(timeout).await;
                if !report.is_healthy() {
                    tracing::warn!(report = %report.to_json(), "batch guard task shutdown report is unhealthy");
                }
                report.is_healthy()
            }
        }
    }

    fn shutdown_blocking(self, timeout: Duration) -> bool {
        match self {
            Self::Tracked { shutdown_tx, handle } => {
                let _ = shutdown_tx.send(true);
                let (report, completed) = handle.shutdown_blocking(timeout);
                if !completed || !report.is_healthy() {
                    tracing::warn!(
                        completed_before_timeout = completed,
                        report = %report.to_json(),
                        "batch guard task blocking shutdown report is unhealthy"
                    );
                }
                completed && report.is_healthy()
            }
        }
    }

    fn task_count(&self) -> usize {
        match self {
            Self::Tracked { handle, .. } => handle.task_count(),
        }
    }
}

fn spawn_guard_task<F>(thread_name: &'static str, shutdown_tx: watch::Sender<bool>, task: F) -> Option<GuardTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    match spawn_client_tracked_task(thread_name, task) {
        Ok(handle) => Some(GuardTaskHandle::Tracked { shutdown_tx, handle }),
        Err(error) => {
            tracing::error!("Failed to spawn {} background task: {}", thread_name, error);
            None
        }
    }
}

enum GuardWakeEvent {
    Deadline,
    Schedule(GuardScheduleCommand),
    Shutdown,
}

fn push_guard_deadline(deadlines: &mut BinaryHeap<GuardDeadline>, command: GuardScheduleCommand, sequence: &mut u64) {
    deadlines.push(GuardDeadline {
        aggregate_key: command.aggregate_key,
        create_time: command.create_time,
        deadline_ms: command.deadline_ms,
        sequence: *sequence,
    });
    *sequence = sequence.wrapping_add(1);
}

async fn wait_guard_deadline_or_schedule(
    shutdown_rx: &mut watch::Receiver<bool>,
    schedule_rx: &mut mpsc::UnboundedReceiver<GuardScheduleCommand>,
    next_deadline_ms: Option<u64>,
) -> GuardWakeEvent {
    if *shutdown_rx.borrow() {
        return GuardWakeEvent::Shutdown;
    }

    if let Some(deadline_ms) = next_deadline_ms {
        let delay = Duration::from_millis(deadline_ms.saturating_sub(current_millis()));
        if delay.is_zero() {
            return GuardWakeEvent::Deadline;
        }

        tokio::select! {
            _ = tokio::time::sleep(delay) => GuardWakeEvent::Deadline,
            command = schedule_rx.recv() => {
                command.map_or(GuardWakeEvent::Shutdown, GuardWakeEvent::Schedule)
            }
            _ = shutdown_rx.changed() => GuardWakeEvent::Shutdown,
        }
    } else {
        tokio::select! {
            command = schedule_rx.recv() => {
                command.map_or(GuardWakeEvent::Shutdown, GuardWakeEvent::Schedule)
            }
            _ = shutdown_rx.changed() => GuardWakeEvent::Shutdown,
        }
    }
}

fn remove_batch_if_same(
    batches: &DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>,
    key: &AggregateKey,
    expected: &Arc<Mutex<MessageAccumulation>>,
) -> Option<Arc<Mutex<MessageAccumulation>>> {
    let should_remove = batches
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current.value(), expected));
    if should_remove {
        batches.remove(key).map(|(_, batch)| batch)
    } else {
        None
    }
}

async fn drain_due_sync_batches(
    deadlines: &mut BinaryHeap<GuardDeadline>,
    batches: &DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>,
    hold_ms: u64,
    metrics: &BatchGuardMetrics,
) {
    let now = current_millis();
    while deadlines.peek().is_some_and(|deadline| deadline.deadline_ms <= now) {
        let deadline = deadlines.pop().expect("deadline should exist after peek");
        let Some(batch) = batches.get(&deadline.aggregate_key).map(|entry| entry.value().clone()) else {
            metrics.record_idle();
            continue;
        };

        let mut remove_empty = false;
        let mut notify = None;
        {
            let batch_guard = batch.lock().await;
            if batch_guard.create_time != deadline.create_time || batch_guard.state() != BatchState::Open {
                metrics.record_idle();
                continue;
            }

            if batch_guard.messages_size.load(Ordering::Acquire) == 0 {
                batch_guard.mark_closed();
                remove_empty = true;
            } else if batch_guard.deadline_ms(hold_ms) <= now {
                notify = Some(batch_guard.completion_notify.clone());
            } else {
                metrics.record_idle();
            }
        }

        if remove_empty {
            remove_batch_if_same(batches, &deadline.aggregate_key, &batch);
            metrics.record_idle();
        } else if let Some(notify) = notify {
            metrics.record_flush();
            notify.notify_waiters();
        }
    }
}

async fn drain_due_async_batches(
    deadlines: &mut BinaryHeap<GuardDeadline>,
    batches: &DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>,
    currently_hold_size: &Arc<AtomicU64>,
    hold_size: usize,
    hold_ms: u64,
    metrics: &BatchGuardMetrics,
) {
    let now = current_millis();
    while deadlines.peek().is_some_and(|deadline| deadline.deadline_ms <= now) {
        let deadline = deadlines.pop().expect("deadline should exist after peek");
        let Some(batch) = batches.get(&deadline.aggregate_key).map(|entry| entry.value().clone()) else {
            metrics.record_idle();
            continue;
        };

        let mut remove_empty = false;
        let mut should_send = false;
        {
            let batch_guard = batch.lock().await;
            if batch_guard.create_time != deadline.create_time || batch_guard.state() != BatchState::Open {
                metrics.record_idle();
                continue;
            }

            if batch_guard.messages_size.load(Ordering::Acquire) == 0 {
                batch_guard.mark_closed();
                remove_empty = true;
            } else if batch_guard.ready_to_send(hold_size, hold_ms) {
                should_send = true;
            } else {
                metrics.record_idle();
            }
        }

        if remove_empty {
            remove_batch_if_same(batches, &deadline.aggregate_key, &batch);
            metrics.record_idle();
            continue;
        }

        if should_send {
            if let Some(batch) = remove_batch_if_same(batches, &deadline.aggregate_key, &batch) {
                metrics.record_flush();
                if let Err(error) =
                    GuardForAsyncSendService::send_batch_async_internal(batch, currently_hold_size.clone()).await
                {
                    tracing::error!("Failed to send batch via guard thread: {:?}", error);
                }
            } else {
                metrics.record_idle();
            }
        }
    }
}

impl GuardForSyncSendService {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            stopped: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            schedule_tx: None,
            metrics: Arc::new(BatchGuardMetrics::default()),
        }
    }

    pub fn start(&mut self, batches: BatchMap, hold_ms: u32) {
        if self.task_handle.as_ref().is_some_and(|handle| !handle.is_finished()) {
            tracing::warn!("{} sync batch guard already started", self.service_name);
            return;
        }

        let service_name = self.service_name.clone();
        let stopped = self.stopped.clone();
        let metrics = self.metrics.clone();
        let (schedule_tx, mut schedule_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        self.schedule_tx = Some(schedule_tx);
        self.stopped.store(false, Ordering::Release);

        self.task_handle = spawn_guard_task("rocketmq-client-sync-batch-guard", shutdown_tx, async move {
            tracing::info!("{} service started", service_name);

            let mut deadlines = BinaryHeap::new();
            let mut sequence = 0u64;

            loop {
                if stopped.load(Ordering::Acquire) {
                    break;
                }

                let next_deadline_ms = deadlines.peek().map(|deadline: &GuardDeadline| deadline.deadline_ms);
                match wait_guard_deadline_or_schedule(&mut shutdown_rx, &mut schedule_rx, next_deadline_ms).await {
                    GuardWakeEvent::Schedule(command) => {
                        push_guard_deadline(&mut deadlines, command, &mut sequence);
                    }
                    GuardWakeEvent::Deadline => {
                        metrics.record_wakeup();
                        drain_due_sync_batches(&mut deadlines, &batches, hold_ms as u64, &metrics).await;
                    }
                    GuardWakeEvent::Shutdown => break,
                }
            }

            tracing::info!("{} service ended", service_name);
        });
    }

    fn schedule_batch(&self, aggregate_key: AggregateKey, create_time: u64, deadline_ms: u64) {
        if let Some(schedule_tx) = &self.schedule_tx {
            let _ = schedule_tx.send(GuardScheduleCommand {
                aggregate_key,
                create_time,
                deadline_ms,
            });
        }
    }

    fn metrics_snapshot(&self) -> BatchGuardMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn shutdown(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.schedule_tx = None;
        if let Some(handle) = self.task_handle.take() {
            if !handle.shutdown_blocking(GUARD_TASK_SHUTDOWN_TIMEOUT) {
                tracing::warn!(
                    "{} sync batch guard did not stop before timeout; aborted",
                    self.service_name
                );
            }
        }
    }

    pub async fn shutdown_async(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.schedule_tx = None;
        if let Some(handle) = self.task_handle.take() {
            if !handle.shutdown_async(GUARD_TASK_SHUTDOWN_TIMEOUT).await {
                tracing::warn!(
                    "{} sync batch guard did not stop before timeout; aborted",
                    self.service_name
                );
            }
        }
    }

    fn task_count(&self) -> usize {
        self.task_handle
            .as_ref()
            .map(GuardTaskHandle::task_count)
            .unwrap_or_default()
    }
}

#[derive(Default)]
struct GuardForAsyncSendService {
    service_name: String,
    stopped: Arc<AtomicBool>,
    task_handle: Option<GuardTaskHandle>,
    schedule_tx: Option<mpsc::UnboundedSender<GuardScheduleCommand>>,
    metrics: Arc<BatchGuardMetrics>,
}

impl GuardForAsyncSendService {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            stopped: Arc::new(AtomicBool::new(false)),
            task_handle: None,
            schedule_tx: None,
            metrics: Arc::new(BatchGuardMetrics::default()),
        }
    }

    pub fn start(&mut self, batches: BatchMap, currently_hold_size: Arc<AtomicU64>, hold_size: usize, hold_ms: u32) {
        if self.task_handle.as_ref().is_some_and(|handle| !handle.is_finished()) {
            tracing::warn!("{} async batch guard already started", self.service_name);
            return;
        }

        let service_name = self.service_name.clone();
        let stopped = self.stopped.clone();
        let metrics = self.metrics.clone();
        let (schedule_tx, mut schedule_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        self.schedule_tx = Some(schedule_tx);
        self.stopped.store(false, Ordering::Release);

        self.task_handle = spawn_guard_task("rocketmq-client-async-batch-guard", shutdown_tx, async move {
            tracing::info!("{} service started", service_name);

            let mut deadlines = BinaryHeap::new();
            let mut sequence = 0u64;

            loop {
                if stopped.load(Ordering::Acquire) {
                    break;
                }

                let next_deadline_ms = deadlines.peek().map(|deadline: &GuardDeadline| deadline.deadline_ms);
                match wait_guard_deadline_or_schedule(&mut shutdown_rx, &mut schedule_rx, next_deadline_ms).await {
                    GuardWakeEvent::Schedule(command) => {
                        push_guard_deadline(&mut deadlines, command, &mut sequence);
                    }
                    GuardWakeEvent::Deadline => {
                        metrics.record_wakeup();
                        drain_due_async_batches(
                            &mut deadlines,
                            &batches,
                            &currently_hold_size,
                            hold_size,
                            hold_ms as u64,
                            &metrics,
                        )
                        .await;
                    }
                    GuardWakeEvent::Shutdown => break,
                }
            }

            tracing::info!("{} service ended", service_name);
        });
    }

    fn schedule_batch(&self, aggregate_key: AggregateKey, create_time: u64, deadline_ms: u64) {
        if let Some(schedule_tx) = &self.schedule_tx {
            let _ = schedule_tx.send(GuardScheduleCommand {
                aggregate_key,
                create_time,
                deadline_ms,
            });
        }
    }

    fn metrics_snapshot(&self) -> BatchGuardMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Internal method to send batch (used by guard thread)
    async fn send_batch_async_internal(
        batch: Arc<Mutex<MessageAccumulation>>,
        currently_hold_size: Arc<AtomicU64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        // Extract all data from the batch without holding the lock across await
        let (messages, mq, mut producer, total_size, callbacks, aggregate_key, keys, notify) = {
            let mut batch_guard = batch.lock().await;
            if !batch_guard.try_mark_closing() {
                return Ok(());
            }
            let notify = batch_guard.completion_notify.clone();

            if batch_guard.messages.is_empty() {
                let error = crate::mq_client_err!("No messages to send");
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                return Err(error);
            }

            let total_size = batch_guard.messages_size.load(Ordering::Acquire) as u64;
            let messages = std::mem::take(&mut batch_guard.messages);
            let callbacks = std::mem::take(&mut batch_guard.send_callbacks);
            let mq = batch_guard.aggregate_key.mq.clone();
            let producer = batch_guard.default_mq_producer.clone();
            let aggregate_key = batch_guard.aggregate_key.clone();
            let keys = batch_guard.keys.clone();

            (
                messages,
                mq,
                producer,
                total_size,
                callbacks,
                aggregate_key,
                keys,
                notify,
            )
        }; // Lock released here

        let batch_msg = match build_message_batch(messages, &aggregate_key, &keys) {
            Ok(batch_msg) => batch_msg,
            Err(error) => {
                release_hold_size(&currently_hold_size, total_size);
                let mut batch_guard = batch.lock().await;
                batch_guard.send_error = Some(error.to_string());
                batch_guard.mark_closed();
                notify.notify_waiters();
                for callback in &callbacks {
                    callback.on_exception(&error);
                }
                return Err(error);
            }
        };

        let callback_currently_hold_size = currently_hold_size.clone();
        let callbacks = Arc::new(callbacks);
        let callbacks_for_send_error = callbacks.clone();

        // Create combined callback
        let combined_callback = move |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
            release_hold_size(&callback_currently_hold_size, total_size);
            if let Some(result) = result {
                match split_send_results(result, callbacks.len()) {
                    Ok(send_results) => {
                        for (callback, result) in callbacks.iter().zip(send_results.iter()) {
                            callback.on_success(result);
                        }
                    }
                    Err(error) => {
                        for callback in callbacks.iter() {
                            callback.on_exception(&error);
                        }
                    }
                }
            } else if let Some(error) = error {
                for callback in callbacks.iter() {
                    callback.on_exception(error);
                }
            }
        };

        // Send without holding any locks
        let send_result = producer
            .send_direct(batch_msg, mq, Some(Arc::new(combined_callback)))
            .await;

        if let Err(error) = &send_result {
            release_hold_size(&currently_hold_size, total_size);
            let mut batch_guard = batch.lock().await;
            batch_guard.send_error = Some(error.to_string());
            batch_guard.mark_closed();
            notify.notify_waiters();
            for callback in callbacks_for_send_error.iter() {
                callback.on_exception(error);
            }
        } else {
            let batch_guard = batch.lock().await;
            batch_guard.mark_closed();
            notify.notify_waiters();
        }

        send_result.map(|_| ())
    }

    pub fn shutdown(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.schedule_tx = None;
        if let Some(handle) = self.task_handle.take() {
            if !handle.shutdown_blocking(GUARD_TASK_SHUTDOWN_TIMEOUT) {
                tracing::warn!(
                    "{} async batch guard did not stop before timeout; aborted",
                    self.service_name
                );
            }
        }
    }

    pub async fn shutdown_async(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.schedule_tx = None;
        if let Some(handle) = self.task_handle.take() {
            if !handle.shutdown_async(GUARD_TASK_SHUTDOWN_TIMEOUT).await {
                tracing::warn!(
                    "{} async batch guard did not stop before timeout; aborted",
                    self.service_name
                );
            }
        }
    }

    fn task_count(&self) -> usize {
        self.task_handle
            .as_ref()
            .map(GuardTaskHandle::task_count)
            .unwrap_or_default()
    }
}
