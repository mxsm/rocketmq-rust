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

use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;

use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;

#[derive(Default)]
pub struct ProduceAccumulator {
    total_hold_size: usize,
    hold_size: usize,
    hold_ms: u32,
    guard_thread_for_sync_send: GuardForSyncSendService,
    guard_thread_for_async_send: GuardForAsyncSendService,
    currently_hold_size: Arc<AtomicU64>,
    instance_name: String,
    currently_hold_size_lock: Arc<parking_lot::Mutex<()>>,
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
            guard_thread_for_async_send: GuardForAsyncSendService {
                service_name: instance_name.to_string(),
                stopped: Arc::new(AtomicBool::new(false)),
                task_handle: None,
            },
            guard_thread_for_sync_send: GuardForSyncSendService {
                service_name: instance_name.to_string(),
                stopped: Arc::new(AtomicBool::new(false)),
                task_handle: None,
            },
            ..Default::default()
        }
    }
}

impl ProduceAccumulator {
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
    }

    pub(crate) fn try_add_message<T: MessageTrait>(&self, message: &T) -> bool {
        let lock = self.currently_hold_size_lock.lock();
        if self.currently_hold_size.load(Ordering::Acquire) as usize > self.total_hold_size {
            drop(lock);
            return false;
        }
        self.currently_hold_size
            .fetch_add(message.get_body().unwrap().len() as u64, Ordering::AcqRel);
        drop(lock);
        true
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

        // Lock the batch for exclusive access and add message
        let add_result = {
            let mut batch_guard = batch.lock().await;
            batch_guard.add(message, None)?
        }; // batch_guard dropped here

        // Check if add failed (batch closed)
        if !add_result {
            // Batch is closed, cannot retry because message is already consumed
            self.sync_send_batchs.remove(&partition_key);
            return Err(crate::mq_client_err!("Batch is closed, cannot add message"));
        }

        // Get the message index (count - 1 at the time of add)
        let msg_index = {
            let batch_guard = batch.lock().await;
            batch_guard.count - 1
        };

        // Wait for batch to be ready and sent
        loop {
            // Check if batch is closed (sent)
            let (is_closed, should_send, notify) = {
                let batch_guard = batch.lock().await;
                let is_closed = batch_guard.closed.load(Ordering::Acquire);
                let should_send = batch_guard.ready_to_send(self.hold_size, self.hold_ms as u64);
                let notify = batch_guard.completion_notify.clone();
                (is_closed, should_send, notify)
            };

            if is_closed {
                // Batch has been sent, get result
                let result = {
                    let batch_guard = batch.lock().await;
                    batch_guard
                        .send_results
                        .as_ref()
                        .and_then(|results| results.get(msg_index).cloned())
                };

                return Ok(result);
            }

            if should_send {
                // Try to remove and send the batch
                let batch_to_send = self.sync_send_batchs.remove(&partition_key).map(|(_, v)| v);

                if let Some(batch_arc) = batch_to_send {
                    // Send the batch (without holding the lock)
                    self.send_batch_sync(batch_arc).await?;
                    // Continue to wait loop to get result
                }
            } else {
                // Wait for notification instead of polling
                notify.notified().await;
            }
        }
    }

    pub(crate) async fn send_callback<M>(
        &mut self,
        message: M,
        mq: Option<MessageQueue>,
        send_callback: Option<SendMessageCallback>,
        default_mq_producer: DefaultMQProducer,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync + 'static,
    {
        let partition_key = AggregateKey::new_from_message_queue(&message, mq);

        let batch = self
            .get_or_create_async_send_batch(partition_key.clone(), &default_mq_producer)
            .await;

        // Lock the batch for exclusive access
        let add_result = {
            let mut batch_guard = batch.lock().await;
            batch_guard.add(message, send_callback)?
        }; // batch_guard dropped here

        // Try to add message to batch
        match add_result {
            true => {
                // Message added successfully, check if ready to send
                let should_send = {
                    let batch_guard = batch.lock().await;
                    batch_guard.ready_to_send(self.hold_size, self.hold_ms as u64)
                };

                if should_send {
                    // Remove batch from map
                    let batch_to_send = self.async_send_batchs.remove(&partition_key).map(|(_, v)| v);

                    if let Some(batch_arc) = batch_to_send {
                        // Send the batch (without holding the lock)
                        self.send_batch_async(batch_arc).await?;
                    }
                }
                Ok(())
            }
            false => {
                // Batch is closed, remove it and return error
                self.async_send_batchs.remove(&partition_key);
                Err(crate::mq_client_err!("Batch is closed, please retry"))
            }
        }
    }

    async fn get_or_create_sync_send_batch(
        &self,
        aggregate_key: AggregateKey,
        default_mq_producer: &DefaultMQProducer,
    ) -> Arc<Mutex<MessageAccumulation>> {
        self.sync_send_batchs
            .entry(aggregate_key.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(MessageAccumulation::new(
                    aggregate_key,
                    ArcMut::new(default_mq_producer.clone()),
                )))
            })
            .clone()
    }

    async fn get_or_create_async_send_batch(
        &self,
        aggregate_key: AggregateKey,
        default_mq_producer: &DefaultMQProducer,
    ) -> Arc<Mutex<MessageAccumulation>> {
        self.async_send_batchs
            .entry(aggregate_key.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(MessageAccumulation::new(
                    aggregate_key,
                    ArcMut::new(default_mq_producer.clone()),
                )))
            })
            .clone()
    }

    /// Send a batch synchronously (extracted to avoid holding lock across await)
    async fn send_batch_sync(&self, batch: Arc<Mutex<MessageAccumulation>>) -> rocketmq_error::RocketMQResult<()> {
        // Extract all data from the batch without holding the lock across await
        let (messages, mq, mut producer, total_size, count, notify) = {
            let mut batch_guard = batch.lock().await;
            batch_guard.closed.store(true, Ordering::Release);

            if batch_guard.messages.is_empty() {
                return Err(crate::mq_client_err!("No messages to send"));
            }

            let total_size = batch_guard.messages_size.load(Ordering::Acquire) as u64;
            let messages = std::mem::take(&mut batch_guard.messages);
            let mq = batch_guard.aggregate_key.mq.clone();
            let producer = batch_guard.default_mq_producer.clone();
            let count = batch_guard.count;
            let notify = batch_guard.completion_notify.clone();

            (messages, mq, producer, total_size, count, notify)
        }; // Lock released here

        // Convert to MessageBatch
        let mut concrete_messages = Vec::new();
        for boxed_msg in messages {
            if let Some(msg) = boxed_msg.as_any().downcast_ref::<Message>() {
                concrete_messages.push(msg.clone());
            } else {
                let mut msg = Message::default();
                msg.set_topic(boxed_msg.get_topic().clone());
                if let Some(body) = boxed_msg.get_body() {
                    msg.set_body(body.clone());
                }
                msg.set_flag(boxed_msg.get_flag());
                msg.set_properties(boxed_msg.get_properties().clone());
                concrete_messages.push(msg);
            }
        }

        let batch_msg =
            rocketmq_common::common::message::message_batch::MessageBatch::generate_from_vec(concrete_messages)?;

        // Send without holding any locks
        let send_result = producer.send_direct(batch_msg, mq, None).await?;

        // Decrement currently_hold_size
        self.currently_hold_size.fetch_sub(total_size, Ordering::AcqRel);

        // Store results in batch for waiting threads to retrieve
        if let Some(result) = send_result {
            let mut batch_guard = batch.lock().await;
            // Split result for each message (Java: splitSendResults)
            let mut results = Vec::new();
            for _ in 0..count {
                results.push(result.clone());
            }
            batch_guard.send_results = Some(results);
        }

        // Notify all waiting threads
        notify.notify_waiters();

        Ok(())
    }

    /// Send a batch asynchronously (extracted to avoid holding lock across await)
    async fn send_batch_async(&self, batch: Arc<Mutex<MessageAccumulation>>) -> rocketmq_error::RocketMQResult<()> {
        // Extract all data from the batch without holding the lock across await
        let (messages, mq, mut producer, total_size, callbacks) = {
            let mut batch_guard = batch.lock().await;
            batch_guard.closed.store(true, Ordering::Release);

            if batch_guard.messages.is_empty() {
                return Err(crate::mq_client_err!("No messages to send"));
            }

            let total_size = batch_guard.messages_size.load(Ordering::Acquire) as u64;
            let messages = std::mem::take(&mut batch_guard.messages);
            let callbacks = std::mem::take(&mut batch_guard.send_callbacks);
            let mq = batch_guard.aggregate_key.mq.clone();
            let producer = batch_guard.default_mq_producer.clone();

            (messages, mq, producer, total_size, callbacks)
        }; // Lock released here

        // Convert to MessageBatch
        let mut concrete_messages = Vec::new();
        for boxed_msg in messages {
            if let Some(msg) = boxed_msg.as_any().downcast_ref::<Message>() {
                concrete_messages.push(msg.clone());
            } else {
                let mut msg = Message::default();
                msg.set_topic(boxed_msg.get_topic().clone());
                if let Some(body) = boxed_msg.get_body() {
                    msg.set_body(body.clone());
                }
                msg.set_flag(boxed_msg.get_flag());
                msg.set_properties(boxed_msg.get_properties().clone());
                concrete_messages.push(msg);
            }
        }

        let batch_msg =
            rocketmq_common::common::message::message_batch::MessageBatch::generate_from_vec(concrete_messages)?;

        // Create combined callback (without accessing currently_hold_size)
        let combined_callback = move |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
            // Invoke all registered callbacks
            for callback in &callbacks {
                callback(result, error);
            }
        };

        // Send without holding any locks
        let send_result = producer
            .send_direct(batch_msg, mq, Some(Arc::new(combined_callback)))
            .await;

        // Always decrement currently_hold_size (even on error, like Java impl)
        self.currently_hold_size.fetch_sub(total_size, Ordering::AcqRel);

        send_result.map(|_| ())
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
            topic: message.get_topic().clone(),
            mq: None,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.get_tags(),
        }
    }

    pub fn new_from_message_queue<M: MessageTrait>(message: &M, mq: Option<MessageQueue>) -> Self {
        Self {
            topic: message.get_topic().clone(),
            mq,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.get_tags(),
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
    send_callbacks: Vec<SendMessageCallback>,
    keys: HashSet<String>,
    closed: Arc<AtomicBool>,
    send_results: Option<Vec<SendResult>>, // Stores results for sync send
    aggregate_key: AggregateKey,
    messages_size: Arc<AtomicI32>,
    count: usize,
    create_time: u64,
    completion_notify: Arc<tokio::sync::Notify>,
}

impl MessageAccumulation {
    pub fn new(aggregate_key: AggregateKey, default_mq_producer: ArcMut<DefaultMQProducer>) -> Self {
        Self {
            default_mq_producer,
            messages: vec![],
            send_callbacks: vec![],
            keys: HashSet::new(),
            closed: Arc::new(AtomicBool::new(false)),
            send_results: None,
            aggregate_key,
            messages_size: Arc::new(AtomicI32::new(0)),
            count: 0,
            create_time: get_current_millis(),
            completion_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Check if batch is ready to send based on size or time thresholds
    fn ready_to_send(&self, hold_size: usize, hold_ms: u64) -> bool {
        // Condition 1: Size threshold
        let current_size = self.messages_size.load(Ordering::Acquire);
        if current_size >= hold_size as i32 {
            return true;
        }

        // Condition 2: Time threshold
        let elapsed = get_current_millis() - self.create_time;
        if elapsed >= hold_ms {
            return true;
        }

        false
    }

    pub fn add<M: MessageTrait + Send + Sync + 'static>(
        &mut self,
        msg: M,
        send_callback: Option<SendMessageCallback>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        // Check if batch is already closed
        if self.closed.load(Ordering::Acquire) {
            return Ok(false);
        }

        // Calculate message body size
        let body_size = msg.get_body().map(|b| b.len()).unwrap_or(0) as i32;

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
        self.count += 1;

        Ok(true)
    }
}

#[derive(Default)]
struct GuardForSyncSendService {
    service_name: String,
    stopped: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GuardForSyncSendService {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            stopped: Arc::new(AtomicBool::new(false)),
            task_handle: None,
        }
    }

    pub fn start(&mut self, batches: Arc<DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>>, hold_ms: u32) {
        let service_name = self.service_name.clone();
        let stopped = self.stopped.clone();
        let sleep_time = std::cmp::max(1, hold_ms / 2) as u64;

        let task_handle = tokio::spawn(async move {
            tracing::info!("{} service started", service_name);

            let mut interval = tokio::time::interval(Duration::from_millis(sleep_time));

            while !stopped.load(Ordering::Acquire) {
                interval.tick().await;

                // Process batches - DashMap provides concurrent iteration
                // Collect empty batches to remove
                let mut to_remove = Vec::new();
                for item in batches.iter() {
                    let key = item.key();
                    let batch = item.value();
                    let batch_guard = batch.lock().await;
                    let messages_size = batch_guard.messages_size.load(Ordering::Acquire);
                    if messages_size == 0 {
                        batch_guard.closed.store(true, Ordering::Release);
                        to_remove.push(key.clone());
                    }
                }

                // Remove empty batches
                for key in to_remove {
                    batches.remove(&key);
                }
            }

            tracing::info!("{} service ended", service_name);
        });

        self.task_handle = Some(task_handle);
    }

    pub fn shutdown(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}

#[derive(Default)]
struct GuardForAsyncSendService {
    service_name: String,
    stopped: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl GuardForAsyncSendService {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
            stopped: Arc::new(AtomicBool::new(false)),
            task_handle: None,
        }
    }

    pub fn start(
        &mut self,
        batches: Arc<DashMap<AggregateKey, Arc<Mutex<MessageAccumulation>>>>,
        currently_hold_size: Arc<AtomicU64>,
        hold_size: usize,
        hold_ms: u32,
    ) {
        let service_name = self.service_name.clone();
        let stopped = self.stopped.clone();
        let sleep_time = std::cmp::max(1, hold_ms / 2) as u64;

        let task_handle = tokio::spawn(async move {
            tracing::info!("{} service started", service_name);

            let mut interval = tokio::time::interval(Duration::from_millis(sleep_time));

            while !stopped.load(Ordering::Acquire) {
                interval.tick().await;

                // Collect keys of ready batches (without holding locks during iteration)
                let mut ready_keys = Vec::new();
                for item in batches.iter() {
                    let key = item.key();
                    let batch = item.value();

                    // Quick check without locking first
                    let should_check = {
                        let batch_guard = batch.lock().await;
                        let is_closed = batch_guard.closed.load(Ordering::Acquire);
                        !is_closed && batch_guard.ready_to_send(hold_size, hold_ms as u64)
                    };

                    if should_check {
                        ready_keys.push(key.clone());
                    }
                }

                // Send ready batches
                for key in ready_keys {
                    if let Some((_, batch)) = batches.remove(&key) {
                        // Send the batch asynchronously
                        if let Err(e) = Self::send_batch_async_internal(batch, currently_hold_size.clone()).await {
                            tracing::error!("Failed to send batch via guard thread: {:?}", e);
                        }
                    }
                }

                // Collect empty batches to remove
                let mut empty_keys = Vec::new();
                for item in batches.iter() {
                    let key = item.key();
                    let batch = item.value();

                    let is_empty = {
                        let batch_guard = batch.lock().await;
                        batch_guard.messages_size.load(Ordering::Acquire) == 0
                    };

                    if is_empty {
                        empty_keys.push(key.clone());
                    }
                }

                // Remove empty batches
                for key in empty_keys {
                    if let Some((_, batch)) = batches.remove(&key) {
                        let batch_guard = batch.lock().await;
                        batch_guard.closed.store(true, Ordering::Release);
                    }
                }
            }

            tracing::info!("{} service ended", service_name);
        });

        self.task_handle = Some(task_handle);
    }

    /// Internal method to send batch (used by guard thread)
    async fn send_batch_async_internal(
        batch: Arc<Mutex<MessageAccumulation>>,
        currently_hold_size: Arc<AtomicU64>,
    ) -> rocketmq_error::RocketMQResult<()> {
        // Extract all data from the batch without holding the lock across await
        let (messages, mq, mut producer, total_size, callbacks) = {
            let mut batch_guard = batch.lock().await;
            batch_guard.closed.store(true, Ordering::Release);

            if batch_guard.messages.is_empty() {
                return Err(crate::mq_client_err!("No messages to send"));
            }

            let total_size = batch_guard.messages_size.load(Ordering::Acquire) as u64;
            let messages = std::mem::take(&mut batch_guard.messages);
            let callbacks = std::mem::take(&mut batch_guard.send_callbacks);
            let mq = batch_guard.aggregate_key.mq.clone();
            let producer = batch_guard.default_mq_producer.clone();

            (messages, mq, producer, total_size, callbacks)
        }; // Lock released here

        // Convert to MessageBatch
        let mut concrete_messages = Vec::new();
        for boxed_msg in messages {
            if let Some(msg) = boxed_msg.as_any().downcast_ref::<Message>() {
                concrete_messages.push(msg.clone());
            } else {
                let mut msg = Message::default();
                msg.set_topic(boxed_msg.get_topic().clone());
                if let Some(body) = boxed_msg.get_body() {
                    msg.set_body(body.clone());
                }
                msg.set_flag(boxed_msg.get_flag());
                msg.set_properties(boxed_msg.get_properties().clone());
                concrete_messages.push(msg);
            }
        }

        let batch_msg =
            rocketmq_common::common::message::message_batch::MessageBatch::generate_from_vec(concrete_messages)?;

        // Create combined callback
        let combined_callback = move |result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
            for callback in &callbacks {
                callback(result, error);
            }
        };

        // Send without holding any locks
        let send_result = producer
            .send_direct(batch_msg, mq, Some(Arc::new(combined_callback)))
            .await;

        // Always decrement currently_hold_size (even on error)
        currently_hold_size.fetch_sub(total_size, Ordering::AcqRel);

        send_result.map(|_| ())
    }

    pub fn shutdown(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}
