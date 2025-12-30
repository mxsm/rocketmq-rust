//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::Builder;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
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
    currently_hold_size: AtomicU64,
    instance_name: String,
    currently_hold_size_lock: Arc<parking_lot::Mutex<()>>,
    sync_send_batchs: Arc<Mutex<HashMap<AggregateKey, ArcMut<MessageAccumulation>>>>,
    async_send_batchs: Arc<Mutex<HashMap<AggregateKey, ArcMut<MessageAccumulation>>>>,
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
            },
            guard_thread_for_sync_send: GuardForSyncSendService {
                service_name: instance_name.to_string(),
            },
            ..Default::default()
        }
    }
}

impl ProduceAccumulator {
    pub fn start(&mut self) {
        self.guard_thread_for_sync_send.start();
        self.guard_thread_for_async_send.start();
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
        unimplemented!("send")
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
        loop {
            let batch = self.get_or_create_async_send_batch(partition_key.clone(), &default_mq_producer);
            /*if batch.add(message.clone(), send_callback.clone()) {
                self.async_send_batchs.lock().await.remove(&partition_key);
            } else {
                return Ok(());
            }*/
        }
    }

    fn get_or_create_async_send_batch(
        &mut self,
        aggregate_key: AggregateKey,
        default_mq_producer: &DefaultMQProducer,
    ) -> ArcMut<MessageAccumulation> {
        unimplemented!("getOrCreateAsyncSendBatch")
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
    send_results: Option<Vec<SendResult>>,
    aggregate_key: AggregateKey,
    messages_size: Arc<AtomicI32>,
    count: usize,
    create_time: u64,
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
        }
    }

    pub fn add<M: MessageTrait + Send + Sync + 'static>(
        &mut self,
        msg: M,
        send_callback: Option<SendMessageCallback>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        unimplemented!()
    }

    /*    fn ready_to_send(&self, hold_size: i32, hold_ms: u128) -> bool {
        self.messages_size.load(Ordering::SeqCst) > hold_size
            || SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis()
                >= self.create_time + hold_ms
    }



    pub fn add_with_callback(
        &mut self,
        msg: Message,
        send_callback: SendCallback,
    ) -> rocketmq_error::RocketMQResult<bool, ()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(());
        }
        self.count += 1;
        self.messages.push_back(msg);
        self.send_callbacks.push_back(send_callback);
        self.messages_size
            .fetch_add(msg.get_body().len() as i32, Ordering::SeqCst);

        Ok(true)
    }

    fn batch(&self) -> MessageBatch {
        // Implementation for creating a message batch from the accumulated messages
        MessageBatch {}
    }

    fn split_send_results(&mut self, send_result: SendResult) {
        // Implementation for splitting send results
    }

    pub fn send(&mut self) -> rocketmq_error::RocketMQResult<(), ()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let message_batch = self.batch();
        let send_result = self
            .default_mq_producer
            .send_direct(message_batch, None, None)?;
        self.split_send_results(send_result);
        Ok(())
    }

    pub fn send_with_callback(&mut self, send_callback: SendCallback) -> rocketmq_error::RocketMQResult<(), ()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let message_batch = self.batch();
        let size = self.messages_size.load(Ordering::SeqCst);

        self.default_mq_producer
            .send_direct(message_batch, None, Some(send_callback))?;

        Ok(())
    }*/
}

#[derive(Default)]
struct GuardForSyncSendService {
    service_name: String,
}

impl GuardForSyncSendService {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
        }
    }

    pub fn start(&mut self) {
        let service_name = self.service_name.clone();
        Builder::new()
            .name(service_name)
            .spawn(|| {
                // Implementation for starting the guard thread
            })
            .expect("Failed to start guard thread");
    }

    pub fn shutdown(&mut self) {
        // Implementation for shutting down the guard thread
    }
}

#[derive(Default)]
struct GuardForAsyncSendService {
    service_name: String,
}

impl GuardForAsyncSendService {
    pub fn new(service_name: &str) -> Self {
        Self {
            service_name: service_name.to_string(),
        }
    }

    pub fn start(&mut self) {
        let service_name = self.service_name.clone();
        Builder::new()
            .name(service_name)
            .spawn(|| {
                // Implementation for starting the guard thread
            })
            .expect("Failed to start guard thread");
    }

    pub fn shutdown(&mut self) {
        // Implementation for shutting down the guard thread
    }
}
