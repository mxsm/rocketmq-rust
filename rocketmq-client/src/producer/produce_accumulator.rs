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
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::Result;

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
    pub fn start(&mut self) {}
    pub fn shutdown(&self) {}

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

    pub(crate) fn send<M: MessageTrait>(
        &mut self,
        message: M,
        mq: Option<MessageQueue>,
        default_mq_producer: DefaultMQProducer,
    ) -> Result<Option<SendResult>> {
        unimplemented!("send")
    }

    pub(crate) fn send_callback<M: MessageTrait>(
        &mut self,
        message: M,
        send_callback: Option<SendMessageCallback>,
        default_mq_producer: DefaultMQProducer,
    ) -> Result<()> {
        unimplemented!("send")
    }
}

#[derive(Clone, Debug)]
pub struct AggregateKey {
    pub topic: String,
    pub mq: Option<MessageQueue>,
    pub wait_store_msg_ok: bool,
    pub tag: Option<String>,
}

impl AggregateKey {
    pub fn new_from_message<M: MessageTrait>(message: &M) -> Self {
        Self {
            topic: message.get_topic().to_string(),
            mq: None,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.get_tags(),
        }
    }

    pub fn new_from_message_queue<M: MessageTrait>(message: &M, mq: Option<MessageQueue>) -> Self {
        Self {
            topic: message.get_topic().to_string(),
            mq,
            wait_store_msg_ok: message.is_wait_store_msg_ok(),
            tag: message.get_tags(),
        }
    }

    pub fn new(
        topic: String,
        mq: Option<MessageQueue>,
        wait_store_msg_ok: bool,
        tag: Option<String>,
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
    default_mq_producer: ArcRefCellWrapper<DefaultMQProducer>,
    messages: Vec<Message>,
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
    pub fn new(
        aggregate_key: AggregateKey,
        default_mq_producer: ArcRefCellWrapper<DefaultMQProducer>,
    ) -> Self {
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

    /*    fn ready_to_send(&self, hold_size: i32, hold_ms: u128) -> bool {
        self.messages_size.load(Ordering::SeqCst) > hold_size
            || SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis()
                >= self.create_time + hold_ms
    }

    pub fn add(&mut self, msg: Message) -> Result<usize, ()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(());
        }
        let ret = self.count;
        self.count += 1;
        self.messages.push_back(msg);
        self.messages_size
            .fetch_add(msg.get_body().len() as i32, Ordering::SeqCst);
        if let Some(msg_keys) = msg.get_keys() {
            self.keys.extend(msg_keys.split(',').map(|s| s.to_string()));
        }
        Ok(ret)
    }

    pub fn add_with_callback(
        &mut self,
        msg: Message,
        send_callback: SendCallback,
    ) -> Result<bool, ()> {
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

    pub fn send(&mut self) -> Result<(), ()> {
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

    pub fn send_with_callback(&mut self, send_callback: SendCallback) -> Result<(), ()> {
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
#[derive(Default)]
struct GuardForAsyncSendService {
    service_name: String,
}
