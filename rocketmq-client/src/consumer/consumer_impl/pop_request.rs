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
use std::hash::Hash;
use std::hash::Hasher;

use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;

#[derive(Clone)]
pub struct PopRequest {
    topic: String,
    consumer_group: String,
    message_queue: MessageQueue,
    pop_process_queue: PopProcessQueue,
    locked_first: bool,
    init_mode: i32,
}

impl PopRequest {
    pub fn new(
        topic: String,
        consumer_group: String,
        message_queue: MessageQueue,
        pop_process_queue: PopProcessQueue,
        init_mode: i32,
    ) -> Self {
        PopRequest {
            topic,
            consumer_group,
            message_queue,
            pop_process_queue,
            locked_first: false,
            init_mode,
        }
    }

    pub fn is_locked_first(&self) -> bool {
        self.locked_first
    }

    pub fn set_locked_first(&mut self, locked_first: bool) {
        self.locked_first = locked_first;
    }

    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }

    pub fn set_consumer_group(&mut self, consumer_group: String) {
        self.consumer_group = consumer_group;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    pub fn set_message_queue(&mut self, message_queue: MessageQueue) {
        self.message_queue = message_queue;
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    pub fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    pub fn get_pop_process_queue(&self) -> &PopProcessQueue {
        &self.pop_process_queue
    }

    pub fn set_pop_process_queue(&mut self, pop_process_queue: PopProcessQueue) {
        self.pop_process_queue = pop_process_queue;
    }

    pub fn get_init_mode(&self) -> i32 {
        self.init_mode
    }

    pub fn set_init_mode(&mut self, init_mode: i32) {
        self.init_mode = init_mode;
    }
}

impl MessageRequest for PopRequest {
    fn get_message_request_mode(&self) -> MessageRequestMode {
        MessageRequestMode::Pop
    }
}

impl Hash for PopRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.consumer_group.hash(state);
        self.message_queue.hash(state);
    }
}

impl PartialEq for PopRequest {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.consumer_group == other.consumer_group
            && self.message_queue == other.message_queue
    }
}

impl Eq for PopRequest {}

impl std::fmt::Display for PopRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PopRequest [topic={}, consumer_group={}, message_queue={:?}]",
            self.topic, self.consumer_group, self.message_queue
        )
    }
}
