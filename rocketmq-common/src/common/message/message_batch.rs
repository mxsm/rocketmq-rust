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

use std::collections::HashMap;

use crate::common::message::{
    message_single::{Message, MessageExtBrokerInner},
    MessageTrait,
};

pub struct MessageBatch {
    pub messages: Vec<Message>,
}

impl MessageTrait for MessageBatch {
    fn get_topic(&self) -> &str {
        todo!()
    }

    fn set_topic(&mut self, _topic: impl Into<String>) {
        todo!()
    }

    fn get_tags(&self) -> Option<&str> {
        todo!()
    }

    fn set_tags(&mut self, _tags: impl Into<String>) {
        todo!()
    }

    fn put_property(&mut self, _key: impl Into<String>, _value: impl Into<String>) {
        todo!()
    }

    fn get_properties(&self) -> &HashMap<String, String> {
        todo!()
    }

    fn put_user_property(&mut self, _name: impl Into<String>, _value: impl Into<String>) {
        todo!()
    }

    fn get_delay_time_level(&self) -> i32 {
        todo!()
    }

    fn set_delay_time_level(&self, _level: i32) -> i32 {
        todo!()
    }
}

#[derive(Debug)]
pub struct MessageExtBatch {
    pub message_ext_broker_inner: MessageExtBrokerInner,
    pub is_inner_batch: bool,
}
