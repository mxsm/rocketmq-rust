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
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;

use bytes::Bytes;

use crate::common::message::message_client_id_setter::MessageClientIDSetter;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageTrait;

#[derive(Clone, Debug, Default)]
pub struct MessageClientExt {
    pub message_ext_inner: MessageExt,
}

impl MessageClientExt {
    pub fn get_offset_msg_id(&self) -> &str {
        self.message_ext_inner.msg_id()
    }

    pub fn set_offset_msg_id(&mut self, offset_msg_id: impl Into<String>) {
        self.message_ext_inner.set_msg_id(offset_msg_id.into());
    }

    pub fn get_msg_id(&self) -> String {
        let uniq_id = MessageClientIDSetter::get_uniq_id(&self.message_ext_inner);
        if let Some(uniq_id) = uniq_id {
            uniq_id
        } else {
            self.message_ext_inner.msg_id().to_string()
        }
    }
}

impl Display for MessageClientExt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MessageClientExt {{ message_ext_inner: {:?} }}",
            self.message_ext_inner
        )
    }
}

impl MessageTrait for MessageClientExt {
    fn put_property(&mut self, key: &str, value: &str) {
        self.message_ext_inner.put_property(key, value);
    }

    fn clear_property(&mut self, name: &str) {
        self.message_ext_inner.clear_property(name);
    }

    fn get_property(&self, name: &str) -> Option<String> {
        self.message_ext_inner.get_property(name)
    }

    fn get_topic(&self) -> &str {
        self.message_ext_inner.get_topic()
    }

    fn set_topic(&mut self, topic: &str) {
        self.message_ext_inner.set_topic(topic);
    }

    fn get_flag(&self) -> i32 {
        self.message_ext_inner.get_flag()
    }

    fn set_flag(&mut self, flag: i32) {
        self.message_ext_inner.set_flag(flag);
    }

    fn get_body(&self) -> Option<&Bytes> {
        self.message_ext_inner.get_body()
    }

    fn set_body(&mut self, body: Bytes) {
        self.message_ext_inner.set_body(body);
    }

    fn get_properties(&self) -> &HashMap<String, String> {
        self.message_ext_inner.get_properties()
    }

    fn set_properties(&mut self, properties: HashMap<String, String>) {
        self.message_ext_inner.set_properties(properties);
    }

    fn get_transaction_id(&self) -> &str {
        self.message_ext_inner.get_transaction_id()
    }

    fn set_transaction_id(&mut self, transaction_id: &str) {
        self.message_ext_inner.set_transaction_id(transaction_id);
    }

    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        self.message_ext_inner.get_compressed_body_mut()
    }

    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.message_ext_inner.get_compressed_body()
    }

    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.message_ext_inner
            .set_compressed_body_mut(compressed_body);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
