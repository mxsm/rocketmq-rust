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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;

use bytes::Bytes;
use cheetah_string::CheetahString;

use crate::common::message::message_client_id_setter::MessageClientIDSetter;
use crate::common::message::message_ext::MessageExt;
use crate::common::message::MessageTrait;

#[derive(Clone, Debug, Default)]
pub struct MessageClientExt {
    pub message_ext_inner: MessageExt,
}

impl MessageClientExt {
    pub fn new(message: MessageExt) -> Self {
        Self {
            message_ext_inner: message,
        }
    }

    pub fn get_offset_msg_id(&self) -> &str {
        self.message_ext_inner.msg_id()
    }

    pub fn set_offset_msg_id(&mut self, offset_msg_id: impl Into<CheetahString>) {
        self.message_ext_inner.set_msg_id(offset_msg_id.into());
    }

    pub fn get_msg_id(&self) -> CheetahString {
        let uniq_id = MessageClientIDSetter::get_uniq_id(&self.message_ext_inner);
        if let Some(uniq_id) = uniq_id {
            uniq_id
        } else {
            self.message_ext_inner.msg_id().clone()
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
    #[inline]
    fn put_property(&mut self, key: CheetahString, value: CheetahString) {
        self.message_ext_inner.put_property(key, value);
    }

    #[inline]
    fn clear_property(&mut self, name: &str) {
        self.message_ext_inner.clear_property(name);
    }

    #[inline]
    fn get_property(&self, name: &CheetahString) -> Option<CheetahString> {
        self.message_ext_inner.get_property(name)
    }

    #[inline]
    fn get_topic(&self) -> &CheetahString {
        self.message_ext_inner.get_topic()
    }

    #[inline]
    fn set_topic(&mut self, topic: CheetahString) {
        self.message_ext_inner.set_topic(topic);
    }

    #[inline]
    fn get_flag(&self) -> i32 {
        self.message_ext_inner.get_flag()
    }

    #[inline]
    fn set_flag(&mut self, flag: i32) {
        self.message_ext_inner.set_flag(flag);
    }

    #[inline]
    fn get_body(&self) -> Option<&Bytes> {
        self.message_ext_inner.get_body()
    }

    #[inline]
    fn set_body(&mut self, body: Bytes) {
        self.message_ext_inner.set_body(body);
    }

    #[inline]
    fn get_properties(&self) -> &HashMap<CheetahString, CheetahString> {
        self.message_ext_inner.get_properties()
    }

    #[inline]
    fn set_properties(&mut self, properties: HashMap<CheetahString, CheetahString>) {
        self.message_ext_inner.set_properties(properties);
    }

    #[inline]
    fn get_transaction_id(&self) -> Option<&CheetahString> {
        self.message_ext_inner.get_transaction_id()
    }

    #[inline]
    fn set_transaction_id(&mut self, transaction_id: CheetahString) {
        self.message_ext_inner.set_transaction_id(transaction_id);
    }

    #[inline]
    fn get_compressed_body_mut(&mut self) -> &mut Option<Bytes> {
        self.message_ext_inner.get_compressed_body_mut()
    }

    #[inline]
    fn get_compressed_body(&self) -> Option<&Bytes> {
        self.message_ext_inner.get_compressed_body()
    }

    #[inline]
    fn set_compressed_body_mut(&mut self, compressed_body: Bytes) {
        self.message_ext_inner.set_compressed_body_mut(compressed_body);
    }

    #[inline]
    fn take_body(&mut self) -> Option<Bytes> {
        self.message_ext_inner.take_body()
    }

    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
