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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_error::RocketMQError;

use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::send_result::SendResult;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SendMessageTraceSnapshot {
    pub topic: CheetahString,
    pub tags: CheetahString,
    pub keys: CheetahString,
    pub body_length: i32,
}

impl SendMessageTraceSnapshot {
    pub fn from_message(message: &(dyn MessageTrait + Send + Sync)) -> Self {
        Self {
            topic: message.topic().clone(),
            tags: message.tags().unwrap_or_default(),
            keys: message.get_keys().unwrap_or_default(),
            body_length: message.get_body().map_or(0, |body| body.len() as i32),
        }
    }
}

#[derive(Default)]
pub struct SendMessageContext<'a> {
    pub producer_group: Option<CheetahString>,
    pub message: Option<&'a (dyn MessageTrait + Send + Sync)>,
    pub message_trace_snapshot: Option<SendMessageTraceSnapshot>,
    pub mq: Option<&'a MessageQueue>,
    pub broker_addr: Option<CheetahString>,
    pub born_host: Option<CheetahString>,
    pub communication_mode: Option<CommunicationMode>,
    pub send_result: Option<&'a SendResult>,
    pub exception: Option<Arc<RocketMQError>>,
    pub mq_trace_context: Option<Arc<Box<dyn std::any::Any + Send + Sync>>>,
    pub trace_start_time: Option<u64>,
    pub props: HashMap<CheetahString, CheetahString>,
    pub msg_type: Option<MessageType>,
    pub namespace: Option<CheetahString>,
}
