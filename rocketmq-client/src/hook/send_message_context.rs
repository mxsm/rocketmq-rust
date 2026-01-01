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
use std::error::Error;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_rust::ArcMut;

use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::send_result::SendResult;

#[derive(Default)]
pub struct SendMessageContext<'a> {
    pub producer_group: Option<CheetahString>,
    pub message: Option<&'a (dyn MessageTrait + Send + Sync)>,
    pub mq: Option<&'a MessageQueue>,
    pub broker_addr: Option<CheetahString>,
    pub born_host: Option<CheetahString>,
    pub communication_mode: Option<CommunicationMode>,
    pub send_result: Option<SendResult>,
    pub exception: Option<Arc<Box<dyn Error + Send + Sync>>>,
    pub mq_trace_context: Option<Arc<Box<dyn std::any::Any + Send + Sync>>>,
    pub props: HashMap<CheetahString, CheetahString>,
    pub producer: Option<ArcMut<DefaultMQProducerImpl>>,
    pub msg_type: Option<MessageType>,
    pub namespace: Option<CheetahString>,
}
