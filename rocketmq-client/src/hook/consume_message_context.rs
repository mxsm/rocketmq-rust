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
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_rust::ArcMut;

use crate::base::access_channel::AccessChannel;

#[derive(Default)]
pub struct ConsumeMessageContext<'a> {
    pub consumer_group: CheetahString,
    pub msg_list: &'a [ArcMut<MessageExt>],
    pub mq: Option<MessageQueue>,
    pub success: bool,
    pub status: CheetahString,
    pub mq_trace_context: Option<Arc<Box<dyn std::any::Any + Send + Sync>>>,
    pub props: HashMap<CheetahString, CheetahString>,
    pub namespace: CheetahString,
    pub access_channel: Option<AccessChannel>,
}
