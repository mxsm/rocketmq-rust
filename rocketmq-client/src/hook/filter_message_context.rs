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

use std::fmt;

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;

#[derive(Default)]
pub struct FilterMessageContext<'a> {
    pub(crate) consumer_group: Option<String>,
    pub(crate) msg_list: &'a [MessageExt],
    pub(crate) mq: Option<&'a MessageQueue>,
    pub(crate) arg: Option<Box<dyn std::any::Any>>,
    pub(crate) unit_mode: bool,
}

impl<'a> FilterMessageContext<'a> {
    pub fn new(
        consumer_group: Option<String>,
        msg_list: &'a [MessageExt],
        mq: Option<&'a MessageQueue>,
        arg: Option<Box<dyn std::any::Any>>,
        unit_mode: bool,
    ) -> Self {
        FilterMessageContext {
            consumer_group,
            msg_list,
            mq,
            arg,
            unit_mode,
        }
    }

    pub fn consumer_group(&self) -> Option<&String> {
        self.consumer_group.as_ref()
    }

    pub fn msg_list(&self) -> &'a [MessageExt] {
        self.msg_list
    }

    pub fn mq(&self) -> Option<&'a MessageQueue> {
        self.mq
    }

    pub fn arg(&self) -> &Option<Box<dyn std::any::Any>> {
        &self.arg
    }

    pub fn unit_mode(&self) -> bool {
        self.unit_mode
    }

    pub fn set_consumer_group(&mut self, consumer_group: Option<String>) {
        self.consumer_group = consumer_group;
    }

    pub fn set_msg_list(&mut self, msg_list: &'a [MessageExt]) {
        self.msg_list = msg_list;
    }

    pub fn set_mq(&mut self, mq: Option<&'a MessageQueue>) {
        self.mq = mq;
    }

    pub fn set_arg(&mut self, arg: Option<Box<dyn std::any::Any>>) {
        self.arg = arg;
    }

    pub fn set_unit_mode(&mut self, unit_mode: bool) {
        self.unit_mode = unit_mode;
    }
}

impl fmt::Debug for FilterMessageContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FilterMessageContext {{ consumer_group: {:?}, msg_list: {:?}, mq: {:?}, arg: {:?}, unit_mode: {} }}",
            self.consumer_group, self.msg_list, self.mq, self.arg, self.unit_mode
        )
    }
}
