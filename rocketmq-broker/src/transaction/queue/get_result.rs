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

use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_common::common::message::message_ext::MessageExt;

pub(crate) struct GetResult {
    pub(crate) msg: Option<MessageExt>,
    pub(crate) pull_result: Option<PullResult>,
}

impl GetResult {
    pub fn new() -> Self {
        Self {
            pull_result: None,
            msg: None,
        }
    }

    pub fn get_pull_result(&self) -> Option<&PullResult> {
        self.pull_result.as_ref()
    }

    pub fn set_pull_result(&mut self, pull_result: Option<PullResult>) {
        self.pull_result = pull_result;
    }

    pub fn get_msg(&self) -> Option<&MessageExt> {
        self.msg.as_ref()
    }

    pub fn set_msg(&mut self, msg: Option<MessageExt>) {
        self.msg = msg;
    }
}
