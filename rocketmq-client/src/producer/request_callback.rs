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

use std::sync::Arc;

use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageTrait;

pub type RequestCallbackFn = Arc<dyn Fn(Option<&dyn MessageTrait>, Option<&dyn std::error::Error>) + Send + Sync>;

pub trait RequestCallback: Sync + Send {
    fn on_success(&self, response: &Message);
    fn on_exception(&self, e: &rocketmq_error::RocketMQError);
}
