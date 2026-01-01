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

use rocketmq_common::common::message::message_ext::MessageExt;

use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;

pub trait MessageListenerOrderly: Sync + Send {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus>;
}

pub type ArcBoxMessageListenerOrderly = Arc<Box<dyn MessageListenerOrderly>>;

pub type MessageListenerOrderlyFn = Arc<
    dyn Fn(&[&MessageExt], &ConsumeOrderlyContext) -> rocketmq_error::RocketMQResult<ConsumeOrderlyStatus>
        + Send
        + Sync,
>;
