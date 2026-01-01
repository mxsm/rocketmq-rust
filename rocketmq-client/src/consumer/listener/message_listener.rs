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

use crate::consumer::listener::message_listener_concurrently::ArcBoxMessageListenerConcurrently;
use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrentlyFn;
use crate::consumer::listener::message_listener_orderly::ArcBoxMessageListenerOrderly;
use crate::consumer::listener::message_listener_orderly::MessageListenerOrderlyFn;

pub(crate) struct MessageListener {
    pub(crate) message_listener_concurrently: Option<(
        Option<ArcBoxMessageListenerConcurrently>,
        Option<MessageListenerConcurrentlyFn>,
    )>,
    pub(crate) message_listener_orderly:
        Option<(Option<ArcBoxMessageListenerOrderly>, Option<MessageListenerOrderlyFn>)>,
}

impl MessageListener {
    pub(crate) fn new(
        message_listener_concurrently: Option<(
            Option<ArcBoxMessageListenerConcurrently>,
            Option<MessageListenerConcurrentlyFn>,
        )>,
        message_listener_orderly: Option<(Option<ArcBoxMessageListenerOrderly>, Option<MessageListenerOrderlyFn>)>,
    ) -> Self {
        Self {
            message_listener_concurrently,
            message_listener_orderly,
        }
    }
}
