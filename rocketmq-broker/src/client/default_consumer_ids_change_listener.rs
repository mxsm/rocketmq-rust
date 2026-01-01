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

use tracing::warn;

use crate::client::consumer_group_event::ConsumerGroupEvent;
use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;

#[derive(Default)]
pub struct DefaultConsumerIdsChangeListener {}

impl ConsumerIdsChangeListener for DefaultConsumerIdsChangeListener {
    fn handle(&self, _event: ConsumerGroupEvent, _group: &str, _args: &[&dyn Any]) {}

    fn shutdown(&self) {
        warn!("DefaultConsumerIdsChangeListener shutdown not implemented");
    }
}
