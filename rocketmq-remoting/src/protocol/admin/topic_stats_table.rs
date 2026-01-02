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

use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

use crate::protocol::admin::topic_offset::TopicOffset;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TopicStatsTable {
    #[serde(with = "any_key_map")]
    offset_table: HashMap<MessageQueue, TopicOffset>,
}

impl TopicStatsTable {
    pub fn new() -> Self {
        Self {
            offset_table: HashMap::new(),
        }
    }

    pub fn get_offset_table(&self) -> HashMap<MessageQueue, TopicOffset> {
        self.offset_table.clone()
    }

    pub fn set_offset_table(&mut self, offset_table: HashMap<MessageQueue, TopicOffset>) {
        self.offset_table = offset_table;
    }
}
