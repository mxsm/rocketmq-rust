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

use crate::protocol::admin::offset_wrapper::OffsetWrapper;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ConsumeStats {
    #[serde(with = "any_key_map")]
    pub offset_table: HashMap<MessageQueue, OffsetWrapper>,
    pub consume_tps: f64,
}

impl ConsumeStats {
    pub fn new() -> Self {
        Self {
            offset_table: HashMap::new(),
            consume_tps: 0.0,
        }
    }

    pub fn compute_total_diff(&self) -> i64 {
        self.offset_table
            .values()
            .map(|value| value.get_broker_offset() - value.get_consumer_offset())
            .sum()
    }

    pub fn compute_inflight_total_diff(&self) -> i64 {
        self.offset_table
            .values()
            .map(|value| value.get_pull_offset() - value.get_consumer_offset())
            .sum()
    }

    pub fn get_offset_table(&self) -> &HashMap<MessageQueue, OffsetWrapper> {
        &self.offset_table
    }

    pub fn get_offset_table_mut(&mut self) -> &mut HashMap<MessageQueue, OffsetWrapper> {
        &mut self.offset_table
    }

    pub fn set_offset_table(&mut self, offset_table: HashMap<MessageQueue, OffsetWrapper>) {
        self.offset_table = offset_table;
    }

    pub fn get_consume_tps(&self) -> f64 {
        self.consume_tps
    }

    pub fn set_consume_tps(&mut self, consume_tps: f64) {
        self.consume_tps = consume_tps;
    }
}
