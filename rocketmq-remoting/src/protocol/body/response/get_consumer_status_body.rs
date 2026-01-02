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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

/// Response body for get consumer status operation.
/// Maps client ID to their message queue offset table.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusBody {
    /// Maps MessageQueue to offset (used for single client response)
    #[serde(default, with = "any_key_map")]
    pub message_queue_table: HashMap<MessageQueue, i64>,

    /// Maps client ID to their MessageQueue offset table (used for aggregated response)
    #[serde(default)]
    pub consumer_table: HashMap<CheetahString, HashMap<MessageQueue, i64>>,
}

impl GetConsumerStatusBody {
    pub fn new() -> Self {
        Self {
            message_queue_table: HashMap::new(),
            consumer_table: HashMap::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    pub fn decode(body: &[u8]) -> Option<Self> {
        serde_json::from_slice(body).ok()
    }
}
