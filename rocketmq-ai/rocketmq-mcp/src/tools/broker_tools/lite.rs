// Copyright 2026 The RocketMQ Rust Authors
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

//! What one Broker holds for Lite Topics.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetLiteBrokerInfoArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Logical Broker name as reported by `rocketmq_list_brokers`, for example `broker-a`. It is a name, not a
    /// network address.
    pub broker_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct LiteTopicSummary {
    pub topic: String,
    /// How long an idle Lite Topic is kept, in minutes. `-1` when no expiration is configured.
    pub expiration_minutes: i32,
    /// How many Consumer Groups are bound to the Topic.
    pub bound_group_count: usize,
    /// The bound Consumer Groups in ascending order, at most 8.
    pub bound_groups: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct GetLiteBrokerInfoOutput {
    pub cluster: String,
    pub broker_name: String,
    /// The kind of store the Broker keeps Lite Topic queues in.
    pub store_type: Option<String>,
    /// The most Lite Topic queues the Broker accepts.
    pub max_lmq_count: i32,
    /// The Lite Topic queues the Broker holds now.
    pub current_lmq_count: i32,
    pub lite_subscription_count: i32,
    pub order_info_count: i32,
    pub consume_queue_count: i32,
    pub offset_count: i32,
    pub event_count: i32,
    /// How many Lite Topics the Broker knows.
    pub lite_topic_count: usize,
    /// The Lite Topics in ascending order of name, at most 100.
    pub lite_topics: Vec<LiteTopicSummary>,
    /// Whether Lite Topics or bound groups beyond those bounds were left out. The counts still cover them.
    pub truncated: bool,
    pub generated_at: String,
}
