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

//! How many messages of a Consumer Group were dead-lettered, and when the last one was.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetDlqSummaryArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Exact Consumer Group name. `rocketmq_list_consumer_groups` lists the names.
    pub consumer_group: String,
}

/// The part of a dead-letter Topic that one Broker holds.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct DlqBrokerSummary {
    pub broker_name: String,
    pub queue_count: usize,
    pub message_count: u64,
    /// Store time of the newest dead letter on this Broker.
    pub last_message_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct GetDlqSummaryOutput {
    pub cluster: String,
    pub consumer_group: String,
    /// Name of the dead-letter Topic of the group: `%DLQ%` followed by the group name.
    pub dlq_topic: String,
    /// False when the dead-letter Topic does not exist: no message of the group was dead-lettered in this cluster.
    pub exists: bool,
    pub queue_count: usize,
    /// Dead letters the Topic holds now. A Broker deletes them when their retention time ends, so this is not
    /// the number that was ever dead-lettered.
    pub message_count: u64,
    /// Store time of the newest dead letter.
    pub last_message_at: Option<String>,
    /// How long before this observation the newest dead letter was stored, in milliseconds.
    pub last_message_age_ms: Option<u64>,
    /// The same counts for each Broker that holds a part of the Topic, in ascending order of Broker name.
    pub brokers: Vec<DlqBrokerSummary>,
    /// Whether the Topic has more queues than the server reads. The per-Broker rows then cover only the queues
    /// that were read; the totals still cover every queue.
    pub truncated: bool,
    pub generated_at: String,
}
