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

//! The trace records clients wrote about one message.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueryMessageTraceArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Identifier the producing client gave the message: the message identifier of its send result, exactly as
    /// RocketMQ reported it. Trace records are indexed by this identifier, not by the offset message identifier.
    #[schemars(length(min = 1, max = 255))]
    pub message_id: String,
    /// Name of the trace Topic. Omit it unless the clients are configured with a custom one; the default is
    /// `RMQ_SYS_TRACE_TOPIC`.
    #[serde(default)]
    #[schemars(length(min = 1, max = 255))]
    pub trace_topic: Option<String>,
}

/// The step in the life of a message that a trace record is about.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MessageTraceStep {
    /// A producer sent the message.
    Publish,
    /// A consumer received the message and is about to run its listener.
    ConsumeStart,
    /// The listener of a consumer returned.
    ConsumeEnd,
    /// A producer committed or rolled back a transactional message.
    EndTransaction,
    /// A producer recalled a scheduled message.
    Recall,
}

/// One trace record. Clients write trace records, so a record is a claim of a client and not a fact a
/// Broker verified.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct MessageTraceRecord {
    pub step: MessageTraceStep,
    /// When the step happened.
    pub at: Option<String>,
    /// The producer or consumer group of the client that wrote the record.
    pub group: Option<String>,
    /// How long the step took, in milliseconds.
    pub cost_time_ms: i32,
    pub success: bool,
    pub topic: Option<String>,
    /// How often the message had been redelivered when a consumer received it.
    pub retry_times: i32,
    /// Size of the message body in bytes. The body itself is never returned.
    pub body_length: i32,
    pub from_transaction_check: bool,
    /// The Broker the record names as the store, when it is a Broker of this cluster.
    pub store_broker_name: Option<String>,
    /// Pseudonym of the host of the client that wrote the record. Null without the `rocketmq:diagnose` scope.
    pub client_host_alias: Option<String>,
    /// The tags of the message. Null without the `rocketmq:diagnose` scope.
    pub tags: Option<String>,
    /// Pseudonyms of the keys of the message, at most 4. Empty without the `rocketmq:diagnose` scope.
    pub key_aliases: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct QueryMessageTraceOutput {
    pub cluster: String,
    /// Pseudonym of the queried identifier, the same one `rocketmq_get_message_metadata` reports as
    /// `unique_message_alias`.
    pub unique_message_alias: String,
    /// Pseudonym of the offset message identifier the records name, the same one
    /// `rocketmq_get_message_metadata` reports as `message_alias`. Null when no record names one.
    pub message_alias: Option<String>,
    pub trace_topic: String,
    /// False when the trace Topic has no route in this cluster: no client traces messages there, or the clients
    /// use another trace Topic.
    pub trace_topic_found: bool,
    /// Whether client hosts, tags and keys were left out because the caller lacks the `rocketmq:diagnose` scope.
    pub redacted: bool,
    /// The records in ascending order of time, at most 128.
    pub records: Vec<MessageTraceRecord>,
    /// Whether the message has more trace records than were returned.
    pub truncated: bool,
    pub queried_broker_count: usize,
    /// The Brokers that host the trace Topic and did not answer. Their records are missing.
    pub unanswered_brokers: Vec<String>,
    pub generated_at: String,
}
