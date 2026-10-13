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

//! The messages of one Topic that carry a key, within a bounded store-time window.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// The window a search covers when `window_minutes` is omitted.
pub(crate) const DEFAULT_KEY_SEARCH_WINDOW_MINUTES: u32 = 60;
/// The longest window a search covers: 24 hours.
pub(crate) const MAX_KEY_SEARCH_WINDOW_MINUTES: u32 = 1_440;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FindMessagesByKeyArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Exact Topic name. `rocketmq_list_topics` lists the names.
    pub topic: String,
    /// One whole message key, exactly as the producer set it. It is matched exactly and case-sensitively: it is
    /// neither a prefix nor a pattern, and it holds no whitespace.
    #[schemars(length(min = 1, max = 255))]
    pub key: String,
    /// End of the searched store-time window as an RFC 3339 timestamp, for example `2026-10-01T08:30:00Z`. Omit it
    /// to search up to now.
    #[serde(default)]
    pub end_at: Option<String>,
    /// Length of the searched window in minutes, from 1 to 1440. The window ends at `end_at`. Defaults to 60.
    #[serde(default)]
    #[schemars(range(min = 1, max = 1440))]
    pub window_minutes: Option<u32>,
}

/// The fixed metadata of one message a search found. It names the message by pseudonyms only.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct MessageKeyMatch {
    pub message_alias: String,
    pub unique_message_alias: Option<String>,
    /// The Broker that stores the message.
    pub broker_name: String,
    pub born_at: Option<String>,
    pub stored_at: Option<String>,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub store_size: i32,
    pub reconsume_times: i32,
    pub sys_flag: i32,
    pub flag: i32,
    pub prepared_transaction_offset: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct FindMessagesByKeyOutput {
    pub cluster: String,
    pub topic: String,
    /// The store-time window that was searched.
    pub window_begin_at: String,
    pub window_end_at: String,
    /// The matches, newest first, at most 32.
    pub messages: Vec<MessageKeyMatch>,
    /// Whether more messages match than were returned. Search a shorter window to reach them.
    pub truncated: bool,
    pub queried_broker_count: usize,
    /// The Brokers that host the Topic and did not answer. Their matches are missing.
    pub unanswered_brokers: Vec<String>,
    pub generated_at: String,
}
