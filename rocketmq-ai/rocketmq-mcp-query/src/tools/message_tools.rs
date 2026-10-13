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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

mod key_search;
mod trace;

pub use key_search::FindMessagesByKeyArgs;
pub use key_search::FindMessagesByKeyOutput;
pub use key_search::MessageKeyMatch;
pub(crate) use key_search::DEFAULT_KEY_SEARCH_WINDOW_MINUTES;
pub(crate) use key_search::MAX_KEY_SEARCH_WINDOW_MINUTES;
pub use trace::MessageTraceRecord;
pub use trace::MessageTraceStep;
pub use trace::QueryMessageTraceArgs;
pub use trace::QueryMessageTraceOutput;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MessageMetadataArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Identifier of the message to read, exactly as RocketMQ reported it.
    pub message_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct MessageMetadataOutput {
    pub cluster: String,
    pub message_alias: String,
    pub unique_message_alias: Option<String>,
    pub topic: String,
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
