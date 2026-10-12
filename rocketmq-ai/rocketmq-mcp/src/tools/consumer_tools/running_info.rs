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

//! What one connected consumer client reports about itself.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::model::contract::Page;
use crate::model::contract::PageRequest;

/// Returns whether `alias` has the form of a client pseudonym: the word `client`, a hyphen and
/// 32 hexadecimal digits. The input schema of the Tool states the same form as a pattern.
pub(crate) fn is_client_alias(alias: &str) -> bool {
    alias
        .strip_prefix("client-")
        .is_some_and(|tag| tag.len() == 32 && tag.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GetConsumerRunningInfoArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
    /// Exact Consumer Group name. `rocketmq_list_consumer_groups` lists the names.
    pub consumer_group: String,
    /// Pseudonym of one connected client of the group: the `client_alias` of an item that
    /// `rocketmq_list_consumer_connections` returned. It is neither a client identifier nor an address.
    #[schemars(regex(pattern = "^client-[0-9a-f]{32}$"))]
    pub client_alias: String,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerClientSubscription {
    pub topic: String,
    /// The kind of filter, `TAG` or `SQL92`.
    pub expression_type: String,
    /// The filter expression, `*` for every message. Null when it is longer than 1,024 bytes.
    pub expression: Option<String>,
    pub sub_version: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ConsumerClientTopicStatus {
    pub topic: String,
    /// Average time of one pull, in milliseconds.
    pub pull_rt_ms: f64,
    pub pull_tps: f64,
    /// Average time the listener takes for one message, in milliseconds.
    pub consume_rt_ms: f64,
    pub consume_ok_tps: f64,
    pub consume_failed_tps: f64,
    /// Messages the listener failed in the last hour.
    pub consume_failed_messages: i64,
}

/// How a client takes messages from a queue.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerClientQueueMode {
    Pull,
    Pop,
}

/// What a client holds for one queue. A member its `mode` does not have is null, and so is a time
/// the client never recorded.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ConsumerClientQueueRow {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
    pub mode: ConsumerClientQueueMode,
    /// Whether the client gave the queue up, as it does after a rebalance.
    pub dropped: bool,
    /// When the client last pulled or popped from the queue.
    pub last_pull_at: Option<String>,
    /// The offset the client has committed. `pull` only.
    pub commit_offset: Option<i64>,
    /// Messages the client holds that its listener has not finished. `pull` only.
    pub cached_message_count: Option<u32>,
    pub cached_message_size_mib: Option<u32>,
    /// Whether the client holds the queue lock that ordered consumption needs. `pull` only.
    pub locked: Option<bool>,
    pub try_unlock_times: Option<u64>,
    pub last_lock_at: Option<String>,
    pub last_consume_at: Option<String>,
    /// Messages popped and not yet acknowledged. `pop` only.
    pub wait_ack_count: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct GetConsumerRunningInfoOutput {
    pub cluster: String,
    pub consumer_group: String,
    pub client_alias: String,
    /// The Broker that relayed the request to the client.
    pub broker_name: String,
    pub consume_type: Option<String>,
    pub consume_orderly: Option<bool>,
    pub thread_pool_core_size: Option<u32>,
    pub client_version: Option<String>,
    pub started_at: Option<String>,
    /// The subscriptions the client itself holds, at most 256. Clients of one group must hold the same ones.
    pub subscriptions: Vec<ConsumerClientSubscription>,
    /// Pull and consume rates of the client for each Topic, at most 256.
    pub topic_status: Vec<ConsumerClientTopicStatus>,
    /// How many queues the client reported.
    pub queue_count: usize,
    /// Whether the client reported more subscriptions, Topics or queues than the server keeps.
    pub truncated: bool,
    #[serde(flatten)]
    #[schemars(flatten)]
    pub page: Page<ConsumerClientQueueRow>,
    pub generated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_alias_has_one_form_that_the_schema_and_the_check_agree_on() {
        // The pattern the input schema publishes for `client_alias`.
        let schema = serde_json::to_value(schemars::schema_for!(GetConsumerRunningInfoArgs)).unwrap();
        let pattern = regex::Regex::new(schema["properties"]["client_alias"]["pattern"].as_str().unwrap()).unwrap();
        let valid = format!("client-{}", "0123456789abcdef".repeat(2));
        let cases = [
            valid.clone(),
            valid.to_uppercase(),
            valid.replace("client-", "message-"),
            format!("{valid}0"),
            valid[..valid.len() - 1].to_string(),
            format!(" {valid}"),
            format!("{valid}\n"),
            "client-".to_string(),
            String::new(),
            "consumer@127.0.0.1@4242".to_string(),
        ];
        for case in &cases {
            assert_eq!(is_client_alias(case), pattern.is_match(case), "{case:?}");
        }
        assert!(is_client_alias(&valid));
        assert_eq!(cases.iter().filter(|case| is_client_alias(case)).count(), 1);
    }
}
