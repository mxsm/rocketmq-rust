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

use std::collections::BTreeMap;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;
use serde_json_any_key::*;

use crate::protocol::body::consume_status::ConsumeStatus;
use crate::protocol::body::process_queue_info::ProcessQueueInfo;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

/// Java RocketMQ 4.7.1 `ConsumerRunningInfo` — wire-compatible with the broker's
/// `GetConsumerRunningInfo` command and the RocketMQ console.
///
/// Field names and types mirror the Java client's JSON serialization exactly so that the
/// RocketMQ console can attribute Rust-owned queues just as it does Java-owned ones.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerRunningInfo {
    /// Flat string properties: `PROP_NAMESERVER_ADDR`, `PROP_THREADPOOL_CORE_SIZE`, etc.
    pub properties: BTreeMap<CheetahString, CheetahString>,

    /// Active subscriptions keyed by topic.
    #[serde(default)]
    pub subscription_set: Vec<SubscriptionData>,

    /// Per-queue process state.  Uses `serde_json_any_key` so that `MessageQueue` (a struct)
    /// serialises as the JSON object key that the Java client and console expect.
    #[serde(default, with = "any_key_map")]
    pub mq_table: HashMap<MessageQueue, ProcessQueueInfo>,

    /// Per-topic consume rate/error statistics.
    #[serde(default)]
    pub status_table: HashMap<CheetahString, ConsumeStatus>,

    /// Optional thread-stack dump (only included when `jstackEnable=true`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jstack: Option<CheetahString>,
}

impl ConsumerRunningInfo {
    /// Well-known property keys used by the Java console and tools.
    pub const PROP_NAMESERVER_ADDR: &'static str = "PROP_NAMESERVER_ADDR";
    pub const PROP_CONSUME_TYPE: &'static str = "PROP_CONSUME_TYPE";
    pub const PROP_CLIENT_VERSION: &'static str = "PROP_CLIENT_VERSION";
    pub const PROP_CONSUME_ORDERLY: &'static str = "PROP_CONSUME_ORDERLY";
    pub const PROP_THREADPOOL_CORE_SIZE: &'static str = "PROP_THREADPOOL_CORE_SIZE";
    pub const PROP_CONSUMER_START_TIMESTAMP: &'static str = "PROP_CONSUMER_START_TIMESTAMP";

    /// Serialise to JSON bytes, as expected by `RemotingCommand::set_body`.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialise from JSON bytes (used in tests and admin clients).
    pub fn decode(body: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConsumerRunningInfo {
        let mut info = ConsumerRunningInfo::default();
        info.properties.insert(
            CheetahString::from_static_str(ConsumerRunningInfo::PROP_CONSUME_TYPE),
            CheetahString::from_static_str("CONSUME_PASSIVELY"),
        );
        let mq = MessageQueue::from_parts("topic", "broker-a", 0);
        info.mq_table.insert(mq, ProcessQueueInfo {
            commit_offset: 5,
            cached_msg_count: 3,
            locked: false,
            droped: false,
            last_pull_timestamp: 1_000,
            last_consume_timestamp: 900,
            ..Default::default()
        });
        info
    }

    #[test]
    fn encode_decode_roundtrip() {
        let info = sample();
        let bytes = info.encode().unwrap();
        let decoded = ConsumerRunningInfo::decode(&bytes).unwrap();
        assert_eq!(decoded.properties.get(ConsumerRunningInfo::PROP_CONSUME_TYPE).map(|s| s.as_str()), Some("CONSUME_PASSIVELY"));
        assert_eq!(decoded.mq_table.len(), 1);
        let pq = decoded.mq_table.values().next().unwrap();
        assert_eq!(pq.commit_offset, 5);
        assert_eq!(pq.cached_msg_count, 3);
    }

    #[test]
    fn json_contains_java_compatible_field_names() {
        let bytes = sample().encode().unwrap();
        let json = std::str::from_utf8(&bytes).unwrap();
        assert!(json.contains("\"properties\""), "missing properties");
        assert!(json.contains("\"subscriptionSet\""), "missing subscriptionSet");
        assert!(json.contains("\"mqTable\""), "missing mqTable");
        assert!(json.contains("\"statusTable\""), "missing statusTable");
    }
}
