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
use serde::Deserialize;
use serde::Serialize;

use super::consume_stats::normalize_nonstandard_offset_table_keys;
use super::consume_stats::ConsumeStats;
use crate::protocol::RemotingDeserializable;

#[derive(Serialize, Deserialize)]
pub struct ConsumeStatsList {
    #[serde(rename = "consumeStatsList")]
    pub consume_stats_list: Vec<HashMap<CheetahString, Vec<ConsumeStats>>>,

    #[serde(rename = "brokerAddr")]
    pub broker_addr: Option<CheetahString>,

    #[serde(rename = "totalDiff")]
    pub total_diff: i64,

    #[serde(rename = "totalInflightDiff")]
    pub total_inflight_diff: i64,
}

impl ConsumeStatsList {
    pub fn encode_java_compatible(&self) -> rocketmq_error::RocketMQResult<Vec<u8>> {
        Ok(self.to_java_compatible_json()?.into_bytes())
    }

    pub fn to_java_compatible_json(&self) -> rocketmq_error::RocketMQResult<String> {
        let mut body = String::new();
        body.push_str("{\"consumeStatsList\":[");

        for (list_index, group_map) in self.consume_stats_list.iter().enumerate() {
            if list_index > 0 {
                body.push(',');
            }
            body.push('{');
            for (group_index, (group, stats_list)) in group_map.iter().enumerate() {
                if group_index > 0 {
                    body.push(',');
                }
                body.push_str(&serde_json::to_string(group)?);
                body.push(':');
                body.push('[');
                for (stats_index, stats) in stats_list.iter().enumerate() {
                    if stats_index > 0 {
                        body.push(',');
                    }
                    body.push_str(&stats.to_java_compatible_json()?);
                }
                body.push(']');
            }
            body.push('}');
        }

        body.push_str("],\"brokerAddr\":");
        match &self.broker_addr {
            Some(broker_addr) => body.push_str(&serde_json::to_string(broker_addr)?),
            None => body.push_str("null"),
        }
        body.push_str(",\"totalDiff\":");
        body.push_str(&self.total_diff.to_string());
        body.push_str(",\"totalInflightDiff\":");
        body.push_str(&self.total_inflight_diff.to_string());
        body.push('}');
        Ok(body)
    }

    pub fn decode(body: &[u8]) -> rocketmq_error::RocketMQResult<Self> {
        match <Self as RemotingDeserializable>::decode(body) {
            Ok(stats) => Ok(stats),
            Err(error) => {
                let Ok(raw_body) = std::str::from_utf8(body) else {
                    return Err(error);
                };
                let normalized_body = normalize_nonstandard_offset_table_keys(raw_body);
                if normalized_body == raw_body {
                    return Err(error);
                }
                <Self as RemotingDeserializable>::decode_str(&normalized_body)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn consume_status_list_serialization() {
        let mut map = HashMap::new();
        let consume_stats_list = vec![ConsumeStats {
            offset_table: HashMap::new(),
            consume_tps: 1.2,
        }];
        map.insert(CheetahString::from("group1"), consume_stats_list);
        let consume_status_list = ConsumeStatsList {
            consume_stats_list: vec![map],
            broker_addr: Some(CheetahString::from("addr")),
            total_diff: 2,
            total_inflight_diff: 1,
        };
        let serialized = serde_json::to_string(&consume_status_list).unwrap();
        assert!(serialized.contains("\"consumeStatsList\""));
        let deserialized: ConsumeStatsList = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.broker_addr.unwrap(), CheetahString::from("addr"));
        assert_eq!(deserialized.total_diff, 2);
        assert_eq!(deserialized.total_inflight_diff, 1);
        let a = deserialized.consume_stats_list[0]
            .get(&CheetahString::from("group1"))
            .unwrap();
        assert_eq!(a[0].consume_tps, 1.2);
    }

    #[test]
    fn consume_status_list_java_compatible_serialization() {
        use rocketmq_common::common::message::message_queue::MessageQueue;

        let mut stats = ConsumeStats::new();
        stats
            .get_offset_table_mut()
            .insert(MessageQueue::from_parts("TopicTest", "broker-a", 0), Default::default());
        let mut map = HashMap::new();
        map.insert(CheetahString::from("group1"), vec![stats]);
        let list = ConsumeStatsList {
            consume_stats_list: vec![map],
            broker_addr: Some(CheetahString::from("127.0.0.1:10911")),
            total_diff: 0,
            total_inflight_diff: 0,
        };

        let encoded = String::from_utf8(list.encode_java_compatible().expect("encode consume stats list"))
            .expect("utf8 consume stats list");

        assert!(encoded.contains(r#""consumeStatsList":[{"group1":[{"offsetTable":{{"topic":"TopicTest""#));
        assert!(!encoded.contains(r#""{\"topic\""#));

        let decoded = ConsumeStatsList::decode(encoded.as_bytes()).expect("decode consume stats list");
        let group = decoded.consume_stats_list[0]
            .get(&CheetahString::from("group1"))
            .expect("group1 stats");
        assert_eq!(group[0].offset_table.len(), 1);
    }
}
