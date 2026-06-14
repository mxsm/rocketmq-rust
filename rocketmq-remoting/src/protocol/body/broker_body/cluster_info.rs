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
use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;
use crate::protocol::RemotingDeserializable;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterInfo {
    #[serde(rename = "brokerAddrTable")]
    pub broker_addr_table: Option<HashMap<CheetahString, BrokerData>>,

    #[serde(rename = "clusterAddrTable")]
    pub cluster_addr_table: Option<HashMap<CheetahString, HashSet<CheetahString>>>,
}

impl ClusterInfo {
    pub fn new(
        broker_addr_table: Option<HashMap<CheetahString, BrokerData>>,
        cluster_addr_table: Option<HashMap<CheetahString, HashSet<CheetahString>>>,
    ) -> ClusterInfo {
        ClusterInfo {
            broker_addr_table,
            cluster_addr_table,
        }
    }

    pub fn decode(bytes: &[u8]) -> rocketmq_error::RocketMQResult<Self> {
        match <Self as RemotingDeserializable>::decode(bytes) {
            Ok(cluster_info) => Ok(cluster_info),
            Err(error) => {
                let Ok(raw_body) = std::str::from_utf8(bytes) else {
                    return Err(error);
                };
                let Some(normalized_body) = quote_unquoted_numeric_object_keys(raw_body) else {
                    return Err(error);
                };
                <Self as RemotingDeserializable>::decode_str(&normalized_body)
            }
        }
    }
}

fn quote_unquoted_numeric_object_keys(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut output = String::with_capacity(input.len() + 8);
    let mut last_copied = 0;
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    let mut changed = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }

        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }

        if byte == b'{' || byte == b',' {
            let mut key_start = index + 1;
            while key_start < bytes.len() && bytes[key_start].is_ascii_whitespace() {
                key_start += 1;
            }

            let mut digit_start = key_start;
            if digit_start < bytes.len() && bytes[digit_start] == b'-' {
                digit_start += 1;
            }

            if digit_start < bytes.len() && bytes[digit_start].is_ascii_digit() {
                let mut digit_end = digit_start + 1;
                while digit_end < bytes.len() && bytes[digit_end].is_ascii_digit() {
                    digit_end += 1;
                }

                let mut colon_index = digit_end;
                while colon_index < bytes.len() && bytes[colon_index].is_ascii_whitespace() {
                    colon_index += 1;
                }

                if colon_index < bytes.len() && bytes[colon_index] == b':' {
                    output.push_str(&input[last_copied..key_start]);
                    output.push('"');
                    output.push_str(&input[key_start..digit_end]);
                    output.push('"');
                    last_copied = digit_end;
                    changed = true;
                    index = digit_end;
                    continue;
                }
            }
        }

        index += 1;
    }

    if changed {
        output.push_str(&input[last_copied..]);
        Some(output)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_info_default() {
        let cluster_info = ClusterInfo::default();
        assert!(cluster_info.broker_addr_table.is_none());
        assert!(cluster_info.cluster_addr_table.is_none());
    }

    #[test]
    fn test_cluster_info_new() {
        let mut broker_addr_table = HashMap::new();
        let broker_data = BrokerData::default();
        broker_addr_table.insert(CheetahString::from("broker1"), broker_data);

        let mut cluster_addr_table = HashMap::new();
        let mut brokers = HashSet::new();
        brokers.insert(CheetahString::from("broker1"));
        cluster_addr_table.insert(CheetahString::from("cluster1"), brokers);

        let cluster_info = ClusterInfo::new(Some(broker_addr_table.clone()), Some(cluster_addr_table.clone()));
        assert_eq!(cluster_info.broker_addr_table, Some(broker_addr_table));
        assert_eq!(cluster_info.cluster_addr_table, Some(cluster_addr_table));
    }

    #[test]
    fn test_cluster_info_serialization() {
        let mut broker_addr_table = HashMap::new();
        let broker_data = BrokerData::default();
        broker_addr_table.insert(CheetahString::from("broker1"), broker_data);

        let mut cluster_addr_table = HashMap::new();
        let mut brokers = HashSet::new();
        brokers.insert(CheetahString::from("broker1"));
        cluster_addr_table.insert(CheetahString::from("cluster1"), brokers);

        let cluster_info = ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table));
        let serialized = serde_json::to_string(&cluster_info).unwrap();
        assert!(serialized.contains("brokerAddrTable"));
        assert!(serialized.contains("clusterAddrTable"));
    }

    #[test]
    fn test_cluster_info_deserialization() {
        let json = r#"{"brokerAddrTable":{"broker1":{"cluster":"c1","brokerName":"b1","brokerAddrs":{"0":"127.0.0.1:10911"},"enableActingMaster":false}},"clusterAddrTable":{"cluster1":["broker1"]}}"#;
        let cluster_info = ClusterInfo::decode(json.as_bytes()).unwrap();
        assert!(cluster_info.broker_addr_table.is_some());
        assert!(cluster_info.cluster_addr_table.is_some());
        assert_eq!(
            cluster_info
                .broker_addr_table
                .unwrap()
                .get(&CheetahString::from("broker1"))
                .unwrap()
                .broker_name(),
            "b1"
        );
    }

    #[test]
    fn test_cluster_info_decode_java_fastjson_numeric_broker_ids() {
        let java_body = br#"{"brokerAddrTable":{"broker-a":{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster","enableActingMaster":false}},"clusterAddrTable":{"DefaultCluster":["broker-a"]}}"#;

        let cluster_info = ClusterInfo::decode(java_body).expect("decode Java fastjson cluster info");

        let broker_data = cluster_info
            .broker_addr_table
            .as_ref()
            .and_then(|items| items.get(&CheetahString::from("broker-a")))
            .expect("broker-a should decode");
        assert_eq!(
            broker_data.broker_addrs().get(&0),
            Some(&CheetahString::from("127.0.0.1:10911"))
        );
    }
}
