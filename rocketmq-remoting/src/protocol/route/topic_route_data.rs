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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;
use crate::protocol::route::route_data_view::QueueData;
use crate::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
use crate::protocol::RemotingDeserializable;
use crate::protocol::RemotingSerializable;

#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq, PartialEq)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf")]
    pub order_topic_conf: Option<CheetahString>,
    #[serde(rename = "queueDatas")]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    pub broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    pub filter_server_table: HashMap<CheetahString, Vec<CheetahString>>,
    #[serde(rename = "topicQueueMappingInfo", alias = "topicQueueMappingByBroker")]
    pub topic_queue_mapping_by_broker: Option<HashMap<CheetahString, TopicQueueMappingInfo>>,
}

impl TopicRouteData {
    pub fn topic_route_data_changed(&self, old_data: Option<&TopicRouteData>) -> bool {
        if old_data.is_none() {
            return true;
        }
        let mut now = TopicRouteData::from_existing(self);
        let mut old = TopicRouteData::from_existing(old_data.unwrap());
        now.queue_datas.sort();
        now.broker_datas.sort();
        old.queue_datas.sort();
        old.broker_datas.sort();
        now != old
    }

    pub fn new() -> Self {
        TopicRouteData {
            order_topic_conf: None,
            queue_datas: Vec::new(),
            broker_datas: Vec::new(),
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        }
    }

    pub fn from_existing(topic_route_data: &TopicRouteData) -> Self {
        TopicRouteData {
            order_topic_conf: topic_route_data.order_topic_conf.clone(),
            queue_datas: topic_route_data.queue_datas.clone(),
            broker_datas: topic_route_data.broker_datas.clone(),
            filter_server_table: topic_route_data.filter_server_table.clone(),
            topic_queue_mapping_by_broker: topic_route_data.topic_queue_mapping_by_broker.clone(),
        }
    }

    /// Encode route data using the Java `acceptStandardJsonOnly` semantics.
    ///
    /// The observable difference from the legacy path is deterministic map key ordering,
    /// which mirrors Java's `MapSortField` behavior for route responses.
    pub fn encode_standard_json(&self) -> rocketmq_error::RocketMQResult<Vec<u8>> {
        StandardJsonTopicRouteData::from(self).encode()
    }

    pub fn decode(bytes: &[u8]) -> rocketmq_error::RocketMQResult<Self> {
        match <Self as RemotingDeserializable>::decode(bytes) {
            Ok(route_data) => Ok(route_data),
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

#[derive(Serialize)]
struct StandardJsonTopicRouteData<'a> {
    #[serde(rename = "orderTopicConf")]
    order_topic_conf: Option<&'a CheetahString>,
    #[serde(rename = "queueDatas")]
    queue_datas: &'a [QueueData],
    #[serde(rename = "brokerDatas")]
    broker_datas: Vec<StandardJsonBrokerData<'a>>,
    #[serde(rename = "filterServerTable")]
    filter_server_table: BTreeMap<String, &'a Vec<CheetahString>>,
    #[serde(rename = "topicQueueMappingInfo")]
    topic_queue_mapping_by_broker: Option<BTreeMap<String, StandardJsonTopicQueueMappingInfo<'a>>>,
}

#[derive(Serialize)]
struct StandardJsonBrokerData<'a> {
    cluster: &'a str,
    #[serde(rename = "brokerName")]
    broker_name: &'a CheetahString,
    #[serde(rename = "brokerAddrs")]
    broker_addrs: BTreeMap<String, &'a CheetahString>,
    #[serde(rename = "zoneName")]
    zone_name: Option<&'a CheetahString>,
    #[serde(rename = "enableActingMaster")]
    enable_acting_master: bool,
}

#[derive(Serialize)]
struct StandardJsonTopicQueueMappingInfo<'a> {
    topic: Option<&'a CheetahString>,
    scope: Option<&'a CheetahString>,
    #[serde(rename = "totalQueues")]
    total_queues: i32,
    bname: Option<&'a CheetahString>,
    epoch: i64,
    dirty: bool,
    #[serde(rename = "currIdMap")]
    curr_id_map: Option<BTreeMap<String, i32>>,
}

impl<'a> From<&'a TopicRouteData> for StandardJsonTopicRouteData<'a> {
    fn from(value: &'a TopicRouteData) -> Self {
        Self {
            order_topic_conf: value.order_topic_conf.as_ref(),
            queue_datas: &value.queue_datas,
            broker_datas: value.broker_datas.iter().map(StandardJsonBrokerData::from).collect(),
            filter_server_table: value
                .filter_server_table
                .iter()
                .map(|(broker_addr, filter_servers)| (broker_addr.to_string(), filter_servers))
                .collect(),
            topic_queue_mapping_by_broker: value.topic_queue_mapping_by_broker.as_ref().map(|mapping| {
                mapping
                    .iter()
                    .map(|(broker_name, info)| (broker_name.to_string(), StandardJsonTopicQueueMappingInfo::from(info)))
                    .collect()
            }),
        }
    }
}

impl<'a> From<&'a BrokerData> for StandardJsonBrokerData<'a> {
    fn from(value: &'a BrokerData) -> Self {
        Self {
            cluster: value.cluster(),
            broker_name: value.broker_name(),
            broker_addrs: value
                .broker_addrs()
                .iter()
                .map(|(broker_id, broker_addr)| (broker_id.to_string(), broker_addr))
                .collect(),
            zone_name: value.zone_name(),
            enable_acting_master: value.enable_acting_master(),
        }
    }
}

impl<'a> From<&'a TopicQueueMappingInfo> for StandardJsonTopicQueueMappingInfo<'a> {
    fn from(value: &'a TopicQueueMappingInfo) -> Self {
        Self {
            topic: value.topic.as_ref(),
            scope: value.scope.as_ref(),
            total_queues: value.total_queues,
            bname: value.bname.as_ref(),
            epoch: value.epoch,
            dirty: value.dirty,
            curr_id_map: value.curr_id_map.as_ref().map(|mapping| {
                mapping
                    .iter()
                    .map(|(queue_id, mapped_id)| (queue_id.to_string(), *mapped_id))
                    .collect()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json;

    use super::*;
    use crate::protocol::route::route_data_view::BrokerData;

    #[test]
    fn topic_route_data_default_values() {
        let topic_route_data = TopicRouteData::default();
        assert!(topic_route_data.order_topic_conf.is_none());
        assert!(topic_route_data.queue_datas.is_empty());
        assert!(topic_route_data.broker_datas.is_empty());
        assert!(topic_route_data.filter_server_table.is_empty());
        assert!(topic_route_data.topic_queue_mapping_by_broker.is_none());
    }

    #[test]
    fn topic_route_data_with_values() {
        let mut filter_server_table = HashMap::new();
        filter_server_table.insert(CheetahString::from("key"), vec![CheetahString::from("value")]);
        let mut topic_queue_mapping_by_broker = HashMap::new();
        topic_queue_mapping_by_broker.insert(CheetahString::from("broker"), TopicQueueMappingInfo::default());
        let topic_route_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("conf")),
            queue_datas: vec![QueueData::default()],
            broker_datas: vec![BrokerData::default()],
            filter_server_table,
            topic_queue_mapping_by_broker: Some(topic_queue_mapping_by_broker),
        };
        assert_eq!(topic_route_data.order_topic_conf, Some(CheetahString::from("conf")));
        assert_eq!(topic_route_data.queue_datas.len(), 1);
        assert_eq!(topic_route_data.broker_datas.len(), 1);
        assert_eq!(topic_route_data.filter_server_table.len(), 1);
        assert!(topic_route_data.topic_queue_mapping_by_broker.is_some());
    }

    #[test]
    fn serialize_topic_route_data() {
        let mut filter_server_table = HashMap::new();
        filter_server_table.insert(CheetahString::from("key"), vec![CheetahString::from("value")]);
        let mut topic_queue_mapping_by_broker = HashMap::new();
        topic_queue_mapping_by_broker.insert(CheetahString::from("broker"), TopicQueueMappingInfo::default());
        let topic_route_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("conf")),
            queue_datas: vec![QueueData::default()],
            broker_datas: vec![BrokerData::default()],
            filter_server_table,
            topic_queue_mapping_by_broker: Some(topic_queue_mapping_by_broker),
        };
        let serialized = serde_json::to_string(&topic_route_data).unwrap();
        assert!(serialized.contains("\"orderTopicConf\":\"conf\""));
        assert!(serialized.contains("\"queueDatas\":["));
        assert!(serialized.contains("\"brokerDatas\":["));
        assert!(serialized.contains("\"filterServerTable\":{\"key\":[\"value\"]}"));
        assert!(serialized.contains("\"topicQueueMappingInfo\":{\"broker\":{"));
    }

    #[test]
    fn decode_java_fastjson_topic_route_data() {
        let java_body = br#"{"brokerDatas":[{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"interopBroker","cluster":"DefaultCluster","enableActingMaster":false}],"filterServerTable":{},"queueDatas":[{"brokerName":"interopBroker","perm":6,"readQueueNums":4,"topicSysFlag":0,"writeQueueNums":4}],"topicQueueMappingByBroker":{"interopBroker":{"topic":"TopicA","scope":"ScopeA","totalQueues":4,"bname":"interopBroker","epoch":1,"dirty":false,"currIdMap":{0:0}}}}"#;

        let decoded = TopicRouteData::decode(java_body).expect("decode Java fastjson route data");

        assert_eq!(decoded.queue_datas.len(), 1);
        assert_eq!(decoded.queue_datas[0].broker_name, CheetahString::from("interopBroker"));
        assert_eq!(decoded.broker_datas.len(), 1);
        assert_eq!(
            decoded.broker_datas[0].broker_addrs().get(&0),
            Some(&CheetahString::from("127.0.0.1:10911"))
        );
        let mapping = decoded
            .topic_queue_mapping_by_broker
            .as_ref()
            .and_then(|items| items.get(&CheetahString::from("interopBroker")))
            .expect("topicQueueMappingByBroker alias should decode");
        assert_eq!(mapping.curr_id_map.as_ref().and_then(|items| items.get(&0)), Some(&0));
    }

    #[test]
    fn encode_standard_json_sorts_nested_map_keys() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(10, CheetahString::from("10.0.0.10:10911"));
        broker_addrs.insert(2, CheetahString::from("10.0.0.2:10911"));

        let mut filter_server_table = HashMap::new();
        filter_server_table.insert(CheetahString::from("z-broker"), vec![CheetahString::from("fs-z")]);
        filter_server_table.insert(CheetahString::from("a-broker"), vec![CheetahString::from("fs-a")]);

        let mut curr_id_map = HashMap::new();
        curr_id_map.insert(8, 80);
        curr_id_map.insert(1, 10);

        let mut topic_queue_mapping_by_broker = HashMap::new();
        topic_queue_mapping_by_broker.insert(
            CheetahString::from("mapping-broker"),
            TopicQueueMappingInfo {
                topic: Some(CheetahString::from("TestTopic")),
                scope: Some(CheetahString::from("scope-a")),
                total_queues: 8,
                bname: Some(CheetahString::from("broker-a")),
                epoch: 3,
                dirty: false,
                curr_id_map: Some(curr_id_map),
            },
        );

        let topic_route_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("order-conf")),
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                Some(CheetahString::from("zone-a")),
            )],
            filter_server_table,
            topic_queue_mapping_by_broker: Some(topic_queue_mapping_by_broker),
        };

        let encoded = String::from_utf8(topic_route_data.encode_standard_json().unwrap()).unwrap();
        let broker_addrs_index = encoded.find("\"brokerAddrs\":{\"10\"").unwrap();
        let broker_addrs_second_index = encoded.find("\"2\":\"10.0.0.2:10911\"").unwrap();
        let filter_server_index = encoded.find("\"filterServerTable\":{\"a-broker\"").unwrap();
        let filter_server_second_index = encoded.find("\"z-broker\":[\"fs-z\"]").unwrap();
        let curr_id_map_index = encoded.find("\"currIdMap\":{\"1\":10").unwrap();
        let curr_id_map_second_index = encoded.find("\"8\":80").unwrap();

        assert!(broker_addrs_index < broker_addrs_second_index);
        assert!(filter_server_index < filter_server_second_index);
        assert!(curr_id_map_index < curr_id_map_second_index);
    }

    #[test]
    fn topic_route_data_changed_with_none_old_data() {
        let topic_route_data = TopicRouteData::default();
        assert!(topic_route_data.topic_route_data_changed(None));
    }

    #[test]
    fn topic_route_data_changed_with_different_data() {
        let topic_route_data = TopicRouteData::default();
        let old_data = TopicRouteData {
            order_topic_conf: Some(CheetahString::from("conf")),
            ..Default::default()
        };
        assert!(topic_route_data.topic_route_data_changed(Some(&old_data)));
    }

    #[test]
    fn topic_route_data_changed_with_same_data() {
        let topic_route_data = TopicRouteData::default();
        let old_data = TopicRouteData::default();
        assert!(!topic_route_data.topic_route_data_changed(Some(&old_data)));
    }
}
