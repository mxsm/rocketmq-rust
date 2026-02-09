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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2, Default)]
pub struct QueryTopicsByConsumerRequestHeader {
    #[required]
    #[serde(rename = "group")]
    pub group: CheetahString,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl QueryTopicsByConsumerRequestHeader {
    pub fn new(group: impl Into<CheetahString>) -> Self {
        Self {
            group: group.into(),
            rpc_request_header: None,
        }
    }

    pub fn get_group(&self) -> &CheetahString {
        &self.group
    }

    pub fn set_group(&mut self, group: CheetahString) {
        self.group = group;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn query_topics_by_consumer_request_header_default() {
        let header = QueryTopicsByConsumerRequestHeader::default();
        assert_eq!(header.get_group(), "");
        assert!(header.rpc_request_header.is_none());
    }

    #[test]
    fn query_topics_by_consumer_request_header_new() {
        let mut header = QueryTopicsByConsumerRequestHeader::new("group1");
        assert_eq!(header.get_group(), "group1");
        assert!(header.rpc_request_header.is_none());
        header.set_group(CheetahString::from("group2"));
        assert_eq!(header.get_group(), "group2");
    }

    #[test]
    fn query_topics_by_consumer_request_header_serialization() {
        let header = QueryTopicsByConsumerRequestHeader {
            group: CheetahString::from("group1"),
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from("broker")),
                ..Default::default()
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"group\":\"group1\""));
        assert!(json.contains("\"brokerName\":\"broker\""));
    }

    #[test]
    fn query_topics_by_consumer_request_header_deserialization() {
        let json = r#"{"group":"group1","brokerName":"broker"}"#;
        let header: QueryTopicsByConsumerRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.group, "group1");
        assert_eq!(
            header.rpc_request_header.unwrap().broker_name,
            Some(CheetahString::from("broker"))
        );
    }

    #[test]
    fn query_topics_by_consumer_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("group"), CheetahString::from("group1"));
        map.insert(CheetahString::from("brokerName"), CheetahString::from("broker1"));

        let header = <QueryTopicsByConsumerRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.group, "group1");
        assert_eq!(
            header.rpc_request_header.unwrap().broker_name,
            Some(CheetahString::from("broker1"))
        );
    }
}
