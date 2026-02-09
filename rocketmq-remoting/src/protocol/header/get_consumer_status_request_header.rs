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

/// Request header for getting consumer status from client.
#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetConsumerStatusRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub group: CheetahString,

    pub client_addr: Option<CheetahString>,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

impl GetConsumerStatusRequestHeader {
    pub fn new(topic: CheetahString, group: CheetahString) -> Self {
        Self {
            topic,
            group,
            client_addr: None,
            rpc_request_header: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_consumer_status_request_header_default() {
        let header = GetConsumerStatusRequestHeader::default();
        assert_eq!(header.topic, "");
        assert_eq!(header.group, "");
        assert!(header.client_addr.is_none());
        assert!(header.rpc_request_header.is_none());
    }

    #[test]
    fn get_consumer_status_request_header_new() {
        let header = GetConsumerStatusRequestHeader::new(CheetahString::from("topic1"), CheetahString::from("group1"));
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.group, "group1");
        assert!(header.client_addr.is_none());
        assert!(header.rpc_request_header.is_none());
    }

    #[test]
    fn get_consumer_status_request_header_serialization() {
        let header = GetConsumerStatusRequestHeader {
            topic: CheetahString::from("topic1"),
            group: CheetahString::from("group1"),
            client_addr: Some(CheetahString::from("127.0.0.1")),
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from("broker")),
                ..Default::default()
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"topic\":\"topic1\""));
        assert!(json.contains("\"group\":\"group1\""));
        assert!(json.contains("\"clientAddr\":\"127.0.0.1\""));
        assert!(json.contains("\"brokerName\":\"broker\""));
    }

    #[test]
    fn get_consumer_status_request_header_deserialization() {
        let json = r#"{"topic":"topic1","group":"group1","clientAddr":"127.0.0.1","brokerName":"broker"}"#;
        let header: GetConsumerStatusRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.group, "group1");
        assert_eq!(header.client_addr, Some(CheetahString::from("127.0.0.1")));
        assert_eq!(
            header.rpc_request_header.unwrap().broker_name,
            Some(CheetahString::from("broker"))
        );
    }

    #[test]
    fn get_consumer_status_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("topic"), CheetahString::from("topic1"));
        map.insert(CheetahString::from("group"), CheetahString::from("group1"));
        map.insert(CheetahString::from("clientAddr"), CheetahString::from("127.0.0.1"));
        map.insert(CheetahString::from("brokerName"), CheetahString::from("broker1"));

        let header = <GetConsumerStatusRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.group, "group1");
        assert_eq!(header.client_addr, Some(CheetahString::from("127.0.0.1")));
        assert_eq!(
            header.rpc_request_header.unwrap().broker_name,
            Some(CheetahString::from("broker1"))
        );
    }
}
