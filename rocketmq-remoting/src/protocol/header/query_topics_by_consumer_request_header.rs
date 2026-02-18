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

    #[test]
    fn debug_impl_contains_group() {
        let header = QueryTopicsByConsumerRequestHeader::new("dbg_group");
        let s = format!("{:?}", header);
        assert!(s.contains("group"));
        assert!(s.contains("dbg_group"));
    }

    #[test]
    fn getter_setter_multiple_calls() {
        let mut header = QueryTopicsByConsumerRequestHeader::new("g1");
        assert_eq!(header.get_group(), "g1");
        header.set_group(CheetahString::from("g2"));
        assert_eq!(header.get_group(), "g2");
        header.set_group(CheetahString::from("g3"));
        assert_eq!(header.get_group(), "g3");
    }

    #[test]
    fn serde_deserialize_missing_required_field_errors() {
        let json = r#"{}"#;
        let res: Result<QueryTopicsByConsumerRequestHeader, _> = serde_json::from_str(json);
        assert!(res.is_err());
    }

    #[test]
    fn serialization_includes_group_and_flattened_none() {
        let header = QueryTopicsByConsumerRequestHeader::new("");
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"group\":"));
        // rpc_request_header is None so brokerName should not appear
        assert!(!json.contains("brokerName"));
    }

    #[test]
    fn serialization_with_rpc_request_header_some() {
        let header = QueryTopicsByConsumerRequestHeader {
            group: CheetahString::from("g_with_rpc"),
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from("brokerX")),
                ..Default::default()
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"group\":\"g_with_rpc\""));
        assert!(json.contains("\"brokerName\":\"brokerX\""));
    }

    #[test]
    fn deserialization_with_extra_fields_ignored() {
        let json = r#"{"group":"gextra","unknownField":"x"}"#;
        let header: QueryTopicsByConsumerRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.group, "gextra");
    }

    #[test]
    fn round_trip_serialization_deserialization() {
        let header = QueryTopicsByConsumerRequestHeader {
            group: CheetahString::from("round"),
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from("rb")),
                ..Default::default()
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        let header2: QueryTopicsByConsumerRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header2.group, "round");
        assert_eq!(
            header2.rpc_request_header.unwrap().broker_name,
            Some(CheetahString::from("rb"))
        );
    }

    #[test]
    fn nested_rpc_request_header_access_none_and_some() {
        let header_none = QueryTopicsByConsumerRequestHeader::new("g_none");
        assert!(header_none
            .rpc_request_header
            .as_ref()
            .and_then(|h| h.broker_name.clone())
            .is_none());

        let header_some = QueryTopicsByConsumerRequestHeader {
            group: CheetahString::from("g_some"),
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from("bk1")),
                ..Default::default()
            }),
        };
        assert_eq!(
            header_some
                .rpc_request_header
                .as_ref()
                .and_then(|h| h.broker_name.clone()),
            Some(CheetahString::from("bk1"))
        );
    }

    #[test]
    fn special_characters_and_long_group_names() {
        let special = "g-💖-\n-\u{2764}";
        let header = QueryTopicsByConsumerRequestHeader::new(special);
        let json = serde_json::to_string(&header).unwrap();
        let header2: QueryTopicsByConsumerRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header2.group, special);

        let long = "a".repeat(5000);
        let header_long = QueryTopicsByConsumerRequestHeader::new(long.clone());
        let json_long = serde_json::to_string(&header_long).unwrap();
        let header_long2: QueryTopicsByConsumerRequestHeader = serde_json::from_str(&json_long).unwrap();
        assert_eq!(header_long2.group, long.as_str());
    }

    #[test]
    fn malformed_json_and_wrong_field_types_error() {
        let bad = "{";
        let res: Result<QueryTopicsByConsumerRequestHeader, _> = serde_json::from_str(bad);
        assert!(res.is_err());

        let wrong_type = r#"{"group":123}"#;
        let res2: Result<QueryTopicsByConsumerRequestHeader, _> = serde_json::from_str(wrong_type);
        assert!(res2.is_err());
    }

    #[test]
    fn struct_size_check_and_empty_group_behavior() {
        use std::mem;
        let _sz = mem::size_of::<QueryTopicsByConsumerRequestHeader>();
        assert!(_sz > 0);

        let header_empty = QueryTopicsByConsumerRequestHeader::new("");
        let json = serde_json::to_string(&header_empty).unwrap();
        assert!(json.contains("\"group\":\"\""));
        let header2: QueryTopicsByConsumerRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(header2.group, "");
    }
}
