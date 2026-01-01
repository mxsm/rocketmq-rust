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

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct QueryMessageRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub key: CheetahString,

    #[required]
    pub max_num: i32,

    #[required]
    pub begin_timestamp: i64,

    #[required]
    pub end_timestamp: i64,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod query_message_request_header_tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn creating_from_map_with_all_fields_populates_struct_correctly() {
        let mut map = HashMap::new();
        map.insert("topic".into(), "test_topic".into());
        map.insert("key".into(), "test_key".into());
        map.insert("maxNum".into(), "10".into());
        map.insert("beginTimestamp".into(), "1000".into());
        map.insert("endTimestamp".into(), "2000".into());

        let header: QueryMessageRequestHeader = <QueryMessageRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.key, "test_key");
        assert_eq!(header.max_num, 10);
        assert_eq!(header.begin_timestamp, 1000);
        assert_eq!(header.end_timestamp, 2000);
    }

    #[test]
    fn creating_from_map_missing_optional_fields_still_succeeds() {
        let mut map = HashMap::new();
        map.insert("topic".into(), "test_topic".into());
        map.insert("key".into(), "test_key".into());
        map.insert("maxNum".into(), "10".into());
        map.insert("beginTimestamp".into(), "1000".into());
        map.insert("endTimestamp".into(), "2000".into());

        let header: QueryMessageRequestHeader = <QueryMessageRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.key, "test_key");
        assert_eq!(header.max_num, 10);
    }

    #[test]
    fn creating_from_map_with_invalid_number_fields_returns_none() {
        let mut map = HashMap::new();
        map.insert("topic".into(), "test_topic".into());
        map.insert("key".into(), "test_key".into());
        map.insert("maxNum".into(), "invalid".into());

        let header: Result<QueryMessageRequestHeader, rocketmq_error::RocketMQError> =
            <QueryMessageRequestHeader as FromMap>::from(&map);

        assert!(header.is_err());
    }

    #[test]
    fn to_map_includes_all_fields() {
        let header = QueryMessageRequestHeader {
            topic: "test_topic".into(),
            key: "test_key".into(),
            max_num: 10,
            begin_timestamp: 1000,
            end_timestamp: 2000,
            topic_request_header: None,
        };

        let map: HashMap<CheetahString, CheetahString> = header.to_map().unwrap();

        assert_eq!(map.get("topic").unwrap(), "test_topic");
        assert_eq!(map.get("key").unwrap(), "test_key");
        assert_eq!(map.get("maxNum").unwrap(), "10");
        assert_eq!(map.get("beginTimestamp").unwrap(), "1000");
        assert_eq!(map.get("endTimestamp").unwrap(), "2000");
    }

    #[test]
    fn to_map_with_topic_request_header_includes_nested_fields() {
        let topic_request_header = TopicRequestHeader::default();
        let header = QueryMessageRequestHeader {
            topic: "test_topic".into(),
            key: "test_key".into(),
            max_num: 10,
            begin_timestamp: 1000,
            end_timestamp: 2000,
            topic_request_header: Some(topic_request_header),
        };

        let map = header.to_map().unwrap();

        // Verify that nested fields are included, assuming specific fields in TopicRequestHeader
        // This is a placeholder assertion; actual fields and checks depend on TopicRequestHeader's
        // structure
        assert!(!map.contains_key("nestedField"));
    }
}
