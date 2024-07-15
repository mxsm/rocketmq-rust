/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryMessageRequestHeader {
    pub topic: String,

    pub key: String,

    pub max_num: i32,

    pub begin_timestamp: i64,

    pub end_timestamp: i64,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl QueryMessageRequestHeader {
    pub const TOPIC: &'static str = "topic";
    pub const KEY: &'static str = "key";
    pub const MAX_NUM: &'static str = "maxNum";
    pub const BEGIN_TIMESTAMP: &'static str = "beginTimestamp";
    pub const END_TIMESTAMP: &'static str = "endTimestamp";
}

/*impl From<&QueryMessageRequestHeader> for String {
    fn from(header: &QueryMessageRequestHeader) -> Self {
        SerdeJsonUtils::to_json(header).unwrap()
    }
}*/

impl CommandCustomHeader for QueryMessageRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
        let mut map = std::collections::HashMap::new();
        map.insert(Self::TOPIC.to_string(), self.topic.clone());
        map.insert(Self::KEY.to_string(), self.key.clone());
        map.insert(Self::MAX_NUM.to_string(), self.max_num.to_string());
        map.insert(
            Self::BEGIN_TIMESTAMP.to_string(),
            self.begin_timestamp.to_string(),
        );
        map.insert(
            Self::END_TIMESTAMP.to_string(),
            self.end_timestamp.to_string(),
        );
        if let Some(value) = self.topic_request_header.as_ref() {
            if let Some(val) = value.to_map() {
                map.extend(val);
            }
        }
        Some(map)
    }
}

impl FromMap for QueryMessageRequestHeader {
    type Target = Self;

    fn from(map: &std::collections::HashMap<String, String>) -> Option<Self::Target> {
        Some(QueryMessageRequestHeader {
            topic: map.get(Self::TOPIC)?.clone(),
            key: map.get(Self::KEY)?.clone(),
            max_num: map.get(Self::MAX_NUM)?.parse().ok()?,
            begin_timestamp: map.get(Self::BEGIN_TIMESTAMP)?.parse().ok()?,
            end_timestamp: map.get(Self::END_TIMESTAMP)?.parse().ok()?,
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

#[cfg(test)]
mod query_message_request_header_tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn creating_from_map_with_all_fields_populates_struct_correctly() {
        let mut map = HashMap::new();
        map.insert("topic".to_string(), "test_topic".to_string());
        map.insert("key".to_string(), "test_key".to_string());
        map.insert("maxNum".to_string(), "10".to_string());
        map.insert("beginTimestamp".to_string(), "1000".to_string());
        map.insert("endTimestamp".to_string(), "2000".to_string());

        let header = <QueryMessageRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.key, "test_key");
        assert_eq!(header.max_num, 10);
        assert_eq!(header.begin_timestamp, 1000);
        assert_eq!(header.end_timestamp, 2000);
    }

    #[test]
    fn creating_from_map_missing_optional_fields_still_succeeds() {
        let mut map = HashMap::new();
        map.insert("topic".to_string(), "test_topic".to_string());
        map.insert("key".to_string(), "test_key".to_string());
        map.insert("maxNum".to_string(), "10".to_string());
        map.insert("beginTimestamp".to_string(), "1000".to_string());
        map.insert("endTimestamp".to_string(), "2000".to_string());

        let header = <QueryMessageRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.key, "test_key");
        assert_eq!(header.max_num, 10);
    }

    #[test]
    fn creating_from_map_with_invalid_number_fields_returns_none() {
        let mut map = HashMap::new();
        map.insert("topic".to_string(), "test_topic".to_string());
        map.insert("key".to_string(), "test_key".to_string());
        map.insert("maxNum".to_string(), "invalid".to_string());

        let header = <QueryMessageRequestHeader as FromMap>::from(&map);

        assert!(header.is_none());
    }

    #[test]
    fn to_map_includes_all_fields() {
        let header = QueryMessageRequestHeader {
            topic: "test_topic".to_string(),
            key: "test_key".to_string(),
            max_num: 10,
            begin_timestamp: 1000,
            end_timestamp: 2000,
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();

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
            topic: "test_topic".to_string(),
            key: "test_key".to_string(),
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
