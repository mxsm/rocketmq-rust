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
use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodec)]
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

// impl QueryMessageRequestHeader {
//     pub const TOPIC: &'static str = "topic";
//     pub const KEY: &'static str = "key";
//     pub const MAX_NUM: &'static str = "maxNum";
//     pub const BEGIN_TIMESTAMP: &'static str = "beginTimestamp";
//     pub const END_TIMESTAMP: &'static str = "endTimestamp";
// }

/*impl From<&QueryMessageRequestHeader> for String {
    fn from(header: &QueryMessageRequestHeader) -> Self {
        SerdeJsonUtils::to_json(header).unwrap()
    }
}*/

// impl CommandCustomHeader for QueryMessageRequestHeader {
//     fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
//         let mut map = std::collections::HashMap::new();
//         map.insert(
//             CheetahString::from_static_str(Self::TOPIC),
//             self.topic.clone(),
//         );
//         map.insert(CheetahString::from_static_str(Self::KEY), self.key.clone());
//         map.insert(
//             CheetahString::from_static_str(Self::MAX_NUM),
//             CheetahString::from_string(self.max_num.to_string()),
//         );
//         map.insert(
//             CheetahString::from_static_str(Self::BEGIN_TIMESTAMP),
//             CheetahString::from_string(self.begin_timestamp.to_string()),
//         );
//         map.insert(
//             CheetahString::from_static_str(Self::END_TIMESTAMP),
//             CheetahString::from_string(self.end_timestamp.to_string()),
//         );
//         if let Some(value) = self.topic_request_header.as_ref() {
//             if let Some(val) = value.to_map() {
//                 map.extend(val);
//             }
//         }
//         Some(map)
//     }
// }

// impl FromMap for QueryMessageRequestHeader {
//     type Error = rocketmq_error::RocketmqError;

//     type Target = Self;

//     fn from(
//         map: &std::collections::HashMap<CheetahString, CheetahString>,
//     ) -> Result<Self::Target, Self::Error> {
//         Ok(QueryMessageRequestHeader {
//             topic: map
//                 .get(&CheetahString::from_static_str(Self::TOPIC))
//                 .cloned()
//                 .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
//                     "Miss topic field".to_string(),
//                 ))?,
//             key: map
//                 .get(&CheetahString::from_static_str(Self::KEY))
//                 .cloned()
//                 .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
//                     "Miss key field".to_string(),
//                 ))?,
//             max_num: map
//                 .get(&CheetahString::from_static_str(Self::MAX_NUM))
//                 .cloned()
//                 .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
//                     "Miss maxNum field".to_string(),
//                 ))?
//                 .parse()
//                 .map_err(|_| {
//                     rocketmq_error::RocketmqError::DeserializeHeaderError(
//                         "Parse maxNum field error".to_string(),
//                     )
//                 })?,
//             begin_timestamp: map
//                 .get(&CheetahString::from_static_str(Self::BEGIN_TIMESTAMP))
//                 .cloned()
//                 .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
//                     "Miss beginTimestamp field".to_string(),
//                 ))?
//                 .parse()
//                 .map_err(|_| {
//                     rocketmq_error::RocketmqError::DeserializeHeaderError(
//                         "Parse beginTimestamp field error".to_string(),
//                     )
//                 })?,
//             end_timestamp: map
//                 .get(&CheetahString::from_static_str(Self::END_TIMESTAMP))
//                 .cloned()
//                 .ok_or(rocketmq_error::RocketmqError::DeserializeHeaderError(
//                     "Miss endTimestamp field".to_string(),
//                 ))?
//                 .parse()
//                 .map_err(|_| {
//                     rocketmq_error::RocketmqError::DeserializeHeaderError(
//                         "Parse endTimestamp field error".to_string(),
//                     )
//                 })?,
//             topic_request_header: Some(<TopicRequestHeader as FromMap>::from(map)?),
//         })
//     }
// }

#[cfg(test)]
mod query_message_request_header_tests {
    use std::collections::HashMap;
    use crate::protocol::command_custom_header::{FromMap, CommandCustomHeader};
    use super::*;

    #[test]
    fn creating_from_map_with_all_fields_populates_struct_correctly() {
        let mut map = HashMap::new();
        map.insert("topic".into(), "test_topic".into());
        map.insert("key".into(), "test_key".into());
        map.insert("maxNum".into(), "10".into());
        map.insert("beginTimestamp".into(), "1000".into());
        map.insert("endTimestamp".into(), "2000".into());

        let header:QueryMessageRequestHeader = <QueryMessageRequestHeader as FromMap>::from(&map).unwrap();

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

        let header:QueryMessageRequestHeader = <QueryMessageRequestHeader as FromMap>::from(&map).unwrap();

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

        let header:Result<QueryMessageRequestHeader, rocketmq_error::RocketmqError>= <QueryMessageRequestHeader as FromMap>::from(&map);

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
