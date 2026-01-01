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

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2)]
pub struct QueryTopicConsumeByWhoRequestHeader {
    #[required]
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn query_topic_consume_by_who_request_header_serializes_correctly() {
        let header: QueryTopicConsumeByWhoRequestHeader = QueryTopicConsumeByWhoRequestHeader {
            topic: CheetahString::from_static_str("test_topic"),
            topic_request_header: Some(TopicRequestHeader::default()),
        };
        let map = header.to_map().unwrap();
        assert_eq!(map.get(&CheetahString::from_static_str("topic")).unwrap(), "test_topic");
        assert_eq!(map.get(&CheetahString::from_static_str("topicRequestHeader")), None);
    }

    #[test]
    fn query_topic_consume_by_who_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );

        let header: QueryTopicConsumeByWhoRequestHeader =
            <QueryTopicConsumeByWhoRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, "test_topic");
    }

    #[test]
    fn query_topic_consume_by_who_request_header_deserializes_correctly_deserialize_from_json() {
        // 测试 topic_request_header.lo 为 None 的情况
        let data = r#"{"topic":"test_topic"}"#;
        let header: QueryTopicConsumeByWhoRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.topic, CheetahString::from_static_str("test_topic"));
        assert!(header
            .topic_request_header
            .as_ref()
            .map(|h| h.lo.is_none())
            .unwrap_or(true));

        let data = r#"{"topic":"test_topic","lo":null}"#;
        let header: QueryTopicConsumeByWhoRequestHeader = serde_json::from_str(data).unwrap();
        assert!(header
            .topic_request_header
            .as_ref()
            .map(|h| h.lo.is_none())
            .unwrap_or(true));
    }
}
