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

/// Represents the request header for acknowledging a message.
#[derive(Debug, Serialize, Deserialize, Clone, RequestHeaderCodecV2)]
pub struct AckMessageRequestHeader {
    /// Consumer group name (required)
    #[serde(rename = "consumerGroup")]
    #[required]
    pub consumer_group: CheetahString,

    /// Topic name (required)
    #[serde(rename = "topic")]
    #[required]
    pub topic: CheetahString,

    /// Queue ID (required)
    #[serde(rename = "queueId")]
    #[required]
    pub queue_id: i32,

    /// Extra information (required)
    #[serde(rename = "extraInfo")]
    #[required]
    pub extra_info: CheetahString,

    /// Offset (required)
    #[serde(rename = "offset")]
    #[required]
    pub offset: i64,

    #[serde(rename = "liteTopic", skip_serializing_if = "Option::is_none")]
    pub lite_topic: Option<CheetahString>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use serde_json;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn serialize_ack_message_request_header() {
        let header = AckMessageRequestHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            extra_info: CheetahString::from("extra_info"),
            offset: 12345,
            lite_topic: Some(CheetahString::from("lite_topic")),
            topic_request_header: None,
        };
        let json = serde_json::to_string(&header).unwrap();
        let expected = r#"{"consumerGroup":"test_group","topic":"test_topic","queueId":1,"extraInfo":"extra_info","offset":12345,"liteTopic":"lite_topic"}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn deserialize_ack_message_request_header() {
        let json = r#"{"consumerGroup":"test_group","topic":"test_topic","queueId":1,"extraInfo":"extra_info","offset":12345,"liteTopic":"lite_topic"}"#;
        let header: AckMessageRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from("test_group"));
        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.extra_info, CheetahString::from("extra_info"));
        assert_eq!(header.offset, 12345);
        assert_eq!(header.lite_topic.as_deref(), Some("lite_topic"));
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn deserialize_ack_message_request_header_with_topic_request_header() {
        let json = r#"{"consumerGroup":"test_group","topic":"test_topic","queueId":1,"extraInfo":"extra_info","offset":12345,"topicRequestHeader":{"someField":"someValue"}}"#;
        let header: AckMessageRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from("test_group"));
        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.extra_info, CheetahString::from("extra_info"));
        assert_eq!(header.offset, 12345);
        assert!(header.lite_topic.is_none());
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn serialize_ack_message_request_header_with_topic_request_header() {
        let header = AckMessageRequestHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            extra_info: CheetahString::from("extra_info"),
            offset: 12345,
            lite_topic: None,
            topic_request_header: None,
        };
        let json = serde_json::to_string(&header).unwrap();
        let expected = r#"{"consumerGroup":"test_group","topic":"test_topic","queueId":1,"extraInfo":"extra_info","offset":12345}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn ack_message_request_header_maps_lite_topic_like_java_header() {
        let header = AckMessageRequestHeader {
            consumer_group: CheetahString::from("test_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            extra_info: CheetahString::from("extra_info"),
            offset: 12345,
            lite_topic: Some(CheetahString::from("lite_topic")),
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(map.get("liteTopic").map(|value| value.as_str()), Some("lite_topic"));

        let decoded = <AckMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(decoded.lite_topic.as_deref(), Some("lite_topic"));
    }
}
