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

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct ResetOffsetRequestHeader {
    #[required]
    pub topic: CheetahString,
    #[required]
    pub group: CheetahString,
    pub queue_id: i32,
    pub offset: Option<i64>,
    #[required]
    pub timestamp: i64,
    #[required]
    pub is_force: bool,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl Default for ResetOffsetRequestHeader {
    fn default() -> Self {
        ResetOffsetRequestHeader {
            topic: CheetahString::empty(),
            group: CheetahString::empty(),
            queue_id: -1,
            offset: None,
            timestamp: 0,
            is_force: false,
            topic_request_header: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn reset_offset_request_header_serializes_correctly() {
        let header = ResetOffsetRequestHeader {
            topic: CheetahString::from_static_str("test_topic"),
            group: CheetahString::from_static_str("test_group"),
            queue_id: 1,
            offset: Some(100),
            timestamp: 1234567890,
            is_force: true,
            topic_request_header: None,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        let expected = r#"{"topic":"test_topic","group":"test_group","queueId":1,"offset":100,"timestamp":1234567890,"isForce":true}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn reset_offset_request_header_deserializes_correctly() {
        let data = r#"{"topic":"test_topic","group":"test_group","queueId":1,"offset":100,"timestamp":1234567890,"isForce":true}"#;
        let header: ResetOffsetRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.topic, CheetahString::from_static_str("test_topic"));
        assert_eq!(header.group, CheetahString::from_static_str("test_group"));
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.offset.unwrap(), 100);
        assert_eq!(header.timestamp, 1234567890);
        assert!(header.is_force);
    }

    #[test]
    fn reset_offset_request_header_handles_missing_optional_fields() {
        let data = r#"{"topic":"test_topic","group":"test_group","queueId":1,"timestamp":1234567890,"isForce":true}"#;
        let header: ResetOffsetRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.topic, CheetahString::from_static_str("test_topic"));
        assert_eq!(header.group, CheetahString::from_static_str("test_group"));
        assert_eq!(header.queue_id, 1);
        assert!(header.offset.is_none());
        assert_eq!(header.timestamp, 1234567890);
        assert!(header.is_force);
    }

    #[test]
    fn reset_offset_request_header_handles_invalid_data() {
        let data = r#"{"topic":12345,"group":"test_group","queueId":1,"timestamp":1234567890,"isForce":true}"#;
        let result: Result<ResetOffsetRequestHeader, _> = serde_json::from_str(data);
        assert!(result.is_err());
    }

    #[test]
    fn reset_offset_request_header_default_values() {
        let header = ResetOffsetRequestHeader::default();
        assert_eq!(header.topic, CheetahString::empty());
        assert_eq!(header.group, CheetahString::empty());
        assert_eq!(header.queue_id, -1);
        assert!(header.offset.is_none());
        assert_eq!(header.timestamp, 0);
        assert!(!header.is_force);
    }
}
