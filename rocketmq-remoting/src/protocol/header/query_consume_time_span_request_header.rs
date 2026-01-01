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

use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumeTimeSpanRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub group: CheetahString,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn query_consume_time_span_request_header_serializes_correctly() {
        let header = QueryConsumeTimeSpanRequestHeader {
            topic: CheetahString::from_static_str("test_topic"),
            group: CheetahString::from_static_str("test_group"),
            topic_request_header: None,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        let expected = r#"{"topic":"test_topic","group":"test_group"}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn query_consume_time_span_request_header_deserializes_correctly() {
        let data = r#"{"topic":"test_topic","group":"test_group"}"#;
        let header: QueryConsumeTimeSpanRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.topic, CheetahString::from_static_str("test_topic"));
        assert_eq!(header.group, CheetahString::from_static_str("test_group"));
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn query_consume_time_span_request_header_handles_missing_optional_fields() {
        let data = r#"{"topic":"test_topic","group":"test_group"}"#;
        let header: QueryConsumeTimeSpanRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.topic, CheetahString::from_static_str("test_topic"));
        assert_eq!(header.group, CheetahString::from_static_str("test_group"));
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn query_consume_time_span_request_header_handles_invalid_data() {
        let data = r#"{"topic":12345,"group":"test_group"}"#;
        let result: Result<QueryConsumeTimeSpanRequestHeader, _> = serde_json::from_str(data);
        assert!(result.is_err());
    }
}
