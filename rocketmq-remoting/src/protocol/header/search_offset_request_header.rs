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
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Default, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct SearchOffsetRequestHeader {
    pub topic: CheetahString,
    pub queue_id: i32,
    pub timestamp: i64,
    pub boundary_type: BoundaryType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_offset_request_header_default() {
        let header = SearchOffsetRequestHeader::default();
        assert_eq!(header.topic, CheetahString::default());
        assert_eq!(header.queue_id, 0);
        assert_eq!(header.timestamp, 0);
        assert_eq!(header.boundary_type, BoundaryType::Lower);
    }

    #[test]
    fn search_offset_request_header_creation() {
        let header = SearchOffsetRequestHeader {
            topic: CheetahString::from("test_topic"),
            queue_id: 1,
            timestamp: 1702345678000,
            boundary_type: BoundaryType::Upper,
        };

        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.timestamp, 1702345678000);
        assert_eq!(header.boundary_type, BoundaryType::Upper);
    }

    #[test]
    fn search_offset_request_header_serializes_to_json() {
        let header = SearchOffsetRequestHeader {
            topic: CheetahString::from("my_topic"),
            queue_id: 2,
            timestamp: 1702345678999,
            boundary_type: BoundaryType::Upper,
        };

        let json = serde_json::to_string(&header).unwrap();

        // Verify camelCase field names and uppercase enum
        assert!(json.contains(r#""topic":"my_topic""#));
        assert!(json.contains(r#""queueId":2"#));
        assert!(json.contains(r#""timestamp":1702345678999"#));
        assert!(json.contains(r#""boundaryType":"UPPER""#));
    }

    #[test]
    fn search_offset_request_header_deserializes_from_json() {
        let json = r#"{
            "topic": "test_topic",
            "queueId": 3,
            "timestamp": 1702345678123,
            "boundaryType": "LOWER"
        }"#;

        let header: SearchOffsetRequestHeader = serde_json::from_str(json).unwrap();

        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert_eq!(header.queue_id, 3);
        assert_eq!(header.timestamp, 1702345678123);
        assert_eq!(header.boundary_type, BoundaryType::Lower);
    }

    #[test]
    fn search_offset_request_header_deserializes_with_uppercase_boundary_type() {
        let json = r#"{
            "topic": "topic1",
            "queueId": 0,
            "timestamp": 1000000000,
            "boundaryType": "UPPER"
        }"#;

        let header: SearchOffsetRequestHeader = serde_json::from_str(json).unwrap();

        assert_eq!(header.boundary_type, BoundaryType::Upper);
    }

    #[test]
    fn search_offset_request_header_deserializes_with_lowercase_boundary_type() {
        let json = r#"{
            "topic": "topic2",
            "queueId": 5,
            "timestamp": 2000000000,
            "boundaryType": "lower"
        }"#;

        let header: SearchOffsetRequestHeader = serde_json::from_str(json).unwrap();

        assert_eq!(header.boundary_type, BoundaryType::Lower);
    }

    #[test]
    fn search_offset_request_header_boundary_type_defaults_to_lower_for_invalid_value() {
        let json = r#"{
            "topic": "topic3",
            "queueId": 1,
            "timestamp": 3000000000,
            "boundaryType": "invalid"
        }"#;

        let header: SearchOffsetRequestHeader = serde_json::from_str(json).unwrap();

        // Matches Java behavior: defaults to Lower
        assert_eq!(header.boundary_type, BoundaryType::Lower);
    }

    #[test]
    fn search_offset_request_header_roundtrip_serialization() {
        let original = SearchOffsetRequestHeader {
            topic: CheetahString::from("roundtrip_topic"),
            queue_id: 10,
            timestamp: 1702400000000,
            boundary_type: BoundaryType::Upper,
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: SearchOffsetRequestHeader = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.topic, original.topic);
        assert_eq!(deserialized.queue_id, original.queue_id);
        assert_eq!(deserialized.timestamp, original.timestamp);
        assert_eq!(deserialized.boundary_type, original.boundary_type);
    }

    #[test]
    fn search_offset_request_header_with_negative_queue_id() {
        let header = SearchOffsetRequestHeader {
            topic: CheetahString::from("test"),
            queue_id: -1,
            timestamp: 1000,
            boundary_type: BoundaryType::Lower,
        };

        assert_eq!(header.queue_id, -1);
    }

    #[test]
    fn search_offset_request_header_with_empty_topic() {
        let header = SearchOffsetRequestHeader {
            topic: CheetahString::new(),
            queue_id: 0,
            timestamp: 0,
            boundary_type: BoundaryType::Lower,
        };

        assert!(header.topic.is_empty());
    }

    #[test]
    fn search_offset_request_header_with_large_timestamp() {
        let header = SearchOffsetRequestHeader {
            topic: CheetahString::from("test"),
            queue_id: 0,
            timestamp: i64::MAX,
            boundary_type: BoundaryType::Upper,
        };

        assert_eq!(header.timestamp, i64::MAX);

        let json = serde_json::to_string(&header).unwrap();
        let deserialized: SearchOffsetRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.timestamp, i64::MAX);
    }
}
