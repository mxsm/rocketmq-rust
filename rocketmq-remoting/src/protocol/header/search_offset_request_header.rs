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
use rocketmq_common::common::boundary_type::BoundaryType;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Default, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct SearchOffsetRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub queue_id: i32,

    #[required]
    pub timestamp: i64,

    pub boundary_type: BoundaryType,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl TopicRequestHeaderTrait for SearchOffsetRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        if let Some(header) = self.topic_request_header.as_mut() {
            header.lo = lo;
        }
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().and_then(|h| h.lo)
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc_request_header.as_ref())
            .and_then(|h| h.broker_name.as_ref())
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.broker_name = Some(broker_name);
            }
        }
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc_request_header.as_ref())
            .and_then(|r| r.namespace.as_deref())
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.namespace = Some(namespace);
            }
        }
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc_request_header.as_ref())
            .and_then(|r| r.namespaced)
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.namespaced = Some(namespaced);
            }
        }
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc_request_header.as_ref())
            .and_then(|r| r.oneway)
    }

    fn set_oneway(&mut self, oneway: bool) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc_request_header.as_mut() {
                rpc_header.oneway = Some(oneway);
            }
        }
    }

    fn queue_id(&self) -> i32 {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: i32) {
        self.queue_id = queue_id;
    }
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
            topic_request_header: None,
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
            topic_request_header: None,
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
            topic_request_header: None,
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
            topic_request_header: None,
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
            topic_request_header: None,
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
            topic_request_header: None,
        };

        assert_eq!(header.timestamp, i64::MAX);

        let json = serde_json::to_string(&header).unwrap();
        let deserialized: SearchOffsetRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.timestamp, i64::MAX);
    }

    // Tests for TopicRequestHeaderTrait implementation
    #[test]
    fn topic_request_header_trait_set_and_get_topic() {
        let mut header = SearchOffsetRequestHeader::default();
        let topic = CheetahString::from("test_topic");

        header.set_topic(topic.clone());
        assert_eq!(header.topic(), &topic);
    }

    #[test]
    fn topic_request_header_trait_set_and_get_queue_id() {
        let mut header = SearchOffsetRequestHeader::default();

        header.set_queue_id(5);
        assert_eq!(header.queue_id(), 5);

        header.set_queue_id(-1);
        assert_eq!(header.queue_id(), -1);
    }

    #[test]
    fn topic_request_header_trait_lo_with_none_topic_request_header() {
        let mut header = SearchOffsetRequestHeader::default();

        // Should not panic when topic_request_header is None
        header.set_lo(Some(true));
        assert_eq!(header.lo(), None);
    }

    #[test]
    fn topic_request_header_trait_lo_with_some_topic_request_header() {
        let mut header = SearchOffsetRequestHeader {
            topic: CheetahString::from("test"),
            queue_id: 0,
            timestamp: 0,
            boundary_type: BoundaryType::Lower,
            topic_request_header: Some(TopicRequestHeader::default()),
        };

        header.set_lo(Some(true));
        assert_eq!(header.lo(), Some(true));

        header.set_lo(Some(false));
        assert_eq!(header.lo(), Some(false));

        header.set_lo(None);
        assert_eq!(header.lo(), None);
    }

    #[test]
    fn topic_request_header_trait_broker_name_with_none() {
        let header = SearchOffsetRequestHeader::default();
        assert_eq!(header.broker_name(), None);
    }

    #[test]
    fn topic_request_header_trait_set_broker_name_with_none_topic_request_header() {
        let mut header = SearchOffsetRequestHeader::default();

        // Should not panic when topic_request_header is None
        header.set_broker_name(CheetahString::from("broker1"));
        assert_eq!(header.broker_name(), None);
    }

    #[test]
    fn topic_request_header_trait_namespace_with_none() {
        let header = SearchOffsetRequestHeader::default();
        assert_eq!(header.namespace(), None);
    }

    #[test]
    fn topic_request_header_trait_set_namespace_with_none_topic_request_header() {
        let mut header = SearchOffsetRequestHeader::default();

        // Should not panic when topic_request_header is None
        header.set_namespace(CheetahString::from("namespace1"));
        assert_eq!(header.namespace(), None);
    }

    #[test]
    fn topic_request_header_trait_namespaced_with_none() {
        let header = SearchOffsetRequestHeader::default();
        assert_eq!(header.namespaced(), None);
    }

    #[test]
    fn topic_request_header_trait_set_namespaced_with_none_topic_request_header() {
        let mut header = SearchOffsetRequestHeader::default();

        // Should not panic when topic_request_header is None
        header.set_namespaced(true);
        assert_eq!(header.namespaced(), None);
    }

    #[test]
    fn topic_request_header_trait_oneway_with_none() {
        let header = SearchOffsetRequestHeader::default();
        assert_eq!(header.oneway(), None);
    }

    #[test]
    fn topic_request_header_trait_set_oneway_with_none_topic_request_header() {
        let mut header = SearchOffsetRequestHeader::default();

        // Should not panic when topic_request_header is None
        header.set_oneway(true);
        assert_eq!(header.oneway(), None);
    }

    #[test]
    fn topic_request_header_trait_all_methods_safe_with_none() {
        let mut header = SearchOffsetRequestHeader::default();

        // All these operations should be safe and not panic
        header.set_lo(Some(true));
        header.set_broker_name(CheetahString::from("broker"));
        header.set_namespace(CheetahString::from("ns"));
        header.set_namespaced(true);
        header.set_oneway(false);

        assert_eq!(header.lo(), None);
        assert_eq!(header.broker_name(), None);
        assert_eq!(header.namespace(), None);
        assert_eq!(header.namespaced(), None);
        assert_eq!(header.oneway(), None);
    }
}
