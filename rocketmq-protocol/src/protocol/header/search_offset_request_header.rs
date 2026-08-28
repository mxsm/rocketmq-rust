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
use rocketmq_macros::RequestHeaderCodecV3;
use rocketmq_model::boundary_type::BoundaryType;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Default, Debug, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct SearchOffsetRequestHeader {
    #[header(required)]
    pub topic: CheetahString,

    pub lite_topic: Option<CheetahString>,

    #[header(required)]
    pub queue_id: i32,

    #[header(required)]
    pub timestamp: i64,

    #[header(default, default_semantic = "literal:LOWER")]
    pub boundary_type: BoundaryType,

    #[serde(flatten)]
    #[header(flatten, presence = "any")]
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
    use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    fn header_with_rpc_envelope() -> SearchOffsetRequestHeader {
        SearchOffsetRequestHeader {
            topic: CheetahString::from("topic-a"),
            lite_topic: Some(CheetahString::from("lite-topic-a")),
            queue_id: 1,
            timestamp: i64::MAX,
            boundary_type: BoundaryType::Upper,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader::default()),
                lo: None,
            }),
        }
    }

    #[test]
    fn v3_codec_preserves_java_keys_and_numeric_boundaries() {
        let header = SearchOffsetRequestHeader {
            topic_request_header: None,
            ..header_with_rpc_envelope()
        };
        let map = header.to_map().unwrap();

        assert_eq!(map.get("topic").map(|value| value.as_str()), Some("topic-a"));
        assert_eq!(map.get("liteTopic").map(|value| value.as_str()), Some("lite-topic-a"));

        let decoded = <SearchOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(decoded.topic, "topic-a");
        assert_eq!(decoded.lite_topic.as_deref(), Some("lite-topic-a"));
        assert_eq!(decoded.queue_id, 1);
        assert_eq!(decoded.timestamp, i64::MAX);
        assert_eq!(decoded.boundary_type, BoundaryType::Upper);
    }

    #[test]
    fn serde_uses_java_names_and_the_uppercase_boundary_value() {
        let header = SearchOffsetRequestHeader {
            topic_request_header: None,
            ..header_with_rpc_envelope()
        };
        let value = serde_json::to_value(&header).unwrap();

        assert_eq!(value["topic"], "topic-a");
        assert_eq!(value["liteTopic"], "lite-topic-a");
        assert_eq!(value["queueId"], 1);
        assert_eq!(value["timestamp"], i64::MAX);
        assert_eq!(value["boundaryType"], "UPPER");

        let decoded: SearchOffsetRequestHeader = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.boundary_type, BoundaryType::Upper);
    }

    #[test]
    fn serde_matches_java_fallback_for_non_upper_boundary_values() {
        for boundary_type in ["LOWER", "lower", "invalid"] {
            let json = format!(r#"{{"topic":"topic-a","queueId":1,"timestamp":1,"boundaryType":"{boundary_type}"}}"#);
            let decoded: SearchOffsetRequestHeader = serde_json::from_str(&json).unwrap();

            assert_eq!(decoded.boundary_type, BoundaryType::Lower);
        }
    }

    #[test]
    fn topic_request_trait_reads_and_updates_every_forwarded_field() {
        let mut header = header_with_rpc_envelope();

        header.set_topic(CheetahString::from("topic-b"));
        header.set_queue_id(2);
        header.set_lo(Some(true));
        header.set_broker_name(CheetahString::from("broker-a"));
        header.set_namespace(CheetahString::from("namespace-a"));
        header.set_namespaced(true);
        header.set_oneway(false);

        assert_eq!(header.topic(), "topic-b");
        assert_eq!(header.queue_id(), 2);
        assert_eq!(header.lo(), Some(true));
        assert_eq!(header.broker_name().map(|value| value.as_str()), Some("broker-a"));
        assert_eq!(header.namespace(), Some("namespace-a"));
        assert_eq!(header.namespaced(), Some(true));
        assert_eq!(header.oneway(), Some(false));
    }

    #[test]
    fn nested_setters_are_noops_without_an_rpc_envelope() {
        let mut header = SearchOffsetRequestHeader::default();

        header.set_lo(Some(true));
        header.set_broker_name(CheetahString::from("broker-a"));
        header.set_namespace(CheetahString::from("namespace-a"));
        header.set_namespaced(true);
        header.set_oneway(true);

        assert_eq!(header.lo(), None);
        assert_eq!(header.broker_name(), None);
        assert_eq!(header.namespace(), None);
        assert_eq!(header.namespaced(), None);
        assert_eq!(header.oneway(), None);
    }
}
