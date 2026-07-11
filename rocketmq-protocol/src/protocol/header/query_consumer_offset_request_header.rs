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

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryConsumerOffsetRequestHeader {
    pub consumer_group: CheetahString,

    pub topic: CheetahString,

    pub queue_id: i32,

    pub set_zero_if_not_found: Option<bool>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl QueryConsumerOffsetRequestHeader {
    pub fn new(consumer_group: impl Into<CheetahString>, topic: impl Into<CheetahString>, queue_id: i32) -> Self {
        Self {
            consumer_group: consumer_group.into(),
            topic: topic.into(),
            queue_id,
            set_zero_if_not_found: None,
            topic_request_header: None,
        }
    }
}

impl TopicRequestHeaderTrait for QueryConsumerOffsetRequestHeader {
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
            .and_then(|h| h.rpc.as_ref())
            .and_then(|h| h.broker_name.as_ref())
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
                rpc_header.broker_name = Some(broker_name);
            }
        }
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|r| r.namespace.as_deref())
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
                rpc_header.namespace = Some(namespace);
            }
        }
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|r| r.namespaced)
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
                rpc_header.namespaced = Some(namespaced);
            }
        }
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|r| r.oneway)
    }

    fn set_oneway(&mut self, oneway: bool) {
        if let Some(header) = self.topic_request_header.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
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
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    #[test]
    fn query_consumer_offset_request_header_default() {
        let header = QueryConsumerOffsetRequestHeader::default();
        assert_eq!(header.consumer_group, "");
        assert_eq!(header.topic, "");
        assert_eq!(header.queue_id, 0);
        assert!(header.set_zero_if_not_found.is_none());
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn query_consumer_offset_request_header_new() {
        let header = QueryConsumerOffsetRequestHeader::new("group", "topic", 1);
        assert_eq!(header.consumer_group, "group");
        assert_eq!(header.topic, "topic");
        assert_eq!(header.queue_id, 1);
        assert!(header.set_zero_if_not_found.is_none());
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn query_consumer_offset_request_header_trait_impl() {
        let mut header = QueryConsumerOffsetRequestHeader::default();

        assert!(header.lo().is_none());
        header.topic_request_header = Some(TopicRequestHeader::default());
        header.set_lo(Some(true));
        assert_eq!(header.lo(), Some(true));

        header.set_topic(CheetahString::from("test_topic"));
        assert_eq!(header.topic(), &CheetahString::from("test_topic"));

        assert!(header.broker_name().is_none());
        header.topic_request_header.as_mut().unwrap().rpc = Some(RpcRequestHeader::default());
        header.set_broker_name(CheetahString::from("broker"));
        assert_eq!(header.broker_name(), Some(&CheetahString::from("broker")));

        assert!(header.namespace().is_none());
        header.set_namespace(CheetahString::from("ns"));
        assert_eq!(header.namespace(), Some("ns"));

        assert!(header.namespaced().is_none());
        header.set_namespaced(true);
        assert_eq!(header.namespaced(), Some(true));

        assert!(header.oneway().is_none());
        header.set_oneway(true);
        assert_eq!(header.oneway(), Some(true));

        header.set_queue_id(1);
        assert_eq!(header.queue_id(), 1);
    }

    #[test]
    fn query_consumer_offset_request_header_serialization() {
        let header = QueryConsumerOffsetRequestHeader {
            consumer_group: CheetahString::from("group"),
            topic: CheetahString::from("topic"),
            queue_id: 1,
            set_zero_if_not_found: Some(true),
            topic_request_header: Some(TopicRequestHeader {
                lo: Some(true),
                ..Default::default()
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"consumerGroup\":\"group\""));
        assert!(json.contains("\"topic\":\"topic\""));
        assert!(json.contains("\"queueId\":1"));
        assert!(json.contains("\"setZeroIfNotFound\":true"));
        assert!(json.contains("\"lo\":true"));
    }

    #[test]
    fn query_consumer_offset_request_header_deserialization() {
        let json = r#"{"consumerGroup":"group","topic":"topic","queueId":1,"setZeroIfNotFound":true,"lo":true}"#;
        let header: QueryConsumerOffsetRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.consumer_group, "group");
        assert_eq!(header.topic, "topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.set_zero_if_not_found, Some(true));
        assert_eq!(header.topic_request_header.unwrap().lo, Some(true));
    }

    #[test]
    fn query_consumer_offset_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("consumerGroup"), CheetahString::from("group1"));
        map.insert(CheetahString::from("topic"), CheetahString::from("topic1"));
        map.insert(CheetahString::from("queueId"), CheetahString::from("2"));
        map.insert(CheetahString::from("setZeroIfNotFound"), CheetahString::from("true"));
        map.insert(CheetahString::from("lo"), CheetahString::from("true"));

        let header = <QueryConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "group1");
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.queue_id, 2);
        assert_eq!(header.set_zero_if_not_found, Some(true));
        assert_eq!(header.topic_request_header.unwrap().lo, Some(true));
    }
}
