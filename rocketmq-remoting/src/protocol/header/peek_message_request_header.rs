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

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PeekMessageRequestHeader {
    #[required]
    pub consumer_group: CheetahString,

    #[required]
    pub topic: CheetahString,

    /// If negative, peek from all queues
    #[required]
    pub queue_id: i32,

    #[required]
    pub max_msg_nums: i32,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl Default for PeekMessageRequestHeader {
    fn default() -> Self {
        PeekMessageRequestHeader {
            consumer_group: CheetahString::new(),
            topic: CheetahString::new(),
            queue_id: 0,
            max_msg_nums: 32,
            topic_request_header: None,
        }
    }
}

impl TopicRequestHeaderTrait for PeekMessageRequestHeader {
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
    use super::*;
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    #[test]
    fn test_default_implementation() {
        let header = PeekMessageRequestHeader::default();
        assert_eq!(header.consumer_group, CheetahString::new());
        assert_eq!(header.topic, CheetahString::new());
        assert_eq!(header.queue_id, 0);
        assert_eq!(header.max_msg_nums, 32);
        assert!(header.topic_request_header.is_none());
    }
    #[test]
    fn test_topic_request_header_trait_methods() {
        let mut header = PeekMessageRequestHeader::default();

        // Test set_topic and topic
        let test_topic = CheetahString::from("test_topic");
        header.set_topic(test_topic.clone());
        assert_eq!(header.topic(), &test_topic);

        // Test set_queue_id and queue_id
        header.set_queue_id(10);
        assert_eq!(header.queue_id(), 10);

        // Test with topic_request_header set
        let rpc_header = RpcRequestHeader {
            broker_name: Some(CheetahString::from("test_broker")),
            namespace: Some(CheetahString::from("test_namespace")),
            namespaced: Some(true),
            oneway: Some(false),
        };

        let topic_request_header = TopicRequestHeader {
            lo: Some(true),
            rpc: Some(rpc_header),
        };

        header.topic_request_header = Some(topic_request_header);

        // Test broker_name
        assert_eq!(header.broker_name(), Some(&CheetahString::from("test_broker")));

        let new_broker = CheetahString::from("new_broker");
        header.set_broker_name(new_broker.clone());
        assert_eq!(header.broker_name(), Some(&new_broker));

        // Test namespace
        assert_eq!(header.namespace(), Some("test_namespace"));

        let new_namespace = CheetahString::from("new_namespace");
        header.set_namespace(new_namespace);
        assert_eq!(header.namespace(), Some("new_namespace"));

        // Test namespaced
        assert_eq!(header.namespaced(), Some(true));
        header.set_namespaced(false);
        assert_eq!(header.namespaced(), Some(false));

        // Test oneway
        assert_eq!(header.oneway(), Some(false));
        header.set_oneway(true);
        assert_eq!(header.oneway(), Some(true));

        // Test lo
        assert_eq!(header.lo(), Some(true));
        header.set_lo(Some(false));
        assert_eq!(header.lo(), Some(false));
    }

    #[test]
    fn test_serialization_deserialization() {
        let header = PeekMessageRequestHeader {
            consumer_group: CheetahString::from("test_consumer_group"),
            topic: CheetahString::from("test_topic"),
            queue_id: 10,
            max_msg_nums: 32,
            ..Default::default()
        };

        // Serialize to JSON
        let json = serde_json::to_string(&header).unwrap();

        // Deserialize from JSON
        let deserialized: PeekMessageRequestHeader = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.consumer_group, header.consumer_group);
        assert_eq!(deserialized.topic, header.topic);
        assert_eq!(deserialized.queue_id, header.queue_id);
        assert_eq!(deserialized.max_msg_nums, header.max_msg_nums);
    }
}
