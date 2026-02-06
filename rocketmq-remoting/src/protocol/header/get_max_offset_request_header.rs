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
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetMaxOffsetRequestHeader {
    pub topic: CheetahString,

    pub queue_id: i32,

    pub committed: bool,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl TopicRequestHeaderTrait for GetMaxOffsetRequestHeader {
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
    use crate::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
    use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
    use crate::rpc::rpc_request_header::RpcRequestHeader;
    use crate::rpc::topic_request_header::TopicRequestHeader;
    use cheetah_string::CheetahString;
    use cheetah_string::{self};
    #[test]
    fn get_max_offset_request_header_with_required_fields_only() {
        let header = GetMaxOffsetRequestHeader {
            topic: cheetah_string::CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: None,
        };

        assert_eq!(header.topic, cheetah_string::CheetahString::from("testTopic"));
        assert_eq!(header.queue_id, 1);
        assert!(header.committed);
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn get_max_offset_request_header_with_all_fields() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };

        assert_eq!(header.topic, CheetahString::from("testTopic"));
        assert_eq!(header.queue_id, 1);
        assert!(header.committed);
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn get_max_offset_request_header_with_empty_topic() {
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from(""),
            queue_id: 0,
            committed: false,
            topic_request_header: None,
        };
        assert_eq!(header.topic, CheetahString::from(""));
        assert_eq!(header.queue_id, 0);
        assert!(!header.committed);
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn get_max_offset_request_header_with_long_values() {
        let long_string = "a".repeat(1000);
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from(&long_string),
            queue_id: 1,
            committed: true,
            topic_request_header: None,
        };
        assert_eq!(header.topic, CheetahString::from(&long_string));
        assert_eq!(header.queue_id, 1);
        assert!(header.committed);
        assert!(header.topic_request_header.is_none());
    }

    #[test]
    fn fn_lo() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };

        assert_eq!(header.lo(), Some(true));
    }

    #[test]
    fn fn_set_lo() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };

        header.set_lo(Some(false));
        assert_eq!(header.lo(), Some(false));
    }

    #[test]
    fn fn_topic() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        assert_eq!(header.topic(), &CheetahString::from("testTopic"));
    }

    #[test]
    fn fn_set_topic() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        header.set_topic(CheetahString::from("test_topic"));
        assert_eq!(header.topic(), &CheetahString::from("test_topic"));
    }

    #[test]
    fn fn_broker_name() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        assert_eq!(header.broker_name(), Some(&CheetahString::from("broker-0")));
    }

    #[test]
    fn fn_set_broker_name() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        header.set_broker_name(CheetahString::from("broker-1"));
        assert_eq!(header.broker_name(), Some(&CheetahString::from("broker-1")));
    }

    #[test]
    fn fn_namespace() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        assert_eq!(header.namespace(), Some(CheetahString::from("ns1")).as_deref());
    }

    #[test]
    fn fn_set_namespace() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        header.set_namespace(CheetahString::from("ns2"));
        assert_eq!(header.namespace(), Some(CheetahString::from("ns2")).as_deref());
    }

    #[test]
    fn fn_namespaced() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        assert_eq!(header.namespaced(), Some(true));
    }

    #[test]
    fn fn_set_namespaced() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        header.set_namespaced(false);
        assert_eq!(header.namespaced(), Some(false));
    }

    #[test]
    fn fn_oneway() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        assert_eq!(header.oneway(), Some(false));
    }

    #[test]
    fn fn_set_oneway() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        header.set_oneway(true);
        assert_eq!(header.oneway(), Some(true));
    }

    #[test]
    fn fn_queue_id() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        assert_eq!(header.queue_id(), 1);
    }

    #[test]
    fn fn_set_queue_id() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("ns1")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-0")),
            oneway: Some(false),
        };
        let topic_req = TopicRequestHeader {
            rpc_request_header: Some(rpc_header),
            lo: Some(true),
        };
        let mut header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from("testTopic"),
            queue_id: 1,
            committed: true,
            topic_request_header: Some(topic_req),
        };
        header.set_queue_id(2);
        assert_eq!(header.queue_id(), 2);
    }
}
