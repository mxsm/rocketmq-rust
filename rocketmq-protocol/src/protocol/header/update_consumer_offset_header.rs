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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader"
)]
pub struct UpdateConsumerOffsetResponseHeader {}

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct UpdateConsumerOffsetRequestHeader {
    #[header(required)]
    pub consumer_group: CheetahString,
    #[header(required)]
    pub topic: CheetahString,
    #[header(required)]
    pub queue_id: i32,
    #[header(required)]
    pub commit_offset: i64,
    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl TopicRequestHeaderTrait for UpdateConsumerOffsetRequestHeader {
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
    use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};
    use crate::rpc::rpc_request_header::RpcRequestHeader;

    fn request_with_rpc_envelope() -> UpdateConsumerOffsetRequestHeader {
        UpdateConsumerOffsetRequestHeader {
            consumer_group: CheetahString::from("group-a"),
            topic: CheetahString::from("topic-a"),
            queue_id: 1,
            commit_offset: 100,
            topic_request_header: Some(TopicRequestHeader {
                lo: None,
                rpc: Some(RpcRequestHeader::default()),
            }),
        }
    }

    #[test]
    fn v3_codec_round_trips_required_fields_and_always_creates_the_rpc_envelope() {
        let header = UpdateConsumerOffsetRequestHeader {
            topic_request_header: None,
            ..request_with_rpc_envelope()
        };
        let map = header.to_map().unwrap();

        assert_eq!(map.get("consumerGroup").map(|value| value.as_str()), Some("group-a"));
        assert_eq!(map.get("topic").map(|value| value.as_str()), Some("topic-a"));
        assert_eq!(map.get("queueId").map(|value| value.as_str()), Some("1"));
        assert_eq!(map.get("commitOffset").map(|value| value.as_str()), Some("100"));

        let decoded = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(decoded.consumer_group, "group-a");
        assert_eq!(decoded.topic, "topic-a");
        assert_eq!(decoded.queue_id, 1);
        assert_eq!(decoded.commit_offset, 100);
        assert!(decoded.topic_request_header.is_some());
    }

    #[test]
    fn v3_codec_rejects_missing_or_invalid_required_values() {
        assert!(<UpdateConsumerOffsetRequestHeader as FromMap>::from(&HashMap::new()).is_err());

        let mut map = request_with_rpc_envelope().to_map().unwrap();
        map.insert("queueId".into(), "invalid".into());
        assert!(<UpdateConsumerOffsetRequestHeader as FromMap>::from(&map).is_err());
    }

    #[test]
    fn topic_request_trait_reads_and_updates_every_forwarded_field() {
        let mut header = request_with_rpc_envelope();

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
        let mut header = UpdateConsumerOffsetRequestHeader {
            topic_request_header: None,
            ..request_with_rpc_envelope()
        };

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

    #[test]
    fn empty_response_header_has_empty_serde_and_v3_representations() {
        let header = UpdateConsumerOffsetResponseHeader::default();

        assert_eq!(serde_json::to_string(&header).unwrap(), "{}");
        assert!(serde_json::from_str::<UpdateConsumerOffsetResponseHeader>("{}").is_ok());
        assert!(header.to_map().unwrap().is_empty());
    }
}
