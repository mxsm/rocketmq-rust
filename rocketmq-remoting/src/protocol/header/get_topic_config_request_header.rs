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
use crate::rpc::rpc_request_header::RpcRequestHeader;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2)]
pub struct GetTopicConfigRequestHeader {
    #[required]
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl GetTopicConfigRequestHeader {
    pub fn get_topic(&self) -> &CheetahString {
        &self.topic
    }
    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }
}

impl TopicRequestHeaderTrait for GetTopicConfigRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header
            .get_or_insert_with(TopicRequestHeader::default)
            .lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().and_then(|header| header.lo)
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
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|rpc| rpc.broker_name.as_ref())
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.topic_request_header
            .get_or_insert_with(TopicRequestHeader::default)
            .rpc_request_header
            .get_or_insert_with(RpcRequestHeader::default)
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|rpc| rpc.namespace.as_deref())
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        self.topic_request_header
            .get_or_insert_with(TopicRequestHeader::default)
            .rpc_request_header
            .get_or_insert_with(RpcRequestHeader::default)
            .namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|rpc| rpc.namespaced)
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.topic_request_header
            .get_or_insert_with(TopicRequestHeader::default)
            .rpc_request_header
            .get_or_insert_with(RpcRequestHeader::default)
            .namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|rpc| rpc.oneway)
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.topic_request_header
            .get_or_insert_with(TopicRequestHeader::default)
            .rpc_request_header
            .get_or_insert_with(RpcRequestHeader::default)
            .oneway = Some(oneway);
    }

    fn queue_id(&self) -> i32 {
        -1
    }

    fn set_queue_id(&mut self, _queue_id: i32) {}
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_topic_config_request_header_serialization() {
        let header = GetTopicConfigRequestHeader {
            topic: CheetahString::from("topic1"),
            topic_request_header: None,
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"topic\":\"topic1\""));
    }

    #[test]
    fn get_topic_config_request_header_deserialization() {
        let json = r#"{"topic":"topic1"}"#;
        let header: GetTopicConfigRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.topic, "topic1");
    }

    #[test]
    fn get_topic_config_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("topic"), CheetahString::from("topic1"));
        let header = <GetTopicConfigRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, "topic1");
    }

    #[test]
    fn getters_and_setters() {
        let mut header = GetTopicConfigRequestHeader {
            topic: CheetahString::from("topic1"),
            topic_request_header: None,
        };
        assert_eq!(header.get_topic(), "topic1");
        header.set_topic(CheetahString::from("topic2"));
        assert_eq!(header.get_topic(), "topic2");
    }
}
