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

#[derive(Debug, Clone, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetEarliestMsgStoretimeRequestHeader {
    #[required]
    pub topic: CheetahString,

    #[required]
    pub queue_id: i32,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl TopicRequestHeaderTrait for GetEarliestMsgStoretimeRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        if let Some(header) = self.topic_request_header.as_mut() {
            header.lo = lo;
        }
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
            .and_then(|header| header.broker_name.as_ref())
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
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|header| header.namespace.as_deref())
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
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|header| header.namespaced)
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
            .and_then(|header| header.rpc_request_header.as_ref())
            .and_then(|header| header.oneway)
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
    fn get_earliest_msg_storetime_request_header_serializes() {
        let header = GetEarliestMsgStoretimeRequestHeader {
            topic: CheetahString::from_static_str("topic-a"),
            queue_id: 3,
            topic_request_header: None,
        };

        let serialized = serde_json::to_string(&header).expect("serialize earliest msg storetime request header");
        assert_eq!(serialized, r#"{"topic":"topic-a","queueId":3}"#);
    }
}
