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

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct PullMessageRequestHeader {
    #[required]
    pub consumer_group: CheetahString,

    #[required]
    pub topic: CheetahString,

    #[required]
    pub queue_id: i32,

    #[required]
    pub queue_offset: i64,

    #[required]
    pub max_msg_nums: i32,

    #[required]
    pub sys_flag: i32,

    #[required]
    pub commit_offset: i64,

    #[required]
    pub suspend_timeout_millis: u64,

    #[required]
    pub sub_version: i64,

    pub subscription: Option<CheetahString>,
    pub expression_type: Option<CheetahString>,
    pub max_msg_bytes: Option<i32>,
    pub request_source: Option<i32>,
    pub proxy_forward_client_id: Option<CheetahString>,
    #[serde(flatten)]
    pub topic_request: Option<TopicRequestHeader>,
}

impl TopicRequestHeaderTrait for PullMessageRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        if let Some(header) = self.topic_request.as_mut() {
            header.lo = lo;
        }
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request.as_ref().and_then(|h| h.lo)
    }

    fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    fn topic(&self) -> &CheetahString {
        &self.topic
    }

    fn broker_name(&self) -> Option<&CheetahString> {
        self.topic_request
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|h| h.broker_name.as_ref())
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        if let Some(header) = self.topic_request.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
                rpc_header.broker_name = Some(broker_name);
            }
        }
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|r| r.namespace.as_deref())
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        if let Some(header) = self.topic_request.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
                rpc_header.namespace = Some(namespace);
            }
        }
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|r| r.namespaced)
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        if let Some(header) = self.topic_request.as_mut() {
            if let Some(rpc_header) = header.rpc.as_mut() {
                rpc_header.namespaced = Some(namespaced);
            }
        }
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request
            .as_ref()
            .and_then(|h| h.rpc.as_ref())
            .and_then(|r| r.oneway)
    }

    fn set_oneway(&mut self, oneway: bool) {
        if let Some(header) = self.topic_request.as_mut() {
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

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn pull_message_request_header_serializes_correctly() {
        let header = PullMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("test_consumer_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            queue_offset: 100,
            max_msg_nums: 10,
            sys_flag: 0,
            commit_offset: 50,
            suspend_timeout_millis: 3000,
            subscription: Some(CheetahString::from_static_str("test_subscription")),
            sub_version: 1,
            expression_type: Some(CheetahString::from_static_str("test_expression")),
            max_msg_bytes: Some(1024),
            request_source: Some(1),
            proxy_forward_client_id: Some(CheetahString::from_static_str("test_client_id")),
            topic_request: None,
        };
        let map: HashMap<CheetahString, CheetahString> = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("consumerGroup")).unwrap(),
            "test_consumer_group"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("topic")).unwrap(), "test_topic");
        assert_eq!(map.get(&CheetahString::from_static_str("queueId")).unwrap(), "1");
        assert_eq!(map.get(&CheetahString::from_static_str("queueOffset")).unwrap(), "100");
        assert_eq!(map.get(&CheetahString::from_static_str("maxMsgNums")).unwrap(), "10");
        assert_eq!(map.get(&CheetahString::from_static_str("sysFlag")).unwrap(), "0");
        assert_eq!(map.get(&CheetahString::from_static_str("commitOffset")).unwrap(), "50");
        assert_eq!(
            map.get(&CheetahString::from_static_str("suspendTimeoutMillis"))
                .unwrap(),
            "3000"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("subscription")).unwrap(),
            "test_subscription"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("subVersion")).unwrap(), "1");
        assert_eq!(
            map.get(&CheetahString::from_static_str("expressionType")).unwrap(),
            "test_expression"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("maxMsgBytes")).unwrap(), "1024");
        assert_eq!(map.get(&CheetahString::from_static_str("requestSource")).unwrap(), "1");
        assert_eq!(
            map.get(&CheetahString::from_static_str("proxyForwardClientId"))
                .unwrap(),
            "test_client_id"
        );
    }

    #[test]
    fn pull_message_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("queueOffset"),
            CheetahString::from_static_str("100"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNums"),
            CheetahString::from_static_str("10"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("50"),
        );
        map.insert(
            CheetahString::from_static_str("suspendTimeoutMillis"),
            CheetahString::from_static_str("3000"),
        );
        map.insert(
            CheetahString::from_static_str("subscription"),
            CheetahString::from_static_str("test_subscription"),
        );
        map.insert(
            CheetahString::from_static_str("subVersion"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("expressionType"),
            CheetahString::from_static_str("test_expression"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgBytes"),
            CheetahString::from_static_str("1024"),
        );
        map.insert(
            CheetahString::from_static_str("requestSource"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("proxyForwardClientId"),
            CheetahString::from_static_str("test_client_id"),
        );

        let header = <PullMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_consumer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.queue_offset, 100);
        assert_eq!(header.max_msg_nums, 10);
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.commit_offset, 50);
        assert_eq!(header.suspend_timeout_millis, 3000);
        assert_eq!(header.subscription.unwrap(), "test_subscription");
        assert_eq!(header.sub_version, 1);
        assert_eq!(header.expression_type.unwrap(), "test_expression");
        assert_eq!(header.max_msg_bytes.unwrap(), 1024);
        assert_eq!(header.request_source.unwrap(), 1);
        assert_eq!(header.proxy_forward_client_id.unwrap(), "test_client_id");
    }

    #[test]
    fn pull_message_request_header_handles_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("1"),
        );
        map.insert(
            CheetahString::from_static_str("queueOffset"),
            CheetahString::from_static_str("100"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNums"),
            CheetahString::from_static_str("10"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("0"),
        );
        map.insert(
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("50"),
        );
        map.insert(
            CheetahString::from_static_str("suspendTimeoutMillis"),
            CheetahString::from_static_str("3000"),
        );
        map.insert(
            CheetahString::from_static_str("subVersion"),
            CheetahString::from_static_str("1"),
        );

        let header = <PullMessageRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_consumer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.queue_offset, 100);
        assert_eq!(header.max_msg_nums, 10);
        assert_eq!(header.sys_flag, 0);
        assert_eq!(header.commit_offset, 50);
        assert_eq!(header.suspend_timeout_millis, 3000);
        assert_eq!(header.sub_version, 1);
        assert!(header.subscription.is_none());
        assert!(header.expression_type.is_none());
        assert!(header.max_msg_bytes.is_none());
        assert!(header.request_source.is_none());
        assert!(header.proxy_forward_client_id.is_none());
    }

    #[test]
    fn pull_message_request_header_handles_invalid_data() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str("queueId"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("queueOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("maxMsgNums"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("sysFlag"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("suspendTimeoutMillis"),
            CheetahString::from_static_str("invalid"),
        );
        map.insert(
            CheetahString::from_static_str("subVersion"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <PullMessageRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
