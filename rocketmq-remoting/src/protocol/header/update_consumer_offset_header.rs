//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
pub struct UpdateConsumerOffsetResponseHeader {}

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct UpdateConsumerOffsetRequestHeader {
    #[required]
    pub consumer_group: CheetahString,
    #[required]
    pub topic: CheetahString,
    #[required]
    pub queue_id: i32,
    #[required]
    pub commit_offset: i64,
    #[serde(flatten)]
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

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn update_consumer_offset_request_header_serializes_correctly() {
        let header = UpdateConsumerOffsetRequestHeader {
            consumer_group: CheetahString::from_static_str("test_consumer_group"),
            topic: CheetahString::from_static_str("test_topic"),
            queue_id: 1,
            commit_offset: 100,
            topic_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("consumerGroup"))
                .unwrap(),
            "test_consumer_group"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("topic")).unwrap(),
            "test_topic"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("queueId")).unwrap(),
            "1"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("commitOffset"))
                .unwrap(),
            "100"
        );
    }

    #[test]
    fn update_consumer_offset_request_header_deserializes_correctly() {
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
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("100"),
        );

        let header = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_consumer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.commit_offset, 100);
    }

    #[test]
    fn update_consumer_offset_request_header_handles_missing_optional_fields() {
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
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("100"),
        );

        let header = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_consumer_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.commit_offset, 100);
        assert!(header.topic_request_header.is_some());
    }

    #[test]
    fn update_consumer_offset_request_header_handles_invalid_data() {
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
            CheetahString::from_static_str("commitOffset"),
            CheetahString::from_static_str("invalid"),
        );

        let result = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&map);
        assert!(result.is_err());
    }
}
