/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodec)]
pub struct UpdateConsumerOffsetResponseHeader {}

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodec)]
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
/*impl UpdateConsumerOffsetRequestHeader {
    pub const CONSUMER_GROUP: &'static str = "consumerGroup";
    pub const TOPIC: &'static str = "topic";
    pub const QUEUE_ID: &'static str = "queueId";
    pub const COMMIT_OFFSET: &'static str = "commitOffset";
}

impl CommandCustomHeader for UpdateConsumerOffsetRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();

        map.insert(
            CheetahString::from_static_str(Self::CONSUMER_GROUP),
            self.consumer_group.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        if let Some(queue_id) = self.queue_id {
            map.insert(
                CheetahString::from_static_str(Self::QUEUE_ID),
                CheetahString::from_string(queue_id.to_string()),
            );
        }
        if let Some(commit_offset) = self.commit_offset {
            map.insert(
                CheetahString::from_static_str(Self::COMMIT_OFFSET),
                CheetahString::from_string(commit_offset.to_string()),
            );
        }
        if let Some(ref value) = self.topic_request_header {
            if let Some(val) = value.to_map() {
                map.extend(val);
            }
        }
        Some(map)
    }
}

impl FromMap for UpdateConsumerOffsetRequestHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(UpdateConsumerOffsetRequestHeader {
            consumer_group: map
                .get(&CheetahString::from_static_str(
                    UpdateConsumerOffsetRequestHeader::CONSUMER_GROUP,
                ))
                .cloned()
                .unwrap_or_default(),
            topic: map
                .get(&CheetahString::from_static_str(
                    UpdateConsumerOffsetRequestHeader::TOPIC,
                ))
                .cloned()
                .unwrap_or_default(),
            queue_id: map
                .get(&CheetahString::from_static_str(
                    UpdateConsumerOffsetRequestHeader::QUEUE_ID,
                ))
                .and_then(|v| v.parse().ok()),
            commit_offset: map
                .get(&CheetahString::from_static_str(
                    UpdateConsumerOffsetRequestHeader::COMMIT_OFFSET,
                ))
                .and_then(|v| v.parse().ok()),
            topic_request_header: Some(<TopicRequestHeader as FromMap>::from(map)?),
        })
    }
}*/

impl TopicRequestHeaderTrait for UpdateConsumerOffsetRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().unwrap().lo
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
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .broker_name
            .as_ref()
    }

    fn set_broker_name(&mut self, broker_name: CheetahString) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespace
            .as_deref()
    }

    fn set_namespace(&mut self, namespace: CheetahString) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .namespace = Some(namespace);
    }

    fn namespaced(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespaced
    }

    fn set_namespaced(&mut self, namespaced: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .namespaced = Some(namespaced);
    }

    fn oneway(&self) -> Option<bool> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .oneway
    }

    fn set_oneway(&mut self, oneway: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .oneway = Some(oneway);
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

