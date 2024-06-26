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
use std::collections::HashMap;

use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodec)]
pub struct UpdateConsumerOffsetResponseHeader {}

#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateConsumerOffsetRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: Option<i32>,
    pub commit_offset: Option<i64>,
    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}
impl UpdateConsumerOffsetRequestHeader {
    pub const CONSUMER_GROUP: &'static str = "consumerGroup";
    pub const TOPIC: &'static str = "topic";
    pub const QUEUE_ID: &'static str = "queueId";
    pub const COMMIT_OFFSET: &'static str = "commitOffset";
}

impl CommandCustomHeader for UpdateConsumerOffsetRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::new();
        map.insert(
            Self::CONSUMER_GROUP.to_string(),
            self.consumer_group.clone(),
        );
        map.insert(Self::TOPIC.to_string(), self.topic.clone());
        if let Some(queue_id) = self.queue_id {
            map.insert(Self::QUEUE_ID.to_string(), queue_id.to_string());
        }
        if let Some(commit_offset) = self.commit_offset {
            map.insert(Self::COMMIT_OFFSET.to_string(), commit_offset.to_string());
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
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(UpdateConsumerOffsetRequestHeader {
            consumer_group: map
                .get(UpdateConsumerOffsetRequestHeader::CONSUMER_GROUP)
                .cloned()
                .unwrap_or_default(),
            topic: map
                .get(UpdateConsumerOffsetRequestHeader::TOPIC)
                .cloned()
                .unwrap_or_default(),
            queue_id: map
                .get(UpdateConsumerOffsetRequestHeader::QUEUE_ID)
                .and_then(|v| v.parse().ok()),
            commit_offset: map
                .get(UpdateConsumerOffsetRequestHeader::COMMIT_OFFSET)
                .and_then(|v| v.parse().ok()),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

impl TopicRequestHeaderTrait for UpdateConsumerOffsetRequestHeader {
    fn with_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().unwrap().lo
    }

    fn with_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    fn topic(&self) -> String {
        self.topic.clone()
    }

    fn broker_name(&self) -> Option<String> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .broker_name
            .clone()
    }

    fn with_broker_name(&mut self, broker_name: String) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .broker_name = Some(broker_name);
    }

    fn namespace(&self) -> Option<String> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .namespace
            .clone()
    }

    fn with_namespace(&mut self, namespace: String) {
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

    fn with_namespaced(&mut self, namespaced: bool) {
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

    fn with_oneway(&mut self, oneway: bool) {
        self.topic_request_header
            .as_mut()
            .unwrap()
            .rpc
            .as_mut()
            .unwrap()
            .oneway = Some(oneway);
    }

    fn queue_id(&self) -> Option<i32> {
        self.queue_id
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.queue_id = queue_id;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn update_consumer_offset_request_header_to_map() {
        let header = UpdateConsumerOffsetRequestHeader {
            consumer_group: "group1".to_string(),
            topic: "topic1".to_string(),
            queue_id: Some(1),
            commit_offset: Some(100),
            topic_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(UpdateConsumerOffsetRequestHeader::CONSUMER_GROUP),
            Some(&"group1".to_string())
        );
        assert_eq!(
            map.get(UpdateConsumerOffsetRequestHeader::TOPIC),
            Some(&"topic1".to_string())
        );
        assert_eq!(
            map.get(UpdateConsumerOffsetRequestHeader::QUEUE_ID),
            Some(&"1".to_string())
        );
        assert_eq!(
            map.get(UpdateConsumerOffsetRequestHeader::COMMIT_OFFSET),
            Some(&"100".to_string())
        );
    }

    #[test]
    fn update_consumer_offset_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(
            UpdateConsumerOffsetRequestHeader::CONSUMER_GROUP.to_string(),
            "group1".to_string(),
        );
        map.insert(
            UpdateConsumerOffsetRequestHeader::TOPIC.to_string(),
            "topic1".to_string(),
        );
        map.insert(
            UpdateConsumerOffsetRequestHeader::QUEUE_ID.to_string(),
            "1".to_string(),
        );
        map.insert(
            UpdateConsumerOffsetRequestHeader::COMMIT_OFFSET.to_string(),
            "100".to_string(),
        );
        let header = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "group1");
        assert_eq!(header.topic, "topic1");
        assert_eq!(header.queue_id, Some(1));
        assert_eq!(header.commit_offset, Some(100));
    }

    #[test]
    fn update_consumer_offset_request_header_from_map_with_missing_fields() {
        let map = HashMap::new();
        let header = <UpdateConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "");
        assert_eq!(header.topic, "");
        assert_eq!(header.queue_id, Some(0));
        assert_eq!(header.commit_offset, Some(0));
    }
}
