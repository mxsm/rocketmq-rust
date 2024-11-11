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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub const CONSUMER_GROUP: &'static str = "consumerGroup";
    pub const TOPIC: &'static str = "topic";
    pub const QUEUE_ID: &'static str = "queueId";
    pub const SET_ZERO_IF_NOT_FOUND: &'static str = "setZeroIfNotFound";
}

impl CommandCustomHeader for QueryConsumerOffsetRequestHeader {
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
        map.insert(
            CheetahString::from_static_str(Self::QUEUE_ID),
            CheetahString::from_string(self.queue_id.to_string()),
        );
        if let Some(value) = self.set_zero_if_not_found {
            map.insert(
                CheetahString::from_static_str(Self::SET_ZERO_IF_NOT_FOUND),
                CheetahString::from_string(value.to_string()),
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

impl FromMap for QueryConsumerOffsetRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(QueryConsumerOffsetRequestHeader {
            consumer_group: map
                .get(&CheetahString::from_static_str(Self::CONSUMER_GROUP))
                .cloned()
                .unwrap_or_default(),
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned()
                .unwrap_or_default(),
            queue_id: map
                .get(&CheetahString::from_static_str(Self::QUEUE_ID))
                .map_or(0, |value| value.parse::<i32>().unwrap()),
            set_zero_if_not_found: map
                .get(&CheetahString::from_static_str(Self::SET_ZERO_IF_NOT_FOUND))
                .and_then(|value| value.parse::<bool>().ok()),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}

impl TopicRequestHeaderTrait for QueryConsumerOffsetRequestHeader {
    fn set_lo(&mut self, lo: Option<bool>) {
        self.topic_request_header.as_mut().unwrap().lo = lo;
    }

    fn lo(&self) -> Option<bool> {
        self.topic_request_header.as_ref().unwrap().lo
    }

    fn set_topic(&mut self, topic: String) {
        self.topic = topic;
    }

    fn topic(&self) -> &str {
        self.topic.as_str()
    }

    fn broker_name(&self) -> Option<&str> {
        self.topic_request_header
            .as_ref()
            .unwrap()
            .rpc
            .as_ref()
            .unwrap()
            .broker_name
            .as_deref()
    }

    fn set_broker_name(&mut self, broker_name: String) {
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

    fn set_namespace(&mut self, namespace: String) {
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

    fn queue_id(&self) -> Option<i32> {
        Some(self.queue_id)
    }

    fn set_queue_id(&mut self, queue_id: Option<i32>) {
        self.queue_id = queue_id.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn query_consumer_offset_request_header_to_map() {
        let header = QueryConsumerOffsetRequestHeader {
            consumer_group: "test_group".to_string(),
            topic: "test_topic".to_string(),
            queue_id: 1,
            set_zero_if_not_found: Some(true),
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(QueryConsumerOffsetRequestHeader::CONSUMER_GROUP),
            Some(&"test_group".to_string())
        );
        assert_eq!(
            map.get(QueryConsumerOffsetRequestHeader::TOPIC),
            Some(&"test_topic".to_string())
        );
        assert_eq!(
            map.get(QueryConsumerOffsetRequestHeader::QUEUE_ID),
            Some(&"1".to_string())
        );
        assert_eq!(
            map.get(QueryConsumerOffsetRequestHeader::SET_ZERO_IF_NOT_FOUND),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn query_consumer_offset_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(
            QueryConsumerOffsetRequestHeader::CONSUMER_GROUP.to_string(),
            "test_group".to_string(),
        );
        map.insert(
            QueryConsumerOffsetRequestHeader::TOPIC.to_string(),
            "test_topic".to_string(),
        );
        map.insert(
            QueryConsumerOffsetRequestHeader::QUEUE_ID.to_string(),
            "1".to_string(),
        );
        map.insert(
            QueryConsumerOffsetRequestHeader::SET_ZERO_IF_NOT_FOUND.to_string(),
            "true".to_string(),
        );

        let header = <QueryConsumerOffsetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_group");
        assert_eq!(header.topic, "test_topic");
        assert_eq!(header.queue_id, 1);
        assert_eq!(header.set_zero_if_not_found, Some(true));
    }
}
