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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTopicRequestHeader {
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[serde(rename = "defaultTopic")]
    pub default_topic: CheetahString,

    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,

    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,

    #[serde(rename = "perm")]
    pub perm: i32,

    #[serde(rename = "topicFilterType")]
    pub topic_filter_type: CheetahString,

    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: Option<i32>,

    #[serde(rename = "order")]
    pub order: bool,

    #[serde(rename = "attributes")]
    pub attributes: Option<CheetahString>,

    #[serde(rename = "force")]
    pub force: Option<bool>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

impl CreateTopicRequestHeader {
    pub const TOPIC: &'static str = "topic";
    pub const DEFAULT_TOPIC: &'static str = "defaultTopic";
    pub const READ_QUEUE_NUMS: &'static str = "readQueueNums";
    pub const WRITE_QUEUE_NUMS: &'static str = "writeQueueNums";
    pub const PERM: &'static str = "perm";
    pub const TOPIC_FILTER_TYPE: &'static str = "topicFilterType";
    pub const TOPIC_SYS_FLAG: &'static str = "topicSysFlag";
    pub const ORDER: &'static str = "order";
    pub const ATTRIBUTES: &'static str = "attributes";
    pub const FORCE: &'static str = "force";
}

impl CommandCustomHeader for CreateTopicRequestHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::DEFAULT_TOPIC),
            self.default_topic.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::READ_QUEUE_NUMS),
            CheetahString::from_string(self.read_queue_nums.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::WRITE_QUEUE_NUMS),
            CheetahString::from_string(self.write_queue_nums.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::PERM),
            CheetahString::from_string(self.perm.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::TOPIC_FILTER_TYPE),
            self.topic_filter_type.clone(),
        );

        if let Some(ref topic_sys_flag) = self.topic_sys_flag {
            map.insert(
                CheetahString::from_static_str(Self::TOPIC_SYS_FLAG),
                CheetahString::from_string(topic_sys_flag.to_string()),
            );
        }
        map.insert(
            CheetahString::from_static_str(Self::ORDER),
            CheetahString::from_string(self.order.to_string()),
        );

        if let Some(ref attributes) = self.attributes {
            map.insert(
                CheetahString::from_static_str(Self::ATTRIBUTES),
                attributes.clone(),
            );
        }
        if let Some(ref force) = self.force {
            map.insert(
                CheetahString::from_static_str(Self::FORCE),
                CheetahString::from_string(force.to_string()),
            );
        }
        if let Some(value) = self.topic_request_header.as_ref() {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }
}

impl FromMap for CreateTopicRequestHeader {
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(
        map: &std::collections::HashMap<CheetahString, CheetahString>,
    ) -> Result<Self::Target, Self::Error> {
        Ok(CreateTopicRequestHeader {
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned()
                .unwrap_or_default(),
            default_topic: map
                .get(&CheetahString::from_static_str(Self::DEFAULT_TOPIC))
                .cloned()
                .unwrap_or_default(),
            read_queue_nums: map
                .get(&CheetahString::from_static_str(Self::READ_QUEUE_NUMS))
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            write_queue_nums: map
                .get(&CheetahString::from_static_str(Self::WRITE_QUEUE_NUMS))
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            perm: map
                .get(&CheetahString::from_static_str(Self::PERM))
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            topic_filter_type: map
                .get(&CheetahString::from_static_str(Self::TOPIC_FILTER_TYPE))
                .cloned()
                .unwrap_or_default(),
            topic_sys_flag: map
                .get(&CheetahString::from_static_str(Self::TOPIC_SYS_FLAG))
                .and_then(|v| v.parse().ok()),
            order: map
                .get(&CheetahString::from_static_str(Self::ORDER))
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            attributes: map
                .get(&CheetahString::from_static_str(Self::ATTRIBUTES))
                .cloned(),
            force: map
                .get(&CheetahString::from_static_str(Self::FORCE))
                .and_then(|v| v.parse().ok()),
            topic_request_header: Some(<TopicRequestHeader as FromMap>::from(map)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn create_topic_request_header_to_map() {
        let header = CreateTopicRequestHeader {
            topic: CheetahString::from("test_topic"),
            default_topic: CheetahString::from("default_topic"),
            read_queue_nums: 4,
            write_queue_nums: 4,
            perm: 6,
            topic_filter_type: CheetahString::from("filter_type"),
            topic_sys_flag: Some(1),
            order: true,
            attributes: Some(CheetahString::from("attributes")),
            force: Some(true),
            topic_request_header: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::TOPIC
            ))
            .unwrap(),
            &CheetahString::from("test_topic")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::DEFAULT_TOPIC
            ))
            .unwrap(),
            &CheetahString::from("default_topic")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::READ_QUEUE_NUMS
            ))
            .unwrap(),
            &CheetahString::from("4")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::WRITE_QUEUE_NUMS
            ))
            .unwrap(),
            &CheetahString::from("4")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::PERM
            ))
            .unwrap(),
            &CheetahString::from("6")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::TOPIC_FILTER_TYPE
            ))
            .unwrap(),
            &CheetahString::from("filter_type")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::TOPIC_SYS_FLAG
            ))
            .unwrap(),
            &CheetahString::from("1")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::ORDER
            ))
            .unwrap(),
            &CheetahString::from("true")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::ATTRIBUTES
            ))
            .unwrap(),
            &CheetahString::from("attributes")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(
                CreateTopicRequestHeader::FORCE
            ))
            .unwrap(),
            &CheetahString::from("true")
        );
    }

    #[test]
    fn create_topic_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::TOPIC),
            CheetahString::from("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::DEFAULT_TOPIC),
            CheetahString::from("default_topic"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::READ_QUEUE_NUMS),
            CheetahString::from("4"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::WRITE_QUEUE_NUMS),
            CheetahString::from("4"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::PERM),
            CheetahString::from("6"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::TOPIC_FILTER_TYPE),
            CheetahString::from("filter_type"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::TOPIC_SYS_FLAG),
            CheetahString::from("1"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::ORDER),
            CheetahString::from("true"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::ATTRIBUTES),
            CheetahString::from("attributes"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::FORCE),
            CheetahString::from("true"),
        );

        let header = <CreateTopicRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert_eq!(header.default_topic, CheetahString::from("default_topic"));
        assert_eq!(header.read_queue_nums, 4);
        assert_eq!(header.write_queue_nums, 4);
        assert_eq!(header.perm, 6);
        assert_eq!(header.topic_filter_type, CheetahString::from("filter_type"));
        assert_eq!(header.topic_sys_flag, Some(1));
        assert_eq!(header.order, true);
        assert_eq!(header.attributes, Some(CheetahString::from("attributes")));
        assert_eq!(header.force, Some(true));
    }

    #[test]
    fn create_topic_request_header_from_map_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::TOPIC),
            CheetahString::from("test_topic"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::DEFAULT_TOPIC),
            CheetahString::from("default_topic"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::READ_QUEUE_NUMS),
            CheetahString::from("4"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::WRITE_QUEUE_NUMS),
            CheetahString::from("4"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::PERM),
            CheetahString::from("6"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::TOPIC_FILTER_TYPE),
            CheetahString::from("filter_type"),
        );
        map.insert(
            CheetahString::from_static_str(CreateTopicRequestHeader::ORDER),
            CheetahString::from("true"),
        );

        let header = <CreateTopicRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.topic, CheetahString::from("test_topic"));
        assert_eq!(header.default_topic, CheetahString::from("default_topic"));
        assert_eq!(header.read_queue_nums, 4);
        assert_eq!(header.write_queue_nums, 4);
        assert_eq!(header.perm, 6);
        assert_eq!(header.topic_filter_type, CheetahString::from("filter_type"));
        assert_eq!(header.topic_sys_flag, None);
        assert_eq!(header.order, true);
        assert_eq!(header.attributes, None);
        assert_eq!(header.force, None);
    }
}
