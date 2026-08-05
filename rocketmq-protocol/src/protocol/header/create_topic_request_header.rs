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

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2)]
pub struct CreateTopicRequestHeader {
    #[required]
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[required]
    #[serde(rename = "defaultTopic")]
    pub default_topic: CheetahString,

    #[required]
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,

    #[required]
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,

    #[required]
    #[serde(rename = "perm")]
    pub perm: i32,

    #[required]
    #[serde(rename = "topicFilterType")]
    pub topic_filter_type: CheetahString,

    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: Option<i32>,

    #[required]
    #[serde(rename = "order")]
    pub order: bool,

    #[serde(rename = "attributes")]
    pub attributes: Option<CheetahString>,

    #[serde(rename = "force")]
    pub force: Option<bool>,

    #[serde(flatten)]
    pub topic_request_header: Option<TopicRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

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
            map.get(&CheetahString::from_static_str(CreateTopicRequestHeader::TOPIC))
                .unwrap(),
            &CheetahString::from("test_topic")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(CreateTopicRequestHeader::DEFAULT_TOPIC))
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
            map.get(&CheetahString::from_static_str(CreateTopicRequestHeader::PERM))
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
            map.get(&CheetahString::from_static_str(CreateTopicRequestHeader::ORDER))
                .unwrap(),
            &CheetahString::from("true")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(CreateTopicRequestHeader::ATTRIBUTES))
                .unwrap(),
            &CheetahString::from("attributes")
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str(CreateTopicRequestHeader::FORCE))
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
        assert!(header.order);
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
        assert!(header.order);
        assert_eq!(header.attributes, None);
        assert_eq!(header.force, None);
    }
}
