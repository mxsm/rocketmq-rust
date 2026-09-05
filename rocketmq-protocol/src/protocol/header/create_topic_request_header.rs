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

use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader",
    validate = "Self::validate"
)]
pub struct CreateTopicRequestHeader {
    #[header(required)]
    #[serde(rename = "topic")]
    pub topic: CheetahString,

    #[header(required)]
    #[serde(rename = "defaultTopic")]
    pub default_topic: CheetahString,

    #[header(required)]
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,

    #[header(required)]
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,

    #[header(required)]
    #[serde(rename = "perm")]
    pub perm: i32,

    #[header(required)]
    #[serde(rename = "topicFilterType")]
    pub topic_filter_type: CheetahString,

    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: Option<i32>,

    #[header(required)]
    #[serde(rename = "order")]
    pub order: bool,

    #[serde(rename = "attributes")]
    pub attributes: Option<CheetahString>,

    #[serde(rename = "force")]
    #[serde(default = "default_force")]
    #[header(default_with = "default_force", default_semantic = "literal:false")]
    pub force: Option<bool>,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub topic_request_header: Option<TopicRequestHeader>,
}

fn default_force() -> Option<bool> {
    Some(false)
}

impl CreateTopicRequestHeader {
    fn validate(&self) -> Result<(), crate::ProtocolContractViolation> {
        match self.topic_filter_type.as_str() {
            "SINGLE_TAG" | "MULTI_TAG" => Ok(()),
            _ => Err(crate::ProtocolContractViolation::Validation {
                header: "rocketmq_protocol::protocol::header::create_topic_request_header::CreateTopicRequestHeader",
                rule: "supported_topic_filter_type",
            }),
        }
    }
}

#[cfg(test)]
impl CreateTopicRequestHeader {
    const TOPIC: &'static str = "topic";
    const DEFAULT_TOPIC: &'static str = "defaultTopic";
    const READ_QUEUE_NUMS: &'static str = "readQueueNums";
    const WRITE_QUEUE_NUMS: &'static str = "writeQueueNums";
    const PERM: &'static str = "perm";
    const TOPIC_FILTER_TYPE: &'static str = "topicFilterType";
    const TOPIC_SYS_FLAG: &'static str = "topicSysFlag";
    const ORDER: &'static str = "order";
    const ATTRIBUTES: &'static str = "attributes";
    const FORCE: &'static str = "force";
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
            topic_filter_type: CheetahString::from("SINGLE_TAG"),
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
            &CheetahString::from("SINGLE_TAG")
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
            CheetahString::from("SINGLE_TAG"),
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
        assert_eq!(header.topic_filter_type, CheetahString::from("SINGLE_TAG"));
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
            CheetahString::from("SINGLE_TAG"),
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
        assert_eq!(header.topic_filter_type, CheetahString::from("SINGLE_TAG"));
        assert_eq!(header.topic_sys_flag, None);
        assert!(header.order);
        assert_eq!(header.attributes, None);
        assert_eq!(header.force, Some(false));
    }

    #[test]
    fn create_topic_request_header_validates_java_filter_types() {
        let base = HashMap::from([
            ("topic".into(), "TopicA".into()),
            ("defaultTopic".into(), "TBW102".into()),
            ("readQueueNums".into(), "4".into()),
            ("writeQueueNums".into(), "4".into()),
            ("perm".into(), "6".into()),
            ("order".into(), "false".into()),
        ]);
        for value in ["SINGLE_TAG", "MULTI_TAG"] {
            let mut fields = base.clone();
            fields.insert("topicFilterType".into(), value.into());
            assert!(<CreateTopicRequestHeader as FromMap>::from(&fields).is_ok());
        }
        let mut fields = base;
        fields.insert("topicFilterType".into(), "SQL92".into());
        assert!(<CreateTopicRequestHeader as FromMap>::from(&fields).is_err());
    }
}
