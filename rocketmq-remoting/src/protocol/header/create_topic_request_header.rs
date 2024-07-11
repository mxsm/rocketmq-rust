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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::rpc::topic_request_header::TopicRequestHeader;

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTopicRequestHeader {
    #[serde(rename = "topic")]
    pub topic: String,

    #[serde(rename = "defaultTopic")]
    pub default_topic: String,

    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,

    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,

    #[serde(rename = "perm")]
    pub perm: i32,

    #[serde(rename = "topicFilterType")]
    pub topic_filter_type: String,

    #[serde(rename = "topicSysFlag")]
    pub topic_sys_flag: Option<i32>,

    #[serde(rename = "order")]
    pub order: bool,

    #[serde(rename = "attributes")]
    pub attributes: Option<String>,

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
    fn to_map(&self) -> Option<std::collections::HashMap<String, String>> {
        let mut map = std::collections::HashMap::new();
        map.insert(Self::TOPIC.to_string(), self.topic.clone());
        map.insert(Self::DEFAULT_TOPIC.to_string(), self.default_topic.clone());
        map.insert(
            Self::READ_QUEUE_NUMS.to_string(),
            self.read_queue_nums.to_string(),
        );
        map.insert(
            Self::WRITE_QUEUE_NUMS.to_string(),
            self.write_queue_nums.to_string(),
        );
        map.insert(Self::PERM.to_string(), self.perm.to_string());
        map.insert(
            Self::TOPIC_FILTER_TYPE.to_string(),
            self.topic_filter_type.clone(),
        );
        if let Some(ref topic_sys_flag) = self.topic_sys_flag {
            map.insert(Self::TOPIC_SYS_FLAG.to_string(), topic_sys_flag.to_string());
        }
        map.insert(Self::ORDER.to_string(), self.order.to_string());
        if let Some(ref attributes) = self.attributes {
            map.insert(Self::ATTRIBUTES.to_string(), attributes.clone());
        }
        if let Some(ref force) = self.force {
            map.insert(Self::FORCE.to_string(), force.to_string());
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
    type Target = Self;

    fn from(map: &std::collections::HashMap<String, String>) -> Option<Self::Target> {
        Some(CreateTopicRequestHeader {
            topic: map.get(Self::TOPIC).cloned().unwrap_or_default(),
            default_topic: map.get(Self::DEFAULT_TOPIC).cloned().unwrap_or_default(),
            read_queue_nums: map
                .get(Self::READ_QUEUE_NUMS)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            write_queue_nums: map
                .get(Self::WRITE_QUEUE_NUMS)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            perm: map
                .get(Self::PERM)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            topic_filter_type: map
                .get(Self::TOPIC_FILTER_TYPE)
                .cloned()
                .unwrap_or_default(),
            topic_sys_flag: map.get(Self::TOPIC_SYS_FLAG).and_then(|v| v.parse().ok()),
            order: map
                .get(Self::ORDER)
                .and_then(|v| v.parse().ok())
                .unwrap_or_default(),
            attributes: map.get(Self::ATTRIBUTES).cloned(),
            force: map.get(Self::FORCE).and_then(|v| v.parse().ok()),
            topic_request_header: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}
