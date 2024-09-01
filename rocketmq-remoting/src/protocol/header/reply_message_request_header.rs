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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

/// Represents the header of a reply message request.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplyMessageRequestHeader {
    /// Producer group associated with the message.
    pub producer_group: String,

    /// The topic of the message.
    pub topic: String,

    /// Default topic used when the specified topic is not found.
    pub default_topic: String,

    /// Number of queues in the default topic.
    pub default_topic_queue_nums: i32,

    /// Queue ID of the message.
    pub queue_id: i32,

    /// System flags associated with the message.
    pub sys_flag: i32,

    /// Timestamp of when the message was born.
    pub born_timestamp: i64,

    /// Flags associated with the message.
    pub flag: i32,

    /// Properties of the message (nullable).
    pub properties: Option<String>,

    /// Number of times the message has been reconsumed (nullable).
    pub reconsume_times: Option<i32>,

    /// Whether the message processing is in unit mode (nullable).
    pub unit_mode: Option<bool>,

    /// Host where the message was born.
    pub born_host: String,

    /// Host where the message is stored.
    pub store_host: String,

    /// Timestamp of when the message was stored.
    pub store_timestamp: i64,

    #[serde(flatten)]
    pub topic_request: Option<TopicRequestHeader>,
}

impl ReplyMessageRequestHeader {
    pub const PRODUCER_GROUP: &'static str = "producerGroup";
    pub const TOPIC: &'static str = "topic";
    pub const DEFAULT_TOPIC: &'static str = "defaultTopic";
    pub const DEFAULT_TOPIC_QUEUE_NUMS: &'static str = "defaultTopicQueueNums";
    pub const QUEUE_ID: &'static str = "queueId";
    pub const SYS_FLAG: &'static str = "sysFlag";
    pub const BORN_TIMESTAMP: &'static str = "bornTimestamp";
    pub const FLAG: &'static str = "flag";
    pub const PROPERTIES: &'static str = "properties";
    pub const RECONSUME_TIMES: &'static str = "reconsumeTimes";
    pub const UNIT_MODE: &'static str = "unitMode";
    pub const BORN_HOST: &'static str = "bornHost";
    pub const STORE_HOST: &'static str = "storeHost";
    pub const STORE_TIMESTAMP: &'static str = "storeTimestamp";
}

impl CommandCustomHeader for ReplyMessageRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::new();
        map.insert(
            Self::PRODUCER_GROUP.to_string(),
            self.producer_group.clone(),
        );
        map.insert(Self::TOPIC.to_string(), self.topic.clone());
        map.insert(Self::DEFAULT_TOPIC.to_string(), self.default_topic.clone());
        map.insert(
            Self::DEFAULT_TOPIC_QUEUE_NUMS.to_string(),
            self.default_topic_queue_nums.to_string(),
        );
        map.insert(Self::QUEUE_ID.to_string(), self.queue_id.to_string());
        map.insert(Self::SYS_FLAG.to_string(), self.sys_flag.to_string());
        map.insert(
            Self::BORN_TIMESTAMP.to_string(),
            self.born_timestamp.to_string(),
        );
        map.insert(Self::FLAG.to_string(), self.flag.to_string());
        if let Some(value) = self.properties.as_ref() {
            map.insert(Self::PROPERTIES.to_string(), value.clone());
        }
        if let Some(value) = self.reconsume_times.as_ref() {
            map.insert(Self::RECONSUME_TIMES.to_string(), value.to_string());
        }
        if let Some(value) = self.unit_mode.as_ref() {
            map.insert(Self::UNIT_MODE.to_string(), value.to_string());
        }
        map.insert(Self::BORN_HOST.to_string(), self.born_host.clone());
        map.insert(Self::STORE_HOST.to_string(), self.store_host.clone());
        map.insert(
            Self::STORE_TIMESTAMP.to_string(),
            self.store_timestamp.to_string(),
        );
        if let Some(value) = self.topic_request.as_ref() {
            if let Some(value) = value.to_map() {
                map.extend(value);
            }
        }
        Some(map)
    }
}

impl FromMap for ReplyMessageRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(ReplyMessageRequestHeader {
            producer_group: map.get(Self::PRODUCER_GROUP).cloned().unwrap_or_default(),
            topic: map.get(Self::TOPIC).cloned().unwrap_or_default(),
            default_topic: map.get(Self::DEFAULT_TOPIC).cloned().unwrap_or_default(),
            default_topic_queue_nums: map
                .get(Self::DEFAULT_TOPIC_QUEUE_NUMS)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            queue_id: map
                .get(Self::QUEUE_ID)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            sys_flag: map
                .get(Self::SYS_FLAG)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            born_timestamp: map
                .get(Self::BORN_TIMESTAMP)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            flag: map
                .get(Self::FLAG)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            properties: map.get(Self::PROPERTIES).cloned(),
            reconsume_times: map
                .get(Self::RECONSUME_TIMES)
                .and_then(|value| value.parse().ok()),
            unit_mode: map
                .get(Self::UNIT_MODE)
                .and_then(|value| value.parse().ok()),
            born_host: map.get(Self::BORN_HOST).cloned().unwrap_or_default(),
            store_host: map.get(Self::STORE_HOST).cloned().unwrap_or_default(),
            store_timestamp: map
                .get(Self::STORE_TIMESTAMP)
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            topic_request: <TopicRequestHeader as FromMap>::from(map),
        })
    }
}
