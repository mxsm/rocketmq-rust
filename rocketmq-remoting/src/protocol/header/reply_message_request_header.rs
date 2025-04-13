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
use crate::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;

/// Represents the header of a reply message request.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplyMessageRequestHeader {
    /// Producer group associated with the message.
    pub producer_group: CheetahString,

    /// The topic of the message.
    pub topic: CheetahString,

    /// Default topic used when the specified topic is not found.
    pub default_topic: CheetahString,

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
    pub properties: Option<CheetahString>,

    /// Number of times the message has been reconsumed (nullable).
    pub reconsume_times: Option<i32>,

    /// Whether the message processing is in unit mode (nullable).
    pub unit_mode: Option<bool>,

    /// Host where the message was born.
    pub born_host: CheetahString,

    /// Host where the message is stored.
    pub store_host: CheetahString,

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
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str(Self::PRODUCER_GROUP),
            self.producer_group.clone(),
        );

        map.insert(
            CheetahString::from_static_str(Self::TOPIC),
            self.topic.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::DEFAULT_TOPIC),
            self.default_topic.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::DEFAULT_TOPIC_QUEUE_NUMS),
            CheetahString::from_string(self.default_topic_queue_nums.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::QUEUE_ID),
            CheetahString::from_string(self.queue_id.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::SYS_FLAG),
            CheetahString::from_string(self.sys_flag.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::BORN_TIMESTAMP),
            CheetahString::from_string(self.born_timestamp.to_string()),
        );
        map.insert(
            CheetahString::from_static_str(Self::FLAG),
            CheetahString::from_string(self.flag.to_string()),
        );

        if let Some(value) = self.properties.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::PROPERTIES),
                value.clone(),
            );
        }
        if let Some(value) = self.reconsume_times.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::RECONSUME_TIMES),
                CheetahString::from_string(value.to_string()),
            );
        }
        if let Some(value) = self.unit_mode.as_ref() {
            map.insert(
                CheetahString::from_static_str(Self::UNIT_MODE),
                CheetahString::from_string(value.to_string()),
            );
        }
        map.insert(
            CheetahString::from_static_str(Self::BORN_HOST),
            self.born_host.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::STORE_HOST),
            self.store_host.clone(),
        );
        map.insert(
            CheetahString::from_static_str(Self::STORE_TIMESTAMP),
            CheetahString::from_string(self.store_timestamp.to_string()),
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
    type Error = rocketmq_error::RocketmqError;

    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(ReplyMessageRequestHeader {
            producer_group: map
                .get(&CheetahString::from_static_str(Self::PRODUCER_GROUP))
                .cloned()
                .unwrap_or_default(),
            topic: map
                .get(&CheetahString::from_static_str(Self::TOPIC))
                .cloned()
                .unwrap_or_default(),
            default_topic: map
                .get(&CheetahString::from_static_str(Self::DEFAULT_TOPIC))
                .cloned()
                .unwrap_or_default(),
            default_topic_queue_nums: map
                .get(&CheetahString::from_static_str(
                    Self::DEFAULT_TOPIC_QUEUE_NUMS,
                ))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            queue_id: map
                .get(&CheetahString::from_static_str(Self::QUEUE_ID))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            sys_flag: map
                .get(&CheetahString::from_static_str(Self::SYS_FLAG))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            born_timestamp: map
                .get(&CheetahString::from_static_str(Self::BORN_TIMESTAMP))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            flag: map
                .get(&CheetahString::from_static_str(Self::FLAG))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            properties: map
                .get(&CheetahString::from_static_str(Self::PROPERTIES))
                .cloned(),
            reconsume_times: map
                .get(&CheetahString::from_static_str(Self::RECONSUME_TIMES))
                .and_then(|value| value.parse().ok()),
            unit_mode: map
                .get(&CheetahString::from_static_str(Self::UNIT_MODE))
                .and_then(|value| value.parse().ok()),
            born_host: map
                .get(&CheetahString::from_static_str(Self::BORN_HOST))
                .cloned()
                .unwrap_or_default(),
            store_host: map
                .get(&CheetahString::from_static_str(Self::STORE_HOST))
                .cloned()
                .unwrap_or_default(),
            store_timestamp: map
                .get(&CheetahString::from_static_str(Self::STORE_TIMESTAMP))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            topic_request: Some(<TopicRequestHeader as FromMap>::from(map)?),
        })
    }
}
