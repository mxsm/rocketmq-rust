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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

const DEFAULT_TOPIC_PERMISSION: u32 = 0x1 << 2 | 0x1 << 1;
const MESSAGE_TYPE_ATTRIBUTE: &str = "message.type";
const LITE_EXPIRATION_ATTRIBUTE: &str = "lite.topic.expiration";

pub const RETRY_GROUP_TOPIC_PREFIX: &str = "%RETRY%";
pub const DLQ_GROUP_TOPIC_PREFIX: &str = "%DLQ%";
pub const SYSTEM_TOPIC_PREFIX: &str = "rmq_sys_";
pub const SYSTEM_TOPICS: &[&str] = &[
    "SCHEDULE_TOPIC_XXXX",
    "BenchmarkTest",
    "RMQ_SYS_TRANS_HALF_TOPIC",
    "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC",
    "RMQ_SYS_TRACE_TOPIC",
    "RMQ_SYS_TRANS_OP_HALF_TOPIC",
    "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC",
    "TRANS_CHECK_MAX_TIME_TOPIC",
    "SELF_TEST_TOPIC",
    "OFFSET_MOVED_EVENT",
    "CHECKPOINT_TOPIC",
];

/// Returns whether a topic belongs to the stable RocketMQ system-topic set.
pub fn is_system_topic(topic: &str) -> bool {
    topic.starts_with(SYSTEM_TOPIC_PREFIX) || SYSTEM_TOPICS.contains(&topic)
}

/// Topic filter expression mode.
#[derive(Clone, Default, Eq, PartialEq, Copy)]
pub enum TopicFilterType {
    #[default]
    SingleTag,
    MultiTag,
}

impl TopicFilterType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SingleTag => "SINGLE_TAG",
            Self::MultiTag => "MULTI_TAG",
        }
    }
}

impl Display for TopicFilterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for TopicFilterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for TopicFilterType {
    fn from(value: &str) -> Self {
        match value {
            "MULTI_TAG" => Self::MultiTag,
            _ => Self::SingleTag,
        }
    }
}

impl From<String> for TopicFilterType {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<i32> for TopicFilterType {
    fn from(value: i32) -> Self {
        match value {
            1 => Self::MultiTag,
            _ => Self::SingleTag,
        }
    }
}

impl Serialize for TopicFilterType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TopicFilterType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        match value.as_str() {
            "SINGLE_TAG" => Ok(Self::SingleTag),
            "MULTI_TAG" => Ok(Self::MultiTag),
            _ => Err(serde::de::Error::unknown_variant(&value, &["SINGLE_TAG", "MULTI_TAG"])),
        }
    }
}

/// Semantic message kind associated with a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicMessageType {
    Unspecified,
    Normal,
    Fifo,
    Delay,
    Transaction,
    Priority,
    Lite,
    Mixed,
}

impl From<String> for TopicMessageType {
    fn from(value: String) -> Self {
        match value.to_uppercase().as_str() {
            "UNSPECIFIED" => Self::Unspecified,
            "NORMAL" => Self::Normal,
            "FIFO" => Self::Fifo,
            "DELAY" => Self::Delay,
            "TRANSACTION" => Self::Transaction,
            "PRIORITY" => Self::Priority,
            "LITE" => Self::Lite,
            "MIXED" => Self::Mixed,
            _ => Self::Unspecified,
        }
    }
}

impl TopicMessageType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Normal => "NORMAL",
            Self::Fifo => "FIFO",
            Self::Delay => "DELAY",
            Self::Transaction => "TRANSACTION",
            Self::Priority => "PRIORITY",
            Self::Lite => "LITE",
            Self::Mixed => "MIXED",
        }
    }

    pub fn topic_message_type_set() -> HashSet<String> {
        [
            Self::Unspecified,
            Self::Normal,
            Self::Fifo,
            Self::Delay,
            Self::Transaction,
            Self::Priority,
            Self::Lite,
            Self::Mixed,
        ]
        .into_iter()
        .map(|value| value.as_str().to_owned())
        .collect()
    }

    pub fn parse_from_message_property(properties: &HashMap<String, String>) -> Self {
        if properties
            .get("TRAN_MSG")
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            Self::Transaction
        } else if ["DELAY", "TIMER_DELIVER_MS", "TIMER_DELAY_SEC", "TIMER_DELAY_MS"]
            .iter()
            .any(|key| properties.contains_key(*key))
        {
            Self::Delay
        } else if properties.contains_key("__SHARDINGKEY") {
            Self::Fifo
        } else if properties.contains_key("_SYS_MSG_PRIORITY_") {
            Self::Priority
        } else if properties.contains_key("__LITE_TOPIC") {
            Self::Lite
        } else {
            Self::Normal
        }
    }

    pub fn get_metrics_value(&self) -> String {
        self.as_str().to_ascii_lowercase()
    }
}

impl Display for TopicMessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Persistent topic configuration value object.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfig {
    pub topic_name: Option<CheetahString>,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub perm: u32,
    pub topic_filter_type: TopicFilterType,
    pub topic_sys_flag: u32,
    pub order: bool,
    pub attributes: HashMap<CheetahString, CheetahString>,
}

impl TopicConfig {
    const DEFAULT_READ_QUEUE_NUMS: u32 = 16;
    const DEFAULT_WRITE_QUEUE_NUMS: u32 = 16;
    const SEPARATOR: &'static str = " ";

    pub fn new(topic_name: impl Into<CheetahString>) -> Self {
        Self {
            topic_name: Some(topic_name.into()),
            ..Self::default()
        }
    }

    pub fn with_queues(topic_name: impl Into<CheetahString>, read_queue_nums: u32, write_queue_nums: u32) -> Self {
        Self {
            read_queue_nums,
            write_queue_nums,
            ..Self::new(topic_name)
        }
    }

    pub fn with_perm(
        topic_name: impl Into<CheetahString>,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
    ) -> Self {
        Self {
            read_queue_nums,
            write_queue_nums,
            perm,
            ..Self::new(topic_name)
        }
    }

    pub fn with_sys_flag(
        topic_name: impl Into<CheetahString>,
        read_queue_nums: u32,
        write_queue_nums: u32,
        perm: u32,
        topic_sys_flag: u32,
    ) -> Self {
        Self {
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_sys_flag,
            ..Self::new(topic_name)
        }
    }

    pub fn get_topic_message_type(&self) -> TopicMessageType {
        self.attributes
            .get(MESSAGE_TYPE_ATTRIBUTE)
            .map(|value| TopicMessageType::from(value.to_string()))
            .unwrap_or(TopicMessageType::Normal)
    }

    pub fn set_lite_topic_expiration(&mut self, lite_topic_expiration: i32) {
        if self.get_topic_message_type() == TopicMessageType::Lite {
            self.attributes.insert(
                CheetahString::from_static_str(LITE_EXPIRATION_ATTRIBUTE),
                CheetahString::from_string(lite_topic_expiration.to_string()),
            );
        }
    }

    pub fn get_lite_topic_expiration(&self) -> i32 {
        if self.get_topic_message_type() != TopicMessageType::Lite {
            return -1;
        }
        self.attributes
            .get(LITE_EXPIRATION_ATTRIBUTE)
            .and_then(|value| value.parse().ok())
            .unwrap_or(-1)
    }

    pub fn encode(&self) -> String {
        let mut parts = vec![
            self.topic_name.clone().unwrap_or_default().to_string(),
            self.read_queue_nums.to_string(),
            self.write_queue_nums.to_string(),
            self.perm.to_string(),
            self.topic_filter_type.to_string(),
        ];
        if !self.attributes.is_empty() {
            parts.push(serde_json::to_string(&self.attributes).unwrap_or_default());
        }
        parts.join(Self::SEPARATOR)
    }

    pub fn decode(&mut self, input: &str) -> bool {
        let parts: Vec<&str> = input.split(Self::SEPARATOR).collect();
        if parts.len() < 5 {
            return false;
        }
        self.topic_name = Some(parts[0].into());
        self.read_queue_nums = parts[1].parse().unwrap_or(Self::DEFAULT_READ_QUEUE_NUMS);
        self.write_queue_nums = parts[2].parse().unwrap_or(Self::DEFAULT_WRITE_QUEUE_NUMS);
        self.perm = parts[3].parse().unwrap_or(DEFAULT_TOPIC_PERMISSION);
        self.topic_filter_type = parts[4].into();
        if let Some(attributes) = parts.get(5).and_then(|value| serde_json::from_str(value).ok()) {
            self.attributes = attributes;
        }
        true
    }

    pub fn get_read_queue_nums(&self) -> u32 {
        self.read_queue_nums
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            topic_name: None,
            read_queue_nums: Self::DEFAULT_READ_QUEUE_NUMS,
            write_queue_nums: Self::DEFAULT_WRITE_QUEUE_NUMS,
            perm: DEFAULT_TOPIC_PERMISSION,
            topic_filter_type: TopicFilterType::SingleTag,
            topic_sys_flag: 0,
            order: false,
            attributes: HashMap::new(),
        }
    }
}

impl Display for TopicConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TopicConfig {{ topic_name: {:?}, read_queue_nums: {}, write_queue_nums: {}, perm: {}, topic_filter_type: \
             {}, topic_sys_flag: {}, order: {}, attributes: {:?} }}",
            self.topic_name,
            self.read_queue_nums,
            self.write_queue_nums,
            self.perm,
            self.topic_filter_type,
            self.topic_sys_flag,
            self.order,
            self.attributes
        )
    }
}
