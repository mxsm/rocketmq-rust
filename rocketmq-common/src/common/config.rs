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
use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use super::TopicFilterType;
use crate::common::attribute::topic_message_type::TopicMessageType;
use crate::common::attribute::Attribute;
use crate::common::constant::PermName;
use crate::TopicAttributes::TopicAttributes;

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

impl Display for TopicConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            topic_name: None,
            read_queue_nums: Self::DEFAULT_READ_QUEUE_NUMS,
            write_queue_nums: Self::DEFAULT_WRITE_QUEUE_NUMS,
            perm: PermName::PERM_READ | PermName::PERM_WRITE,
            topic_filter_type: TopicFilterType::SingleTag,
            topic_sys_flag: 0,
            order: false,
            attributes: HashMap::new(),
        }
    }
}

impl TopicConfig {
    const DEFAULT_READ_QUEUE_NUMS: u32 = 16;
    const DEFAULT_WRITE_QUEUE_NUMS: u32 = 16;
    const SEPARATOR: &'static str = " ";

    pub fn get_topic_message_type(&self) -> TopicMessageType {
        if self.attributes.is_empty() {
            return TopicMessageType::Normal;
        }
        let content = self
            .attributes
            .get(TopicAttributes::topic_message_type_attribute().name());
        if let Some(content) = content {
            return TopicMessageType::from(content.to_string());
        }
        TopicMessageType::Normal
    }

    pub fn new(topic_name: impl Into<CheetahString>) -> Self {
        TopicConfig {
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

    pub fn encode(&self) -> String {
        let mut sb = String::new();
        sb.push_str(self.topic_name.clone().unwrap_or(CheetahString::empty()).as_str());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&self.read_queue_nums.to_string());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&self.write_queue_nums.to_string());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&self.perm.to_string());
        sb.push_str(Self::SEPARATOR);
        sb.push_str(&format!("{}", self.topic_filter_type));
        sb.push_str(Self::SEPARATOR);
        if !self.attributes.is_empty() {
            sb.push_str(&serde_json::to_string(&self.attributes).unwrap());
        }
        sb.trim().to_string()
    }

    pub fn decode(&mut self, input: &str) -> bool {
        let parts: Vec<&str> = input.split(Self::SEPARATOR).collect();
        if parts.len() >= 5 {
            self.topic_name = Some(parts[0].into());
            self.read_queue_nums = parts[1].parse().unwrap_or(Self::DEFAULT_READ_QUEUE_NUMS);
            self.write_queue_nums = parts[2].parse().unwrap_or(Self::DEFAULT_WRITE_QUEUE_NUMS);
            self.perm = parts[3].parse().unwrap_or(PermName::PERM_READ | PermName::PERM_WRITE);
            self.topic_filter_type = From::from(parts[4]);
            if parts.len() >= 6 {
                if let Ok(attrs) = serde_json::from_str(parts[5]) {
                    self.attributes = attrs
                }
            }
            true
        } else {
            false
        }
    }

    pub fn get_read_queue_nums(&self) -> u32 {
        self.read_queue_nums
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn default_topic_config() {
        let config = TopicConfig::default();
        assert_eq!(config.topic_name, None);
        assert_eq!(config.read_queue_nums, TopicConfig::DEFAULT_READ_QUEUE_NUMS);
        assert_eq!(config.write_queue_nums, TopicConfig::DEFAULT_WRITE_QUEUE_NUMS);
        assert_eq!(config.perm, PermName::PERM_READ | PermName::PERM_WRITE);
        assert_eq!(config.topic_filter_type, TopicFilterType::SingleTag);
        assert_eq!(config.topic_sys_flag, 0);
        assert!(!config.order);
        assert!(config.attributes.is_empty());
    }

    #[test]
    fn new_topic_config() {
        let topic_name = CheetahString::from("test_topic");
        let config = TopicConfig::new(topic_name.clone());
        assert_eq!(config.topic_name, Some(topic_name));
    }

    #[test]
    fn with_queues_topic_config() {
        let topic_name = CheetahString::from("test_topic");
        let config = TopicConfig::with_queues(topic_name.clone(), 8, 8);
        assert_eq!(config.topic_name, Some(topic_name));
        assert_eq!(config.read_queue_nums, 8);
        assert_eq!(config.write_queue_nums, 8);
    }

    #[test]
    fn with_perm_topic_config() {
        let topic_name = CheetahString::from("test_topic");
        let config = TopicConfig::with_perm(topic_name.clone(), 8, 8, PermName::PERM_READ);
        assert_eq!(config.topic_name, Some(topic_name));
        assert_eq!(config.read_queue_nums, 8);
        assert_eq!(config.write_queue_nums, 8);
        assert_eq!(config.perm, PermName::PERM_READ);
    }

    #[test]
    fn with_sys_flag_topic_config() {
        let topic_name = CheetahString::from("test_topic");
        let config = TopicConfig::with_sys_flag(topic_name.clone(), 8, 8, PermName::PERM_READ, 1);
        assert_eq!(config.topic_name, Some(topic_name));
        assert_eq!(config.read_queue_nums, 8);
        assert_eq!(config.write_queue_nums, 8);
        assert_eq!(config.perm, PermName::PERM_READ);
        assert_eq!(config.topic_sys_flag, 1);
    }

    #[test]
    fn encode_topic_config() {
        let topic_name = CheetahString::from("test_topic");
        let mut attributes = HashMap::new();
        attributes.insert(CheetahString::from("key"), CheetahString::from("value"));
        let config = TopicConfig {
            topic_name: Some(topic_name.clone()),
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: PermName::PERM_READ,
            topic_filter_type: TopicFilterType::SingleTag,
            topic_sys_flag: 1,
            order: false,
            attributes,
        };
        let encoded = config.encode();
        assert!(encoded.contains("test_topic 8 8 4 SINGLE_TAG {\"key\":\"value\"}"));
    }

    #[test]
    fn decode_topic_config() {
        let mut config = TopicConfig::default();
        let input = "test_topic 8 8 2 SingleTag {\"key\":\"value\"}";
        let result = config.decode(input);
        assert!(result);
        assert_eq!(config.topic_name, Some(CheetahString::from("test_topic")));
        assert_eq!(config.read_queue_nums, 8);
        assert_eq!(config.write_queue_nums, 8);
        assert_eq!(config.perm, PermName::PERM_WRITE);
        assert_eq!(config.topic_filter_type, TopicFilterType::SingleTag);
        assert_eq!(
            config.attributes.get(&CheetahString::from("key")),
            Some(&CheetahString::from("value"))
        );
    }

    #[test]
    fn get_topic_message_type_normal() {
        let config = TopicConfig::default();
        assert_eq!(config.get_topic_message_type(), TopicMessageType::Normal);
    }

    #[test]
    fn get_topic_message_type_from_attributes() {
        let mut config = TopicConfig::default();
        config.attributes.insert(
            CheetahString::from(TopicAttributes::topic_message_type_attribute().name()),
            CheetahString::from("Normal"),
        );
        assert_eq!(config.get_topic_message_type(), TopicMessageType::Normal);
    }
}
