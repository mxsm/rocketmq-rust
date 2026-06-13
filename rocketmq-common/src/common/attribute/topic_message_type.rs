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
use std::fmt::Display;
use std::str::FromStr;

use crate::common::message::MessageConst;

#[derive(Debug, PartialEq, Eq, Hash)]
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
    fn from(s: String) -> Self {
        match s.to_uppercase().as_str() {
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
        vec![
            Self::Unspecified.as_str().to_string(),
            Self::Normal.as_str().to_string(),
            Self::Fifo.as_str().to_string(),
            Self::Delay.as_str().to_string(),
            Self::Transaction.as_str().to_string(),
            Self::Priority.as_str().to_string(),
            Self::Lite.as_str().to_string(),
            Self::Mixed.as_str().to_string(),
        ]
        .into_iter()
        .collect()
    }

    pub fn parse_from_message_property(message_property: &HashMap<String, String>) -> Self {
        if message_property
            .get(MessageConst::PROPERTY_TRANSACTION_PREPARED)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return Self::Transaction;
        } else if message_property.contains_key(MessageConst::PROPERTY_DELAY_TIME_LEVEL)
            || message_property.contains_key(MessageConst::PROPERTY_TIMER_DELIVER_MS)
            || message_property.contains_key(MessageConst::PROPERTY_TIMER_DELAY_SEC)
            || message_property.contains_key(MessageConst::PROPERTY_TIMER_DELAY_MS)
        {
            return Self::Delay;
        } else if message_property.contains_key(MessageConst::PROPERTY_SHARDING_KEY) {
            return Self::Fifo;
        } else if message_property.contains_key(MessageConst::PROPERTY_PRIORITY) {
            return Self::Priority;
        } else if message_property.contains_key(MessageConst::PROPERTY_LITE_TOPIC) {
            return Self::Lite;
        }
        Self::Normal
    }

    pub fn get_metrics_value(&self) -> String {
        self.as_str().to_ascii_lowercase()
    }
}

impl Display for TopicMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_topic_message_type_set() {
        let expected_set: HashSet<String> = vec![
            "UNSPECIFIED".to_string(),
            "NORMAL".to_string(),
            "FIFO".to_string(),
            "DELAY".to_string(),
            "TRANSACTION".to_string(),
            "PRIORITY".to_string(),
            "LITE".to_string(),
            "MIXED".to_string(),
        ]
        .into_iter()
        .collect();
        assert_eq!(TopicMessageType::topic_message_type_set(), expected_set);
    }

    #[test]
    fn test_parse_from_message_property_transaction() {
        let mut message_property = HashMap::new();
        message_property.insert(
            MessageConst::PROPERTY_TRANSACTION_PREPARED.to_string(),
            "true".to_string(),
        );
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Transaction
        );
    }

    #[test]
    fn test_parse_from_message_property_delay() {
        let mut message_property = HashMap::new();
        message_property.insert(MessageConst::PROPERTY_DELAY_TIME_LEVEL.to_string(), "1".to_string());
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Delay
        );
    }

    #[test]
    fn test_parse_from_message_property_fifo() {
        let mut message_property = HashMap::new();
        message_property.insert(MessageConst::PROPERTY_SHARDING_KEY.to_string(), "key".to_string());
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Fifo
        );
    }

    #[test]
    fn test_parse_from_message_property_normal() {
        let message_property = HashMap::new();
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Normal
        );
    }

    #[test]
    fn test_parse_from_message_property_transaction_matches_java_boolean_parse() {
        let mut message_property = HashMap::new();
        message_property.insert(
            MessageConst::PROPERTY_TRANSACTION_PREPARED.to_string(),
            "TRUE".to_string(),
        );
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Transaction
        );

        message_property.insert(
            MessageConst::PROPERTY_TRANSACTION_PREPARED.to_string(),
            "yes".to_string(),
        );
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Normal
        );
    }

    #[test]
    fn test_parse_from_message_property_priority() {
        let mut message_property = HashMap::new();
        message_property.insert(MessageConst::PROPERTY_PRIORITY.to_string(), "6".to_string());
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Priority
        );
    }

    #[test]
    fn test_parse_from_message_property_priority_precedes_lite_like_java() {
        let mut message_property = HashMap::new();
        message_property.insert(MessageConst::PROPERTY_PRIORITY.to_string(), "6".to_string());
        message_property.insert(MessageConst::PROPERTY_LITE_TOPIC.to_string(), "true".to_string());
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Priority
        );
    }

    #[test]
    fn test_parse_from_message_property_lite() {
        let mut message_property = HashMap::new();
        message_property.insert(MessageConst::PROPERTY_LITE_TOPIC.to_string(), "true".to_string());
        assert_eq!(
            TopicMessageType::parse_from_message_property(&message_property),
            TopicMessageType::Lite
        );
    }

    #[test]
    fn test_from_string_lite() {
        assert_eq!(TopicMessageType::from("LITE".to_string()), TopicMessageType::Lite);
    }

    #[test]
    fn test_from_string_priority() {
        assert_eq!(
            TopicMessageType::from("PRIORITY".to_string()),
            TopicMessageType::Priority
        );
    }

    #[test]
    fn test_get_metrics_value() {
        assert_eq!(TopicMessageType::Normal.get_metrics_value(), "normal");
        assert_eq!(TopicMessageType::Priority.get_metrics_value(), "priority");
    }
}
