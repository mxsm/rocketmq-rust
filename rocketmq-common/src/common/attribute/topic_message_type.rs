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
            "MIXED" => Self::Mixed,
            _ => Self::Unspecified,
        }
    }
}

impl TopicMessageType {
    pub fn topic_message_type_set() -> HashSet<String> {
        vec![
            Self::Unspecified.to_string(),
            Self::Normal.to_string(),
            Self::Fifo.to_string(),
            Self::Delay.to_string(),
            Self::Transaction.to_string(),
            Self::Mixed.to_string(),
        ]
        .into_iter()
        .collect()
    }

    pub fn parse_from_message_property(message_property: &HashMap<String, String>) -> Self {
        let is_trans = message_property.get(MessageConst::PROPERTY_TRANSACTION_PREPARED);
        let is_trans_value = "true";
        if Some(&is_trans_value.to_string()) == is_trans {
            return Self::Transaction;
        } else if message_property.contains_key(MessageConst::PROPERTY_DELAY_TIME_LEVEL)
            || message_property.contains_key(MessageConst::PROPERTY_TIMER_DELIVER_MS)
            || message_property.contains_key(MessageConst::PROPERTY_TIMER_DELAY_SEC)
        {
            return Self::Delay;
        } else if message_property.contains_key(MessageConst::PROPERTY_SHARDING_KEY) {
            return Self::Fifo;
        }
        Self::Normal
    }

    pub fn get_metrics_value(&self) -> String {
        self.to_string().to_lowercase()
    }
}

impl Display for TopicMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Self::Unspecified => "UNSPECIFIED".to_string(),
            Self::Normal => "NORMAL".to_string(),
            Self::Fifo => "FIFO".to_string(),
            Self::Delay => "DELAY".to_string(),
            Self::Transaction => "TRANSACTION".to_string(),
            Self::Mixed => "MIXED".to_string(),
        };
        write!(f, "{str}",)
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
    fn test_get_metrics_value() {
        assert_eq!(TopicMessageType::Normal.get_metrics_value(), "normal");
    }
}
