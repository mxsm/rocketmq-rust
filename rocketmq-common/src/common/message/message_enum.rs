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

use std::fmt;

use serde::de;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, PartialEq, Copy, Clone, Default)]
pub enum MessageType {
    #[default]
    NormalMsg,
    TransMsgHalf,
    TransMsgCommit,
    DelayMsg,
    OrderMsg,
}

impl MessageType {
    pub fn get_short_name(&self) -> &'static str {
        match self {
            MessageType::NormalMsg => "Normal",
            MessageType::TransMsgHalf => "Trans",
            MessageType::TransMsgCommit => "TransCommit",
            MessageType::DelayMsg => "Delay",
            MessageType::OrderMsg => "Order",
        }
    }

    pub fn get_by_short_name(short_name: &str) -> MessageType {
        match short_name {
            "Normal" => MessageType::NormalMsg,
            "Trans" => MessageType::TransMsgHalf,
            "TransCommit" => MessageType::TransMsgCommit,
            "Delay" => MessageType::DelayMsg,
            "Order" => MessageType::OrderMsg,
            _ => MessageType::NormalMsg,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone, Hash, Eq)]
pub enum MessageRequestMode {
    Pull,
    Pop,
}

impl MessageRequestMode {
    pub fn get_name(&self) -> &'static str {
        match self {
            MessageRequestMode::Pull => "PULL",
            MessageRequestMode::Pop => "POP",
        }
    }
}

impl Serialize for MessageRequestMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match *self {
            MessageRequestMode::Pull => "PULL",
            MessageRequestMode::Pop => "POP",
        })
    }
}

impl<'de> Deserialize<'de> for MessageRequestMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageRequestModeVisitor;

        impl Visitor<'_> for MessageRequestModeVisitor {
            type Value = MessageRequestMode;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing a MessageRequestMode")
            }

            fn visit_str<E>(self, value: &str) -> Result<MessageRequestMode, E>
            where
                E: de::Error,
            {
                match value {
                    "PULL" | "Pull" => Ok(MessageRequestMode::Pull),
                    "POP" | "Pop" => Ok(MessageRequestMode::Pop),
                    _ => Err(de::Error::unknown_variant(value, &["PULL/Pull", "POP/Pop"])),
                }
            }
        }

        deserializer.deserialize_str(MessageRequestModeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_short_name() {
        assert_eq!(MessageType::NormalMsg.get_short_name(), "Normal");
        assert_eq!(MessageType::TransMsgHalf.get_short_name(), "Trans");
        assert_eq!(MessageType::TransMsgCommit.get_short_name(), "TransCommit");
        assert_eq!(MessageType::DelayMsg.get_short_name(), "Delay");
        assert_eq!(MessageType::OrderMsg.get_short_name(), "Order");
    }

    #[test]
    fn test_get_by_short_name() {
        assert_eq!(MessageType::get_by_short_name("Normal"), MessageType::NormalMsg);
        assert_eq!(MessageType::get_by_short_name("Trans"), MessageType::TransMsgHalf);
        assert_eq!(
            MessageType::get_by_short_name("TransCommit"),
            MessageType::TransMsgCommit
        );
        assert_eq!(MessageType::get_by_short_name("Delay"), MessageType::DelayMsg);
        assert_eq!(MessageType::get_by_short_name("Order"), MessageType::OrderMsg);
        assert_eq!(MessageType::get_by_short_name("Invalid"), MessageType::NormalMsg);
    }

    #[test]
    fn test_get_name() {
        assert_eq!(MessageRequestMode::Pull.get_name(), "PULL");
        assert_eq!(MessageRequestMode::Pop.get_name(), "POP");
    }

    #[test]
    fn serialize_message_request_mode_pull() {
        let mode = MessageRequestMode::Pull;
        let serialized = serde_json::to_string(&mode).unwrap();
        assert_eq!(serialized, "\"PULL\"");
    }

    #[test]
    fn serialize_message_request_mode_pop() {
        let mode = MessageRequestMode::Pop;
        let serialized = serde_json::to_string(&mode).unwrap();
        assert_eq!(serialized, "\"POP\"");
    }

    #[test]
    fn deserialize_message_request_mode_pull() {
        let json = "\"PULL\"";
        let deserialized: MessageRequestMode = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MessageRequestMode::Pull);

        let json = "\"Pull\"";
        let deserialized: MessageRequestMode = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MessageRequestMode::Pull);
    }

    #[test]
    fn deserialize_message_request_mode_pop() {
        let json = "\"POP\"";
        let deserialized: MessageRequestMode = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MessageRequestMode::Pop);

        let json = "\"Pop\"";
        let deserialized: MessageRequestMode = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MessageRequestMode::Pop);
    }

    #[test]
    fn deserialize_message_request_mode_invalid() {
        let json = "\"INVALID\"";
        let deserialized: Result<MessageRequestMode, _> = serde_json::from_str(json);
        assert!(deserialized.is_err());
    }
}
