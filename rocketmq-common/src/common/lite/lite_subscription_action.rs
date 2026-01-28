//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::fmt;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Default)]
pub enum LiteSubscriptionAction {
    #[default]
    PartialAdd,
    PartialRemove,
    CompleteAdd,
    CompleteRemove,
}

impl Serialize for LiteSubscriptionAction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            LiteSubscriptionAction::PartialAdd => "PARTIAL_ADD",
            LiteSubscriptionAction::PartialRemove => "PARTIAL_REMOVE",
            LiteSubscriptionAction::CompleteAdd => "COMPLETE_ADD",
            LiteSubscriptionAction::CompleteRemove => "COMPLETE_REMOVE",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for LiteSubscriptionAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LiteSubscriptionActionVisitor;

        impl serde::de::Visitor<'_> for LiteSubscriptionActionVisitor {
            type Value = LiteSubscriptionAction;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing LiteSubscriptionAction")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "PARTIAL_ADD" => Ok(LiteSubscriptionAction::PartialAdd),
                    "PARTIAL_REMOVE" => Ok(LiteSubscriptionAction::PartialRemove),
                    "COMPLETE_ADD" => Ok(LiteSubscriptionAction::CompleteAdd),
                    "COMPLETE_REMOVE" => Ok(LiteSubscriptionAction::CompleteRemove),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["PartialAdd", "PartialRemove", "CompleteAdd", "CompleteRemove"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(LiteSubscriptionActionVisitor)
    }
}

impl fmt::Display for LiteSubscriptionAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteSubscriptionAction::PartialAdd => write!(f, "PARTIAL_ADD"),
            LiteSubscriptionAction::PartialRemove => write!(f, "PARTIAL_REMOVE"),
            LiteSubscriptionAction::CompleteAdd => write!(f, "COMPLETE_ADD"),
            LiteSubscriptionAction::CompleteRemove => write!(f, "COMPLETE_REMOVE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lite_subscription_action_serialize() {
        assert_eq!(
            serde_json::to_string(&LiteSubscriptionAction::PartialAdd).unwrap(),
            "\"PARTIAL_ADD\""
        );
        assert_eq!(
            serde_json::to_string(&LiteSubscriptionAction::PartialRemove).unwrap(),
            "\"PARTIAL_REMOVE\""
        );
        assert_eq!(
            serde_json::to_string(&LiteSubscriptionAction::CompleteAdd).unwrap(),
            "\"COMPLETE_ADD\""
        );
        assert_eq!(
            serde_json::to_string(&LiteSubscriptionAction::CompleteRemove).unwrap(),
            "\"COMPLETE_REMOVE\""
        );
    }

    #[test]
    fn lite_subscription_action_deserialize() {
        assert_eq!(
            serde_json::from_str::<LiteSubscriptionAction>("\"PARTIAL_ADD\"").unwrap(),
            LiteSubscriptionAction::PartialAdd
        );
        assert_eq!(
            serde_json::from_str::<LiteSubscriptionAction>("\"PARTIAL_REMOVE\"").unwrap(),
            LiteSubscriptionAction::PartialRemove
        );
        assert_eq!(
            serde_json::from_str::<LiteSubscriptionAction>("\"COMPLETE_ADD\"").unwrap(),
            LiteSubscriptionAction::CompleteAdd
        );
        assert_eq!(
            serde_json::from_str::<LiteSubscriptionAction>("\"COMPLETE_REMOVE\"").unwrap(),
            LiteSubscriptionAction::CompleteRemove
        );
        assert!(serde_json::from_str::<LiteSubscriptionAction>("\"unknown\"").is_err());
    }

    #[test]
    fn lite_subscription_action_display() {
        assert_eq!(format!("{}", LiteSubscriptionAction::PartialAdd), "PARTIAL_ADD");
        assert_eq!(format!("{}", LiteSubscriptionAction::PartialRemove), "PARTIAL_REMOVE");
        assert_eq!(format!("{}", LiteSubscriptionAction::CompleteAdd), "COMPLETE_ADD");
        assert_eq!(format!("{}", LiteSubscriptionAction::CompleteRemove), "COMPLETE_REMOVE");
    }

    #[test]
    fn lite_subscription_action_default() {
        assert_eq!(LiteSubscriptionAction::default(), LiteSubscriptionAction::PartialAdd);
    }
}
