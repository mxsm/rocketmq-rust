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
use std::fmt::Display;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Default)]
pub enum MessageModel {
    Broadcasting,
    #[default]
    Clustering,
}

impl MessageModel {
    fn get_mode_cn(&self) -> &'static str {
        match self {
            MessageModel::Broadcasting => "BROADCASTING",
            MessageModel::Clustering => "CLUSTERING",
        }
    }
}

impl Serialize for MessageModel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            MessageModel::Broadcasting => "BROADCASTING",
            MessageModel::Clustering => "CLUSTERING",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for MessageModel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageModelVisitor;

        impl serde::de::Visitor<'_> for MessageModelVisitor {
            type Value = MessageModel;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing MessageModel")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "BROADCASTING" => Ok(MessageModel::Broadcasting),
                    "CLUSTERING" => Ok(MessageModel::Clustering),
                    _ => Err(E::custom(format_args!(
                        "MessageModel: {}",
                        E::unknown_variant(value, &["BROADCASTING", "CLUSTERING"])
                    ))),
                }
            }
        }

        deserializer.deserialize_str(MessageModelVisitor)
    }
}

impl Display for MessageModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get_mode_cn())
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn serialize_message_model_broadcasting() {
        let model = MessageModel::Broadcasting;
        let serialized = serde_json::to_string(&model).unwrap();
        assert_eq!(serialized, "\"BROADCASTING\"");
    }

    #[test]
    fn serialize_message_model_clustering() {
        let model = MessageModel::Clustering;
        let serialized = serde_json::to_string(&model).unwrap();
        assert_eq!(serialized, "\"CLUSTERING\"");
    }

    #[test]
    fn deserialize_message_model_broadcasting() {
        let json = "\"BROADCASTING\"";
        let deserialized: MessageModel = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MessageModel::Broadcasting);
    }

    #[test]
    fn deserialize_message_model_clustering() {
        let json = "\"CLUSTERING\"";
        let deserialized: MessageModel = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, MessageModel::Clustering);
    }

    #[test]
    fn deserialize_message_model_invalid() {
        let json = "\"INVALID\"";
        let error = serde_json::from_str::<MessageModel>(json).unwrap_err().to_string();
        for fragment in ["MessageModel", "unknown variant", "BROADCASTING", "CLUSTERING"] {
            assert!(error.contains(fragment), "{error}");
        }
        let error = serde_json::from_str::<MessageModel>("1").unwrap_err().to_string();
        assert!(error.contains("a string representing MessageModel"), "{error}");
    }

    #[test]
    fn display_message_model_broadcasting() {
        let model = MessageModel::Broadcasting;
        assert_eq!(model.to_string(), "BROADCASTING");
    }

    #[test]
    fn display_message_model_clustering() {
        let model = MessageModel::Clustering;
        assert_eq!(model.to_string(), "CLUSTERING");
    }
}
