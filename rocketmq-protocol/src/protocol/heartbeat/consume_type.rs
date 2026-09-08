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

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Default)]
pub enum ConsumeType {
    #[default]
    ConsumeActively,
    ConsumePassively,
    ConsumePop,
}

impl Serialize for ConsumeType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            ConsumeType::ConsumeActively => "CONSUME_ACTIVELY",
            ConsumeType::ConsumePassively => "CONSUME_PASSIVELY",
            ConsumeType::ConsumePop => "CONSUME_POP",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for ConsumeType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConsumeTypeVisitor;

        impl serde::de::Visitor<'_> for ConsumeTypeVisitor {
            type Value = ConsumeType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing TopicFilterType")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CONSUME_ACTIVELY" => Ok(ConsumeType::ConsumeActively),
                    "CONSUME_PASSIVELY" => Ok(ConsumeType::ConsumePassively),
                    "CONSUME_POP" => Ok(ConsumeType::ConsumePop),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["ConsumeActively", "ConsumePassively", "ConsumePop"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(ConsumeTypeVisitor)
    }
}

impl Display for ConsumeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsumeType::ConsumeActively => write!(f, "PULL"),
            ConsumeType::ConsumePassively => write!(f, "PUSH"),
            ConsumeType::ConsumePop => write!(f, "POP"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WIRE_PAIRS: [(ConsumeType, &str); 3] = [
        (ConsumeType::ConsumeActively, "\"CONSUME_ACTIVELY\""),
        (ConsumeType::ConsumePassively, "\"CONSUME_PASSIVELY\""),
        (ConsumeType::ConsumePop, "\"CONSUME_POP\""),
    ];

    #[test]
    fn serialize_consume_type_to_java_wire_string() {
        for (consume_type, json) in WIRE_PAIRS {
            assert_eq!(serde_json::to_string(&consume_type).unwrap(), json);
        }
    }

    #[test]
    fn deserialize_java_wire_string_to_consume_type() {
        for (consume_type, json) in WIRE_PAIRS {
            let deserialized: ConsumeType = serde_json::from_str(json).unwrap();
            assert_eq!(deserialized, consume_type);

            let round_tripped: ConsumeType =
                serde_json::from_str(&serde_json::to_string(&consume_type).unwrap()).unwrap();
            assert_eq!(round_tripped, consume_type);
        }
    }

    #[test]
    fn display_consume_type() {
        assert_eq!(ConsumeType::ConsumeActively.to_string(), "PULL");
        assert_eq!(ConsumeType::ConsumePassively.to_string(), "PUSH");
        assert_eq!(ConsumeType::ConsumePop.to_string(), "POP");
    }

    #[test]
    fn deserialize_consume_type_unknown_string_names_supported_variants() {
        for json in ["\"CONSUME_UNKNOWN\"", "\"PULL\""] {
            let message = serde_json::from_str::<ConsumeType>(json).unwrap_err().to_string();
            assert!(message.contains("unknown variant"), "{message}");
            for variant in ["ConsumeActively", "ConsumePassively", "ConsumePop"] {
                assert!(message.contains(variant), "{message}");
            }
        }
    }

    #[test]
    fn default_consume_type_is_consume_actively() {
        assert_eq!(ConsumeType::default(), ConsumeType::ConsumeActively);
    }

    #[test]
    fn consume_type_embedded_in_struct_uses_java_wire_string() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Wrapper {
            consume_type: ConsumeType,
        }

        let wrapper = Wrapper {
            consume_type: ConsumeType::ConsumePassively,
        };
        let json = serde_json::to_string(&wrapper).unwrap();
        assert_eq!(json, r#"{"consume_type":"CONSUME_PASSIVELY"}"#);

        let deserialized: Wrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, wrapper);
    }
}
