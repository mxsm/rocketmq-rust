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

impl ConsumeType {
    fn get_type_cn(&self) -> &'static str {
        match self {
            ConsumeType::ConsumeActively => "PULL",
            ConsumeType::ConsumePassively => "PUSH",
            ConsumeType::ConsumePop => "POP",
        }
    }
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
