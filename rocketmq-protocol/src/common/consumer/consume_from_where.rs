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

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(deprecated)]
pub enum ConsumeFromWhere {
    ///On the first startup, consume from the last position of the queue; on subsequent startups,
    /// continue consuming from the last consumed position.
    #[default]
    ConsumeFromLastOffset,

    #[deprecated]
    ConsumeFromLastOffsetAndFromMinWhenBootFirst,

    #[deprecated]
    ConsumeFromMinOffset,

    #[deprecated]
    ConsumeFromMaxOffset,

    ///On the first startup, consume from the initial position of the queue; on subsequent
    /// startups, continue consuming from the last consumed position.
    ConsumeFromFirstOffset,

    ///: On the first startup, consume from the specified timestamp position; on subsequent
    /// startups, continue consuming from the last consumed position.
    ConsumeFromTimestamp,
}

#[allow(deprecated)]
impl From<i32> for ConsumeFromWhere {
    fn from(value: i32) -> Self {
        match value {
            0 => ConsumeFromWhere::ConsumeFromLastOffset,
            1 => ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst,
            2 => ConsumeFromWhere::ConsumeFromMinOffset,
            3 => ConsumeFromWhere::ConsumeFromMaxOffset,
            4 => ConsumeFromWhere::ConsumeFromFirstOffset,
            5 => ConsumeFromWhere::ConsumeFromTimestamp,
            _ => ConsumeFromWhere::ConsumeFromLastOffset,
        }
    }
}

#[allow(deprecated)]
impl From<ConsumeFromWhere> for i32 {
    fn from(consume_from_where: ConsumeFromWhere) -> i32 {
        match consume_from_where {
            ConsumeFromWhere::ConsumeFromLastOffset => 0,
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => 1,
            ConsumeFromWhere::ConsumeFromMinOffset => 2,
            ConsumeFromWhere::ConsumeFromMaxOffset => 3,
            ConsumeFromWhere::ConsumeFromFirstOffset => 4,
            ConsumeFromWhere::ConsumeFromTimestamp => 5,
        }
    }
}

#[allow(deprecated)]
impl Serialize for ConsumeFromWhere {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            ConsumeFromWhere::ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET",
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => {
                "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
            }
            ConsumeFromWhere::ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET",
            ConsumeFromWhere::ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET",
            ConsumeFromWhere::ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET",
            ConsumeFromWhere::ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP",
        };
        serializer.serialize_str(value)
    }
}

#[allow(deprecated)]
impl<'de> Deserialize<'de> for ConsumeFromWhere {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConsumeFromWhereVisitor;

        impl serde::de::Visitor<'_> for ConsumeFromWhereVisitor {
            type Value = ConsumeFromWhere;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing TopicFilterType")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CONSUME_FROM_LAST_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromLastOffset),
                    "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" => {
                        Ok(ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst)
                    }
                    "CONSUME_FROM_MIN_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromMinOffset),
                    "CONSUME_FROM_MAX_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromMaxOffset),
                    "CONSUME_FROM_FIRST_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromFirstOffset),
                    "CONSUME_FROM_TIMESTAMP" => Ok(ConsumeFromWhere::ConsumeFromTimestamp),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &[
                            "CONSUME_FROM_LAST_OFFSET",
                            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
                            "CONSUME_FROM_MIN_OFFSET",
                            "CONSUME_FROM_MAX_OFFSET",
                            "CONSUME_FROM_FIRST_OFFSET",
                            "CONSUME_FROM_TIMESTAMP",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_str(ConsumeFromWhereVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn serialize_consume_from_where() {
        let consume_from_where = ConsumeFromWhere::ConsumeFromLastOffset;
        let serialized = serde_json::to_string(&consume_from_where).unwrap();
        assert_eq!(serialized, "\"CONSUME_FROM_LAST_OFFSET\"");
    }

    #[test]
    fn deserialize_consume_from_where() {
        let data = json!("CONSUME_FROM_LAST_OFFSET");
        let consume_from_where: ConsumeFromWhere = serde_json::from_value(data).unwrap();
        assert_eq!(consume_from_where, ConsumeFromWhere::ConsumeFromLastOffset);
    }

    #[test]
    fn deserialize_invalid_consume_from_where() {
        let data = json!("INVALID_VALUE");
        let result: Result<ConsumeFromWhere, _> = serde_json::from_value(data);
        assert!(result.is_err());
    }
}
