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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrackType {
    Consumed,
    ConsumedButFiltered,
    Pull,
    NotConsumedYet,
    NotOnline,
    ConsumeBroadcasting,
    Unknown,
}

impl std::fmt::Display for TrackType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackType::Consumed => write!(f, "CONSUMED"),
            TrackType::ConsumedButFiltered => write!(f, "CONSUMED_BUT_FILTERED"),
            TrackType::Pull => write!(f, "PULL"),
            TrackType::NotConsumedYet => write!(f, "NOT_CONSUME_YET"),
            TrackType::NotOnline => write!(f, "NOT_ONLINE"),
            TrackType::ConsumeBroadcasting => write!(f, "CONSUME_BROADCASTING"),
            TrackType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl Serialize for TrackType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match *self {
            TrackType::Consumed => "CONSUMED",
            TrackType::ConsumedButFiltered => "CONSUMED_BUT_FILTERED",
            TrackType::Pull => "PULL",
            TrackType::NotConsumedYet => "NOT_CONSUME_YET",
            TrackType::NotOnline => "NOT_ONLINE",
            TrackType::ConsumeBroadcasting => "CONSUME_BROADCASTING",
            TrackType::Unknown => "UNKNOWN",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for TrackType {
    fn deserialize<D>(deserializer: D) -> Result<TrackType, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TrackTypeVisitor;

        impl Visitor<'_> for TrackTypeVisitor {
            type Value = TrackType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid track type string")
            }

            fn visit_str<E>(self, value: &str) -> Result<TrackType, E>
            where
                E: de::Error,
            {
                match value {
                    "CONSUMED" => Ok(TrackType::Consumed),
                    "CONSUMED_BUT_FILTERED" => Ok(TrackType::ConsumedButFiltered),
                    "PULL" => Ok(TrackType::Pull),
                    "NOT_CONSUME_YET" => Ok(TrackType::NotConsumedYet),
                    "NOT_ONLINE" => Ok(TrackType::NotOnline),
                    "CONSUME_BROADCASTING" => Ok(TrackType::ConsumeBroadcasting),
                    "UNKNOWN" => Ok(TrackType::Unknown),
                    _ => Err(de::Error::unknown_variant(
                        value,
                        &[
                            "CONSUMED",
                            "CONSUMED_BUT_FILTERED",
                            "PULL",
                            "NOT_CONSUME_YET",
                            "NOT_ONLINE",
                            "CONSUME_BROADCASTING",
                            "UNKNOWN",
                        ],
                    )),
                }
            }
        }
        deserializer.deserialize_str(TrackTypeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_consumed_but_filtered() {
        let track_type = TrackType::ConsumedButFiltered;
        let serialized = serde_json::to_string(&track_type).unwrap();
        assert_eq!(serialized, "\"CONSUMED_BUT_FILTERED\"");
    }

    #[test]
    fn deserialize_not_consumed_yet() {
        let json = "\"NOT_CONSUME_YET\"";
        let deserialized: TrackType = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, TrackType::NotConsumedYet);
    }

    #[test]
    fn serialize_not_online() {
        let track_type = TrackType::NotOnline;
        let serialized = serde_json::to_string(&track_type).unwrap();
        assert_eq!(serialized, "\"NOT_ONLINE\"");
    }

    #[test]
    fn deserialize_consume_broadcasting() {
        let json = "\"CONSUME_BROADCASTING\"";
        let deserialized: TrackType = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized, TrackType::ConsumeBroadcasting);
    }

    #[test]
    fn display_pull_variant() {
        let track_type = TrackType::Pull;
        assert_eq!(track_type.to_string(), "PULL");
    }
}
