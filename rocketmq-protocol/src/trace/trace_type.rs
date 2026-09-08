// Copyright 2026 The RocketMQ Rust Authors
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

use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TraceType {
    #[default]
    Pub,
    SubBefore,
    SubAfter,
    EndTransaction,
    Recall,
}

impl TraceType {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Pub" => Some(Self::Pub),
            "SubBefore" => Some(Self::SubBefore),
            "SubAfter" => Some(Self::SubAfter),
            "EndTransaction" => Some(Self::EndTransaction),
            "Recall" => Some(Self::Recall),
            _ => None,
        }
    }
}

impl Display for TraceType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Pub => "Pub",
            Self::SubBefore => "SubBefore",
            Self::SubAfter => "SubAfter",
            Self::EndTransaction => "EndTransaction",
            Self::Recall => "Recall",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WIRE_NAMES: [(&str, TraceType); 5] = [
        ("Pub", TraceType::Pub),
        ("SubBefore", TraceType::SubBefore),
        ("SubAfter", TraceType::SubAfter),
        ("EndTransaction", TraceType::EndTransaction),
        ("Recall", TraceType::Recall),
    ];

    #[test]
    fn parse_maps_each_wire_name_to_its_variant() {
        for (name, expected) in WIRE_NAMES {
            assert_eq!(TraceType::parse(name), Some(expected), "parse({name:?})");
        }
    }

    #[test]
    fn parse_rejects_unknown_empty_and_case_variants() {
        for input in [
            "",
            "Unknown",
            "pub",
            "PUB",
            "subBefore",
            "SUBAFTER",
            " Pub",
            "Pub ",
            "Pub\u{1}",
        ] {
            assert_eq!(TraceType::parse(input), None, "parse({input:?}) should be None");
        }
    }

    #[test]
    fn display_prints_exact_wire_name_and_round_trips_through_parse() {
        for (name, variant) in WIRE_NAMES {
            let rendered = variant.to_string();
            assert_eq!(rendered, name, "{variant:?} wire name");
            assert_eq!(TraceType::parse(&rendered), Some(variant), "{variant:?} round trip");
        }
    }

    #[test]
    fn default_is_pub() {
        assert_eq!(TraceType::default(), TraceType::Pub);
    }
}
