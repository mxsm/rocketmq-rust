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

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum ResourcePattern {
    Any = 1,
    Literal = 2,
    Prefixed = 3,
}

impl ResourcePattern {
    /// Case-insensitive lookup by name. Accepts either uppercase like Java or mixed case.
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("ANY") {
            Some(Self::Any)
        } else if name.eq_ignore_ascii_case("LITERAL") {
            Some(Self::Literal)
        } else if name.eq_ignore_ascii_case("PREFIXED") {
            Some(Self::Prefixed)
        } else {
            None
        }
    }

    #[inline]
    pub fn code(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn name(self) -> &'static str {
        match self {
            Self::Any => "ANY",
            Self::Literal => "LITERAL",
            Self::Prefixed => "PREFIXED",
        }
    }
}

impl Serialize for ResourcePattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for ResourcePattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(ResourcePattern::Any),
            2 => Ok(ResourcePattern::Literal),
            3 => Ok(ResourcePattern::Prefixed),
            _ => Err(serde::de::Error::custom("invalid ResourcePattern code")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::resource::resource_pattern::ResourcePattern;

    #[test]
    fn given_string_any_return_enum_value() {
        let any = ResourcePattern::get_by_name("ANY");
        assert_eq!(Some(ResourcePattern::Any), any);
    }

    #[test]
    fn given_string_literal_return_enum_value() {
        let literal = ResourcePattern::get_by_name("LITERAL");
        assert_eq!(Some(ResourcePattern::Literal), literal);
    }

    #[test]
    fn given_string_prefixed_return_enum_value() {
        let prefixed = ResourcePattern::get_by_name("PREFIXED");
        assert_eq!(Some(ResourcePattern::Prefixed), prefixed);
    }

    #[test]
    fn get_by_name_is_case_insensitive() {
        assert_eq!(Some(ResourcePattern::Any), ResourcePattern::get_by_name("aNy"));
        assert_eq!(Some(ResourcePattern::Literal), ResourcePattern::get_by_name("LiTeRaL"));
        assert_eq!(
            Some(ResourcePattern::Prefixed),
            ResourcePattern::get_by_name("pReFixed")
        );
    }

    #[test]
    fn invalid_name_returns_none() {
        assert!(ResourcePattern::get_by_name("UNKNOWN").is_none());
        assert!(ResourcePattern::get_by_name("123").is_none());
        assert!(ResourcePattern::get_by_name("").is_none());
    }

    #[test]
    fn name_and_code_methods_are_consistent() {
        assert_eq!("ANY", ResourcePattern::Any.name());
        assert_eq!(1, ResourcePattern::Any.code());
        assert_eq!("LITERAL", ResourcePattern::Literal.name());
        assert_eq!(2, ResourcePattern::Literal.code());
        assert_eq!("PREFIXED", ResourcePattern::Prefixed.name());
        assert_eq!(3, ResourcePattern::Prefixed.code());
    }

    #[test]
    fn serde_json_serializes_to_number() {
        assert_eq!(
            "1",
            serde_json::to_string(&ResourcePattern::Any).expect("ResourcePattern::Any must serialize")
        );
        assert_eq!(
            "2",
            serde_json::to_string(&ResourcePattern::Literal).expect("ResourcePattern::Literal must serialize")
        );
        assert_eq!(
            "3",
            serde_json::to_string(&ResourcePattern::Prefixed).expect("ResourcePattern::Prefixed must serialize")
        );
    }

    #[test]
    fn serde_json_deserializes_from_number() {
        let any: ResourcePattern = serde_json::from_str("1").expect("Must deserialize 1");
        assert_eq!(ResourcePattern::Any, any);

        let literal: ResourcePattern = serde_json::from_str("2").expect("Must deserialize 2");
        assert_eq!(ResourcePattern::Literal, literal);

        let prefixed: ResourcePattern = serde_json::from_str("3").expect("Must deserialize 3");
        assert_eq!(ResourcePattern::Prefixed, prefixed);
    }

    #[test]
    fn serde_json_deserializes_invalid_fails() {
        assert!(serde_json::from_str::<ResourcePattern>("0").is_err());
        assert!(serde_json::from_str::<ResourcePattern>("999").is_err());
    }

    #[test]
    fn serde_json_roundtrip_variants() {
        for &variant in &[
            ResourcePattern::Any,
            ResourcePattern::Literal,
            ResourcePattern::Prefixed,
        ] {
            let serialized = serde_json::to_string(&variant)
                .unwrap_or_else(|e| panic!("Could not serialize ResourcePattern::{:?}: {}", &variant.name(), e));
            let parsed: ResourcePattern = serde_json::from_str(&serialized)
                .unwrap_or_else(|e| panic!("Could not parse {:?} as ResourcePattern: {}", &serialized, e));
            assert_eq!(variant, parsed);
        }
    }
}
