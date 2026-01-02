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
pub enum PolicyType {
    Custom = 1,
    Default = 2,
}

impl PolicyType {
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Custom") {
            Some(Self::Custom)
        } else if name.eq_ignore_ascii_case("Default") {
            Some(Self::Default)
        } else {
            None
        }
    }

    pub fn code(self) -> u8 {
        self as u8
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Custom => "Custom",
            Self::Default => "Default",
        }
    }
}

impl Serialize for PolicyType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for PolicyType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(PolicyType::Custom),
            2 => Ok(PolicyType::Default),
            _ => Err(serde::de::Error::custom("invalid PolicyType code")),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::authorization::enums::policy_type::PolicyType;

    #[test]
    fn get_by_name_with_string_custom_return_enum_variant() {
        let custom = PolicyType::get_by_name("Custom");
        assert_eq!(Some(PolicyType::Custom), custom);
    }

    #[test]
    fn get_by_name_with_string_default_return_enum_variant() {
        let default = PolicyType::get_by_name("Default");
        assert_eq!(Some(PolicyType::Default), default);
    }

    #[test]
    fn get_by_name_is_case_insensitive() {
        assert_eq!(Some(PolicyType::Custom), PolicyType::get_by_name("cUsToM"));
        assert_eq!(Some(PolicyType::Default), PolicyType::get_by_name("DeFaUlT"));
    }

    #[test]
    fn invalid_name_returns_none() {
        assert!(PolicyType::get_by_name("UNKNOWN").is_none());
        assert!(PolicyType::get_by_name("123").is_none());
        assert!(PolicyType::get_by_name("").is_none());
    }

    #[test]
    fn name_and_code_methods_are_consistent() {
        assert_eq!("Custom", PolicyType::Custom.name());
        assert_eq!(1, PolicyType::Custom.code());
        assert_eq!("Default", PolicyType::Default.name());
        assert_eq!(2, PolicyType::Default.code());
    }

    #[test]
    fn serde_json_serializes_to_number() {
        assert_eq!(
            "1",
            serde_json::to_string(&PolicyType::Custom).expect("PolicyType::Custom must serialize")
        );
        assert_eq!(
            "2",
            serde_json::to_string(&PolicyType::Default).expect("PolicyType::Default must serialize")
        );
    }

    #[test]
    fn serde_json_deserializes_from_number() {
        let custom: PolicyType = serde_json::from_str("1").expect("Must deserialize 1");
        assert_eq!(PolicyType::Custom, custom);

        let default: PolicyType = serde_json::from_str("2").expect("Must deserialize 2");
        assert_eq!(PolicyType::Default, default);
    }

    #[test]
    fn serde_json_deserializes_invalid_fails() {
        assert!(serde_json::from_str::<PolicyType>("0").is_err());
        assert!(serde_json::from_str::<PolicyType>("999").is_err());
    }

    #[test]
    fn serde_json_roundtrip_variants() {
        for &variant in &[PolicyType::Custom, PolicyType::Default] {
            let serialized = serde_json::to_string(&variant)
                .unwrap_or_else(|e| panic!("Could not serialize PolicyType::{:?}: {}", &variant.name(), e));
            let parsed: PolicyType = serde_json::from_str(&serialized)
                .unwrap_or_else(|e| panic!("Could not parse {:?} as PolicyType: {}", &serialized, e));
            assert_eq!(variant, parsed);
        }
    }
}
