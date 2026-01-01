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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectType {
    User = 1,
}
impl SubjectType {
    #[inline]
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("User") {
            Some(SubjectType::User)
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
            SubjectType::User => "User",
        }
    }
}

impl Serialize for SubjectType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for SubjectType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(SubjectType::User),
            _ => Err(serde::de::Error::custom("invalid SubjectType")),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_get_by_name() {
        assert_eq!(SubjectType::get_by_name("User"), Some(SubjectType::User));
        assert_eq!(SubjectType::get_by_name("user"), Some(SubjectType::User));
        assert_eq!(SubjectType::get_by_name("USER"), Some(SubjectType::User));
        assert_eq!(SubjectType::get_by_name("invalid"), None);
    }

    #[test]
    fn test_code() {
        assert_eq!(SubjectType::User.code(), 1);
    }

    #[test]
    fn test_name() {
        assert_eq!(SubjectType::User.name(), "User");
    }

    #[test]
    fn test_serialize() {
        let user_json = serde_json::to_string(&SubjectType::User).unwrap();
        assert_eq!(user_json, "1");
    }

    #[test]
    fn test_deserialize() {
        let user: SubjectType = serde_json::from_str("1").unwrap();
        assert_eq!(user, SubjectType::User);
    }

    #[test]
    fn test_deserialize_invalid() {
        let result: Result<SubjectType, _> = serde_json::from_str("0");
        assert!(result.is_err());

        let result: Result<SubjectType, _> = serde_json::from_str("2");
        assert!(result.is_err());

        let result: Result<SubjectType, _> = serde_json::from_str("\"User\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_serialization() {
        let original = SubjectType::User;
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: SubjectType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }
}
