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
pub enum UserStatus {
    Enable = 1,
    Disable = 2,
}

impl UserStatus {
    #[inline]
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("enable") {
            Some(UserStatus::Enable)
        } else if name.eq_ignore_ascii_case("disable") {
            Some(UserStatus::Disable)
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
            UserStatus::Enable => "enable",
            UserStatus::Disable => "disable",
        }
    }
}

impl Serialize for UserStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for UserStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(UserStatus::Enable),
            2 => Ok(UserStatus::Disable),
            _ => Err(serde::de::Error::custom("invalid UserStatus")),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_get_by_name() {
        assert_eq!(UserStatus::get_by_name("enable"), Some(UserStatus::Enable));
        assert_eq!(UserStatus::get_by_name("ENABLE"), Some(UserStatus::Enable));
        assert_eq!(UserStatus::get_by_name("Enable"), Some(UserStatus::Enable));
        assert_eq!(UserStatus::get_by_name("disable"), Some(UserStatus::Disable));
        assert_eq!(UserStatus::get_by_name("DISABLE"), Some(UserStatus::Disable));
        assert_eq!(UserStatus::get_by_name("Disable"), Some(UserStatus::Disable));
        assert_eq!(UserStatus::get_by_name("invalid"), None);
        assert_eq!(UserStatus::get_by_name(""), None);
    }

    #[test]
    fn test_code() {
        assert_eq!(UserStatus::Enable.code(), 1);
        assert_eq!(UserStatus::Disable.code(), 2);
    }

    #[test]
    fn test_name() {
        assert_eq!(UserStatus::Enable.name(), "enable");
        assert_eq!(UserStatus::Disable.name(), "disable");
    }

    #[test]
    fn test_serialize() {
        let enable_json = serde_json::to_string(&UserStatus::Enable).unwrap();
        assert_eq!(enable_json, "1");

        let disable_json = serde_json::to_string(&UserStatus::Disable).unwrap();
        assert_eq!(disable_json, "2");
    }

    #[test]
    fn test_deserialize() {
        let enable: UserStatus = serde_json::from_str("1").unwrap();
        assert_eq!(enable, UserStatus::Enable);

        let disable: UserStatus = serde_json::from_str("2").unwrap();
        assert_eq!(disable, UserStatus::Disable);
    }

    #[test]
    fn test_deserialize_invalid() {
        let result: Result<UserStatus, _> = serde_json::from_str("0");
        assert!(result.is_err());

        let result: Result<UserStatus, _> = serde_json::from_str("3");
        assert!(result.is_err());

        let result: Result<UserStatus, _> = serde_json::from_str("\"invalid\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_serialization() {
        let original = UserStatus::Enable;
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: UserStatus = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);

        let original = UserStatus::Disable;
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: UserStatus = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }
}
