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
pub enum UserType {
    Super = 1,
    Normal = 2,
}

impl UserType {
    #[inline]
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Super") {
            Some(UserType::Super)
        } else if name.eq_ignore_ascii_case("Normal") {
            Some(UserType::Normal)
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
            UserType::Super => "Super",
            UserType::Normal => "Normal",
        }
    }
}

impl Serialize for UserType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for UserType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(UserType::Super),
            2 => Ok(UserType::Normal),
            _ => Err(serde::de::Error::custom("invalid UserType")),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_get_by_name() {
        assert_eq!(UserType::get_by_name("Super"), Some(UserType::Super));
        assert_eq!(UserType::get_by_name("super"), Some(UserType::Super));

        assert_eq!(UserType::get_by_name("Normal"), Some(UserType::Normal));
        assert_eq!(UserType::get_by_name("NORMAL"), Some(UserType::Normal));

        assert_eq!(UserType::get_by_name("invalid"), None);
    }

    #[test]
    fn test_code() {
        assert_eq!(UserType::Super.code(), 1);
        assert_eq!(UserType::Normal.code(), 2);
    }

    #[test]
    fn test_name() {
        assert_eq!(UserType::Super.name(), "Super");
        assert_eq!(UserType::Normal.name(), "Normal");
    }

    #[test]
    fn test_serialize() {
        let super_json = serde_json::to_string(&UserType::Super).unwrap();
        assert_eq!(super_json, "1");

        let normal_json = serde_json::to_string(&UserType::Normal).unwrap();
        assert_eq!(normal_json, "2");
    }

    #[test]
    fn test_deserialize() {
        let super_user: UserType = serde_json::from_str("1").unwrap();
        assert_eq!(super_user, UserType::Super);

        let normal_user: UserType = serde_json::from_str("2").unwrap();
        assert_eq!(normal_user, UserType::Normal);
    }

    #[test]
    fn test_deserialize_invalid() {
        let result: Result<UserType, _> = serde_json::from_str("0");
        assert!(result.is_err());

        let result: Result<UserType, _> = serde_json::from_str("3");
        assert!(result.is_err());

        let result: Result<UserType, _> = serde_json::from_str("\"Super\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip_serialization() {
        let original = UserType::Super;
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: UserType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);

        let original = UserType::Normal;
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: UserType = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }
}
