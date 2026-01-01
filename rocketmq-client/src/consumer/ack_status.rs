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

use std::fmt::Display;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckStatus {
    #[default]
    Ok, //ack success
    NotExist, // msg not exist
}

impl AckStatus {
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(AckStatus::Ok),
            1 => Some(AckStatus::NotExist),
            _ => None,
        }
    }

    pub fn to_i32(self) -> i32 {
        match self {
            AckStatus::Ok => 0,
            AckStatus::NotExist => 1,
        }
    }
}

impl Display for AckStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AckStatus::Ok => write!(f, "OK"),
            AckStatus::NotExist => write!(f, "NO_EXIST"),
        }
    }
}

impl From<String> for AckStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "OK" => AckStatus::Ok,
            "NO_EXIST" => AckStatus::NotExist,
            _ => AckStatus::default(),
        }
    }
}

impl From<&str> for AckStatus {
    fn from(s: &str) -> Self {
        match s {
            "OK" => AckStatus::Ok,
            "NO_EXIST" => AckStatus::NotExist,
            _ => AckStatus::default(),
        }
    }
}

impl From<AckStatus> for String {
    fn from(status: AckStatus) -> Self {
        match status {
            AckStatus::Ok => "OK".to_string(),
            AckStatus::NotExist => "NO_EXIST".to_string(),
        }
    }
}

impl From<AckStatus> for i32 {
    fn from(status: AckStatus) -> Self {
        match status {
            AckStatus::Ok => 0,
            AckStatus::NotExist => 1,
        }
    }
}

impl From<i32> for AckStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => AckStatus::Ok,
            1 => AckStatus::NotExist,
            _ => AckStatus::default(),
        }
    }
}

impl From<CheetahString> for AckStatus {
    fn from(s: CheetahString) -> Self {
        match s.as_str() {
            "OK" => AckStatus::Ok,
            "NO_EXIST" => AckStatus::NotExist,
            _ => AckStatus::default(),
        }
    }
}

impl Serialize for AckStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match *self {
            AckStatus::Ok => "OK",
            AckStatus::NotExist => "NO_EXIST",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for AckStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "OK" => Ok(AckStatus::Ok),
            "NO_EXIST" => Ok(AckStatus::NotExist),
            _ => Err(serde::de::Error::custom("unknown value")),
        }
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use serde_json;

    use super::*;

    #[test]
    fn ack_status_from_i32_valid_values() {
        assert_eq!(AckStatus::from_i32(0), Some(AckStatus::Ok));
        assert_eq!(AckStatus::from_i32(1), Some(AckStatus::NotExist));
    }

    #[test]
    fn ack_status_from_i32_invalid_value() {
        assert_eq!(AckStatus::from_i32(2), None);
    }

    #[test]
    fn ack_status_to_i32() {
        assert_eq!(AckStatus::Ok.to_i32(), 0);
        assert_eq!(AckStatus::NotExist.to_i32(), 1);
    }

    #[test]
    fn ack_status_display() {
        assert_eq!(format!("{}", AckStatus::Ok), "OK");
        assert_eq!(format!("{}", AckStatus::NotExist), "NO_EXIST");
    }

    #[test]
    fn ack_status_from_string() {
        assert_eq!(AckStatus::from("OK".to_string()), AckStatus::Ok);
        assert_eq!(AckStatus::from("NO_EXIST".to_string()), AckStatus::NotExist);
        assert_eq!(AckStatus::from("UNKNOWN".to_string()), AckStatus::default());
    }

    #[test]
    fn ack_status_from_str() {
        assert_eq!(AckStatus::from("OK"), AckStatus::Ok);
        assert_eq!(AckStatus::from("NO_EXIST"), AckStatus::NotExist);
        assert_eq!(AckStatus::from("UNKNOWN"), AckStatus::default());
    }

    #[test]
    fn ack_status_from_cheetah_string() {
        assert_eq!(AckStatus::from(CheetahString::from("OK")), AckStatus::Ok);
        assert_eq!(AckStatus::from(CheetahString::from("NO_EXIST")), AckStatus::NotExist);
        assert_eq!(AckStatus::from(CheetahString::from("UNKNOWN")), AckStatus::default());
    }

    #[test]
    fn ack_status_to_string() {
        assert_eq!(String::from(AckStatus::Ok), "OK".to_string());
        assert_eq!(String::from(AckStatus::NotExist), "NO_EXIST".to_string());
    }

    #[test]
    fn ack_status_from_i32_conversion() {
        assert_eq!(i32::from(AckStatus::Ok), 0);
        assert_eq!(i32::from(AckStatus::NotExist), 1);
    }

    #[test]
    fn ack_status_from_i32_default() {
        assert_eq!(AckStatus::from(2), AckStatus::default());
    }

    #[test]
    fn ack_status_serialize() {
        let ok_status = AckStatus::Ok;
        let serialized = serde_json::to_string(&ok_status).unwrap();
        assert_eq!(serialized, "\"OK\"");

        let not_exist_status = AckStatus::NotExist;
        let serialized = serde_json::to_string(&not_exist_status).unwrap();
        assert_eq!(serialized, "\"NO_EXIST\"");
    }

    #[test]
    fn ack_status_deserialize() {
        let ok_status: AckStatus = serde_json::from_str("\"OK\"").unwrap();
        assert_eq!(ok_status, AckStatus::Ok);

        let not_exist_status: AckStatus = serde_json::from_str("\"NO_EXIST\"").unwrap();
        assert_eq!(not_exist_status, AckStatus::NotExist);
    }
}
