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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CMResult {
    #[default]
    CRSuccess,
    CRLater,
    CRRollback,
    CRCommit,
    CRThrowException,
    CRReturnNull,
}

impl Serialize for CMResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            CMResult::CRSuccess => "CR_SUCCESS",
            CMResult::CRLater => "CR_LATER",
            CMResult::CRRollback => "CR_ROLLBACK",
            CMResult::CRCommit => "CR_COMMIT",
            CMResult::CRThrowException => "CR_THROW_EXCEPTION",
            CMResult::CRReturnNull => "CR_RETURN_NULL",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for CMResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CMResultVisitor;

        impl serde::de::Visitor<'_> for CMResultVisitor {
            type Value = CMResult;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing CMResult")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CR_SUCCESS" => Ok(CMResult::CRSuccess),
                    "CR_LATER" => Ok(CMResult::CRLater),
                    "CR_ROLLBACK" => Ok(CMResult::CRRollback),
                    "CR_COMMIT" => Ok(CMResult::CRCommit),
                    "CR_THROW_EXCEPTION" => Ok(CMResult::CRThrowException),
                    "CR_RETURN_NULL" => Ok(CMResult::CRReturnNull),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &[
                            "CR_SUCCESS",
                            "CR_LATER",
                            "CR_ROLLBACK",
                            "CR_COMMIT",
                            "CR_THROW_EXCEPTION",
                            "CR_RETURN_NULL",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_str(CMResultVisitor)
    }
}

impl From<i32> for CMResult {
    fn from(i: i32) -> Self {
        match i {
            0 => CMResult::CRSuccess,
            1 => CMResult::CRLater,
            2 => CMResult::CRRollback,
            3 => CMResult::CRCommit,
            4 => CMResult::CRThrowException,
            5 => CMResult::CRReturnNull,
            _ => CMResult::CRSuccess,
        }
    }
}

impl From<CMResult> for i32 {
    fn from(cm_result: CMResult) -> Self {
        match cm_result {
            CMResult::CRSuccess => 0,
            CMResult::CRLater => 1,
            CMResult::CRRollback => 2,
            CMResult::CRCommit => 3,
            CMResult::CRThrowException => 4,
            CMResult::CRReturnNull => 5,
        }
    }
}

impl Display for CMResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CMResult::CRSuccess => write!(f, "CR_SUCCESS"),
            CMResult::CRLater => write!(f, "CR_LATER"),
            CMResult::CRRollback => write!(f, "CR_ROLLBACK"),
            CMResult::CRCommit => write!(f, "CR_COMMIT"),
            CMResult::CRThrowException => write!(f, "CR_THROW_EXCEPTION"),
            CMResult::CRReturnNull => write!(f, "CR_RETURN_NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn serialize_cr_success() {
        let result = serde_json::to_string(&CMResult::CRSuccess).unwrap();
        assert_eq!(result, "\"CR_SUCCESS\"");
    }

    #[test]
    fn deserialize_cr_success() {
        let result: CMResult = serde_json::from_str("\"CR_SUCCESS\"").unwrap();
        assert_eq!(result, CMResult::CRSuccess);
    }

    #[test]
    fn from_i32_to_cmresult() {
        let result = CMResult::from(0);
        assert_eq!(result, CMResult::CRSuccess);
    }

    #[test]
    fn from_cmresult_to_i32() {
        let result: i32 = CMResult::CRSuccess.into();
        assert_eq!(result, 0);
    }

    #[test]
    fn display_cr_success() {
        let result = format!("{}", CMResult::CRSuccess);
        assert_eq!(result, "CR_SUCCESS");
    }

    #[test]
    fn deserialize_unknown_variant() {
        let result: Result<CMResult, _> = serde_json::from_str("\"UNKNOWN\"");
        assert!(result.is_err());
    }
}
