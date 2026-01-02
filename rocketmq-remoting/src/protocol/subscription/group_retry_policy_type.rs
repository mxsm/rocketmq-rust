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

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum GroupRetryPolicyType {
    #[default]
    Exponential,
    Customized,
}

impl Serialize for GroupRetryPolicyType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            GroupRetryPolicyType::Exponential => "EXPONENTIAL",
            GroupRetryPolicyType::Customized => "CUSTOMIZED",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for GroupRetryPolicyType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct GroupRetryPolicyTypeVisitor;

        impl serde::de::Visitor<'_> for GroupRetryPolicyTypeVisitor {
            type Value = GroupRetryPolicyType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing GroupRetryPolicyTypeVisitor")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "EXPONENTIAL" => Ok(GroupRetryPolicyType::Exponential),
                    "CUSTOMIZED" => Ok(GroupRetryPolicyType::Customized),
                    _ => Err(serde::de::Error::unknown_variant(value, &["Exponential", "Customized"])),
                }
            }
        }

        deserializer.deserialize_str(GroupRetryPolicyTypeVisitor)
    }
}

#[cfg(test)]
mod group_retry_policy_type_tests {
    use serde_json;

    use super::*;

    #[test]
    fn serialization_of_exponential_policy() {
        let policy = GroupRetryPolicyType::Exponential;
        let serialized = serde_json::to_string(&policy).unwrap();
        assert_eq!(serialized, "\"EXPONENTIAL\"");
    }

    #[test]
    fn serialization_of_customized_policy() {
        let policy = GroupRetryPolicyType::Customized;
        let serialized = serde_json::to_string(&policy).unwrap();
        assert_eq!(serialized, "\"CUSTOMIZED\"");
    }

    #[test]
    fn deserialization_of_exponential_policy() {
        let serialized = "\"EXPONENTIAL\"";
        let policy: GroupRetryPolicyType = serde_json::from_str(serialized).unwrap();
        assert_eq!(policy, GroupRetryPolicyType::Exponential);
    }

    #[test]
    fn deserialization_of_customized_policy() {
        let serialized = "\"CUSTOMIZED\"";
        let policy: GroupRetryPolicyType = serde_json::from_str(serialized).unwrap();
        assert_eq!(policy, GroupRetryPolicyType::Customized);
    }

    #[test]
    fn deserialization_fails_for_invalid_value() {
        let serialized = "\"UNKNOWN\"";
        let result: Result<GroupRetryPolicyType, _> = serde_json::from_str(serialized);
        assert!(result.is_err());
    }
}
