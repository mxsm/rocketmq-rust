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
pub enum ResourceType {
    Unknown = 0,
    Any = 1,
    Cluster = 2,
    Namespace = 3,
    Topic = 4,
    Group = 5,
}

impl ResourceType {
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Unknown") {
            Some(Self::Unknown)
        } else if name.eq_ignore_ascii_case("Any") {
            Some(Self::Any)
        } else if name.eq_ignore_ascii_case("Cluster") {
            Some(Self::Cluster)
        } else if name.eq_ignore_ascii_case("Namespace") {
            Some(Self::Namespace)
        } else if name.eq_ignore_ascii_case("Topic") {
            Some(Self::Topic)
        } else if name.eq_ignore_ascii_case("Group") {
            Some(Self::Group)
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
            Self::Unknown => "Unknown",
            Self::Any => "Any",
            Self::Cluster => "Cluster",
            Self::Namespace => "Namespace",
            Self::Topic => "Topic",
            Self::Group => "Group",
        }
    }
}

impl Serialize for ResourceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for ResourceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            0 => Ok(ResourceType::Unknown),
            1 => Ok(ResourceType::Any),
            2 => Ok(ResourceType::Cluster),
            3 => Ok(ResourceType::Namespace),
            4 => Ok(ResourceType::Topic),
            5 => Ok(ResourceType::Group),
            _ => Err(serde::de::Error::custom("invalid ResourceType code")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_by_name() {
        assert_eq!(ResourceType::get_by_name("Unknown"), Some(ResourceType::Unknown));
        assert_eq!(ResourceType::get_by_name("Any"), Some(ResourceType::Any));
        assert_eq!(ResourceType::get_by_name("Cluster"), Some(ResourceType::Cluster));
        assert_eq!(ResourceType::get_by_name("Namespace"), Some(ResourceType::Namespace));
        assert_eq!(ResourceType::get_by_name("Topic"), Some(ResourceType::Topic));
        assert_eq!(ResourceType::get_by_name("TOPIC"), Some(ResourceType::Topic));
        assert_eq!(ResourceType::get_by_name("ToPiC"), Some(ResourceType::Topic));
        assert_eq!(ResourceType::get_by_name("GROUP"), Some(ResourceType::Group));
        assert_eq!(ResourceType::get_by_name("Group"), Some(ResourceType::Group));
        assert_eq!(ResourceType::get_by_name("Invalid"), None);
    }

    #[test]
    fn test_code() {
        assert_eq!(ResourceType::Unknown.code(), 0);
        assert_eq!(ResourceType::Any.code(), 1);
        assert_eq!(ResourceType::Cluster.code(), 2);
        assert_eq!(ResourceType::Namespace.code(), 3);
        assert_eq!(ResourceType::Topic.code(), 4);
        assert_eq!(ResourceType::Group.code(), 5);
    }

    #[test]
    fn test_name() {
        assert_eq!(ResourceType::Unknown.name(), "Unknown");
        assert_eq!(ResourceType::Any.name(), "Any");
        assert_eq!(ResourceType::Cluster.name(), "Cluster");
        assert_eq!(ResourceType::Namespace.name(), "Namespace");
        assert_eq!(ResourceType::Topic.name(), "Topic");
        assert_eq!(ResourceType::Group.name(), "Group");
    }

    #[test]
    fn test_serialize_to_json() {
        let unknown = ResourceType::Unknown;
        let json = serde_json::to_string(&unknown).unwrap();
        assert_eq!(json, "0");

        let any = ResourceType::Any;
        let json = serde_json::to_string(&any).unwrap();
        assert_eq!(json, "1");

        let topic = ResourceType::Topic;
        let json = serde_json::to_string(&topic).unwrap();
        assert_eq!(json, "4");

        let group = ResourceType::Group;
        let json = serde_json::to_string(&group).unwrap();
        assert_eq!(json, "5");
    }

    #[test]
    fn test_deserialize_from_json() {
        let json = "0";
        let resource: ResourceType = serde_json::from_str(json).unwrap();
        assert_eq!(resource, ResourceType::Unknown);

        let json = "3";
        let resource: ResourceType = serde_json::from_str(json).unwrap();
        assert_eq!(resource, ResourceType::Namespace);

        let json = "5";
        let resource: ResourceType = serde_json::from_str(json).unwrap();
        assert_eq!(resource, ResourceType::Group);
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let variants = vec![
            ResourceType::Unknown,
            ResourceType::Any,
            ResourceType::Cluster,
            ResourceType::Namespace,
            ResourceType::Topic,
            ResourceType::Group,
        ];

        for original in variants {
            // Serialize to JSON
            let json = serde_json::to_string(&original).unwrap();

            // Deserialize back from JSON
            let deserialized: ResourceType = serde_json::from_str(&json).unwrap();

            // Should match the original
            assert_eq!(original, deserialized);
        }
    }

    #[test]
    fn test_deserialize_invalid_code() {
        // Test invalid codes
        for invalid_code in ["6", "99", "255"] {
            let result: Result<ResourceType, _> = serde_json::from_str(invalid_code);
            assert!(result.is_err());
            let err = result.unwrap_err();
            let err_msg = err.to_string();
            assert!(err_msg.contains("invalid ResourceType code"));
        }
    }

    #[test]
    fn test_serialize_all_variants() {
        let test_cases = vec![
            (ResourceType::Unknown, "0"),
            (ResourceType::Any, "1"),
            (ResourceType::Cluster, "2"),
            (ResourceType::Namespace, "3"),
            (ResourceType::Topic, "4"),
            (ResourceType::Group, "5"),
        ];

        for (resource_type, expected_json) in test_cases {
            let json = serde_json::to_string(&resource_type).unwrap();
            assert_eq!(json, expected_json);
        }
    }
}
