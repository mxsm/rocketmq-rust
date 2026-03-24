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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct UpdateUserRequestHeader {
    pub username: CheetahString,
}
impl UpdateUserRequestHeader {
    pub fn set_username(&mut self, username: CheetahString) {
        self.username = username;
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use serde_json;

    use super::*;

    #[test]
    fn test_default_username_is_empty() {
        let header = UpdateUserRequestHeader::default();
        assert_eq!(header.username, CheetahString::from(""));
    }

    #[test]
    fn test_new_with_explicit_username() {
        let header = UpdateUserRequestHeader {
            username: CheetahString::from("alice"),
        };
        assert_eq!(header.username, CheetahString::from("alice"));
    }

    #[test]
    fn test_set_username_updates_value() {
        let mut header = UpdateUserRequestHeader::default();
        header.set_username(CheetahString::from("bob"));
        assert_eq!(header.username, CheetahString::from("bob"));
    }

    #[test]
    fn test_set_username_overwrites_existing() {
        let mut header = UpdateUserRequestHeader {
            username: CheetahString::from("old_user"),
        };
        header.set_username(CheetahString::from("new_user"));
        assert_eq!(header.username, CheetahString::from("new_user"));
    }

    #[test]
    fn test_set_username_with_empty_string() {
        let mut header = UpdateUserRequestHeader {
            username: CheetahString::from("someone"),
        };
        header.set_username(CheetahString::from(""));
        assert_eq!(header.username, CheetahString::from(""));
    }

    #[test]
    fn test_set_username_with_unicode() {
        let mut header = UpdateUserRequestHeader::default();
        header.set_username(CheetahString::from("用户名"));
        assert_eq!(header.username, CheetahString::from("用户名"));
    }

    #[test]
    fn test_set_username_with_special_characters() {
        let mut header = UpdateUserRequestHeader::default();
        header.set_username(CheetahString::from("user@domain.com!#$"));
        assert_eq!(header.username, CheetahString::from("user@domain.com!#$"));
    }

    #[test]
    fn test_clone_produces_equal_value() {
        let original = UpdateUserRequestHeader {
            username: CheetahString::from("charlie"),
        };
        let cloned = original.clone();
        assert_eq!(original.username, cloned.username);
    }

    #[test]
    fn test_clone_is_independent() {
        let original = UpdateUserRequestHeader {
            username: CheetahString::from("delta"),
        };
        let mut cloned = original.clone();
        cloned.set_username(CheetahString::from("epsilon"));
        // Original must be unchanged
        assert_eq!(original.username, CheetahString::from("delta"));
        assert_eq!(cloned.username, CheetahString::from("epsilon"));
    }

    #[test]
    fn test_serialise_uses_camel_case_key() {
        let header = UpdateUserRequestHeader {
            username: CheetahString::from("frank"),
        };
        let json = serde_json::to_string(&header).expect("serialisation failed");
        // The serde rename_all = "camelCase" keeps "username" as-is (already camel)
        assert!(json.contains("\"username\""));
        assert!(json.contains("\"frank\""));
    }

    #[test]
    fn test_serialise_default_produces_empty_username() {
        let header = UpdateUserRequestHeader::default();
        let json = serde_json::to_string(&header).expect("serialisation failed");
        assert!(json.contains("\"username\":\"\""));
    }

    #[test]
    fn test_deserialise_from_valid_json() {
        let json = r#"{"username":"grace"}"#;
        let header: UpdateUserRequestHeader = serde_json::from_str(json).expect("deserialisation failed");
        assert_eq!(header.username, CheetahString::from("grace"));
    }
    #[test]
    fn test_roundtrip_serialisation() {
        let original = UpdateUserRequestHeader {
            username: CheetahString::from("henry"),
        };
        let json = serde_json::to_string(&original).expect("serialisation failed");
        let restored: UpdateUserRequestHeader = serde_json::from_str(&json).expect("deserialisation failed");
        assert_eq!(original.username, restored.username);
    }

    #[test]
    fn test_roundtrip_with_whitespace_username() {
        let original = UpdateUserRequestHeader {
            username: CheetahString::from("  spaces  "),
        };
        let json = serde_json::to_string(&original).unwrap();
        let restored: UpdateUserRequestHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(original.username, restored.username);
    }
    #[test]
    fn test_debug_output_contains_username() {
        let header = UpdateUserRequestHeader {
            username: CheetahString::from("ivy"),
        };
        let debug_str = format!("{:?}", header);
        assert!(debug_str.contains("ivy"));
    }
}
