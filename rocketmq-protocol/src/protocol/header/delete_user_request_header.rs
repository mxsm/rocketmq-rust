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
pub struct DeleteUserRequestHeader {
    pub username: CheetahString,
}

impl DeleteUserRequestHeader {
    pub fn new(username: CheetahString) -> Self {
        Self { username }
    }

    pub fn set_username(&mut self, username: CheetahString) {
        self.username = username;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn delete_user_request_header_new() {
        let username = CheetahString::from_static_str("test_user");
        let header = DeleteUserRequestHeader::new(username.clone());

        assert_eq!(header.username, username);
    }

    #[test]
    fn delete_user_request_header_default() {
        let header = DeleteUserRequestHeader::default();

        assert!(header.username.is_empty());
    }

    #[test]
    fn delete_user_request_header_set_username() {
        let mut header = DeleteUserRequestHeader::default();
        let username = CheetahString::from_static_str("admin");

        header.set_username(username.clone());

        assert_eq!(header.username, username);
    }

    #[test]
    fn delete_user_request_header_serializes_correctly() {
        let header = DeleteUserRequestHeader {
            username: CheetahString::from_static_str("test_admin"),
        };

        let map = header.to_map().unwrap();

        assert_eq!(
            map.get(&CheetahString::from_static_str("username")).unwrap(),
            "test_admin"
        );
    }

    #[test]
    fn delete_user_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("username"),
            CheetahString::from_static_str("deserialized_user"),
        );

        let header = <DeleteUserRequestHeader as FromMap>::from(&map).unwrap();

        assert_eq!(header.username.as_str(), "deserialized_user");
    }
}
