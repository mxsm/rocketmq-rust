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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::update_user_request_header::UpdateUserRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader"
)]
pub struct UpdateUserRequestHeader {
    #[header(default, default_semantic = "literal:")]
    pub username: CheetahString,
}
impl UpdateUserRequestHeader {
    pub fn set_username(&mut self, username: CheetahString) {
        self.username = username;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

    #[test]
    fn set_username_replaces_the_current_value() {
        let mut header = UpdateUserRequestHeader {
            username: CheetahString::from("before"),
        };

        header.set_username(CheetahString::from("after"));

        assert_eq!(header.username, "after");
    }

    #[test]
    fn serde_and_v3_codec_preserve_the_username() {
        let header = UpdateUserRequestHeader {
            username: CheetahString::from("user-a"),
        };
        let json = serde_json::to_string(&header).unwrap();
        let map = header.to_map().unwrap();

        assert_eq!(json, r#"{"username":"user-a"}"#);
        assert_eq!(
            serde_json::from_str::<UpdateUserRequestHeader>(&json).unwrap().username,
            "user-a"
        );
        assert_eq!(
            <UpdateUserRequestHeader as FromMap>::from(&map).unwrap().username,
            "user-a"
        );
        assert!(<UpdateUserRequestHeader as FromMap>::from(&HashMap::new())
            .unwrap()
            .username
            .is_empty());
    }
}
