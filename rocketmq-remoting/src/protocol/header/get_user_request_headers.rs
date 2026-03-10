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
pub struct GetUserRequestHeader {
    pub username: CheetahString,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_user_request_header_default() {
        let header = GetUserRequestHeader::default();
        assert_eq!(header.username, CheetahString::default());
    }

    #[test]
    fn get_user_request_header_serialize() {
        let header = GetUserRequestHeader {
            username: CheetahString::from("value"),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"username":"value"}"#);
    }

    #[test]
    fn get_user_request_header_deserialize() {
        let json = r#"{"username":"value"}"#;
        let header: GetUserRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.username, CheetahString::from("value"));
    }

    #[test]
    fn get_user_request_header_clone() {
        let header = GetUserRequestHeader {
            username: CheetahString::from("value"),
        };
        let cloned_header = header.clone();
        assert_eq!(header.username, cloned_header.username);
    }
}
