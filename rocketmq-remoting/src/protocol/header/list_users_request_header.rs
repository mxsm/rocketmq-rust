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
pub struct ListUsersRequestHeader {
    pub filter: CheetahString,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_users_request_header_default() {
        let body = ListUsersRequestHeader::default();
        assert_eq!(body.filter, "");
    }

    #[test]
    fn list_users_request_header_clone() {
        let body = ListUsersRequestHeader { filter: "test".into() };
        let cloned = body.clone();
        assert_eq!(body.filter, cloned.filter);
    }

    #[test]
    fn list_users_request_header_serialize_deserialize() {
        let body = ListUsersRequestHeader { filter: "test".into() };
        let serialized = serde_json::to_string(&body).unwrap();
        let deserialized: ListUsersRequestHeader = serde_json::from_str(&serialized).unwrap();
        assert_eq!(body.filter, deserialized.filter);
    }
}
