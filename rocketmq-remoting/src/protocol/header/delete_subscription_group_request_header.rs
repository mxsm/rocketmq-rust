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

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct DeleteSubscriptionGroupRequestHeader {
    #[required]
    pub group_name: CheetahString,

    pub clean_offset: bool,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn delete_subscription_group_request_header_serializes_correctly() {
        let header = DeleteSubscriptionGroupRequestHeader {
            group_name: CheetahString::from_static_str("test_group"),
            clean_offset: true,
            rpc_request_header: None,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        let expected = r#"{"groupName":"test_group","cleanOffset":true}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn delete_subscription_group_request_header_deserializes_correctly() {
        let data = r#"{"groupName":"test_group","cleanOffset":true}"#;
        let header: DeleteSubscriptionGroupRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.group_name, CheetahString::from_static_str("test_group"));
        assert!(header.clean_offset);
        assert!(header.rpc_request_header.is_some());
    }

    #[test]
    fn delete_subscription_group_request_header_handles_missing_optional_fields() {
        let data = r#"{"groupName":"test_group","cleanOffset":false}"#;
        let header: DeleteSubscriptionGroupRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.group_name, CheetahString::from_static_str("test_group"));
        assert!(!header.clean_offset);
        assert!(header.rpc_request_header.is_some());
    }

    #[test]
    fn delete_subscription_group_request_header_handles_invalid_data() {
        let data = r#"{"groupName":12345}"#;
        let result: Result<DeleteSubscriptionGroupRequestHeader, _> = serde_json::from_str(data);
        assert!(result.is_err());
    }
}
