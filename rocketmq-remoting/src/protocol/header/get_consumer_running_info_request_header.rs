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
pub struct GetConsumerRunningInfoRequestHeader {
    #[required]
    pub consumer_group: CheetahString,

    #[required]
    pub client_id: CheetahString,

    pub jstack_enable: bool,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn get_consumer_running_info_request_header_serializes_correctly() {
        let header = GetConsumerRunningInfoRequestHeader {
            consumer_group: CheetahString::from_static_str("test_group"),
            client_id: CheetahString::from_static_str("client_id"),
            jstack_enable: true,
            rpc_request_header: None,
        };
        let serialized = serde_json::to_string(&header).unwrap();
        let expected = r#"{"consumerGroup":"test_group","clientId":"client_id","jstackEnable":true}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn get_consumer_running_info_request_header_deserializes_correctly() {
        let data = r#"{"consumerGroup":"test_group","clientId":"client_id","jstackEnable":true}"#;
        let header: GetConsumerRunningInfoRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from_static_str("test_group"));
        assert_eq!(header.client_id, CheetahString::from_static_str("client_id"));
        assert!(header.jstack_enable);
        assert!(header.rpc_request_header.is_some());
    }

    #[test]
    fn get_consumer_running_info_request_header_handles_missing_optional_fields() {
        let data = r#"{"consumerGroup":"test_group","clientId":"client_id","jstackEnable":false}"#;
        let header: GetConsumerRunningInfoRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.consumer_group, CheetahString::from_static_str("test_group"));
        assert_eq!(header.client_id, CheetahString::from_static_str("client_id"));
        assert!(!header.jstack_enable);
        assert!(header.rpc_request_header.is_some());
    }

    #[test]
    fn get_consumer_running_info_request_header_handles_invalid_data() {
        let data = r#"{"consumerGroup":12345,"clientId":"client_id"}"#;
        let result: Result<GetConsumerRunningInfoRequestHeader, _> = serde_json::from_str(data);
        assert!(result.is_err());
    }
}
