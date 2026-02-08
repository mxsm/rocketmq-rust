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

#[derive(Serialize, Deserialize, Debug, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct NotifyConsumerIdsChangedRequestHeader {
    #[required]
    pub consumer_group: CheetahString,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_consumer_ids_changed_request_header_serialization() {
        let header = NotifyConsumerIdsChangedRequestHeader {
            consumer_group: CheetahString::from("group1"),
            rpc_request_header: None,
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"consumerGroup\":\"group1\""));
    }

    #[test]
    fn notify_consumer_ids_changed_request_header_deserialization() {
        let json = r#"{"consumerGroup":"group1"}"#;
        let header: NotifyConsumerIdsChangedRequestHeader = serde_json::from_str(json).unwrap();
        assert_eq!(header.consumer_group, "group1");
    }
}
