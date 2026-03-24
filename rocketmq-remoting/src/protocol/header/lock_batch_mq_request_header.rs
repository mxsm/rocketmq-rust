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

use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Serialize, Deserialize, Debug, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct LockBatchMqRequestHeader {
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;

    #[test]
    fn lock_batch_mq_request_header_default() {
        let header: LockBatchMqRequestHeader = Default::default();
        assert!(header.rpc_request_header.is_none());
    }

    #[test]
    fn lock_batch_mq_request_header_serialize() {
        let header: LockBatchMqRequestHeader = LockBatchMqRequestHeader {
            rpc_request_header: Some(RpcRequestHeader::new(
                Some(CheetahString::from("some_value")),
                Some(true),
                Some(CheetahString::from("brokerName")),
                Some(true),
            )),
        };

        let json_str = serde_json::to_string(&header).unwrap();

        // Verify the serialized JSON contains expected fields with camelCase naming
        assert!(json_str.contains("\"namespace\":\"some_value\""));
        assert!(json_str.contains("\"namespaced\":true"));
        assert!(json_str.contains("\"brokerName\":\"brokerName\""));
        assert!(json_str.contains("\"oneway\":true"));
    }

    #[test]
    fn lock_batch_mq_request_header_deserialize() {
        let json_str = r#"{"namespace":"some_value","namespaced":true,"brokerName":"brokerName","oneway":true}"#;
        let header: LockBatchMqRequestHeader = serde_json::from_str(json_str).unwrap();
        assert!(header.rpc_request_header.is_some());
        assert!(header.rpc_request_header.as_ref().unwrap().namespace.is_some());
        assert_eq!(
            header.rpc_request_header.as_ref().unwrap().namespace.as_ref().unwrap(),
            "some_value"
        );
        assert!(header.rpc_request_header.as_ref().unwrap().namespaced.is_some());
        assert!(header.rpc_request_header.as_ref().unwrap().namespaced.unwrap());
        assert!(header.rpc_request_header.as_ref().unwrap().broker_name.is_some());
        assert_eq!(
            header
                .rpc_request_header
                .as_ref()
                .unwrap()
                .broker_name
                .as_ref()
                .unwrap(),
            "brokerName"
        );
        assert!(header.rpc_request_header.as_ref().unwrap().oneway.is_some());
        assert!(header.rpc_request_header.as_ref().unwrap().oneway.unwrap());
    }
}
