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
pub struct UnlockBatchMqRequestHeader {
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_unlock_batch_mq_request_header_serialization() {
        let rpc_header = RpcRequestHeader {
            namespace: Some("test_ns".into()),
            broker_name: Some("broker_a".into()),
            oneway: Some(true),
            ..Default::default()
        };

        let header = UnlockBatchMqRequestHeader {
            rpc_request_header: Some(rpc_header),
        };

        let json = serde_json::to_string(&header).unwrap();

        assert!(json.contains("\"namespace\":\"test_ns\""));
        assert!(json.contains("\"brokerName\":\"broker_a\""));
        assert!(json.contains("\"oneway\":true"));
    }

    #[test]
    fn test_unlock_batch_mq_request_header_deserialization() {
        let json = r#"{
            "namespace": "standard_ns",
            "namespaced": true,
            "brokerName": "rocketmq_broker",
            "oneway": false
        }"#;

        let decoded: UnlockBatchMqRequestHeader = serde_json::from_str(json).unwrap();
        let rpc = decoded.rpc_request_header.expect("RpcRequestHeader should be present");

        assert_eq!(rpc.namespace.unwrap().as_str(), "standard_ns");
        assert_eq!(rpc.namespaced, Some(true));
        assert_eq!(rpc.broker_name.unwrap().as_str(), "rocketmq_broker");
        assert_eq!(rpc.oneway, Some(false));
    }

    #[test]
    fn test_default_values() {
        let header = UnlockBatchMqRequestHeader::default();
        assert!(header.rpc_request_header.is_none());
    }

    #[test]
    fn test_rpc_request_header_new() {
        let ns = Some("ns".into());
        let namespaced = Some(false);
        let broker = Some("b1".into());
        let oneway = Some(true);

        let header = RpcRequestHeader::new(ns.clone(), namespaced, broker.clone(), oneway);

        assert_eq!(header.namespace, ns);
        assert_eq!(header.namespaced, namespaced);
        assert_eq!(header.broker_name, broker);
        assert_eq!(header.oneway, oneway);
    }

    #[test]
    fn test_partial_fields_deserialization() {
        let json = r#"{"brokerName": "only_broker"}"#;
        let decoded: UnlockBatchMqRequestHeader = serde_json::from_str(json).unwrap();

        let rpc = decoded.rpc_request_header.unwrap();

        assert_eq!(rpc.broker_name.unwrap().as_str(), "only_broker");
        assert!(rpc.namespace.is_none());
        assert!(rpc.oneway.is_none());
    }
}
