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

use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Serialize, Deserialize, Debug, Default, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::heartbeat_request_header::HeartbeatRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.HeartbeatRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequestHeader {
    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub rpc_request: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn serde_flattens_the_java_rpc_keys() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("namespace-a")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from("broker-a")),
                oneway: Some(false),
            }),
        };
        let value = serde_json::to_value(&header).unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "ns": "namespace-a",
                "nsd": true,
                "bname": "broker-a",
                "oway": false
            })
        );

        let decoded: HeartbeatRequestHeader = serde_json::from_value(value).unwrap();
        let rpc = decoded.rpc_request.unwrap();
        assert_eq!(rpc.namespace.as_deref(), Some("namespace-a"));
        assert_eq!(rpc.namespaced, Some(true));
        assert_eq!(rpc.broker_name.as_deref(), Some("broker-a"));
        assert_eq!(rpc.oneway, Some(false));
    }

    #[test]
    fn empty_inputs_still_create_the_always_present_rpc_envelope() {
        assert_eq!(
            serde_json::to_string(&HeartbeatRequestHeader { rpc_request: None }).unwrap(),
            "{}"
        );

        let serde_decoded: HeartbeatRequestHeader = serde_json::from_str("{}").unwrap();
        assert!(serde_decoded.rpc_request.is_some());

        let map_decoded = <HeartbeatRequestHeader as FromMap>::from(&HashMap::new()).unwrap();
        assert!(map_decoded.rpc_request.is_some());
    }

    #[test]
    fn v3_codec_decodes_partial_rpc_fields() {
        let map = HashMap::from([
            ("namespace".into(), "namespace-a".into()),
            ("brokerName".into(), "broker-a".into()),
        ]);
        let decoded = <HeartbeatRequestHeader as FromMap>::from(&map).unwrap();
        let rpc = decoded.rpc_request.unwrap();

        assert_eq!(rpc.namespace.as_deref(), Some("namespace-a"));
        assert_eq!(rpc.broker_name.as_deref(), Some("broker-a"));
        assert_eq!(rpc.namespaced, None);
        assert_eq!(rpc.oneway, None);
    }
}
