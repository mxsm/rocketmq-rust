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

#[derive(Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
pub struct UnregisterClientRequestHeader {
    #[required]
    #[serde(rename = "clientID")]
    pub client_id: CheetahString,
    pub producer_group: Option<CheetahString>,
    pub consumer_group: Option<CheetahString>,
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn un_register_client_request_header_serializes_correctly() {
        let header: UnregisterClientRequestHeader = UnregisterClientRequestHeader {
            client_id: CheetahString::from_static_str("test_client_id"),
            producer_group: Some(CheetahString::from_static_str("test_producer_group")),
            consumer_group: Some(CheetahString::from_static_str("test_consumer_group")),
            rpc_request_header: None,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("consumerGroup")).unwrap(),
            "test_consumer_group"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("clientId")).unwrap(),
            "test_client_id"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("producerGroup")).unwrap(),
            "test_producer_group"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("rpcRequestHeader")), None);
    }

    #[test]
    fn un_register_client_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("consumerGroup"),
            CheetahString::from_static_str("test_consumer_group"),
        );
        map.insert(
            CheetahString::from_static_str("clientId"),
            CheetahString::from_static_str("test_client_id"),
        );
        map.insert(
            CheetahString::from_static_str("producerGroup"),
            CheetahString::from_static_str("test_producer_group"),
        );
        let header: UnregisterClientRequestHeader = <UnregisterClientRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group.as_deref(), Some("test_consumer_group"));
        assert_eq!(header.client_id, "test_client_id");
        assert_eq!(header.producer_group.as_deref(), Some("test_producer_group"));
    }

    #[test]
    fn un_register_client_request_header_missing_optional_fields() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("clientId"),
            CheetahString::from_static_str("test_client_id"),
        );

        let header: UnregisterClientRequestHeader = <UnregisterClientRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.client_id, "test_client_id");
        assert!(header.rpc_request_header.is_some());
    }

    #[test]
    fn un_register_client_request_header_deserialize_from_json() {
        let data = r#"{"clientID":"test_client_id","consumerGroup":"test_consumer_group"}"#;
        let header: UnregisterClientRequestHeader = serde_json::from_str(data).unwrap();
        assert_eq!(header.client_id, CheetahString::from_static_str("test_client_id"));
        assert_eq!(header.consumer_group.as_deref(), None);
        assert!(header.rpc_request_header.is_some());
    }
}
