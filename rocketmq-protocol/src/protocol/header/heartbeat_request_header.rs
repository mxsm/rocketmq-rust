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
pub struct HeartbeatRequestHeader {
    #[serde(flatten)]
    pub rpc_request: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn default_initialization_creates_empty_struct() {
        let header = HeartbeatRequestHeader::default();
        assert!(header.rpc_request.is_none());
    }

    #[test]
    fn serialization_with_none_rpc_request() {
        let header = HeartbeatRequestHeader { rpc_request: None };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, "{}");
    }

    #[test]
    fn serialization_with_some_rpc_request() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("test_namespace")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from("test_broker")),
                oneway: Some(false),
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert!(json.contains("\"namespace\":\"test_namespace\""));
        assert!(json.contains("\"namespaced\":true"));
        assert!(json.contains("\"brokerName\":\"test_broker\""));
        assert!(json.contains("\"oneway\":false"));

        // Should not contain a nested "rpc_request" or "rpcRequest" field
        assert!(!json.contains("rpcRequest"));
        assert!(!json.contains("rpc_request"));
    }

    #[test]
    fn deserialization_with_empty_json() {
        let json = "{}";
        let header: HeartbeatRequestHeader = serde_json::from_str(json).unwrap();
        // With flattened attribute, empty JSON creates Some with all None fields
        assert!(header.rpc_request.is_some());
        let rpc = header.rpc_request.unwrap();
        assert!(rpc.namespace.is_none());
        assert!(rpc.namespaced.is_none());
        assert!(rpc.broker_name.is_none());
        assert!(rpc.oneway.is_none());
    }

    #[test]
    fn deserialization_with_rpc_request_fields() {
        let json = r#"{
            "namespace": "test_namespace",
            "namespaced": true,
            "brokerName": "test_broker",
            "oneway": false
        }"#;
        let header: HeartbeatRequestHeader = serde_json::from_str(json).unwrap();
        assert!(header.rpc_request.is_some());
        let rpc = header.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "test_namespace");
        assert!(rpc.namespaced.unwrap());
        assert_eq!(rpc.broker_name.unwrap(), "test_broker");
        assert!(!rpc.oneway.unwrap());
    }

    #[test]
    fn deserialization_with_partial_rpc_request_fields() {
        let json = r#"{
            "namespace": "test_namespace",
            "brokerName": "test_broker"
        }"#;
        let header: HeartbeatRequestHeader = serde_json::from_str(json).unwrap();
        assert!(header.rpc_request.is_some());
        let rpc = header.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "test_namespace");
        assert_eq!(rpc.broker_name.unwrap(), "test_broker");
        assert!(rpc.namespaced.is_none());
        assert!(rpc.oneway.is_none());
    }

    #[test]
    fn serialization_deserialization_roundtrip_with_none() {
        let original = HeartbeatRequestHeader { rpc_request: None };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HeartbeatRequestHeader = serde_json::from_str(&json).unwrap();
        // With flattened attribute, empty JSON creates Some with all None fields
        assert!(deserialized.rpc_request.is_some());
        let rpc = deserialized.rpc_request.unwrap();
        assert!(rpc.namespace.is_none());
        assert!(rpc.namespaced.is_none());
        assert!(rpc.broker_name.is_none());
        assert!(rpc.oneway.is_none());
    }

    #[test]
    fn serialization_deserialization_roundtrip_with_some() {
        let original = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("test_namespace")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from("test_broker")),
                oneway: Some(false),
            }),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HeartbeatRequestHeader = serde_json::from_str(&json).unwrap();

        assert!(deserialized.rpc_request.is_some());
        let rpc = deserialized.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "test_namespace");
        assert!(rpc.namespaced.unwrap());
        assert_eq!(rpc.broker_name.unwrap(), "test_broker");
        assert!(!rpc.oneway.unwrap());
    }

    #[test]
    fn serialization_deserialization_roundtrip_with_partial_fields() {
        let original = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("test_namespace")),
                namespaced: None,
                broker_name: Some(CheetahString::from("test_broker")),
                oneway: None,
            }),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HeartbeatRequestHeader = serde_json::from_str(&json).unwrap();

        assert!(deserialized.rpc_request.is_some());
        let rpc = deserialized.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "test_namespace");
        assert_eq!(rpc.broker_name.unwrap(), "test_broker");
        assert!(rpc.namespaced.is_none());
        assert!(rpc.oneway.is_none());
    }

    #[test]
    fn flattened_serde_attribute_works_correctly() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("ns")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from("broker")),
                oneway: Some(true),
            }),
        };
        let json = serde_json::to_string(&header).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Verify fields are at top level (not nested)
        assert!(value.get("namespace").is_some());
        assert!(value.get("namespaced").is_some());
        assert!(value.get("brokerName").is_some());
        assert!(value.get("oneway").is_some());

        // Verify no nested structure
        assert!(value.get("rpcRequest").is_none());
        assert!(value.get("rpc_request").is_none());
    }

    #[test]
    fn from_map_with_empty_map() {
        let map = HashMap::new();
        let header = <HeartbeatRequestHeader as FromMap>::from(&map).unwrap();

        assert!(header.rpc_request.is_some());
        let rpc = header.rpc_request.unwrap();

        assert!(rpc.namespace.is_none());
        assert!(rpc.namespaced.is_none());
        assert!(rpc.broker_name.is_none());
        assert!(rpc.oneway.is_none());
    }

    #[test]
    fn from_map_with_rpc_request_fields() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("namespace"), CheetahString::from("test_namespace"));
        map.insert(CheetahString::from("namespaced"), CheetahString::from("true"));
        map.insert(CheetahString::from("brokerName"), CheetahString::from("test_broker"));
        map.insert(CheetahString::from("oneway"), CheetahString::from("false"));

        let header = <HeartbeatRequestHeader as FromMap>::from(&map).unwrap();

        assert!(header.rpc_request.is_some());
        let rpc = header.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "test_namespace");
        assert!(rpc.namespaced.unwrap());
        assert_eq!(rpc.broker_name.unwrap(), "test_broker");
        assert!(!rpc.oneway.unwrap());
    }

    #[test]
    fn from_map_with_partial_fields() {
        let mut map = HashMap::new();
        map.insert(CheetahString::from("namespace"), CheetahString::from("test_namespace"));

        let header = <HeartbeatRequestHeader as FromMap>::from(&map).unwrap();

        assert!(header.rpc_request.is_some());
        let rpc = header.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "test_namespace");
        assert!(rpc.namespaced.is_none());
        assert!(rpc.broker_name.is_none());
        assert!(rpc.oneway.is_none());
    }

    #[test]
    fn debug_trait_implementation() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("test_namespace")),
                namespaced: Some(true),
                broker_name: Some(CheetahString::from("test_broker")),
                oneway: Some(false),
            }),
        };
        let debug_string = format!("{:?}", header);

        assert!(debug_string.contains("HeartbeatRequestHeader"));
        assert!(debug_string.contains("rpc_request"));
    }

    #[test]
    fn debug_trait_with_none() {
        let header = HeartbeatRequestHeader { rpc_request: None };
        let debug_string = format!("{:?}", header);

        assert!(debug_string.contains("HeartbeatRequestHeader"));
        assert!(debug_string.contains("None"));
    }

    #[test]
    fn default_trait_implementation() {
        let header = HeartbeatRequestHeader::default();
        assert!(header.rpc_request.is_none());

        // Verify it's equivalent to manual construction with None
        let manual_header = HeartbeatRequestHeader { rpc_request: None };
        let default_json = serde_json::to_string(&header).unwrap();
        let manual_json = serde_json::to_string(&manual_header).unwrap();

        assert_eq!(default_json, manual_json);
    }

    #[test]
    fn complete_rpc_request_header_structure() {
        let rpc_header = RpcRequestHeader {
            namespace: Some(CheetahString::from("production")),
            namespaced: Some(true),
            broker_name: Some(CheetahString::from("broker-master")),
            oneway: Some(false),
        };

        let header = HeartbeatRequestHeader {
            rpc_request: Some(rpc_header),
        };

        let json = serde_json::to_string(&header).unwrap();
        let deserialized: HeartbeatRequestHeader = serde_json::from_str(&json).unwrap();

        let rpc = deserialized.rpc_request.unwrap();
        assert_eq!(rpc.namespace.unwrap(), "production");
        assert!(rpc.namespaced.unwrap());
        assert_eq!(rpc.broker_name.unwrap(), "broker-master");
        assert!(!rpc.oneway.unwrap());
    }

    #[test]
    fn rpc_request_header_namespace_field() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("test_ns")),
                namespaced: None,
                broker_name: None,
                oneway: None,
            }),
        };

        assert!(header.rpc_request.is_some());
        assert_eq!(
            header.rpc_request.as_ref().unwrap().namespace.as_ref().unwrap(),
            "test_ns"
        );
    }

    #[test]
    fn rpc_request_header_namespaced_field() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: None,
                namespaced: Some(true),
                broker_name: None,
                oneway: None,
            }),
        };

        assert!(header.rpc_request.is_some());
        assert!(header.rpc_request.as_ref().unwrap().namespaced.unwrap());
    }

    #[test]
    fn rpc_request_header_broker_name_field() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: Some(CheetahString::from("my_broker")),
                oneway: None,
            }),
        };

        assert!(header.rpc_request.is_some());
        assert_eq!(
            header.rpc_request.as_ref().unwrap().broker_name.as_ref().unwrap(),
            "my_broker"
        );
    }

    #[test]
    fn rpc_request_header_oneway_field() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: None,
                oneway: Some(true),
            }),
        };

        assert!(header.rpc_request.is_some());
        assert!(header.rpc_request.as_ref().unwrap().oneway.unwrap());
    }

    #[test]
    fn camel_case_field_names_in_json() {
        let header = HeartbeatRequestHeader {
            rpc_request: Some(RpcRequestHeader {
                namespace: Some(CheetahString::from("ns")),
                namespaced: Some(false),
                broker_name: Some(CheetahString::from("broker")),
                oneway: Some(true),
            }),
        };

        let json = serde_json::to_string(&header).unwrap();

        // Verify camelCase is used (not snake_case)
        assert!(json.contains("brokerName"));
        assert!(!json.contains("broker_name"));
        assert!(json.contains("namespace"));
        assert!(json.contains("namespaced"));
        assert!(json.contains("oneway"));
    }
}
