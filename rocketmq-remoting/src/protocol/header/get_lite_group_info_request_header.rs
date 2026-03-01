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

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteGroupInfoRequestHeader {
    #[required]
    pub group: CheetahString,

    #[required]
    pub lite_topic: CheetahString,

    #[required]
    pub top_k: i32,

    #[serde(flatten)]
    pub rpc: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_lite_group_info_request_header_creation() {
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("test_group"),
            lite_topic: CheetahString::from("test_lite_topic"),
            top_k: 10,
            rpc: None,
        };

        assert_eq!(header.group, CheetahString::from("test_group"));
        assert_eq!(header.lite_topic, CheetahString::from("test_lite_topic"));
        assert_eq!(header.top_k, 10);
        assert!(header.rpc.is_none());
    }

    #[test]
    fn get_lite_group_info_request_header_serializes_to_map() {
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("my_group"),
            lite_topic: CheetahString::from("my_lite_topic"),
            top_k: 5,
            rpc: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("group")).unwrap(),
            "my_group"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("liteTopic")).unwrap(),
            "my_lite_topic"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("topK")).unwrap(),
            "5"
        );
    }

    #[test]
    fn get_lite_group_info_request_header_deserializes_from_map() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("group"),
            CheetahString::from("deserialized_group"),
        );
        map.insert(
            CheetahString::from_static_str("liteTopic"),
            CheetahString::from("deserialized_lite"),
        );
        map.insert(
            CheetahString::from_static_str("topK"),
            CheetahString::from("20"),
        );

        let header = <GetLiteGroupInfoRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.group, CheetahString::from("deserialized_group"));
        assert_eq!(header.lite_topic, CheetahString::from("deserialized_lite"));
        assert_eq!(header.top_k, 20);
    }

    #[test]
    fn get_lite_group_info_request_header_clone() {
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("group"),
            lite_topic: CheetahString::from("lite"),
            top_k: 3,
            rpc: None,
        };

        let cloned = header.clone();
        assert_eq!(header.group, cloned.group);
        assert_eq!(header.lite_topic, cloned.lite_topic);
        assert_eq!(header.top_k, cloned.top_k);
    }

    #[test]
    fn get_lite_group_info_request_header_debug() {
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("group"),
            lite_topic: CheetahString::from("lite"),
            top_k: 7,
            rpc: None,
        };

        let debug_str = format!("{:?}", header);
        assert!(debug_str.contains("group"));
        assert!(debug_str.contains("lite"));
        assert!(debug_str.contains("7"));
    }

    #[test]
    fn get_lite_group_info_request_header_with_different_top_k_values() {
        let header_zero = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("g"),
            lite_topic: CheetahString::from("t"),
            top_k: 0,
            rpc: None,
        };
        assert_eq!(header_zero.top_k, 0);

        let header_negative = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("g"),
            lite_topic: CheetahString::from("t"),
            top_k: -1,
            rpc: None,
        };
        assert_eq!(header_negative.top_k, -1);

        let header_large = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("g"),
            lite_topic: CheetahString::from("t"),
            top_k: i32::MAX,
            rpc: None,
        };
        assert_eq!(header_large.top_k, i32::MAX);
    }
}
