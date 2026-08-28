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
use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

use crate::rpc::rpc_request_header::RpcRequestHeader;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV3)]
#[header(
    type_id = "rocketmq_protocol::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetLiteGroupInfoRequestHeader"
)]
#[serde(rename_all = "camelCase")]
pub struct GetLiteGroupInfoRequestHeader {
    #[header(required)]
    pub group: CheetahString,

    #[serde(default)]
    #[header(default, default_semantic = "literal:")]
    pub lite_topic: CheetahString,

    #[serde(default)]
    #[header(default, default_semantic = "literal:0")]
    pub top_k: i32,

    #[serde(flatten)]
    #[header(flatten, presence = "always")]
    pub rpc: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_lite_group_info_request_header_serializes_to_map() {
        let header = GetLiteGroupInfoRequestHeader {
            group: CheetahString::from("my_group"),
            lite_topic: CheetahString::from("my_lite_topic"),
            top_k: 5,
            rpc: None,
        };

        let map = header.to_map().unwrap();
        assert_eq!(map.get(&CheetahString::from_static_str("group")).unwrap(), "my_group");
        assert_eq!(
            map.get(&CheetahString::from_static_str("liteTopic")).unwrap(),
            "my_lite_topic"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("topK")).unwrap(), "5");
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
        map.insert(CheetahString::from_static_str("topK"), CheetahString::from("20"));

        let header = <GetLiteGroupInfoRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.group, CheetahString::from("deserialized_group"));
        assert_eq!(header.lite_topic, CheetahString::from("deserialized_lite"));
        assert_eq!(header.top_k, 20);
    }
}
