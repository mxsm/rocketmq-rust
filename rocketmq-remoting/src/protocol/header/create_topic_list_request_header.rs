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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct CreateTopicListRequestHeader {
    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn create_topic_list_request_header_maps_rpc_fields_like_java_header() {
        let header = CreateTopicListRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from_static_str("broker-a")),
                namespace: Some(CheetahString::from_static_str("ns-a")),
                namespaced: Some(true),
                oneway: Some(false),
            }),
        };

        let map = header.to_map().expect("header should encode");
        assert_eq!(map.get("brokerName").map(|value| value.as_str()), Some("broker-a"));
        assert_eq!(map.get("namespace").map(|value| value.as_str()), Some("ns-a"));
        assert_eq!(map.get("namespaced").map(|value| value.as_str()), Some("true"));
        assert_eq!(map.get("oneway").map(|value| value.as_str()), Some("false"));

        let decoded = <CreateTopicListRequestHeader as FromMap>::from(&map).expect("header should decode");
        let rpc = decoded
            .rpc_request_header
            .expect("flattened rpc request header should decode");
        assert_eq!(rpc.broker_name.as_deref(), Some("broker-a"));
        assert_eq!(rpc.namespace.as_deref(), Some("ns-a"));
        assert_eq!(rpc.namespaced, Some(true));
        assert_eq!(rpc.oneway, Some(false));
    }
}
