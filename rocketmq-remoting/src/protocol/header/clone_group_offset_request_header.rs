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

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct CloneGroupOffsetRequestHeader {
    #[required]
    pub src_group: CheetahString,

    #[required]
    pub dest_group: CheetahString,

    pub topic: Option<CheetahString>,

    pub offline: bool,

    #[serde(flatten)]
    pub rpc_request_header: Option<RpcRequestHeader>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_group_offset_request_header_round_trips() {
        let header = CloneGroupOffsetRequestHeader {
            src_group: CheetahString::from_static_str("src"),
            dest_group: CheetahString::from_static_str("dest"),
            topic: Some(CheetahString::from_static_str("topic-a")),
            offline: true,
            rpc_request_header: None,
        };

        let serialized = serde_json::to_string(&header).expect("serialize clone group offset header");
        assert_eq!(
            serialized,
            r#"{"srcGroup":"src","destGroup":"dest","topic":"topic-a","offline":true}"#
        );

        let decoded: CloneGroupOffsetRequestHeader =
            serde_json::from_str(&serialized).expect("deserialize clone group offset header");
        assert_eq!(decoded.src_group, "src");
        assert_eq!(decoded.dest_group, "dest");
        assert_eq!(decoded.topic, Some(CheetahString::from_static_str("topic-a")));
        assert!(decoded.offline);
    }
}
