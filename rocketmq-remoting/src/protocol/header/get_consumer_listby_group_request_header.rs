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
pub struct GetConsumerListByGroupRequestHeader {
    #[required]
    pub consumer_group: CheetahString,

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
    fn get_consumer_list_by_group_request_header_to_map() {
        let header = GetConsumerListByGroupRequestHeader {
            consumer_group: "test_group".into(),
            rpc: None,
        };

        let map: HashMap<CheetahString, CheetahString> = header.to_map().unwrap();
        assert_eq!(
            map.get(GetConsumerListByGroupRequestHeader::CONSUMER_GROUP),
            Some(&"test_group".into())
        );
    }

    #[test]
    fn get_consumer_list_by_group_request_header_from_map() {
        let mut map = HashMap::new();
        map.insert(
            GetConsumerListByGroupRequestHeader::CONSUMER_GROUP.into(),
            "test_group".into(),
        );

        let header = <GetConsumerListByGroupRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.consumer_group, "test_group");
    }
}
