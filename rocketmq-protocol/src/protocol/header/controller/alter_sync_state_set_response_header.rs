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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::controller::alter_sync_state_set_response_header::AlterSyncStateSetResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader"
)]
pub struct AlterSyncStateSetResponseHeader {
    #[header(default, default_semantic = "literal:0")]
    pub new_sync_state_set_epoch: i32,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn alter_sync_state_set_response_header_serializes_correctly() {
        let header = AlterSyncStateSetResponseHeader {
            new_sync_state_set_epoch: 10,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("newSyncStateSetEpoch"))
                .unwrap(),
            "10"
        );
    }

    #[test]
    fn alter_sync_state_set_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("newSyncStateSetEpoch"),
            CheetahString::from_static_str("10"),
        );

        let header = <AlterSyncStateSetResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.new_sync_state_set_epoch, 10);
    }
}
