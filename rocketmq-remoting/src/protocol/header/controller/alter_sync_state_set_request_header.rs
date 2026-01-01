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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct AlterSyncStateSetRequestHeader {
    pub broker_name: CheetahString,
    pub master_broker_id: i64,
    pub master_epoch: i32,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn alter_sync_state_set_request_header_serializes_correctly() {
        let header = AlterSyncStateSetRequestHeader {
            broker_name: CheetahString::from_static_str("test_broker"),
            master_broker_id: 1234567890,
            master_epoch: 5,
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerName")).unwrap(),
            "test_broker"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("masterBrokerId")).unwrap(),
            "1234567890"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("masterEpoch")).unwrap(), "5");
    }

    #[test]
    fn alter_sync_state_set_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("test_broker"),
        );
        map.insert(
            CheetahString::from_static_str("masterBrokerId"),
            CheetahString::from_static_str("1234567890"),
        );
        map.insert(
            CheetahString::from_static_str("masterEpoch"),
            CheetahString::from_static_str("5"),
        );

        let header = <AlterSyncStateSetRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.broker_name, "test_broker");
        assert_eq!(header.master_broker_id, 1234567890);
        assert_eq!(header.master_epoch, 5);
    }

    #[test]
    fn alter_sync_state_set_request_header_default() {
        let header = AlterSyncStateSetRequestHeader::default();
        assert_eq!(header.broker_name, "");
        assert_eq!(header.master_broker_id, 0);
        assert_eq!(header.master_epoch, 0);
    }

    #[test]
    fn alter_sync_state_set_request_header_clone() {
        let header = AlterSyncStateSetRequestHeader {
            broker_name: CheetahString::from_static_str("test_broker"),
            master_broker_id: 1234567890,
            master_epoch: 5,
        };
        let cloned = header.clone();
        assert_eq!(header.broker_name, cloned.broker_name);
        assert_eq!(header.master_broker_id, cloned.master_broker_id);
        assert_eq!(header.master_epoch, cloned.master_epoch);
    }
}
