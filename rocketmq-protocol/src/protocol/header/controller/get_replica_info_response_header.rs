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

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct GetReplicaInfoResponseHeader {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: Option<i32>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_replica_info_response_header_serializes_correctly() {
        let header = GetReplicaInfoResponseHeader {
            master_broker_id: Some(123),
            master_address: Some("127.0.0.1:10911".to_string()),
            master_epoch: Some(5),
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("masterBrokerId")).unwrap(),
            "123"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("masterAddress")).unwrap(),
            "127.0.0.1:10911"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("masterEpoch")).unwrap(), "5");
    }

    #[test]
    fn get_replica_info_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("masterBrokerId"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("masterAddress"),
            CheetahString::from_static_str("127.0.0.1:10911"),
        );
        map.insert(
            CheetahString::from_static_str("masterEpoch"),
            CheetahString::from_static_str("5"),
        );

        let header = <GetReplicaInfoResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.master_broker_id, Some(123));
        assert_eq!(header.master_address, Some("127.0.0.1:10911".to_string()));
        assert_eq!(header.master_epoch, Some(5));
    }

    #[test]
    fn get_replica_info_response_header_default() {
        let header = GetReplicaInfoResponseHeader::default();
        assert_eq!(header.master_broker_id, None);
        assert_eq!(header.master_address, None);
        assert_eq!(header.master_epoch, None);
    }

    #[test]
    fn get_replica_info_response_header_clone() {
        let header = GetReplicaInfoResponseHeader {
            master_broker_id: Some(123),
            master_address: Some("127.0.0.1:10911".to_string()),
            master_epoch: Some(5),
        };
        let cloned = header.clone();
        assert_eq!(header.master_broker_id, cloned.master_broker_id);
        assert_eq!(header.master_address, cloned.master_address);
        assert_eq!(header.master_epoch, cloned.master_epoch);
    }
}
