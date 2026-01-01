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
pub struct RegisterBrokerToControllerResponseHeader {
    pub cluster_name: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub master_broker_id: Option<i64>,
    pub master_address: Option<CheetahString>,
    pub master_epoch: Option<i32>,
    pub sync_state_set_epoch: Option<i32>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn register_broker_to_controller_response_header_serializes_correctly() {
        let header = RegisterBrokerToControllerResponseHeader {
            cluster_name: Some(CheetahString::from_static_str("test_cluster")),
            broker_name: Some(CheetahString::from_static_str("test_broker")),
            master_broker_id: Some(123),
            master_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            master_epoch: Some(5),
            sync_state_set_epoch: Some(10),
        };
        let map = header.to_map().unwrap();
        assert_eq!(
            map.get(&CheetahString::from_static_str("clusterName")).unwrap(),
            "test_cluster"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerName")).unwrap(),
            "test_broker"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("masterBrokerId")).unwrap(),
            "123"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("masterAddress")).unwrap(),
            "127.0.0.1:10911"
        );
        assert_eq!(map.get(&CheetahString::from_static_str("masterEpoch")).unwrap(), "5");
        assert_eq!(
            map.get(&CheetahString::from_static_str("syncStateSetEpoch")).unwrap(),
            "10"
        );
    }

    #[test]
    fn register_broker_to_controller_response_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("clusterName"),
            CheetahString::from_static_str("test_cluster"),
        );
        map.insert(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("test_broker"),
        );
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
        map.insert(
            CheetahString::from_static_str("syncStateSetEpoch"),
            CheetahString::from_static_str("10"),
        );

        let header = <RegisterBrokerToControllerResponseHeader as FromMap>::from(&map).unwrap();
        assert_eq!(
            header.cluster_name,
            Some(CheetahString::from_static_str("test_cluster"))
        );
        assert_eq!(header.broker_name, Some(CheetahString::from_static_str("test_broker")));
        assert_eq!(header.master_broker_id, Some(123));
        assert_eq!(
            header.master_address,
            Some(CheetahString::from_static_str("127.0.0.1:10911"))
        );
        assert_eq!(header.master_epoch, Some(5));
        assert_eq!(header.sync_state_set_epoch, Some(10));
    }

    #[test]
    fn register_broker_to_controller_response_header_default() {
        let header = RegisterBrokerToControllerResponseHeader::default();
        assert_eq!(header.cluster_name, None);
        assert_eq!(header.broker_name, None);
        assert_eq!(header.master_broker_id, None);
        assert_eq!(header.master_address, None);
        assert_eq!(header.master_epoch, None);
        assert_eq!(header.sync_state_set_epoch, None);
    }

    #[test]
    fn register_broker_to_controller_response_header_clone() {
        let header = RegisterBrokerToControllerResponseHeader {
            cluster_name: Some(CheetahString::from_static_str("test_cluster")),
            broker_name: Some(CheetahString::from_static_str("test_broker")),
            master_broker_id: Some(123),
            master_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            master_epoch: Some(5),
            sync_state_set_epoch: Some(10),
        };
        let cloned = header.clone();
        assert_eq!(header.cluster_name, cloned.cluster_name);
        assert_eq!(header.broker_name, cloned.broker_name);
        assert_eq!(header.master_broker_id, cloned.master_broker_id);
        assert_eq!(header.master_address, cloned.master_address);
        assert_eq!(header.master_epoch, cloned.master_epoch);
        assert_eq!(header.sync_state_set_epoch, cloned.sync_state_set_epoch);
    }
}
