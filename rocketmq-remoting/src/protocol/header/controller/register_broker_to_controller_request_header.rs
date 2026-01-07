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
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_macros::RequestHeaderCodecV2;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RegisterBrokerToControllerRequestHeader {
    pub cluster_name: Option<CheetahString>,
    pub broker_name: Option<CheetahString>,
    pub broker_id: Option<i64>,
    pub broker_address: Option<CheetahString>,
    pub invoke_time: u64,
}

impl Default for RegisterBrokerToControllerRequestHeader {
    fn default() -> Self {
        Self {
            cluster_name: None,
            broker_name: None,
            broker_id: None,
            broker_address: None,
            invoke_time: get_current_millis(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn register_broker_to_controller_request_header_serializes_correctly() {
        let header = RegisterBrokerToControllerRequestHeader {
            cluster_name: Some(CheetahString::from_static_str("test_cluster")),
            broker_name: Some(CheetahString::from_static_str("test_broker")),
            broker_id: Some(123),
            broker_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            invoke_time: 1234567890,
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
        assert_eq!(map.get(&CheetahString::from_static_str("brokerId")).unwrap(), "123");
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerAddress")).unwrap(),
            "127.0.0.1:10911"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("invokeTime")).unwrap(),
            "1234567890"
        );
    }

    #[test]
    fn register_broker_to_controller_request_header_deserializes_correctly() {
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
            CheetahString::from_static_str("brokerId"),
            CheetahString::from_static_str("123"),
        );
        map.insert(
            CheetahString::from_static_str("brokerAddress"),
            CheetahString::from_static_str("127.0.0.1:10911"),
        );
        map.insert(
            CheetahString::from_static_str("invokeTime"),
            CheetahString::from_static_str("1234567890"),
        );

        let header = <RegisterBrokerToControllerRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(
            header.cluster_name,
            Some(CheetahString::from_static_str("test_cluster"))
        );
        assert_eq!(header.broker_name, Some(CheetahString::from_static_str("test_broker")));
        assert_eq!(header.broker_id, Some(123));
        assert_eq!(
            header.broker_address,
            Some(CheetahString::from_static_str("127.0.0.1:10911"))
        );
        assert_eq!(header.invoke_time, 1234567890);
    }

    #[test]
    fn register_broker_to_controller_request_header_default() {
        let header = RegisterBrokerToControllerRequestHeader::default();
        assert_eq!(header.cluster_name, None);
        assert_eq!(header.broker_name, None);
        assert_eq!(header.broker_id, None);
        assert_eq!(header.broker_address, None);
        assert!(header.invoke_time > 0);
    }

    #[test]
    fn register_broker_to_controller_request_header_clone() {
        let header = RegisterBrokerToControllerRequestHeader {
            cluster_name: Some(CheetahString::from_static_str("test_cluster")),
            broker_name: Some(CheetahString::from_static_str("test_broker")),
            broker_id: Some(123),
            broker_address: Some(CheetahString::from_static_str("127.0.0.1:10911")),
            invoke_time: 1234567890,
        };
        let cloned = header.clone();
        assert_eq!(header.cluster_name, cloned.cluster_name);
        assert_eq!(header.broker_name, cloned.broker_name);
        assert_eq!(header.broker_id, cloned.broker_id);
        assert_eq!(header.broker_address, cloned.broker_address);
        assert_eq!(header.invoke_time, cloned.invoke_time);
    }
}
