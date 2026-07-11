// Copyright 2026 The RocketMQ Rust Authors
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, RequestHeaderCodecV2)]
#[serde(rename_all = "camelCase")]
pub struct RemoveBrokerRequestHeader {
    #[required]
    pub broker_name: CheetahString,
    #[required]
    pub broker_cluster_name: CheetahString,
    #[required]
    pub broker_id: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn remove_broker_request_header_serializes_broker_identity() {
        let header = RemoveBrokerRequestHeader {
            broker_name: CheetahString::from_static_str("broker-a"),
            broker_cluster_name: CheetahString::from_static_str("DefaultCluster"),
            broker_id: 1,
        };

        let map = header.to_map().expect("header should encode");
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerName")),
            Some(&CheetahString::from_static_str("broker-a"))
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerClusterName")),
            Some(&CheetahString::from_static_str("DefaultCluster"))
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("brokerId")),
            Some(&CheetahString::from_static_str("1"))
        );
    }

    #[test]
    fn remove_broker_request_header_deserializes_broker_identity() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("broker-a"),
        );
        map.insert(
            CheetahString::from_static_str("brokerClusterName"),
            CheetahString::from_static_str("DefaultCluster"),
        );
        map.insert(
            CheetahString::from_static_str("brokerId"),
            CheetahString::from_static_str("1"),
        );

        let header = <RemoveBrokerRequestHeader as FromMap>::from(&map).expect("header should decode");
        assert_eq!(header.broker_name, CheetahString::from_static_str("broker-a"));
        assert_eq!(
            header.broker_cluster_name,
            CheetahString::from_static_str("DefaultCluster")
        );
        assert_eq!(header.broker_id, 1);
    }
}
