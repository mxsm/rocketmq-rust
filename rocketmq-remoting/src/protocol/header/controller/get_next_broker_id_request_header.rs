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
pub struct GetNextBrokerIdRequestHeader {
    pub cluster_name: CheetahString,
    pub broker_name: CheetahString,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::protocol::command_custom_header::CommandCustomHeader;
    use crate::protocol::command_custom_header::FromMap;

    #[test]
    fn get_next_broker_id_request_header_serializes_correctly() {
        let header = GetNextBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("test_cluster"),
            broker_name: CheetahString::from_static_str("test_broker"),
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
    }

    #[test]
    fn get_next_broker_id_request_header_deserializes_correctly() {
        let mut map = HashMap::new();
        map.insert(
            CheetahString::from_static_str("clusterName"),
            CheetahString::from_static_str("test_cluster"),
        );
        map.insert(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("test_broker"),
        );

        let header = <GetNextBrokerIdRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.cluster_name, "test_cluster");
        assert_eq!(header.broker_name, "test_broker");
    }

    #[test]
    fn get_next_broker_id_request_header_default() {
        let header = GetNextBrokerIdRequestHeader::default();
        assert_eq!(header.cluster_name, "");
        assert_eq!(header.broker_name, "");
    }

    #[test]
    fn get_next_broker_id_request_header_clone() {
        let header = GetNextBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("test_cluster"),
            broker_name: CheetahString::from_static_str("test_broker"),
        };
        let cloned = header.clone();
        assert_eq!(header.cluster_name, cloned.cluster_name);
        assert_eq!(header.broker_name, cloned.broker_name);
    }
}
