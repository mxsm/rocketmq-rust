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

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterInfo {
    #[serde(rename = "brokerAddrTable")]
    pub broker_addr_table: Option<HashMap<CheetahString, BrokerData>>,

    #[serde(rename = "clusterAddrTable")]
    pub cluster_addr_table: Option<HashMap<CheetahString, HashSet<CheetahString>>>,
}

impl ClusterInfo {
    pub fn new(
        broker_addr_table: Option<HashMap<CheetahString, BrokerData>>,
        cluster_addr_table: Option<HashMap<CheetahString, HashSet<CheetahString>>>,
    ) -> ClusterInfo {
        ClusterInfo {
            broker_addr_table,
            cluster_addr_table,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_info_default() {
        let cluster_info = ClusterInfo::default();
        assert!(cluster_info.broker_addr_table.is_none());
        assert!(cluster_info.cluster_addr_table.is_none());
    }

    #[test]
    fn test_cluster_info_new() {
        let mut broker_addr_table = HashMap::new();
        let broker_data = BrokerData::default();
        broker_addr_table.insert(CheetahString::from("broker1"), broker_data);

        let mut cluster_addr_table = HashMap::new();
        let mut brokers = HashSet::new();
        brokers.insert(CheetahString::from("broker1"));
        cluster_addr_table.insert(CheetahString::from("cluster1"), brokers);

        let cluster_info = ClusterInfo::new(Some(broker_addr_table.clone()), Some(cluster_addr_table.clone()));
        assert_eq!(cluster_info.broker_addr_table, Some(broker_addr_table));
        assert_eq!(cluster_info.cluster_addr_table, Some(cluster_addr_table));
    }

    #[test]
    fn test_cluster_info_serialization() {
        let mut broker_addr_table = HashMap::new();
        let broker_data = BrokerData::default();
        broker_addr_table.insert(CheetahString::from("broker1"), broker_data);

        let mut cluster_addr_table = HashMap::new();
        let mut brokers = HashSet::new();
        brokers.insert(CheetahString::from("broker1"));
        cluster_addr_table.insert(CheetahString::from("cluster1"), brokers);

        let cluster_info = ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table));
        let serialized = serde_json::to_string(&cluster_info).unwrap();
        assert!(serialized.contains("brokerAddrTable"));
        assert!(serialized.contains("clusterAddrTable"));
    }

    #[test]
    fn test_cluster_info_deserialization() {
        let json = r#"{"brokerAddrTable":{"broker1":{"cluster":"c1","brokerName":"b1","brokerAddrs":{"0":"127.0.0.1:10911"},"enableActingMaster":false}},"clusterAddrTable":{"cluster1":["broker1"]}}"#;
        let cluster_info: ClusterInfo = serde_json::from_str(json).unwrap();
        assert!(cluster_info.broker_addr_table.is_some());
        assert!(cluster_info.cluster_addr_table.is_some());
        assert_eq!(
            cluster_info
                .broker_addr_table
                .unwrap()
                .get(&CheetahString::from("broker1"))
                .unwrap()
                .broker_name(),
            "b1"
        );
    }
}
