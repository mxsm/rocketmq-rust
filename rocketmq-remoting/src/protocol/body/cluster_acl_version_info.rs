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

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::DataVersion;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ClusterAclVersionInfo {
    pub broker_name: CheetahString,
    pub broker_addr: CheetahString,
    //Deprecated
    pub acl_config_data_version: Option<DataVersion>,
    pub all_acl_config_data_version: HashMap<CheetahString, DataVersion>,
    pub cluster_name: CheetahString,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use cheetah_string::CheetahString;
    use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;

    use super::*;
    use crate::protocol::DataVersion;

    #[test]
    fn cluster_acl_version_info_default_values() {
        let info = ClusterAclVersionInfo {
            broker_name: CheetahString::new(),
            broker_addr: CheetahString::new(),
            acl_config_data_version: None,
            all_acl_config_data_version: HashMap::new(),
            cluster_name: CheetahString::new(),
        };
        assert_eq!(info.broker_name, CheetahString::new());
        assert_eq!(info.broker_addr, CheetahString::new());
        assert!(info.acl_config_data_version.is_none());
        assert!(info.all_acl_config_data_version.is_empty());
        assert_eq!(info.cluster_name, CheetahString::new());
    }

    #[test]
    fn cluster_acl_version_info_equality() {
        let mut all_acl_config_data_version = HashMap::new();
        let version = DataVersion::default();
        all_acl_config_data_version.insert(CheetahString::from("key1"), version.clone());

        let info1 = ClusterAclVersionInfo {
            broker_name: CheetahString::from("broker1"),
            broker_addr: CheetahString::from("addr1"),
            acl_config_data_version: Some(version.clone()),
            all_acl_config_data_version: all_acl_config_data_version.clone(),
            cluster_name: CheetahString::from("cluster1"),
        };

        let info2 = ClusterAclVersionInfo {
            broker_name: CheetahString::from("broker1"),
            broker_addr: CheetahString::from("addr1"),
            acl_config_data_version: Some(version),
            all_acl_config_data_version,
            cluster_name: CheetahString::from("cluster1"),
        };

        assert_eq!(
            serde_json::to_string(&info1).unwrap(),
            serde_json::to_string(&info2).unwrap()
        );
    }

    #[test]
    fn cluster_acl_version_info_inequality() {
        let info1 = ClusterAclVersionInfo {
            broker_name: CheetahString::from("broker1"),
            broker_addr: CheetahString::from("addr1"),
            acl_config_data_version: Some(DataVersion::default()),
            all_acl_config_data_version: HashMap::new(),
            cluster_name: CheetahString::from("cluster1"),
        };

        let info2 = ClusterAclVersionInfo {
            broker_name: CheetahString::from("broker2"),
            broker_addr: CheetahString::from("addr2"),
            acl_config_data_version: None,
            all_acl_config_data_version: HashMap::new(),
            cluster_name: CheetahString::from("cluster2"),
        };

        assert_ne!(
            serde_json::to_string(&info1).unwrap(),
            serde_json::to_string(&info2).unwrap()
        );
    }

    #[test]
    fn serialize_cluster_acl_version_info() {
        let mut all_acl_config_data_version = HashMap::new();
        all_acl_config_data_version.insert(CheetahString::from("key1"), DataVersion::default());

        let info = ClusterAclVersionInfo {
            broker_name: CheetahString::from("broker1"),
            broker_addr: CheetahString::from("addr1"),
            acl_config_data_version: Some(DataVersion::default()),
            all_acl_config_data_version,
            cluster_name: CheetahString::from("cluster1"),
        };
        let serialized = serde_json::to_string(&info).unwrap();
        assert!(serialized.contains("\"brokerName\":\"broker1\""));
        assert!(serialized.contains("\"brokerAddr\":\"addr1\""));
        assert!(serialized.contains("\"aclConfigDataVersion\":"));
        assert!(serialized.contains("\"allAclConfigDataVersion\":"));
        assert!(serialized.contains("\"clusterName\":\"cluster1\""));
    }

    #[test]
    fn deserialize_cluster_acl_version_info() {
        let json = r#"{
            "brokerName": "broker1",
            "brokerAddr": "addr1",
            "aclConfigDataVersion": null,
            "allAclConfigDataVersion": {},
            "clusterName": "cluster1"
        }"#;
        let deserialized: ClusterAclVersionInfo = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.broker_name, CheetahString::from("broker1"));
        assert_eq!(deserialized.broker_addr, CheetahString::from("addr1"));
        assert!(deserialized.acl_config_data_version.is_none());
        assert!(deserialized.all_acl_config_data_version.is_empty());
        assert_eq!(deserialized.cluster_name, CheetahString::from("cluster1"));
    }

    #[test]
    fn deserialize_cluster_acl_version_info_with_data_version() {
        let json = r#"{
            "brokerName": "broker1",
            "brokerAddr": "addr1",
            "aclConfigDataVersion": {"counter": 1, "timestamp": 123456789,"stateVersion": 0},
            "allAclConfigDataVersion": {"key1": {"counter": 1, "timestamp": 123456789,"stateVersion": 0}},
            "clusterName": "cluster1"
        }"#;
        let deserialized: ClusterAclVersionInfo = SerdeJsonUtils::from_json_str(json).unwrap();
        assert_eq!(&deserialized.broker_name, &CheetahString::from("broker1"));
        assert_eq!(&deserialized.broker_addr, &CheetahString::from("addr1"));
        assert!(deserialized.acl_config_data_version.is_some());
        assert_eq!(
            deserialized
                .acl_config_data_version
                .as_ref()
                .unwrap()
                .counter
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            deserialized.acl_config_data_version.as_ref().unwrap().timestamp,
            123456789
        );
        assert_eq!(
            deserialized
                .all_acl_config_data_version
                .get(&CheetahString::from("key1"))
                .unwrap()
                .counter
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            deserialized
                .all_acl_config_data_version
                .get(&CheetahString::from("key1"))
                .unwrap()
                .timestamp,
            123456789
        );
        assert_eq!(deserialized.cluster_name, CheetahString::from("cluster1"));
    }
}
