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
pub struct ElectMasterRequestHeader {
    #[required]
    pub cluster_name: CheetahString,

    #[required]
    pub broker_name: CheetahString,

    #[required]
    pub broker_id: i64,

    #[required]
    pub designate_elect: bool,

    pub invoke_time: u64,
}

impl ElectMasterRequestHeader {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: i64,
        designate_elect: bool,
        invoke_time: u64,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
            designate_elect,
            invoke_time,
        }
    }
}

impl Default for ElectMasterRequestHeader {
    fn default() -> Self {
        Self {
            cluster_name: CheetahString::new(),
            broker_name: CheetahString::new(),
            broker_id: 0,
            designate_elect: false,
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
    fn elect_master_request_header_new() {
        let header = ElectMasterRequestHeader::new("test_cluster", "test_broker", 123, true, 1234567890);
        assert_eq!(header.cluster_name, "test_cluster");
        assert_eq!(header.broker_name, "test_broker");
        assert_eq!(header.broker_id, 123);
        assert!(header.designate_elect);
        assert_eq!(header.invoke_time, 1234567890);
    }

    #[test]
    fn elect_master_request_header_serializes_correctly() {
        let header = ElectMasterRequestHeader::new("test_cluster", "test_broker", 123, true, 1234567890);
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
            map.get(&CheetahString::from_static_str("designateElect")).unwrap(),
            "true"
        );
        assert_eq!(
            map.get(&CheetahString::from_static_str("invokeTime")).unwrap(),
            "1234567890"
        );
    }

    #[test]
    fn elect_master_request_header_deserializes_correctly() {
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
            CheetahString::from_static_str("designateElect"),
            CheetahString::from_static_str("true"),
        );
        map.insert(
            CheetahString::from_static_str("invokeTime"),
            CheetahString::from_static_str("1234567890"),
        );

        let header = <ElectMasterRequestHeader as FromMap>::from(&map).unwrap();
        assert_eq!(header.cluster_name, "test_cluster");
        assert_eq!(header.broker_name, "test_broker");
        assert_eq!(header.broker_id, 123);
        assert!(header.designate_elect);
        assert_eq!(header.invoke_time, 1234567890);
    }

    #[test]
    fn elect_master_request_header_default() {
        let header = ElectMasterRequestHeader::default();
        assert_eq!(header.cluster_name, "");
        assert_eq!(header.broker_name, "");
        assert_eq!(header.broker_id, 0);
        assert!(!header.designate_elect);
        assert!(header.invoke_time > 0);
    }

    #[test]
    fn elect_master_request_header_clone() {
        let header = ElectMasterRequestHeader::new("test_cluster", "test_broker", 123, true, 1234567890);
        let cloned = header.clone();
        assert_eq!(header.cluster_name, cloned.cluster_name);
        assert_eq!(header.broker_name, cloned.broker_name);
        assert_eq!(header.broker_id, cloned.broker_id);
        assert_eq!(header.designate_elect, cloned.designate_elect);
        assert_eq!(header.invoke_time, cloned.invoke_time);
    }
}
