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

use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ElectMasterResponseBody {
    pub broker_member_group: Option<BrokerMemberGroup>,
    pub sync_state_set: HashSet<i64>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn elect_master_response_body_serializes_correctly() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from_static_str("127.0.0.1:10911"));
        broker_addrs.insert(1, CheetahString::from_static_str("127.0.0.1:10912"));

        let broker_member_group = BrokerMemberGroup {
            cluster: CheetahString::from_static_str("test_cluster"),
            broker_name: CheetahString::from_static_str("test_broker"),
            broker_addrs,
        };

        let mut sync_state_set = HashSet::new();
        sync_state_set.insert(0);
        sync_state_set.insert(1);

        let body = ElectMasterResponseBody {
            broker_member_group: Some(broker_member_group),
            sync_state_set,
        };

        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("brokerMemberGroup"));
        assert!(json.contains("syncStateSet"));
        assert!(json.contains("test_cluster"));
        assert!(json.contains("test_broker"));
    }

    #[test]
    fn elect_master_response_body_deserializes_correctly() {
        let json = r#"{
            "brokerMemberGroup": {
                "cluster": "test_cluster",
                "brokerName": "test_broker",
                "brokerAddrs": {
                    "0": "127.0.0.1:10911",
                    "1": "127.0.0.1:10912"
                }
            },
            "syncStateSet": [0, 1, 2]
        }"#;

        let body: ElectMasterResponseBody = serde_json::from_str(json).unwrap();
        assert!(body.broker_member_group.is_some());
        let group = body.broker_member_group.unwrap();
        assert_eq!(group.cluster, "test_cluster");
        assert_eq!(group.broker_name, "test_broker");
        assert_eq!(group.broker_addrs.len(), 2);
        assert_eq!(body.sync_state_set.len(), 3);
        assert!(body.sync_state_set.contains(&0));
        assert!(body.sync_state_set.contains(&1));
        assert!(body.sync_state_set.contains(&2));
    }

    #[test]
    fn elect_master_response_body_default() {
        let body = ElectMasterResponseBody::default();
        assert!(body.broker_member_group.is_none());
        assert!(body.sync_state_set.is_empty());
    }

    #[test]
    fn elect_master_response_body_clone() {
        let mut sync_state_set = HashSet::new();
        sync_state_set.insert(0);
        sync_state_set.insert(1);

        let broker_member_group = BrokerMemberGroup {
            cluster: CheetahString::from_static_str("test_cluster"),
            broker_name: CheetahString::from_static_str("test_broker"),
            broker_addrs: HashMap::new(),
        };

        let body = ElectMasterResponseBody {
            broker_member_group: Some(broker_member_group),
            sync_state_set,
        };

        let cloned = body.clone();
        assert_eq!(cloned.sync_state_set.len(), body.sync_state_set.len());
        assert!(cloned.broker_member_group.is_some());
    }
}
