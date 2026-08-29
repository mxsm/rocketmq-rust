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
    fn serde_round_trip_preserves_the_election_result() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from_static_str("127.0.0.1:10911"));
        let broker_member_group = BrokerMemberGroup {
            cluster: CheetahString::from_static_str("test_cluster"),
            broker_name: CheetahString::from_static_str("test_broker"),
            broker_addrs,
        };
        let body = ElectMasterResponseBody {
            broker_member_group: Some(broker_member_group),
            sync_state_set: HashSet::from([0, 1]),
        };

        let value = serde_json::to_value(&body).expect("serialize election result");
        assert!(value.get("brokerMemberGroup").is_some());
        assert!(value.get("syncStateSet").is_some());

        let decoded: ElectMasterResponseBody = serde_json::from_value(value).expect("deserialize election result");
        let group = decoded.broker_member_group.expect("broker member group");
        assert_eq!(group.cluster, "test_cluster");
        assert_eq!(group.broker_name, "test_broker");
        assert_eq!(
            group.broker_addrs.get(&0),
            Some(&CheetahString::from_static_str("127.0.0.1:10911"))
        );
        assert_eq!(decoded.sync_state_set, HashSet::from([0, 1]));
    }
}
