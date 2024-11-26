/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BrokerMemberGroup {
    pub cluster: CheetahString,
    pub broker_name: CheetahString,
    pub broker_addrs: HashMap<u64 /* brokerId */, CheetahString /* broker address */>,
}

impl BrokerMemberGroup {
    pub fn new(cluster: CheetahString, broker_name: CheetahString) -> Self {
        Self {
            cluster,
            broker_name,
            broker_addrs: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetBrokerMemberGroupResponseBody {
    pub broker_member_group: Option<BrokerMemberGroup>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn new_creates_broker_member_group_with_empty_broker_addrs() {
        let cluster = CheetahString::from("test_cluster");
        let broker_name = CheetahString::from("test_broker");
        let group = BrokerMemberGroup::new(cluster.clone(), broker_name.clone());

        assert_eq!(group.cluster, cluster);
        assert_eq!(group.broker_name, broker_name);
        assert!(group.broker_addrs.is_empty());
    }

    #[test]
    fn broker_member_group_serializes_correctly() {
        let cluster = CheetahString::from("test_cluster");
        let broker_name = CheetahString::from("test_broker");
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(1, CheetahString::from("127.0.0.1:10911"));
        let group = BrokerMemberGroup {
            cluster: cluster.clone(),
            broker_name: broker_name.clone(),
            broker_addrs: broker_addrs.clone(),
        };

        let serialized = serde_json::to_string(&group).unwrap();
        let expected = format!(
            r#"{{"cluster":"{}","brokerName":"{}","brokerAddrs":{{"1":"{}"}}}}"#,
            cluster,
            broker_name,
            broker_addrs.get(&1).unwrap()
        );
        assert_eq!(serialized, expected);
    }

    #[test]
    fn broker_member_group_deserializes_correctly() {
        let data = r#"{"cluster":"test_cluster","brokerName":"test_broker","brokerAddrs":{"1":"127.0.0.1:10911"}}"#;
        let group: BrokerMemberGroup = serde_json::from_str(data).unwrap();

        assert_eq!(group.cluster, CheetahString::from("test_cluster"));
        assert_eq!(group.broker_name, CheetahString::from("test_broker"));
        assert_eq!(
            group.broker_addrs.get(&1).unwrap(),
            &CheetahString::from("127.0.0.1:10911")
        );
    }

    #[test]
    fn get_broker_member_group_response_body_serializes_correctly() {
        let cluster = CheetahString::from("test_cluster");
        let broker_name = CheetahString::from("test_broker");
        let group = BrokerMemberGroup::new(cluster.clone(), broker_name.clone());
        let response_body = GetBrokerMemberGroupResponseBody {
            broker_member_group: Some(group.clone()),
        };

        let serialized = serde_json::to_string(&response_body).unwrap();
        let expected = format!(
            r#"{{"brokerMemberGroup":{{"cluster":"{}","brokerName":"{}","brokerAddrs":{{}}}}}}"#,
            cluster, broker_name
        );
        assert_eq!(serialized, expected);
    }

    #[test]
    fn get_broker_member_group_response_body_deserializes_correctly() {
        let data = r#"{"brokerMemberGroup":{"cluster":"test_cluster","brokerName":"test_broker","brokerAddrs":{}}}"#;
        let response_body: GetBrokerMemberGroupResponseBody = serde_json::from_str(data).unwrap();

        assert!(response_body.broker_member_group.is_some());
        let group = response_body.broker_member_group.unwrap();
        assert_eq!(group.cluster, CheetahString::from("test_cluster"));
        assert_eq!(group.broker_name, CheetahString::from("test_broker"));
        assert!(group.broker_addrs.is_empty());
    }
}
