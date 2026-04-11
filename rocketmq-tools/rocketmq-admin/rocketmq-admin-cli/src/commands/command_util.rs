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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;

use rocketmq_admin_core::core::resolver::BrokerAddressResolver;

pub struct CommandUtil;

impl CommandUtil {
    pub const NO_MASTER_PLACEHOLDER: &'static str = BrokerAddressResolver::NO_MASTER_PLACEHOLDER;

    pub fn fetch_master_addr_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        BrokerAddressResolver::fetch_master_addr_by_cluster_name(cluster_info, cluster_name)
    }

    #[allow(unused)]
    pub fn fetch_master_addr_by_broker_name(
        cluster_info: &ClusterInfo,
        broker_name: &str,
    ) -> RocketMQResult<CheetahString> {
        BrokerAddressResolver::fetch_master_addr_by_broker_name(cluster_info, broker_name)
    }

    pub fn fetch_master_and_slave_addr_by_broker_name(
        cluster_info: &ClusterInfo,
        broker_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        BrokerAddressResolver::fetch_master_and_slave_addr_by_broker_name(cluster_info, broker_name)
    }

    #[allow(unused)]
    pub fn fetch_broker_name_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<String>> {
        BrokerAddressResolver::fetch_broker_name_by_cluster_name(cluster_info, cluster_name)
    }

    #[allow(unused)]
    pub fn fetch_broker_name_by_addr(cluster_info: &ClusterInfo, broker_addr: &str) -> RocketMQResult<String> {
        BrokerAddressResolver::fetch_broker_name_by_addr(cluster_info, broker_addr)
    }

    pub fn fetch_master_and_slave_addr_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(cluster_info, cluster_name)
    }

    pub fn fetch_master_and_slave_distinguish(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<std::collections::HashMap<CheetahString, Vec<CheetahString>>> {
        BrokerAddressResolver::fetch_master_and_slave_distinguish(cluster_info, cluster_name)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    use super::*;

    fn create_test_cluster_info() -> ClusterInfo {
        let mut broker_addr_table = HashMap::new();
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0u64, CheetahString::from_static_str("192.168.1.1:10911"));
        broker_addrs.insert(1u64, CheetahString::from_static_str("192.168.1.2:10911"));

        let broker_data = BrokerData::new(
            CheetahString::from_static_str("DefaultCluster"),
            CheetahString::from_static_str("broker-a"),
            broker_addrs,
            None,
        );
        broker_addr_table.insert(CheetahString::from_static_str("broker-a"), broker_data);

        let mut cluster_addr_table = HashMap::new();
        let mut broker_names = HashSet::new();
        broker_names.insert(CheetahString::from_static_str("broker-a"));
        cluster_addr_table.insert(CheetahString::from_static_str("DefaultCluster"), broker_names);

        ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table))
    }

    #[test]
    fn fetch_master_addr_by_cluster_name() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, "DefaultCluster").unwrap();

        assert_eq!(result[0].as_str(), "192.168.1.1:10911");
    }

    #[test]
    fn fetch_master_addr_by_cluster_name_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, "NonExistentCluster");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_master_addr_by_broker_name() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_addr_by_broker_name(&cluster_info, "broker-a");

        assert_eq!(result.unwrap().as_str(), "192.168.1.1:10911");
    }

    #[test]
    fn fetch_master_addr_by_broker_name_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_addr_by_broker_name(&cluster_info, "broker-z");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_broker_name_by_cluster_name() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, "DefaultCluster").unwrap();

        assert_eq!(result[0], "broker-a");
    }

    #[test]
    fn fetch_broker_name_by_cluster_name_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, "NonExistentCluster");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_broker_name_by_addr() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_broker_name_by_addr(&cluster_info, "192.168.1.1:10911");

        assert_eq!(result.unwrap(), "broker-a");
    }

    #[test]
    fn fetch_broker_name_by_addr_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_broker_name_by_addr(&cluster_info, "192.168.1.99:10911");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_master_and_slave_addr_by_cluster_name() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, "DefaultCluster").unwrap();

        assert!(result.contains(&CheetahString::from_static_str("192.168.1.1:10911")));
        assert!(result.contains(&CheetahString::from_static_str("192.168.1.2:10911")));
    }

    #[test]
    fn fetch_master_and_slave_addr_by_cluster_name_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, "NonExistentCluster");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_master_and_slave_distinguish() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, "DefaultCluster").unwrap();

        assert_eq!(result.len(), 1);
        let master_addr = CheetahString::from_static_str("192.168.1.1:10911");
        assert!(result.contains_key(&master_addr));

        let slaves = result.get(&master_addr).unwrap();
        assert_eq!(slaves.len(), 1);
        assert_eq!(slaves[0].as_str(), "192.168.1.2:10911");
    }

    #[test]
    fn fetch_master_and_slave_distinguish_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, "NonExistentCluster");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Make sure the specified clusterName exists"));
        assert!(err_msg.contains("NonExistentCluster"));
    }

    #[test]
    fn fetch_master_and_slave_distinguish_no_master() {
        let mut broker_addr_table = HashMap::new();
        let mut broker_addrs = HashMap::new();
        // Only add slave, no master (broker id = 0)
        broker_addrs.insert(1u64, CheetahString::from_static_str("192.168.1.2:10911"));
        broker_addrs.insert(2u64, CheetahString::from_static_str("192.168.1.3:10911"));

        let broker_data = BrokerData::new(
            CheetahString::from_static_str("DefaultCluster"),
            CheetahString::from_static_str("broker-a"),
            broker_addrs,
            None,
        );
        broker_addr_table.insert(CheetahString::from_static_str("broker-a"), broker_data);

        let mut cluster_addr_table = HashMap::new();
        let mut broker_names = HashSet::new();
        broker_names.insert(CheetahString::from_static_str("broker-a"));
        cluster_addr_table.insert(CheetahString::from_static_str("DefaultCluster"), broker_names);

        let cluster_info = ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table));
        let result = CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, "DefaultCluster").unwrap();

        // Should have NO_MASTER_PLACEHOLDER as key
        assert_eq!(result.len(), 1);
        let no_master_key = CheetahString::from_static_str(CommandUtil::NO_MASTER_PLACEHOLDER);
        assert!(result.contains_key(&no_master_key));

        let slaves = result.get(&no_master_key).unwrap();
        assert_eq!(slaves.len(), 2);
    }
}
