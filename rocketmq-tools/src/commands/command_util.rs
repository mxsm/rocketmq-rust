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

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RocketmqError;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;

pub struct CommandUtil;

impl CommandUtil {
    const MASTER_ID: u64 = 0;

    /// Collects the master broker addresses for the given cluster name from the provided ClusterInfo.
    ///
    /// # Examples
    ///
    /// ```
    /// // assuming `cluster_info` contains mappings for "DefaultCluster"
    /// let addrs = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, "DefaultCluster").unwrap();
    /// assert!(addrs.iter().any(|a| a.contains("192.168.1.1")));
    /// ```
    ///
    /// # Returns
    ///
    /// A vector containing the master broker addresses (addresses at broker ID 0) for the specified cluster; the vector will be empty if no master addresses are found.
    pub fn fetch_master_addr_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        let cluster_addr_table = cluster_info.cluster_addr_table.as_ref().ok_or_else(|| {
            RocketmqError::SubCommand(
                "CommandUtil".into(),
                "No cluster address table available from nameserver.".into(),
            )
        })?;
        let broker_names = cluster_addr_table.get(cluster_name).ok_or_else(|| {
            RocketmqError::SubCommand(
                "CommandUtil".into(),
                format!(
                    "Make sure the specified clusterName exists or the nameserver which connected \
                     to is correct. Cluster: {}",
                    cluster_name
                ),
            )
        })?;
        let broker_addr_table = cluster_info.broker_addr_table.as_ref().ok_or_else(|| {
            RocketmqError::SubCommand(
                "CommandUtil".into(),
                "No broker address table available from nameserver.".into(),
            )
        })?;

        let mut master_addrs = Vec::new();
        for broker_name in broker_names {
            if let Some(broker_data) = broker_addr_table.get(broker_name) {
                if let Some(master_addr) = broker_data.broker_addrs().get(&Self::MASTER_ID) {
                    master_addrs.push(master_addr.clone());
                }
            }
        }
        Ok(master_addrs)
    }

    /// Retrieve the master broker address for the specified broker name.
    ///
    /// Looks up the broker in the provided `ClusterInfo` and returns the address mapped to the master ID (0).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // assuming `cluster_info` is a populated `ClusterInfo`
    /// let master = CommandUtil::fetch_master_addr_by_broker_name(&cluster_info, "broker-a")?;
    /// println!("master address: {}", master);
    /// ```
    #[allow(unused)]
    pub fn fetch_master_addr_by_broker_name(
        cluster_info: &ClusterInfo,
        broker_name: &str,
    ) -> RocketMQResult<CheetahString> {
        if let Some(broker_addr_table) = &cluster_info.broker_addr_table {
            if let Some(broker_data) = broker_addr_table.get(broker_name) {
                if let Some(master_addr) = broker_data.broker_addrs().get(&Self::MASTER_ID) {
                    return Ok(master_addr.clone());
                }
            }
        }
        Err(RocketmqError::SubCommand(
            "CommandUtil".into(),
            format!("No broker address for broker name: {}", broker_name),
        ))
    }

    /// Retrieve the broker names associated with a cluster.
    ///
    /// Returns a `Vec<String>` containing the broker names mapped to `cluster_name`.
    /// If the cluster name is not present in `cluster_info.cluster_addr_table`, an error is returned.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let names = CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, "DefaultCluster")?;
    /// assert!(names.contains(&"broker-a".to_string()));
    /// ```
    #[allow(unused)]
    pub fn fetch_broker_name_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<String>> {
        if let Some(cluster_addr_table) = &cluster_info.cluster_addr_table {
            if let Some(broker_names) = cluster_addr_table.get(cluster_name) {
                return Ok(broker_names.iter().map(|n| n.to_string()).collect());
            }
        }
        Err(RocketmqError::SubCommand(
            "CommandUtil".into(),
            format!(
                "Make sure the specified clusterName exists or the nameserver which connected to \
                 is correct. Cluster: {}",
                cluster_name
            ),
        ))
    }

    /// Finds the broker name that corresponds to a given broker address.
    ///
    /// Returns `Ok(String)` containing the broker name if the address is present in the cluster's broker address table, `Err` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// // Construct a minimal ClusterInfo equivalent for the example:
    /// use std::collections::{BTreeMap, BTreeSet};
    /// use rocketmq_tools::namesrv::ClusterInfo;
    /// use rocketmq_tools::namesrv::BrokerData;
    ///
    /// let mut broker_addrs = BTreeMap::new();
    /// broker_addrs.insert(0, "192.168.1.1:10911".into());
    /// let broker_data = BrokerData::new("DefaultCluster".into(), "broker-a".into(), broker_addrs);
    ///
    /// let mut broker_addr_table = BTreeMap::new();
    /// broker_addr_table.insert("broker-a".into(), broker_data);
    ///
    /// let mut cluster_addr_table = BTreeMap::new();
    /// let mut set = BTreeSet::new();
    /// set.insert("broker-a".into());
    /// cluster_addr_table.insert("DefaultCluster".into(), set);
    ///
    /// let cluster_info = ClusterInfo { broker_addr_table: Some(broker_addr_table), cluster_addr_table: Some(cluster_addr_table) };
    ///
    /// let name = crate::commands::command_util::CommandUtil::fetch_broker_name_by_addr(&cluster_info, "192.168.1.1:10911").unwrap();
    /// assert_eq!(name, "broker-a");
    /// ```
    #[allow(unused)]
    pub fn fetch_broker_name_by_addr(
        cluster_info: &ClusterInfo,
        broker_addr: &str,
    ) -> RocketMQResult<String> {
        if let Some(broker_addr_table) = &cluster_info.broker_addr_table {
            for (broker_name, broker_data) in broker_addr_table.iter() {
                for addr in broker_data.broker_addrs().values() {
                    if addr.as_str() == broker_addr {
                        return Ok(broker_name.to_string());
                    }
                }
            }
        }
        Err(RocketmqError::SubCommand(
            "CommandUtil".into(),
            format!(
                "Make sure the specified broker address exists. Address: {}",
                broker_addr
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;

    use super::*;

    /// Creates a ClusterInfo for tests containing a single broker.
    ///
    /// The returned ClusterInfo includes:
    /// - broker_addr_table with "broker-a" -> BrokerData where broker_addrs contains
    ///   id 0 -> "192.168.1.1:10911" (master) and id 1 -> "192.168.1.2:10911" (slave).
    /// - cluster_addr_table with "DefaultCluster" -> {"broker-a"}.
    ///
    /// # Examples
    ///
    /// ```
    /// let ci = create_test_cluster_info();
    /// let masters = CommandUtil::fetch_master_addr_by_cluster_name(&ci, "DefaultCluster").unwrap();
    /// assert_eq!(masters[0].as_str(), "192.168.1.1:10911");
    /// ```
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
        cluster_addr_table.insert(
            CheetahString::from_static_str("DefaultCluster"),
            broker_names,
        );

        ClusterInfo::new(Some(broker_addr_table), Some(cluster_addr_table))
    }

    #[test]
    fn fetch_master_addr_by_cluster_name() {
        let cluster_info = create_test_cluster_info();
        let result =
            CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, "DefaultCluster");

        assert!(result.is_ok());
        let addrs = result.unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].as_str(), "192.168.1.1:10911");
    }

    #[test]
    fn fetch_master_addr_by_cluster_name_not_found() {
        let cluster_info = create_test_cluster_info();
        let result =
            CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, "NonExistentCluster");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_master_addr_by_broker_name() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_master_addr_by_broker_name(&cluster_info, "broker-a");

        assert!(result.is_ok());
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
        let result =
            CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, "DefaultCluster");

        assert!(result.is_ok());
        let names = result.unwrap();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "broker-a");
    }

    #[test]
    fn fetch_broker_name_by_cluster_name_not_found() {
        let cluster_info = create_test_cluster_info();
        let result =
            CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, "NonExistentCluster");

        assert!(result.is_err());
    }

    #[test]
    fn fetch_broker_name_by_addr() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_broker_name_by_addr(&cluster_info, "192.168.1.1:10911");

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "broker-a");
    }

    #[test]
    fn fetch_broker_name_by_addr_not_found() {
        let cluster_info = create_test_cluster_info();
        let result = CommandUtil::fetch_broker_name_by_addr(&cluster_info, "192.168.1.99:10911");

        assert!(result.is_err());
    }
}