use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;

pub struct BrokerAddressResolver;

impl BrokerAddressResolver {
    const MASTER_ID: u64 = 0;
    pub const NO_MASTER_PLACEHOLDER: &'static str = "NO_MASTER";

    pub fn fetch_master_addr_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        let cluster_addr_table = cluster_info.cluster_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No cluster address table available from nameserver.".into())
        })?;
        let broker_names = cluster_addr_table.get(cluster_name).ok_or_else(|| {
            RocketMQError::Internal(format!(
                "BrokerAddressResolver: Make sure the specified clusterName exists or the nameserver which connected \
                 to is correct. Cluster: {}",
                cluster_name
            ))
        })?;
        let broker_addr_table = cluster_info.broker_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No broker address table available from nameserver.".into())
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
        Err(RocketMQError::Internal(format!(
            "BrokerAddressResolver: No broker address for broker name: {}",
            broker_name
        )))
    }

    pub fn fetch_master_and_slave_addr_by_broker_name(
        cluster_info: &ClusterInfo,
        broker_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        let broker_addr_table = cluster_info.broker_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No broker address table available from nameserver.".into())
        })?;
        let broker_data = broker_addr_table.get(broker_name).ok_or_else(|| {
            RocketMQError::Internal(format!(
                "BrokerAddressResolver: No broker data found for broker name: {}",
                broker_name
            ))
        })?;
        let mut addrs: Vec<CheetahString> = broker_data.broker_addrs().values().cloned().collect();
        addrs.sort();
        addrs.dedup();
        Ok(addrs)
    }

    pub fn fetch_broker_name_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<String>> {
        if let Some(cluster_addr_table) = &cluster_info.cluster_addr_table {
            if let Some(broker_names) = cluster_addr_table.get(cluster_name) {
                return Ok(broker_names.iter().map(ToString::to_string).collect());
            }
        }
        Err(RocketMQError::Internal(format!(
            "BrokerAddressResolver: Make sure the specified clusterName exists or the nameserver which connected to \
             is correct. Cluster: {}",
            cluster_name
        )))
    }

    pub fn fetch_broker_name_by_addr(cluster_info: &ClusterInfo, broker_addr: &str) -> RocketMQResult<String> {
        if let Some(broker_addr_table) = &cluster_info.broker_addr_table {
            for (broker_name, broker_data) in broker_addr_table.iter() {
                for addr in broker_data.broker_addrs().values() {
                    if addr.as_str() == broker_addr {
                        return Ok(broker_name.to_string());
                    }
                }
            }
        }
        Err(RocketMQError::Internal(format!(
            "BrokerAddressResolver: Make sure the specified broker address exists. Address: {}",
            broker_addr
        )))
    }

    pub fn fetch_master_and_slave_addr_by_cluster_name(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<Vec<CheetahString>> {
        let cluster_addr_table = cluster_info.cluster_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No cluster address table available from nameserver.".into())
        })?;
        let broker_names = cluster_addr_table.get(cluster_name).ok_or_else(|| {
            RocketMQError::Internal(format!(
                "BrokerAddressResolver: Make sure the specified clusterName exists or the nameserver which connected \
                 to is correct. Cluster: {}",
                cluster_name
            ))
        })?;
        let broker_addr_table = cluster_info.broker_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No broker address table available from nameserver.".into())
        })?;

        let mut all_addrs = Vec::new();
        for broker_name in broker_names {
            if let Some(broker_data) = broker_addr_table.get(broker_name) {
                all_addrs.extend(broker_data.broker_addrs().values().cloned());
            }
        }
        Ok(all_addrs)
    }

    pub fn fetch_master_and_slave_distinguish(
        cluster_info: &ClusterInfo,
        cluster_name: &str,
    ) -> RocketMQResult<HashMap<CheetahString, Vec<CheetahString>>> {
        let mut master_and_slave_map = HashMap::new();

        let cluster_addr_table = cluster_info.cluster_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No cluster address table available from nameserver.".into())
        })?;

        let broker_names = cluster_addr_table.get(cluster_name).ok_or_else(|| {
            RocketMQError::Internal(format!(
                "BrokerAddressResolver: Make sure the specified clusterName exists or the nameserver which connected \
                 to is correct. Cluster: {}",
                cluster_name
            ))
        })?;

        let broker_addr_table = cluster_info.broker_addr_table.as_ref().ok_or_else(|| {
            RocketMQError::Internal("BrokerAddressResolver: No broker address table available from nameserver.".into())
        })?;

        for broker_name in broker_names {
            let broker_data = match broker_addr_table.get(broker_name) {
                Some(data) => data,
                None => continue,
            };

            let broker_addrs = broker_data.broker_addrs();
            if broker_addrs.is_empty() {
                continue;
            }

            let master_addr = broker_addrs.get(&Self::MASTER_ID);
            let key = if let Some(addr) = master_addr {
                master_and_slave_map.entry(addr.clone()).or_insert_with(Vec::new);
                addr.clone()
            } else {
                let placeholder = CheetahString::from_static_str(Self::NO_MASTER_PLACEHOLDER);
                master_and_slave_map.entry(placeholder.clone()).or_insert_with(Vec::new);
                placeholder
            };

            for (broker_id, addr) in broker_addrs {
                if *broker_id != Self::MASTER_ID {
                    if let Some(slaves) = master_and_slave_map.get_mut(&key) {
                        slaves.push(addr.clone());
                    }
                }
            }
        }

        Ok(master_and_slave_map)
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
    fn resolves_master_addresses_by_cluster_name() {
        let cluster_info = create_test_cluster_info();
        let result = BrokerAddressResolver::fetch_master_addr_by_cluster_name(&cluster_info, "DefaultCluster").unwrap();

        assert_eq!(result[0].as_str(), "192.168.1.1:10911");
    }

    #[test]
    fn resolves_master_and_slave_addresses_by_cluster_name() {
        let cluster_info = create_test_cluster_info();
        let result =
            BrokerAddressResolver::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, "DefaultCluster")
                .unwrap();

        assert!(result.contains(&CheetahString::from_static_str("192.168.1.1:10911")));
        assert!(result.contains(&CheetahString::from_static_str("192.168.1.2:10911")));
    }

    #[test]
    fn distinguishes_master_and_slave_addresses() {
        let cluster_info = create_test_cluster_info();
        let result =
            BrokerAddressResolver::fetch_master_and_slave_distinguish(&cluster_info, "DefaultCluster").unwrap();

        let master_addr = CheetahString::from_static_str("192.168.1.1:10911");
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&master_addr).unwrap()[0].as_str(), "192.168.1.2:10911");
    }

    #[test]
    fn distinguishes_no_master_clusters() {
        let mut broker_addr_table = HashMap::new();
        let mut broker_addrs = HashMap::new();
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
        let result =
            BrokerAddressResolver::fetch_master_and_slave_distinguish(&cluster_info, "DefaultCluster").unwrap();

        let no_master_key = CheetahString::from_static_str(BrokerAddressResolver::NO_MASTER_PLACEHOLDER);
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&no_master_key).unwrap().len(), 2);
    }
}
