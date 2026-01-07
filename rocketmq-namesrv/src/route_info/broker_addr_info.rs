use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_remoting::net::channel::ChannelId;
use rocketmq_remoting::protocol::DataVersion;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct BrokerAddrInfo {
    // mq cluster name
    #[serde(rename = "clusterName")]
    pub cluster_name: CheetahString,
    // broker ip address
    #[serde(rename = "brokerAddr")]
    pub broker_addr: CheetahString,
}

impl BrokerAddrInfo {
    pub fn new(cluster_name: impl Into<CheetahString>, broker_addr: impl Into<CheetahString>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_addr: broker_addr.into(),
        }
    }
}

impl AsRef<Self> for BrokerAddrInfo {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Display for BrokerAddrInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cluster Name: {}, Broker Address: {}",
            self.cluster_name, self.broker_addr
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BrokerStatusChangeInfo {
    pub(crate) broker_addrs: HashMap<u64, CheetahString>,
    pub(crate) offline_broker_addr: CheetahString,
    pub(crate) ha_broker_addr: CheetahString,
}

impl BrokerStatusChangeInfo {
    fn new(
        broker_addrs: HashMap<u64, CheetahString>,
        offline_broker_addr: CheetahString,
        ha_broker_addr: CheetahString,
    ) -> Self {
        BrokerStatusChangeInfo {
            broker_addrs,
            offline_broker_addr,
            ha_broker_addr,
        }
    }

    fn get_broker_addrs(&self) -> &HashMap<u64, CheetahString> {
        &self.broker_addrs
    }

    fn set_broker_addrs(&mut self, broker_addrs: HashMap<u64, CheetahString>) {
        self.broker_addrs = broker_addrs;
    }

    fn get_offline_broker_addr(&self) -> &CheetahString {
        &self.offline_broker_addr
    }

    fn set_offline_broker_addr(&mut self, offline_broker_addr: CheetahString) {
        self.offline_broker_addr = offline_broker_addr;
    }

    fn get_ha_broker_addr(&self) -> &CheetahString {
        &self.ha_broker_addr
    }

    fn set_ha_broker_addr(&mut self, ha_broker_addr: CheetahString) {
        self.ha_broker_addr = ha_broker_addr;
    }
}

impl Display for BrokerStatusChangeInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Broker Addresses: {:?}, Offline Broker Address: {}, HA Broker Address: {}",
            self.broker_addrs, self.offline_broker_addr, self.ha_broker_addr
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BrokerLiveInfo {
    pub last_update_timestamp: i64,
    pub heartbeat_timeout_millis: i64,
    pub data_version: DataVersion,
    pub ha_server_addr: CheetahString,
    pub remote_addr: SocketAddr,
    pub channel_id: ChannelId,
}

impl BrokerLiveInfo {
    pub fn new(
        last_update_timestamp: i64,
        heartbeat_timeout_millis: i64,
        data_version: DataVersion,
        ha_server_addr: CheetahString,
        remote_addr: SocketAddr,
        channel_id: ChannelId,
    ) -> Self {
        Self {
            last_update_timestamp,
            heartbeat_timeout_millis,
            data_version,
            ha_server_addr,
            remote_addr,
            channel_id,
        }
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn last_update_timestamp(&self) -> i64 {
        self.last_update_timestamp
    }

    pub fn heartbeat_timeout_millis(&self) -> i64 {
        self.heartbeat_timeout_millis
    }

    pub fn ha_server_addr(&self) -> &CheetahString {
        &self.ha_server_addr
    }

    pub fn channel_id(&self) -> &ChannelId {
        &self.channel_id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    use rocketmq_common::utils::correlation_id_util::CorrelationIdUtil;

    use super::*;

    #[test]
    fn broker_addr_info_display_format() {
        let broker_info = BrokerAddrInfo::new("TestCluster", "192.168.1.1");
        assert_eq!(
            format!("{}", broker_info),
            "Cluster Name: TestCluster, Broker Address: 192.168.1.1"
        );
    }

    #[test]
    fn broker_status_change_info_display_format() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(1, "192.168.1.1".into());
        let broker_status_info = BrokerStatusChangeInfo::new(broker_addrs, "192.168.1.2".into(), "192.168.1.3".into());
        assert_eq!(
            format!("{}", broker_status_info),
            "Broker Addresses: {1: \"192.168.1.1\"}, Offline Broker Address: 192.168.1.2, HA Broker Address: \
             192.168.1.3"
        );
    }

    #[test]
    fn broker_live_info_properties() {
        let data_version = DataVersion::new();
        let broker_live_info = BrokerLiveInfo::new(
            1000,
            2000,
            data_version.clone(),
            "192.168.1.4".into(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)), 8080),
            CorrelationIdUtil::create_correlation_id().into(),
        );
        assert_eq!(broker_live_info.last_update_timestamp(), 1000);
        assert_eq!(broker_live_info.heartbeat_timeout_millis(), 2000);
        assert_eq!(broker_live_info.data_version(), &data_version);
        assert_eq!(broker_live_info.ha_server_addr(), "192.168.1.4");
    }
}
