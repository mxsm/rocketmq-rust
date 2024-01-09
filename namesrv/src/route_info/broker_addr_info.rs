use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use rocketmq_remoting::protocol::DataVersion;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) struct BrokerAddrInfo {
    // mq cluster name
    #[serde(rename = "clusterName")]
    pub(crate) cluster_name: String,
    // broker ip address
    #[serde(rename = "brokerAddr")]
    pub(crate) broker_addr: String,
}

impl BrokerAddrInfo {
    pub fn new(cluster_name: impl Into<String>, broker_addr: impl Into<String>) -> Self {
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
    broker_addrs: HashMap<i64, String>,
    offline_broker_addr: String,
    ha_broker_addr: String,
}

impl BrokerStatusChangeInfo {
    fn new(
        broker_addrs: HashMap<i64, String>,
        offline_broker_addr: String,
        ha_broker_addr: String,
    ) -> Self {
        BrokerStatusChangeInfo {
            broker_addrs,
            offline_broker_addr,
            ha_broker_addr,
        }
    }

    fn get_broker_addrs(&self) -> &HashMap<i64, String> {
        &self.broker_addrs
    }

    fn set_broker_addrs(&mut self, broker_addrs: HashMap<i64, String>) {
        self.broker_addrs = broker_addrs;
    }

    fn get_offline_broker_addr(&self) -> &String {
        &self.offline_broker_addr
    }

    fn set_offline_broker_addr(&mut self, offline_broker_addr: String) {
        self.offline_broker_addr = offline_broker_addr;
    }

    fn get_ha_broker_addr(&self) -> &String {
        &self.ha_broker_addr
    }

    fn set_ha_broker_addr(&mut self, ha_broker_addr: String) {
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
    last_update_timestamp: i64,
    heartbeat_timeout_millis: i64,
    data_version: DataVersion,
    ha_server_addr: String,
}

impl BrokerLiveInfo {
    pub fn new(
        last_update_timestamp: i64,
        heartbeat_timeout_millis: i64,
        data_version: DataVersion,
        ha_server_addr: String,
    ) -> Self {
        Self {
            last_update_timestamp,
            heartbeat_timeout_millis,
            data_version,
            ha_server_addr,
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
    pub fn ha_server_addr(&self) -> &str {
        &self.ha_server_addr
    }
}
