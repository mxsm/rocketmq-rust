use std::fmt::Display;
use std::fmt::Formatter;

use cheetah_string::CheetahString;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_addr_info_display_format() {
        let broker_info = BrokerAddrInfo::new("TestCluster", "192.168.1.1");
        assert_eq!(
            format!("{}", broker_info),
            "Cluster Name: TestCluster, Broker Address: 192.168.1.1"
        );
    }
}
