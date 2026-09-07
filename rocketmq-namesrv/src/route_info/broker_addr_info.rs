// Copyright 2026 The RocketMQ Rust Authors
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

use std::fmt::Display;
use std::fmt::Formatter;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
/// Identifies a broker in NameServer route tables by its cluster and address.
pub struct BrokerAddrInfo {
    /// The broker's cluster name, serialized as `clusterName`.
    #[serde(rename = "clusterName")]
    pub cluster_name: CheetahString,
    /// The broker address, serialized as `brokerAddr`.
    #[serde(rename = "brokerAddr")]
    pub broker_addr: CheetahString,
}

impl BrokerAddrInfo {
    /// Creates an identity from a cluster name and broker address.
    pub fn new(cluster_name: impl Into<CheetahString>, broker_addr: impl Into<CheetahString>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_addr: broker_addr.into(),
        }
    }
}

impl AsRef<Self> for BrokerAddrInfo {
    /// Returns this broker identity by reference.
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Display for BrokerAddrInfo {
    /// Formats the cluster name and broker address for display.
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
