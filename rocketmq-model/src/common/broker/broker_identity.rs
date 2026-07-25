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
use serde::Deserialize;
use serde::Serialize;

use crate::common::mix_all;

const DEFAULT_CLUSTER_NAME: &str = "DefaultCluster";
const DEFAULT_BROKER_NAME: &str = "DEFAULT_BROKER";

/// Stable identity of a broker process.
///
/// This is a domain value shared by service and storage code. Runtime and
/// service-specific configuration remain in their owning crates.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BrokerIdentity {
    #[serde(default = "default_broker_name")]
    pub broker_name: CheetahString,

    #[serde(default = "default_cluster_name")]
    pub broker_cluster_name: CheetahString,

    #[serde(default = "default_broker_id")]
    pub broker_id: u64,

    #[serde(default)]
    pub is_broker_container: bool,

    #[serde(default)]
    pub is_in_broker_container: bool,
}

impl Default for BrokerIdentity {
    fn default() -> Self {
        Self::new()
    }
}

impl BrokerIdentity {
    #[must_use]
    pub fn new() -> Self {
        Self {
            broker_name: default_broker_name(),
            broker_cluster_name: default_cluster_name(),
            broker_id: default_broker_id(),
            is_broker_container: false,
            is_in_broker_container: false,
        }
    }

    #[must_use]
    pub fn get_canonical_name(&self) -> String {
        if self.is_broker_container {
            "BrokerContainer".to_string()
        } else {
            format!("{}_{}_{}", self.broker_cluster_name, self.broker_name, self.broker_id)
        }
    }
}

fn default_broker_name() -> CheetahString {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| DEFAULT_BROKER_NAME.to_string())
        .into()
}

fn default_cluster_name() -> CheetahString {
    DEFAULT_CLUSTER_NAME.into()
}

const fn default_broker_id() -> u64 {
    mix_all::MASTER_ID
}

#[cfg(test)]
mod tests {
    use super::BrokerIdentity;

    #[test]
    fn serde_layout_uses_existing_camel_case_fields() {
        let identity: BrokerIdentity = serde_json::from_str(
            r#"{"brokerName":"broker-a","brokerClusterName":"cluster-a","brokerId":7,"isBrokerContainer":false,"isInBrokerContainer":true}"#,
        )
        .expect("broker identity should deserialize");

        assert_eq!(identity.broker_name.as_str(), "broker-a");
        assert_eq!(identity.broker_cluster_name.as_str(), "cluster-a");
        assert_eq!(identity.broker_id, 7);
        assert!(identity.is_in_broker_container);
    }
}
