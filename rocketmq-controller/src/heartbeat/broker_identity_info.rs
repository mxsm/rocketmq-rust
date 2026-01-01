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
use std::fmt;

use cheetah_string::CheetahString;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BrokerIdentityInfo {
    pub cluster_name: CheetahString,
    pub broker_name: CheetahString,
    pub broker_id: Option<u64>,
}

impl BrokerIdentityInfo {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: Option<u64>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cluster_name.trim().is_empty() && self.broker_name.trim().is_empty() && self.broker_id.is_none()
    }
}

impl fmt::Display for BrokerIdentityInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrokerIdentityInfo{{clusterName='{}', brokerName='{}', brokerId={:?}}}",
            self.cluster_name, self.broker_name, self.broker_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_identity_info_new() {
        let info = BrokerIdentityInfo::new("test_cluster", "test_broker", Some(0));
        assert_eq!(info.cluster_name, "test_cluster");
        assert_eq!(info.broker_name, "test_broker");
        assert_eq!(info.broker_id, Some(0));

        let info = BrokerIdentityInfo::new("cluster1", "broker1", None);
        assert_eq!(info.cluster_name, "cluster1");
        assert_eq!(info.broker_name, "broker1");
        assert_eq!(info.broker_id, None);
    }

    #[test]
    fn broker_identity_info_is_empty() {
        let info = BrokerIdentityInfo::new("", "", None);
        assert!(info.is_empty());

        let info = BrokerIdentityInfo::new("  ", "  ", None);
        assert!(info.is_empty());

        let info = BrokerIdentityInfo::new("cluster", "broker", Some(1));
        assert!(!info.is_empty());

        let info = BrokerIdentityInfo::new("cluster", "", None);
        assert!(!info.is_empty());
    }

    #[test]
    fn broker_identity_info_clone_and_equality() {
        let info1 = BrokerIdentityInfo::new("test_cluster", "test_broker", Some(100));
        let info2 = info1.clone();
        assert_eq!(info1, info2);
    }

    #[test]
    fn broker_identity_info_display() {
        let info = BrokerIdentityInfo::new("test_cluster", "test_broker", Some(0));
        let display_str = format!("{}", info);
        assert!(display_str.contains("test_cluster"));
        assert!(display_str.contains("test_broker"));
        assert!(display_str.contains("Some(0)"));

        let info = BrokerIdentityInfo::new("cluster", "broker", None);
        let display_str = format!("{}", info);
        let expected = "BrokerIdentityInfo{clusterName='cluster', brokerName='broker', brokerId=None}";
        assert_eq!(display_str, expected);
    }
}
