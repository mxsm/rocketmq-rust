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

//! Shared Cluster-domain request models for dashboard implementations.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHomePageRequest {
    pub force_refresh: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterBrokerConfigRequest {
    pub broker_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterBrokerStatusRequest {
    pub broker_addr: String,
}

#[cfg(test)]
mod tests {
    use super::ClusterBrokerConfigRequest;
    use super::ClusterBrokerStatusRequest;
    use super::ClusterHomePageRequest;

    #[test]
    fn cluster_request_uses_camel_case_fields() {
        let request = ClusterHomePageRequest { force_refresh: true };
        let json = serde_json::to_string(&request).expect("serialize cluster request");

        assert!(json.contains("\"forceRefresh\""));
    }

    #[test]
    fn cluster_broker_config_request_uses_expected_field_name() {
        let request = ClusterBrokerConfigRequest {
            broker_addr: "127.0.0.1:10911".to_string(),
        };
        let json = serde_json::to_string(&request).expect("serialize cluster broker config request");

        assert!(json.contains("\"brokerAddr\""));
    }

    #[test]
    fn cluster_broker_status_request_uses_expected_field_name() {
        let request = ClusterBrokerStatusRequest {
            broker_addr: "127.0.0.1:10911".to_string(),
        };
        let json = serde_json::to_string(&request).expect("serialize cluster broker status request");

        assert!(json.contains("\"brokerAddr\""));
    }
}
