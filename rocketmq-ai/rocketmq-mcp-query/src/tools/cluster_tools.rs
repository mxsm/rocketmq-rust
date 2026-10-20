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

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterOverviewArgs {
    /// Logical cluster name configured on this server. It is a name, not a NameServer address.
    #[schemars(length(min = 1))]
    pub cluster: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct BrokerSummary {
    pub cluster: String,
    pub broker_name: String,
    pub broker_id: u64,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub broker_addr: String,
    pub version: String,
    pub in_tps: String,
    pub out_tps: String,
    pub timer_progress: String,
    pub page_cache_lock_time_millis: String,
    pub hour: String,
    pub space: String,
    pub broker_active: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ClusterOverviewOutput {
    pub cluster: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub namesrv_addr: String,
    pub brokers: Vec<BrokerSummary>,
    pub topic_count: usize,
    pub consumer_group_count: usize,
    pub generated_at: String,
}

/// `rocketmq_list_clusters` takes no argument: it is the one Tool that is not bound to a cluster.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListClustersArgs {}

/// One logical cluster the caller may query, with the names its other Tools accept.
///
/// It carries names only. NameServer addresses, Proxy and Controller endpoints, and the physical
/// cluster name are server configuration and never leave it.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ClusterEntry {
    /// Logical cluster name. Pass it as the `cluster` argument of every other Tool.
    pub name: String,
    /// Whether a Tool that allows `cluster` to be omitted selects this cluster.
    pub default: bool,
    /// Proxy aliases of this cluster. Pass one as `proxy_name` to rocketmq_get_proxy_drain_state.
    pub proxy_names: Vec<String>,
    /// Controller aliases of this cluster. Pass some as `controller_names` to rocketmq_get_ha_status or
    /// rocketmq_get_controller_metadata to narrow them to those Controllers.
    pub controller_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListClustersOutput {
    /// The clusters the caller may query, in configuration order. A cluster the caller has no
    /// access to is left out, so the list can be shorter than the server's configuration.
    pub clusters: Vec<ClusterEntry>,
}
