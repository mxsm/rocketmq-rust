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

use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

/// Low-cardinality runtime state for the bounded Cluster command executor.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterExecutionDiagnostics {
    pub active_keys: usize,
    pub active_lane_tasks: usize,
    pub queued_and_active: usize,
    pub retained_bytes: usize,
    pub oldest_queued_age_ms: Option<u64>,
    pub current_inflight: usize,
    pub max_inflight: usize,
    pub admitted: u64,
    pub rejected: u64,
    pub timed_out: u64,
    pub cancelled: u64,
    pub shutdown_rejected: u64,
    pub closed: bool,
}

/// Configuration owned by the Client-backed cluster adapter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ClusterConfig {
    pub namesrv_addr: Option<String>,
    pub broker_cluster_name: String,
    pub instance_name: String,
    pub mq_client_api_timeout_ms: u64,
    pub query_assignment_strategy_name: String,
    pub producer_group_prefix: String,
    pub send_message_timeout_ms: u64,
    pub route_cache_ttl_ms: u64,
    pub metadata_cache_ttl_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub command_queue_capacity: usize,
    pub command_queue_max_bytes: usize,
    pub command_queue_max_age_ms: u64,
    pub io_max_inflight: usize,
    pub control_reserve: usize,
    pub execution_lane_idle_timeout_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            namesrv_addr: None,
            broker_cluster_name: "DefaultCluster".to_owned(),
            instance_name: "rocketmq-proxy-cluster".to_owned(),
            mq_client_api_timeout_ms: 3_000,
            query_assignment_strategy_name: "AVG".to_owned(),
            producer_group_prefix: "PROXY_SEND".to_owned(),
            send_message_timeout_ms: 3_000,
            route_cache_ttl_ms: 5_000,
            metadata_cache_ttl_ms: 5_000,
            shutdown_timeout_ms: 5_000,
            command_queue_capacity: 1_024,
            command_queue_max_bytes: 64 * 1024 * 1024,
            command_queue_max_age_ms: 30_000,
            io_max_inflight: 16,
            control_reserve: 2,
            execution_lane_idle_timeout_ms: 30_000,
        }
    }
}

impl ClusterConfig {
    pub fn route_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.route_cache_ttl_ms)
    }

    pub fn metadata_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.metadata_cache_ttl_ms)
    }

    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }

    pub fn command_queue_max_age(&self) -> Duration {
        Duration::from_millis(self.command_queue_max_age_ms)
    }

    pub fn execution_lane_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.execution_lane_idle_timeout_ms)
    }
}
