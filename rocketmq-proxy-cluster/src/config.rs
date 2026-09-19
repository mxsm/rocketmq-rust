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
    /// Number of registered command ordering keys.
    pub active_keys: usize,
    /// Number of running tasks that service ordering lanes.
    pub active_lane_tasks: usize,
    /// Number of queued and active commands across regular and long-poll budgets.
    pub queued_and_active: usize,
    /// Bytes retained by queued and active commands across both budgets.
    pub retained_bytes: usize,
    /// Number of queued and active long-poll commands.
    pub long_poll_queued_and_active: usize,
    /// Bytes retained by queued and active long-poll commands.
    pub long_poll_retained_bytes: usize,
    /// Age of the oldest queued command in milliseconds, or `None` if no lane is queued.
    pub oldest_queued_age_ms: Option<u64>,
    /// Number of commands holding an I/O permit, including long polls.
    pub current_inflight: usize,
    /// Highest observed number of commands concurrently holding I/O permits.
    pub max_inflight: usize,
    /// Number of long-poll I/O permits currently held.
    pub current_long_poll_inflight: usize,
    /// Configured limit on concurrent long-poll I/O operations.
    pub long_poll_max_inflight: usize,
    /// Cumulative number of commands successfully admitted to a lane queue.
    pub admitted: u64,
    /// Cumulative number of commands rejected by queue admission.
    pub rejected: u64,
    /// Cumulative number of recorded queue-age and request-deadline timeouts.
    pub timed_out: u64,
    /// Cumulative number of commands cancelled by their callers.
    pub cancelled: u64,
    /// Cumulative number of commands rejected while the executor shuts down.
    pub shutdown_rejected: u64,
    /// Whether executor admission has been closed.
    pub closed: bool,
}

/// Configuration owned by the Client-backed cluster adapter.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct ClusterConfig {
    /// Semicolon-separated NameServer addresses; `None` leaves client discovery in effect.
    pub namesrv_addr: Option<String>,
    /// Broker cluster used to select a broker for metadata queries.
    pub broker_cluster_name: String,
    /// Instance name assigned to the backing RocketMQ client.
    pub instance_name: String,
    /// Default timeout for client API requests, in milliseconds.
    pub mq_client_api_timeout_ms: u64,
    /// Broker-side queue assignment strategy name.
    pub query_assignment_strategy_name: String,
    /// Prefix used to generate proxy producer group names.
    pub producer_group_prefix: String,
    /// Default timeout for sending messages, in milliseconds.
    pub send_message_timeout_ms: u64,
    /// Time cached topic routes remain fresh, in milliseconds.
    pub route_cache_ttl_ms: u64,
    /// Time cached topic, subscription, user, and ACL metadata remain fresh, in milliseconds.
    pub metadata_cache_ttl_ms: u64,
    /// Maximum time allowed for adapter shutdown, in milliseconds.
    pub shutdown_timeout_ms: u64,
    /// Maximum queued and active command count for each regular and long-poll budget.
    pub command_queue_capacity: usize,
    /// Maximum retained command bytes for each regular and long-poll budget.
    pub command_queue_max_bytes: usize,
    /// Maximum time a command may remain queued, in milliseconds.
    pub command_queue_max_age_ms: u64,
    /// Maximum concurrent regular I/O operations, including control commands.
    pub io_max_inflight: usize,
    /// Command and regular I/O slots reserved for control operations, with proportional byte capacity.
    pub control_reserve: usize,
    /// Maximum concurrent long-poll I/O operations, separate from regular I/O permits.
    pub long_poll_max_inflight: usize,
    /// Time an empty execution lane waits before retiring, in milliseconds.
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
            long_poll_max_inflight: 256,
            execution_lane_idle_timeout_ms: 30_000,
        }
    }
}

impl ClusterConfig {
    /// Returns [`Self::route_cache_ttl_ms`] as a [`Duration`].
    pub fn route_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.route_cache_ttl_ms)
    }

    /// Returns [`Self::metadata_cache_ttl_ms`] as a [`Duration`].
    pub fn metadata_cache_ttl(&self) -> Duration {
        Duration::from_millis(self.metadata_cache_ttl_ms)
    }

    /// Returns [`Self::shutdown_timeout_ms`] as a [`Duration`].
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }

    /// Returns [`Self::command_queue_max_age_ms`] as a [`Duration`].
    pub fn command_queue_max_age(&self) -> Duration {
        Duration::from_millis(self.command_queue_max_age_ms)
    }

    /// Returns [`Self::execution_lane_idle_timeout_ms`] as a [`Duration`].
    pub fn execution_lane_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.execution_lane_idle_timeout_ms)
    }
}
