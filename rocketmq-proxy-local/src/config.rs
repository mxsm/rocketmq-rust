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

const LOCAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_LOCAL_COMMAND_QUEUE_CAPACITY: usize = 1_024;
const DEFAULT_LOCAL_COMMAND_QUEUE_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_LOCAL_COMMAND_QUEUE_MAX_AGE_MILLIS: u64 = 1_000;
const DEFAULT_LOCAL_IO_MAX_INFLIGHT: usize = 16;
const DEFAULT_LOCAL_CONTROL_RESERVE: usize = 2;
const DEFAULT_LOCAL_LONG_POLL_MAX_INFLIGHT: usize = 256;
const DEFAULT_LOCAL_EXECUTION_LANE_IDLE_TIMEOUT_MILLIS: u64 = 30_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct LocalConfig {
    pub broker_cluster_name: String,
    pub broker_name: String,
    pub broker_ip: String,
    pub broker_listen_port: u16,
    pub store_root_dir: String,
    pub query_assignment_strategy_name: String,
    pub command_queue_capacity: usize,
    pub command_queue_max_bytes: usize,
    pub command_queue_max_age_millis: u64,
    pub io_max_inflight: usize,
    pub control_reserve: usize,
    pub long_poll_max_inflight: usize,
    pub execution_lane_idle_timeout_millis: u64,
}

impl LocalConfig {
    pub fn shutdown_timeout(&self) -> Duration {
        LOCAL_SHUTDOWN_TIMEOUT
    }

    pub fn command_queue_max_age(&self) -> Duration {
        Duration::from_millis(self.command_queue_max_age_millis)
    }

    pub fn execution_lane_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.execution_lane_idle_timeout_millis)
    }
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            broker_cluster_name: "DefaultCluster".to_owned(),
            broker_name: "rocketmq-proxy-local".to_owned(),
            broker_ip: "127.0.0.1".to_owned(),
            broker_listen_port: 10911,
            store_root_dir: "store/proxy/local-broker".to_owned(),
            query_assignment_strategy_name: "AVG".to_owned(),
            command_queue_capacity: DEFAULT_LOCAL_COMMAND_QUEUE_CAPACITY,
            command_queue_max_bytes: DEFAULT_LOCAL_COMMAND_QUEUE_MAX_BYTES,
            command_queue_max_age_millis: DEFAULT_LOCAL_COMMAND_QUEUE_MAX_AGE_MILLIS,
            io_max_inflight: DEFAULT_LOCAL_IO_MAX_INFLIGHT,
            control_reserve: DEFAULT_LOCAL_CONTROL_RESERVE,
            long_poll_max_inflight: DEFAULT_LOCAL_LONG_POLL_MAX_INFLIGHT,
            execution_lane_idle_timeout_millis: DEFAULT_LOCAL_EXECUTION_LANE_IDLE_TIMEOUT_MILLIS,
        }
    }
}
