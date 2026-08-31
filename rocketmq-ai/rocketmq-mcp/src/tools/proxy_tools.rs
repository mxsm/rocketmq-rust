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
pub struct ProxyDrainStateArgs {
    pub cluster: String,
    pub proxy_name: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyDrainPhase {
    Accepting,
    Draining,
    Drained,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ProxyDrainPending {
    pub active_connections: usize,
    pub sessions: usize,
    pub receipt_handles: usize,
    pub prepared_transactions: usize,
    pub telemetry_links: usize,
    pub remoting_channels: usize,
    pub telemetry_commands: usize,
    pub rpc_in_flight: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ProxyDrainStateOutput {
    pub cluster: String,
    pub proxy_name: String,
    pub state_schema_version: String,
    pub phase: ProxyDrainPhase,
    pub operation_id: Option<String>,
    pub admission_open: bool,
    pub routing_open: bool,
    pub readiness_published: bool,
    pub zero_pending: bool,
    pub pending: ProxyDrainPending,
}
