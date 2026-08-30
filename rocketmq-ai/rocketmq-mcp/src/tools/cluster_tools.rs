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
