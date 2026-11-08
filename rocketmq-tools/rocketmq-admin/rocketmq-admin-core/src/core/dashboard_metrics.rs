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

//! Read-only Dashboard evidence without legacy missing-rate defaults.

use super::AdminFuture;

/// Upper bound on rate candidates per refresh; offset sampling selects twenty of these.
pub const DASHBOARD_RATE_CANDIDATES: usize = 200;

/// Uses the same system, retry and DLQ categories as the detailed Topic catalog.
pub fn is_dashboard_application_topic(topic: &str) -> bool {
    !rocketmq_model::topic::is_system_topic(topic) && !topic.starts_with("%RETRY%") && !topic.starts_with("%DLQ%")
}

/// One Topic's rates. A failed target makes that aggregate unavailable, not zero.
#[derive(Clone, Debug, PartialEq)]
pub struct DashboardTopicRates {
    pub topic: String,
    pub produced_messages_24h: Option<u64>,
    pub produce_tps: Option<f64>,
    pub consume_tps: Option<f64>,
    pub failed_targets: usize,
}

/// Complete discovery and consumption evidence for the overview's aggregate backlog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DashboardConsumerSummary {
    pub group_count: Option<usize>,
    pub complete_backlog: Option<i64>,
    pub failed_targets: usize,
}

/// Concurrent query capability; it does not acquire or expose a mutation session.
pub trait DashboardMetricsAdmin: Send + Sync {
    /// Lists registered names with one NameServer inventory call and no per-Topic route calls.
    fn dashboard_topic_names(&self) -> AdminFuture<'_, Vec<String>>;

    /// Reads at most two hundred candidate Topics and two thousand target RPCs.
    ///
    /// Candidates beyond the limit are not queried. Each returned Topic retains its own failures.
    fn dashboard_topic_rates<'a>(&'a self, topics: &'a [String]) -> AdminFuture<'a, Vec<DashboardTopicRates>>;

    /// Returns a backlog only if discovery and every Broker observation are complete.
    fn dashboard_consumer_summary(&self) -> AdminFuture<'_, DashboardConsumerSummary>;
}
