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

use serde::Deserialize;
use serde::Serialize;
use std::fmt;

pub(crate) type DashboardResult<T> = Result<T, DashboardError>;

#[derive(Debug)]
pub(crate) enum DashboardError {
    Cluster(String),
    Topic(String),
}

impl fmt::Display for DashboardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster(message) => write!(f, "Cluster error: {message}"),
            Self::Topic(message) => write!(f, "Topic error: {message}"),
        }
    }
}

impl std::error::Error for DashboardError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardBrokerSummary {
    pub(crate) total_clusters: usize,
    pub(crate) total_brokers: usize,
    pub(crate) total_masters: usize,
    pub(crate) total_slaves: usize,
    pub(crate) active_brokers: usize,
    pub(crate) inactive_brokers: usize,
    pub(crate) brokers_with_status_errors: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardBrokerTopItem {
    pub(crate) cluster_name: String,
    pub(crate) broker_name: String,
    pub(crate) broker_id: u64,
    pub(crate) address: String,
    pub(crate) received_total: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardBrokerTpsItem {
    pub(crate) cluster_name: String,
    pub(crate) broker_name: String,
    pub(crate) broker_id: u64,
    pub(crate) address: String,
    pub(crate) produce_tps: f64,
    pub(crate) consume_tps: f64,
    pub(crate) total_tps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardBrokerOverviewResponse {
    pub(crate) current_namesrv: String,
    pub(crate) use_vip_channel: bool,
    pub(crate) use_tls: bool,
    pub(crate) summary: DashboardBrokerSummary,
    pub(crate) broker_top: Vec<DashboardBrokerTopItem>,
    pub(crate) broker_tps: Vec<DashboardBrokerTpsItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardTopicQueueItem {
    pub(crate) topic: String,
    pub(crate) category: String,
    pub(crate) read_queue_count: u32,
    pub(crate) write_queue_count: u32,
    pub(crate) total_queue_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardTopicCategoryItem {
    pub(crate) category: String,
    pub(crate) count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DashboardTopicCurrentResponse {
    pub(crate) current_namesrv: String,
    pub(crate) use_vip_channel: bool,
    pub(crate) use_tls: bool,
    pub(crate) total_topics: usize,
    pub(crate) topic_queue_top: Vec<DashboardTopicQueueItem>,
    pub(crate) topic_category_distribution: Vec<DashboardTopicCategoryItem>,
}
