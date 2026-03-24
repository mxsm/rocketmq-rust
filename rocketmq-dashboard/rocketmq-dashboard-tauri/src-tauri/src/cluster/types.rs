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

use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;

pub(crate) type ClusterResult<T> = Result<T, ClusterError>;

#[derive(Debug)]
pub(crate) enum ClusterError {
    Configuration(String),
    Validation(String),
    RocketMQ(String),
}

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "Configuration error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
            Self::RocketMQ(message) => write!(f, "RocketMQ error: {message}"),
        }
    }
}

impl std::error::Error for ClusterError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterOverviewSummary {
    pub(crate) total_clusters: usize,
    pub(crate) total_brokers: usize,
    pub(crate) total_masters: usize,
    pub(crate) total_slaves: usize,
    pub(crate) active_brokers: usize,
    pub(crate) inactive_brokers: usize,
    pub(crate) brokers_with_status_errors: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterBrokerCardItem {
    pub(crate) cluster_name: String,
    pub(crate) broker_name: String,
    pub(crate) broker_id: u64,
    pub(crate) role: String,
    pub(crate) address: String,
    pub(crate) version: String,
    pub(crate) produce_tps: f64,
    pub(crate) consume_tps: f64,
    pub(crate) today_received_total: i64,
    pub(crate) yesterday_produce: i64,
    pub(crate) yesterday_consume: i64,
    pub(crate) today_produce: i64,
    pub(crate) today_consume: i64,
    pub(crate) is_active: bool,
    pub(crate) status_load_error: Option<String>,
    pub(crate) raw_status: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterHomePageResponse {
    pub(crate) clusters: Vec<String>,
    pub(crate) items: Vec<ClusterBrokerCardItem>,
    pub(crate) summary: ClusterOverviewSummary,
    pub(crate) current_namesrv: String,
    pub(crate) use_vip_channel: bool,
    pub(crate) use_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterBrokerConfigView {
    pub(crate) broker_addr: String,
    pub(crate) entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterBrokerStatusView {
    pub(crate) broker_addr: String,
    pub(crate) entries: BTreeMap<String, String>,
}
