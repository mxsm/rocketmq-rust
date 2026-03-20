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

pub(crate) type ConsumerResult<T> = Result<T, ConsumerError>;

#[derive(Debug)]
pub(crate) enum ConsumerError {
    Configuration(String),
    Validation(String),
    RocketMQ(String),
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "Configuration error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
            Self::RocketMQ(message) => write!(f, "RocketMQ error: {message}"),
        }
    }
}

impl std::error::Error for ConsumerError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerGroupListSummary {
    pub(crate) total_groups: usize,
    pub(crate) normal_groups: usize,
    pub(crate) fifo_groups: usize,
    pub(crate) system_groups: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerGroupListItem {
    pub(crate) display_group_name: String,
    pub(crate) raw_group_name: String,
    pub(crate) category: String,
    pub(crate) connection_count: usize,
    pub(crate) consume_tps: i64,
    pub(crate) diff_total: i64,
    pub(crate) message_model: String,
    pub(crate) consume_type: String,
    pub(crate) version: Option<i32>,
    pub(crate) version_desc: String,
    pub(crate) broker_addresses: Vec<String>,
    pub(crate) update_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerGroupListResponse {
    pub(crate) items: Vec<ConsumerGroupListItem>,
    pub(crate) summary: ConsumerGroupListSummary,
    pub(crate) current_namesrv: String,
    pub(crate) use_vip_channel: bool,
    pub(crate) use_tls: bool,
}
