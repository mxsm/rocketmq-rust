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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerConnectionItem {
    pub(crate) client_id: String,
    pub(crate) client_addr: String,
    pub(crate) language: String,
    pub(crate) version: i32,
    pub(crate) version_desc: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerSubscriptionItem {
    pub(crate) topic: String,
    pub(crate) sub_string: String,
    pub(crate) expression_type: String,
    pub(crate) tags_set: Vec<String>,
    pub(crate) code_set: Vec<i32>,
    pub(crate) sub_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerConnectionView {
    pub(crate) consumer_group: String,
    pub(crate) connection_count: usize,
    pub(crate) consume_type: String,
    pub(crate) message_model: String,
    pub(crate) consume_from_where: String,
    pub(crate) connections: Vec<ConsumerConnectionItem>,
    pub(crate) subscriptions: Vec<ConsumerSubscriptionItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerTopicDetailQueueItem {
    pub(crate) broker_name: String,
    pub(crate) queue_id: i32,
    pub(crate) broker_offset: i64,
    pub(crate) consumer_offset: i64,
    pub(crate) diff_total: i64,
    pub(crate) client_info: String,
    pub(crate) last_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerTopicDetailItem {
    pub(crate) topic: String,
    pub(crate) diff_total: i64,
    pub(crate) last_timestamp: i64,
    pub(crate) queue_stat_info_list: Vec<ConsumerTopicDetailQueueItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConsumerTopicDetailView {
    pub(crate) consumer_group: String,
    pub(crate) topic_count: usize,
    pub(crate) total_diff: i64,
    pub(crate) topics: Vec<ConsumerTopicDetailItem>,
}
