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
use std::collections::HashMap;
use std::fmt;

pub(crate) type TopicResult<T> = Result<T, TopicError>;

#[derive(Debug)]
pub(crate) enum TopicError {
    Configuration(String),
    Validation(String),
    RocketMQ(String),
}

impl fmt::Display for TopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "Configuration error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
            Self::RocketMQ(message) => write!(f, "RocketMQ error: {message}"),
        }
    }
}

impl std::error::Error for TopicError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicTargetOption {
    pub(crate) cluster_name: String,
    pub(crate) broker_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicListItem {
    pub(crate) topic: String,
    pub(crate) category: String,
    pub(crate) message_type: String,
    pub(crate) clusters: Vec<String>,
    pub(crate) brokers: Vec<String>,
    pub(crate) read_queue_count: u32,
    pub(crate) write_queue_count: u32,
    pub(crate) perm: i32,
    pub(crate) order: bool,
    pub(crate) system_topic: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicListResponse {
    pub(crate) items: Vec<TopicListItem>,
    pub(crate) total: usize,
    pub(crate) targets: Vec<TopicTargetOption>,
    pub(crate) current_namesrv: String,
    pub(crate) use_vip_channel: bool,
    pub(crate) use_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicBrokerAddressView {
    pub(crate) broker_id: i64,
    pub(crate) address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicRouteBrokerView {
    pub(crate) cluster_name: String,
    pub(crate) broker_name: String,
    pub(crate) addresses: Vec<TopicBrokerAddressView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicRouteQueueView {
    pub(crate) broker_name: String,
    pub(crate) read_queue_nums: u32,
    pub(crate) write_queue_nums: u32,
    pub(crate) perm: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicRouteView {
    pub(crate) topic: String,
    pub(crate) brokers: Vec<TopicRouteBrokerView>,
    pub(crate) queues: Vec<TopicRouteQueueView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicStatusOffsetView {
    pub(crate) broker_name: String,
    pub(crate) queue_id: i32,
    pub(crate) min_offset: i64,
    pub(crate) max_offset: i64,
    pub(crate) last_update_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicStatusView {
    pub(crate) topic: String,
    pub(crate) total_message_count: i64,
    pub(crate) queue_count: usize,
    pub(crate) offsets: Vec<TopicStatusOffsetView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicConfigView {
    pub(crate) topic_name: String,
    pub(crate) broker_name: String,
    pub(crate) cluster_name: Option<String>,
    pub(crate) broker_name_list: Vec<String>,
    pub(crate) cluster_name_list: Vec<String>,
    pub(crate) read_queue_nums: i32,
    pub(crate) write_queue_nums: i32,
    pub(crate) perm: i32,
    pub(crate) order: bool,
    pub(crate) message_type: String,
    pub(crate) attributes: HashMap<String, String>,
    pub(crate) inconsistent_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicMutationResult {
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) topic_name: Option<String>,
    pub(crate) affected_queues: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicConsumerGroupListResponse {
    pub(crate) topic: String,
    pub(crate) consumer_groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicConsumerInfoView {
    pub(crate) consumer_group: String,
    pub(crate) total_diff: i64,
    pub(crate) inflight_diff: i64,
    pub(crate) consume_tps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicConsumerInfoResponse {
    pub(crate) topic: String,
    pub(crate) items: Vec<TopicConsumerInfoView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicSendMessageResult {
    pub(crate) topic: String,
    pub(crate) send_status: String,
    pub(crate) message_id: Option<String>,
    pub(crate) broker_name: Option<String>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) queue_offset: u64,
    pub(crate) transaction_id: Option<String>,
    pub(crate) region_id: Option<String>,
    pub(crate) local_transaction_state: Option<String>,
}
