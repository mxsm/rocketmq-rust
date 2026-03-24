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
use std::collections::BTreeMap;
use std::fmt;

pub(crate) type MessageResult<T> = Result<T, MessageError>;

#[derive(Debug)]
pub(crate) enum MessageError {
    Configuration(String),
    Validation(String),
    RocketMQ(String),
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "Configuration error: {message}"),
            Self::Validation(message) => write!(f, "Validation error: {message}"),
            Self::RocketMQ(message) => write!(f, "RocketMQ error: {message}"),
        }
    }
}

impl std::error::Error for MessageError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageSummaryView {
    pub(crate) topic: String,
    pub(crate) msg_id: String,
    pub(crate) query_msg_id: String,
    pub(crate) tags: Option<String>,
    pub(crate) keys: Option<String>,
    pub(crate) store_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageSummaryListResponse {
    pub(crate) items: Vec<MessageSummaryView>,
    pub(crate) total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessagePageView {
    pub(crate) content: Vec<MessageSummaryView>,
    pub(crate) number: u32,
    pub(crate) size: u32,
    pub(crate) total_elements: usize,
    pub(crate) total_pages: u32,
    pub(crate) number_of_elements: usize,
    pub(crate) first: bool,
    pub(crate) last: bool,
    pub(crate) empty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessagePageResponse {
    pub(crate) page: MessagePageView,
    pub(crate) task_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageResendResult {
    pub(crate) success: bool,
    pub(crate) message: String,
    pub(crate) consumer_group: String,
    pub(crate) topic: String,
    pub(crate) msg_id: String,
    pub(crate) consume_result: Option<String>,
    pub(crate) remark: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageBatchResendResponse {
    pub(crate) items: Vec<MessageResendResult>,
    pub(crate) total: usize,
    pub(crate) success_count: usize,
    pub(crate) failure_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct MessageTrackView {
    pub(crate) consumer_group: String,
    pub(crate) track_type: String,
    pub(crate) exception_desc: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct MessageDetailView {
    pub(crate) topic: String,
    pub(crate) msg_id: String,
    pub(crate) born_host: Option<String>,
    pub(crate) store_host: Option<String>,
    pub(crate) born_timestamp: Option<i64>,
    pub(crate) store_timestamp: Option<i64>,
    pub(crate) queue_id: Option<i32>,
    pub(crate) queue_offset: Option<i64>,
    pub(crate) store_size: Option<i32>,
    pub(crate) reconsume_times: Option<i32>,
    pub(crate) body_crc: Option<u32>,
    pub(crate) sys_flag: Option<i32>,
    pub(crate) flag: Option<i32>,
    pub(crate) prepared_transaction_offset: Option<i64>,
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) body_text: Option<String>,
    pub(crate) body_base64: Option<String>,
    pub(crate) message_track_list: Option<Vec<MessageTrackView>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageTraceNodeView {
    pub(crate) trace_type: String,
    pub(crate) role: String,
    pub(crate) group_name: String,
    pub(crate) client_host: String,
    pub(crate) store_host: String,
    pub(crate) timestamp: i64,
    pub(crate) cost_time: i32,
    pub(crate) status: String,
    pub(crate) retry_times: i32,
    pub(crate) from_transaction_check: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageTraceConsumerGroupView {
    pub(crate) consumer_group: String,
    pub(crate) nodes: Vec<MessageTraceNodeView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MessageTraceDetailView {
    pub(crate) msg_id: String,
    pub(crate) trace_topic: String,
    pub(crate) topic: Option<String>,
    pub(crate) tags: Option<String>,
    pub(crate) keys: Option<String>,
    pub(crate) store_host: Option<String>,
    pub(crate) producer_group: Option<String>,
    pub(crate) producer_client_host: Option<String>,
    pub(crate) producer_store_host: Option<String>,
    pub(crate) producer_timestamp: Option<i64>,
    pub(crate) producer_cost_time: Option<i32>,
    pub(crate) producer_status: Option<String>,
    pub(crate) producer_trace_type: Option<String>,
    pub(crate) min_timestamp: Option<i64>,
    pub(crate) max_timestamp: Option<i64>,
    pub(crate) total_span_ms: Option<i64>,
    pub(crate) timeline: Vec<MessageTraceNodeView>,
    pub(crate) consumer_groups: Vec<MessageTraceConsumerGroupView>,
    pub(crate) transaction_checks: Vec<MessageTraceNodeView>,
}
