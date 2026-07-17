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

//! Message query, paging, trace, and direct-consume contracts.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;

use crate::core::queue::QueueRef;
use crate::core::AdminFuture;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRecord {
    pub topic: String,
    /// Offset message identifier used by direct broker lookup.
    pub message_id: String,
    /// Client-assigned unique identifier, when present.
    pub unique_message_id: Option<String>,
    pub keys: Option<String>,
    pub tags: Option<String>,
    pub born_timestamp: i64,
    pub store_timestamp: i64,
    pub born_host: String,
    pub store_host: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub store_size: i32,
    pub reconsume_times: i32,
    pub body_crc: u32,
    pub sys_flag: i32,
    pub flag: i32,
    pub prepared_transaction_offset: i64,
    pub body: Vec<u8>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessagesByKeyRequest {
    pub topic: String,
    pub key: String,
    pub max_messages: i32,
    pub begin: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessagesResult {
    pub messages: Vec<MessageRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageLookupRequest {
    pub topic: String,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageTrackRecord {
    pub consumer_group: String,
    pub track_type: String,
    pub exception_desc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageDetailRecord {
    pub message: MessageRecord,
    pub tracks: Vec<MessageTrackRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageQueueRange {
    pub broker_addr: String,
    pub queue: QueueRef,
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageQueuePlan {
    pub topic_exists: bool,
    pub queues: Vec<MessageQueueRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageQueuePlanRequest {
    pub topic: String,
    pub begin: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessagePullStatus {
    Found,
    NoMatchedMsg,
    NoNewMsg,
    OffsetIllegal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullMessagesRequest {
    pub broker_addr: String,
    pub queue: QueueRef,
    pub offset: i64,
    pub max_messages: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullMessagesResult {
    pub status: MessagePullStatus,
    pub next_begin_offset: i64,
    pub messages: Vec<MessageRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectConsumeRequest {
    pub topic: String,
    pub consumer_group: String,
    pub message_id: String,
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectConsumeResult {
    pub success: bool,
    pub consume_result: Option<String>,
    pub remark: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqMessageLookupRequest {
    pub consumer_group: String,
    pub message_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqResendResult {
    pub topic: String,
    pub message_id: String,
    pub consume: DirectConsumeResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceQueryRequest {
    pub message_id: String,
    pub trace_topic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSeed {
    pub trace_type: String,
    pub group_name: String,
    pub client_host: String,
    pub store_host: String,
    pub timestamp: i64,
    pub cost_time: i32,
    pub status: String,
    pub topic: Option<String>,
    pub tags: Option<String>,
    pub keys: Option<String>,
    pub retry_times: i32,
    pub from_transaction_check: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceData {
    pub message_id: String,
    pub trace_topic: String,
    pub seeds: Vec<TraceSeed>,
}

pub trait MessageAdmin: Send {
    fn query_messages_by_key<'a>(
        &'a mut self,
        request: &'a QueryMessagesByKeyRequest,
    ) -> AdminFuture<'a, QueryMessagesResult>;

    fn find_message<'a>(&'a mut self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageRecord>;

    fn message_detail<'a>(&'a mut self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageDetailRecord>;

    fn message_queue_plan<'a>(&'a mut self, request: &'a MessageQueuePlanRequest) -> AdminFuture<'a, MessageQueuePlan>;

    fn pull_messages<'a>(&'a mut self, request: &'a PullMessagesRequest) -> AdminFuture<'a, PullMessagesResult>;

    fn consume_message_directly<'a>(
        &'a mut self,
        request: &'a DirectConsumeRequest,
    ) -> AdminFuture<'a, DirectConsumeResult>;

    fn find_dlq_message<'a>(&'a mut self, request: &'a DlqMessageLookupRequest) -> AdminFuture<'a, MessageRecord>;

    fn resend_dlq_message<'a>(&'a mut self, request: &'a DlqMessageLookupRequest) -> AdminFuture<'a, DlqResendResult>;

    fn query_trace_data<'a>(&'a mut self, request: &'a TraceQueryRequest) -> AdminFuture<'a, TraceData>;
}

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::message::*;
