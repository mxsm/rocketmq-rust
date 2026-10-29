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

//! Topic-verified Trace reads. Missing wire observations remain optional.

use super::{message_index::MessageTraceStep, AdminFuture};

pub const MESSAGE_TRACE_NODE_LIMIT: usize = 128;
pub const MESSAGE_TRACE_BROKER_LIMIT: usize = 32;

#[derive(Clone, PartialEq, Eq)]
pub struct MessageTraceLookupRequest {
    pub topic: String,
    /// A physical or producer-assigned ID, resolved within `topic` before querying trace records.
    pub message_id: String,
    pub trace_topic: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageTraceObservation {
    pub step: MessageTraceStep,
    pub timestamp: Option<i64>,
    pub group: Option<String>,
    pub success: Option<bool>,
    /// A duration explicitly reported by the client, never an inferred edge latency.
    pub cost_time_ms: Option<u32>,
    pub client_host: Option<String>,
    pub store_host: Option<String>,
    pub offset_message_id: Option<String>,
    pub retry_times: Option<u32>,
    pub transaction_state: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageTraceReadResult {
    pub topic: String,
    pub message_id: String,
    /// The producing client's ID from the verified message; absent IDs are not replaced by offsets.
    pub producer_message_id: Option<String>,
    pub trace_topic: String,
    pub trace_topic_found: bool,
    pub nodes: Vec<MessageTraceObservation>,
    pub limit_reached: bool,
    pub unanswered_brokers: Vec<String>,
}

/// Concurrent, read-only trace lookup through the current NameServer session.
pub trait MessageTraceReadAdmin: Send + Sync {
    /// Resolves the selected message in its Topic, then reads its producer-ID index from the
    /// explicit trace Topic. Reused producer IDs can name several real sends.
    ///
    /// # Errors
    /// Returns an error for an invalid identity, failed message lookup, failed route discovery,
    /// or when no queried trace Broker answers. A successful empty trace is an ordinary result.
    fn message_trace<'a>(&'a self, request: &'a MessageTraceLookupRequest) -> AdminFuture<'a, MessageTraceReadResult>;
}
