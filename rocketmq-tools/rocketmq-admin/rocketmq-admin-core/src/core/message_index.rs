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

//! Typed, body-free contracts for what Brokers index under a message key: the messages that
//! carry a key, and the trace records of one message.

use serde::Deserialize;
use serde::Serialize;

use crate::core::error::required;
use crate::core::message::MessageMetadata;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Maximum messages one key search returns.
pub const MAX_MESSAGE_KEY_MATCHES: usize = 32;
/// Longest store-time window one key search covers, in milliseconds: 24 hours.
pub const MAX_MESSAGE_KEY_WINDOW_MILLIS: i64 = 24 * 60 * 60 * 1000;
/// Longest message key or message identifier a search accepts, in bytes.
pub const MAX_MESSAGE_KEY_BYTES: usize = 255;
/// Maximum trace records one trace query returns.
pub const MAX_MESSAGE_TRACE_RECORDS: usize = 128;
/// Maximum keys retained from one trace record.
pub const MAX_MESSAGE_TRACE_KEYS: usize = 4;
/// The Topic clients write trace records to unless they are configured otherwise.
pub const DEFAULT_MESSAGE_TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";

/// Search for the messages of one Topic that carry a key, stored within a bounded time window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindMessagesByKeyRequest {
    pub cluster: String,
    pub topic: String,
    pub key: String,
    /// Store-time bounds in Unix milliseconds, both inclusive.
    pub begin_timestamp: i64,
    pub end_timestamp: i64,
}

impl FindMessagesByKeyRequest {
    /// # Errors
    ///
    /// Returns an invalid-argument error when a name is blank, when the key is longer than
    /// [`MAX_MESSAGE_KEY_BYTES`] or holds whitespace or a control character, or when the window
    /// is negative, reversed or longer than [`MAX_MESSAGE_KEY_WINDOW_MILLIS`].
    pub fn try_new(
        cluster: impl Into<String>,
        topic: impl Into<String>,
        key: impl Into<String>,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> AdminResult<Self> {
        if begin_timestamp < 0 || end_timestamp < begin_timestamp {
            return Err(AdminError::invalid_argument(
                "begin_timestamp",
                "must not be negative or after end_timestamp",
            ));
        }
        if end_timestamp - begin_timestamp > MAX_MESSAGE_KEY_WINDOW_MILLIS {
            return Err(AdminError::invalid_argument(
                "end_timestamp",
                format!("must be within {MAX_MESSAGE_KEY_WINDOW_MILLIS} milliseconds of begin_timestamp"),
            ));
        }
        Ok(Self {
            cluster: required("cluster", cluster)?,
            topic: required("topic", topic)?,
            key: index_key("key", key)?,
            begin_timestamp,
            end_timestamp,
        })
    }
}

/// One message a key search found, with the logical Broker that stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageKeyMatch {
    pub broker_name: String,
    pub metadata: MessageMetadata,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindMessagesByKeyResult {
    pub topic: String,
    /// The matches, newest first, at most [`MAX_MESSAGE_KEY_MATCHES`].
    pub matches: Vec<MessageKeyMatch>,
    /// Whether more messages match than were returned.
    pub truncated: bool,
    pub queried_broker_count: usize,
    /// Logical names of the Brokers that host the Topic and did not answer, in ascending order.
    pub unanswered_brokers: Vec<String>,
}

/// Lookup of the trace records clients wrote about one message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryMessageTraceRequest {
    pub cluster: String,
    /// The identifier the producing client gave the message.
    pub message_id: String,
    /// The trace Topic to read; [`DEFAULT_MESSAGE_TRACE_TOPIC`] when `None`.
    pub trace_topic: Option<String>,
}

impl QueryMessageTraceRequest {
    pub fn try_new(cluster: impl Into<String>, message_id: impl Into<String>) -> AdminResult<Self> {
        Ok(Self {
            cluster: required("cluster", cluster)?,
            message_id: index_key("message_id", message_id)?,
            trace_topic: None,
        })
    }

    pub fn with_trace_topic(mut self, trace_topic: impl Into<String>) -> AdminResult<Self> {
        self.trace_topic = Some(required("trace_topic", trace_topic)?);
        Ok(self)
    }
}

/// The step in the life of a message that a trace record is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageTraceStep {
    Publish,
    ConsumeStart,
    ConsumeEnd,
    EndTransaction,
    Recall,
}

/// One trace record, reduced to closed and bounded members.
///
/// Clients write trace records, so nothing in one is verified. A text member that is empty,
/// too long or holds a control character is `None`. The message identifiers, the keys and the
/// client host still require caller-owned redaction or pseudonymization before external
/// exposure. A record never holds a message body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageTraceRecord {
    pub step: MessageTraceStep,
    /// When the step happened, in Unix milliseconds.
    pub timestamp: i64,
    /// The producer or consumer group of the client that wrote the record.
    pub group: Option<String>,
    pub cost_time_ms: i32,
    pub success: bool,
    pub topic: Option<String>,
    pub message_id: String,
    pub offset_message_id: Option<String>,
    pub tags: Option<String>,
    pub keys: Vec<String>,
    /// The logical Broker the record names as the store, when it is one of the selected cluster.
    pub store_broker_name: Option<String>,
    pub client_host: Option<String>,
    pub retry_times: i32,
    pub body_length: i32,
    pub from_transaction_check: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageTraceResult {
    pub trace_topic: String,
    /// `false` when the trace Topic has no route in the selected cluster: nothing is traced there.
    pub trace_topic_found: bool,
    /// The records in ascending order of time, at most [`MAX_MESSAGE_TRACE_RECORDS`].
    pub records: Vec<MessageTraceRecord>,
    pub truncated: bool,
    pub queried_broker_count: usize,
    /// Logical names of the Brokers that host the trace Topic and did not answer.
    pub unanswered_brokers: Vec<String>,
}

/// Body-free reads of what Brokers index under a message key.
///
/// A Broker that does not answer is named in the result, and the others still answer. A read
/// fails as a whole only when no Broker answers.
pub trait MessageIndexQueryAdmin: Send {
    fn find_messages_by_key<'a>(
        &'a mut self,
        request: &'a FindMessagesByKeyRequest,
    ) -> AdminFuture<'a, FindMessagesByKeyResult>;

    fn query_message_trace<'a>(
        &'a mut self,
        request: &'a QueryMessageTraceRequest,
    ) -> AdminFuture<'a, MessageTraceResult>;
}

/// Accepts one key as a Broker indexes it. Keys are separated by a space, so a value that holds
/// whitespace names no single key.
fn index_key(field: &'static str, value: impl Into<String>) -> AdminResult<String> {
    let value = required(field, value)?;
    if value.len() > MAX_MESSAGE_KEY_BYTES {
        return Err(AdminError::invalid_argument(
            field,
            format!("must not be longer than {MAX_MESSAGE_KEY_BYTES} bytes"),
        ));
    }
    if value
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(AdminError::invalid_argument(
            field,
            "must not contain whitespace or control characters",
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 60 * 60 * 1000;

    #[test]
    fn a_key_search_is_bounded_in_time_and_takes_one_whole_key() {
        let request =
            FindMessagesByKeyRequest::try_new(" cluster-a ", " orders ", " order-42 ", HOUR, 25 * HOUR).unwrap();
        assert_eq!(
            (request.cluster.as_str(), request.topic.as_str(), request.key.as_str()),
            ("cluster-a", "orders", "order-42")
        );

        let search =
            |key: &str, begin: i64, end: i64| FindMessagesByKeyRequest::try_new("cluster-a", "orders", key, begin, end);
        assert!(search("order-42", 0, 0).is_ok());
        assert!(search("order-42", HOUR, 25 * HOUR + 1).is_err());
        assert!(search("order-42", 2 * HOUR, HOUR).is_err());
        assert!(search("order-42", -1, HOUR).is_err());
        assert!(search("order-42", 0, i64::MAX).is_err());
        assert!(search("order 42", 0, HOUR).is_err());
        assert!(search("order\u{0}42", 0, HOUR).is_err());
        assert!(search(" ", 0, HOUR).is_err());
        assert!(search(&"k".repeat(MAX_MESSAGE_KEY_BYTES), 0, HOUR).is_ok());
        assert!(search(&"k".repeat(MAX_MESSAGE_KEY_BYTES + 1), 0, HOUR).is_err());
    }

    #[test]
    fn a_trace_query_names_one_message_and_optionally_a_trace_topic() {
        let request = QueryMessageTraceRequest::try_new("cluster-a", " 7F000001000078BF ").unwrap();
        assert_eq!(request.message_id, "7F000001000078BF");
        assert_eq!(request.trace_topic, None);
        assert_eq!(
            request
                .with_trace_topic(" custom_trace ")
                .unwrap()
                .trace_topic
                .as_deref(),
            Some("custom_trace")
        );
        assert!(QueryMessageTraceRequest::try_new("cluster-a", "two ids").is_err());
        assert!(QueryMessageTraceRequest::try_new("cluster-a", " ").is_err());
        assert!(QueryMessageTraceRequest::try_new("cluster-a", "id")
            .unwrap()
            .with_trace_topic(" ")
            .is_err());
    }

    #[test]
    fn trace_steps_have_a_closed_stable_wire_form() {
        let steps = [
            MessageTraceStep::Publish,
            MessageTraceStep::ConsumeStart,
            MessageTraceStep::ConsumeEnd,
            MessageTraceStep::EndTransaction,
            MessageTraceStep::Recall,
        ];
        assert_eq!(
            serde_json::to_value(steps).unwrap(),
            serde_json::json!(["publish", "consume_start", "consume_end", "end_transaction", "recall"])
        );
    }
}
