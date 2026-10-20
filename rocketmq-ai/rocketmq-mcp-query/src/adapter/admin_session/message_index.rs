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

//! What Brokers index under a message key: the messages that carry a key, and the trace records
//! of one message.
//!
//! The values here still hold message identifiers, keys and client hosts. They stay inside the
//! adapter: a workflow turns them into pseudonyms, or leaves them out, before it answers.

use rocketmq_admin_core::core::message_index::FindMessagesByKeyRequest;
use rocketmq_admin_core::core::message_index::MessageIndexQueryAdmin;
use rocketmq_admin_core::core::message_index::MessageTraceStep as AdminTraceStep;
use rocketmq_admin_core::core::message_index::QueryMessageTraceRequest;

use super::map_admin_error;
use super::AdminCoreSession;
use super::SessionMessageMetadata;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;
use crate::tools::message_tools::MessageTraceStep;

/// One message a key search found, with the logical Broker that stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionMessageKeyMatch {
    pub broker_name: String,
    pub metadata: SessionMessageMetadata,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SessionMessageKeyMatches {
    /// The matches, newest first.
    pub matches: Vec<SessionMessageKeyMatch>,
    pub truncated: bool,
    pub queried_broker_count: usize,
    pub unanswered_brokers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionMessageTraceRecord {
    pub step: MessageTraceStep,
    pub timestamp: i64,
    pub group: Option<String>,
    pub cost_time_ms: i32,
    pub success: bool,
    pub topic: Option<String>,
    pub offset_message_id: Option<String>,
    pub tags: Option<String>,
    pub keys: Vec<String>,
    pub store_broker_name: Option<String>,
    pub client_host: Option<String>,
    pub retry_times: i32,
    pub body_length: i32,
    pub from_transaction_check: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SessionMessageTrace {
    pub trace_topic: String,
    pub trace_topic_found: bool,
    /// The records in ascending order of time.
    pub records: Vec<SessionMessageTraceRecord>,
    pub truncated: bool,
    pub queried_broker_count: usize,
    pub unanswered_brokers: Vec<String>,
}

fn invalid_arguments<E>(_: E) -> ToolFailure {
    ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None })
}

impl AdminCoreSession {
    pub(super) async fn query_messages_by_key(
        &mut self,
        topic: &str,
        key: &str,
        begin_millis: i64,
        end_millis: i64,
    ) -> Result<SessionMessageKeyMatches, ToolFailure> {
        let request = FindMessagesByKeyRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            topic,
            key,
            begin_millis,
            end_millis,
        )
        .map_err(invalid_arguments)?;
        let found = self
            .admin_mut()?
            .find_messages_by_key(&request)
            .await
            .map_err(map_admin_error(NotFoundEntity::Topic))?;
        Ok(SessionMessageKeyMatches {
            matches: found
                .matches
                .into_iter()
                .map(|found| SessionMessageKeyMatch {
                    broker_name: found.broker_name,
                    metadata: SessionMessageMetadata {
                        message_id: found.metadata.message_id,
                        unique_message_id: found.metadata.unique_message_id,
                        topic: found.metadata.topic,
                        born_timestamp: found.metadata.born_timestamp,
                        store_timestamp: found.metadata.store_timestamp,
                        queue_id: found.metadata.queue_id,
                        queue_offset: found.metadata.queue_offset,
                        store_size: found.metadata.store_size,
                        reconsume_times: found.metadata.reconsume_times,
                        sys_flag: found.metadata.sys_flag,
                        flag: found.metadata.flag,
                        prepared_transaction_offset: found.metadata.prepared_transaction_offset,
                    },
                })
                .collect(),
            truncated: found.truncated,
            queried_broker_count: found.queried_broker_count,
            unanswered_brokers: found.unanswered_brokers,
        })
    }

    pub(super) async fn query_message_trace(
        &mut self,
        message_id: &str,
        trace_topic: Option<&str>,
    ) -> Result<SessionMessageTrace, ToolFailure> {
        let mut request = QueryMessageTraceRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), message_id)
            .map_err(invalid_arguments)?;
        if let Some(trace_topic) = trace_topic {
            request = request.with_trace_topic(trace_topic).map_err(invalid_arguments)?;
        }
        let trace = self
            .admin_mut()?
            .query_message_trace(&request)
            .await
            .map_err(map_admin_error(NotFoundEntity::Cluster))?;
        Ok(SessionMessageTrace {
            trace_topic: trace.trace_topic,
            trace_topic_found: trace.trace_topic_found,
            records: trace
                .records
                .into_iter()
                .map(|record| SessionMessageTraceRecord {
                    step: match record.step {
                        AdminTraceStep::Publish => MessageTraceStep::Publish,
                        AdminTraceStep::ConsumeStart => MessageTraceStep::ConsumeStart,
                        AdminTraceStep::ConsumeEnd => MessageTraceStep::ConsumeEnd,
                        AdminTraceStep::EndTransaction => MessageTraceStep::EndTransaction,
                        AdminTraceStep::Recall => MessageTraceStep::Recall,
                    },
                    timestamp: record.timestamp,
                    group: record.group,
                    cost_time_ms: record.cost_time_ms,
                    success: record.success,
                    topic: record.topic,
                    offset_message_id: record.offset_message_id,
                    tags: record.tags,
                    keys: record.keys,
                    store_broker_name: record.store_broker_name,
                    client_host: record.client_host,
                    retry_times: record.retry_times,
                    body_length: record.body_length,
                    from_transaction_check: record.from_transaction_check,
                })
                .collect(),
            truncated: trace.truncated,
            queried_broker_count: trace.queried_broker_count,
            unanswered_brokers: trace.unanswered_brokers,
        })
    }
}
