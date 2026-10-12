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

//! What Brokers index under a message key: the trace records of one message, and the messages
//! of a Topic that carry a key.
//!
//! Neither workflow reads a message body. Message identifiers and keys leave only as pseudonyms,
//! and what a trace record says about hosts, tags and keys leaves only for a caller that may
//! diagnose.

use super::alias_error;
use super::normalized_identifier;
use super::normalized_logical_identifier;
use super::AdminSession;
use super::AdminSessionFactory;
use super::QueryFacade;
use crate::adapter::admin_session::SessionMessageKeyMatches;
use crate::adapter::admin_session::SessionMessageTrace;
use crate::adapter::identifier_alias::IdentifierAliaser;
use crate::guard::context::VisibilityClass;
use crate::infrastructure::snapshot::RetainedSize;
use crate::model::contract::now_millis;
use crate::model::contract::observed_at;
use crate::model::contract::observed_at_from_millis;
use crate::model::contract::CacheStatus;
use crate::model::contract::QueryCompleteness;
use crate::model::contract::QueryPayload;
use crate::model::contract::QueryResult;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;
use crate::tools::message_tools::FindMessagesByKeyArgs;
use crate::tools::message_tools::FindMessagesByKeyOutput;
use crate::tools::message_tools::MessageKeyMatch;
use crate::tools::message_tools::MessageTraceRecord;
use crate::tools::message_tools::QueryMessageTraceArgs;
use crate::tools::message_tools::QueryMessageTraceOutput;
use crate::tools::message_tools::DEFAULT_KEY_SEARCH_WINDOW_MINUTES;
use crate::tools::message_tools::MAX_KEY_SEARCH_WINDOW_MINUTES;

/// Warning of a result that misses what the Brokers in `unanswered_brokers` hold.
const BROKERS_UNANSWERED_WARNING: &str = "message_index_brokers_unanswered";
const KEY_MATCHES_TRUNCATED_WARNING: &str = "message_key_matches_truncated";
const TRACE_RECORDS_TRUNCATED_WARNING: &str = "message_trace_records_truncated";
const MAX_INDEX_KEY_BYTES: usize = 255;
const MILLIS_PER_MINUTE: i64 = 60_000;

impl RetainedSize for MessageTraceRecord {
    fn retained_heap_size(&self) -> usize {
        self.at
            .retained_heap_size()
            .saturating_add(self.group.retained_heap_size())
            .saturating_add(self.topic.retained_heap_size())
            .saturating_add(self.store_broker_name.retained_heap_size())
            .saturating_add(self.client_host_alias.retained_heap_size())
            .saturating_add(self.tags.retained_heap_size())
            .saturating_add(self.key_aliases.retained_heap_size())
    }
}

impl RetainedSize for QueryMessageTraceOutput {
    fn retained_heap_size(&self) -> usize {
        self.cluster
            .retained_heap_size()
            .saturating_add(self.unique_message_alias.retained_heap_size())
            .saturating_add(self.message_alias.retained_heap_size())
            .saturating_add(self.trace_topic.retained_heap_size())
            .saturating_add(self.records.retained_heap_size())
            .saturating_add(self.unanswered_brokers.retained_heap_size())
            .saturating_add(self.generated_at.retained_heap_size())
    }
}

fn invalid_arguments() -> ToolFailure {
    ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None })
}

/// Accepts one key as a Broker indexes it. Keys are separated by a space, so a value that holds
/// whitespace names no single key.
fn index_key(value: &str) -> Result<String, ToolFailure> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > MAX_INDEX_KEY_BYTES
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(invalid_arguments());
    }
    Ok(value.to_string())
}

/// What a result says about the Brokers that did not answer and about a cut.
fn completeness(unanswered_brokers: &[String], truncated: Option<&'static str>) -> QueryCompleteness {
    let mut warnings = Vec::new();
    if !unanswered_brokers.is_empty() {
        warnings.push(BROKERS_UNANSWERED_WARNING.to_string());
    }
    warnings.extend(truncated.map(str::to_string));
    QueryCompleteness {
        partial: !warnings.is_empty(),
        warnings,
        source_failures: Vec::new(),
    }
}

/// The store-time window of one key search, in Unix milliseconds, both bounds inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchWindow {
    begin: i64,
    end: i64,
}

impl SearchWindow {
    /// Ends the window at `end_at`, or at `now` without one, and makes it `window_minutes` long.
    fn resolve(end_at: Option<&str>, window_minutes: Option<u32>, now: i64) -> Result<Self, ToolFailure> {
        let minutes = window_minutes.unwrap_or(DEFAULT_KEY_SEARCH_WINDOW_MINUTES);
        if !(1..=MAX_KEY_SEARCH_WINDOW_MINUTES).contains(&minutes) {
            return Err(invalid_arguments());
        }
        let end = match end_at {
            Some(end_at) => chrono::DateTime::parse_from_rfc3339(end_at.trim())
                .map_err(|_| invalid_arguments())?
                .timestamp_millis(),
            None => now,
        };
        let begin = end - i64::from(minutes) * MILLIS_PER_MINUTE;
        if begin < 0 {
            return Err(invalid_arguments());
        }
        Ok(Self { begin, end })
    }
}

impl<F> QueryFacade<F>
where
    F: AdminSessionFactory,
{
    /// Reads the trace records clients wrote about one message.
    ///
    /// A record names hosts, tags and keys of a business message. They leave only for a caller
    /// that may diagnose: the host and the keys as pseudonyms, the tags as they are. The answer
    /// is kept per visibility class, so one caller never receives what was prepared for another.
    pub(crate) async fn message_trace(
        &self,
        mut args: QueryMessageTraceArgs,
    ) -> Result<QueryResult<QueryMessageTraceOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        let message_id = index_key(&args.message_id)?;
        let trace_topic = args
            .trace_topic
            .as_deref()
            .map(|trace_topic| normalized_identifier("trace_topic", trace_topic))
            .transpose()?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let unique_message_alias = self.aliases.unique_message_alias(&message_id).map_err(alias_error)?;
        let key = self.cache_key(
            "message_trace",
            &cluster.name,
            &format!(
                "message={unique_message_alias}|trace_topic={}",
                trace_topic.as_deref().unwrap_or_default()
            ),
        );
        let ttl = self.config.cache.message_metadata_ttl();
        let aliases = self.aliases.clone();
        let detailed = self.visibility_class == VisibilityClass::Sensitive;
        self.cache
            .get_or_try_init_cancellable(
                key,
                ttl,
                &self.control.cancellation,
                || ToolFailure::Rejected(ToolRejection::Cancelled),
                || async {
                    self.run_workflow(cluster, move |session, cluster| {
                        Box::pin(async move {
                            let trace = session.message_trace(&message_id, trace_topic.as_deref()).await?;
                            trace_output(&aliases, &cluster.name, unique_message_alias, trace, detailed)
                        })
                    })
                    .await
                },
            )
            .await
    }

    /// Searches one Topic for the messages that carry a key, in a bounded store-time window.
    ///
    /// The answer is read every time: a window that ends now is a new question each time it is
    /// asked, and an old answer would hide the newest matches.
    pub(crate) async fn find_messages_by_key(
        &self,
        mut args: FindMessagesByKeyArgs,
    ) -> Result<QueryResult<FindMessagesByKeyOutput>, ToolFailure> {
        args.cluster = normalized_logical_identifier("cluster", &args.cluster)?;
        args.topic = normalized_identifier("topic", &args.topic)?;
        let key = index_key(&args.key)?;
        let window = SearchWindow::resolve(args.end_at.as_deref(), args.window_minutes, now_millis())?;
        let cluster = self.resolve_cluster(&args.cluster)?;
        let aliases = self.aliases.clone();
        let topic = args.topic;
        let payload = self
            .run_workflow(cluster, move |session, cluster| {
                Box::pin(async move {
                    let found = session
                        .find_messages_by_key(&topic, &key, window.begin, window.end)
                        .await?;
                    key_search_output(&aliases, &cluster.name, topic, window, found)
                })
            })
            .await?;
        Ok(QueryResult::from_payload(
            payload,
            observed_at(),
            0,
            CacheStatus::Bypass,
        ))
    }
}

fn trace_output(
    aliases: &IdentifierAliaser,
    cluster: &str,
    unique_message_alias: String,
    trace: SessionMessageTrace,
    detailed: bool,
) -> Result<QueryPayload<QueryMessageTraceOutput>, ToolFailure> {
    let mut message_alias = None;
    let mut records = Vec::with_capacity(trace.records.len());
    for record in trace.records {
        if let (None, Some(offset_message_id)) = (&message_alias, record.offset_message_id.as_deref()) {
            message_alias = Some(aliases.message_alias(offset_message_id).map_err(alias_error)?);
        }
        let (client_host_alias, tags, key_aliases) = if detailed {
            (
                record
                    .client_host
                    .as_deref()
                    .map(|host| aliases.host_alias(host))
                    .transpose()
                    .map_err(alias_error)?,
                record.tags,
                record
                    .keys
                    .iter()
                    .map(|key| aliases.key_alias(key))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(alias_error)?,
            )
        } else {
            (None, None, Vec::new())
        };
        records.push(MessageTraceRecord {
            step: record.step,
            at: Some(record.timestamp)
                .filter(|timestamp| *timestamp > 0)
                .and_then(observed_at_from_millis),
            group: record.group,
            cost_time_ms: record.cost_time_ms,
            success: record.success,
            topic: record.topic,
            retry_times: record.retry_times,
            body_length: record.body_length,
            from_transaction_check: record.from_transaction_check,
            store_broker_name: record.store_broker_name,
            client_host_alias,
            tags,
            key_aliases,
        });
    }
    let completeness = completeness(
        &trace.unanswered_brokers,
        trace.truncated.then_some(TRACE_RECORDS_TRUNCATED_WARNING),
    );
    Ok(completeness.wrap(QueryMessageTraceOutput {
        cluster: cluster.to_string(),
        unique_message_alias,
        message_alias,
        trace_topic: trace.trace_topic,
        trace_topic_found: trace.trace_topic_found,
        redacted: !detailed,
        records,
        truncated: trace.truncated,
        queried_broker_count: trace.queried_broker_count,
        unanswered_brokers: trace.unanswered_brokers,
        generated_at: observed_at(),
    }))
}

fn key_search_output(
    aliases: &IdentifierAliaser,
    cluster: &str,
    topic: String,
    window: SearchWindow,
    found: SessionMessageKeyMatches,
) -> Result<QueryPayload<FindMessagesByKeyOutput>, ToolFailure> {
    let messages = found
        .matches
        .into_iter()
        .map(|found| {
            let metadata = found.metadata;
            Ok(MessageKeyMatch {
                message_alias: aliases.message_alias(&metadata.message_id).map_err(alias_error)?,
                unique_message_alias: metadata
                    .unique_message_id
                    .as_deref()
                    .map(|message_id| aliases.unique_message_alias(message_id))
                    .transpose()
                    .map_err(alias_error)?,
                broker_name: found.broker_name,
                born_at: observed_at_from_millis(metadata.born_timestamp),
                stored_at: observed_at_from_millis(metadata.store_timestamp),
                queue_id: metadata.queue_id,
                queue_offset: metadata.queue_offset,
                store_size: metadata.store_size,
                reconsume_times: metadata.reconsume_times,
                sys_flag: metadata.sys_flag,
                flag: metadata.flag,
                prepared_transaction_offset: metadata.prepared_transaction_offset,
            })
        })
        .collect::<Result<Vec<_>, ToolFailure>>()?;
    let completeness = completeness(
        &found.unanswered_brokers,
        found.truncated.then_some(KEY_MATCHES_TRUNCATED_WARNING),
    );
    Ok(completeness.wrap(FindMessagesByKeyOutput {
        cluster: cluster.to_string(),
        topic,
        window_begin_at: observed_at_from_millis(window.begin).ok_or_else(invalid_arguments)?,
        window_end_at: observed_at_from_millis(window.end).ok_or_else(invalid_arguments)?,
        messages,
        truncated: found.truncated,
        queried_broker_count: found.queried_broker_count,
        unanswered_brokers: found.unanswered_brokers,
        generated_at: observed_at(),
    }))
}

#[cfg(test)]
#[path = "message_index_tests.rs"]
mod tests;
