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

//! Read-client implementation of the body-free message index reads.

use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminMessageIndexReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_client_rust::MessageIndexQuery;
use rocketmq_client_rust::MessageMetadataRead;
use rocketmq_client_rust::MessageTraceRead;
use rocketmq_client_rust::TraceType;
use rocketmq_error::Error as CanonicalError;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::core::message::MessageMetadata;
use crate::core::message_index::FindMessagesByKeyRequest;
use crate::core::message_index::FindMessagesByKeyResult;
use crate::core::message_index::MessageKeyMatch;
use crate::core::message_index::MessageTraceRecord;
use crate::core::message_index::MessageTraceResult;
use crate::core::message_index::MessageTraceStep;
use crate::core::message_index::QueryMessageTraceRequest;
use crate::core::message_index::DEFAULT_MESSAGE_TRACE_TOPIC;
use crate::core::message_index::MAX_MESSAGE_KEY_BYTES;
use crate::core::message_index::MAX_MESSAGE_KEY_MATCHES;
use crate::core::message_index::MAX_MESSAGE_TRACE_KEYS;
use crate::core::message_index::MAX_MESSAGE_TRACE_RECORDS;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::AdminError;
use crate::core::AdminFailure;
use crate::core::AdminResult;
use crate::topic_observation::promote_to_admin_error;
use crate::topic_observation::selected_cluster_route_masters;
use crate::topic_observation::BrokerTarget;

/// The resource label of the error that says a Topic has no route in the selected cluster.
const ROUTE_IN_CLUSTER: &str = "topic route in selected cluster";
/// Trace messages one Broker is asked for. One trace message carries several records.
const TRACE_MESSAGES_PER_BROKER: i32 = 64;
const MAX_TRACE_TEXT_BYTES: usize = 255;

#[allow(async_fn_in_trait)]
trait MessageIndexSource: Send {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError>;
    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, CanonicalError>;
    async fn metadata_by_key(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<MessageMetadataRead>, CanonicalError>;
    async fn trace(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<MessageTraceRead>, CanonicalError>;
}

impl MessageIndexSource for DefaultMQAdminExt {
    async fn cluster_info(&self) -> Result<ClusterInfo, CanonicalError> {
        MQAdminReadExt::examine_broker_cluster_info(self)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn topic_route(&self, topic: &str) -> Result<Option<TopicRouteData>, CanonicalError> {
        MQAdminReadExt::examine_topic_route_info(self, CheetahString::from(topic))
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn metadata_by_key(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<MessageMetadataRead>, CanonicalError> {
        MQAdminMessageIndexReadExt::message_metadata_by_key_at(self, broker_addr, query)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }

    async fn trace(
        &self,
        broker_addr: CheetahString,
        query: MessageIndexQuery,
    ) -> Result<Vec<MessageTraceRead>, CanonicalError> {
        MQAdminMessageIndexReadExt::message_trace_at(self, broker_addr, query)
            .await
            .map_err(crate::IntoCanonicalError::into_canonical_error)
    }
}

pub(crate) async fn find_messages_by_key(
    admin: &DefaultMQAdminExt,
    request: &FindMessagesByKeyRequest,
) -> AdminResult<FindMessagesByKeyResult> {
    find_messages_by_key_from(admin, request).await
}

pub(crate) async fn query_message_trace(
    admin: &DefaultMQAdminExt,
    request: &QueryMessageTraceRequest,
) -> AdminResult<MessageTraceResult> {
    query_message_trace_from(admin, request).await
}

/// The masters of one Topic route in the selected cluster, and the Brokers that cannot be asked.
struct IndexTargets {
    cluster_info: ClusterInfo,
    targets: Vec<BrokerTarget>,
    unanswered: Vec<String>,
}

async fn index_targets<S: MessageIndexSource>(source: &S, cluster: &str, topic: &str) -> AdminResult<IndexTargets> {
    let cluster_info = source
        .cluster_info()
        .await
        .map_err(|source| promote_to_admin_error("examine_broker_cluster_info", source))?;
    let route = source
        .topic_route(topic)
        .await
        .map_err(|source| promote_to_admin_error("examine_topic_route_info", source))?
        .ok_or_else(|| AdminError::topic_not_found(topic))?;
    // The source only labels the targets that cannot be asked, and those are named, not labelled.
    let (targets, failures) =
        selected_cluster_route_masters(&cluster_info, &route, cluster, AdminQuerySource::TopicStats)?;
    Ok(IndexTargets {
        cluster_info,
        targets,
        unanswered: failures
            .iter()
            .map(AdminSourceFailure::logical_target)
            .map(str::to_owned)
            .collect(),
    })
}

/// Fails the read when Brokers were asked and none of them answered: an empty result would then
/// claim that nothing matches.
fn require_an_answer(operation: &'static str, answered: usize, unanswered: &mut Vec<String>) -> AdminResult<()> {
    unanswered.sort();
    unanswered.dedup();
    if answered == 0 && !unanswered.is_empty() {
        return Err(AdminError::unavailable(
            operation,
            "No Broker that hosts the Topic answered",
        ));
    }
    Ok(())
}

async fn find_messages_by_key_from<S: MessageIndexSource>(
    source: &S,
    request: &FindMessagesByKeyRequest,
) -> AdminResult<FindMessagesByKeyResult> {
    let request = FindMessagesByKeyRequest::try_new(
        &request.cluster,
        &request.topic,
        &request.key,
        request.begin_timestamp,
        request.end_timestamp,
    )?;
    let IndexTargets {
        targets,
        mut unanswered,
        ..
    } = index_targets(source, &request.cluster, &request.topic).await?;
    let queried_broker_count = targets.len().saturating_add(unanswered.len());
    // One more than is returned, so that a Broker that holds more is noticed.
    let per_broker = i32::try_from(MAX_MESSAGE_KEY_MATCHES + 1).unwrap_or(i32::MAX);

    let mut matches = Vec::new();
    let mut truncated = false;
    let mut answered = 0usize;
    for (broker_name, broker_addr) in targets {
        let query = MessageIndexQuery {
            topic: CheetahString::from(request.topic.as_str()),
            key: CheetahString::from(request.key.as_str()),
            max_messages: per_broker,
            begin_timestamp: request.begin_timestamp,
            end_timestamp: request.end_timestamp,
        };
        match source.metadata_by_key(broker_addr, query).await {
            Ok(found) => {
                answered += 1;
                truncated |= found.len() > MAX_MESSAGE_KEY_MATCHES;
                matches.extend(
                    found
                        .into_iter()
                        // A Broker selects by the time of its index entries, which is close to
                        // the store time and not the same.
                        .filter(|message| {
                            (request.begin_timestamp..=request.end_timestamp).contains(&message.store_timestamp)
                        })
                        .map(|message| MessageKeyMatch {
                            broker_name: broker_name.clone(),
                            metadata: metadata(message),
                        }),
                );
            }
            Err(_) => unanswered.push(broker_name),
        }
    }
    require_an_answer("find_messages_by_key", answered, &mut unanswered)?;

    matches.sort_by(|left, right| {
        right
            .metadata
            .store_timestamp
            .cmp(&left.metadata.store_timestamp)
            .then_with(|| left.broker_name.cmp(&right.broker_name))
            .then_with(|| left.metadata.queue_id.cmp(&right.metadata.queue_id))
            .then_with(|| left.metadata.queue_offset.cmp(&right.metadata.queue_offset))
    });
    if matches.len() > MAX_MESSAGE_KEY_MATCHES {
        matches.truncate(MAX_MESSAGE_KEY_MATCHES);
        truncated = true;
    }
    Ok(FindMessagesByKeyResult {
        topic: request.topic,
        matches,
        truncated,
        queried_broker_count,
        unanswered_brokers: unanswered,
    })
}

fn metadata(message: MessageMetadataRead) -> MessageMetadata {
    MessageMetadata {
        topic: message.topic,
        message_id: message.message_id,
        unique_message_id: message.unique_message_id,
        born_timestamp: message.born_timestamp,
        store_timestamp: message.store_timestamp,
        queue_id: message.queue_id,
        queue_offset: message.queue_offset,
        store_size: message.store_size,
        reconsume_times: message.reconsume_times,
        sys_flag: message.sys_flag,
        flag: message.flag,
        prepared_transaction_offset: message.prepared_transaction_offset,
    }
}

async fn query_message_trace_from<S: MessageIndexSource>(
    source: &S,
    request: &QueryMessageTraceRequest,
) -> AdminResult<MessageTraceResult> {
    let mut validated = QueryMessageTraceRequest::try_new(&request.cluster, &request.message_id)?;
    if let Some(trace_topic) = request.trace_topic.as_deref() {
        validated = validated.with_trace_topic(trace_topic)?;
    }
    let trace_topic = validated
        .trace_topic
        .unwrap_or_else(|| DEFAULT_MESSAGE_TRACE_TOPIC.to_owned());

    let IndexTargets {
        cluster_info,
        targets,
        mut unanswered,
    } = match index_targets(source, &validated.cluster, &trace_topic).await {
        Ok(targets) => targets,
        // A cluster in which nothing is traced has no trace Topic. That is an answer about the
        // cluster and not a failed lookup of the message.
        Err(error) if trace_topic_is_absent(&error) => {
            return Ok(MessageTraceResult {
                trace_topic,
                ..MessageTraceResult::default()
            });
        }
        Err(error) => return Err(error),
    };
    let queried_broker_count = targets.len().saturating_add(unanswered.len());
    let brokers = broker_names_by_address(&cluster_info, &validated.cluster);

    let mut records = Vec::new();
    let mut answered = 0usize;
    for (broker_name, broker_addr) in targets {
        let query = MessageIndexQuery {
            topic: CheetahString::from(trace_topic.as_str()),
            key: CheetahString::from(validated.message_id.as_str()),
            max_messages: TRACE_MESSAGES_PER_BROKER,
            begin_timestamp: 0,
            end_timestamp: i64::MAX,
        };
        match source.trace(broker_addr, query).await {
            Ok(found) => {
                answered += 1;
                records.extend(found.into_iter().map(|record| trace_record(record, &brokers)));
            }
            Err(_) => unanswered.push(broker_name),
        }
    }
    require_an_answer("query_message_trace", answered, &mut unanswered)?;

    records.sort_by(|left, right| {
        (left.timestamp, left.step, &left.group, left.retry_times).cmp(&(
            right.timestamp,
            right.step,
            &right.group,
            right.retry_times,
        ))
    });
    records.dedup();
    let truncated = records.len() > MAX_MESSAGE_TRACE_RECORDS;
    records.truncate(MAX_MESSAGE_TRACE_RECORDS);
    Ok(MessageTraceResult {
        trace_topic,
        trace_topic_found: true,
        records,
        truncated,
        queried_broker_count,
        unanswered_brokers: unanswered,
    })
}

/// Whether an error says that the trace Topic has no route, at all or in the selected cluster.
fn trace_topic_is_absent(error: &AdminError) -> bool {
    error.failure() == AdminFailure::NotFound && matches!(error.resource(), Some("topic" | ROUTE_IN_CLUSTER))
}

/// Maps every instance address of the selected cluster to the logical name of its Broker.
fn broker_names_by_address(cluster_info: &ClusterInfo, cluster: &str) -> BTreeMap<String, String> {
    let mut names = BTreeMap::new();
    for broker in cluster_info.broker_addr_table.iter().flat_map(|table| table.values()) {
        if broker.cluster() != cluster {
            continue;
        }
        for address in broker.broker_addrs().values() {
            names.insert(address.to_string(), broker.broker_name().to_string());
        }
    }
    names
}

fn trace_record(record: MessageTraceRead, brokers: &BTreeMap<String, String>) -> MessageTraceRecord {
    MessageTraceRecord {
        step: match record.trace_type {
            TraceType::Pub => MessageTraceStep::Publish,
            TraceType::SubBefore => MessageTraceStep::ConsumeStart,
            TraceType::SubAfter => MessageTraceStep::ConsumeEnd,
            TraceType::EndTransaction => MessageTraceStep::EndTransaction,
            TraceType::Recall => MessageTraceStep::Recall,
        },
        timestamp: record.timestamp,
        group: text(&record.group_name),
        cost_time_ms: record.cost_time,
        success: record.success,
        topic: text(&record.topic),
        message_id: record.message_id,
        offset_message_id: text(&record.offset_message_id),
        tags: text(&record.tags),
        keys: record
            .keys
            .split(' ')
            .filter(|key| key.len() <= MAX_MESSAGE_KEY_BYTES)
            .filter_map(text)
            .take(MAX_MESSAGE_TRACE_KEYS)
            .collect(),
        store_broker_name: brokers.get(record.store_host.as_str()).cloned(),
        client_host: text(&record.client_host),
        retry_times: record.retry_times,
        body_length: record.body_length,
        from_transaction_check: record.from_transaction_check,
    }
}

/// Keeps a text a client wrote into a trace record when it is short and printable.
fn text(value: &str) -> Option<String> {
    (!value.is_empty() && value.len() <= MAX_TRACE_TEXT_BYTES && !value.chars().any(char::is_control))
        .then(|| value.to_owned())
}

#[cfg(test)]
#[path = "message_index_tests.rs"]
mod tests;
