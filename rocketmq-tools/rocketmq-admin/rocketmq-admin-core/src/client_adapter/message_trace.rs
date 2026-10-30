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

//! Real, bounded Trace observations for a message verified within its selected Topic.

use super::AdminSession;
use crate::core::{
    message::{MessageLookupReadAdmin, MessageLookupRequest, MessageRecord},
    message_index::MessageTraceStep,
    message_trace::{
        MessageTraceLookupRequest, MessageTraceObservation, MessageTraceReadAdmin, MessageTraceReadResult,
        MESSAGE_TRACE_BROKER_LIMIT, MESSAGE_TRACE_NODE_LIMIT,
    },
    AdminError, AdminFuture, AdminResult,
};
use cheetah_string::CheetahString;
use rocketmq_client_rust::{MQAdminMessageIndexReadExt, MQAdminReadExt, MessageIndexQuery, MessageTraceRecordsRead};
use rocketmq_protocol::{
    protocol::route::topic_route_data::TopicRouteData,
    protocol::route_facade::BrokerDataExt,
    trace::{TraceRecord, TraceType},
};
use std::collections::BTreeMap;

impl MessageTraceReadAdmin for AdminSession {
    fn message_trace<'a>(&'a self, request: &'a MessageTraceLookupRequest) -> AdminFuture<'a, MessageTraceReadResult> {
        Box::pin(async move {
            self.ensure_open()?;
            read_trace(self, request).await
        })
    }
}

trait TraceSource: Sync {
    fn lookup<'a>(&'a self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageRecord>;
    fn route<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Option<TopicRouteData>>;
    fn records<'a>(&'a self, broker: &'a str, query: MessageIndexQuery) -> AdminFuture<'a, MessageTraceRecordsRead>;
}
impl TraceSource for AdminSession {
    fn lookup<'a>(&'a self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageRecord> {
        MessageLookupReadAdmin::lookup_message(self, request)
    }
    fn route<'a>(&'a self, topic: &'a str) -> AdminFuture<'a, Option<TopicRouteData>> {
        Box::pin(async move {
            MQAdminReadExt::examine_topic_route_info(&self.inner, topic.into())
                .await
                .map_err(|source| {
                    AdminError::from_error(
                        "message_trace_route",
                        crate::IntoCanonicalError::into_canonical_error(source),
                    )
                })
        })
    }
    fn records<'a>(&'a self, broker: &'a str, query: MessageIndexQuery) -> AdminFuture<'a, MessageTraceRecordsRead> {
        Box::pin(async move {
            self.inner
                .message_trace_records_at(broker.into(), query)
                .await
                .map_err(|source| {
                    AdminError::from_error(
                        "message_trace_records",
                        crate::IntoCanonicalError::into_canonical_error(source),
                    )
                })
        })
    }
}

async fn read_trace(
    source: &impl TraceSource,
    request: &MessageTraceLookupRequest,
) -> AdminResult<MessageTraceReadResult> {
    for (field, value) in [
        ("topic", &request.topic),
        ("messageId", &request.message_id),
        ("traceTopic", &request.trace_topic),
    ] {
        if text(value).is_none() || value.trim() != value {
            return Err(AdminError::invalid_argument(
                field,
                "must be a bounded, nonempty identifier",
            ));
        }
    }
    let message = source
        .lookup(&MessageLookupRequest {
            topic: request.topic.clone(),
            message_id: request.message_id.clone(),
        })
        .await?;
    if message.topic != request.topic {
        return Err(AdminError::invalid_argument(
            "topic",
            "message lookup returned a different Topic",
        ));
    }
    let producer_id = message.unique_message_id.as_deref().and_then(text);
    let mut result = MessageTraceReadResult {
        topic: message.topic,
        message_id: message.message_id,
        producer_message_id: producer_id.clone(),
        trace_topic: request.trace_topic.clone(),
        trace_topic_found: false,
        nodes: Vec::new(),
        limit_reached: false,
        unanswered_brokers: Vec::new(),
    };
    let Some(producer_id) = producer_id else {
        return Ok(result);
    };
    let Some(route) = source.route(&request.trace_topic).await? else {
        return Ok(result);
    };
    result.trace_topic_found = true;
    let mut targets = BTreeMap::new();
    for broker in &route.broker_datas {
        match broker.select_broker_addr() {
            Some(address) => {
                targets.insert(broker.broker_name().to_string(), address);
            }
            None => result.unanswered_brokers.push(broker.broker_name().to_string()),
        }
    }
    result.limit_reached = targets.len() > MESSAGE_TRACE_BROKER_LIMIT;
    let mut answered = 0;
    for (name, address) in targets.into_iter().take(MESSAGE_TRACE_BROKER_LIMIT) {
        match source
            .records(
                address.as_str(),
                MessageIndexQuery {
                    topic: request.trace_topic.as_str().into(),
                    key: producer_id.as_str().into(),
                    max_messages: 64,
                    begin_timestamp: 0,
                    end_timestamp: i64::MAX,
                },
            )
            .await
        {
            Ok(page) => {
                answered += 1;
                result.limit_reached |= page.limit_reached;
                for wire in page.records {
                    if let Some(node) = observation(&wire, &producer_id, &request.topic) {
                        if result.nodes.len() < MESSAGE_TRACE_NODE_LIMIT {
                            result.nodes.push(node);
                        } else {
                            result.limit_reached = true;
                        }
                    }
                }
            }
            Err(_) => result.unanswered_brokers.push(name),
        }
    }
    if answered == 0 {
        return Err(AdminError::unavailable(
            "message_trace",
            "No Broker hosting the trace Topic answered",
        ));
    }
    // Stable sort retains returned order for equal timestamps, without generating edges or durations.
    result
        .nodes
        .sort_by_key(|node| (node.timestamp.is_none(), node.timestamp.unwrap_or_default()));
    result.unanswered_brokers.sort();
    result.unanswered_brokers.dedup();
    Ok(result)
}

fn text(value: &str) -> Option<String> {
    (!value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)).then(|| value.to_owned())
}

fn observation(wire: &TraceRecord, producer_id: &str, topic: &str) -> Option<MessageTraceObservation> {
    let field = |index| wire.fields.get(index).map(CheetahString::as_str);
    let id_index = match wire.trace_type {
        TraceType::SubAfter => 1,
        TraceType::Pub | TraceType::SubBefore | TraceType::EndTransaction | TraceType::Recall => 4,
    };
    if field(id_index)? != producer_id {
        return None;
    }
    if matches!(
        wire.trace_type,
        TraceType::Pub | TraceType::EndTransaction | TraceType::Recall
    ) && field(3)? != topic
    {
        return None;
    }
    let timestamp = |index| field(index)?.parse::<i64>().ok().filter(|value| *value >= 0);
    let number = |index| field(index)?.parse::<u32>().ok();
    let label = |index| field(index).and_then(text);
    let boolean = |index| field(index)?.parse::<bool>().ok();
    let mut node = MessageTraceObservation {
        step: match wire.trace_type {
            TraceType::Pub => MessageTraceStep::Publish,
            TraceType::SubBefore => MessageTraceStep::ConsumeStart,
            TraceType::SubAfter => MessageTraceStep::ConsumeEnd,
            TraceType::EndTransaction => MessageTraceStep::EndTransaction,
            TraceType::Recall => MessageTraceStep::Recall,
        },
        timestamp: timestamp(0),
        group: label(2),
        success: None,
        cost_time_ms: None,
        client_host: None,
        store_host: None,
        offset_message_id: None,
        retry_times: None,
        transaction_state: None,
    };
    match wire.trace_type {
        TraceType::Pub => {
            node.cost_time_ms = number(9);
            node.store_host = label(7);
            match wire.fields.len() {
                12 => node.success = boolean(11),
                13.. => {
                    node.offset_message_id = label(11);
                    node.success = boolean(12);
                    node.client_host = label(13);
                }
                _ => {}
            }
        }
        TraceType::SubBefore => node.retry_times = number(5),
        TraceType::SubAfter => {
            node.timestamp = timestamp(6);
            node.group = label(7);
            node.cost_time_ms = number(2);
            node.success = boolean(3);
        }
        TraceType::EndTransaction => {
            node.store_host = label(7);
            node.transaction_state = label(10);
        }
        TraceType::Recall => node.success = boolean(5),
    }
    Some(node)
}

#[cfg(test)]
#[path = "message_trace_tests.rs"]
mod tests;
