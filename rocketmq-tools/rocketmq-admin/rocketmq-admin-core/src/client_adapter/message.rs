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

use std::collections::BTreeMap;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin_adapter_compat::error::RocketMQError;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_client_rust::TraceDataEncoder;
use rocketmq_model::message::MessageQueue;
use rocketmq_model::result::PullStatus;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::common::wire_constants::MASTER_ID;
use rocketmq_protocol::protocol::body::cm_result::CMResult;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::message::*;
use crate::core::queue::QueueRef;
use crate::core::AdminError;
use crate::core::AdminFuture;

const PULL_TIMEOUT_MILLIS: u64 = 3_000;
const DEFAULT_TRACE_TOPIC: &str = "RMQ_SYS_TRACE_TOPIC";
const PROPERTY_UNIQUE_KEY: &str = "UNIQ_KEY";
const PROPERTY_KEYS: &str = "KEYS";
const PROPERTY_RETRY_TOPIC: &str = "RETRY_TOPIC";
const PROPERTY_ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";
const PROPERTY_DLQ_ORIGIN_MESSAGE_ID: &str = "DLQ_ORIGIN_MESSAGE_ID";
const PROPERTY_WAIT_STORE_MSG_OK: &str = "WAIT";

macro_rules! map_message_record {
    ($message:expr) => {{
        let properties = $message
            .properties()
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>();
        MessageRecord {
            topic: $message.topic().to_string(),
            message_id: $message.msg_id().to_string(),
            unique_message_id: properties
                .get(PROPERTY_UNIQUE_KEY)
                .map(String::as_str)
                .and_then(non_empty),
            keys: properties.get(PROPERTY_KEYS).cloned(),
            tags: $message.tags().map(ToString::to_string),
            born_timestamp: $message.born_timestamp(),
            store_timestamp: $message.store_timestamp(),
            born_host: $message.born_host().to_string(),
            store_host: $message.store_host().to_string(),
            queue_id: $message.queue_id(),
            queue_offset: $message.queue_offset(),
            store_size: $message.store_size(),
            reconsume_times: $message.reconsume_times(),
            body_crc: $message.body_crc(),
            sys_flag: $message.sys_flag(),
            flag: $message.flag(),
            prepared_transaction_offset: $message.prepared_transaction_offset(),
            body: $message.body().map(|body| body.to_vec()).unwrap_or_default(),
            properties,
        }
    }};
}

impl MessageAdmin for AdminSession {
    fn query_messages_by_key<'a>(
        &'a mut self,
        request: &'a QueryMessagesByKeyRequest,
    ) -> AdminFuture<'a, QueryMessagesResult> {
        Box::pin(async move {
            self.ensure_open()?;
            require_non_empty("topic", &request.topic)?;
            require_non_empty("key", &request.key)?;
            let result = self
                .inner
                .query_message_by_key(
                    None,
                    request.topic.as_str().into(),
                    request.key.as_str().into(),
                    request.max_messages,
                    request.begin,
                    request.end,
                    CheetahString::from_static_str("K"),
                    None,
                )
                .await
                .map_err(|error| backend_error("query_message_by_key", error))?;
            let mut messages = result
                .message_list()
                .iter()
                .map(|message| map_message_record!(message))
                .collect::<Vec<_>>();
            sort_messages(&mut messages);
            Ok(QueryMessagesResult { messages })
        })
    }

    fn find_message<'a>(&'a mut self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageRecord> {
        Box::pin(async move { find_message(self, request).await })
    }

    fn message_detail<'a>(&'a mut self, request: &'a MessageLookupRequest) -> AdminFuture<'a, MessageDetailRecord> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic = require_non_empty("topic", &request.topic)?;
            let message_id = require_non_empty("messageId", &request.message_id)?;
            let message = find_raw_message(self, topic, message_id).await?;
            #[allow(
                deprecated,
                reason = "the SDK track result remains the compatibility source inside the M07 adapter"
            )]
            let tracks = self
                .inner
                .message_track_detail(message.clone())
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(operation = "message_track_detail", error = %error, "message track lookup failed");
                    Vec::new()
                })
                .into_iter()
                .map(|track| MessageTrackRecord {
                    consumer_group: track.consumer_group,
                    track_type: track
                        .track_type
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "UNKNOWN".to_string()),
                    exception_desc: non_empty(&track.exception_desc),
                })
                .collect();
            let mut message = map_message_record!(&message);
            message.properties.remove(PROPERTY_WAIT_STORE_MSG_OK);
            Ok(MessageDetailRecord { message, tracks })
        })
    }

    fn message_queue_plan<'a>(&'a mut self, request: &'a MessageQueuePlanRequest) -> AdminFuture<'a, MessageQueuePlan> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic = require_non_empty("topic", &request.topic)?;
            let Some(route) = self
                .inner
                .examine_topic_route_info(topic.into())
                .await
                .map_err(|error| backend_error("examine_topic_route_info", error))?
            else {
                return Ok(MessageQueuePlan {
                    topic_exists: false,
                    queues: Vec::new(),
                });
            };
            let addresses = route
                .broker_datas
                .iter()
                .filter_map(|broker| {
                    broker
                        .broker_addrs()
                        .get(&MASTER_ID)
                        .or_else(|| broker.broker_addrs().values().next())
                        .map(|address| (broker.broker_name().to_string(), address.to_string()))
                })
                .collect::<BTreeMap<_, _>>();
            let mut queues = Vec::new();
            for queue_data in &route.queue_datas {
                let broker_name = queue_data.broker_name().to_string();
                let Some(broker_addr) = addresses.get(&broker_name) else {
                    continue;
                };
                for queue_id in 0..queue_data.read_queue_nums() {
                    let queue_id = i32::try_from(queue_id)
                        .map_err(|_| AdminError::backend("message_queue_plan", "queue id exceeds i32"))?;
                    let start = self
                        .inner
                        .search_offset(
                            broker_addr.as_str().into(),
                            topic.into(),
                            queue_id,
                            request.begin as u64,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await
                        .map_err(|error| backend_error("search_offset", error))? as i64;
                    let end = self
                        .inner
                        .search_offset(
                            broker_addr.as_str().into(),
                            topic.into(),
                            queue_id,
                            request.end as u64,
                            PULL_TIMEOUT_MILLIS,
                        )
                        .await
                        .map_err(|error| backend_error("search_offset", error))? as i64;
                    queues.push(MessageQueueRange {
                        broker_addr: broker_addr.clone(),
                        queue: QueueRef {
                            topic: topic.to_string(),
                            broker_name: broker_name.clone(),
                            queue_id,
                        },
                        start,
                        end,
                    });
                }
            }
            Ok(MessageQueuePlan {
                topic_exists: true,
                queues,
            })
        })
    }

    fn pull_messages<'a>(&'a mut self, request: &'a PullMessagesRequest) -> AdminFuture<'a, PullMessagesResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let queue = MessageQueue::from_parts(
                request.queue.topic.as_str(),
                request.queue.broker_name.as_str(),
                request.queue.queue_id,
            );
            let result = self
                .inner
                .pull_message_from_queue(
                    request.broker_addr.as_str(),
                    &queue,
                    "*",
                    request.offset,
                    request.max_messages,
                    PULL_TIMEOUT_MILLIS,
                )
                .await
                .map_err(|error| backend_error("pull_message_from_queue", error))?;
            let status = match *result.pull_status() {
                PullStatus::Found => MessagePullStatus::Found,
                PullStatus::NoMatchedMsg => MessagePullStatus::NoMatchedMsg,
                PullStatus::NoNewMsg => MessagePullStatus::NoNewMsg,
                PullStatus::OffsetIllegal => MessagePullStatus::OffsetIllegal,
            };
            let messages = result
                .msg_found_list()
                .into_iter()
                .flatten()
                .map(|message| map_message_record!(message))
                .collect();
            Ok(PullMessagesResult {
                status,
                next_begin_offset: result.next_begin_offset() as i64,
                messages,
            })
        })
    }

    fn consume_message_directly<'a>(
        &'a mut self,
        request: &'a DirectConsumeRequest,
    ) -> AdminFuture<'a, DirectConsumeResult> {
        Box::pin(async move { consume_directly(self, request).await })
    }

    fn find_dlq_message<'a>(&'a mut self, request: &'a DlqMessageLookupRequest) -> AdminFuture<'a, MessageRecord> {
        Box::pin(async move {
            let topic = dlq_topic(&request.consumer_group)?;
            find_message(
                self,
                &MessageLookupRequest {
                    topic,
                    message_id: request.message_id.clone(),
                },
            )
            .await
        })
    }

    fn resend_dlq_message<'a>(&'a mut self, request: &'a DlqMessageLookupRequest) -> AdminFuture<'a, DlqResendResult> {
        Box::pin(async move {
            let dlq_message = self.find_dlq_message(request).await?;
            let topic = property(&dlq_message, PROPERTY_RETRY_TOPIC).ok_or_else(|| {
                AdminError::invalid_argument(
                    "message",
                    "DLQ message is missing `RETRY_TOPIC`, so it cannot be resent safely.",
                )
            })?;
            let message_id = property(&dlq_message, PROPERTY_ORIGIN_MESSAGE_ID)
                .or_else(|| property(&dlq_message, PROPERTY_DLQ_ORIGIN_MESSAGE_ID))
                .ok_or_else(|| {
                    AdminError::invalid_argument(
                        "message",
                        "DLQ message is missing `ORIGIN_MESSAGE_ID`, so it cannot be resent safely.",
                    )
                })?;
            let consume = consume_directly(
                self,
                &DirectConsumeRequest {
                    topic: topic.clone(),
                    consumer_group: request.consumer_group.trim().to_string(),
                    message_id: message_id.clone(),
                    client_id: None,
                },
            )
            .await?;
            Ok(DlqResendResult {
                topic,
                message_id,
                consume,
            })
        })
    }

    fn query_trace_data<'a>(&'a mut self, request: &'a TraceQueryRequest) -> AdminFuture<'a, TraceData> {
        Box::pin(async move {
            self.ensure_open()?;
            let message_id = require_non_empty("messageId", &request.message_id)?;
            let trace_topic = request
                .trace_topic
                .as_deref()
                .and_then(non_empty)
                .unwrap_or_else(|| DEFAULT_TRACE_TOPIC.to_string());
            let result = self
                .inner
                .query_message_by_key(
                    None,
                    trace_topic.as_str().into(),
                    message_id.into(),
                    64,
                    0,
                    i64::MAX,
                    CheetahString::from_static_str(""),
                    None,
                )
                .await
                .map_err(|error| backend_error("query_message_by_key", error))?;
            let mut seeds = Vec::new();
            for trace_message in result.message_list() {
                let Some(body) = trace_message.body() else {
                    continue;
                };
                let body_text = String::from_utf8_lossy(body.as_ref());
                if body_text.trim().is_empty() {
                    continue;
                }
                for context in TraceDataEncoder::decoder_from_trace_data_string(&body_text) {
                    let trace_type = context
                        .trace_type
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "Unknown".to_string());
                    let group_name = context.group_name.to_string();
                    let status = if context.is_success { "success" } else { "failed" }.to_string();
                    for bean in context.trace_beans.into_iter().flatten() {
                        if bean.msg_id.as_str() != message_id {
                            continue;
                        }
                        seeds.push(TraceSeed {
                            trace_type: trace_type.clone(),
                            group_name: group_name.clone(),
                            client_host: if bean.client_host.is_empty() {
                                trace_message.born_host().to_string()
                            } else {
                                bean.client_host.to_string()
                            },
                            store_host: if bean.store_host.is_empty() {
                                trace_message.store_host().to_string()
                            } else {
                                bean.store_host.to_string()
                            },
                            timestamp: if bean.store_time > 0 {
                                bean.store_time
                            } else {
                                context.time_stamp as i64
                            },
                            cost_time: context.cost_time,
                            status: status.clone(),
                            topic: non_empty(bean.topic.as_str()),
                            tags: non_empty(bean.tags.as_str()),
                            keys: non_empty(bean.keys.as_str()),
                            retry_times: bean.retry_times,
                            from_transaction_check: bean.from_transaction_check,
                        });
                    }
                }
            }
            if seeds.is_empty() {
                return Err(AdminError::invalid_argument(
                    "messageId",
                    "No trace information matched the current message.",
                ));
            }
            Ok(TraceData {
                message_id: message_id.to_string(),
                trace_topic,
                seeds,
            })
        })
    }
}

async fn find_message(session: &mut AdminSession, request: &MessageLookupRequest) -> Result<MessageRecord, AdminError> {
    session.ensure_open()?;
    let topic = require_non_empty("topic", &request.topic)?;
    let message_id = require_non_empty("messageId", &request.message_id)?;
    let message = find_raw_message(session, topic, message_id).await?;
    Ok(map_message_record!(&message))
}

pub(super) async fn find_raw_message(
    session: &mut AdminSession,
    topic: &str,
    message_id: &str,
) -> Result<rocketmq_client_rust::admin_adapter_compat::message::MessageExt, AdminError> {
    match session
        .inner
        .query_message_by_unique_key(None, topic.into(), message_id.into(), 32, 0, i64::MAX)
        .await
    {
        Ok(result) if !result.message_list().is_empty() => {
            let mut messages = result
                .message_list()
                .iter()
                .filter(|message| {
                    message
                        .properties()
                        .get(PROPERTY_UNIQUE_KEY)
                        .is_some_and(|value| value.as_str() == message_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            if messages.is_empty() {
                return session
                    .inner
                    .query_message(CheetahString::default(), topic.into(), message_id.into())
                    .await
                    .map_err(|error| backend_error("query_message", error));
            }
            messages.sort_by(|left, right| {
                right
                    .store_timestamp()
                    .cmp(&left.store_timestamp())
                    .then_with(|| left.msg_id().cmp(right.msg_id()))
            });
            return messages
                .into_iter()
                .next()
                .ok_or_else(|| AdminError::backend("query_message_by_unique_key", "message was not found"));
        }
        Ok(_) | Err(_) => {}
    }
    session
        .inner
        .query_message(CheetahString::default(), topic.into(), message_id.into())
        .await
        .map_err(|error| backend_error("query_message", error))
}

async fn consume_directly(
    session: &mut AdminSession,
    request: &DirectConsumeRequest,
) -> Result<DirectConsumeResult, AdminError> {
    session.ensure_open()?;
    let result = session
        .inner
        .consume_message_directly(
            require_non_empty("consumerGroup", &request.consumer_group)?.into(),
            request.client_id.as_deref().unwrap_or_default().into(),
            require_non_empty("topic", &request.topic)?.into(),
            require_non_empty("messageId", &request.message_id)?.into(),
        )
        .await
        .map_err(|error| backend_error("consume_message_directly", error))?;
    Ok(DirectConsumeResult {
        success: matches!(result.consume_result(), Some(value) if *value == CMResult::CRSuccess),
        consume_result: result.consume_result().map(ToString::to_string),
        remark: result
            .remark()
            .map(ToString::to_string)
            .and_then(|value| non_empty(&value)),
    })
}

fn dlq_topic(consumer_group: &str) -> Result<String, AdminError> {
    let consumer_group = require_non_empty("consumerGroup", consumer_group)?;
    Ok(if consumer_group.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
        consumer_group.to_string()
    } else {
        format!("{DLQ_GROUP_TOPIC_PREFIX}{consumer_group}")
    })
}

fn property(message: &MessageRecord, key: &str) -> Option<String> {
    message.properties.get(key).map(String::as_str).and_then(non_empty)
}

fn require_non_empty<'a>(field: &'static str, value: &'a str) -> Result<&'a str, AdminError> {
    let value = value.trim();
    if value.is_empty() {
        Err(AdminError::invalid_argument(field, "must not be empty"))
    } else {
        Ok(value)
    }
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn sort_messages(messages: &mut [MessageRecord]) {
    messages.sort_by(|left, right| {
        right
            .store_timestamp
            .cmp(&left.store_timestamp)
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
}

fn backend_error(operation: &'static str, error: RocketMQError) -> AdminError {
    let view = error.boundary_view();
    let context = (!view.context().is_empty()).then(|| view.context().to_string());
    AdminError::backend_view(
        operation,
        view.code().as_str(),
        view.message(),
        context,
        view.http().status.as_u16(),
        view.is_retryable(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dlq_topic_accepts_plain_and_prefixed_consumer_groups() {
        assert_eq!(dlq_topic("group-a").unwrap(), "%DLQ%group-a");
        assert_eq!(dlq_topic("%DLQ%group-a").unwrap(), "%DLQ%group-a");
    }

    #[test]
    fn message_record_property_ignores_blank_values() {
        let mut message = MessageRecord {
            topic: "topic".to_string(),
            message_id: "id".to_string(),
            unique_message_id: None,
            keys: None,
            tags: None,
            born_timestamp: 0,
            store_timestamp: 0,
            born_host: String::new(),
            store_host: String::new(),
            queue_id: 0,
            queue_offset: 0,
            store_size: 0,
            reconsume_times: 0,
            body_crc: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
            body: Vec::new(),
            properties: BTreeMap::new(),
        };
        message
            .properties
            .insert(PROPERTY_RETRY_TOPIC.to_string(), "   ".to_string());
        assert_eq!(property(&message, PROPERTY_RETRY_TOPIC), None);
    }
}
