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

use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::STRING_HASH_SET;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::context::ResolvedAddressScheme;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::AckMessagePlan;
use crate::processor::AckMessageRequest;
use crate::processor::AckMessageResultEntry;
use crate::processor::ChangeInvisibleDurationPlan;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::ConsumerFilterExpression;
use crate::processor::EndTransactionPlan;
use crate::processor::EndTransactionRequest;
use crate::processor::ForwardMessageToDeadLetterQueuePlan;
use crate::processor::ForwardMessageToDeadLetterQueueRequest;
use crate::processor::GetOffsetPlan;
use crate::processor::GetOffsetRequest;
use crate::processor::MessageQueueTarget;
use crate::processor::PullMessagePlan;
use crate::processor::PullMessageRequest;
use crate::processor::QueryAssignmentPlan;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryOffsetPlan;
use crate::processor::QueryOffsetPolicy;
use crate::processor::QueryOffsetRequest;
use crate::processor::QueryRoutePlan;
use crate::processor::QueryRouteRequest;
use crate::processor::RecallMessagePlan;
use crate::processor::RecallMessageRequest;
use crate::processor::ReceiveMessagePlan;
use crate::processor::ReceiveMessageRequest;
use crate::processor::ReceiveTarget;
use crate::processor::ReceivedMessage;
use crate::processor::SendMessageEntry;
use crate::processor::SendMessagePlan;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
use crate::processor::TransactionResolution;
use crate::processor::TransactionSource;
use crate::processor::UpdateOffsetPlan;
use crate::processor::UpdateOffsetRequest;
use crate::proto::v2;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
use crate::status::ProxyPayloadStatus;
use crate::status::ProxyStatusMapper;

pub fn build_query_route_request(
    config: &ProxyConfig,
    request: &v2::QueryRouteRequest,
) -> ProxyResult<QueryRouteRequest> {
    Ok(QueryRouteRequest {
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        endpoints: resolve_endpoints(config, request.endpoints.as_ref())?,
    })
}

pub fn build_query_assignment_request(
    config: &ProxyConfig,
    request: &v2::QueryAssignmentRequest,
) -> ProxyResult<QueryAssignmentRequest> {
    Ok(QueryAssignmentRequest {
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        group: resource_identity(request.group.as_ref(), "group")?,
        endpoints: resolve_endpoints(config, request.endpoints.as_ref())?,
    })
}

pub fn build_send_message_request(
    context: &ProxyContext,
    request: &v2::SendMessageRequest,
) -> ProxyResult<SendMessageRequest> {
    if request.messages.is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument("messages must not be empty").into());
    }

    let messages = request
        .messages
        .iter()
        .map(build_send_message_entry)
        .collect::<ProxyResult<Vec<_>>>()?;

    Ok(SendMessageRequest {
        messages,
        timeout: context.deadline(),
    })
}

pub fn build_recall_message_request(request: &v2::RecallMessageRequest) -> ProxyResult<RecallMessageRequest> {
    Ok(RecallMessageRequest {
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        recall_handle: validate_non_empty_string("recallHandle", request.recall_handle.as_str())?,
    })
}

pub fn build_receive_message_request(request: &v2::ReceiveMessageRequest) -> ProxyResult<ReceiveMessageRequest> {
    let group = resource_identity(request.group.as_ref(), "group")?;
    let message_queue = request
        .message_queue
        .as_ref()
        .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("messageQueue must not be empty"))?;
    let topic = resource_identity(message_queue.topic.as_ref(), "messageQueue.topic")?;
    let batch_size = validate_batch_size(request.batch_size)?;
    let invisible_duration = request
        .invisible_duration
        .as_ref()
        .map(|duration| {
            positive_duration(duration, "invisibleDuration", |message| {
                ProxyError::illegal_invisible_time(message)
            })
        })
        .transpose()?
        .unwrap_or_else(|| Duration::from_secs(15));
    let long_polling_timeout = request
        .long_polling_timeout
        .as_ref()
        .map(|duration| {
            non_negative_duration(duration, "longPollingTimeout", |message| {
                ProxyError::illegal_polling_time(message)
            })
        })
        .transpose()?
        .unwrap_or_else(|| Duration::from_secs(15));
    let filter_expression = build_filter_expression(request.filter_expression.as_ref())?;
    let (broker_name, broker_addr) = message_queue_broker(message_queue)?;

    Ok(ReceiveMessageRequest {
        group,
        target: ReceiveTarget {
            topic,
            queue_id: message_queue.id,
            broker_name,
            broker_addr,
            fifo: message_queue
                .accept_message_types
                .contains(&(v2::MessageType::Fifo as i32)),
        },
        filter_expression,
        batch_size,
        invisible_duration,
        auto_renew: request.auto_renew,
        long_polling_timeout,
        attempt_id: request
            .attempt_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
    })
}

pub fn build_pull_message_request(request: &v2::PullMessageRequest) -> ProxyResult<PullMessageRequest> {
    Ok(PullMessageRequest {
        group: resource_identity(request.group.as_ref(), "group")?,
        target: message_queue_target(request.message_queue.as_ref(), "messageQueue")?,
        offset: validate_offset("offset", request.offset)?,
        batch_size: validate_batch_size(request.batch_size)?,
        filter_expression: build_filter_expression(request.filter_expression.as_ref())?,
        long_polling_timeout: request
            .long_polling_timeout
            .as_ref()
            .map(|duration| non_negative_duration(duration, "longPollingTimeout", ProxyError::illegal_polling_time))
            .transpose()?
            .unwrap_or_default(),
    })
}

pub fn build_ack_message_request(request: &v2::AckMessageRequest) -> ProxyResult<AckMessageRequest> {
    if request.entries.is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument("entries must not be empty").into());
    }

    let entries = request
        .entries
        .iter()
        .map(|entry| {
            Ok(crate::processor::AckMessageEntry {
                message_id: validate_non_empty_string("entries.messageId", entry.message_id.as_str())?,
                receipt_handle: validate_non_empty_string("entries.receiptHandle", entry.receipt_handle.as_str())?,
                lite_topic: entry
                    .lite_topic
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
            })
        })
        .collect::<ProxyResult<Vec<_>>>()?;

    Ok(AckMessageRequest {
        group: resource_identity(request.group.as_ref(), "group")?,
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        entries,
    })
}

pub fn build_forward_message_to_dead_letter_queue_request(
    request: &v2::ForwardMessageToDeadLetterQueueRequest,
) -> ProxyResult<ForwardMessageToDeadLetterQueueRequest> {
    Ok(ForwardMessageToDeadLetterQueueRequest {
        group: resource_identity(request.group.as_ref(), "group")?,
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        receipt_handle: validate_non_empty_string("receiptHandle", request.receipt_handle.as_str())?,
        message_id: validate_non_empty_string("messageId", request.message_id.as_str())?,
        delivery_attempt: request.delivery_attempt,
        max_delivery_attempts: request.max_delivery_attempts,
        lite_topic: request
            .lite_topic
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
    })
}

pub fn build_change_invisible_duration_request(
    request: &v2::ChangeInvisibleDurationRequest,
) -> ProxyResult<ChangeInvisibleDurationRequest> {
    Ok(ChangeInvisibleDurationRequest {
        group: resource_identity(request.group.as_ref(), "group")?,
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        receipt_handle: validate_non_empty_string("receiptHandle", request.receipt_handle.as_str())?,
        invisible_duration: positive_duration(
            request.invisible_duration.as_ref().ok_or_else(|| {
                rocketmq_error::RocketMQError::illegal_argument("invisibleDuration must not be empty")
            })?,
            "invisibleDuration",
            ProxyError::illegal_invisible_time,
        )?,
        message_id: request.message_id.trim().to_owned(),
        lite_topic: request
            .lite_topic
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        suspend: request.suspend,
    })
}

pub fn build_update_offset_request(request: &v2::UpdateOffsetRequest) -> ProxyResult<UpdateOffsetRequest> {
    Ok(UpdateOffsetRequest {
        group: resource_identity(request.group.as_ref(), "group")?,
        target: message_queue_target(request.message_queue.as_ref(), "messageQueue")?,
        offset: validate_offset("offset", request.offset)?,
    })
}

pub fn build_get_offset_request(request: &v2::GetOffsetRequest) -> ProxyResult<GetOffsetRequest> {
    Ok(GetOffsetRequest {
        group: resource_identity(request.group.as_ref(), "group")?,
        target: message_queue_target(request.message_queue.as_ref(), "messageQueue")?,
    })
}

pub fn build_query_offset_request(request: &v2::QueryOffsetRequest) -> ProxyResult<QueryOffsetRequest> {
    let policy = match v2::QueryOffsetPolicy::try_from(request.query_offset_policy)
        .unwrap_or(v2::QueryOffsetPolicy::Beginning)
    {
        v2::QueryOffsetPolicy::Beginning => QueryOffsetPolicy::Beginning,
        v2::QueryOffsetPolicy::End => QueryOffsetPolicy::End,
        v2::QueryOffsetPolicy::Timestamp => QueryOffsetPolicy::Timestamp,
    };

    let timestamp_ms = request
        .timestamp
        .as_ref()
        .map(timestamp_to_offset_query_epoch_millis)
        .transpose()?;
    if matches!(policy, QueryOffsetPolicy::Timestamp) && timestamp_ms.is_none() {
        return Err(ProxyError::illegal_offset("timestamp policy requires timestamp"));
    }

    Ok(QueryOffsetRequest {
        target: message_queue_target(request.message_queue.as_ref(), "messageQueue")?,
        policy,
        timestamp_ms,
    })
}

pub fn build_end_transaction_request(request: &v2::EndTransactionRequest) -> ProxyResult<EndTransactionRequest> {
    let resolution = match v2::TransactionResolution::try_from(request.resolution)
        .unwrap_or(v2::TransactionResolution::Unspecified)
    {
        v2::TransactionResolution::Commit => TransactionResolution::Commit,
        v2::TransactionResolution::Rollback => TransactionResolution::Rollback,
        v2::TransactionResolution::Unspecified => {
            return Err(rocketmq_error::RocketMQError::illegal_argument("resolution must not be unspecified").into());
        }
    };
    let source =
        match v2::TransactionSource::try_from(request.source).unwrap_or(v2::TransactionSource::SourceUnspecified) {
            v2::TransactionSource::SourceClient => TransactionSource::Client,
            v2::TransactionSource::SourceServerCheck => TransactionSource::ServerCheck,
            v2::TransactionSource::SourceUnspecified => {
                return Err(rocketmq_error::RocketMQError::illegal_argument("source must not be unspecified").into());
            }
        };

    Ok(EndTransactionRequest {
        topic: resource_identity(request.topic.as_ref(), "topic")?,
        message_id: validate_non_empty_string("messageId", request.message_id.as_str())?,
        transaction_id: validate_transaction_id(request.transaction_id.as_str())?,
        resolution,
        source,
        trace_context: {
            let trimmed = request.trace_context.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        },
        producer_group: None,
        transaction_state_table_offset: None,
        commit_log_message_id: None,
    })
}

pub fn build_query_route_response(request: &v2::QueryRouteRequest, plan: &QueryRoutePlan) -> v2::QueryRouteResponse {
    let broker_map = build_broker_map(&plan.route);
    let topic = request.topic.clone().unwrap_or_default();
    let message_queues = plan
        .route
        .queue_datas
        .iter()
        .flat_map(|queue_data| {
            broker_map
                .get(queue_data.broker_name.as_str())
                .into_iter()
                .flat_map(move |broker_id_map| broker_id_map.values())
                .flat_map({
                    let topic = topic.clone();
                    move |broker| {
                        gen_message_queue_from_queue_data(queue_data, &topic, plan.topic_message_type, broker.clone())
                    }
                })
        })
        .collect();

    v2::QueryRouteResponse {
        status: Some(ProxyStatusMapper::ok()),
        message_queues,
    }
}

pub fn build_query_assignment_response(
    request: &v2::QueryAssignmentRequest,
    plan: &QueryAssignmentPlan,
) -> v2::QueryAssignmentResponse {
    if let Some(assignments) = build_query_assignment_response_from_server(request, plan) {
        return v2::QueryAssignmentResponse {
            status: Some(ProxyStatusMapper::ok()),
            assignments,
        };
    }

    let broker_map = build_broker_map(&plan.route);
    let topic = request.topic.clone().unwrap_or_default();
    let is_fifo = plan
        .subscription_group
        .as_ref()
        .map(|group| group.consume_message_orderly)
        .unwrap_or(false);
    let is_lite = plan
        .subscription_group
        .as_ref()
        .and_then(|group| group.lite_bind_topic.as_ref())
        .is_some();

    let assignments: Vec<v2::Assignment> = plan
        .route
        .queue_datas
        .iter()
        .filter(|queue_data| PermName::is_readable(queue_data.perm) && queue_data.read_queue_nums > 0)
        .filter_map(|queue_data| {
            broker_map
                .get(queue_data.broker_name.as_str())
                .and_then(|id_map| id_map.get(&MASTER_ID))
                .map(|broker| (queue_data, broker.clone()))
        })
        .flat_map(|(queue_data, broker)| {
            let permission = convert_permission(queue_data.perm);
            if is_fifo && !is_lite {
                (0..queue_data.read_queue_nums)
                    .map({
                        let topic = topic.clone();
                        move |queue_id| v2::Assignment {
                            message_queue: Some(v2::MessageQueue {
                                topic: Some(topic.clone()),
                                id: queue_id as i32,
                                permission,
                                broker: Some(broker.clone()),
                                accept_message_types: Vec::new(),
                            }),
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![v2::Assignment {
                    message_queue: Some(v2::MessageQueue {
                        topic: Some(topic.clone()),
                        id: -1,
                        permission,
                        broker: Some(broker),
                        accept_message_types: Vec::new(),
                    }),
                }]
            }
        })
        .collect();

    if assignments.is_empty() {
        return v2::QueryAssignmentResponse {
            status: Some(ProxyStatusMapper::from_code(v2::Code::Forbidden, "no readable queue")),
            assignments,
        };
    }

    v2::QueryAssignmentResponse {
        status: Some(ProxyStatusMapper::ok()),
        assignments,
    }
}

pub fn build_send_message_response(plan: &SendMessagePlan, request: &SendMessageRequest) -> v2::SendMessageResponse {
    let entries: Vec<v2::SendResultEntry> = plan
        .entries
        .iter()
        .zip(request.messages.iter())
        .map(|(result, message)| build_send_result_entry(result, message))
        .collect();

    v2::SendMessageResponse {
        status: Some(summarize_send_response_status(&plan.entries).into()),
        entries,
    }
}

pub fn build_receive_message_responses(plan: &ReceiveMessagePlan) -> Vec<v2::ReceiveMessageResponse> {
    let mut responses = Vec::new();
    if let Some(delivery_timestamp) = plan.delivery_timestamp_ms.and_then(timestamp_from_epoch_millis) {
        responses.push(v2::ReceiveMessageResponse {
            content: Some(v2::receive_message_response::Content::DeliveryTimestamp(
                delivery_timestamp,
            )),
        });
    }
    responses.extend(plan.messages.iter().map(build_receive_message_response));
    responses.push(v2::ReceiveMessageResponse {
        content: Some(v2::receive_message_response::Content::Status(
            plan.status.clone().into(),
        )),
    });
    responses
}

pub fn build_pull_message_responses(plan: &PullMessagePlan) -> Vec<v2::PullMessageResponse> {
    let mut responses = plan
        .messages
        .iter()
        .map(|message| v2::PullMessageResponse {
            content: Some(v2::pull_message_response::Content::Message(build_message_from_ext(
                message, None,
            ))),
        })
        .collect::<Vec<_>>();
    responses.push(v2::PullMessageResponse {
        content: Some(v2::pull_message_response::Content::NextOffset(plan.next_offset)),
    });
    responses.push(v2::PullMessageResponse {
        content: Some(v2::pull_message_response::Content::Status(plan.status.clone().into())),
    });
    responses
}

pub fn build_ack_message_response(plan: &AckMessagePlan) -> v2::AckMessageResponse {
    v2::AckMessageResponse {
        status: Some(summarize_ack_response_status(&plan.entries).into()),
        entries: plan.entries.iter().map(build_ack_result_entry).collect(),
    }
}

pub fn build_forward_message_to_dead_letter_queue_response(
    plan: &ForwardMessageToDeadLetterQueuePlan,
) -> v2::ForwardMessageToDeadLetterQueueResponse {
    v2::ForwardMessageToDeadLetterQueueResponse {
        status: Some(plan.status.clone().into()),
    }
}

pub fn build_change_invisible_duration_response(
    plan: &ChangeInvisibleDurationPlan,
) -> v2::ChangeInvisibleDurationResponse {
    v2::ChangeInvisibleDurationResponse {
        status: Some(plan.status.clone().into()),
        receipt_handle: plan.receipt_handle.clone(),
    }
}

pub fn build_end_transaction_response(plan: &EndTransactionPlan) -> v2::EndTransactionResponse {
    v2::EndTransactionResponse {
        status: Some(plan.status.clone().into()),
    }
}

pub fn build_recall_message_response(plan: &RecallMessagePlan) -> v2::RecallMessageResponse {
    v2::RecallMessageResponse {
        status: Some(plan.status.clone().into()),
        message_id: plan.message_id.clone(),
    }
}

pub fn build_update_offset_response(plan: &UpdateOffsetPlan) -> v2::UpdateOffsetResponse {
    v2::UpdateOffsetResponse {
        status: Some(plan.status.clone().into()),
    }
}

pub fn build_get_offset_response(plan: &GetOffsetPlan) -> v2::GetOffsetResponse {
    v2::GetOffsetResponse {
        status: Some(plan.status.clone().into()),
        offset: plan.offset,
    }
}

pub fn build_query_offset_response(plan: &QueryOffsetPlan) -> v2::QueryOffsetResponse {
    v2::QueryOffsetResponse {
        status: Some(plan.status.clone().into()),
        offset: plan.offset,
    }
}

fn build_query_assignment_response_from_server(
    request: &v2::QueryAssignmentRequest,
    plan: &QueryAssignmentPlan,
) -> Option<Vec<v2::Assignment>> {
    let assignments = plan.assignments.as_ref()?;
    let broker_map = build_broker_map(&plan.route);
    let topic = request.topic.clone().unwrap_or_default();

    Some(
        assignments
            .iter()
            .filter_map(|assignment| build_assignment_from_server(&topic, &broker_map, &plan.route, assignment))
            .collect(),
    )
}

pub fn error_query_route_response(status: v2::Status) -> v2::QueryRouteResponse {
    v2::QueryRouteResponse {
        status: Some(status),
        message_queues: Vec::new(),
    }
}

pub fn error_query_assignment_response(status: v2::Status) -> v2::QueryAssignmentResponse {
    v2::QueryAssignmentResponse {
        status: Some(status),
        assignments: Vec::new(),
    }
}

pub fn error_send_message_response(status: v2::Status) -> v2::SendMessageResponse {
    v2::SendMessageResponse {
        status: Some(status),
        entries: Vec::new(),
    }
}

pub fn error_receive_message_responses(status: v2::Status) -> Vec<v2::ReceiveMessageResponse> {
    vec![v2::ReceiveMessageResponse {
        content: Some(v2::receive_message_response::Content::Status(status)),
    }]
}

pub fn error_pull_message_responses(status: v2::Status) -> Vec<v2::PullMessageResponse> {
    vec![v2::PullMessageResponse {
        content: Some(v2::pull_message_response::Content::Status(status)),
    }]
}

pub fn error_ack_message_response(status: v2::Status) -> v2::AckMessageResponse {
    v2::AckMessageResponse {
        status: Some(status),
        entries: Vec::new(),
    }
}

pub fn error_forward_message_to_dead_letter_queue_response(
    status: v2::Status,
) -> v2::ForwardMessageToDeadLetterQueueResponse {
    v2::ForwardMessageToDeadLetterQueueResponse { status: Some(status) }
}

pub fn error_change_invisible_duration_response(status: v2::Status) -> v2::ChangeInvisibleDurationResponse {
    v2::ChangeInvisibleDurationResponse {
        status: Some(status),
        receipt_handle: String::new(),
    }
}

pub fn error_end_transaction_response(status: v2::Status) -> v2::EndTransactionResponse {
    v2::EndTransactionResponse { status: Some(status) }
}

pub fn error_recall_message_response(status: v2::Status) -> v2::RecallMessageResponse {
    v2::RecallMessageResponse {
        status: Some(status),
        message_id: String::new(),
    }
}

pub fn error_update_offset_response(status: v2::Status) -> v2::UpdateOffsetResponse {
    v2::UpdateOffsetResponse { status: Some(status) }
}

pub fn error_get_offset_response(status: v2::Status) -> v2::GetOffsetResponse {
    v2::GetOffsetResponse {
        status: Some(status),
        offset: 0,
    }
}

pub fn error_query_offset_response(status: v2::Status) -> v2::QueryOffsetResponse {
    v2::QueryOffsetResponse {
        status: Some(status),
        offset: 0,
    }
}

fn resource_identity(resource: Option<&v2::Resource>, field: &'static str) -> ProxyResult<ResourceIdentity> {
    let resource = resource.ok_or_else(|| {
        rocketmq_error::RocketMQError::illegal_argument(format!("{field} resource must not be empty"))
    })?;
    if resource.name.trim().is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument(format!("{field} name must not be empty")).into());
    }

    Ok(ResourceIdentity::new(
        resource.resource_namespace.clone(),
        resource.name.clone(),
    ))
}

fn resolve_endpoints(config: &ProxyConfig, endpoints: Option<&v2::Endpoints>) -> ProxyResult<Vec<ResolvedEndpoint>> {
    let endpoints =
        endpoints.ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("endpoints must not be empty"))?;

    if endpoints.addresses.is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument("endpoints must not be empty").into());
    }

    let fallback_port = config.grpc.listen_port()?;
    let scheme = match v2::AddressScheme::try_from(endpoints.scheme) {
        Ok(v2::AddressScheme::IPv4) => ResolvedAddressScheme::Ipv4,
        Ok(v2::AddressScheme::IPv6) => ResolvedAddressScheme::Ipv6,
        Ok(v2::AddressScheme::DomainName) => ResolvedAddressScheme::DomainName,
        _ => ResolvedAddressScheme::Unspecified,
    };

    endpoints
        .addresses
        .iter()
        .map(|address| {
            if address.host.trim().is_empty() {
                return Err(rocketmq_error::RocketMQError::illegal_argument("endpoint host must not be empty").into());
            }

            let port = if config.grpc.use_endpoint_port_from_request {
                address.port
            } else {
                i32::from(fallback_port)
            };

            if !(0..=u16::MAX as i32).contains(&port) {
                return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                    "endpoint port out of range: {port}"
                ))
                .into());
            }

            Ok(ResolvedEndpoint {
                scheme,
                host: address.host.clone(),
                port: port as u16,
            })
        })
        .collect()
}

fn message_queue_broker(message_queue: &v2::MessageQueue) -> ProxyResult<(Option<String>, Option<String>)> {
    let Some(broker) = message_queue.broker.as_ref() else {
        return Ok((None, None));
    };

    let broker_name = (!broker.name.trim().is_empty()).then(|| broker.name.clone());
    let broker_addr = broker
        .endpoints
        .as_ref()
        .and_then(|endpoints| endpoints.addresses.first())
        .map(endpoint_address)
        .transpose()?;

    Ok((broker_name, broker_addr))
}

fn message_queue_target(
    message_queue: Option<&v2::MessageQueue>,
    field: &'static str,
) -> ProxyResult<MessageQueueTarget> {
    let message_queue = message_queue
        .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument(format!("{field} must not be empty")))?;
    if message_queue.id < 0 {
        return Err(ProxyError::illegal_offset(format!("{field}.id must not be negative")));
    }

    let (broker_name, broker_addr) = message_queue_broker(message_queue)?;
    Ok(MessageQueueTarget {
        topic: resource_identity(message_queue.topic.as_ref(), "messageQueue.topic")?,
        queue_id: message_queue.id,
        broker_name,
        broker_addr,
    })
}

fn endpoint_address(address: &v2::Address) -> ProxyResult<String> {
    if address.host.trim().is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument("broker endpoint host must not be empty").into());
    }
    if !(0..=u16::MAX as i32).contains(&address.port) {
        return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
            "broker endpoint port out of range: {}",
            address.port
        ))
        .into());
    }

    if address.host.contains(':') && !address.host.starts_with('[') {
        Ok(format!("[{}]:{}", address.host, address.port))
    } else {
        Ok(format!("{}:{}", address.host, address.port))
    }
}

fn build_filter_expression(filter_expression: Option<&v2::FilterExpression>) -> ProxyResult<ConsumerFilterExpression> {
    let Some(filter_expression) = filter_expression else {
        return Ok(ConsumerFilterExpression {
            expression_type: ExpressionType::TAG.to_owned(),
            expression: "*".to_owned(),
        });
    };

    match v2::FilterType::try_from(filter_expression.r#type).unwrap_or(v2::FilterType::Unspecified) {
        v2::FilterType::Unspecified | v2::FilterType::Tag => Ok(ConsumerFilterExpression {
            expression_type: ExpressionType::TAG.to_owned(),
            expression: default_tag_expression(filter_expression.expression.as_str()),
        }),
        v2::FilterType::Sql => {
            let expression = filter_expression.expression.trim();
            if expression.is_empty() {
                return Err(ProxyError::illegal_filter_expression(
                    "SQL filter expression must not be empty",
                ));
            }

            Ok(ConsumerFilterExpression {
                expression_type: ExpressionType::SQL92.to_owned(),
                expression: expression.to_owned(),
            })
        }
    }
}

fn default_tag_expression(expression: &str) -> String {
    let expression = expression.trim();
    if expression.is_empty() {
        "*".to_owned()
    } else {
        expression.to_owned()
    }
}

fn validate_batch_size(batch_size: i32) -> ProxyResult<u32> {
    if batch_size <= 0 {
        return Err(rocketmq_error::RocketMQError::illegal_argument("batchSize must be greater than zero").into());
    }

    Ok(batch_size as u32)
}

fn validate_offset(field: &'static str, offset: i64) -> ProxyResult<i64> {
    if offset < 0 {
        return Err(ProxyError::illegal_offset(format!("{field} must not be negative")));
    }

    Ok(offset)
}

fn validate_non_empty_string(field: &'static str, value: &str) -> ProxyResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument(format!("{field} must not be empty")).into());
    }

    Ok(trimmed.to_owned())
}

fn positive_duration<F>(duration: &prost_types::Duration, field: &'static str, invalid: F) -> ProxyResult<Duration>
where
    F: Fn(String) -> ProxyError + Copy,
{
    let duration = checked_duration(duration, field, invalid)?;
    if duration.is_zero() {
        return Err(invalid(format!("{field} must be greater than zero")));
    }
    Ok(duration)
}

fn non_negative_duration<F>(duration: &prost_types::Duration, field: &'static str, invalid: F) -> ProxyResult<Duration>
where
    F: Fn(String) -> ProxyError + Copy,
{
    checked_duration(duration, field, invalid)
}

fn checked_duration<F>(duration: &prost_types::Duration, field: &'static str, invalid: F) -> ProxyResult<Duration>
where
    F: Fn(String) -> ProxyError + Copy,
{
    if duration.seconds < 0 || duration.nanos < 0 {
        return Err(invalid(format!("{field} must not be negative")));
    }
    if duration.nanos >= 1_000_000_000 {
        return Err(invalid(format!("{field} nanos are out of range")));
    }

    let seconds = u64::try_from(duration.seconds).map_err(|_| invalid(format!("{field} seconds are out of range")))?;
    let nanos = u32::try_from(duration.nanos).map_err(|_| invalid(format!("{field} nanos are out of range")))?;
    Ok(Duration::new(seconds, nanos))
}

fn build_broker_map(route: &TopicRouteData) -> HashMap<String, HashMap<u64, v2::Broker>> {
    let mut broker_map = HashMap::new();

    for broker_data in &route.broker_datas {
        let mut broker_id_map = HashMap::new();
        let broker_name = broker_data.broker_name().to_string();
        for (broker_id, broker_addr) in broker_data.broker_addrs() {
            let (scheme, address) = parse_broker_address(broker_addr.as_str());
            let broker = v2::Broker {
                name: broker_name.clone(),
                id: *broker_id as i32,
                endpoints: Some(v2::Endpoints {
                    scheme: scheme as i32,
                    addresses: vec![address],
                }),
            };
            broker_id_map.insert(*broker_id, broker);
        }
        broker_map.insert(broker_name, broker_id_map);
    }

    broker_map
}

fn build_send_message_entry(message: &v2::Message) -> ProxyResult<SendMessageEntry> {
    let topic = resource_identity(message.topic.as_ref(), "topic")?;
    let system = message
        .system_properties
        .as_ref()
        .ok_or_else(|| rocketmq_error::RocketMQError::illegal_argument("message systemProperties must not be empty"))?;
    let client_message_id = validate_client_message_id(system.message_id.as_str())?;

    if message.body.is_empty() {
        return Err(rocketmq_error::RocketMQError::illegal_argument("message body must not be empty").into());
    }

    let effective_type = infer_message_type(system)?;
    let mut rocketmq_message = build_rocketmq_message(&topic, message, system, &client_message_id)?;
    apply_message_type(&mut rocketmq_message, system, effective_type)?;

    Ok(SendMessageEntry {
        topic,
        client_message_id,
        message: rocketmq_message,
        queue_id: explicit_queue_id(system.queue_id),
    })
}

fn validate_client_message_id(message_id: &str) -> ProxyResult<String> {
    let trimmed = message_id.trim();
    if trimmed.is_empty() {
        return Err(ProxyError::illegal_message_id(
            "message systemProperties.messageId must not be empty",
        ));
    }

    Ok(trimmed.to_owned())
}

fn validate_transaction_id(transaction_id: &str) -> ProxyResult<String> {
    let trimmed = transaction_id.trim();
    if trimmed.is_empty() {
        return Err(ProxyError::invalid_transaction_id("transactionId must not be empty"));
    }

    Ok(trimmed.to_owned())
}

fn infer_message_type(system: &v2::SystemProperties) -> ProxyResult<ProxyTopicMessageType> {
    let has_message_group = system
        .message_group
        .as_deref()
        .map(|group| !group.trim().is_empty())
        .unwrap_or(false);
    let has_delivery_timestamp = system.delivery_timestamp.is_some();

    match v2::MessageType::try_from(system.message_type).unwrap_or(v2::MessageType::Unspecified) {
        v2::MessageType::Unspecified | v2::MessageType::Normal => {
            if has_message_group && has_delivery_timestamp {
                return Err(ProxyError::message_property_conflict(
                    "FIFO and delay properties cannot be set on the same message",
                ));
            }

            if has_delivery_timestamp {
                Ok(ProxyTopicMessageType::Delay)
            } else if has_message_group {
                Ok(ProxyTopicMessageType::Fifo)
            } else {
                Ok(ProxyTopicMessageType::Normal)
            }
        }
        v2::MessageType::Fifo => {
            if has_delivery_timestamp {
                return Err(ProxyError::message_property_conflict(
                    "FIFO message does not support delivery timestamp",
                ));
            }

            Ok(ProxyTopicMessageType::Fifo)
        }
        v2::MessageType::Delay => {
            if has_message_group {
                return Err(ProxyError::message_property_conflict(
                    "delay message does not support message group",
                ));
            }

            Ok(ProxyTopicMessageType::Delay)
        }
        v2::MessageType::Transaction => {
            if has_message_group || has_delivery_timestamp {
                return Err(ProxyError::message_property_conflict(
                    "transaction message does not support message group or delivery timestamp",
                ));
            }

            Ok(ProxyTopicMessageType::Transaction)
        }
        v2::MessageType::Lite => Err(ProxyError::not_implemented("SendMessage(lite-topic)")),
        v2::MessageType::Priority => Err(ProxyError::not_implemented("SendMessage(priority)")),
    }
}

fn build_rocketmq_message(
    topic: &ResourceIdentity,
    message: &v2::Message,
    system: &v2::SystemProperties,
    client_message_id: &str,
) -> ProxyResult<Message> {
    if !matches!(
        v2::Encoding::try_from(system.body_encoding).unwrap_or(v2::Encoding::Unspecified),
        v2::Encoding::Unspecified | v2::Encoding::Identity
    ) {
        return Err(ProxyError::not_implemented("SendMessage(non-identity body encoding)"));
    }

    let mut rocketmq_message = Message::builder()
        .topic(topic.to_string())
        .body(message.body.clone())
        .build_unchecked();
    rocketmq_message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
        CheetahString::from(client_message_id),
    );

    if let Some(tag) = system.tag.as_deref() {
        let tag = tag.trim();
        if !tag.is_empty() {
            rocketmq_message.set_tags(CheetahString::from(tag));
        }
    }

    if !system.keys.is_empty() {
        rocketmq_message.set_keys(CheetahString::from(system.keys.join(MessageConst::KEY_SEPARATOR)));
    }

    if let Some(trace_context) = system.trace_context.as_deref() {
        let trace_context = trace_context.trim();
        if !trace_context.is_empty() {
            rocketmq_message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_CONTEXT),
                CheetahString::from(trace_context),
            );
        }
    }

    for (key, value) in &message.user_properties {
        rocketmq_message.put_user_property(CheetahString::from(key.as_str()), CheetahString::from(value.as_str()))?;
    }

    Ok(rocketmq_message)
}

fn apply_message_type(
    message: &mut Message,
    system: &v2::SystemProperties,
    message_type: ProxyTopicMessageType,
) -> ProxyResult<()> {
    match message_type {
        ProxyTopicMessageType::Normal => Ok(()),
        ProxyTopicMessageType::Fifo => {
            let message_group = system
                .message_group
                .as_deref()
                .map(str::trim)
                .filter(|group| !group.is_empty())
                .ok_or_else(|| {
                    ProxyError::illegal_message_group("FIFO message requires systemProperties.messageGroup")
                })?;
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_SHARDING_KEY),
                CheetahString::from(message_group),
            );
            Ok(())
        }
        ProxyTopicMessageType::Delay => {
            let deliver_time_ms = system
                .delivery_timestamp
                .as_ref()
                .map(timestamp_to_epoch_millis)
                .transpose()?
                .ok_or_else(|| {
                    ProxyError::illegal_delivery_time("delay message requires systemProperties.deliveryTimestamp")
                })?;
            message.set_deliver_time_ms(deliver_time_ms);
            Ok(())
        }
        ProxyTopicMessageType::Transaction => {
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_PREPARED),
                CheetahString::from_static_str("true"),
            );
            Ok(())
        }
        ProxyTopicMessageType::Mixed => Err(ProxyError::not_implemented("SendMessage(mixed-type topic)")),
        ProxyTopicMessageType::Lite => Err(ProxyError::not_implemented("SendMessage(lite-topic)")),
        ProxyTopicMessageType::Priority => Err(ProxyError::not_implemented("SendMessage(priority)")),
        ProxyTopicMessageType::Unspecified => Ok(()),
    }
}

fn timestamp_to_epoch_millis(timestamp: &prost_types::Timestamp) -> ProxyResult<u64> {
    if timestamp.seconds < 0 {
        return Err(ProxyError::illegal_delivery_time(
            "delivery timestamp must not be negative",
        ));
    }

    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(ProxyError::illegal_delivery_time(
            "delivery timestamp nanos are out of range",
        ));
    }

    let millis_from_seconds = (timestamp.seconds as u64)
        .checked_mul(1_000)
        .ok_or_else(|| ProxyError::illegal_delivery_time("delivery timestamp overflowed milliseconds"))?;
    let millis_from_nanos = (timestamp.nanos as u64) / 1_000_000;

    millis_from_seconds
        .checked_add(millis_from_nanos)
        .ok_or_else(|| ProxyError::illegal_delivery_time("delivery timestamp overflowed milliseconds"))
}

fn timestamp_to_offset_query_epoch_millis(timestamp: &prost_types::Timestamp) -> ProxyResult<i64> {
    if timestamp.seconds < 0 {
        return Err(ProxyError::illegal_offset(
            "query offset timestamp must not be negative",
        ));
    }

    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(ProxyError::illegal_offset(
            "query offset timestamp nanos are out of range",
        ));
    }

    let millis_from_seconds = timestamp
        .seconds
        .checked_mul(1_000)
        .ok_or_else(|| ProxyError::illegal_offset("query offset timestamp overflowed milliseconds"))?;
    let millis_from_nanos = i64::from(timestamp.nanos / 1_000_000);

    millis_from_seconds
        .checked_add(millis_from_nanos)
        .ok_or_else(|| ProxyError::illegal_offset("query offset timestamp overflowed milliseconds"))
}

fn explicit_queue_id(queue_id: i32) -> Option<i32> {
    (queue_id > 0).then_some(queue_id)
}

fn build_receive_message_response(message: &ReceivedMessage) -> v2::ReceiveMessageResponse {
    v2::ReceiveMessageResponse {
        content: Some(v2::receive_message_response::Content::Message(build_received_message(
            message,
        ))),
    }
}

fn build_received_message(message: &ReceivedMessage) -> v2::Message {
    build_message_from_ext(&message.message, Some(message.invisible_duration))
}

fn build_message_from_ext(message: &MessageExt, invisible_duration: Option<Duration>) -> v2::Message {
    v2::Message {
        topic: Some(resource_from_topic_name(message.topic().as_str())),
        user_properties: build_user_properties(message),
        system_properties: Some(build_message_system_properties(message, invisible_duration)),
        body: message.body().unwrap_or_default().to_vec(),
    }
}

fn build_message_system_properties(
    message_ext: &MessageExt,
    invisible_duration: Option<Duration>,
) -> v2::SystemProperties {
    let keys = message_ext
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_KEYS))
        .map(|value| {
            value
                .split(MessageConst::KEY_SEPARATOR)
                .filter(|key| !key.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default();

    v2::SystemProperties {
        tag: message_ext.get_tags().map(|tag| tag.to_string()),
        keys,
        message_id: message_ext.msg_id().to_string(),
        body_digest: None,
        body_encoding: v2::Encoding::Identity as i32,
        message_type: received_message_type(message_ext) as i32,
        born_timestamp: timestamp_from_epoch_millis(message_ext.born_timestamp()),
        born_host: message_ext.born_host().ip().to_string(),
        store_timestamp: timestamp_from_epoch_millis(message_ext.store_timestamp()),
        store_host: message_ext.store_host().ip().to_string(),
        delivery_timestamp: None,
        receipt_handle: message_ext
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
            .map(|value| value.to_string()),
        queue_id: message_ext.queue_id(),
        queue_offset: Some(message_ext.queue_offset()),
        invisible_duration: invisible_duration.and_then(duration_to_proto),
        delivery_attempt: Some(message_ext.reconsume_times().saturating_add(1)),
        message_group: message_ext
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_SHARDING_KEY))
            .map(|value| value.to_string()),
        trace_context: message_ext
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TRACE_CONTEXT))
            .map(|value| value.to_string()),
        orphaned_transaction_recovery_duration: None,
        dead_letter_queue: dead_letter_queue(message_ext),
        lite_topic: None,
        priority: None,
    }
}

fn build_user_properties(message: &MessageExt) -> HashMap<String, String> {
    message
        .properties()
        .iter()
        .filter(|(key, _)| !is_reserved_message_property(key.as_str()))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn is_reserved_message_property(key: &str) -> bool {
    STRING_HASH_SET.contains(key)
        || key.starts_with(MessageConst::PROPERTY_TRANSIENT_PREFIX)
        || key == MessageConst::PROPERTY_SHARDING_KEY
        || key == MessageConst::PROPERTY_TRACE_CONTEXT
}

fn dead_letter_queue(message: &MessageExt) -> Option<v2::DeadLetterQueue> {
    let topic = message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_DLQ_ORIGIN_TOPIC))?;
    let message_id = message.property(&CheetahString::from_static_str(
        MessageConst::PROPERTY_DLQ_ORIGIN_MESSAGE_ID,
    ))?;
    Some(v2::DeadLetterQueue {
        topic: topic.to_string(),
        message_id: message_id.to_string(),
    })
}

fn received_message_type(message: &MessageExt) -> v2::MessageType {
    if message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_SHARDING_KEY))
        .is_some()
    {
        return v2::MessageType::Fifo;
    }
    if message
        .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_OUT_MS))
        .is_some()
        || message
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS))
            .is_some()
    {
        return v2::MessageType::Delay;
    }
    if message
        .property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ))
        .is_some()
    {
        return v2::MessageType::Transaction;
    }
    v2::MessageType::Normal
}

fn resource_from_topic_name(topic: &str) -> v2::Resource {
    match topic.split_once('%') {
        Some((namespace, name)) if !namespace.is_empty() && !name.is_empty() => v2::Resource {
            resource_namespace: namespace.to_owned(),
            name: name.to_owned(),
        },
        _ => v2::Resource {
            resource_namespace: String::new(),
            name: topic.to_owned(),
        },
    }
}

fn timestamp_from_epoch_millis(epoch_millis: i64) -> Option<prost_types::Timestamp> {
    if epoch_millis <= 0 {
        return None;
    }

    Some(prost_types::Timestamp {
        seconds: epoch_millis.div_euclid(1_000),
        nanos: (epoch_millis.rem_euclid(1_000) as i32) * 1_000_000,
    })
}

fn duration_to_proto(duration: Duration) -> Option<prost_types::Duration> {
    Some(prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    })
}

fn build_ack_result_entry(entry: &AckMessageResultEntry) -> v2::AckMessageResultEntry {
    v2::AckMessageResultEntry {
        message_id: entry.message_id.clone(),
        receipt_handle: entry.receipt_handle.clone(),
        status: Some(entry.status.clone().into()),
    }
}

fn build_send_result_entry(result: &SendMessageResultEntry, request: &SendMessageEntry) -> v2::SendResultEntry {
    v2::SendResultEntry {
        status: Some(result.status.clone().into()),
        message_id: result
            .send_result
            .as_ref()
            .and_then(|send_result| send_result.msg_id.as_ref().map(ToString::to_string))
            .unwrap_or_else(|| request.client_message_id.clone()),
        transaction_id: result
            .send_result
            .as_ref()
            .and_then(|send_result| send_result.transaction_id.clone())
            .unwrap_or_default(),
        offset: result
            .send_result
            .as_ref()
            .map(|send_result| send_result.queue_offset as i64)
            .unwrap_or_default(),
        recall_handle: result
            .send_result
            .as_ref()
            .and_then(|send_result| send_result.recall_handle().map(ToOwned::to_owned))
            .unwrap_or_default(),
    }
}

fn summarize_send_response_status(entries: &[SendMessageResultEntry]) -> ProxyPayloadStatus {
    match entries {
        [] => ProxyStatusMapper::ok_payload(),
        [entry] => entry.status.clone(),
        _ if entries
            .iter()
            .all(|entry| entry.status.code() == entries[0].status.code()) =>
        {
            entries[0].status.clone()
        }
        _ => ProxyStatusMapper::from_payload_code(
            v2::Code::MultipleResults,
            "send message entries contain mixed success or failure results",
        ),
    }
}

fn summarize_ack_response_status(entries: &[AckMessageResultEntry]) -> ProxyPayloadStatus {
    match entries {
        [] => ProxyStatusMapper::ok_payload(),
        [entry] => entry.status.clone(),
        _ if entries
            .iter()
            .all(|entry| entry.status.code() == entries[0].status.code()) =>
        {
            entries[0].status.clone()
        }
        _ => ProxyStatusMapper::from_payload_code(
            v2::Code::MultipleResults,
            "ack entries contain mixed success or failure results",
        ),
    }
}

fn gen_message_queue_from_queue_data(
    queue_data: &QueueData,
    topic: &v2::Resource,
    topic_message_type: ProxyTopicMessageType,
    broker: v2::Broker,
) -> Vec<v2::MessageQueue> {
    let mut read_only = 0;
    let mut write_only = 0;
    let mut read_write = 0;
    let mut inaccessible = 0;

    if PermName::is_writeable(queue_data.perm) && PermName::is_readable(queue_data.perm) {
        read_write = queue_data.write_queue_nums.min(queue_data.read_queue_nums);
        read_only = queue_data.read_queue_nums - read_write;
        write_only = queue_data.write_queue_nums - read_write;
    } else if PermName::is_writeable(queue_data.perm) {
        write_only = queue_data.write_queue_nums;
    } else if PermName::is_readable(queue_data.perm) {
        read_only = queue_data.read_queue_nums;
    } else {
        inaccessible = queue_data.write_queue_nums.max(queue_data.read_queue_nums).max(1);
    }

    let mut queue_id = 0_i32;
    let mut result = Vec::new();

    for _ in 0..read_only {
        result.push(v2::MessageQueue {
            topic: Some(topic.clone()),
            id: queue_id,
            permission: v2::Permission::Read as i32,
            broker: Some(broker.clone()),
            accept_message_types: topic_message_types(topic_message_type),
        });
        queue_id += 1;
    }

    for _ in 0..write_only {
        result.push(v2::MessageQueue {
            topic: Some(topic.clone()),
            id: queue_id,
            permission: v2::Permission::Write as i32,
            broker: Some(broker.clone()),
            accept_message_types: topic_message_types(topic_message_type),
        });
        queue_id += 1;
    }

    for _ in 0..read_write {
        result.push(v2::MessageQueue {
            topic: Some(topic.clone()),
            id: queue_id,
            permission: v2::Permission::ReadWrite as i32,
            broker: Some(broker.clone()),
            accept_message_types: topic_message_types(topic_message_type),
        });
        queue_id += 1;
    }

    for _ in 0..inaccessible {
        result.push(v2::MessageQueue {
            topic: Some(topic.clone()),
            id: queue_id,
            permission: v2::Permission::None as i32,
            broker: Some(broker.clone()),
            accept_message_types: topic_message_types(topic_message_type),
        });
        queue_id += 1;
    }

    result
}

fn topic_message_types(topic_message_type: ProxyTopicMessageType) -> Vec<i32> {
    match topic_message_type {
        ProxyTopicMessageType::Normal => vec![v2::MessageType::Normal as i32],
        ProxyTopicMessageType::Fifo => vec![v2::MessageType::Fifo as i32],
        ProxyTopicMessageType::Delay => vec![v2::MessageType::Delay as i32],
        ProxyTopicMessageType::Transaction => vec![v2::MessageType::Transaction as i32],
        ProxyTopicMessageType::Mixed => vec![
            v2::MessageType::Normal as i32,
            v2::MessageType::Fifo as i32,
            v2::MessageType::Delay as i32,
            v2::MessageType::Transaction as i32,
        ],
        ProxyTopicMessageType::Lite => vec![v2::MessageType::Lite as i32],
        ProxyTopicMessageType::Priority => vec![v2::MessageType::Priority as i32],
        ProxyTopicMessageType::Unspecified => vec![v2::MessageType::Unspecified as i32],
    }
}

fn convert_permission(perm: u32) -> i32 {
    if PermName::is_readable(perm) && PermName::is_writeable(perm) {
        return v2::Permission::ReadWrite as i32;
    }
    if PermName::is_readable(perm) {
        return v2::Permission::Read as i32;
    }
    if PermName::is_writeable(perm) {
        return v2::Permission::Write as i32;
    }
    v2::Permission::None as i32
}

fn build_assignment_from_server(
    topic: &v2::Resource,
    broker_map: &HashMap<String, HashMap<u64, v2::Broker>>,
    route: &TopicRouteData,
    assignment: &MessageQueueAssignment,
) -> Option<v2::Assignment> {
    let message_queue = assignment.message_queue.as_ref()?;
    let broker_name = message_queue.broker_name().to_string();
    let broker = broker_map
        .get(broker_name.as_str())
        .and_then(|entries| entries.get(&MASTER_ID))
        .cloned()
        .or_else(|| {
            broker_map
                .get(broker_name.as_str())
                .and_then(|entries| entries.values().next().cloned())
        })?;
    let permission = route
        .queue_datas
        .iter()
        .find(|queue_data| queue_data.broker_name.as_str() == broker_name)
        .map(|queue_data| permission_for_queue_id(queue_data, message_queue.queue_id()))
        .unwrap_or(v2::Permission::ReadWrite as i32);

    Some(v2::Assignment {
        message_queue: Some(v2::MessageQueue {
            topic: Some(topic.clone()),
            id: message_queue.queue_id(),
            permission,
            broker: Some(broker),
            accept_message_types: Vec::new(),
        }),
    })
}

fn permission_for_queue_id(queue_data: &QueueData, queue_id: i32) -> i32 {
    if queue_id < 0 {
        return convert_permission(queue_data.perm);
    }

    let queue_id = queue_id as u32;
    if PermName::is_readable(queue_data.perm) && PermName::is_writeable(queue_data.perm) {
        let read_write = queue_data.write_queue_nums.min(queue_data.read_queue_nums);
        if queue_id < read_write {
            return v2::Permission::ReadWrite as i32;
        }
        if queue_id < queue_data.read_queue_nums {
            return v2::Permission::Read as i32;
        }
        if queue_id < queue_data.write_queue_nums {
            return v2::Permission::Write as i32;
        }
        return v2::Permission::None as i32;
    }

    convert_permission(queue_data.perm)
}

fn parse_broker_address(address: &str) -> (v2::AddressScheme, v2::Address) {
    if let Ok(parsed) = SocketAddr::from_str(address) {
        let scheme = match parsed.ip() {
            IpAddr::V4(_) => v2::AddressScheme::IPv4,
            IpAddr::V6(_) => v2::AddressScheme::IPv6,
        };
        return (
            scheme,
            v2::Address {
                host: parsed.ip().to_string(),
                port: i32::from(parsed.port()),
            },
        );
    }

    if let Some((host, port)) = address.rsplit_once(':') {
        if let Ok(port) = port.parse::<i32>() {
            let scheme = if host.parse::<IpAddr>().map(|ip| ip.is_ipv4()).unwrap_or(false) {
                v2::AddressScheme::IPv4
            } else if host.parse::<IpAddr>().map(|ip| ip.is_ipv6()).unwrap_or(false) {
                v2::AddressScheme::IPv6
            } else {
                v2::AddressScheme::DomainName
            };

            return (
                scheme,
                v2::Address {
                    host: host.trim_matches(&['[', ']'][..]).to_owned(),
                    port,
                },
            );
        }
    }

    (
        v2::AddressScheme::DomainName,
        v2::Address {
            host: address.to_owned(),
            port: 0,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_client_rust::producer::send_result::SendResult;
    use rocketmq_client_rust::producer::send_status::SendStatus;
    use rocketmq_common::common::message::message_enum::MessageRequestMode;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
    use rocketmq_common::common::message::message_single::Message;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

    use super::build_query_assignment_response;
    use super::build_query_route_response;
    use super::build_send_message_request;
    use super::build_send_message_response;
    use super::gen_message_queue_from_queue_data;
    use crate::context::ProxyContext;
    use crate::processor::QueryAssignmentPlan;
    use crate::processor::QueryRoutePlan;
    use crate::processor::SendMessagePlan;
    use crate::processor::SendMessageResultEntry;
    use crate::proto::v2;
    use crate::service::ProxyTopicMessageType;
    use crate::service::ResourceIdentity;
    use crate::service::SubscriptionGroupMetadata;
    use crate::status::ProxyStatusMapper;

    #[test]
    fn route_response_preserves_permission_split() {
        let queue_data = QueueData::new(CheetahString::from("broker-a"), 2, 1, 6, 0);
        let broker = v2::Broker {
            name: "broker-a".to_owned(),
            id: 0,
            endpoints: None,
        };

        let generated = gen_message_queue_from_queue_data(
            &queue_data,
            &v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            },
            ProxyTopicMessageType::Normal,
            broker,
        );

        assert_eq!(generated.len(), 2);
        assert_eq!(generated[0].permission, v2::Permission::Read as i32);
        assert_eq!(generated[1].permission, v2::Permission::ReadWrite as i32);
    }

    #[test]
    fn route_response_builds_queue_entries() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 1, 1, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        };
        let response = build_query_route_response(
            &v2::QueryRouteRequest {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                endpoints: Some(v2::Endpoints::default()),
            },
            &QueryRoutePlan {
                route,
                topic_message_type: ProxyTopicMessageType::Normal,
            },
        );

        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.message_queues.len(), 1);
    }

    #[test]
    fn query_assignment_prefers_server_side_assignments_when_present() {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0_u64, CheetahString::from("127.0.0.1:10911"));
        let route = TopicRouteData {
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 2, 2, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            ..Default::default()
        };

        let response = build_query_assignment_response(
            &v2::QueryAssignmentRequest {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                group: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "GroupA".to_owned(),
                }),
                endpoints: Some(v2::Endpoints::default()),
            },
            &QueryAssignmentPlan {
                route,
                assignments: Some(vec![MessageQueueAssignment {
                    message_queue: Some(MessageQueue::from_parts("TopicA", "broker-a", 1)),
                    mode: MessageRequestMode::Pull,
                    attachments: None,
                }]),
                subscription_group: Some(SubscriptionGroupMetadata::default()),
            },
        );

        assert_eq!(response.status.unwrap().code, v2::Code::Ok as i32);
        assert_eq!(response.assignments.len(), 1);
        assert_eq!(response.assignments[0].message_queue.as_ref().unwrap().id, 1);
    }

    #[test]
    fn build_send_message_request_maps_fifo_properties() {
        let context = ProxyContext::from_grpc_request("SendMessage", &tonic::Request::new(()))
            .expect("context should be constructed");
        let request = v2::SendMessageRequest {
            messages: vec![v2::Message {
                topic: Some(v2::Resource {
                    resource_namespace: "ns".to_owned(),
                    name: "TopicA".to_owned(),
                }),
                user_properties: HashMap::from([("k".to_owned(), "v".to_owned())]),
                system_properties: Some(v2::SystemProperties {
                    tag: Some("TagA".to_owned()),
                    keys: vec!["KeyA".to_owned(), "KeyB".to_owned()],
                    message_id: "msg-1".to_owned(),
                    body_encoding: v2::Encoding::Identity as i32,
                    message_group: Some("group-a".to_owned()),
                    trace_context: Some("trace-a".to_owned()),
                    ..Default::default()
                }),
                body: Bytes::from_static(b"hello").to_vec(),
            }],
        };

        let mapped = build_send_message_request(&context, &request).unwrap();
        let message = &mapped.messages[0].message;
        assert_eq!(mapped.messages[0].topic, ResourceIdentity::new("ns", "TopicA"));
        assert_eq!(message.topic().as_str(), "ns%TopicA");
        assert_eq!(message.get_tags().unwrap().as_str(), "TagA");
        assert_eq!(
            message
                .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_SHARDING_KEY))
                .unwrap(),
            CheetahString::from("group-a")
        );
        assert_eq!(
            message.get_user_property(CheetahString::from("k")).unwrap().as_str(),
            "v"
        );
    }

    #[test]
    fn build_send_message_request_rejects_missing_message_id() {
        let context = ProxyContext::from_grpc_request("SendMessage", &tonic::Request::new(()))
            .expect("context should be constructed");
        let request = v2::SendMessageRequest {
            messages: vec![v2::Message {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                user_properties: HashMap::new(),
                system_properties: Some(v2::SystemProperties::default()),
                body: Bytes::from_static(b"hello").to_vec(),
            }],
        };

        let error = build_send_message_request(&context, &request).unwrap_err();
        let status = ProxyStatusMapper::from_error(&error);
        assert_eq!(status.code, v2::Code::IllegalMessageId as i32);
    }

    #[test]
    fn send_message_response_maps_send_status() {
        let mut send_result = SendResult::new(
            SendStatus::FlushDiskTimeout,
            Some(CheetahString::from("server-msg-id")),
            None,
            None,
            12,
        );
        send_result.set_recall_handle("recall-handle".to_string());
        let response = build_send_message_response(
            &SendMessagePlan {
                entries: vec![SendMessageResultEntry {
                    status: ProxyStatusMapper::from_send_result_payload(&send_result),
                    send_result: Some(send_result),
                }],
            },
            &crate::processor::SendMessageRequest {
                messages: vec![crate::processor::SendMessageEntry {
                    topic: ResourceIdentity::new("", "TopicA"),
                    client_message_id: "client-msg-id".to_owned(),
                    message: Message::builder()
                        .topic("TopicA")
                        .body(Bytes::from_static(b"hello"))
                        .build_unchecked(),
                    queue_id: None,
                }],
                timeout: None,
            },
        );

        assert_eq!(response.status.unwrap().code, v2::Code::MasterPersistenceTimeout as i32);
        assert_eq!(response.entries[0].message_id, "server-msg-id");
        assert_eq!(response.entries[0].recall_handle, "recall-handle");
        assert_eq!(
            response.entries[0].status.as_ref().unwrap().code,
            v2::Code::MasterPersistenceTimeout as i32
        );
    }

    #[test]
    fn send_message_response_uses_shared_failure_code_when_all_entries_fail_same_way() {
        let response = build_send_message_response(
            &SendMessagePlan {
                entries: vec![
                    SendMessageResultEntry {
                        status: ProxyStatusMapper::from_payload_code(v2::Code::TopicNotFound, "topic a missing"),
                        send_result: None,
                    },
                    SendMessageResultEntry {
                        status: ProxyStatusMapper::from_payload_code(v2::Code::TopicNotFound, "topic b missing"),
                        send_result: None,
                    },
                ],
            },
            &crate::processor::SendMessageRequest {
                messages: vec![
                    crate::processor::SendMessageEntry {
                        topic: ResourceIdentity::new("", "TopicA"),
                        client_message_id: "client-msg-id-1".to_owned(),
                        message: Message::builder()
                            .topic("TopicA")
                            .body(Bytes::from_static(b"hello"))
                            .build_unchecked(),
                        queue_id: None,
                    },
                    crate::processor::SendMessageEntry {
                        topic: ResourceIdentity::new("", "TopicA"),
                        client_message_id: "client-msg-id-2".to_owned(),
                        message: Message::builder()
                            .topic("TopicA")
                            .body(Bytes::from_static(b"world"))
                            .build_unchecked(),
                        queue_id: None,
                    },
                ],
                timeout: None,
            },
        );

        assert_eq!(response.status.unwrap().code, v2::Code::TopicNotFound as i32);
    }
}
