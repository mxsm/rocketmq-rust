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

use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::config::ProxyConfig;
use crate::context::ProxyContext;
use crate::context::ResolvedAddressScheme;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::QueryAssignmentPlan;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryRoutePlan;
use crate::processor::QueryRouteRequest;
use crate::processor::SendMessageEntry;
use crate::processor::SendMessagePlan;
use crate::processor::SendMessageRequest;
use crate::processor::SendMessageResultEntry;
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
        v2::MessageType::Transaction => Err(ProxyError::not_implemented("SendMessage(transaction)")),
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
        ProxyTopicMessageType::Transaction => Err(ProxyError::not_implemented("SendMessage(transaction)")),
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

fn explicit_queue_id(queue_id: i32) -> Option<i32> {
    (queue_id > 0).then_some(queue_id)
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
        recall_handle: String::new(),
    }
}

fn summarize_send_response_status(entries: &[SendMessageResultEntry]) -> ProxyPayloadStatus {
    match entries {
        [] => ProxyStatusMapper::ok_payload(),
        [entry] => entry.status.clone(),
        _ if entries.iter().all(|entry| entry.status.is_ok()) => ProxyStatusMapper::ok_payload(),
        _ => ProxyStatusMapper::from_payload_code(
            v2::Code::MultipleResults,
            "send message entries contain mixed success or failure results",
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
        let context = ProxyContext::from_grpc_request("SendMessage", &tonic::Request::new(()));
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
        let context = ProxyContext::from_grpc_request("SendMessage", &tonic::Request::new(()));
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
        let response = build_send_message_response(
            &SendMessagePlan {
                entries: vec![SendMessageResultEntry {
                    status: ProxyStatusMapper::from_send_result_payload(&SendResult::new(
                        SendStatus::FlushDiskTimeout,
                        Some(CheetahString::from("server-msg-id")),
                        None,
                        None,
                        12,
                    )),
                    send_result: Some(SendResult::new(
                        SendStatus::FlushDiskTimeout,
                        Some(CheetahString::from("server-msg-id")),
                        None,
                        None,
                        12,
                    )),
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
        assert_eq!(
            response.entries[0].status.as_ref().unwrap().code,
            v2::Code::MasterPersistenceTimeout as i32
        );
    }
}
