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

use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::mix_all::MASTER_ID;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

use crate::config::ProxyConfig;
use crate::context::ResolvedAddressScheme;
use crate::context::ResolvedEndpoint;
use crate::error::ProxyResult;
use crate::processor::QueryAssignmentPlan;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryRoutePlan;
use crate::processor::QueryRouteRequest;
use crate::proto::v2;
use crate::service::ProxyTopicMessageType;
use crate::service::ResourceIdentity;
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

    use cheetah_string::CheetahString;
    use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
    use rocketmq_remoting::protocol::route::route_data_view::QueueData;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;

    use super::build_query_route_response;
    use super::gen_message_queue_from_queue_data;
    use crate::processor::QueryRoutePlan;
    use crate::proto::v2;
    use crate::service::ProxyTopicMessageType;

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
}
