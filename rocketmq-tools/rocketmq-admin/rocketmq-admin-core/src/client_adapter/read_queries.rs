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

//! Shared production implementation for narrow read-client queries.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminMessageReadExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_protocol::protocol::body::producer_connection::ProducerConnection;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::core::client_connection::ClientConnectionObservation;
use crate::core::client_connection::QueryTopicProducerConnectionsRequest;
use crate::core::client_connection::QueryTopicProducerConnectionsResult;
use crate::core::client_connection::MAX_TOPIC_PRODUCER_BROKERS;
use crate::core::config_state::ConsumerGroupConfigStateRequest;
use crate::core::config_state::ConsumerGroupConfigStateResult;
use crate::core::config_state::ConsumerGroupConfigStateRow;
use crate::core::config_state::TopicConfigStateRequest;
use crate::core::config_state::TopicConfigStateResult;
use crate::core::config_state::TopicConfigStateRow;
use crate::core::message::MessageMetadata;
use crate::core::message::MessageMetadataRequest;
use crate::core::query::AdminQueryFailureCode;
use crate::core::query::AdminQueryResult;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::AdminError;
use crate::core::AdminResult;

type BrokerTarget = (String, CheetahString);
type BrokerTargetResolution = (Vec<BrokerTarget>, Vec<AdminSourceFailure>);

pub(crate) async fn query_topic_producer_connections(
    admin: &DefaultMQAdminExt,
    request: &QueryTopicProducerConnectionsRequest,
) -> AdminResult<AdminQueryResult<QueryTopicProducerConnectionsResult>> {
    let request = QueryTopicProducerConnectionsRequest::try_new(
        request.cluster.clone(),
        request.topic.clone(),
        request.producer_group.clone(),
        request.max_connections,
    )?;
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let route = admin
        .examine_topic_route_info(CheetahString::from(request.topic.as_str()))
        .await
        .map_err(|error| backend_error("examine_topic_route_info", error))?
        .ok_or_else(|| AdminError::not_found("topic", request.topic.clone()))?;
    let (targets, mut failures) = topic_producer_targets(
        &cluster_info,
        &route,
        &request.cluster,
        AdminQuerySource::ProducerConnection,
    )?;

    let mut rows = BTreeMap::new();
    let mut successful_sources = 0usize;
    let mut queried_brokers = failures
        .iter()
        .map(|failure| failure.logical_target().to_string())
        .collect::<BTreeSet<_>>();
    let mut truncated = false;
    for (broker_name, broker_addr) in targets {
        queried_brokers.insert(broker_name.clone());
        match admin
            .observe_producer_connection_at(CheetahString::from(request.producer_group.as_str()), broker_addr)
            .await
        {
            Ok(connection) => {
                successful_sources += 1;
                truncated |=
                    insert_bounded_producer_connections(&mut rows, &broker_name, connection, request.max_connections);
            }
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ProducerConnection,
                &broker_name,
                &error,
            )),
        }
    }

    let failed_brokers = failure_targets(&failures);
    AdminQueryResult::from_sources(
        QueryTopicProducerConnectionsResult {
            topic: request.topic.clone(),
            producer_group: request.producer_group.clone(),
            connections: rows.into_values().collect(),
            queried_broker_count: queried_brokers.len(),
            failed_brokers,
            truncated,
        },
        successful_sources,
        failures,
    )
}

pub(crate) async fn query_message_metadata(
    admin: &DefaultMQAdminExt,
    request: &MessageMetadataRequest,
) -> AdminResult<MessageMetadata> {
    let request = MessageMetadataRequest::try_new(request.cluster.clone(), request.message_id.clone())?;
    let endpoint = decode_message_endpoint(request.message_id.as_str())?;
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    if !cluster_advertises_endpoint(&cluster_info, &request.cluster, endpoint)? {
        return Err(AdminError::invalid_argument(
            "message_id",
            "target is not advertised by the selected cluster",
        ));
    }
    let metadata = admin
        .query_message_metadata_by_id(CheetahString::from(request.message_id.as_str()))
        .await
        .map_err(|error| backend_error("query_message_metadata", error))?;
    Ok(MessageMetadata {
        topic: metadata.topic,
        message_id: metadata.message_id,
        unique_message_id: metadata.unique_message_id,
        born_timestamp: metadata.born_timestamp,
        store_timestamp: metadata.store_timestamp,
        queue_id: metadata.queue_id,
        queue_offset: metadata.queue_offset,
        store_size: metadata.store_size,
        reconsume_times: metadata.reconsume_times,
        sys_flag: metadata.sys_flag,
        flag: metadata.flag,
        prepared_transaction_offset: metadata.prepared_transaction_offset,
    })
}

pub(crate) async fn query_topic_config_state(
    admin: &DefaultMQAdminExt,
    request: &TopicConfigStateRequest,
) -> AdminResult<AdminQueryResult<TopicConfigStateResult>> {
    let request = TopicConfigStateRequest::try_new(
        request.cluster.clone(),
        request.topic.clone(),
        request.broker_names.clone(),
    )?;
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let (targets, mut failures) = cluster_master_targets(
        &cluster_info,
        &request.cluster,
        &request.broker_names,
        AdminQuerySource::TopicConfig,
    )?;
    let mut states = Vec::new();
    let mut successful_sources = 0usize;
    for (broker_name, broker_addr) in targets {
        match admin
            .topic_config_with_version(broker_addr, CheetahString::from(request.topic.as_str()))
            .await
        {
            Ok(snapshot) => {
                successful_sources += 1;
                states.push(TopicConfigStateRow {
                    broker_name,
                    version: snapshot.version,
                    read_queue_nums: snapshot.config.read_queue_nums,
                    write_queue_nums: snapshot.config.write_queue_nums,
                    order: snapshot.config.order,
                });
            }
            Err(error) => failures.push(source_failure(AdminQuerySource::TopicConfig, &broker_name, &error)),
        }
    }
    states.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    AdminQueryResult::from_sources(
        TopicConfigStateResult {
            topic: request.topic.clone(),
            states,
        },
        successful_sources,
        failures,
    )
}

pub(crate) async fn query_consumer_group_config_state(
    admin: &DefaultMQAdminExt,
    request: &ConsumerGroupConfigStateRequest,
) -> AdminResult<AdminQueryResult<ConsumerGroupConfigStateResult>> {
    let request = ConsumerGroupConfigStateRequest::try_new(
        request.cluster.clone(),
        request.group.clone(),
        request.broker_names.clone(),
    )?;
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let (targets, mut failures) = cluster_master_targets(
        &cluster_info,
        &request.cluster,
        &request.broker_names,
        AdminQuerySource::ConsumerGroupConfig,
    )?;
    let mut states = Vec::new();
    let mut successful_sources = 0usize;
    for (broker_name, broker_addr) in targets {
        match admin
            .subscription_group_config_with_version(broker_addr, CheetahString::from(request.group.as_str()))
            .await
        {
            Ok(snapshot) => match consumer_group_state_row(&broker_name, snapshot) {
                Some(row) => {
                    successful_sources += 1;
                    states.push(row);
                }
                None => failures.push(AdminSourceFailure::new(
                    AdminQuerySource::ConsumerGroupConfig,
                    AdminQueryFailureCode::InvalidResponse,
                    false,
                    &broker_name,
                )),
            },
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ConsumerGroupConfig,
                &broker_name,
                &error,
            )),
        }
    }
    states.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    AdminQueryResult::from_sources(
        ConsumerGroupConfigStateResult {
            group: request.group.clone(),
            states,
        },
        successful_sources,
        failures,
    )
}

fn topic_producer_targets(
    cluster_info: &ClusterInfo,
    route: &TopicRouteData,
    cluster: &str,
    source: AdminQuerySource,
) -> AdminResult<BrokerTargetResolution> {
    let route_brokers = route
        .broker_datas
        .iter()
        .map(|broker| broker.broker_name().to_string())
        .collect::<BTreeSet<_>>();
    let cluster_brokers = cluster_broker_names(cluster_info, cluster)?;
    let requested = cluster_brokers
        .intersection(&route_brokers)
        .cloned()
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Err(AdminError::not_found("topic route in selected cluster", cluster));
    }
    if requested.len() > MAX_TOPIC_PRODUCER_BROKERS {
        return Err(AdminError::backend_view(
            "query_topic_producer_connections",
            "TOPIC_PRODUCER_TARGET_LIMIT_EXCEEDED",
            "Topic route has too many selected-cluster Broker targets",
            None,
            422,
            false,
        ));
    }
    cluster_master_targets(cluster_info, cluster, &requested, source)
}

fn decode_message_endpoint(message_id: &str) -> AdminResult<SocketAddr> {
    MessageDecoder::decode_message_id(message_id)
        .map(|decoded| decoded.address)
        .map_err(|_| AdminError::invalid_argument("message_id", "must be a valid offset message identifier"))
}

fn cluster_master_targets(
    cluster_info: &ClusterInfo,
    cluster: &str,
    broker_names: &[String],
    source: AdminQuerySource,
) -> AdminResult<BrokerTargetResolution> {
    let cluster_names = cluster_broker_names(cluster_info, cluster)?;
    let broker_table = cluster_info.broker_addr_table.as_ref();
    let mut targets = Vec::new();
    let mut failures = Vec::new();
    for broker_name in broker_names {
        if !is_safe_broker_name(broker_name) {
            failures.push(AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name,
            ));
            continue;
        }
        if !cluster_names.contains(broker_name) {
            failures.push(AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::NotFound,
                false,
                broker_name,
            ));
            continue;
        }
        let broker = broker_table.and_then(|table| table.get(broker_name.as_str()));
        let master = broker.and_then(|broker| broker.broker_addrs().get(&mix_all::MASTER_ID));
        match master.filter(|address| !address.trim().is_empty()) {
            Some(address) => targets.push((broker_name.clone(), address.clone())),
            None => failures.push(AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name,
            )),
        }
    }
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    Ok((targets, failures))
}

fn is_safe_broker_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.is_ascii()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.'))
}

fn cluster_broker_names(cluster_info: &ClusterInfo, cluster: &str) -> AdminResult<BTreeSet<String>> {
    cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|table| table.get(cluster))
        .map(|names| names.iter().map(ToString::to_string).collect())
        .ok_or_else(|| AdminError::not_found("cluster", cluster))
}

fn cluster_advertises_endpoint(cluster_info: &ClusterInfo, cluster: &str, endpoint: SocketAddr) -> AdminResult<bool> {
    let broker_names = cluster_broker_names(cluster_info, cluster)?;
    let Some(broker_table) = cluster_info.broker_addr_table.as_ref() else {
        return Ok(false);
    };
    Ok(broker_names.iter().any(|broker_name| {
        broker_table.get(broker_name.as_str()).is_some_and(|broker| {
            broker
                .broker_addrs()
                .values()
                .filter_map(|address| address.trim().parse::<SocketAddr>().ok())
                .any(|advertised| advertised == endpoint)
        })
    }))
}

type ConnectionIdentity = (String, String, String, String, i32);

fn insert_bounded_producer_connections(
    rows: &mut BTreeMap<ConnectionIdentity, ClientConnectionObservation>,
    broker_name: &str,
    connection: ProducerConnection,
    limit: usize,
) -> bool {
    let mut truncated = false;
    for item in connection.connection_set() {
        let row = ClientConnectionObservation {
            broker_name: broker_name.to_owned(),
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            last_update_timestamp: None,
        };
        rows.entry(connection_identity(&row)).or_insert(row);
        if rows.len() > limit {
            rows.pop_last();
            truncated = true;
        }
    }
    truncated
}

fn connection_identity(connection: &ClientConnectionObservation) -> ConnectionIdentity {
    (
        connection.broker_name.clone(),
        connection.client_id.clone(),
        connection.client_addr.clone(),
        connection.language.clone(),
        connection.version,
    )
}

fn consumer_group_state_row(
    broker_name: &str,
    snapshot: rocketmq_client_rust::SubscriptionGroupConfigVersioned,
) -> Option<ConsumerGroupConfigStateRow> {
    let config = snapshot.config;
    Some(ConsumerGroupConfigStateRow {
        broker_name: broker_name.to_owned(),
        version: snapshot.version,
        retry_max_times: bounded_positive(config.retry_max_times(), 16)?,
        retry_queue_nums: bounded_positive(config.retry_queue_nums(), 8)?,
        consume_timeout_minutes: bounded_positive(config.consume_timeout_minute(), 1_440)?,
        consume_enable: config.consume_enable(),
        consume_from_min_enable: config.consume_from_min_enable(),
        consume_broadcast_enable: config.consume_broadcast_enable(),
        consume_message_orderly: config.consume_message_orderly(),
        broker_id: config.broker_id(),
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly(),
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable(),
        group_sys_flag: config.group_sys_flag(),
    })
}

fn bounded_positive(value: i32, maximum: u32) -> Option<u32> {
    let value = u32::try_from(value).ok()?;
    (1..=maximum).contains(&value).then_some(value)
}

fn failure_targets(failures: &[AdminSourceFailure]) -> Vec<String> {
    failures
        .iter()
        .map(|failure| failure.logical_target().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn source_failure(source: AdminQuerySource, broker_name: &str, error: &RocketMQError) -> AdminSourceFailure {
    let view = error.boundary_view();
    let code = match view.http().status.as_u16() {
        401 | 403 => AdminQueryFailureCode::PermissionDenied,
        404 => AdminQueryFailureCode::NotFound,
        408 | 504 => AdminQueryFailureCode::Timeout,
        429 => AdminQueryFailureCode::RateLimited,
        400 | 413 | 422 => AdminQueryFailureCode::InvalidResponse,
        _ => AdminQueryFailureCode::SourceUnavailable,
    };
    AdminSourceFailure::new(source, code, view.is_retryable(), broker_name)
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
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rocketmq_protocol::protocol::body::connection::Connection;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::LanguageCode;

    use super::*;

    fn cluster_info() -> ClusterInfo {
        ClusterInfo::new(
            Some(HashMap::from([
                (
                    "broker-a".into(),
                    BrokerData::new(
                        "cluster-a".into(),
                        "broker-a".into(),
                        HashMap::from([(mix_all::MASTER_ID, "127.0.0.1:10911".into())]),
                        None,
                    ),
                ),
                (
                    "broker-b".into(),
                    BrokerData::new(
                        "cluster-b".into(),
                        "broker-b".into(),
                        HashMap::from([(mix_all::MASTER_ID, "127.0.0.2:10911".into())]),
                        None,
                    ),
                ),
            ])),
            Some(HashMap::from([
                ("cluster-a".into(), HashSet::from(["broker-a".into()])),
                ("cluster-b".into(), HashSet::from(["broker-b".into()])),
            ])),
        )
    }

    #[test]
    fn producer_targets_intersect_topic_route_with_selected_cluster() {
        let route = TopicRouteData {
            broker_datas: vec![
                BrokerData::new("cluster-a".into(), "broker-a".into(), HashMap::new(), None),
                BrokerData::new("cluster-b".into(), "broker-b".into(), HashMap::new(), None),
            ],
            ..Default::default()
        };

        let (targets, failures) = topic_producer_targets(
            &cluster_info(),
            &route,
            "cluster-a",
            AdminQuerySource::ProducerConnection,
        )
        .unwrap();
        assert_eq!(targets, [("broker-a".to_owned(), "127.0.0.1:10911".into())]);
        assert!(failures.is_empty());
    }

    #[test]
    fn producer_target_resolution_fails_closed_for_cross_cluster_topic() {
        let route = TopicRouteData {
            broker_datas: vec![BrokerData::new(
                "cluster-b".into(),
                "broker-b".into(),
                HashMap::new(),
                None,
            )],
            ..Default::default()
        };

        let error = topic_producer_targets(
            &cluster_info(),
            &route,
            "cluster-a",
            AdminQuerySource::ProducerConnection,
        )
        .unwrap_err();
        assert!(matches!(error, AdminError::NotFound { .. }));
    }

    #[test]
    fn message_endpoint_must_be_advertised_by_exact_selected_cluster() {
        let topology = cluster_info();
        assert!(cluster_advertises_endpoint(&topology, "cluster-a", "127.0.0.1:10911".parse().unwrap()).unwrap());
        assert!(!cluster_advertises_endpoint(&topology, "cluster-a", "127.0.0.2:10911".parse().unwrap()).unwrap());
        assert!(cluster_advertises_endpoint(&topology, "missing", "127.0.0.1:10911".parse().unwrap()).is_err());
    }

    #[test]
    fn invalid_message_identifier_fails_without_echoing_input() {
        let error = decode_message_endpoint("not-a-message-id").unwrap_err();
        assert!(matches!(
            error,
            AdminError::InvalidArgument {
                field: "message_id",
                ..
            }
        ));
        assert!(!error.to_string().contains("not-a-message-id"));
    }

    #[test]
    fn config_targets_are_sorted_and_missing_targets_become_logical_evidence() {
        let (targets, failures) = cluster_master_targets(
            &cluster_info(),
            "cluster-a",
            &["broker-missing".to_owned(), "broker-a".to_owned()],
            AdminQuerySource::TopicConfig,
        )
        .unwrap();

        assert_eq!(targets, [("broker-a".to_owned(), "127.0.0.1:10911".into())]);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].logical_target(), "broker-missing");
        assert_eq!(failures[0].code(), AdminQueryFailureCode::NotFound);
    }

    #[test]
    fn producer_rows_are_deduplicated_deterministic_and_memory_bounded() {
        let connection = |client_id: &str| {
            let mut row = Connection::new();
            row.set_client_id(client_id.into());
            row.set_client_addr(format!("{client_id}:12000").into());
            row.set_language(LanguageCode::RUST);
            row.set_version(1);
            row
        };
        let mut observed = ProducerConnection::new();
        observed.connection_set_mut().extend([
            connection("client-z"),
            connection("client-a"),
            connection("client-m"),
            connection("client-a"),
        ]);
        let mut rows = BTreeMap::new();

        assert!(insert_bounded_producer_connections(&mut rows, "broker-a", observed, 2,));
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.into_values().map(|row| row.client_id).collect::<Vec<_>>(),
            ["client-a", "client-m"]
        );
    }
}
