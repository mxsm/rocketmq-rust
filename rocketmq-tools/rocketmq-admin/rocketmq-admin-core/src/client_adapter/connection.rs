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
use std::collections::BTreeSet;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::client_connection::ClientConnectionObservation;
use crate::core::client_connection::ClientConnectionQueryAdmin;
use crate::core::client_connection::ListProducerConnectionsRequest;
use crate::core::client_connection::ListProducerConnectionsResult;
use crate::core::client_connection::ProducerConnectionObservation;
use crate::core::client_connection::QueryConsumerConnectionsRequest;
use crate::core::client_connection::QueryConsumerConnectionsResult;
use crate::core::client_connection::QueryTopicProducerConnectionsRequest;
use crate::core::client_connection::QueryTopicProducerConnectionsResult;
use crate::core::query::AdminQueryFailureCode;
use crate::core::query::AdminQueryResult;
use crate::core::query::AdminQuerySource;
use crate::core::query::AdminSourceFailure;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

impl ClientConnectionQueryAdmin for AdminSession {
    fn query_consumer_connections<'a>(
        &'a mut self,
        request: &'a QueryConsumerConnectionsRequest,
    ) -> AdminFuture<'a, QueryConsumerConnectionsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            query_consumer_connections(&self.inner, request)
                .await
                .map(|outcome| outcome.result)
        })
    }

    fn query_consumer_connections_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryConsumerConnectionsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryConsumerConnectionsResult>> {
        Box::pin(async move {
            self.ensure_open()?;
            let outcome = query_consumer_connections(&self.inner, request).await?;
            AdminQueryResult::from_sources(outcome.result, outcome.successful_sources, outcome.failures)
        })
    }

    fn list_producer_connections<'a>(
        &'a mut self,
        request: &'a ListProducerConnectionsRequest,
    ) -> AdminFuture<'a, ListProducerConnectionsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            query_producer_connections(&self.inner, request)
                .await
                .map(|outcome| outcome.result)
        })
    }

    fn list_producer_connections_with_evidence<'a>(
        &'a mut self,
        request: &'a ListProducerConnectionsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<ListProducerConnectionsResult>> {
        Box::pin(async move {
            self.ensure_open()?;
            let outcome = query_producer_connections(&self.inner, request).await?;
            AdminQueryResult::from_sources(outcome.result, outcome.successful_sources, outcome.failures)
        })
    }

    fn query_topic_producer_connections<'a>(
        &'a mut self,
        request: &'a QueryTopicProducerConnectionsRequest,
    ) -> AdminFuture<'a, QueryTopicProducerConnectionsResult> {
        Box::pin(async move {
            crate::client_adapter::targeted_read::query_topic_producer_connections(self, request)
                .await
                .map(|result| result.data)
        })
    }

    fn query_topic_producer_connections_with_evidence<'a>(
        &'a mut self,
        request: &'a QueryTopicProducerConnectionsRequest,
    ) -> AdminFuture<'a, AdminQueryResult<QueryTopicProducerConnectionsResult>> {
        crate::client_adapter::targeted_read::query_topic_producer_connections(self, request)
    }
}

struct ConnectionOutcome<T> {
    result: T,
    successful_sources: usize,
    failures: Vec<AdminSourceFailure>,
}

async fn query_consumer_connections(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    request: &QueryConsumerConnectionsRequest,
) -> AdminResult<ConnectionOutcome<QueryConsumerConnectionsResult>> {
    let (targets, mut failures) =
        cluster_broker_targets(admin, &request.cluster, AdminQuerySource::ConsumerConnection).await?;
    failures.retain(|failure| {
        request
            .broker_name
            .as_ref()
            .is_none_or(|expected| expected == failure.logical_target())
    });
    let mut connections = BTreeMap::new();
    let mut queried_brokers = failures
        .iter()
        .map(|failure| failure.logical_target().to_string())
        .collect::<BTreeSet<_>>();
    let mut successful_sources = 0usize;
    let mut truncated = false;
    for (broker_name, broker_addr) in targets {
        if request
            .broker_name
            .as_ref()
            .is_some_and(|expected| expected != &broker_name)
        {
            continue;
        }
        queried_brokers.insert(broker_name.clone());
        match rocketmq_client_rust::MQAdminReadExt::observe_consumer_connection_at(
            admin,
            CheetahString::from(request.consumer_group.as_str()),
            broker_addr,
        )
        .await
        {
            Ok(connection) => {
                successful_sources += 1;
                for row in consumer_connection_rows(&broker_name, connection) {
                    connections.entry(connection_identity(&row)).or_insert(row);
                    if connections.len() > request.max_connections {
                        truncated = true;
                        break;
                    }
                }
            }
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ConsumerConnection,
                &broker_name,
                &error,
            )),
        }
        if truncated {
            break;
        }
    }
    let failed_brokers = failure_targets(&failures);
    Ok(ConnectionOutcome {
        result: QueryConsumerConnectionsResult {
            consumer_group: request.consumer_group.clone(),
            connections: connections.into_values().take(request.max_connections).collect(),
            queried_broker_count: queried_brokers.len(),
            failed_brokers,
            truncated,
        },
        successful_sources,
        failures,
    })
}

async fn query_producer_connections(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    request: &ListProducerConnectionsRequest,
) -> AdminResult<ConnectionOutcome<ListProducerConnectionsResult>> {
    let (targets, mut failures) =
        cluster_broker_targets(admin, &request.cluster, AdminQuerySource::ProducerConnection).await?;
    failures.retain(|failure| {
        request
            .broker_name
            .as_ref()
            .is_none_or(|expected| expected == failure.logical_target())
    });
    let mut connections = BTreeMap::new();
    let mut queried_brokers = failures
        .iter()
        .map(|failure| failure.logical_target().to_string())
        .collect::<BTreeSet<_>>();
    let mut successful_sources = 0usize;
    let mut truncated = false;
    for (broker_name, broker_addr) in targets {
        if request
            .broker_name
            .as_ref()
            .is_some_and(|expected| expected != &broker_name)
        {
            continue;
        }
        queried_brokers.insert(broker_name.clone());
        match rocketmq_client_rust::MQAdminReadExt::get_all_producer_info(admin, broker_addr).await {
            Ok(table) => {
                successful_sources += 1;
                for row in producer_connection_rows(&broker_name, table, request.producer_group.as_deref()) {
                    let identity = (row.producer_group.clone(), connection_identity(&row.connection));
                    connections.entry(identity).or_insert(row);
                    if connections.len() > request.max_connections {
                        truncated = true;
                        break;
                    }
                }
            }
            Err(error) => failures.push(source_failure(
                AdminQuerySource::ProducerConnection,
                &broker_name,
                &error,
            )),
        }
        if truncated {
            break;
        }
    }
    let failed_brokers = failure_targets(&failures);
    Ok(ConnectionOutcome {
        result: ListProducerConnectionsResult {
            connections: connections.into_values().take(request.max_connections).collect(),
            queried_broker_count: queried_brokers.len(),
            failed_brokers,
            truncated,
        },
        successful_sources,
        failures,
    })
}

async fn cluster_broker_targets(
    admin: &rocketmq_client_rust::DefaultMQAdminExt,
    cluster: &str,
    source: AdminQuerySource,
) -> AdminResult<(Vec<(String, CheetahString)>, Vec<AdminSourceFailure>)> {
    let cluster_info = rocketmq_client_rust::MQAdminReadExt::examine_broker_cluster_info(admin)
        .await
        .map_err(|error| AdminError::backend("examine_broker_cluster_info", error.to_string()))?;
    let broker_names = cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|table| table.get(cluster))
        .cloned()
        .unwrap_or_default();
    let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
    let mut targets = Vec::new();
    let mut failures = Vec::new();
    for broker_name in broker_names {
        let Some(broker) = broker_table.get(&broker_name) else {
            failures.push(AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name.as_str(),
            ));
            continue;
        };
        if broker.broker_addrs().is_empty() {
            failures.push(AdminSourceFailure::new(
                source,
                AdminQueryFailureCode::InvalidResponse,
                false,
                broker_name.as_str(),
            ));
            continue;
        }
        targets.extend(
            broker
                .broker_addrs()
                .iter()
                .map(|(broker_id, address)| (broker_name.to_string(), *broker_id, address.clone())),
        );
    }
    targets.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then(left.1.cmp(&right.1))
            .then(left.2.cmp(&right.2))
    });
    Ok((
        targets
            .into_iter()
            .map(|(broker_name, _, address)| (broker_name, address))
            .collect(),
        failures,
    ))
}

fn consumer_connection_rows(broker_name: &str, connection: ConsumerConnection) -> Vec<ClientConnectionObservation> {
    let mut rows = connection
        .get_connection_set()
        .iter()
        .map(|item| ClientConnectionObservation {
            broker_name: broker_name.to_owned(),
            client_id: item.get_client_id().to_string(),
            client_addr: item.get_client_addr().to_string(),
            language: item.get_language().to_string(),
            version: item.get_version(),
            last_update_timestamp: None,
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(connection_identity);
    rows
}

fn producer_connection_rows(
    broker_name: &str,
    table: ProducerTableInfo,
    producer_group: Option<&str>,
) -> Vec<ProducerConnectionObservation> {
    let mut groups = table.data().iter().collect::<Vec<_>>();
    groups.sort_by(|left, right| left.0.cmp(right.0));
    let mut rows = Vec::new();
    for (group, producers) in groups {
        if producer_group.is_some_and(|expected| expected != group.as_str()) {
            continue;
        }
        let mut producers = producers.iter().collect::<Vec<_>>();
        producers.sort_by(|left, right| {
            left.client_id()
                .cmp(right.client_id())
                .then(left.remote_ip().cmp(right.remote_ip()))
                .then(left.version().cmp(&right.version()))
                .then(left.last_update_timestamp().cmp(&right.last_update_timestamp()))
        });
        rows.extend(producers.into_iter().map(|producer| ProducerConnectionObservation {
            producer_group: group.clone(),
            connection: ClientConnectionObservation {
                broker_name: broker_name.to_owned(),
                client_id: producer.client_id().to_owned(),
                client_addr: producer.remote_ip().to_owned(),
                language: producer.language().to_string(),
                version: producer.version(),
                last_update_timestamp: Some(producer.last_update_timestamp()),
            },
        }));
    }
    rows
}

fn connection_identity(connection: &ClientConnectionObservation) -> (String, String, String, String, i32) {
    (
        connection.broker_name.clone(),
        connection.client_id.clone(),
        connection.client_addr.clone(),
        connection.language.clone(),
        connection.version,
    )
}

fn failure_targets(failures: &[AdminSourceFailure]) -> Vec<String> {
    failures
        .iter()
        .map(|failure| failure.logical_target().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn source_failure(source: AdminQuerySource, logical_target: &str, error: &RocketMQError) -> AdminSourceFailure {
    let view = error.boundary_view();
    let code = match view.http().status.as_u16() {
        401 | 403 => AdminQueryFailureCode::PermissionDenied,
        404 => AdminQueryFailureCode::NotFound,
        408 | 504 => AdminQueryFailureCode::Timeout,
        429 => AdminQueryFailureCode::RateLimited,
        400 | 413 | 422 => AdminQueryFailureCode::InvalidResponse,
        _ => AdminQueryFailureCode::SourceUnavailable,
    };
    AdminSourceFailure::new(source, code, view.is_retryable(), logical_target)
}
