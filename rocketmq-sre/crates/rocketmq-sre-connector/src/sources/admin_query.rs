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

use std::future::Future;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_admin_core::core::broker::BrokerQueryAdmin;
use rocketmq_admin_core::core::broker::ListBrokersRequest;
use rocketmq_admin_core::core::broker::QueryBrokerDiagnosticsRequest;
use rocketmq_admin_core::core::client_connection::ClientConnectionQueryAdmin;
use rocketmq_admin_core::core::client_connection::ListProducerConnectionsRequest;
use rocketmq_admin_core::core::client_connection::ListProducerConnectionsResult;
use rocketmq_admin_core::core::client_connection::QueryConsumerConnectionsRequest;
use rocketmq_admin_core::core::client_connection::QueryConsumerConnectionsResult;
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;
use rocketmq_admin_core::core::consumer::ListConsumerGroupsRequest;
use rocketmq_admin_core::core::consumer::QueryConsumerLagRequest;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_admin_core::core::topic::GetTopicRouteRequest;
use rocketmq_admin_core::core::topic::ListTopicsRequest;
use rocketmq_admin_core::core::topic::TopicQueryAdmin;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;
use rocketmq_admin_core::read_client_adapter::TelemetryHandle;
use rocketmq_runtime::ChildServiceContext;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::common::bounded_future;
use super::common::validate_identifier;
use crate::ConnectorError;
use crate::config::AdminSourceConfig;

struct AdminState {
    runtime: Option<Arc<ClientRuntime>>,
    session: Option<ReadAdminSession>,
}

const CONNECTION_QUERY_LIMIT: usize = 200;
const CONSUMER_CONNECTION_GROUP_LIMIT: usize = 32;

#[derive(Serialize)]
struct ClientConnectionsResult {
    producer: ListProducerConnectionsResult,
    consumers: Vec<QueryConsumerConnectionsResult>,
    failed_consumer_groups: Vec<String>,
    truncated: bool,
}

/// Compile-time read-only RocketMQ Admin adapter. Its dependency enables only
/// `read-client-adapter`; mutation traits and implementations are absent from
/// this crate's resolved feature graph.
pub(crate) struct AdminQuerySource {
    config: Option<AdminSourceConfig>,
    state: Mutex<AdminState>,
}

impl AdminQuerySource {
    pub(crate) fn new(config: Option<AdminSourceConfig>) -> Self {
        Self {
            config,
            state: Mutex::new(AdminState {
                runtime: None,
                session: None,
            }),
        }
    }

    pub(crate) fn configured(&self) -> bool {
        self.config.is_some()
    }

    pub(crate) async fn start(&self, context: ChildServiceContext) -> Result<(), ConnectorError> {
        let Some(config) = &self.config else {
            return Ok(());
        };
        let runtime = ClientRuntime::try_new(
            context.child("rocketmq-read-admin-client"),
            ClientRuntimeConfig {
                shutdown_timeout: config.shutdown_timeout,
                ..ClientRuntimeConfig::default()
            },
            TelemetryHandle::noop(),
        )
        .map_err(|_| ConnectorError::source("read-only Admin client runtime failed to start"))?;
        let mut builder = ReadAdminBuilder::new(runtime.clone())
            .namesrv_addr(config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-read-admin")
            .instance_name("rocketmq-sre-connector")
            .timeout_millis(config.request_timeout.as_millis().min(u64::MAX as u128) as u64)
            .use_tls(config.use_tls);
        if let Some(credentials) = &config.credentials {
            let credentials = AdminCredentials::try_new(
                credentials.access_key.expose(),
                credentials.secret_key.expose(),
                credentials
                    .security_token
                    .as_ref()
                    .map(|value| value.expose().to_owned()),
            )
            .map_err(|_| ConnectorError::configuration("read-admin credential references are invalid"))?;
            builder = builder.credentials(credentials);
        }
        let session = builder
            .build_and_start()
            .await
            .map_err(|_| ConnectorError::source("read-only Admin source failed to start"))?;
        let mut state = self.state.lock().await;
        state.runtime = Some(runtime);
        state.session = Some(session);
        Ok(())
    }

    pub(crate) async fn query(
        &self,
        cluster: &str,
        resource: &str,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let mut state = self.state.lock().await;
        let session = state
            .session
            .as_mut()
            .ok_or_else(|| ConnectorError::source("read-only Admin source is not configured or ready"))?;

        let value = if resource == "admin/brokers" || resource == "brokers" {
            let request = ListBrokersRequest::try_new(cluster)
                .map_err(|_| ConnectorError::source("read-only broker query is invalid"))?;
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.list_brokers(&request),
                    "read-only broker query failed",
                )
                .await?,
            )?
        } else if resource == "auth/diagnostics" || resource == "auth-security/diagnostics" {
            let request = QueryBrokerDiagnosticsRequest::try_new(cluster)
                .map_err(|_| ConnectorError::source("read-only auth diagnostics query is invalid"))?;
            let result = bounded_admin(
                deadline,
                cancel,
                session.query_broker_diagnostics(&request),
                "read-only auth diagnostics query failed",
            )
            .await?;
            return super::auth_security_diagnostics::project(result);
        } else if matches!(
            resource,
            "admin/broker-runtime"
                | "broker/diagnostics"
                | "store/health"
                | "store/recovery"
                | "store/background-index"
                | "store/rocksdb"
                | "store/tiered"
        ) {
            let request = QueryBrokerDiagnosticsRequest::try_new(cluster)
                .map_err(|_| ConnectorError::source("read-only broker diagnostics query is invalid"))?;
            let result = bounded_admin(
                deadline,
                cancel,
                session.query_broker_diagnostics(&request),
                "read-only broker diagnostics query failed",
            )
            .await?;
            return super::broker_store_diagnostics::project(result);
        } else if resource == "admin/topics" || resource == "topics" {
            let request = ListTopicsRequest::new(Some(cluster.to_owned()));
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.list_topics(&request),
                    "read-only topic query failed",
                )
                .await?,
            )?
        } else if resource == "admin/consumer-groups" || resource == "consumer-groups" {
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.list_consumer_groups(&ListConsumerGroupsRequest),
                    "read-only consumer group query failed",
                )
                .await?,
            )?
        } else if let Some(topic) = resource
            .strip_prefix("admin/topic-route/")
            .or_else(|| resource.strip_prefix("topic-route/"))
        {
            validate_identifier(topic, "topic")?;
            let request = GetTopicRouteRequest::try_new(topic)
                .map_err(|_| ConnectorError::source("read-only topic route query is invalid"))?;
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.get_topic_route(&request),
                    "read-only topic route query failed",
                )
                .await?,
            )?
        } else if let Some((consumer_group, topic)) = resource
            .strip_prefix("admin/consumer-lag/")
            .or_else(|| resource.strip_prefix("consumer-lag/"))
            .and_then(|value| value.split_once('/'))
        {
            validate_identifier(consumer_group, "consumer group")?;
            validate_identifier(topic, "topic")?;
            let request = QueryConsumerLagRequest::try_new(topic, consumer_group, false)
                .map_err(|_| ConnectorError::source("read-only consumer lag query is invalid"))?;
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.query_consumer_lag(&request),
                    "read-only consumer lag query failed",
                )
                .await?,
            )?
        } else if let Some(consumer_group) = resource
            .strip_prefix("admin/connections/")
            .or_else(|| resource.strip_prefix("connections/"))
        {
            validate_identifier(consumer_group, "consumer group")?;
            let request = QueryConsumerConnectionsRequest::try_new(cluster, consumer_group, CONNECTION_QUERY_LIMIT)
                .map_err(|_| ConnectorError::source("read-only connection metadata query is invalid"))?;
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.query_consumer_connections(&request),
                    "read-only connection metadata query failed",
                )
                .await?,
            )?
        } else if let Some(producer_group) = resource
            .strip_prefix("admin/producer-connections/")
            .or_else(|| resource.strip_prefix("producer-connections/"))
        {
            validate_identifier(producer_group, "producer group")?;
            let request = ListProducerConnectionsRequest::try_new(cluster, CONNECTION_QUERY_LIMIT)
                .and_then(|request| request.with_producer_group(producer_group))
                .map_err(|_| ConnectorError::source("read-only producer connection query is invalid"))?;
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    session.list_producer_connections(&request),
                    "read-only producer connection query failed",
                )
                .await?,
            )?
        } else if resource == "admin/client-connections" || resource == "client-connections" {
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    query_client_connections(session, cluster, None, CONNECTION_QUERY_LIMIT),
                    "read-only client connection query failed",
                )
                .await?,
            )?
        } else if let Some(broker_name) = resource
            .strip_prefix("admin/broker-connections/")
            .or_else(|| resource.strip_prefix("broker-connections/"))
        {
            validate_identifier(broker_name, "broker name")?;
            serialize(
                bounded_admin(
                    deadline,
                    cancel,
                    query_client_connections(session, cluster, Some(broker_name), CONNECTION_QUERY_LIMIT),
                    "read-only broker connection query failed",
                )
                .await?,
            )?
        } else if resource.starts_with("message/") || resource.starts_with("messages/") {
            return Err(ConnectorError::source(
                "message metadata is not available through the current read-only adapter",
            ));
        } else {
            return Err(ConnectorError::source(
                "the read-only Admin source does not support this resource",
            ));
        };
        Ok(SourceOutput::available(value, Utc::now()))
    }

    pub(crate) async fn query_consumer_connections(
        &self,
        cluster: &str,
        consumer_group: &str,
        max_rows: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        validate_identifier(consumer_group, "consumer group")?;
        let request = QueryConsumerConnectionsRequest::try_new(
            cluster,
            consumer_group,
            max_rows.clamp(1, CONNECTION_QUERY_LIMIT),
        )
        .map_err(|_| ConnectorError::source("read-only consumer connection query is invalid"))?;
        let mut state = self.state.lock().await;
        let session = state
            .session
            .as_mut()
            .ok_or_else(|| ConnectorError::source("read-only Admin source is not configured or ready"))?;
        let result = bounded_admin(
            deadline,
            cancel,
            session.query_consumer_connections(&request),
            "read-only consumer connection query failed",
        )
        .await?;
        Ok(SourceOutput::available(serialize(result)?, Utc::now()))
    }

    pub(crate) async fn query_producer_connections(
        &self,
        cluster: &str,
        max_rows: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError> {
        let request = ListProducerConnectionsRequest::try_new(cluster, max_rows.clamp(1, CONNECTION_QUERY_LIMIT))
            .map_err(|_| ConnectorError::source("read-only producer connection query is invalid"))?;
        let mut state = self.state.lock().await;
        let session = state
            .session
            .as_mut()
            .ok_or_else(|| ConnectorError::source("read-only Admin source is not configured or ready"))?;
        let result = bounded_admin(
            deadline,
            cancel,
            session.list_producer_connections(&request),
            "read-only producer connection query failed",
        )
        .await?;
        Ok(SourceOutput::available(serialize(result)?, Utc::now()))
    }

    pub(crate) async fn shutdown(&self) {
        let (session, runtime) = {
            let mut state = self.state.lock().await;
            (state.session.take(), state.runtime.take())
        };
        if let Some(mut session) = session {
            session.shutdown().await;
        }
        if let Some(runtime) = runtime {
            let report = runtime.shutdown().await;
            if !report.is_healthy() {
                tracing::warn!("read-only Admin client runtime shutdown was incomplete");
            }
        }
    }
}

async fn query_client_connections(
    session: &mut ReadAdminSession,
    cluster: &str,
    broker_name: Option<&str>,
    max_connections: usize,
) -> rocketmq_admin_core::core::AdminResult<ClientConnectionsResult> {
    let mut producer_request = ListProducerConnectionsRequest::try_new(cluster, max_connections)?;
    if let Some(broker_name) = broker_name {
        producer_request = producer_request.with_broker_name(broker_name)?;
    }
    let producer = session.list_producer_connections(&producer_request).await?;
    let groups = session.list_consumer_groups(&ListConsumerGroupsRequest).await?.groups;
    let mut consumers = Vec::new();
    let mut failed_consumer_groups = Vec::new();
    let mut observed_connections = producer.connections.len();
    let mut truncated = producer.truncated || groups.len() > CONSUMER_CONNECTION_GROUP_LIMIT;
    for group in groups.into_iter().take(CONSUMER_CONNECTION_GROUP_LIMIT) {
        let remaining = max_connections.saturating_sub(observed_connections);
        if remaining == 0 {
            truncated = true;
            break;
        }
        let mut request = QueryConsumerConnectionsRequest::try_new(cluster, &group.group, remaining)?;
        if let Some(broker_name) = broker_name {
            request = request.with_broker_name(broker_name)?;
        }
        match session.query_consumer_connections(&request).await {
            Ok(result) => {
                observed_connections = observed_connections.saturating_add(result.connections.len());
                truncated |= result.truncated;
                consumers.push(result);
            }
            Err(_) => failed_consumer_groups.push(group.group),
        }
    }
    Ok(ClientConnectionsResult {
        producer,
        consumers,
        failed_consumer_groups,
        truncated,
    })
}

fn serialize(value: impl serde::Serialize) -> Result<Value, ConnectorError> {
    serde_json::to_value(value).map_err(|_| ConnectorError::source("read-only Admin result cannot be encoded"))
}

async fn bounded_admin<T>(
    deadline: DateTime<Utc>,
    cancel: &CancelSignal,
    future: impl Future<Output = rocketmq_admin_core::core::AdminResult<T>>,
    failure: &'static str,
) -> Result<T, ConnectorError> {
    bounded_future(deadline, cancel, async {
        future.await.map_err(|_| ConnectorError::source(failure))
    })
    .await
}
