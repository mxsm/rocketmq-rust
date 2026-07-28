// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! RocketMQ Client-backed adapter with no mutation traits in its public API.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::MQAdminReadExt;
use rocketmq_client_rust::SessionCredentials;
use rocketmq_client_rust::SigningAlgorithm;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_model::topic::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_model::topic::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::body::producer_table_info::ProducerTableInfo;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;

use crate::core::broker::project_broker_diagnostics;
use crate::core::broker::BrokerAllowlistedConfig;
use crate::core::broker::BrokerQueryAdmin;
use crate::core::broker::BrokerSummary;
use crate::core::broker::ListBrokersRequest;
use crate::core::broker::ListBrokersResult;
use crate::core::broker::ProbeBrokerRuntimeRequest;
use crate::core::broker::ProbeBrokerRuntimeResult;
use crate::core::broker::QueryBrokerAllowlistedConfigRequest;
use crate::core::broker::QueryBrokerDiagnosticsRequest;
use crate::core::broker::QueryBrokerDiagnosticsResult;
use crate::core::client_connection::ClientConnectionObservation;
use crate::core::client_connection::ClientConnectionQueryAdmin;
use crate::core::client_connection::ListProducerConnectionsRequest;
use crate::core::client_connection::ListProducerConnectionsResult;
use crate::core::client_connection::ProducerConnectionObservation;
use crate::core::client_connection::QueryConsumerConnectionsRequest;
use crate::core::client_connection::QueryConsumerConnectionsResult;
use crate::core::clock::Clock;
use crate::core::consumer;
use crate::core::consumer::ConsumerQueryAdmin;
use crate::core::proxy::ProxyDrainPending;
use crate::core::proxy::ProxyDrainState;
use crate::core::proxy::ProxyQueryAdmin;
use crate::core::proxy::QueryProxyDrainStateRequest;
use crate::core::security::AdminCredentials;
use crate::core::topic::GetTopicRouteRequest;
use crate::core::topic::ListTopicsRequest;
use crate::core::topic::ListTopicsResult;
use crate::core::topic::TopicBroker;
use crate::core::topic::TopicQueryAdmin;
use crate::core::topic::TopicQueue;
use crate::core::topic::TopicRoute;
use crate::core::topic::TopicSummary;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

pub use rocketmq_client_rust::ClientRuntime;
pub use rocketmq_client_rust::ClientRuntimeConfig;

#[derive(Clone)]
pub struct ReadAdminBuilder {
    client_runtime: Arc<ClientRuntime>,
    config: crate::core::admin::AdminBuilder,
    credentials: Option<AdminCredentials>,
}

impl ReadAdminBuilder {
    pub fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self {
            client_runtime,
            config: crate::core::admin::AdminBuilder::new(),
            credentials: None,
        }
    }

    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.config = self.config.namesrv_addr(addr);
        self
    }

    pub fn admin_group(mut self, group: impl Into<String>) -> Self {
        self.config = self.config.admin_group(group);
        self
    }

    pub fn instance_name(mut self, name: impl Into<String>) -> Self {
        self.config = self.config.instance_name(name);
        self
    }

    pub fn timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.config = self.config.timeout_millis(timeout_millis);
        self
    }

    pub fn unit_name(mut self, name: impl Into<String>) -> Self {
        self.config = self.config.unit_name(name);
        self
    }

    pub fn vip_channel_enabled(mut self, enabled: bool) -> Self {
        self.config = self.config.vip_channel_enabled(enabled);
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.config = self.config.use_tls(use_tls);
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.config = self.config.clock(clock);
        self
    }

    /// Configures request signing for a normal read-only RocketMQ identity.
    ///
    /// Callers should obtain credential values from an environment or mounted
    /// secret reference. Credential values are redacted by both this crate and
    /// the underlying client hook.
    pub fn credentials(mut self, credentials: AdminCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub async fn build_and_start(self) -> AdminResult<ReadAdminSession> {
        let client_runtime = self.client_runtime;
        let config = self.config;
        let credentials = self.credentials;
        let clock = config.configured_clock();
        let now_millis = clock.now_millis();
        let admin_group = config
            .configured_admin_group()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("sre-read-admin-{now_millis}"));
        let timeout = Duration::from_millis(config.configured_timeout_millis());
        let mut admin = match credentials {
            Some(credentials) => DefaultMQAdminExt::with_admin_ext_group_rpc_hook_and_timeout(
                client_runtime.clone(),
                admin_group,
                Arc::new(read_acl_rpc_hook(&credentials)),
                timeout,
            ),
            None => DefaultMQAdminExt::with_admin_ext_group_and_timeout(client_runtime.clone(), admin_group, timeout),
        };
        if let Some(namesrv_addr) = config.configured_namesrv_addr() {
            admin.set_namesrv_addr(namesrv_addr);
        }
        let instance_name = config
            .configured_instance_name()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("sre-read-{now_millis}"));
        let client_config = admin.client_config_mut();
        client_config.set_instance_name(instance_name.into());
        client_config.set_vip_channel_enabled(config.configured_vip_channel_enabled());
        if let Some(unit_name) = config.configured_unit_name() {
            client_config.set_unit_name(unit_name.into());
        }
        admin.set_use_tls(config.configured_use_tls());
        MQAdminReadExt::start(&mut admin)
            .await
            .map_err(|error| backend_error("start_read_admin_session", error))?;
        Ok(ReadAdminSession {
            inner: admin,
            client_runtime,
            clock,
            closed: false,
        })
    }

    pub async fn build_with_guard(self) -> AdminResult<ReadAdminGuard> {
        self.build_and_start().await.map(ReadAdminGuard::new)
    }
}

impl std::fmt::Debug for ReadAdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadAdminBuilder")
            .field("client_runtime", &"explicit")
            .field("config", &self.config)
            .field("credentials", &self.credentials.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

fn read_acl_rpc_hook(credentials: &AdminCredentials) -> AclClientRPCHook {
    let credentials = match credentials.security_token() {
        Some(security_token) => {
            SessionCredentials::with_token(credentials.access_key(), credentials.secret_key(), security_token)
        }
        None => SessionCredentials::with_keys(credentials.access_key(), credentials.secret_key()),
    };
    AclClientRPCHook::with_signature_algorithm(credentials, SigningAlgorithm::HmacSha256)
}

#[must_use = "a started read admin session must be explicitly shut down"]
pub struct ReadAdminSession {
    inner: DefaultMQAdminExt,
    client_runtime: Arc<ClientRuntime>,
    clock: Arc<dyn Clock>,
    closed: bool,
}

impl ReadAdminSession {
    pub async fn shutdown(&mut self) {
        if !self.closed {
            MQAdminReadExt::shutdown(&mut self.inner).await;
            self.closed = true;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        Arc::clone(&self.client_runtime)
    }

    fn ensure_open(&self) -> AdminResult<()> {
        if self.closed {
            Err(AdminError::SessionClosed)
        } else {
            Ok(())
        }
    }
}

impl Drop for ReadAdminSession {
    fn drop(&mut self) {
        if !self.closed {
            tracing::warn!("read admin session dropped before explicit shutdown");
        }
    }
}

impl ProxyQueryAdmin for ReadAdminSession {
    fn query_drain_state<'a>(
        &'a mut self,
        request: &'a QueryProxyDrainStateRequest,
    ) -> AdminFuture<'a, ProxyDrainState> {
        Box::pin(async move {
            self.ensure_open()?;
            let proxy_addr = request.proxy_addr.trim();
            if proxy_addr.is_empty() {
                return Err(AdminError::invalid_argument("proxyAddr", "must not be empty"));
            }
            let state = self
                .inner
                .proxy_drain_state(CheetahString::from(proxy_addr))
                .await
                .map_err(|error| backend_error("proxy_drain_state", error))?;
            map_proxy_drain_state(state)
        })
    }
}

#[must_use = "the guard owns a live read admin session; call shutdown when complete"]
pub struct ReadAdminGuard {
    session: ReadAdminSession,
}

impl ReadAdminGuard {
    fn new(session: ReadAdminSession) -> Self {
        Self { session }
    }

    pub async fn shutdown(mut self) {
        self.session.shutdown().await;
    }

    pub fn inner(&self) -> &ReadAdminSession {
        &self.session
    }

    pub fn inner_mut(&mut self) -> &mut ReadAdminSession {
        &mut self.session
    }
}

impl Deref for ReadAdminGuard {
    type Target = ReadAdminSession;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl DerefMut for ReadAdminGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_mut()
    }
}

impl BrokerQueryAdmin for ReadAdminSession {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let broker_names = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(request.cluster.as_str()))
                .cloned()
                .unwrap_or_default();
            let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
            let now_millis = self.clock.now_millis();
            let mut brokers = Vec::new();
            for broker_name in broker_names {
                let Some(broker_data) = broker_table.get(&broker_name) else {
                    continue;
                };
                for (broker_id, broker_addr) in broker_data.broker_addrs() {
                    let runtime = self.inner.fetch_broker_runtime_stats(broker_addr.clone()).await.ok();
                    brokers.push(build_broker_summary(
                        request.cluster.clone(),
                        broker_name.to_string(),
                        *broker_id,
                        broker_addr.to_string(),
                        runtime.as_ref(),
                        now_millis,
                    ));
                }
            }
            brokers.sort_by(|left, right| {
                left.broker_name
                    .cmp(&right.broker_name)
                    .then(left.broker_id.cmp(&right.broker_id))
            });
            Ok(ListBrokersResult { brokers })
        })
    }

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let broker_names = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(request.cluster.as_str()))
                .cloned()
                .unwrap_or_default();
            let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
            let mut result = ProbeBrokerRuntimeResult::default();
            let mut failure_counts = BTreeMap::<String, usize>::new();
            for broker_name in broker_names {
                let Some(broker_data) = broker_table.get(&broker_name) else {
                    continue;
                };
                for broker_addr in broker_data.broker_addrs().values() {
                    result.attempted += 1;
                    if let Err(error) = self.inner.fetch_broker_runtime_stats(broker_addr.clone()).await {
                        let code = error.boundary_view().code().as_str().to_string();
                        *failure_counts.entry(code).or_default() += 1;
                    }
                }
            }
            const MAX_FAILURE_CODES: usize = 16;
            let mut overflow = 0usize;
            for (index, (code, count)) in failure_counts.into_iter().enumerate() {
                if index < MAX_FAILURE_CODES - 1 {
                    result.failures.push(format!("code={code};count={count}"));
                } else {
                    overflow += count;
                }
            }
            if overflow > 0 {
                result.failures.push(format!("code=other;count={overflow}"));
            }
            Ok(result)
        })
    }

    fn query_broker_diagnostics<'a>(
        &'a mut self,
        request: &'a QueryBrokerDiagnosticsRequest,
    ) -> AdminFuture<'a, QueryBrokerDiagnosticsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let broker_names = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(request.cluster.as_str()))
                .cloned()
                .unwrap_or_default();
            let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
            let observed_at_millis = self.clock.now_millis();
            let mut brokers = Vec::new();
            let mut unavailable_brokers = 0usize;
            for broker_name in broker_names {
                let Some(broker_data) = broker_table.get(&broker_name) else {
                    continue;
                };
                for (broker_id, broker_addr) in broker_data.broker_addrs() {
                    match self.inner.fetch_broker_runtime_stats(broker_addr.clone()).await {
                        Ok(runtime) => brokers.push(project_broker_diagnostics(
                            broker_name.to_string(),
                            *broker_id,
                            &runtime,
                        )),
                        Err(_) => unavailable_brokers = unavailable_brokers.saturating_add(1),
                    }
                }
            }
            brokers.sort_by(|left, right| {
                left.broker_name
                    .cmp(&right.broker_name)
                    .then(left.broker_id.cmp(&right.broker_id))
            });
            Ok(QueryBrokerDiagnosticsResult {
                schema_version: crate::core::broker::BROKER_DIAGNOSTICS_SCHEMA_VERSION.to_owned(),
                observed_at_millis,
                partial: unavailable_brokers > 0
                    || brokers
                        .iter()
                        .any(|broker| broker.coverage != crate::core::broker::BrokerDiagnosticsCoverage::Available),
                brokers,
                unavailable_brokers,
            })
        })
    }

    fn query_allowlisted_config<'a>(
        &'a mut self,
        request: &'a QueryBrokerAllowlistedConfigRequest,
    ) -> AdminFuture<'a, BrokerAllowlistedConfig> {
        Box::pin(async move {
            self.ensure_open()?;
            self.inner
                .get_broker_config_allowlisted(CheetahString::from(request.broker_addr.as_str()))
                .await
                .map(project_allowlisted_config)
                .map_err(|error| backend_error("get_broker_config_allowlisted", error))
        })
    }
}

impl ClientConnectionQueryAdmin for ReadAdminSession {
    fn query_consumer_connections<'a>(
        &'a mut self,
        request: &'a QueryConsumerConnectionsRequest,
    ) -> AdminFuture<'a, QueryConsumerConnectionsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = cluster_broker_targets(&self.inner, &request.cluster).await?;
            let mut connections = BTreeMap::new();
            let mut failed_brokers = BTreeSet::new();
            let mut queried_brokers = BTreeSet::new();
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
                match self
                    .inner
                    .examine_consumer_connection_info(
                        CheetahString::from(request.consumer_group.as_str()),
                        Some(broker_addr),
                    )
                    .await
                {
                    Ok(connection) => {
                        for row in consumer_connection_rows(&broker_name, connection) {
                            let identity = client_connection_identity(&row);
                            connections.entry(identity).or_insert(row);
                            if connections.len() > request.max_connections {
                                truncated = true;
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        failed_brokers.insert(broker_name);
                    }
                }
                if truncated {
                    break;
                }
            }
            let connections = connections
                .into_values()
                .take(request.max_connections)
                .collect::<Vec<_>>();
            Ok(QueryConsumerConnectionsResult {
                consumer_group: request.consumer_group.clone(),
                connections,
                queried_broker_count: queried_brokers.len(),
                failed_brokers: failed_brokers.into_iter().collect(),
                truncated,
            })
        })
    }

    fn list_producer_connections<'a>(
        &'a mut self,
        request: &'a ListProducerConnectionsRequest,
    ) -> AdminFuture<'a, ListProducerConnectionsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let targets = cluster_broker_targets(&self.inner, &request.cluster).await?;
            let mut connections = BTreeMap::new();
            let mut failed_brokers = BTreeSet::new();
            let mut queried_brokers = BTreeSet::new();
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
                match self.inner.get_all_producer_info(broker_addr).await {
                    Ok(table) => {
                        for row in producer_connection_rows(&broker_name, table, request.producer_group.as_deref()) {
                            let identity = (row.producer_group.clone(), client_connection_identity(&row.connection));
                            connections.entry(identity).or_insert(row);
                            if connections.len() > request.max_connections {
                                truncated = true;
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        failed_brokers.insert(broker_name);
                    }
                }
                if truncated {
                    break;
                }
            }
            let connections = connections
                .into_values()
                .take(request.max_connections)
                .collect::<Vec<_>>();
            Ok(ListProducerConnectionsResult {
                connections,
                queried_broker_count: queried_brokers.len(),
                failed_brokers: failed_brokers.into_iter().collect(),
                truncated,
            })
        })
    }
}

impl TopicQueryAdmin for ReadAdminSession {
    fn list_topics<'a>(&'a mut self, request: &'a ListTopicsRequest) -> AdminFuture<'a, ListTopicsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let Some(cluster_name) = request.cluster.as_deref() else {
                return Ok(ListTopicsResult {
                    topics: topic_list
                        .topic_list
                        .into_iter()
                        .map(|topic| TopicSummary {
                            topic: topic.to_string(),
                            cluster: None,
                            consumer_group: None,
                        })
                        .collect(),
                });
            };
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
            let cluster_brokers = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(cluster_name))
                .cloned()
                .unwrap_or_default();
            let mut topics = Vec::new();
            for topic in topic_list.topic_list {
                if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
                    continue;
                }
                let route = self
                    .inner
                    .examine_topic_route_info(topic.clone())
                    .await
                    .map_err(|error| backend_error("examine_topic_route_info", error))?;
                let Some(route) = route else {
                    continue;
                };
                if !route
                    .broker_datas
                    .iter()
                    .any(|broker| cluster_brokers.contains(broker.broker_name()))
                {
                    continue;
                }
                let group_list = self
                    .inner
                    .query_topic_consume_by_who(topic.clone())
                    .await
                    .map_err(|error| backend_error("query_topic_consume_by_who", error))?;
                if group_list.get_group_list().is_empty() {
                    topics.push(TopicSummary {
                        topic: topic.to_string(),
                        cluster: Some(cluster_name.to_string()),
                        consumer_group: None,
                    });
                } else {
                    topics.extend(group_list.get_group_list().iter().map(|group| TopicSummary {
                        topic: topic.to_string(),
                        cluster: Some(cluster_name.to_string()),
                        consumer_group: Some(group.to_string()),
                    }));
                }
            }
            topics.sort_by(|left, right| {
                left.topic
                    .cmp(&right.topic)
                    .then(left.consumer_group.cmp(&right.consumer_group))
            });
            Ok(ListTopicsResult { topics })
        })
    }

    fn get_topic_route<'a>(&'a mut self, request: &'a GetTopicRouteRequest) -> AdminFuture<'a, Option<TopicRoute>> {
        Box::pin(async move {
            self.ensure_open()?;
            self.inner
                .examine_topic_route_info(CheetahString::from(request.topic.as_str()))
                .await
                .map(|route| route.map(map_topic_route))
                .map_err(|error| backend_error("examine_topic_route_info", error))
        })
    }

    fn get_topic_catalog<'a>(
        &'a mut self,
        _request: &'a crate::core::topic::TopicCatalogRequest,
    ) -> AdminFuture<'a, crate::core::topic::TopicCatalog> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_topic_catalog",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn get_topic_current_stats(&mut self) -> AdminFuture<'_, crate::core::topic::TopicCurrentStats> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_topic_current_stats",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn get_topic_stats<'a>(&'a mut self, _topic: &'a str) -> AdminFuture<'a, crate::core::topic::TopicStats> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_topic_stats",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn get_topic_config<'a>(
        &'a mut self,
        _request: &'a crate::core::topic::GetTopicConfigRequest,
    ) -> AdminFuture<'a, crate::core::topic::TopicConfigDetail> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_topic_config",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn get_topic_consumer_groups<'a>(
        &'a mut self,
        _topic: &'a str,
    ) -> AdminFuture<'a, crate::core::topic::TopicConsumerGroups> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_topic_consumer_groups",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn get_topic_consumers<'a>(&'a mut self, _topic: &'a str) -> AdminFuture<'a, crate::core::topic::TopicConsumers> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_topic_consumers",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }
}

impl ConsumerQueryAdmin for ReadAdminSession {
    fn list_consumer_groups<'a>(
        &'a mut self,
        _request: &'a consumer::ListConsumerGroupsRequest,
    ) -> AdminFuture<'a, consumer::ListConsumerGroupsResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let topic_list = self
                .inner
                .fetch_all_topic_list()
                .await
                .map_err(|error| backend_error("fetch_all_topic_list", error))?;
            let mut groups = Vec::new();
            for retry_topic in topic_list
                .topic_list
                .into_iter()
                .filter(|topic| topic.starts_with(RETRY_GROUP_TOPIC_PREFIX))
            {
                let group = KeyBuilder::parse_group(retry_topic.as_str());
                let mut summary = consumer::ConsumerGroupSummary {
                    group: group.clone(),
                    version: 0,
                    client_count: 0,
                    consume_type: format!("{:?}", ConsumeType::ConsumePassively),
                    message_model: format!("{:?}", MessageModel::Clustering),
                    consume_tps: 0.0,
                    diff_total: 0,
                };
                if let Ok(stats) = self
                    .inner
                    .examine_consume_stats(CheetahString::from(group.as_str()), None, None, None, None)
                    .await
                {
                    summary.consume_tps = stats.get_consume_tps();
                    summary.diff_total = stats.compute_total_diff();
                }
                if let Ok(connection) = self
                    .inner
                    .examine_consumer_connection_info(CheetahString::from(group.as_str()), None)
                    .await
                {
                    summary.client_count = connection.get_connection_set().len() as i32;
                    summary.consume_type = format!(
                        "{:?}",
                        connection.get_consume_type().unwrap_or(ConsumeType::ConsumePassively)
                    );
                    summary.message_model = format!(
                        "{:?}",
                        connection.get_message_model().unwrap_or(MessageModel::Clustering)
                    );
                    summary.version = connection.compute_min_version();
                }
                groups.push(summary);
            }
            groups.sort_by(|left, right| left.group.cmp(&right.group));
            Ok(consumer::ListConsumerGroupsResult { groups })
        })
    }

    fn query_consumer_lag<'a>(
        &'a mut self,
        request: &'a consumer::QueryConsumerLagRequest,
    ) -> AdminFuture<'a, consumer::QueryConsumerLagResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let stats = self
                .inner
                .examine_consume_stats(
                    CheetahString::from(request.consumer_group.as_str()),
                    Some(CheetahString::from(request.topic.as_str())),
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|error| backend_error("examine_consume_stats", error))?;
            let mut queues = stats.get_offset_table().keys().cloned().collect::<Vec<_>>();
            queues.sort();
            let mut result = consumer::QueryConsumerLagResult {
                consume_tps: stats.get_consume_tps(),
                ..consumer::QueryConsumerLagResult::default()
            };
            for queue in queues {
                let Some(offset) = stats.get_offset_table().get(&queue) else {
                    continue;
                };
                let lag = offset.get_broker_offset() - offset.get_consumer_offset();
                let inflight = offset.get_pull_offset() - offset.get_consumer_offset();
                result.total_lag += lag;
                result.inflight_total += inflight;
                result.rows.push(consumer::ConsumerLagRow {
                    topic: queue.topic().to_string(),
                    broker_name: queue.broker_name().to_string(),
                    queue_id: queue.queue_id(),
                    broker_offset: offset.get_broker_offset(),
                    consumer_offset: offset.get_consumer_offset(),
                    lag,
                    inflight,
                    last_timestamp: offset.get_last_timestamp(),
                    client_ip: None,
                });
            }
            Ok(result)
        })
    }

    fn query_dashboard_consumer_groups<'a>(
        &'a mut self,
        _request: &'a consumer::DashboardConsumerGroupListRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerGroupListResult> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_dashboard_consumer_groups",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn query_dashboard_consumer_connection<'a>(
        &'a mut self,
        _request: &'a consumer::DashboardConsumerConnectionRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerConnection> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_dashboard_consumer_connection",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn query_dashboard_consumer_progress<'a>(
        &'a mut self,
        _request: &'a consumer::DashboardConsumerProgressRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerProgress> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_dashboard_consumer_progress",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }

    fn query_dashboard_consumer_config<'a>(
        &'a mut self,
        _request: &'a consumer::DashboardConsumerConfigRequest,
    ) -> AdminFuture<'a, consumer::DashboardConsumerConfig> {
        Box::pin(async {
            Err(AdminError::backend(
                "read_dashboard_consumer_config",
                "query is not enabled by the Phase 00 read adapter",
            ))
        })
    }
}

fn map_topic_route(route: TopicRouteData) -> TopicRoute {
    let mut brokers = route
        .broker_datas
        .iter()
        .map(|broker| TopicBroker {
            cluster: broker.cluster().to_string(),
            broker_name: broker.broker_name().to_string(),
            broker_addrs: broker
                .broker_addrs()
                .iter()
                .map(|(broker_id, address)| (*broker_id, address.to_string()))
                .collect::<BTreeMap<_, _>>(),
            zone_name: broker.zone_name().map(ToString::to_string),
            enable_acting_master: broker.enable_acting_master(),
        })
        .collect::<Vec<_>>();
    brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    let mut queues = route
        .queue_datas
        .iter()
        .map(|queue| TopicQueue {
            broker_name: queue.broker_name().to_string(),
            read_queue_nums: queue.read_queue_nums(),
            write_queue_nums: queue.write_queue_nums(),
            perm: queue.perm(),
            topic_sys_flag: queue.topic_sys_flag(),
        })
        .collect::<Vec<_>>();
    queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    TopicRoute { brokers, queues }
}

async fn cluster_broker_targets(admin: &DefaultMQAdminExt, cluster: &str) -> AdminResult<Vec<(String, CheetahString)>> {
    let cluster_info = admin
        .examine_broker_cluster_info()
        .await
        .map_err(|error| backend_error("examine_broker_cluster_info", error))?;
    let broker_names = cluster_info
        .cluster_addr_table
        .as_ref()
        .and_then(|table| table.get(cluster))
        .cloned()
        .unwrap_or_default();
    let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
    let mut targets = Vec::new();
    for broker_name in broker_names {
        let Some(broker) = broker_table.get(&broker_name) else {
            continue;
        };
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
    Ok(targets
        .into_iter()
        .map(|(broker_name, _, address)| (broker_name, address))
        .collect())
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
    sort_client_connections(&mut rows);
    rows
}

fn sort_client_connections(rows: &mut [ClientConnectionObservation]) {
    rows.sort_by_key(client_connection_identity);
}

fn client_connection_identity(connection: &ClientConnectionObservation) -> (String, String, String, String, i32) {
    (
        connection.broker_name.clone(),
        connection.client_id.clone(),
        connection.client_addr.clone(),
        connection.language.clone(),
        connection.version,
    )
}

fn build_broker_summary(
    cluster: String,
    broker_name: String,
    broker_id: u64,
    broker_addr: String,
    runtime: Option<&KVTable>,
    now_millis: u64,
) -> BrokerSummary {
    let in_tps = runtime_f64(runtime, "putTps");
    let out_tps = runtime_f64(runtime, "getTransferredTps");
    let hour = runtime_value(runtime, "earliestMessageTimeStamp")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|timestamp| now_millis.saturating_sub(timestamp) as f64 / 3_600_000.0)
        .unwrap_or(0.0);
    BrokerSummary {
        cluster,
        broker_name,
        broker_id,
        broker_addr,
        version: runtime_value(runtime, "brokerVersionDesc")
            .unwrap_or_default()
            .to_string(),
        in_tps: format!("{in_tps:.2}"),
        out_tps: format!("{out_tps:.2}"),
        timer_progress: format!(
            "{}-{}",
            runtime_i64(runtime, "timerReadBehind"),
            runtime_i64(runtime, "timerOffsetBehind")
        ),
        page_cache_lock_time_millis: runtime_value(runtime, "pageCacheLockTimeMills")
            .unwrap_or_default()
            .to_string(),
        hour: format!("{hour:.2}"),
        space: format!("{:.4}", runtime_f64(runtime, "commitLogDiskRatio")),
        broker_active: runtime_value(runtime, "brokerActive").is_some_and(|value| value == "true"),
    }
}

fn runtime_value<'a>(runtime: Option<&'a KVTable>, key: &str) -> Option<&'a str> {
    runtime
        .and_then(|runtime| runtime.table.get(&CheetahString::from(key)))
        .map(CheetahString::as_str)
}

fn runtime_i64(runtime: Option<&KVTable>, key: &str) -> i64 {
    runtime_value(runtime, key)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0)
}

fn runtime_f64(runtime: Option<&KVTable>, key: &str) -> f64 {
    runtime_value(runtime, key)
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
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

fn project_allowlisted_config(config: rocketmq_client_rust::BrokerConfigAllowlisted) -> BrokerAllowlistedConfig {
    BrokerAllowlistedConfig {
        generation: config.generation,
        send_message_thread_pool_nums: config.send_message_thread_pool_nums,
        pull_message_thread_pool_nums: config.pull_message_thread_pool_nums,
        flush_delay_offset_interval_ms: config.flush_delay_offset_interval_ms,
    }
}

fn map_proxy_drain_state(
    state: rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody,
) -> AdminResult<ProxyDrainState> {
    ProxyDrainState::try_from_wire_parts(
        state.schema_version,
        state.phase.as_str(),
        state.operation_id,
        state.admission_open,
        state.routing_open,
        state.readiness_published,
        state.zero_pending,
        ProxyDrainPending {
            active_connections: state.pending.active_connections,
            sessions: state.pending.sessions,
            receipt_handles: state.pending.receipt_handles,
            prepared_transactions: state.pending.prepared_transactions,
            telemetry_links: state.pending.telemetry_links,
            remoting_channels: state.pending.remoting_channels,
            telemetry_commands: state.pending.telemetry_commands,
            rpc_in_flight: state.pending.rpc_in_flight,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_protocol::protocol::body::connection::Connection;
    use rocketmq_protocol::protocol::body::producer_info::ProducerInfo;
    use rocketmq_protocol::protocol::LanguageCode;

    use super::*;

    #[test]
    fn producer_rows_are_deterministic_and_filter_exact_groups() {
        let table = ProducerTableInfo::new(HashMap::from([
            (
                "producer-z".to_owned(),
                vec![ProducerInfo::new(
                    "client-z",
                    "10.0.0.9:12000",
                    LanguageCode::RUST,
                    2,
                    20,
                )],
            ),
            (
                "producer-a".to_owned(),
                vec![
                    ProducerInfo::new("client-b", "10.0.0.2:12000", LanguageCode::JAVA, 2, 12),
                    ProducerInfo::new("client-a", "10.0.0.1:12000", LanguageCode::RUST, 1, 11),
                ],
            ),
        ]));

        let rows = producer_connection_rows("broker-a", table, Some("producer-a"));

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.producer_group == "producer-a"));
        assert_eq!(rows[0].connection.client_id, "client-a");
        assert_eq!(rows[1].connection.client_id, "client-b");
        assert_eq!(rows[0].connection.broker_name, "broker-a");
    }

    #[test]
    fn consumer_rows_are_sorted_and_keep_only_observed_protocol_fields() {
        let mut connection = ConsumerConnection::new();
        connection.insert_connection(protocol_connection("client-z", "10.0.0.9:12000", LanguageCode::JAVA, 2));
        connection.insert_connection(protocol_connection("client-a", "10.0.0.1:12000", LanguageCode::RUST, 1));

        let rows = consumer_connection_rows("broker-a", connection);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].client_id, "client-a");
        assert_eq!(rows[1].client_id, "client-z");
        assert!(rows.iter().all(|row| row.last_update_timestamp.is_none()));
    }

    #[test]
    fn allowlisted_config_projection_has_no_arbitrary_property_surface() {
        let projected = project_allowlisted_config(rocketmq_client_rust::BrokerConfigAllowlisted {
            generation: 7,
            send_message_thread_pool_nums: Some(32),
            pull_message_thread_pool_nums: Some(16),
            flush_delay_offset_interval_ms: Some(10_000),
        });

        assert_eq!(
            projected,
            BrokerAllowlistedConfig {
                generation: 7,
                send_message_thread_pool_nums: Some(32),
                pull_message_thread_pool_nums: Some(16),
                flush_delay_offset_interval_ms: Some(10_000),
            }
        );
        assert_eq!(serde_json::to_value(projected).unwrap().as_object().unwrap().len(), 4);
    }

    fn protocol_connection(client_id: &str, client_addr: &str, language: LanguageCode, version: i32) -> Connection {
        let mut connection = Connection::new();
        connection.set_client_id(CheetahString::from(client_id));
        connection.set_client_addr(CheetahString::from(client_addr));
        connection.set_language(language);
        connection.set_version(version);
        connection
    }
}
