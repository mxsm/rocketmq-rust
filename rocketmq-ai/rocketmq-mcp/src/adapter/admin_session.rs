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
use std::future::Future;
use std::sync::Arc;

use rocketmq_admin_core::core::broker::BrokerQueryAdmin;
use rocketmq_admin_core::core::broker::BrokerRuntimeTargetStatus;
use rocketmq_admin_core::core::broker::ListBrokersRequest;
use rocketmq_admin_core::core::broker::ProbeBrokerRuntimeTargetRequest;
use rocketmq_admin_core::core::broker::QueryBrokerAllowlistedConfigTargetRequest;
use rocketmq_admin_core::core::broker::QueryBrokerDiagnosticsTargetRequest;
use rocketmq_admin_core::core::broker::QueryBrokerLogFilterStateTargetRequest;
use rocketmq_admin_core::core::client_connection::ClientConnectionQueryAdmin;
use rocketmq_admin_core::core::client_connection::QueryConsumerConnectionsRequest;
use rocketmq_admin_core::core::client_connection::QueryTopicProducerConnectionsRequest;
use rocketmq_admin_core::core::config_state::ConfigStateQueryAdmin;
use rocketmq_admin_core::core::config_state::ConsumerGroupConfigStateRequest;
use rocketmq_admin_core::core::config_state::TopicConfigStateRequest;
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;
use rocketmq_admin_core::core::consumer::ExactConsumerGroupEnrichmentRequest;
use rocketmq_admin_core::core::consumer::ListConsumerGroupsRequest;
use rocketmq_admin_core::core::consumer::QueryConsumerLagRequest;
use rocketmq_admin_core::core::message::MessageMetadataQueryAdmin;
use rocketmq_admin_core::core::message::MessageMetadataRequest;
use rocketmq_admin_core::core::proxy::ProxyQueryAdmin;
use rocketmq_admin_core::core::proxy::QueryProxyDrainStateRequest;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_admin_core::core::topic::GetTopicRouteRequest;
use rocketmq_admin_core::core::topic::TopicInventoryAdmin;
use rocketmq_admin_core::core::topic::TopicInventoryRequest;
use rocketmq_admin_core::core::topic::TopicQueryAdmin;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminGuard;
use serde::Serialize;

use crate::adapter::admin_session_projection::bounded_proxy_operation_id;
use crate::adapter::admin_session_projection::map_broker_diagnostics;
use crate::adapter::admin_session_projection::safe_operation_id;
use crate::model::contract::observed_at_from_millis;
use crate::model::contract::QueryPayload;
use crate::tools::broker_tools::BrokerDiagnosticsOutput;
use crate::tools::cluster_tools::BrokerSummary;
use crate::tools::config_tools::BrokerConfigSummaryOutput;
use crate::tools::config_tools::BrokerLogFilterStateOutput;
use crate::tools::config_tools::ConsumerGroupConfigStateOutput;
use crate::tools::config_tools::ConsumerGroupConfigStateRow;
use crate::tools::config_tools::TopicConfigStateOutput;
use crate::tools::config_tools::TopicConfigStateRow;
use crate::tools::consumer_tools::ConsumerGroupSummary;
use crate::tools::consumer_tools::ConsumerProgressQueueRow;
use crate::tools::consumer_tools::ConsumerProgressState;
use crate::tools::consumer_tools::GetConsumerGroupDetailsOutput;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::executor::ToolExecutionError;
use crate::tools::proxy_tools::ProxyDrainStateOutput;
use crate::tools::topic_tools::TopicRouteBroker;
use crate::tools::topic_tools::TopicRouteQueue;
use crate::tools::topic_tools::TopicStatsQueueRow;

mod consumer_observation;
mod topic_observation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedCluster {
    pub name: String,
    pub rocketmq_cluster_name: String,
    pub namesrv_addr: String,
    pub credentials: Option<AdminCredentials>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionTopicRoute {
    pub brokers: Vec<TopicRouteBroker>,
    pub queues: Vec<TopicRouteQueue>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionConsumerLag {
    pub queues: Vec<QueueLag>,
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionConsumerProgress {
    pub state: ConsumerProgressState,
    pub topic_count: usize,
    pub queue_count: usize,
    pub total_lag: u64,
    pub max_queue_lag: u64,
    pub total_inflight: u64,
    pub consume_tps: f64,
    pub queues: Vec<ConsumerProgressQueueRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionTopicStats {
    pub total_message_count: u64,
    pub queue_count: usize,
    pub queues: Vec<TopicStatsQueueRow>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionConnectionRow {
    pub broker_name: String,
    pub client_id: String,
    pub client_addr: String,
    pub language: String,
    pub version: i32,
    pub last_update_timestamp: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionConnections {
    pub rows: Vec<SessionConnectionRow>,
    pub queried_broker_count: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionMessageMetadata {
    pub message_id: String,
    pub unique_message_id: Option<String>,
    pub topic: String,
    pub born_timestamp: i64,
    pub store_timestamp: i64,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub store_size: i32,
    pub reconsume_times: i32,
    pub sys_flag: i32,
    pub flag: i32,
    pub prepared_transaction_offset: i64,
}

pub(crate) trait AdminSession: Send {
    fn broker_rows(
        &mut self,
    ) -> impl Future<Output = Result<QueryPayload<Vec<BrokerSummary>>, ToolExecutionError>> + Send;

    fn topic_inventory(&mut self) -> impl Future<Output = Result<Vec<String>, ToolExecutionError>> + Send;

    fn topic_route(
        &mut self,
        topic: &str,
    ) -> impl Future<Output = Result<SessionTopicRoute, ToolExecutionError>> + Send;

    fn topic_stats(
        &mut self,
        _topic: &str,
    ) -> impl Future<Output = Result<QueryPayload<SessionTopicStats>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Topic statistics are unavailable".to_string(),
            ))
        }
    }

    fn topic_config(
        &mut self,
        _topic: &str,
    ) -> impl Future<Output = Result<QueryPayload<crate::tools::config_tools::GetTopicConfigOutput>, ToolExecutionError>>
           + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Topic configuration is unavailable".to_string(),
            ))
        }
    }

    fn consumer_groups(
        &mut self,
    ) -> impl Future<Output = Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError>> + Send;

    fn consumer_group_inventory(
        &mut self,
    ) -> impl Future<Output = Result<QueryPayload<Vec<String>>, ToolExecutionError>> + Send {
        async {
            self.consumer_groups().await.map(|groups| {
                groups.map(|groups| {
                    let mut names = groups.into_iter().map(|group| group.group).collect::<Vec<_>>();
                    names.sort();
                    names.dedup();
                    names
                })
            })
        }
    }

    fn consumer_groups_exact(
        &mut self,
        groups: &[String],
    ) -> impl Future<Output = Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError>> + Send {
        async move {
            self.consumer_groups().await.map(|result| {
                result.map(|summaries| {
                    summaries
                        .into_iter()
                        .filter(|summary| groups.binary_search(&summary.group).is_ok())
                        .collect()
                })
            })
        }
    }

    fn consumer_lag(
        &mut self,
        topic: &str,
        consumer_group: &str,
    ) -> impl Future<Output = Result<QueryPayload<SessionConsumerLag>, ToolExecutionError>> + Send;

    fn consumer_group_details(
        &mut self,
        _consumer_group: &str,
    ) -> impl Future<Output = Result<QueryPayload<GetConsumerGroupDetailsOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "consumer group details are unavailable".to_string(),
            ))
        }
    }

    fn consumer_progress(
        &mut self,
        _consumer_group: &str,
    ) -> impl Future<Output = Result<QueryPayload<SessionConsumerProgress>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "consumer progress is unavailable".to_string(),
            ))
        }
    }

    fn probe_broker_runtime_target(
        &mut self,
        broker_name: &str,
    ) -> impl Future<Output = Result<BrokerRuntimeTargetStatus, ToolExecutionError>> + Send;

    fn broker_diagnostics(
        &mut self,
        _broker_name: &str,
    ) -> impl Future<Output = Result<QueryPayload<BrokerDiagnosticsOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "exact Broker diagnostics are unavailable".to_string(),
            ))
        }
    }

    fn broker_config_summary(
        &mut self,
        _broker_name: &str,
    ) -> impl Future<Output = Result<QueryPayload<BrokerConfigSummaryOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "exact Broker configuration is unavailable".to_string(),
            ))
        }
    }

    fn broker_log_filter_state(
        &mut self,
        _broker_name: &str,
        _logger: &str,
    ) -> impl Future<Output = Result<QueryPayload<BrokerLogFilterStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "exact Broker log-filter state is unavailable".to_string(),
            ))
        }
    }

    fn proxy_drain_state(
        &mut self,
        _proxy_name: &str,
        _proxy_endpoint: &str,
    ) -> impl Future<Output = Result<QueryPayload<ProxyDrainStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Proxy drain state is unavailable".to_string(),
            ))
        }
    }

    fn consumer_connections(
        &mut self,
        _consumer_group: &str,
    ) -> impl Future<Output = Result<QueryPayload<SessionConnections>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "consumer connection observations are unavailable".to_string(),
            ))
        }
    }

    fn producer_connections(
        &mut self,
        _topic: &str,
        _producer_group: &str,
    ) -> impl Future<Output = Result<QueryPayload<SessionConnections>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "producer connection observations are unavailable".to_string(),
            ))
        }
    }

    fn message_metadata(
        &mut self,
        _message_id: &str,
    ) -> impl Future<Output = Result<SessionMessageMetadata, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "message metadata is unavailable".to_string(),
            ))
        }
    }

    fn topic_config_state(
        &mut self,
        _topic: &str,
        _broker_names: &[String],
    ) -> impl Future<Output = Result<QueryPayload<TopicConfigStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Topic configuration state is unavailable".to_string(),
            ))
        }
    }

    fn consumer_group_config_state(
        &mut self,
        _group: &str,
        _broker_names: &[String],
    ) -> impl Future<Output = Result<QueryPayload<ConsumerGroupConfigStateOutput>, ToolExecutionError>> + Send {
        async {
            Err(ToolExecutionError::Backend(
                "Consumer Group configuration state is unavailable".to_string(),
            ))
        }
    }

    fn shutdown(self) -> impl Future<Output = Result<(), ToolExecutionError>> + Send;
}

pub(crate) trait AdminSessionFactory: Clone + Send + Sync + 'static {
    type Session: AdminSession;

    fn start(&self, cluster: ResolvedCluster)
        -> impl Future<Output = Result<Self::Session, ToolExecutionError>> + Send;
}

#[derive(Clone)]
pub(crate) struct AdminCoreSessionFactory {
    client_runtime: Arc<ClientRuntime>,
    #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
    test_session_factory: Option<ProtocolTestSessionFactory>,
}

impl std::fmt::Debug for AdminCoreSessionFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("AdminCoreSessionFactory");
        debug.field("client_runtime", &"explicit");
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        debug.field("test_session_factory", &self.test_session_factory.is_some());
        debug.finish()
    }
}

impl AdminCoreSessionFactory {
    pub(crate) fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self {
            client_runtime,
            #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
            test_session_factory: None,
        }
    }

    #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
    pub(crate) fn with_test_session_factory(mut self, factory: ProtocolTestSessionFactory) -> Self {
        self.test_session_factory = Some(factory);
        self
    }
}

impl AdminSessionFactory for AdminCoreSessionFactory {
    type Session = AdminCoreSession;

    async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(factory) = &self.test_session_factory {
            return Ok(AdminCoreSession {
                cluster,
                admin: None,
                test_session: Some(factory.start()),
            });
        }
        let mut builder = ReadAdminBuilder::new(self.client_runtime.clone()).namesrv_addr(cluster.namesrv_addr.clone());
        if let Some(credentials) = cluster.credentials.clone() {
            builder = builder.credentials(credentials);
        }
        let admin = builder.build_with_guard().await.map_err(ToolExecutionError::backend)?;
        Ok(AdminCoreSession {
            cluster,
            admin: Some(admin),
            #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
            test_session: None,
        })
    }
}

pub(crate) struct AdminCoreSession {
    cluster: ResolvedCluster,
    admin: Option<ReadAdminGuard>,
    #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
    test_session: Option<ProtocolTestSession>,
}

impl AdminCoreSession {
    fn admin_mut(&mut self) -> Result<&mut ReadAdminGuard, ToolExecutionError> {
        self.admin
            .as_mut()
            .ok_or_else(|| ToolExecutionError::internal("admin session is already shut down"))
    }
}

impl AdminSession for AdminCoreSession {
    async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.broker_rows().await;
        }
        let request = ListBrokersRequest::try_new(self.cluster.rocketmq_cluster_name.clone())
            .map_err(ToolExecutionError::backend)?;
        let result = self
            .admin_mut()?
            .list_brokers_with_evidence(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(QueryPayload::from_admin(result).map(|result| result.brokers.iter().map(map_broker_summary).collect()))
    }

    async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.topic_inventory().await;
        }
        let request = TopicInventoryRequest::new(Some(self.cluster.rocketmq_cluster_name.clone()));
        let result = self
            .admin_mut()?
            .get_topic_inventory(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(result.topics)
    }

    async fn topic_route(&mut self, topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.topic_route(topic).await;
        }
        let request = GetTopicRouteRequest::try_new(topic).map_err(ToolExecutionError::backend)?;
        let route = self
            .admin_mut()?
            .get_topic_route(&request)
            .await
            .map_err(ToolExecutionError::backend)?
            .ok_or_else(|| ToolExecutionError::Backend(format!("topic route not found: {topic}")))?;
        let mut brokers = route
            .brokers
            .iter()
            .map(|broker| TopicRouteBroker {
                cluster: broker.cluster.clone(),
                broker_name: broker.broker_name.clone(),
                broker_addrs: broker
                    .broker_addrs
                    .iter()
                    .map(|(broker_id, broker_addr)| (broker_id.to_string(), broker_addr.to_string()))
                    .collect::<BTreeMap<_, _>>(),
                zone_name: broker.zone_name.clone(),
                enable_acting_master: broker.enable_acting_master,
            })
            .collect::<Vec<_>>();
        brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        let mut queues = route
            .queues
            .iter()
            .map(|queue| TopicRouteQueue {
                broker_name: queue.broker_name.clone(),
                read_queue_nums: queue.read_queue_nums,
                write_queue_nums: queue.write_queue_nums,
                perm: queue.perm,
                topic_sys_flag: queue.topic_sys_flag,
            })
            .collect::<Vec<_>>();
        queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        Ok(SessionTopicRoute { brokers, queues })
    }

    async fn topic_stats(&mut self, topic: &str) -> Result<QueryPayload<SessionTopicStats>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.topic_stats(topic).await;
        }
        self.query_topic_stats_observation(topic).await
    }

    async fn topic_config(
        &mut self,
        topic: &str,
    ) -> Result<QueryPayload<crate::tools::config_tools::GetTopicConfigOutput>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.topic_config(topic).await;
        }
        self.query_topic_config_observation(topic).await
    }

    async fn consumer_group_details(
        &mut self,
        consumer_group: &str,
    ) -> Result<QueryPayload<GetConsumerGroupDetailsOutput>, ToolExecutionError> {
        self.query_consumer_group_details_observation(consumer_group).await
    }

    async fn consumer_progress(
        &mut self,
        consumer_group: &str,
    ) -> Result<QueryPayload<SessionConsumerProgress>, ToolExecutionError> {
        self.query_consumer_progress_observation(consumer_group).await
    }

    async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.consumer_groups().await;
        }
        let result = self
            .admin_mut()?
            .list_consumer_groups_with_evidence(&ListConsumerGroupsRequest)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(QueryPayload::from_admin(result).map(|result| {
            result
                .groups
                .into_iter()
                .map(|group| ConsumerGroupSummary {
                    group: group.group,
                    version: group.version,
                    client_count: group.client_count,
                    consume_type: group.consume_type,
                    message_model: group.message_model,
                    consume_tps: group.consume_tps,
                    diff_total: group.diff_total,
                })
                .collect()
        }))
    }

    async fn consumer_group_inventory(&mut self) -> Result<QueryPayload<Vec<String>>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.consumer_group_inventory().await;
        }
        let result = self
            .admin_mut()?
            .list_consumer_group_inventory_with_evidence(&ListConsumerGroupsRequest)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(QueryPayload::from_admin(result).map(|result| result.groups))
    }

    async fn consumer_groups_exact(
        &mut self,
        groups: &[String],
    ) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.consumer_groups_exact(groups).await;
        }
        let request = ExactConsumerGroupEnrichmentRequest::try_new(groups.iter().cloned())
            .map_err(ToolExecutionError::backend)?;
        let result = self
            .admin_mut()?
            .enrich_consumer_groups_exact_with_evidence(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(QueryPayload::from_admin(result).map(|result| {
            result
                .groups
                .into_iter()
                .map(|group| ConsumerGroupSummary {
                    group: group.group,
                    version: group.version,
                    client_count: group.client_count,
                    consume_type: group.consume_type,
                    message_model: group.message_model,
                    consume_tps: group.consume_tps,
                    diff_total: group.diff_total,
                })
                .collect()
        }))
    }

    async fn consumer_lag(
        &mut self,
        topic: &str,
        consumer_group: &str,
    ) -> Result<QueryPayload<SessionConsumerLag>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.consumer_lag(topic, consumer_group).await;
        }
        let request =
            QueryConsumerLagRequest::try_new(topic, consumer_group, false).map_err(ToolExecutionError::backend)?;
        let result = self
            .admin_mut()?
            .query_consumer_lag_with_evidence(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(QueryPayload::from_admin(result).map(|result| {
            let mut queues = result
                .rows
                .iter()
                .map(|row| QueueLag {
                    topic: row.topic.to_string(),
                    broker_name: row.broker_name.to_string(),
                    queue_id: row.queue_id,
                    broker_offset: row.broker_offset,
                    consumer_offset: row.consumer_offset,
                    lag: row.lag,
                    inflight: row.inflight,
                    last_observed_at: observed_at_from_millis(row.last_timestamp),
                    client_ip: row.client_ip.clone(),
                })
                .collect::<Vec<_>>();
            queues.sort_by(|left, right| {
                left.broker_name
                    .cmp(&right.broker_name)
                    .then(left.queue_id.cmp(&right.queue_id))
            });
            SessionConsumerLag {
                queues,
                total_lag: result.total_lag,
                consume_tps: result.consume_tps,
                inflight_total: result.inflight_total,
            }
        }))
    }

    async fn probe_broker_runtime_target(
        &mut self,
        broker_name: &str,
    ) -> Result<BrokerRuntimeTargetStatus, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.probe_broker_runtime_target(broker_name).await;
        }
        let request = ProbeBrokerRuntimeTargetRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            broker_name.to_string(),
        )
        .map_err(ToolExecutionError::backend)?;
        self.admin_mut()?
            .probe_broker_runtime_target(&request)
            .await
            .map_err(ToolExecutionError::backend)
    }

    async fn broker_diagnostics(
        &mut self,
        broker_name: &str,
    ) -> Result<QueryPayload<BrokerDiagnosticsOutput>, ToolExecutionError> {
        let request =
            QueryBrokerDiagnosticsTargetRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), broker_name)
                .map_err(map_logical_admin_error)?;
        let result = self
            .admin_mut()?
            .query_broker_diagnostics_target_with_evidence(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(QueryPayload::from_admin(result).map(|result| BrokerDiagnosticsOutput {
            cluster,
            broker_name: request.broker_name().to_string(),
            diagnostics_schema_version: rocketmq_admin_core::core::broker::BROKER_DIAGNOSTICS_SCHEMA_VERSION
                .to_string(),
            observed_at_millis: result.observed_at_millis,
            brokers: result.brokers.iter().map(map_broker_diagnostics).collect(),
            unavailable_brokers: result.unavailable_brokers,
        }))
    }

    async fn broker_config_summary(
        &mut self,
        broker_name: &str,
    ) -> Result<QueryPayload<BrokerConfigSummaryOutput>, ToolExecutionError> {
        let request =
            QueryBrokerAllowlistedConfigTargetRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), broker_name)
                .map_err(map_logical_admin_error)?;
        let result = self
            .admin_mut()?
            .query_allowlisted_config_target_with_evidence(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        Ok(QueryPayload::from_admin(result).map(|rows| BrokerConfigSummaryOutput {
            cluster,
            broker_name: request.broker_name().to_string(),
            brokers: rows
                .into_iter()
                .map(|row| crate::tools::config_tools::BrokerConfigSummaryRow {
                    broker_name: row.broker_name,
                    broker_id: row.broker_id,
                    generation: row.config.generation,
                    send_message_thread_pool_nums: row.config.send_message_thread_pool_nums,
                    pull_message_thread_pool_nums: row.config.pull_message_thread_pool_nums,
                    flush_delay_offset_interval_ms: row.config.flush_delay_offset_interval_ms,
                    max_client_event_count: row.config.max_client_event_count,
                })
                .collect(),
        }))
    }

    async fn broker_log_filter_state(
        &mut self,
        broker_name: &str,
        logger: &str,
    ) -> Result<QueryPayload<BrokerLogFilterStateOutput>, ToolExecutionError> {
        let request = QueryBrokerLogFilterStateTargetRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            broker_name,
            logger,
        )
        .map_err(map_logical_admin_error)?;
        let result = self
            .admin_mut()?
            .query_log_filter_state_target_with_evidence(&request)
            .await
            .map_err(map_logical_admin_error)?;
        let cluster = self.cluster.name.clone();
        let payload = QueryPayload::from_admin(result);
        let mut sanitized = false;
        let brokers = payload
            .data
            .into_iter()
            .map(|row| {
                let (active_operation_id, active_sanitized) = safe_operation_id(row.state.active_operation_id);
                let (last_completed_operation_id, completed_sanitized) =
                    safe_operation_id(row.state.last_completed_operation_id);
                sanitized |= active_sanitized || completed_sanitized;
                crate::tools::config_tools::BrokerLogFilterStateRow {
                    broker_name: row.broker_name,
                    broker_id: row.broker_id,
                    state_schema_version: rocketmq_admin_core::core::broker::BROKER_LOG_FILTER_STATE_SCHEMA_VERSION
                        .to_string(),
                    supported: row.state.supported,
                    logger: request.logger().to_string(),
                    level: row.state.level.map(|level| match level {
                        rocketmq_admin_core::core::broker::BrokerLogLevel::Info => {
                            crate::tools::config_tools::BrokerLogLevel::Info
                        }
                        rocketmq_admin_core::core::broker::BrokerLogLevel::Debug => {
                            crate::tools::config_tools::BrokerLogLevel::Debug
                        }
                    }),
                    active_operation_id,
                    last_completed_operation_id,
                    expires_at_millis: row.state.expires_at_millis,
                }
            })
            .collect();
        let mut warnings = payload.warnings;
        if sanitized {
            warnings.push("broker_log_filter_operation_id_sanitized".to_string());
        }
        Ok(QueryPayload::new(
            BrokerLogFilterStateOutput {
                cluster,
                broker_name: request.broker_name().to_string(),
                logger: request.logger().to_string(),
                brokers,
            },
            payload.partial,
            warnings,
            payload.source_failures,
        ))
    }

    async fn proxy_drain_state(
        &mut self,
        proxy_name: &str,
        proxy_endpoint: &str,
    ) -> Result<QueryPayload<ProxyDrainStateOutput>, ToolExecutionError> {
        let request = QueryProxyDrainStateRequest {
            proxy_addr: proxy_endpoint.to_string(),
        };
        let state = self
            .admin_mut()?
            .query_drain_state(&request)
            .await
            .map_err(|_| ToolExecutionError::Backend("Proxy drain source is unavailable".to_string()))?;
        let (operation_id, warnings) = bounded_proxy_operation_id(state.operation_id);
        let output = ProxyDrainStateOutput {
            cluster: self.cluster.name.clone(),
            proxy_name: proxy_name.to_string(),
            state_schema_version: "rocketmq.proxy-drain.v1".to_string(),
            phase: match state.phase {
                rocketmq_admin_core::core::proxy::ProxyDrainPhase::Accepting => {
                    crate::tools::proxy_tools::ProxyDrainPhase::Accepting
                }
                rocketmq_admin_core::core::proxy::ProxyDrainPhase::Draining => {
                    crate::tools::proxy_tools::ProxyDrainPhase::Draining
                }
                rocketmq_admin_core::core::proxy::ProxyDrainPhase::Drained => {
                    crate::tools::proxy_tools::ProxyDrainPhase::Drained
                }
            },
            operation_id,
            admission_open: state.admission_open,
            routing_open: state.routing_open,
            readiness_published: state.readiness_published,
            zero_pending: state.zero_pending,
            pending: crate::tools::proxy_tools::ProxyDrainPending {
                active_connections: state.pending.active_connections,
                sessions: state.pending.sessions,
                receipt_handles: state.pending.receipt_handles,
                prepared_transactions: state.pending.prepared_transactions,
                telemetry_links: state.pending.telemetry_links,
                remoting_channels: state.pending.remoting_channels,
                telemetry_commands: state.pending.telemetry_commands,
                rpc_in_flight: state.pending.rpc_in_flight,
            },
        };
        Ok(QueryPayload::new(output, false, warnings, Vec::new()))
    }

    async fn consumer_connections(
        &mut self,
        consumer_group: &str,
    ) -> Result<QueryPayload<SessionConnections>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.consumer_connections(consumer_group).await;
        }
        const MAX_CONNECTIONS: usize = 1_000;
        let request = QueryConsumerConnectionsRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            consumer_group,
            MAX_CONNECTIONS,
        )
        .map_err(|_| ToolExecutionError::InvalidArguments("invalid consumer connection selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_consumer_connections_with_evidence(&request)
            .await
            .map_err(|_| ToolExecutionError::Backend("consumer connection source is unavailable".to_string()))?;
        Ok(QueryPayload::from_admin(result).map(|result| SessionConnections {
            rows: result
                .connections
                .into_iter()
                .map(|row| SessionConnectionRow {
                    broker_name: row.broker_name,
                    client_id: row.client_id,
                    client_addr: row.client_addr,
                    language: row.language,
                    version: row.version,
                    last_update_timestamp: row.last_update_timestamp,
                })
                .collect(),
            queried_broker_count: result.queried_broker_count,
            truncated: result.truncated,
        }))
    }

    async fn producer_connections(
        &mut self,
        topic: &str,
        producer_group: &str,
    ) -> Result<QueryPayload<SessionConnections>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.producer_connections(topic, producer_group).await;
        }
        const MAX_CONNECTIONS: usize = 1_000;
        let request = QueryTopicProducerConnectionsRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            topic,
            producer_group,
            MAX_CONNECTIONS,
        )
        .map_err(|_| ToolExecutionError::InvalidArguments("invalid producer connection selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_topic_producer_connections_with_evidence(&request)
            .await
            .map_err(|_| ToolExecutionError::Backend("producer connection source is unavailable".to_string()))?;
        Ok(QueryPayload::from_admin(result).map(|result| SessionConnections {
            rows: result
                .connections
                .into_iter()
                .map(|row| SessionConnectionRow {
                    broker_name: row.broker_name,
                    client_id: row.client_id,
                    client_addr: row.client_addr,
                    language: row.language,
                    version: row.version,
                    last_update_timestamp: row.last_update_timestamp,
                })
                .collect(),
            queried_broker_count: result.queried_broker_count,
            truncated: result.truncated,
        }))
    }

    async fn message_metadata(&mut self, message_id: &str) -> Result<SessionMessageMetadata, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.message_metadata(message_id).await;
        }
        let request = MessageMetadataRequest::try_new(self.cluster.rocketmq_cluster_name.clone(), message_id)
            .map_err(|_| ToolExecutionError::InvalidArguments("invalid message identifier".to_string()))?;
        let metadata = MessageMetadataQueryAdmin::query_message_metadata(self.admin_mut()?.inner_mut(), &request)
            .await
            .map_err(|_| ToolExecutionError::Backend("message metadata source is unavailable".to_string()))?;
        Ok(SessionMessageMetadata {
            message_id: metadata.message_id,
            unique_message_id: metadata.unique_message_id,
            topic: metadata.topic,
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

    async fn topic_config_state(
        &mut self,
        topic: &str,
        broker_names: &[String],
    ) -> Result<QueryPayload<TopicConfigStateOutput>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.topic_config_state(topic, broker_names).await;
        }
        let request = TopicConfigStateRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            topic,
            broker_names.iter().cloned(),
        )
        .map_err(|_| ToolExecutionError::InvalidArguments("invalid Topic configuration selector".to_string()))?;
        let result = self
            .admin_mut()?
            .query_topic_config_state(&request)
            .await
            .map_err(|_| ToolExecutionError::Backend("Topic configuration state source is unavailable".to_string()))?;
        let cluster = self.cluster.name.clone();
        Ok(QueryPayload::from_admin(result).map(|result| TopicConfigStateOutput {
            cluster,
            topic: result.topic,
            brokers: result
                .states
                .into_iter()
                .map(|row| TopicConfigStateRow {
                    broker_name: row.broker_name,
                    version: row.version,
                    read_queue_nums: row.read_queue_nums,
                    write_queue_nums: row.write_queue_nums,
                    order: row.order,
                })
                .collect(),
        }))
    }

    async fn consumer_group_config_state(
        &mut self,
        group: &str,
        broker_names: &[String],
    ) -> Result<QueryPayload<ConsumerGroupConfigStateOutput>, ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = &mut self.test_session {
            return session.consumer_group_config_state(group, broker_names).await;
        }
        let request = ConsumerGroupConfigStateRequest::try_new(
            self.cluster.rocketmq_cluster_name.clone(),
            group,
            broker_names.iter().cloned(),
        )
        .map_err(|_| {
            ToolExecutionError::InvalidArguments("invalid Consumer Group configuration selector".to_string())
        })?;
        let result = self
            .admin_mut()?
            .query_consumer_group_config_state(&request)
            .await
            .map_err(|_| {
                ToolExecutionError::Backend("Consumer Group configuration state source is unavailable".to_string())
            })?;
        let cluster = self.cluster.name.clone();
        Ok(
            QueryPayload::from_admin(result).map(|result| ConsumerGroupConfigStateOutput {
                cluster,
                group: result.group,
                brokers: result
                    .states
                    .into_iter()
                    .map(|row| ConsumerGroupConfigStateRow {
                        broker_name: row.broker_name,
                        version: row.version,
                        retry_max_times: row.retry_max_times,
                        retry_queue_nums: row.retry_queue_nums,
                        consume_timeout_minutes: row.consume_timeout_minutes,
                        consume_enable: row.consume_enable,
                        consume_from_min_enable: row.consume_from_min_enable,
                        consume_broadcast_enable: row.consume_broadcast_enable,
                        consume_message_orderly: row.consume_message_orderly,
                        broker_id: row.broker_id,
                        which_broker_when_consume_slowly: row.which_broker_when_consume_slowly,
                        notify_consumer_ids_changed_enable: row.notify_consumer_ids_changed_enable,
                        group_sys_flag: row.group_sys_flag,
                    })
                    .collect(),
            }),
        )
    }

    async fn shutdown(mut self) -> Result<(), ToolExecutionError> {
        #[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
        if let Some(session) = self.test_session.take() {
            return session.shutdown().await;
        }
        if let Some(admin) = self.admin.take() {
            admin.shutdown().await;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
mod protocol_test_support {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use tokio::sync::Barrier;

    use super::*;

    #[derive(Debug, Default)]
    pub(crate) struct ProtocolTestCounters {
        pub(crate) starts: AtomicUsize,
        pub(crate) shutdowns: AtomicUsize,
        pub(crate) broker_queries: AtomicUsize,
        pub(crate) topic_inventory_queries: AtomicUsize,
    }

    #[derive(Debug)]
    pub(crate) struct ProtocolTestGate {
        entered: AtomicUsize,
        release: Barrier,
    }

    impl ProtocolTestGate {
        pub(crate) fn new(expected_loaders: usize) -> Self {
            Self {
                entered: AtomicUsize::new(0),
                release: Barrier::new(expected_loaders + 1),
            }
        }

        async fn wait(&self) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.release.wait().await;
        }

        pub(crate) async fn wait_until_entered(&self, expected: usize) {
            for _ in 0..10_000 {
                if self.entered.load(Ordering::SeqCst) == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(self.entered.load(Ordering::SeqCst), expected);
        }

        pub(crate) async fn release(&self) {
            self.release.wait().await;
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct ProtocolTestSessionFactory {
        pub(crate) counters: Arc<ProtocolTestCounters>,
        gate: Option<Arc<ProtocolTestGate>>,
    }

    impl ProtocolTestSessionFactory {
        pub(crate) fn new(gate: Option<Arc<ProtocolTestGate>>) -> Self {
            Self {
                counters: Arc::new(ProtocolTestCounters::default()),
                gate,
            }
        }

        pub(super) fn start(&self) -> ProtocolTestSession {
            self.counters.starts.fetch_add(1, Ordering::SeqCst);
            ProtocolTestSession {
                counters: self.counters.clone(),
                gate: self.gate.clone(),
            }
        }
    }

    pub(crate) struct ProtocolTestSession {
        counters: Arc<ProtocolTestCounters>,
        gate: Option<Arc<ProtocolTestGate>>,
    }

    impl AdminSession for ProtocolTestSession {
        async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolExecutionError> {
            self.counters.broker_queries.fetch_add(1, Ordering::SeqCst);
            Ok(QueryPayload::complete(Vec::new()))
        }

        async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolExecutionError> {
            self.counters.topic_inventory_queries.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.wait().await;
            }
            Ok(vec!["payments".to_string(), "orders".to_string()])
        }

        async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
            Ok(SessionTopicRoute {
                brokers: Vec::new(),
                queues: Vec::new(),
            })
        }

        async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolExecutionError> {
            Ok(QueryPayload::complete(Vec::new()))
        }

        async fn consumer_lag(
            &mut self,
            _topic: &str,
            _consumer_group: &str,
        ) -> Result<QueryPayload<SessionConsumerLag>, ToolExecutionError> {
            Ok(QueryPayload::complete(SessionConsumerLag {
                queues: Vec::new(),
                total_lag: 0,
                consume_tps: 0.0,
                inflight_total: 0,
            }))
        }

        async fn probe_broker_runtime_target(
            &mut self,
            _broker_name: &str,
        ) -> Result<BrokerRuntimeTargetStatus, ToolExecutionError> {
            Ok(BrokerRuntimeTargetStatus::NotFound)
        }

        async fn shutdown(self) -> Result<(), ToolExecutionError> {
            self.counters.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

#[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
pub(crate) use protocol_test_support::ProtocolTestCounters;
#[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
pub(crate) use protocol_test_support::ProtocolTestGate;
#[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
pub(crate) use protocol_test_support::ProtocolTestSession;
#[cfg(all(test, feature = "streamable-http", feature = "stdio"))]
pub(crate) use protocol_test_support::ProtocolTestSessionFactory;

fn map_broker_summary(row: &rocketmq_admin_core::core::broker::BrokerSummary) -> BrokerSummary {
    BrokerSummary {
        cluster: row.cluster.clone(),
        broker_name: row.broker_name.clone(),
        broker_id: row.broker_id,
        broker_addr: row.broker_addr.clone(),
        version: row.version.clone(),
        in_tps: row.in_tps.clone(),
        out_tps: row.out_tps.clone(),
        timer_progress: row.timer_progress.clone(),
        page_cache_lock_time_millis: row.page_cache_lock_time_millis.clone(),
        hour: row.hour.clone(),
        space: row.space.clone(),
        broker_active: row.broker_active,
    }
}

fn map_logical_admin_error(error: rocketmq_admin_core::core::AdminError) -> ToolExecutionError {
    match error {
        rocketmq_admin_core::core::AdminError::InvalidArgument { field, reason } => {
            ToolExecutionError::InvalidArguments(format!("{field}: {reason}"))
        }
        rocketmq_admin_core::core::AdminError::NotFound { resource, name } => {
            ToolExecutionError::InvalidArguments(format!("{resource} not found: {name}"))
        }
        _ => ToolExecutionError::Backend("RocketMQ logical target source is unavailable".to_string()),
    }
}
