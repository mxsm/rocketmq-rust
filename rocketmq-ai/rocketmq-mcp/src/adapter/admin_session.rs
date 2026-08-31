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
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;
use rocketmq_admin_core::core::consumer::ExactConsumerGroupEnrichmentRequest;
use rocketmq_admin_core::core::consumer::ListConsumerGroupsRequest;
use rocketmq_admin_core::core::consumer::QueryConsumerLagRequest;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_admin_core::core::topic::GetTopicRouteRequest;
use rocketmq_admin_core::core::topic::TopicInventoryAdmin;
use rocketmq_admin_core::core::topic::TopicInventoryRequest;
use rocketmq_admin_core::core::topic::TopicQueryAdmin;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminGuard;
use serde::Serialize;

use crate::model::contract::observed_at_from_millis;
use crate::model::contract::QueryPayload;
use crate::tools::cluster_tools::BrokerSummary;
use crate::tools::consumer_tools::ConsumerGroupSummary;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::TopicRouteBroker;
use crate::tools::topic_tools::TopicRouteQueue;

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

pub(crate) trait AdminSession: Send {
    fn broker_rows(
        &mut self,
    ) -> impl Future<Output = Result<QueryPayload<Vec<BrokerSummary>>, ToolExecutionError>> + Send;

    fn topic_inventory(&mut self) -> impl Future<Output = Result<Vec<String>, ToolExecutionError>> + Send;

    fn topic_route(
        &mut self,
        topic: &str,
    ) -> impl Future<Output = Result<SessionTopicRoute, ToolExecutionError>> + Send;

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

    fn probe_broker_runtime_target(
        &mut self,
        broker_name: &str,
    ) -> impl Future<Output = Result<BrokerRuntimeTargetStatus, ToolExecutionError>> + Send;

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
