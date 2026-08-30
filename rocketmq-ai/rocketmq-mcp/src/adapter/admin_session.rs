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
use rocketmq_admin_core::core::broker::ListBrokersRequest;
use rocketmq_admin_core::core::broker::ProbeBrokerRuntimeRequest;
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;
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

use crate::model::contract::observed_at_from_millis;
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

#[derive(Debug, Clone)]
pub(crate) struct SessionTopicRoute {
    pub brokers: Vec<TopicRouteBroker>,
    pub queues: Vec<TopicRouteQueue>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionConsumerLag {
    pub queues: Vec<QueueLag>,
    pub total_lag: i64,
    pub consume_tps: f64,
    pub inflight_total: i64,
}

pub(crate) trait AdminSession: Send {
    fn broker_rows(&mut self) -> impl Future<Output = Result<Vec<BrokerSummary>, ToolExecutionError>> + Send;

    fn topic_inventory(&mut self) -> impl Future<Output = Result<Vec<String>, ToolExecutionError>> + Send;

    fn topic_route(
        &mut self,
        topic: &str,
    ) -> impl Future<Output = Result<SessionTopicRoute, ToolExecutionError>> + Send;

    fn consumer_groups(&mut self)
        -> impl Future<Output = Result<Vec<ConsumerGroupSummary>, ToolExecutionError>> + Send;

    fn consumer_lag(
        &mut self,
        topic: &str,
        consumer_group: &str,
    ) -> impl Future<Output = Result<SessionConsumerLag, ToolExecutionError>> + Send;

    fn probe_broker_runtime(&mut self) -> impl Future<Output = Result<(), ToolExecutionError>> + Send;

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
}

impl std::fmt::Debug for AdminCoreSessionFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminCoreSessionFactory")
            .field("client_runtime", &"explicit")
            .finish()
    }
}

impl AdminCoreSessionFactory {
    pub(crate) fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self { client_runtime }
    }
}

impl AdminSessionFactory for AdminCoreSessionFactory {
    type Session = AdminCoreSession;

    async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
        let mut builder = ReadAdminBuilder::new(self.client_runtime.clone()).namesrv_addr(cluster.namesrv_addr.clone());
        if let Some(credentials) = cluster.credentials.clone() {
            builder = builder.credentials(credentials);
        }
        let admin = builder.build_with_guard().await.map_err(ToolExecutionError::backend)?;
        Ok(AdminCoreSession {
            cluster,
            admin: Some(admin),
        })
    }
}

pub(crate) struct AdminCoreSession {
    cluster: ResolvedCluster,
    admin: Option<ReadAdminGuard>,
}

impl AdminCoreSession {
    fn admin_mut(&mut self) -> Result<&mut ReadAdminGuard, ToolExecutionError> {
        self.admin
            .as_mut()
            .ok_or_else(|| ToolExecutionError::internal("admin session is already shut down"))
    }
}

impl AdminSession for AdminCoreSession {
    async fn broker_rows(&mut self) -> Result<Vec<BrokerSummary>, ToolExecutionError> {
        let request = ListBrokersRequest::try_new(self.cluster.rocketmq_cluster_name.clone())
            .map_err(ToolExecutionError::backend)?;
        let result = self
            .admin_mut()?
            .list_brokers(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(result.brokers.iter().map(map_broker_summary).collect())
    }

    async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolExecutionError> {
        let request = TopicInventoryRequest::new(Some(self.cluster.rocketmq_cluster_name.clone()));
        let result = self
            .admin_mut()?
            .get_topic_inventory(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(result.topics)
    }

    async fn topic_route(&mut self, topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
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

    async fn consumer_groups(&mut self) -> Result<Vec<ConsumerGroupSummary>, ToolExecutionError> {
        let result = self
            .admin_mut()?
            .list_consumer_groups(&ListConsumerGroupsRequest)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(result
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
            .collect())
    }

    async fn consumer_lag(
        &mut self,
        topic: &str,
        consumer_group: &str,
    ) -> Result<SessionConsumerLag, ToolExecutionError> {
        let request =
            QueryConsumerLagRequest::try_new(topic, consumer_group, false).map_err(ToolExecutionError::backend)?;
        let result = self
            .admin_mut()?
            .query_consumer_lag(&request)
            .await
            .map_err(ToolExecutionError::backend)?;
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
        Ok(SessionConsumerLag {
            queues,
            total_lag: result.total_lag,
            consume_tps: result.consume_tps,
            inflight_total: result.inflight_total,
        })
    }

    async fn probe_broker_runtime(&mut self) -> Result<(), ToolExecutionError> {
        let request = ProbeBrokerRuntimeRequest::try_new(self.cluster.rocketmq_cluster_name.clone())
            .map_err(ToolExecutionError::backend)?;
        self.admin_mut()?
            .probe_broker_runtime(&request)
            .await
            .map(|_| ())
            .map_err(ToolExecutionError::backend)
    }

    async fn shutdown(mut self) -> Result<(), ToolExecutionError> {
        if let Some(admin) = self.admin.take() {
            admin.shutdown().await;
        }
        Ok(())
    }
}

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
