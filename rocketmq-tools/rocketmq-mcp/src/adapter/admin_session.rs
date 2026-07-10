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

use rocketmq_admin_core::core::admin::AdminBuilder;
use rocketmq_admin_core::core::admin::AdminGuard;
use rocketmq_admin_core::core::broker::BrokerRuntimeStatsQueryRequest;
use rocketmq_admin_core::core::broker::BrokerService;
use rocketmq_admin_core::core::cluster::ClusterListQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterService;
use rocketmq_admin_core::core::consumer::ConsumerProgressRequest;
use rocketmq_admin_core::core::consumer::ConsumerProgressResult;
use rocketmq_admin_core::core::consumer::ConsumerService;
use rocketmq_admin_core::core::topic::TopicListQueryRequest;
use rocketmq_admin_core::core::topic::TopicService;

use crate::model::contract::observed_at_from_millis;
use crate::tools::cluster_tools::BrokerSummary;
use crate::tools::consumer_tools::ConsumerGroupSummary;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::executor::ToolExecutionError;
use crate::tools::topic_tools::TopicListEntry;
use crate::tools::topic_tools::TopicRouteBroker;
use crate::tools::topic_tools::TopicRouteQueue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedCluster {
    pub name: String,
    pub namesrv_addr: String,
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

#[async_trait::async_trait]
pub(crate) trait AdminSession: Send {
    async fn broker_rows(&mut self) -> Result<Vec<BrokerSummary>, ToolExecutionError>;

    async fn topic_entries(&mut self) -> Result<Vec<TopicListEntry>, ToolExecutionError>;

    async fn topic_route(&mut self, topic: &str) -> Result<SessionTopicRoute, ToolExecutionError>;

    async fn consumer_groups(&mut self) -> Result<Vec<ConsumerGroupSummary>, ToolExecutionError>;

    async fn consumer_lag(
        &mut self,
        topic: &str,
        consumer_group: &str,
    ) -> Result<SessionConsumerLag, ToolExecutionError>;

    async fn probe_broker_runtime(&mut self) -> Result<(), ToolExecutionError>;

    async fn shutdown(self) -> Result<(), ToolExecutionError>;
}

#[async_trait::async_trait]
pub(crate) trait AdminSessionFactory: Clone + Send + Sync + 'static {
    type Session: AdminSession;

    async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AdminCoreSessionFactory;

#[async_trait::async_trait]
impl AdminSessionFactory for AdminCoreSessionFactory {
    type Session = AdminCoreSession;

    async fn start(&self, cluster: ResolvedCluster) -> Result<Self::Session, ToolExecutionError> {
        let admin = AdminBuilder::new()
            .namesrv_addr(cluster.namesrv_addr.clone())
            .build_with_guard()
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(AdminCoreSession {
            cluster,
            admin: Some(admin),
        })
    }
}

pub(crate) struct AdminCoreSession {
    cluster: ResolvedCluster,
    admin: Option<AdminGuard>,
}

impl AdminCoreSession {
    fn admin(&self) -> Result<&AdminGuard, ToolExecutionError> {
        self.admin
            .as_ref()
            .ok_or_else(|| ToolExecutionError::internal("admin session is already shut down"))
    }

    fn admin_mut(&mut self) -> Result<&mut AdminGuard, ToolExecutionError> {
        self.admin
            .as_mut()
            .ok_or_else(|| ToolExecutionError::internal("admin session is already shut down"))
    }
}

#[async_trait::async_trait]
impl AdminSession for AdminCoreSession {
    async fn broker_rows(&mut self) -> Result<Vec<BrokerSummary>, ToolExecutionError> {
        let request = ClusterListQueryRequest::new(false, Some(self.cluster.name.clone()))
            .with_optional_namesrv_addr(Some(self.cluster.namesrv_addr.clone()));
        let result = ClusterService::query_cluster_list_with_admin(self.admin()?, &request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(result.base_rows.iter().map(map_broker_summary).collect())
    }

    async fn topic_entries(&mut self) -> Result<Vec<TopicListEntry>, ToolExecutionError> {
        let request = TopicListQueryRequest::new()
            .with_optional_namesrv_addr(Some(self.cluster.namesrv_addr.clone()))
            .with_optional_cluster_name(Some(self.cluster.name.clone()));
        let result = TopicService::query_topic_list_with_admin(self.admin_mut()?, &request)
            .await
            .map_err(ToolExecutionError::backend)?;
        Ok(result
            .topics
            .iter()
            .map(|item| TopicListEntry {
                topic: item.topic.to_string(),
                cluster: item.cluster.as_ref().map(ToString::to_string),
                consumer_group: item.consumer_group.as_ref().map(ToString::to_string),
            })
            .collect())
    }

    async fn topic_route(&mut self, topic: &str) -> Result<SessionTopicRoute, ToolExecutionError> {
        let route = TopicService::get_topic_route(self.admin_mut()?, topic.to_string())
            .await
            .map_err(ToolExecutionError::backend)?
            .ok_or_else(|| ToolExecutionError::Backend(format!("topic route not found: {topic}")))?;
        let mut brokers = route
            .broker_datas
            .iter()
            .map(|broker| TopicRouteBroker {
                cluster: broker.cluster().to_string(),
                broker_name: broker.broker_name().to_string(),
                broker_addrs: broker
                    .broker_addrs()
                    .iter()
                    .map(|(broker_id, broker_addr)| (broker_id.to_string(), broker_addr.to_string()))
                    .collect::<BTreeMap<_, _>>(),
                zone_name: broker.zone_name().map(ToString::to_string),
                enable_acting_master: broker.enable_acting_master(),
            })
            .collect::<Vec<_>>();
        brokers.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        let mut queues = route
            .queue_datas
            .iter()
            .map(|queue| TopicRouteQueue {
                broker_name: queue.broker_name().to_string(),
                read_queue_nums: queue.read_queue_nums(),
                write_queue_nums: queue.write_queue_nums(),
                perm: queue.perm(),
                topic_sys_flag: queue.topic_sys_flag(),
            })
            .collect::<Vec<_>>();
        queues.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
        Ok(SessionTopicRoute { brokers, queues })
    }

    async fn consumer_groups(&mut self) -> Result<Vec<ConsumerGroupSummary>, ToolExecutionError> {
        let request = ConsumerProgressRequest::try_new(
            None,
            None,
            false,
            Some(self.cluster.name.clone()),
            Some(self.cluster.namesrv_addr.clone()),
        )
        .map_err(ToolExecutionError::backend)?;
        let result = ConsumerService::query_consumer_progress_with_admin(self.admin()?, &request)
            .await
            .map_err(ToolExecutionError::backend)?;
        let ConsumerProgressResult::All(groups) = result else {
            return Err(ToolExecutionError::internal(
                "consumer group query did not return the expected aggregate result",
            ));
        };
        Ok(groups
            .into_iter()
            .map(|group| ConsumerGroupSummary {
                group: group.group,
                version: group.version,
                client_count: group.count,
                consume_type: format!("{:?}", group.consume_type),
                message_model: format!("{:?}", group.message_model),
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
        let request = ConsumerProgressRequest::try_new(
            Some(consumer_group.to_string()),
            Some(topic.to_string()),
            true,
            Some(self.cluster.name.clone()),
            Some(self.cluster.namesrv_addr.clone()),
        )
        .map_err(ToolExecutionError::backend)?;
        let result = ConsumerService::query_consumer_progress_with_admin(self.admin()?, &request)
            .await
            .map_err(ToolExecutionError::backend)?;
        let ConsumerProgressResult::Group(progress) = result else {
            return Err(ToolExecutionError::internal(
                "consumer lag query did not return the expected group result",
            ));
        };
        let mut queues = progress
            .rows
            .iter()
            .map(|row| QueueLag {
                topic: row.topic.to_string(),
                broker_name: row.broker_name.to_string(),
                queue_id: row.queue_id,
                broker_offset: row.broker_offset,
                consumer_offset: row.consumer_offset,
                lag: row.diff,
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
            total_lag: progress.diff_total,
            consume_tps: progress.consume_tps,
            inflight_total: progress.inflight_total,
        })
    }

    async fn probe_broker_runtime(&mut self) -> Result<(), ToolExecutionError> {
        let request = BrokerRuntimeStatsQueryRequest::try_new(None, Some(self.cluster.name.clone()))
            .map_err(ToolExecutionError::backend)?;
        BrokerService::query_broker_runtime_stats_with_admin(self.admin()?, &request)
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

fn map_broker_summary(row: &rocketmq_admin_core::core::cluster::ClusterBaseInfoRow) -> BrokerSummary {
    BrokerSummary {
        cluster: row.cluster_name.clone(),
        broker_name: row.broker_name.clone(),
        broker_id: row.broker_id,
        broker_addr: row.broker_addr.to_string(),
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
