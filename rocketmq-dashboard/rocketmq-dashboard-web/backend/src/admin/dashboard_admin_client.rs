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

use std::cmp::Reverse;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_admin_core::client_adapter::AdminBuilder;
use rocketmq_admin_core::client_adapter::AdminGuard;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::core::dashboard as core;
use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use rocketmq_runtime::ChildServiceContext;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;

use crate::error::DashboardError;
use crate::model::AclMutationResult;
use crate::model::AclPolicyEntryView;
use crate::model::AclPolicyRequest;
use crate::model::AclPolicyView;
use crate::model::AclQuery;
use crate::model::AclUserUpsertRequest;
use crate::model::AclUserView;
use crate::model::BrokerConfigUpdateRequest;
use crate::model::BrokerConfigView;
use crate::model::BrokerInfo;
use crate::model::BrokerListView;
use crate::model::BrokerRuntimeStats;
use crate::model::ConsumerGroupInfo;
use crate::model::ConsumerListView;
use crate::model::ConsumerProgress;
use crate::model::ConsumerQueueProgress;
use crate::model::ConsumerResetOffsetRequest;
use crate::model::DashboardConfigView;
use crate::model::DashboardOverview;
use crate::model::DashboardTopicCurrent;
use crate::model::DlqBatchResendRequest;
use crate::model::DlqExportView;
use crate::model::DlqMessageQuery;
use crate::model::DlqMessageResendResult;
use crate::model::MessageListView;
use crate::model::MessageResendRequest;
use crate::model::MessageTraceNode;
use crate::model::MessageTraceView;
use crate::model::MessageView;
use crate::model::MutationResult;
use crate::model::ProducerConnectionInfo;
use crate::model::ProducerConnectionView;
use crate::model::ProducerInfo;
use crate::model::TopicCurrentMetric;
use crate::model::TopicInfo;
use crate::model::TopicListView;
use crate::model::TopicMutationRequest;
use crate::model::TopicRouteBroker;
use crate::model::TopicRouteInfo;
use crate::model::TopicRouteQueue;
use crate::model::TopicStatsInfo;

mod mapping;

use self::mapping::*;

#[derive(Clone)]
pub struct DashboardAdminClient {
    config: Arc<RwLock<DashboardConfigView>>,
    client_runtime: Arc<ClientRuntime>,
    admin_session: Arc<Mutex<Option<Arc<ManagedAdminSession>>>>,
    next_generation: Arc<AtomicU64>,
    session_tasks: ChildServiceContext,
}

struct ManagedAdminSession {
    owner: Mutex<Option<Arc<AdminSessionOwner>>>,
    snapshot: AdminConfigSnapshot,
    generation: u64,
}

struct AdminSessionOwner {
    guard: Option<AdminGuard>,
    active_leases: AtomicUsize,
    leases_drained: Notify,
}

struct AdminSessionLease {
    owner: Arc<AdminSessionOwner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdminConfigSnapshot {
    namesrv_addr: String,
    use_vip_channel: bool,
    use_tls: bool,
}

macro_rules! run_admin_rpc {
    ($client:expr, |$admin:ident| $operation:expr) => {{
        let managed = $client.acquire_admin_session().await?;
        let lease = managed.lease().await?;
        let result = {
            let $admin = lease.session()?;
            Box::pin($operation).await
        };
        drop(lease);
        $client.ensure_current_generation(&managed).await?;
        result
    }};
}

impl ManagedAdminSession {
    async fn lease(&self) -> Result<AdminSessionLease, DashboardError> {
        let owner = self.owner.lock().await;
        let owner = owner
            .as_ref()
            .ok_or_else(|| DashboardError::Internal("Admin session was already retired".to_string()))?;
        owner.active_leases.fetch_add(1, Ordering::AcqRel);
        Ok(AdminSessionLease {
            owner: Arc::clone(owner),
        })
    }

    async fn shutdown(&self) {
        let Some(owner) = self.owner.lock().await.take() else {
            return;
        };
        loop {
            if owner.active_leases.load(Ordering::Acquire) == 0 {
                break;
            }
            owner.leases_drained.notified().await;
        }
        match Arc::try_unwrap(owner) {
            Ok(mut owner) => {
                if let Some(guard) = owner.guard.take() {
                    guard.shutdown().await;
                }
            }
            Err(_) => {
                tracing::error!("Dashboard admin session retirement retained an owner without an active lease");
            }
        }
    }
}

impl AdminSessionLease {
    fn session(&self) -> Result<&AdminSession, DashboardError> {
        self.owner
            .guard
            .as_ref()
            .map(AdminGuard::inner)
            .ok_or_else(|| DashboardError::Internal("Admin session was not initialized".to_string()))
    }
}

impl Drop for AdminSessionLease {
    fn drop(&mut self) {
        if self.owner.active_leases.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.owner.leases_drained.notify_one();
        }
    }
}

impl DashboardAdminClient {
    pub fn new(config: Arc<RwLock<DashboardConfigView>>, client_runtime: Arc<ClientRuntime>) -> Self {
        let session_tasks = client_runtime.component("dashboard-admin-session");
        Self {
            config,
            client_runtime,
            admin_session: Arc::new(Mutex::new(None)),
            next_generation: Arc::new(AtomicU64::new(1)),
            session_tasks,
        }
    }

    pub async fn shutdown(&self) {
        let session = self.admin_session.lock().await.take();
        if let Some(session) = session {
            session.shutdown().await;
        }
        let report = self.session_tasks.task_group().shutdown(Duration::from_secs(5)).await;
        if !report.is_healthy() {
            tracing::warn!(
                report = %report.to_json(),
                "Dashboard admin session retirement did not shut down cleanly"
            );
        }
    }

    pub async fn dashboard_overview(&self) -> Result<DashboardOverview, DashboardError> {
        let config = self.config.read().await.clone();
        if config.current_namesrv.is_none() {
            return Ok(DashboardOverview {
                current_namesrv: None,
                broker_count: 0,
                topic_count: 0,
                consumer_group_count: 0,
                producer_count: 0,
                message_backlog: 0,
                system_status: "UNCONFIGURED".to_string(),
            });
        }

        let counts = run_admin_rpc!(self, |admin| async {
            let brokers = admin.dashboard_list_brokers().await?;
            let topics = admin.dashboard_list_topics().await?;
            let consumers = admin.dashboard_list_consumers().await.unwrap_or_default();
            let producers = admin.dashboard_list_producers().await.unwrap_or_default();
            let backlog = consumers.items.iter().map(|item| item.diff_total).sum();
            Ok::<_, DashboardError>((
                brokers.items.len(),
                topics.items.len(),
                consumers.items.len(),
                producers.len(),
                backlog,
            ))
        });

        let (broker_count, topic_count, consumer_group_count, producer_count, message_backlog, system_status) =
            match counts {
                Ok((brokers, topics, consumers, producers, backlog)) => {
                    (brokers, topics, consumers, producers, backlog, "UP")
                }
                Err(DashboardError::Admin(error)) => {
                    tracing::warn!(error = %error, "RocketMQ cluster is not reachable while building dashboard overview");
                    (0, 0, 0, 0, 0, "DOWN")
                }
                Err(error) => return Err(error),
            };

        Ok(DashboardOverview {
            current_namesrv: config.current_namesrv,
            broker_count,
            topic_count,
            consumer_group_count,
            producer_count,
            message_backlog,
            system_status: system_status.to_string(),
        })
    }

    pub async fn topic_current(&self) -> Result<DashboardTopicCurrent, DashboardError> {
        let topics = match self.list_topics().await {
            Ok(topics) => topics,
            Err(DashboardError::Admin(error)) => {
                tracing::warn!(error = %error, "RocketMQ cluster is not reachable while building topic-current metrics");
                return Ok(DashboardTopicCurrent {
                    total_topics: 0,
                    top_topics: Vec::new(),
                });
            }
            Err(error) => return Err(error),
        };

        let mut top_topics = run_admin_rpc!(self, |admin| async {
            let mut top_topics = Vec::new();
            for topic in topics.items.iter().take(20) {
                if let Ok(stats) = admin.dashboard_topic_stats(&topic.topic).await {
                    top_topics.push(TopicCurrentMetric {
                        topic: topic.topic.clone(),
                        total_msg: stats.total_max_offset.saturating_sub(stats.total_min_offset),
                        in_tps: 0.0,
                        out_tps: 0.0,
                    });
                }
            }
            Ok::<_, DashboardError>(top_topics)
        })?;
        top_topics.sort_by_key(|topic| Reverse(topic.total_msg));
        Ok(DashboardTopicCurrent {
            total_topics: topics.total,
            top_topics,
        })
    }

    pub async fn list_topics(&self) -> Result<TopicListView, DashboardError> {
        let list = run_admin_rpc!(self, |admin| admin.dashboard_list_topics())?;
        let items = list.items.into_iter().map(map_topic_info).collect::<Vec<_>>();
        Ok(TopicListView {
            total: items.len(),
            items,
        })
    }

    pub async fn get_topic(&self, topic: &str) -> Result<TopicInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let route = self.topic_route(topic).await?;
        Ok(topic_info_from_route(topic, &route))
    }

    pub async fn topic_route(&self, topic: &str) -> Result<TopicRouteInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let route = run_admin_rpc!(self, |admin| admin.dashboard_topic_route(topic))?;
        Ok(map_topic_route(route))
    }

    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let stats = run_admin_rpc!(self, |admin| admin.dashboard_topic_stats(topic))?;
        Ok(TopicStatsInfo {
            topic: stats.topic,
            queue_count: stats.queue_count,
            total_min_offset: stats.total_min_offset,
            total_max_offset: stats.total_max_offset,
        })
    }

    pub async fn create_or_update_topic(
        &self,
        request: TopicMutationRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(&request.topic, "Topic")?;
        if request.cluster_name_list.is_empty() && request.broker_name_list.is_empty() {
            return Err(DashboardError::Validation(
                "Select at least one cluster or broker before saving the topic".to_string(),
            ));
        }
        let request = core::DashboardTopicMutationRequest {
            topic: request.topic,
            read_queue_count: request.read_queue_count,
            write_queue_count: request.write_queue_count,
            perm: request.perm,
            broker_name_list: request.broker_name_list,
            cluster_name_list: request.cluster_name_list,
            order: request.order.unwrap_or(false),
            message_type: request.message_type,
        };
        let result = run_admin_rpc!(self, |admin| admin.dashboard_upsert_topic(&request))?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<MutationResult, DashboardError> {
        validate_name(topic, "Topic")?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_delete_topic(topic))?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn list_consumer_groups(&self) -> Result<ConsumerListView, DashboardError> {
        let list = run_admin_rpc!(self, |admin| admin.dashboard_list_consumers())?;
        let items = list
            .items
            .into_iter()
            .map(|item| ConsumerGroupInfo {
                group: item.group,
                consume_type: item.consume_type,
                message_model: item.message_model,
                client_count: item.client_count,
                diff_total: item.diff_total,
            })
            .collect::<Vec<_>>();
        Ok(ConsumerListView {
            total: items.len(),
            items,
        })
    }

    pub async fn consumer_progress(&self, group: &str) -> Result<ConsumerProgress, DashboardError> {
        validate_name(group, "Consumer group")?;
        let progress = run_admin_rpc!(self, |admin| admin.dashboard_consumer_progress(group))?;
        Ok(map_consumer_progress(progress))
    }

    pub async fn reset_consumer_offset(
        &self,
        group: &str,
        request: ConsumerResetOffsetRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(group, "Consumer group")?;
        validate_name(&request.topic, "Topic")?;
        if request.reset_timestamp < 0 {
            return Err(DashboardError::Validation(
                "Reset timestamp must be a non-negative millisecond timestamp".to_string(),
            ));
        }
        let request = core::DashboardConsumerResetRequest {
            group: group.to_string(),
            topic: request.topic,
            reset_timestamp: request.reset_timestamp as u64,
            force: request.force,
        };
        let result = run_admin_rpc!(self, |admin| admin.dashboard_reset_consumer(&request))?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn list_producers(&self) -> Result<Vec<ProducerInfo>, DashboardError> {
        let items = run_admin_rpc!(self, |admin| admin.dashboard_list_producers())?
            .into_iter()
            .map(|item| ProducerInfo {
                topic: item.topic,
                producer_group: item.producer_group,
                connection_count: item.connection_count,
            })
            .collect();
        Ok(items)
    }

    pub async fn producer_connections(
        &self,
        topic: &str,
        producer_group: &str,
    ) -> Result<ProducerConnectionView, DashboardError> {
        validate_name(topic, "Topic")?;
        validate_name(producer_group, "Producer group")?;
        let connections = run_admin_rpc!(self, |admin| admin
            .dashboard_producer_connections(topic, producer_group))?;
        Ok(ProducerConnectionView {
            topic: connections.topic,
            producer_group: connections.producer_group,
            connections: connections
                .connections
                .into_iter()
                .map(|item| ProducerConnectionInfo {
                    client_id: item.client_id,
                    client_addr: item.client_addr,
                    language: item.language,
                    version: item.version.to_string(),
                })
                .collect(),
        })
    }

    pub async fn list_brokers(&self) -> Result<BrokerListView, DashboardError> {
        let list = run_admin_rpc!(self, |admin| admin.dashboard_list_brokers())?;
        let items = list.items.into_iter().map(map_broker_info).collect::<Vec<_>>();
        Ok(BrokerListView {
            total: items.len(),
            items,
        })
    }

    pub async fn broker_runtime_stats(&self, broker_name: &str) -> Result<BrokerRuntimeStats, DashboardError> {
        validate_name(broker_name, "Broker")?;
        let target = broker_target(broker_name);
        let runtime = run_admin_rpc!(self, |admin| admin.dashboard_broker_runtime(&target))?;
        Ok(BrokerRuntimeStats {
            broker_name: runtime.broker_name,
            address: runtime.address,
            entries: runtime.entries,
        })
    }

    pub async fn broker_config(&self, broker_name: &str) -> Result<BrokerConfigView, DashboardError> {
        validate_name(broker_name, "Broker")?;
        let target = broker_target(broker_name);
        let config = run_admin_rpc!(self, |admin| admin.dashboard_broker_config(&target))?;
        Ok(BrokerConfigView {
            broker_name: config.broker_name,
            address: config.address,
            entries: config.entries,
        })
    }

    pub async fn update_broker_config(
        &self,
        broker_name: &str,
        request: BrokerConfigUpdateRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(broker_name, "Broker")?;
        if request.entries.is_empty() {
            return Err(DashboardError::Validation(
                "Broker config update entries cannot be empty".to_string(),
            ));
        }
        let target = broker_target(broker_name);
        let request = core::DashboardBrokerConfigUpdateRequest {
            broker_name: target.broker_name,
            broker_addr: target.broker_addr,
            entries: request.entries,
        };
        let result = run_admin_rpc!(self, |admin| admin.dashboard_update_broker_config(&request))?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn list_acl_users(&self, query: AclQuery) -> Result<Vec<AclUserView>, DashboardError> {
        let query = map_acl_user_query(query);
        let users = run_admin_rpc!(self, |admin| admin.dashboard_list_acl_users(&query))?;
        Ok(users.into_iter().map(map_acl_user).collect())
    }

    pub async fn create_acl_user(&self, request: AclUserUpsertRequest) -> Result<AclMutationResult, DashboardError> {
        let username = required_request_field(request.username.as_deref(), "username")?.to_string();
        let request = map_acl_user_request(username, request, false)?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_create_acl_user(&request))?;
        Ok(map_acl_mutation(result))
    }

    pub async fn update_acl_user(
        &self,
        username: &str,
        request: AclUserUpsertRequest,
    ) -> Result<AclMutationResult, DashboardError> {
        validate_name(username, "Username")?;
        let request = map_acl_user_request(username.to_string(), request, true)?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_update_acl_user(&request))?;
        Ok(map_acl_mutation(result))
    }

    pub async fn delete_acl_user(&self, username: &str, query: AclQuery) -> Result<AclMutationResult, DashboardError> {
        validate_name(username, "Username")?;
        let selector = map_selector(query.cluster_name, query.broker_name);
        let result = run_admin_rpc!(self, |admin| admin.dashboard_delete_acl_user(&selector, username))?;
        Ok(map_acl_mutation(result))
    }

    pub async fn list_acl_policies(&self, query: AclQuery) -> Result<Vec<AclPolicyView>, DashboardError> {
        let query = map_acl_query(query);
        let policies = run_admin_rpc!(self, |admin| admin.dashboard_list_acl_policies(&query))?;
        Ok(policies.into_iter().map(map_acl_policy).collect())
    }

    pub async fn create_acl_policy(&self, request: AclPolicyRequest) -> Result<AclMutationResult, DashboardError> {
        let request = map_acl_policy_request(request)?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_create_acl_policy(&request))?;
        Ok(map_acl_mutation(result))
    }

    pub async fn update_acl_policy(
        &self,
        subject: &str,
        mut request: AclPolicyRequest,
    ) -> Result<AclMutationResult, DashboardError> {
        validate_name(subject, "ACL subject")?;
        request.subject = subject.to_string();
        let request = map_acl_policy_request(request)?;
        let result = run_admin_rpc!(self, |admin| admin.dashboard_update_acl_policy(&request))?;
        Ok(map_acl_mutation(result))
    }

    pub async fn delete_acl_policy(&self, subject: &str, query: AclQuery) -> Result<AclMutationResult, DashboardError> {
        validate_name(subject, "ACL subject")?;
        let resource = query.resource.unwrap_or_default();
        let selector = map_selector(query.cluster_name, query.broker_name);
        let result = run_admin_rpc!(self, |admin| {
            admin.dashboard_delete_acl_policy(&selector, subject, &resource)
        })?;
        Ok(map_acl_mutation(result))
    }

    #[allow(clippy::too_many_arguments, reason = "preserves the existing HTTP query facade")]
    pub async fn query_messages(
        &self,
        topic: Option<&str>,
        key: Option<&str>,
        message_id: Option<&str>,
        begin: Option<i64>,
        end: Option<i64>,
        page_num: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<MessageListView, DashboardError> {
        let topic = topic.ok_or_else(|| {
            DashboardError::Validation(match message_id {
                Some(_) => "Message ID query requires topic. Use /api/messages?topic=...&messageId=...".to_string(),
                None => "Message query requires topic with either key or messageId".to_string(),
            })
        })?;
        validate_name(topic, "Topic")?;
        if let Some(key) = key {
            validate_name(key, "Message key")?;
        }
        if let Some(message_id) = message_id {
            validate_name(message_id, "Message ID")?;
        }
        if key.is_none() && message_id.is_none() {
            validate_message_window(begin, end)?;
        }
        let query = core::DashboardMessageQuery {
            topic: Some(topic.to_string()),
            key: key.map(ToString::to_string),
            message_id: message_id.map(ToString::to_string),
            begin,
            end,
            page_num,
            page_size,
        };
        self.run_message_query(&query).await
    }

    pub async fn query_message_by_key(&self, topic: &str, key: &str) -> Result<MessageListView, DashboardError> {
        self.query_messages(Some(topic), Some(key), None, None, None, None, None)
            .await
    }

    pub async fn query_message_by_id(&self, message_id: &str) -> Result<MessageListView, DashboardError> {
        validate_name(message_id, "Message ID")?;
        Err(DashboardError::Validation(
            "Message ID query requires topic. Use /api/messages?topic=...&messageId=...".to_string(),
        ))
    }

    pub async fn message_trace(
        &self,
        message_id: &str,
        topic: Option<&str>,
        trace_topic: &str,
    ) -> Result<MessageTraceView, DashboardError> {
        validate_name(message_id, "Message ID")?;
        validate_name(trace_topic, "Trace topic")?;
        let topic = topic.ok_or_else(|| {
            DashboardError::Validation(
                "Message trace requires topic. Use /api/messages/:id/trace?topic=...".to_string(),
            )
        })?;
        validate_name(topic, "Topic")?;
        let trace = run_admin_rpc!(self, |admin| {
            admin.dashboard_message_trace(topic, message_id, trace_topic)
        })?;
        Ok(MessageTraceView {
            message_id: trace.message_id,
            trace_topic: trace.trace_topic,
            nodes: trace.nodes.into_iter().map(map_trace_node).collect(),
        })
    }

    pub async fn resend_message(
        &self,
        message_id: &str,
        request: MessageResendRequest,
    ) -> Result<MutationResult, DashboardError> {
        validate_name(message_id, "Message ID")?;
        validate_name(&request.topic, "Topic")?;
        validate_name(&request.consumer_group, "Consumer group")?;
        let request = core::DashboardDirectConsumeRequest {
            message_id: message_id.to_string(),
            topic: request.topic,
            consumer_group: request.consumer_group,
            client_id: request.client_id,
        };
        let result = run_admin_rpc!(self, |admin| admin.dashboard_consume_message_directly(&request))?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn query_dlq_messages(&self, query: DlqMessageQuery) -> Result<MessageListView, DashboardError> {
        validate_name(&query.consumer_group, "Consumer group")?;
        let query = core::DashboardDlqMessageQuery {
            consumer_group: query.consumer_group,
            key: query.key,
            message_id: query.message_id,
            begin: query.begin,
            end: query.end,
            page_num: query.page_num,
            page_size: query.page_size,
        };
        let list = run_admin_rpc!(self, |admin| admin.dashboard_query_dlq_messages(&query))?;
        Ok(map_message_list(list))
    }

    pub async fn resend_dlq_messages(
        &self,
        request: DlqBatchResendRequest,
    ) -> Result<Vec<DlqMessageResendResult>, DashboardError> {
        if request.messages.is_empty() {
            return Err(DashboardError::Validation(
                "DLQ resend messages cannot be empty".to_string(),
            ));
        }
        let mut results = Vec::with_capacity(request.messages.len());
        for message in request.messages {
            validate_name(&message.consumer_group, "Consumer group")?;
            validate_name(&message.msg_id, "Message ID")?;
            let topic = message
                .topic_name
                .filter(|topic| !topic.trim().is_empty())
                .unwrap_or_else(|| format!("%DLQ%{}", message.consumer_group));
            let result = self
                .resend_message(
                    &message.msg_id,
                    MessageResendRequest {
                        topic,
                        consumer_group: message.consumer_group,
                        client_id: message.client_id,
                    },
                )
                .await?;
            results.push(DlqMessageResendResult {
                msg_id: message.msg_id,
                consume_result: "REQUESTED".to_string(),
                remark: Some(result.message),
            });
        }
        Ok(results)
    }

    pub async fn export_dlq_messages(&self, query: DlqMessageQuery) -> Result<DlqExportView, DashboardError> {
        let consumer_group = query.consumer_group.clone();
        let messages = self.query_dlq_messages(query).await?;
        let csv = build_dlq_csv(&messages.items);
        Ok(DlqExportView {
            file_name: format!("dlq-{consumer_group}.csv"),
            rows: messages.items,
            csv,
        })
    }

    async fn run_message_query(&self, query: &core::DashboardMessageQuery) -> Result<MessageListView, DashboardError> {
        let list = run_admin_rpc!(self, |admin| admin.dashboard_query_messages(query))?;
        Ok(map_message_list(list))
    }

    async fn acquire_admin_session(&self) -> Result<Arc<ManagedAdminSession>, DashboardError> {
        loop {
            let snapshot = self.admin_config_snapshot().await?;
            if let Some(session) = self
                .admin_session
                .lock()
                .await
                .as_ref()
                .filter(|session| session.snapshot == snapshot)
                .cloned()
            {
                return Ok(session);
            }

            let guard = AdminBuilder::new(Arc::clone(&self.client_runtime))
                .namesrv_addr(snapshot.namesrv_addr.clone())
                .admin_group(unique_admin_group())
                .timeout_millis(5_000)
                .vip_channel_enabled(snapshot.use_vip_channel)
                .use_tls(snapshot.use_tls)
                .build_with_guard()
                .await?;
            if self.admin_config_snapshot().await? != snapshot {
                guard.shutdown().await;
                continue;
            }

            let candidate = Arc::new(ManagedAdminSession {
                owner: Mutex::new(Some(Arc::new(AdminSessionOwner {
                    guard: Some(guard),
                    active_leases: AtomicUsize::new(0),
                    leases_drained: Notify::new(),
                }))),
                snapshot: snapshot.clone(),
                generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
            });
            let (selected, retired, unused_candidate) = {
                let mut slot = self.admin_session.lock().await;
                if let Some(current) = slot.as_ref().filter(|session| session.snapshot == snapshot).cloned() {
                    (current, None, Some(candidate))
                } else {
                    let retired = slot.replace(Arc::clone(&candidate));
                    (candidate, retired, None)
                }
            };
            if let Some(unused_candidate) = unused_candidate {
                unused_candidate.shutdown().await;
            }
            if let Some(retired) = retired {
                let retirement = Arc::clone(&retired);
                if let Err(error) =
                    self.session_tasks
                        .spawn_service(format!("retire-generation-{}", retired.generation), async move {
                            retirement.shutdown().await;
                        })
                {
                    tracing::warn!(
                        generation = retired.generation,
                        %error,
                        "Could not schedule Dashboard admin session retirement"
                    );
                    retired.shutdown().await;
                }
            }
            tracing::info!(
                namesrv = %selected.snapshot.namesrv_addr,
                use_vip_channel = selected.snapshot.use_vip_channel,
                use_tls = selected.snapshot.use_tls,
                generation = selected.generation,
                "connected RocketMQ dashboard admin session"
            );
            return Ok(selected);
        }
    }

    async fn ensure_current_generation(&self, session: &Arc<ManagedAdminSession>) -> Result<(), DashboardError> {
        if self.admin_config_snapshot().await? != session.snapshot {
            return Err(stale_session_error(session.generation));
        }
        let current = self.admin_session.lock().await;
        if session_is_current(current.as_ref(), session) {
            Ok(())
        } else {
            Err(stale_session_error(session.generation))
        }
    }

    async fn admin_config_snapshot(&self) -> Result<AdminConfigSnapshot, DashboardError> {
        let config = self.config.read().await;
        let namesrv_addr = config
            .current_namesrv
            .clone()
            .ok_or_else(|| DashboardError::Config("No active NameServer is configured".to_string()))?;
        Ok(AdminConfigSnapshot {
            namesrv_addr,
            use_vip_channel: config.use_vip_channel,
            use_tls: config.use_tls,
        })
    }
}

fn session_is_current(current: Option<&Arc<ManagedAdminSession>>, expected: &Arc<ManagedAdminSession>) -> bool {
    current.is_some_and(|candidate| Arc::ptr_eq(candidate, expected))
}

fn stale_session_error(generation: u64) -> DashboardError {
    DashboardError::Config(format!(
        "Admin session generation {generation} was replaced while the request was in flight; retry the request"
    ))
}

fn map_acl_user(user: core::DashboardAclUser) -> AclUserView {
    AclUserView {
        broker_name: user.broker_name,
        broker_addr: user.broker_addr,
        username: user.username,
        password: None,
        user_type: user.user_type,
        user_status: user.user_status,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_admin_core::core::dashboard::DashboardAclUser;
    use tokio::sync::Mutex;
    use tokio::sync::Notify;

    use super::AdminConfigSnapshot;
    use super::AdminSessionOwner;
    use super::ManagedAdminSession;
    use super::map_acl_user;
    use super::session_is_current;

    #[test]
    fn map_acl_user_does_not_expose_password() {
        let mapped = map_acl_user(DashboardAclUser {
            broker_name: "broker-a".to_string(),
            broker_addr: "127.0.0.1:10911".to_string(),
            username: "alice".to_string(),
            user_type: Some("Normal".to_string()),
            user_status: Some("enable".to_string()),
        });

        assert_eq!(mapped.username, "alice");
        assert_eq!(mapped.password, None);
    }

    fn managed_session(generation: u64) -> Arc<ManagedAdminSession> {
        Arc::new(ManagedAdminSession {
            owner: Mutex::new(Some(Arc::new(AdminSessionOwner {
                guard: None,
                active_leases: std::sync::atomic::AtomicUsize::new(0),
                leases_drained: Notify::new(),
            }))),
            snapshot: AdminConfigSnapshot {
                namesrv_addr: "127.0.0.1:9876".to_string(),
                use_vip_channel: false,
                use_tls: false,
            },
            generation,
        })
    }

    #[tokio::test]
    async fn concurrent_rpc_leases_do_not_hold_a_session_lock() {
        let session = managed_session(1);
        let first = session.lease().await.expect("first RPC lease");

        let second = tokio::time::timeout(Duration::from_secs(1), session.lease())
            .await
            .expect("a second RPC lease must not wait for the first")
            .expect("second RPC lease");

        drop(second);
        drop(first);
    }

    #[test]
    fn generation_fence_rejects_a_replaced_session() {
        let old = managed_session(1);
        let replacement = managed_session(2);

        assert!(session_is_current(Some(&old), &old));
        assert!(!session_is_current(Some(&replacement), &old));
        assert!(!session_is_current(None, &old));
    }

    #[test]
    fn rpc_contract_keeps_remote_awaits_outside_the_lifecycle_slot() {
        let source = include_str!("dashboard_admin_client.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production Dashboard admin client source");
        let macro_start = source.find("macro_rules! run_admin_rpc").expect("RPC macro");
        let macro_end = source[macro_start..]
            .find("impl ManagedAdminSession")
            .map(|offset| macro_start + offset)
            .expect("RPC macro boundary");
        let rpc_macro = &source[macro_start..macro_end];

        assert!(rpc_macro.contains("managed.lease().await"));
        assert!(rpc_macro.contains("ensure_current_generation(&managed).await"));
        assert!(!rpc_macro.contains("admin_session.lock().await"));
        assert!(!rpc_macro.contains("read().await"));
        assert!(!rpc_macro.contains("write().await"));
        assert!(!source.contains(concat!("ensure_admin_session", "(&mut slot)")));
        assert!(!source.contains(concat!("active_session", "(&mut slot)")));
    }
}
