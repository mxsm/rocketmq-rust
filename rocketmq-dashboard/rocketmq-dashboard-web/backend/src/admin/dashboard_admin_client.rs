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
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rocketmq_admin_core::client_adapter::AdminBuilder;
use rocketmq_admin_core::client_adapter::AdminGuard;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::client_adapter::ClientRuntime;
use rocketmq_admin_core::core::dashboard as core;
use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use rocketmq_admin_core::core::security::AdminCredentials;
use rocketmq_runtime::ChildServiceContext;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::sync::oneshot;

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
use crate::model::DlqMessageRef;
use crate::model::DlqMessageResendResult;
use crate::model::MessageListView;
use crate::model::MessageResendRequest;
use crate::model::MessageResendResult;
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
    admin_credentials: Option<AdminCredentials>,
    admin_session: Arc<Mutex<Option<Arc<ManagedAdminSession>>>>,
    topic_admin_sessions: Arc<TopicAdminSessionRegistry<AdminGuard>>,
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
pub(super) struct AdminConfigSnapshot {
    pub(super) namesrv_addr: String,
    pub(super) use_vip_channel: bool,
    pub(super) use_tls: bool,
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

// TopicAdmin methods require exclusive mutable access to an AdminSession. The client owns a
// dedicated session registry, while tracked request tasks borrow a single guard only for their
// RPC. This leaves the guard available for ordered cleanup if a task group must abort a request.
macro_rules! run_topic_admin_rpc {
    ($client:expr, |$admin:ident| $operation:expr) => {{
        let snapshot = $client.admin_config_snapshot().await?;
        let client = $client.clone();
        let admin_group = unique_admin_group();
        let (response_tx, response_rx) = oneshot::channel();
        let config_cancellation = client.session_tasks.task_group().cancellation_token();
        let guard_cancellation = client.session_tasks.task_group().cancellation_token();
        let operation_cancellation = client.session_tasks.task_group().cancellation_token();
        let build_client = client.clone();
        let build_snapshot = snapshot.clone();
        client
            .session_tasks
            .spawn_service(format!("topic-admin-rpc-{admin_group}"), async move {
                let result = run_tracked_topic_admin_service(
                    &client.topic_admin_sessions,
                    &client.config,
                    snapshot,
                    config_cancellation.cancelled(),
                    guard_cancellation.cancelled(),
                    operation_cancellation.cancelled(),
                    move || async move { build_client.build_topic_admin_guard(&build_snapshot, admin_group).await },
                    move |guard| {
                        Box::pin(async move {
                            let $admin = guard.inner_mut();
                            Box::pin($operation).await.map_err(DashboardError::from)
                        })
                    },
                )
                .await;
                let _ = response_tx.send(result);
            })
            .map_err(|error| DashboardError::Internal(format!("Could not start topic admin RPC: {error}")))?;
        response_rx
            .await
            .map_err(|_| DashboardError::Internal("Topic admin RPC stopped before returning a result".to_string()))?
    }};
}

mod topic;
mod topic_session;

use self::topic_session::*;

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
    pub fn new(
        config: Arc<RwLock<DashboardConfigView>>,
        client_runtime: Arc<ClientRuntime>,
        admin_credentials: Option<AdminCredentials>,
    ) -> Self {
        let session_tasks = client_runtime.component("dashboard-admin-session");
        Self {
            config,
            client_runtime,
            admin_credentials,
            admin_session: Arc::new(Mutex::new(None)),
            topic_admin_sessions: Arc::new(TopicAdminSessionRegistry::default()),
            next_generation: Arc::new(AtomicU64::new(1)),
            session_tasks,
        }
    }

    pub async fn shutdown(&self) {
        let report = shutdown_topic_admin_services(
            &self.topic_admin_sessions,
            self.session_tasks.task_group(),
            Duration::from_secs(5),
        )
        .await;
        let session = self.admin_session.lock().await.take();
        if let Some(session) = session {
            session.shutdown().await;
        }
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
    ) -> Result<MessageResendResult, DashboardError> {
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
        Ok(map_direct_consume_result(result))
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
        Ok(resend_dlq_batch(request.messages, |message_id, request| async move {
            self.resend_message(&message_id, request).await
        })
        .await)
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

            let builder = AdminBuilder::new(Arc::clone(&self.client_runtime))
                .namesrv_addr(snapshot.namesrv_addr.clone())
                .admin_group(unique_admin_group())
                .timeout_millis(5_000)
                .vip_channel_enabled(snapshot.use_vip_channel)
                .use_tls(snapshot.use_tls);
            let builder = match self.admin_credentials.clone() {
                Some(credentials) => builder.credentials(credentials),
                None => builder,
            };
            let guard = builder.build_with_guard().await?;
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
        admin_config_snapshot(&self.config).await
    }

    async fn build_topic_admin_guard(
        &self,
        snapshot: &AdminConfigSnapshot,
        admin_group: String,
    ) -> Result<AdminGuard, DashboardError> {
        let builder = AdminBuilder::new(Arc::clone(&self.client_runtime))
            .namesrv_addr(snapshot.namesrv_addr.clone())
            .admin_group(admin_group)
            .timeout_millis(5_000)
            .vip_channel_enabled(snapshot.use_vip_channel)
            .use_tls(snapshot.use_tls);
        let builder = match self.admin_credentials.clone() {
            Some(credentials) => builder.credentials(credentials),
            None => builder,
        };
        builder.build_with_guard().await.map_err(DashboardError::from)
    }
}

pub(super) async fn admin_config_snapshot(
    config: &RwLock<DashboardConfigView>,
) -> Result<AdminConfigSnapshot, DashboardError> {
    let config = config.read().await;
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

async fn resend_dlq_batch<F, Fut>(messages: Vec<DlqMessageRef>, mut resend: F) -> Vec<DlqMessageResendResult>
where
    F: FnMut(String, MessageResendRequest) -> Fut,
    Fut: Future<Output = Result<MessageResendResult, DashboardError>>,
{
    let mut results = Vec::with_capacity(messages.len());
    for message in messages {
        let result_id = message.msg_id.clone();
        let outcome = match dlq_resend_request(message) {
            Ok((message_id, request)) => resend(message_id, request).await,
            Err(error) => Err(error),
        };
        match outcome {
            Ok(result) => results.push(DlqMessageResendResult {
                msg_id: result_id,
                success: result.success,
                consume_result: result.consume_result,
                remark: result.remark,
            }),
            Err(error) => results.push(DlqMessageResendResult {
                msg_id: result_id,
                success: false,
                consume_result: "FAILED".to_string(),
                remark: Some(format!("{}: DLQ resend request failed", error.code())),
            }),
        }
    }
    results
}

fn map_direct_consume_result(result: core::AdminMutationResult) -> MessageResendResult {
    const PREFIX: &str = "Direct consume returned ";
    const REMARK_SEPARATOR: &str = ". Remark: ";

    let consume_result = result
        .message
        .strip_prefix(PREFIX)
        .and_then(|details| details.split_whitespace().next())
        .unwrap_or("UNKNOWN")
        .to_string();
    let remark = result
        .message
        .split_once(REMARK_SEPARATOR)
        .map(|(_, remark)| remark.trim().to_string())
        .filter(|remark| !remark.is_empty());
    MessageResendResult {
        message: result.message,
        success: consume_result == "CR_SUCCESS",
        consume_result,
        remark,
    }
}

fn dlq_resend_request(message: DlqMessageRef) -> Result<(String, MessageResendRequest), DashboardError> {
    validate_name(&message.consumer_group, "Consumer group")?;
    validate_name(&message.msg_id, "Message ID")?;
    let topic = message
        .topic_name
        .map(|topic| topic.trim().to_string())
        .filter(|topic| !topic.is_empty())
        .ok_or_else(|| DashboardError::Validation("DLQ resend requires the original topic".to_string()))?;
    Ok((
        message.msg_id,
        MessageResendRequest {
            topic,
            consumer_group: message.consumer_group,
            client_id: message.client_id,
        },
    ))
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
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use rocketmq_admin_core::core::dashboard::AdminMutationResult as CoreMutationResult;
    use rocketmq_admin_core::core::dashboard::DashboardAclUser;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use tokio::sync::Mutex;
    use tokio::sync::Notify;
    use tokio::sync::RwLock;
    use tokio::sync::oneshot;

    use crate::error::DashboardError;
    use crate::model::DashboardConfigView;
    use crate::model::DlqMessageRef;
    use crate::model::MessageResendResult;

    use super::AdminConfigSnapshot;
    use super::AdminSessionOwner;
    use super::ManagedAdminSession;
    use super::TopicAdminSessionGuard;
    use super::TopicAdminSessionRegistry;
    use super::admin_config_snapshot;
    use super::dlq_resend_request;
    use super::map_acl_user;
    use super::map_direct_consume_result;
    use super::resend_dlq_batch;
    use super::run_tracked_topic_admin_service;
    use super::session_is_current;
    use super::shutdown_topic_admin_services;

    struct TestTopicGuard {
        shutdowns: Arc<AtomicUsize>,
    }

    impl TopicAdminSessionGuard for TestTopicGuard {
        fn shutdown_in_place<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.shutdowns.fetch_add(1, Ordering::AcqRel);
            })
        }
    }

    struct BlockingTestTopicGuard {
        shutdown_started: Arc<Notify>,
        allow_shutdown: Arc<Notify>,
        shutdowns: Arc<AtomicUsize>,
    }

    impl TopicAdminSessionGuard for BlockingTestTopicGuard {
        fn shutdown_in_place<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.shutdown_started.notify_one();
                self.allow_shutdown.notified().await;
                self.shutdowns.fetch_add(1, Ordering::AcqRel);
            })
        }
    }

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

    #[test]
    fn dlq_resend_request_requires_canonical_original_topic() {
        let error = dlq_resend_request(DlqMessageRef {
            topic_name: None,
            consumer_group: "order-service".to_string(),
            msg_id: "MSG-001".to_string(),
            client_id: None,
        })
        .expect_err("missing original topic must fail closed");

        assert!(matches!(error, crate::error::DashboardError::Validation(_)));

        let (message_id, request) = dlq_resend_request(DlqMessageRef {
            topic_name: Some("orders".to_string()),
            consumer_group: "order-service".to_string(),
            msg_id: "MSG-001".to_string(),
            client_id: Some("client-a".to_string()),
        })
        .expect("canonical DLQ resend request");
        assert_eq!(message_id, "MSG-001");
        assert_eq!(request.topic, "orders");
        assert_eq!(request.consumer_group, "order-service");
        assert_eq!(request.client_id.as_deref(), Some("client-a"));
    }

    #[test]
    fn direct_consume_result_classifies_non_success_outcomes() {
        let result = map_direct_consume_result(CoreMutationResult {
            message: "Direct consume returned CR_LATER for `MSG-001` on `orders` in consumer group `order-service`. Remark: retry later".to_string(),
            target_count: 1,
        });

        assert!(!result.success);
        assert_eq!(result.consume_result, "CR_LATER");
        assert_eq!(result.remark.as_deref(), Some("retry later"));
    }

    #[tokio::test]
    async fn resend_dlq_batch_continues_after_a_failed_message() {
        let messages = vec![
            DlqMessageRef {
                topic_name: Some("orders".to_string()),
                consumer_group: "order-service".to_string(),
                msg_id: "MSG-001".to_string(),
                client_id: None,
            },
            DlqMessageRef {
                topic_name: Some("orders".to_string()),
                consumer_group: "order-service".to_string(),
                msg_id: "MSG-002".to_string(),
                client_id: None,
            },
        ];

        let results = resend_dlq_batch(messages, |message_id, _| async move {
            if message_id == "MSG-001" {
                Err(crate::error::DashboardError::Internal("broker unavailable".to_string()))
            } else {
                Ok(MessageResendResult {
                    message: "Direct consume returned CR_SUCCESS".to_string(),
                    success: true,
                    consume_result: "CR_SUCCESS".to_string(),
                    remark: None,
                })
            }
        })
        .await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].msg_id, "MSG-001");
        assert!(!results[0].success);
        assert_eq!(results[0].consume_result, "FAILED");
        assert_eq!(
            results[0].remark.as_deref(),
            Some("INTERNAL_ERROR: DLQ resend request failed")
        );
        assert_eq!(results[1].msg_id, "MSG-002");
        assert!(results[1].success);
        assert_eq!(results[1].consume_result, "CR_SUCCESS");
        assert_eq!(results[1].remark, None);
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
    fn topic_owner_reclaims_a_live_guard_after_tracked_rpc_cancellation() {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        let context = owner.root_context().component("topic-owner-test");
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let operation_started = Arc::new(Notify::new());

        owner.block_on(async {
            let config = Arc::new(RwLock::new(DashboardConfigView::default()));
            let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
            let sessions = Arc::new(TopicAdminSessionRegistry::default());
            let config_cancellation = context.task_group().cancellation_token();
            let guard_cancellation = context.task_group().cancellation_token();
            let operation_cancellation = context.task_group().cancellation_token();
            let (result_tx, result_rx) = oneshot::channel();
            let service_sessions = Arc::clone(&sessions);
            let service_config = Arc::clone(&config);
            let service_shutdowns = Arc::clone(&shutdowns);
            let service_started = Arc::clone(&operation_started);

            context
                .spawn_service("topic-rpc", async move {
                    let result = run_tracked_topic_admin_service(
                        &service_sessions,
                        &service_config,
                        snapshot,
                        config_cancellation.cancelled(),
                        guard_cancellation.cancelled(),
                        operation_cancellation.cancelled(),
                        move || {
                            let shutdowns = Arc::clone(&service_shutdowns);
                            async move { Ok(TestTopicGuard { shutdowns }) }
                        },
                        move |_| {
                            let started = Arc::clone(&service_started);
                            Box::pin(async move {
                                started.notify_one();
                                std::future::pending::<Result<(), DashboardError>>().await
                            })
                        },
                    )
                    .await;
                    let _ = result_tx.send(result);
                })
                .expect("tracked topic RPC");

            operation_started.notified().await;
            context.task_group().cancel();
            sessions.shutdown().await;
            let report = context.task_group().shutdown(Duration::from_millis(100)).await;

            assert!(report.is_healthy(), "{report:?}");
            assert!(matches!(result_rx.await, Ok(Err(DashboardError::Config(_)))));
            assert_eq!(shutdowns.load(Ordering::Acquire), 1);
        });

        owner.shutdown_runtime_blocking().expect("runtime owner shutdown");
    }

    #[test]
    fn topic_owner_reclaims_a_live_guard_after_configuration_recheck_cancellation() {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        let context = owner.root_context().component("topic-config-recheck-test");
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let guard_built = Arc::new(Notify::new());

        owner.block_on(async {
            let config = Arc::new(RwLock::new(DashboardConfigView::default()));
            let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
            let config_write = config.write().await;
            let sessions = Arc::new(TopicAdminSessionRegistry::default());
            let config_cancellation = context.task_group().cancellation_token();
            let guard_cancellation = context.task_group().cancellation_token();
            let operation_cancellation = context.task_group().cancellation_token();
            let (result_tx, result_rx) = oneshot::channel();
            let service_sessions = Arc::clone(&sessions);
            let service_config = Arc::clone(&config);
            let service_shutdowns = Arc::clone(&shutdowns);
            let service_guard_built = Arc::clone(&guard_built);

            context
                .spawn_service("topic-config-recheck", async move {
                    let result = run_tracked_topic_admin_service(
                        &service_sessions,
                        &service_config,
                        snapshot,
                        config_cancellation.cancelled(),
                        guard_cancellation.cancelled(),
                        operation_cancellation.cancelled(),
                        move || {
                            let shutdowns = Arc::clone(&service_shutdowns);
                            let guard_built = Arc::clone(&service_guard_built);
                            async move {
                                guard_built.notify_one();
                                Ok(TestTopicGuard { shutdowns })
                            }
                        },
                        |_| Box::pin(async { Ok(()) }),
                    )
                    .await;
                    let _ = result_tx.send(result);
                })
                .expect("tracked topic config recheck");

            guard_built.notified().await;
            loop {
                if sessions.has_current() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            context.task_group().cancel();
            sessions.shutdown().await;
            drop(config_write);
            let report = context.task_group().shutdown(Duration::from_millis(100)).await;

            assert!(report.is_healthy(), "{report:?}");
            assert!(matches!(result_rx.await, Ok(Err(DashboardError::Config(_)))));
            assert_eq!(shutdowns.load(Ordering::Acquire), 1);
        });

        owner.shutdown_runtime_blocking().expect("runtime owner shutdown");
    }

    #[test]
    fn topic_owner_handles_task_group_timeout_while_guard_construction_is_blocked() {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        let context = owner.root_context().component("topic-build-timeout-test");
        let build_started = Arc::new(Notify::new());

        owner.block_on(async {
            let config = Arc::new(RwLock::new(DashboardConfigView::default()));
            let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
            let sessions = Arc::new(TopicAdminSessionRegistry::<TestTopicGuard>::default());
            let config_cancellation = context.task_group().cancellation_token();
            let guard_cancellation = context.task_group().cancellation_token();
            let operation_cancellation = context.task_group().cancellation_token();
            let service_sessions = Arc::clone(&sessions);
            let service_config = Arc::clone(&config);
            let service_build_started = Arc::clone(&build_started);

            context
                .spawn_service("topic-build", async move {
                    let _ = run_tracked_topic_admin_service(
                        &service_sessions,
                        &service_config,
                        snapshot,
                        config_cancellation.cancelled(),
                        guard_cancellation.cancelled(),
                        operation_cancellation.cancelled(),
                        move || async move {
                            service_build_started.notify_one();
                            std::future::pending::<Result<TestTopicGuard, DashboardError>>().await
                        },
                        |_| Box::pin(async { Ok(()) }),
                    )
                    .await;
                })
                .expect("tracked topic build");

            build_started.notified().await;
            let report = tokio::time::timeout(
                Duration::from_millis(100),
                shutdown_topic_admin_services(&sessions, context.task_group(), Duration::from_millis(10)),
            )
            .await
            .expect("production topic shutdown must reach its bounded task-group drain");

            assert_eq!(report.aborted, 1, "{report:?}");
            assert!(!sessions.has_current());
        });

        owner.shutdown_runtime_blocking().expect("runtime owner shutdown");
    }

    #[test]
    fn topic_shutdown_drains_a_guard_completed_after_admission_closes() {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        let context = owner.root_context().component("topic-late-build-test");
        let build_started = Arc::new(Notify::new());
        let allow_build = Arc::new(Notify::new());
        let shutdowns = Arc::new(AtomicUsize::new(0));

        owner.block_on(async {
            let config = Arc::new(RwLock::new(DashboardConfigView::default()));
            let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
            let sessions = Arc::new(TopicAdminSessionRegistry::default());
            let config_cancellation = context.task_group().cancellation_token();
            let guard_cancellation = context.task_group().cancellation_token();
            let operation_cancellation = context.task_group().cancellation_token();
            let service_sessions = Arc::clone(&sessions);
            let service_config = Arc::clone(&config);
            let service_started = Arc::clone(&build_started);
            let service_allowed = Arc::clone(&allow_build);
            let service_shutdowns = Arc::clone(&shutdowns);

            context
                .spawn_service("topic-late-build", async move {
                    let _ = run_tracked_topic_admin_service(
                        &service_sessions,
                        &service_config,
                        snapshot,
                        config_cancellation.cancelled(),
                        guard_cancellation.cancelled(),
                        operation_cancellation.cancelled(),
                        move || async move {
                            service_started.notify_one();
                            service_allowed.notified().await;
                            Ok(TestTopicGuard {
                                shutdowns: service_shutdowns,
                            })
                        },
                        |_| Box::pin(async { Ok(()) }),
                    )
                    .await;
                })
                .expect("tracked late topic build");

            build_started.notified().await;
            let shutdown_sessions = Arc::clone(&sessions);
            let shutdown_group = context.task_group().clone();
            let shutdown = tokio::spawn(async move {
                shutdown_topic_admin_services(&shutdown_sessions, &shutdown_group, Duration::from_millis(100)).await
            });
            sessions.wait_until_closing().await;
            allow_build.notify_one();
            let report = shutdown.await.expect("topic shutdown task");

            assert!(report.is_healthy(), "{report:?}");
            assert_eq!(shutdowns.load(Ordering::Acquire), 1);
            assert!(!sessions.has_current());
        });

        owner.shutdown_runtime_blocking().expect("runtime owner shutdown");
    }

    #[tokio::test]
    async fn topic_registry_reaps_retired_sessions_before_each_configuration_switch() {
        let sessions = TopicAdminSessionRegistry::default();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        for namesrv_addr in ["127.0.0.1:9876", "127.0.0.2:9876", "127.0.0.3:9876"] {
            let shutdowns = Arc::clone(&shutdowns);
            sessions
                .acquire(
                    AdminConfigSnapshot {
                        namesrv_addr: namesrv_addr.to_string(),
                        use_vip_channel: false,
                        use_tls: false,
                    },
                    move || async move { Ok(TestTopicGuard { shutdowns }) },
                )
                .await
                .expect("configured topic session");
        }
        sessions.reap_retired().await;

        assert_eq!(shutdowns.load(Ordering::Acquire), 2);
        assert!(sessions.has_current());
    }

    #[tokio::test]
    async fn topic_operation_lease_keeps_a_retired_guard_alive_until_the_stale_request_releases_it() {
        let config = Arc::new(RwLock::new(DashboardConfigView::default()));
        let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
        let sessions = Arc::new(TopicAdminSessionRegistry::default());
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let first_operation = Arc::new(AtomicUsize::new(0));
        let phase = sessions.pause_next_operation_before_guard();
        let first_sessions = Arc::clone(&sessions);
        let first_config = Arc::clone(&config);
        let first_shutdowns = Arc::clone(&shutdowns);
        let first_operations = Arc::clone(&first_operation);
        let first_snapshot = snapshot.clone();

        let first = tokio::spawn(async move {
            run_tracked_topic_admin_service(
                &first_sessions,
                &first_config,
                first_snapshot,
                std::future::pending(),
                std::future::pending(),
                std::future::pending(),
                move || async move {
                    Ok(TestTopicGuard {
                        shutdowns: first_shutdowns,
                    })
                },
                move |_| {
                    let operations = Arc::clone(&first_operations);
                    Box::pin(async move {
                        operations.fetch_add(1, Ordering::AcqRel);
                        Ok("stale result")
                    })
                },
            )
            .await
        });

        phase.wait_until_paused().await;
        config.write().await.current_namesrv = Some("127.0.0.2:9876".to_string());
        let replacement_snapshot = admin_config_snapshot(&config).await.expect("replacement snapshot");
        let replacement_builds = Arc::new(AtomicUsize::new(0));
        let second_builds = Arc::clone(&replacement_builds);
        let second_shutdowns = Arc::clone(&shutdowns);
        let second_sessions = Arc::clone(&sessions);
        let second_config = Arc::clone(&config);
        let second = tokio::spawn(async move {
            run_tracked_topic_admin_service(
                &second_sessions,
                &second_config,
                replacement_snapshot,
                std::future::pending(),
                std::future::pending(),
                std::future::pending(),
                move || {
                    let shutdowns = Arc::clone(&second_shutdowns);
                    async move {
                        second_builds.fetch_add(1, Ordering::AcqRel);
                        Ok(TestTopicGuard { shutdowns })
                    }
                },
                |_| Box::pin(async move { Ok("replacement result") }),
            )
            .await
        });
        assert!(matches!(second.await, Ok(Ok("replacement result"))));
        assert_eq!(replacement_builds.load(Ordering::Acquire), 1);
        assert_eq!(shutdowns.load(Ordering::Acquire), 0);

        phase.resume();
        assert!(matches!(first.await, Ok(Err(DashboardError::Config(_)))));
        assert_eq!(first_operation.load(Ordering::Acquire), 0);
        assert_eq!(shutdowns.load(Ordering::Acquire), 1);

        sessions.shutdown().await;
        assert_eq!(shutdowns.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn topic_operation_rejects_a_result_staled_during_the_rpc() {
        let config = Arc::new(RwLock::new(DashboardConfigView::default()));
        let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
        let sessions = TopicAdminSessionRegistry::default();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let operation_config = Arc::clone(&config);
        let build_shutdowns = Arc::clone(&shutdowns);

        let result = run_tracked_topic_admin_service(
            &sessions,
            &config,
            snapshot,
            std::future::pending(),
            std::future::pending(),
            std::future::pending(),
            move || async move {
                Ok(TestTopicGuard {
                    shutdowns: build_shutdowns,
                })
            },
            move |_| {
                let config = Arc::clone(&operation_config);
                Box::pin(async move {
                    config.write().await.current_namesrv = Some("127.0.0.2:9876".to_string());
                    Ok("stale topic result")
                })
            },
        )
        .await;

        assert!(matches!(result, Err(DashboardError::Config(_))));
        assert_eq!(shutdowns.load(Ordering::Acquire), 1);
    }

    #[test]
    fn topic_owner_keeps_an_interrupted_shutdown_guard_for_retry() {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
        let context = owner.root_context().component("topic-shutdown-retry-test");
        let shutdown_started = Arc::new(Notify::new());
        let allow_shutdown = Arc::new(Notify::new());
        let shutdowns = Arc::new(AtomicUsize::new(0));

        owner.block_on(async {
            let sessions = Arc::new(TopicAdminSessionRegistry::default());
            let config = Arc::new(RwLock::new(DashboardConfigView::default()));
            let snapshot = admin_config_snapshot(&config).await.expect("configured snapshot");
            let config_cancellation = context.task_group().cancellation_token();
            let guard_cancellation = context.task_group().cancellation_token();
            let operation_cancellation = context.task_group().cancellation_token();
            let service_sessions = Arc::clone(&sessions);
            let service_config = Arc::clone(&config);
            let service_started = Arc::clone(&shutdown_started);
            let service_allowed = Arc::clone(&allow_shutdown);
            let service_shutdowns = Arc::clone(&shutdowns);
            let (result_tx, result_rx) = oneshot::channel();

            context
                .spawn_service("topic-shutdown-retry", async move {
                    let result = run_tracked_topic_admin_service(
                        &service_sessions,
                        &service_config,
                        snapshot,
                        config_cancellation.cancelled(),
                        guard_cancellation.cancelled(),
                        operation_cancellation.cancelled(),
                        move || async move {
                            Ok(BlockingTestTopicGuard {
                                shutdown_started: service_started,
                                allow_shutdown: service_allowed,
                                shutdowns: service_shutdowns,
                            })
                        },
                        |_| Box::pin(async { Ok(()) }),
                    )
                    .await;
                    let _ = result_tx.send(result);
                })
                .expect("tracked topic shutdown retry");

            assert!(matches!(result_rx.await, Ok(Ok(()))));
            context.task_group().cancel();

            let interrupted_sessions = Arc::clone(&sessions);
            let interrupted = tokio::spawn(async move {
                interrupted_sessions.shutdown().await;
            });
            shutdown_started.notified().await;
            interrupted.abort();
            let _ = interrupted.await;

            let report = context.task_group().shutdown(Duration::from_millis(100)).await;
            assert!(report.is_healthy(), "{report:?}");

            allow_shutdown.notify_one();
            sessions.shutdown().await;
            assert_eq!(shutdowns.load(Ordering::Acquire), 1);
        });

        owner.shutdown_runtime_blocking().expect("runtime owner shutdown");
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
