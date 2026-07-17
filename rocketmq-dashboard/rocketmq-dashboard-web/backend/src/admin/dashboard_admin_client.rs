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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_admin_core::client_adapter::AdminGuard;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::client_adapter::ClientAdminBuilder;
use rocketmq_admin_core::core::dashboard as core;
use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use tokio::sync::Mutex;
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
    admin_session: Arc<Mutex<Option<ManagedAdminSession>>>,
}

struct ManagedAdminSession {
    guard: AdminGuard,
    snapshot: AdminConfigSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdminConfigSnapshot {
    namesrv_addr: String,
    use_vip_channel: bool,
    use_tls: bool,
}

impl DashboardAdminClient {
    pub fn new(config: Arc<RwLock<DashboardConfigView>>) -> Self {
        Self {
            config,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn shutdown(&self) {
        let mut slot = self.admin_session.lock().await;
        if let Some(session) = slot.take() {
            session.guard.shutdown().await;
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

        let counts = async {
            let mut slot = self.admin_session.lock().await;
            self.ensure_admin_session(&mut slot).await?;
            let admin = active_session(&mut slot)?;
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
        }
        .await;

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

        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let admin = active_session(&mut slot)?;
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
        top_topics.sort_by_key(|topic| Reverse(topic.total_msg));
        Ok(DashboardTopicCurrent {
            total_topics: topics.total,
            top_topics,
        })
    }

    pub async fn list_topics(&self) -> Result<TopicListView, DashboardError> {
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let list = active_session(&mut slot)?.dashboard_list_topics().await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let route = active_session(&mut slot)?.dashboard_topic_route(topic).await?;
        Ok(map_topic_route(route))
    }

    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let stats = active_session(&mut slot)?.dashboard_topic_stats(topic).await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_upsert_topic(&request).await?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<MutationResult, DashboardError> {
        validate_name(topic, "Topic")?;
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_delete_topic(topic).await?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn list_consumer_groups(&self) -> Result<ConsumerListView, DashboardError> {
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let list = active_session(&mut slot)?.dashboard_list_consumers().await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let progress = active_session(&mut slot)?.dashboard_consumer_progress(group).await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_reset_consumer(&request).await?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn list_producers(&self) -> Result<Vec<ProducerInfo>, DashboardError> {
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let items = active_session(&mut slot)?
            .dashboard_list_producers()
            .await?
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let connections = active_session(&mut slot)?
            .dashboard_producer_connections(topic, producer_group)
            .await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let list = active_session(&mut slot)?.dashboard_list_brokers().await?;
        let items = list.items.into_iter().map(map_broker_info).collect::<Vec<_>>();
        Ok(BrokerListView {
            total: items.len(),
            items,
        })
    }

    pub async fn broker_runtime_stats(&self, broker_name: &str) -> Result<BrokerRuntimeStats, DashboardError> {
        validate_name(broker_name, "Broker")?;
        let target = broker_target(broker_name);
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let runtime = active_session(&mut slot)?.dashboard_broker_runtime(&target).await?;
        Ok(BrokerRuntimeStats {
            broker_name: runtime.broker_name,
            address: runtime.address,
            entries: runtime.entries,
        })
    }

    pub async fn broker_config(&self, broker_name: &str) -> Result<BrokerConfigView, DashboardError> {
        validate_name(broker_name, "Broker")?;
        let target = broker_target(broker_name);
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let config = active_session(&mut slot)?.dashboard_broker_config(&target).await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?
            .dashboard_update_broker_config(&request)
            .await?;
        Ok(MutationResult {
            message: result.message,
        })
    }

    pub async fn list_acl_users(&self, query: AclQuery) -> Result<Vec<AclUserView>, DashboardError> {
        let query = map_acl_user_query(query);
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let users = active_session(&mut slot)?.dashboard_list_acl_users(&query).await?;
        Ok(users.into_iter().map(map_acl_user).collect())
    }

    pub async fn create_acl_user(&self, request: AclUserUpsertRequest) -> Result<AclMutationResult, DashboardError> {
        let username = required_request_field(request.username.as_deref(), "username")?.to_string();
        let request = map_acl_user_request(username, request, false)?;
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_create_acl_user(&request).await?;
        Ok(map_acl_mutation(result))
    }

    pub async fn update_acl_user(
        &self,
        username: &str,
        request: AclUserUpsertRequest,
    ) -> Result<AclMutationResult, DashboardError> {
        validate_name(username, "Username")?;
        let request = map_acl_user_request(username.to_string(), request, true)?;
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_update_acl_user(&request).await?;
        Ok(map_acl_mutation(result))
    }

    pub async fn delete_acl_user(&self, username: &str, query: AclQuery) -> Result<AclMutationResult, DashboardError> {
        validate_name(username, "Username")?;
        let selector = map_selector(query.cluster_name, query.broker_name);
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?
            .dashboard_delete_acl_user(&selector, username)
            .await?;
        Ok(map_acl_mutation(result))
    }

    pub async fn list_acl_policies(&self, query: AclQuery) -> Result<Vec<AclPolicyView>, DashboardError> {
        let query = map_acl_query(query);
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let policies = active_session(&mut slot)?.dashboard_list_acl_policies(&query).await?;
        Ok(policies.into_iter().map(map_acl_policy).collect())
    }

    pub async fn create_acl_policy(&self, request: AclPolicyRequest) -> Result<AclMutationResult, DashboardError> {
        let request = map_acl_policy_request(request)?;
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_create_acl_policy(&request).await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?.dashboard_update_acl_policy(&request).await?;
        Ok(map_acl_mutation(result))
    }

    pub async fn delete_acl_policy(&self, subject: &str, query: AclQuery) -> Result<AclMutationResult, DashboardError> {
        validate_name(subject, "ACL subject")?;
        let resource = query.resource.unwrap_or_default();
        let selector = map_selector(query.cluster_name, query.broker_name);
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?
            .dashboard_delete_acl_policy(&selector, subject, &resource)
            .await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let trace = active_session(&mut slot)?
            .dashboard_message_trace(topic, message_id, trace_topic)
            .await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let result = active_session(&mut slot)?
            .dashboard_consume_message_directly(&request)
            .await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let list = active_session(&mut slot)?.dashboard_query_dlq_messages(&query).await?;
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
        let mut slot = self.admin_session.lock().await;
        self.ensure_admin_session(&mut slot).await?;
        let list = active_session(&mut slot)?.dashboard_query_messages(query).await?;
        Ok(map_message_list(list))
    }

    async fn ensure_admin_session(&self, slot: &mut Option<ManagedAdminSession>) -> Result<(), DashboardError> {
        let snapshot = self.admin_config_snapshot().await?;
        if slot.as_ref().is_some_and(|session| session.snapshot == snapshot) {
            return Ok(());
        }
        if let Some(session) = slot.take() {
            session.guard.shutdown().await;
        }

        let guard = ClientAdminBuilder::new()
            .namesrv_addr(snapshot.namesrv_addr.clone())
            .admin_group(unique_admin_group())
            .timeout_millis(5_000)
            .vip_channel_enabled(snapshot.use_vip_channel)
            .use_tls(snapshot.use_tls)
            .build_with_guard()
            .await?;
        tracing::info!(
            namesrv = %snapshot.namesrv_addr,
            use_vip_channel = snapshot.use_vip_channel,
            use_tls = snapshot.use_tls,
            "connected RocketMQ dashboard admin session"
        );
        *slot = Some(ManagedAdminSession { guard, snapshot });
        Ok(())
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

fn active_session(slot: &mut Option<ManagedAdminSession>) -> Result<&mut AdminSession, DashboardError> {
    slot.as_mut()
        .map(|session| session.guard.inner_mut())
        .ok_or_else(|| DashboardError::Internal("Admin session was not initialized".to_string()))
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
    use rocketmq_admin_core::core::dashboard::DashboardAclUser;

    use super::map_acl_user;

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
}
