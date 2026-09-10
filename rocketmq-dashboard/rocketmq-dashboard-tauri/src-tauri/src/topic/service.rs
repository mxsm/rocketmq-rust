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

use crate::topic::batch::{TopicBatchOperation, TopicBatchResult, TopicTargetKind, project_batch};
use crate::topic::guard::{TopicIntent, TopicWriteMode, check_topic, validate_targets};
use rocketmq_admin_core::core::topic::{
    SkipTopicAccumulatedRequest, TopicBatchDeleteAdmin, TopicBatchDeleteRequest, TopicBatchMutationAdmin,
    TopicBatchTargetOutcome, TopicBatchUpsertRequest, TopicSkipMutationAdmin,
};
use std::sync::Arc;

use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::topic::DeleteTopicAdminRequest;
use rocketmq_admin_core::core::topic::GetTopicConfigRequest as AdminGetTopicConfigRequest;
use rocketmq_admin_core::core::topic::GetTopicRouteRequest;
use rocketmq_admin_core::core::topic::ResetTopicConsumerOffsetRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;
use rocketmq_admin_core::core::topic::TopicCatalogRequest;
use rocketmq_admin_core::core::topic::TopicRoute;
use rocketmq_admin_core::core::topic::TopicSendRequest as AdminTopicSendRequest;
use rocketmq_admin_core::core::topic::TopicSendResult as AdminTopicSendResult;
use rocketmq_admin_core::core::topic::TopicStats;
use rocketmq_dashboard_common::DeleteTopicByBrokerRequest;
use rocketmq_dashboard_common::DeleteTopicRequest;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use rocketmq_dashboard_common::ResetOffsetRequest;
use rocketmq_dashboard_common::SendTopicMessageRequest;
use rocketmq_dashboard_common::TopicConfigQueryRequest;
use rocketmq_dashboard_common::TopicConfigRequest;
use rocketmq_dashboard_common::TopicListRequest;
use rocketmq_dashboard_common::TopicQueryRequest;
use tokio::sync::Mutex;

use crate::error::DashboardError as TopicError;
use crate::nameserver::NameServerRuntimeState;
use crate::topic::admin::ManagedTopicAdmin;
use crate::topic::types::TopicBrokerAddressView;
use crate::topic::types::TopicConfigView;
use crate::topic::types::TopicConsumerGroupListResponse;
use crate::topic::types::TopicConsumerInfoResponse;
use crate::topic::types::TopicConsumerInfoView;
use crate::topic::types::TopicCurrentStatsFailure;
use crate::topic::types::TopicCurrentStatsItem;
use crate::topic::types::TopicCurrentStatsResponse;
use crate::topic::types::TopicListItem;
use crate::topic::types::TopicListResponse;
use crate::topic::types::TopicMutationResult;
use crate::topic::types::TopicResult;
use crate::topic::types::TopicRouteBrokerView;
use crate::topic::types::TopicRouteQueueView;
use crate::topic::types::TopicRouteView;
use crate::topic::types::TopicSendMessageResult;
use crate::topic::types::TopicStatusOffsetView;
use crate::topic::types::TopicStatusView;
use crate::topic::types::TopicTargetOption;

#[derive(Clone)]
pub(crate) struct TopicManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedTopicAdmin>>>,
    mutation_session: Arc<Mutex<Option<ManagedTopicAdmin>>>,
}

impl TopicManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
            mutation_session: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn shutdown(&self) {
        for slot in [&self.admin_session, &self.mutation_session] {
            let mut session = slot.lock().await;
            if let Some(mut admin) = session.take() {
                admin.shutdown().await;
            }
        }
    }

    pub(crate) async fn get_topic_list(&self, request: TopicListRequest) -> TopicResult<TopicListResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let snapshot = session_guard
                .as_ref()
                .expect("topic admin session should be initialized before use")
                .snapshot
                .clone();
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_list_with_admin(&mut session.admin, &snapshot, request.clone())
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_list failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_list` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_topic_current_stats(&self) -> TopicResult<TopicCurrentStatsResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let snapshot = session_guard
                .as_ref()
                .expect("topic admin session should be initialized before use")
                .snapshot
                .clone();
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_current_stats_with_admin(&mut session.admin, &snapshot)
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_current_stats failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_current_stats` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_topic_route(&self, request: TopicQueryRequest) -> TopicResult<TopicRouteView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_route_with_admin(&mut session.admin, request.clone())
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_route failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_route` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_topic_stats(&self, request: TopicQueryRequest) -> TopicResult<TopicStatusView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_stats_with_admin(&mut session.admin, request.clone())
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_stats failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_stats` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_topic_config(&self, request: TopicConfigQueryRequest) -> TopicResult<TopicConfigView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_config_with_admin(&mut session.admin, request.clone())
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_config failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_config` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn create_or_update_topic(
        &self,
        request: TopicConfigRequest,
        mode: TopicWriteMode,
    ) -> TopicResult<TopicBatchResult> {
        let mut session_guard = self.mutation_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let result = {
            let session = session_guard
                .as_mut()
                .expect("topic admin session should be initialized before use");
            self.create_or_update_topic_with_admin(&mut session.admin, request, mode)
                .await
        };
        if Self::should_reset_session(&result) {
            self.reset_admin_session(&mut session_guard, "create_or_update_topic failed")
                .await;
        }
        drop(session_guard);
        result
    }

    pub(crate) async fn delete_topic(&self, request: DeleteTopicRequest) -> TopicResult<TopicBatchResult> {
        let mut session_guard = self.mutation_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let result = {
            let session = session_guard
                .as_mut()
                .expect("topic admin session should be initialized before use");
            self.delete_topic_with_admin(&mut session.admin, request).await
        };
        if Self::should_reset_session(&result) {
            self.reset_admin_session(&mut session_guard, "delete_topic failed")
                .await;
        }
        drop(session_guard);
        result
    }

    pub(crate) async fn delete_topic_by_broker(
        &self,
        request: DeleteTopicByBrokerRequest,
    ) -> TopicResult<TopicBatchResult> {
        let mut session_guard = self.mutation_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let result = {
            let session = session_guard
                .as_mut()
                .expect("topic admin session should be initialized before use");
            self.delete_topic_by_broker_with_admin(&mut session.admin, request)
                .await
        };
        if Self::should_reset_session(&result) {
            self.reset_admin_session(&mut session_guard, "delete_topic_by_broker failed")
                .await;
        }
        drop(session_guard);
        result
    }

    pub(crate) async fn get_topic_consumer_groups(
        &self,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerGroupListResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_consumer_groups_with_admin(&mut session.admin, request.clone())
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_consumer_groups failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_consumer_groups` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_topic_consumers(
        &self,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerInfoResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("topic admin session should be initialized before use");
                self.get_topic_consumers_with_admin(&mut session.admin, request.clone())
                    .await
            };
            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_topic_consumers failed")
                    .await;
            }
            drop(session_guard);
            match result {
                Ok(response) => return Ok(response),
                Err(_error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_topic_consumers` after reconnect");
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn reset_consumer_offset(&self, request: ResetOffsetRequest) -> TopicResult<TopicMutationResult> {
        self.apply_offset_reset(request, false).await
    }

    pub(crate) async fn skip_message_accumulate(
        &self,
        request: ResetOffsetRequest,
    ) -> TopicResult<TopicMutationResult> {
        self.apply_offset_reset(request, true).await
    }

    pub(crate) async fn send_topic_message(
        &self,
        request: SendTopicMessageRequest,
    ) -> TopicResult<TopicSendMessageResult> {
        let mut session_guard = self.mutation_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let result = {
            let session = session_guard
                .as_mut()
                .expect("topic admin session should be initialized before use");
            self.send_topic_message_with_admin(&mut session.admin, request).await
        };
        if Self::should_reset_session(&result) {
            self.reset_admin_session(&mut session_guard, "send_topic_message failed")
                .await;
        }
        drop(session_guard);
        result
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedTopicAdmin>) -> TopicResult<()> {
        let generation = self.runtime.generation();
        let needs_reconnect = session_slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(generation));
        if needs_reconnect {
            self.reset_admin_session(session_slot, "refreshing topic admin session")
                .await;
            let session = ManagedTopicAdmin::connect(&self.runtime).await?;
            log::info!(
                "Connected topic admin session for namesrv `{}` at generation {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                session.generation
            );
            *session_slot = Some(session);
        }
        Ok(())
    }

    async fn reset_admin_session(&self, session_slot: &mut Option<ManagedTopicAdmin>, reason: &str) {
        if let Some(mut session) = session_slot.take() {
            log::info!(
                "Shutting down topic admin session for namesrv `{}`: {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                reason
            );
            session.shutdown().await;
        }
    }

    fn should_reset_session<T>(result: &TopicResult<T>) -> bool {
        match result {
            Err(TopicError::Admin(error)) => error.is_retryable() || error.is_session_closed(),
            _ => false,
        }
    }

    async fn get_topic_list_with_admin(
        &self,
        admin: &mut AdminSession,
        snapshot: &NameServerConfigSnapshot,
        request: TopicListRequest,
    ) -> TopicResult<TopicListResponse> {
        let catalog = admin
            .get_topic_catalog(&TopicCatalogRequest {
                skip_system_topics: request.skip_sys_process,
                skip_retry_and_dlq_topics: request.skip_retry_and_dlq,
            })
            .await
            .map_err(map_admin_error)?;
        let items = catalog
            .items
            .into_iter()
            .map(|item| TopicListItem {
                topic: item.topic,
                category: item.category,
                message_type: item.message_type,
                clusters: item.clusters,
                brokers: item.brokers,
                read_queue_count: item.read_queue_count,
                write_queue_count: item.write_queue_count,
                perm: item.perm,
                order: item.order,
                system_topic: item.system_topic,
            })
            .collect::<Vec<_>>();
        Ok(TopicListResponse {
            total: items.len(),
            items,
            targets: catalog
                .targets
                .into_iter()
                .map(|target| TopicTargetOption {
                    cluster_name: target.cluster_name,
                    broker_names: target.broker_names,
                })
                .collect(),
            current_namesrv: snapshot.current_namesrv.clone().unwrap_or_default(),
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        })
    }

    async fn get_topic_current_stats_with_admin(
        &self,
        admin: &mut AdminSession,
        snapshot: &NameServerConfigSnapshot,
    ) -> TopicResult<TopicCurrentStatsResponse> {
        let stats = admin.get_topic_current_stats().await.map_err(map_admin_error)?;
        Ok(TopicCurrentStatsResponse {
            items: stats
                .items
                .into_iter()
                .map(|item| TopicCurrentStatsItem {
                    topic: item.topic,
                    total_msg: item.total_msg,
                    produced_msg_count_24h: item.produced_msg_count_24h,
                    consumed_msg_count_24h: item.consumed_msg_count_24h,
                    in_tps: item.in_tps,
                    out_tps: item.out_tps,
                    consumer_group_count: item.consumer_group_count,
                })
                .collect(),
            failures: stats
                .failures
                .into_iter()
                .map(|failure| TopicCurrentStatsFailure {
                    topic: failure.topic,
                    error: failure.error,
                })
                .collect(),
            current_namesrv: snapshot.current_namesrv.clone().unwrap_or_default(),
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        })
    }

    async fn get_topic_route_with_admin(
        &self,
        admin: &mut AdminSession,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicRouteView> {
        let admin_request = GetTopicRouteRequest::try_new(request.topic.clone()).map_err(map_admin_error)?;
        let route = admin
            .get_topic_route(&admin_request)
            .await
            .map_err(map_admin_error)?
            .ok_or_else(|| TopicError::Validation(format!("Topic `{}` was not found.", request.topic)))?;
        Ok(map_route_view(request.topic, route))
    }

    async fn get_topic_stats_with_admin(
        &self,
        admin: &mut AdminSession,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicStatusView> {
        admin
            .get_topic_stats(&request.topic)
            .await
            .map(map_status_view)
            .map_err(map_admin_error)
    }

    async fn get_topic_config_with_admin(
        &self,
        admin: &mut AdminSession,
        request: TopicConfigQueryRequest,
    ) -> TopicResult<TopicConfigView> {
        let request =
            AdminGetTopicConfigRequest::try_new(request.topic, request.broker_name).map_err(map_admin_error)?;
        let config = admin.get_topic_config(&request).await.map_err(map_admin_error)?;
        Ok(TopicConfigView {
            topic_name: config.topic_name,
            broker_name: config.broker_name,
            cluster_name: config.cluster_name,
            broker_name_list: config.broker_name_list,
            cluster_name_list: config.cluster_name_list,
            read_queue_nums: config.read_queue_nums,
            write_queue_nums: config.write_queue_nums,
            perm: config.perm,
            order: config.order,
            message_type: config.message_type,
            attributes: config.attributes.into_iter().collect(),
            inconsistent_fields: config.inconsistent_fields,
        })
    }

    async fn create_or_update_topic_with_admin(
        &self,
        admin: &mut AdminSession,
        mut request: TopicConfigRequest,
        mode: TopicWriteMode,
    ) -> TopicResult<TopicBatchResult> {
        if request.topic_name.trim().is_empty() {
            return Err(TopicError::Validation("Topic name is required.".into()));
        }
        if request.cluster_name_list.is_empty() && request.broker_name_list.is_empty() {
            return Err(TopicError::Validation(
                "Select at least one cluster or broker before saving the topic.".into(),
            ));
        }
        request.topic_name = request.topic_name.trim().to_string();
        if request.write_queue_nums <= 0 || request.read_queue_nums <= 0 || ![2, 4, 6].contains(&request.perm) {
            return Err(TopicError::Validation(
                "Positive queue counts and read/write permissions are required.".into(),
            ));
        }
        let checked = check_topic(admin, &request.topic_name, mode.into()).await?;
        checked
            .execute(|catalog| async move {
                validate_targets(&catalog, &request.cluster_name_list, &request.broker_name_list)?;
                let broker_names = if request.broker_name_list.is_empty() {
                    catalog
                        .targets
                        .iter()
                        .filter(|target| request.cluster_name_list.contains(&target.cluster_name))
                        .flat_map(|target| target.broker_names.clone())
                        .collect()
                } else {
                    request.broker_name_list.clone()
                };
                let batch = TopicBatchUpsertRequest::try_new(
                    request.topic_name.clone(),
                    broker_names,
                    request.write_queue_nums as u32,
                    request.read_queue_nums as u32,
                    request.perm as u32,
                    request.order,
                    request.message_type,
                )
                .map_err(map_admin_error)?;
                let outcome = admin.upsert_topic_batch(&batch).await.map_err(map_admin_error)?;
                let operation = match mode {
                    TopicWriteMode::Create => TopicBatchOperation::Create,
                    TopicWriteMode::Update => TopicBatchOperation::Update,
                };
                Ok(project_batch(
                    operation,
                    request.topic_name,
                    TopicTargetKind::Broker,
                    outcome.targets,
                    outcome.order_config,
                ))
            })
            .await
    }

    async fn delete_topic_with_admin(
        &self,
        admin: &mut AdminSession,
        request: DeleteTopicRequest,
    ) -> TopicResult<TopicBatchResult> {
        let topic = request.topic.trim().to_string();
        if topic.is_empty() {
            return Err(TopicError::Validation("Topic name is required.".into()));
        }
        let checked = check_topic(admin, &topic, TopicIntent::Existing).await?;
        checked
            .execute(|catalog| async move {
                if let Some(cluster) = &request.cluster_name {
                    validate_targets(&catalog, std::slice::from_ref(cluster), &[])?;
                }
                let cluster_names = request.cluster_name.map(|cluster| vec![cluster]).unwrap_or_else(|| {
                    catalog
                        .items
                        .iter()
                        .find(|item| item.topic == topic)
                        .map(|item| item.clusters.clone())
                        .unwrap_or_default()
                });
                let batch = TopicBatchDeleteRequest::try_new(topic.clone(), cluster_names).map_err(map_admin_error)?;
                let outcome = admin.delete_topic_batch(&batch).await.map_err(map_admin_error)?;
                Ok(project_batch(
                    TopicBatchOperation::Delete,
                    topic,
                    TopicTargetKind::Cluster,
                    outcome.targets,
                    outcome.order_config,
                ))
            })
            .await
    }

    async fn delete_topic_by_broker_with_admin(
        &self,
        admin: &mut AdminSession,
        request: DeleteTopicByBrokerRequest,
    ) -> TopicResult<TopicBatchResult> {
        let topic = request.topic.trim().to_string();
        if topic.is_empty() {
            return Err(TopicError::Validation("Topic name is required.".into()));
        }
        let broker_name = request.broker_name.trim().to_string();
        if broker_name.is_empty() {
            return Err(TopicError::Validation("Broker name is required.".into()));
        }
        let checked = check_topic(admin, &topic, TopicIntent::Existing).await?;
        checked
            .execute(|catalog| async move {
                if !catalog
                    .items
                    .iter()
                    .any(|item| item.topic == topic && item.brokers.contains(&broker_name))
                {
                    return Err(TopicError::Validation(
                        "Broker does not host the selected topic.".into(),
                    ));
                }
                let outcome = admin
                    .delete_topic(&DeleteTopicAdminRequest {
                        topic: topic.clone(),
                        cluster_name: None,
                        broker_name: Some(broker_name.clone()),
                    })
                    .await;
                let target = TopicBatchTargetOutcome {
                    broker_name,
                    success: outcome.is_ok(),
                    message: String::new(),
                };
                Ok(project_batch(
                    TopicBatchOperation::DeleteBroker,
                    topic,
                    TopicTargetKind::Broker,
                    vec![target],
                    None,
                ))
            })
            .await
    }

    async fn get_topic_consumer_groups_with_admin(
        &self,
        admin: &mut AdminSession,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerGroupListResponse> {
        let groups = admin
            .get_topic_consumer_groups(&request.topic)
            .await
            .map_err(map_admin_error)?;
        Ok(TopicConsumerGroupListResponse {
            topic: request.topic,
            consumer_groups: groups.groups,
        })
    }

    async fn get_topic_consumers_with_admin(
        &self,
        admin: &mut AdminSession,
        request: TopicQueryRequest,
    ) -> TopicResult<TopicConsumerInfoResponse> {
        let consumers = admin
            .get_topic_consumers(&request.topic)
            .await
            .map_err(map_admin_error)?;
        Ok(TopicConsumerInfoResponse {
            topic: request.topic,
            items: consumers
                .items
                .into_iter()
                .map(|item| TopicConsumerInfoView {
                    consumer_group: item.consumer_group,
                    total_diff: item.total_diff,
                    inflight_diff: item.inflight_diff,
                    consume_tps: item.consume_tps,
                })
                .collect(),
        })
    }

    async fn apply_offset_reset(
        &self,
        request: ResetOffsetRequest,
        skip_accumulate: bool,
    ) -> TopicResult<TopicMutationResult> {
        let reset_timestamp = validate_offset_reset(&request, skip_accumulate)?;
        let operation_name = if skip_accumulate {
            "skip_message_accumulate"
        } else {
            "reset_consumer_offset"
        };
        let mut session_guard = self.mutation_session.lock().await;
        self.ensure_admin_session(&mut session_guard).await?;
        let result = async {
            let session = session_guard
                .as_mut()
                .expect("topic admin session should be initialized before use");
            let checked = check_topic(&mut session.admin, &request.topic, TopicIntent::Existing).await?;
            checked
                .execute(|_| async {
                    let mut affected_queues = 0usize;
                    for consumer_group in &request.consumer_group_list {
                        let outcome = match reset_timestamp {
                            OffsetResetPosition::Latest => {
                                session
                                    .admin
                                    .skip_accumulated(
                                        &SkipTopicAccumulatedRequest::try_new(
                                            request.topic.clone(),
                                            consumer_group.clone(),
                                            None,
                                            request.force,
                                        )
                                        .map_err(map_admin_error)?,
                                    )
                                    .await
                            }
                            OffsetResetPosition::Timestamp(timestamp) => {
                                session
                                    .admin
                                    .reset_topic_consumer_offset(&ResetTopicConsumerOffsetRequest {
                                        consumer_group: consumer_group.clone(),
                                        topic: request.topic.clone(),
                                        reset_timestamp: timestamp,
                                        force: request.force,
                                    })
                                    .await
                            }
                        }
                        .map_err(map_admin_error)?;
                        affected_queues += outcome.target_count;
                    }
                    Ok(TopicMutationResult {
                        success: true,
                        message: if skip_accumulate {
                            format!(
                                "Skipped accumulated messages for {} consumer group(s).",
                                request.consumer_group_list.len()
                            )
                        } else {
                            format!(
                                "Reset offsets for {} consumer group(s).",
                                request.consumer_group_list.len()
                            )
                        },
                        topic_name: Some(request.topic.clone()),
                        affected_queues: Some(affected_queues),
                    })
                })
                .await
        }
        .await;
        if Self::should_reset_session(&result) {
            self.reset_admin_session(&mut session_guard, &format!("{operation_name} failed"))
                .await;
        }
        drop(session_guard);
        result
    }

    async fn send_topic_message_with_admin(
        &self,
        admin: &mut AdminSession,
        request: SendTopicMessageRequest,
    ) -> TopicResult<TopicSendMessageResult> {
        let message_body = normalize_topic_message_body(&request.message_body)?;
        let checked = check_topic(admin, &request.topic, TopicIntent::Existing).await?;
        checked
            .execute(|_| async move {
                let result = admin
                    .send_topic_test_message(&AdminTopicSendRequest {
                        topic: request.topic,
                        key: request.key,
                        tag: request.tag,
                        message_body,
                        trace_enabled: request.trace_enabled,
                    })
                    .await
                    .map_err(map_admin_error)?;
                Ok(map_send_result(result))
            })
            .await
    }
}

mod mapping;

use mapping::*;

#[derive(Debug, PartialEq, Eq)]
enum OffsetResetPosition {
    Latest,
    Timestamp(u64),
}

fn validate_offset_reset(request: &ResetOffsetRequest, skip_accumulate: bool) -> TopicResult<OffsetResetPosition> {
    use rocketmq_admin_core::core::consumer::is_protected_consumer_group;
    if request.consumer_group_list.is_empty()
        || request
            .consumer_group_list
            .iter()
            .any(|group| group.trim().is_empty() || group.trim() != group || is_protected_consumer_group(group))
    {
        return Err(TopicError::Validation(
            "Select non-system Consumer groups before resetting offsets.".into(),
        ));
    }
    if skip_accumulate {
        return Ok(OffsetResetPosition::Latest);
    }
    u64::try_from(request.reset_time)
        .map(OffsetResetPosition::Timestamp)
        .map_err(|_| TopicError::Validation("Reset time must be a nonnegative millisecond timestamp.".into()))
}

#[cfg(test)]
mod tests;
