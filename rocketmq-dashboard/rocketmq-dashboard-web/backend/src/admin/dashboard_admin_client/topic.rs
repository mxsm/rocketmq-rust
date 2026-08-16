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

use rocketmq_admin_core::core::AdminError;
use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use rocketmq_admin_core::core::topic;
use rocketmq_admin_core::core::topic::DeleteTopicAdminRequest;
use rocketmq_admin_core::core::topic::GetTopicConfigRequest;
use rocketmq_admin_core::core::topic::ResetTopicConsumerOffsetRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;
use rocketmq_admin_core::core::topic::TopicBatchMutationAdmin;
use rocketmq_admin_core::core::topic::TopicBatchUpsertRequest;
use rocketmq_admin_core::core::topic::TopicCatalogRequest;
use tokio::sync::OwnedMutexGuard;

use super::*;
use crate::model::TopicConfigView;
use crate::model::TopicConsumerView;
use crate::model::TopicConsumersView;
use crate::model::TopicOffsetResult;
use crate::model::TopicOperationResult;
use crate::model::TopicQueueOffsetView;
use crate::model::TopicResetOffsetRequest;
use crate::model::TopicSendResultView;
use crate::model::TopicSkipOffsetRequest;
use crate::model::TopicTargetOptionView;
use crate::model::TopicTargetResult;
use crate::model::TopicTestMessageRequest;
use crate::model::build_operation_result;

#[path = "topic_operations.rs"]
mod topic_operations;

use topic_operations::run_topic_batch_delete;

impl DashboardAdminClient {
    pub async fn list_topics(&self) -> Result<TopicListView, DashboardError> {
        let catalog = run_topic_admin_rpc!(self, |admin| admin.get_topic_catalog(&TopicCatalogRequest {
            skip_system_topics: false,
            skip_retry_and_dlq_topics: false,
        }))?;
        Ok(map_topic_catalog(catalog))
    }

    pub async fn get_topic(&self, topic: &str) -> Result<TopicInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let catalog = self.list_topics().await?;
        topic_info_from_catalog(&catalog, topic).ok_or_else(|| {
            DashboardError::NotFound(format!(
                "Topic `{topic}` was not found in the authoritative topic catalog"
            ))
        })
    }

    pub async fn topic_route(&self, topic: &str) -> Result<TopicRouteInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let route = run_admin_rpc!(self, |admin| admin.dashboard_topic_route(topic))?;
        Ok(map_topic_route(route))
    }

    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStatsInfo, DashboardError> {
        validate_name(topic, "Topic")?;
        let topic = topic.to_string();
        let stats = run_topic_admin_rpc!(self, |admin| admin.get_topic_stats(&topic))?;
        Ok(map_topic_stats(stats))
    }

    pub async fn topic_config(
        &self,
        topic: &str,
        broker_name: Option<&str>,
    ) -> Result<TopicConfigView, DashboardError> {
        let request = GetTopicConfigRequest::try_new(topic, broker_name.map(str::to_string))?;
        let config = run_topic_admin_rpc!(self, |admin| admin.get_topic_config(&request))?;
        Ok(map_topic_config(config))
    }

    pub async fn topic_consumers(&self, topic: &str) -> Result<TopicConsumersView, DashboardError> {
        validate_name(topic, "Topic")?;
        let topic = topic.to_string();
        let consumers = run_topic_admin_rpc!(self, |admin| admin.get_topic_consumers(&topic))?;
        Ok(map_topic_consumers(consumers))
    }

    pub async fn create_or_update_topic(
        &self,
        request: TopicMutationRequest,
    ) -> Result<TopicOperationResult, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.create_or_update_topic_with_guard(request, mutation_guard).await
    }

    pub(crate) async fn create_or_update_topic_with_guard(
        &self,
        request: TopicMutationRequest,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOperationResult, DashboardError> {
        self.upsert_topic(request, TopicMutationKind::Update, mutation_guard)
            .await
    }

    pub async fn create_topic(&self, request: TopicMutationRequest) -> Result<TopicOperationResult, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.create_topic_with_guard(request, mutation_guard).await
    }

    pub(crate) async fn create_topic_with_guard(
        &self,
        request: TopicMutationRequest,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOperationResult, DashboardError> {
        self.upsert_topic(request, TopicMutationKind::Create, mutation_guard)
            .await
    }

    pub async fn send_topic_test_message(
        &self,
        topic: &str,
        request: TopicTestMessageRequest,
    ) -> Result<TopicSendResultView, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.send_topic_test_message_with_guard(topic, request, mutation_guard)
            .await
    }

    pub(crate) async fn send_topic_test_message_with_guard(
        &self,
        topic: &str,
        request: TopicTestMessageRequest,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicSendResultView, DashboardError> {
        let topic = topic.trim().to_string();
        validate_name(&topic, "Topic")?;
        let request = normalize_test_message_request(request)?;
        run_topic_admin_rpc!(self, Some(mutation_guard), |admin| async {
            let catalog = authoritative_topic_catalog(admin).await?;
            let mut executor = TopicAdminSendExecutor { admin };
            run_topic_send(catalog, topic, request, &mut executor).await
        })
    }

    pub async fn reset_topic_consumer_offset(
        &self,
        topic: &str,
        request: TopicResetOffsetRequest,
    ) -> Result<TopicOffsetResult, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.reset_topic_consumer_offset_with_guard(topic, request, mutation_guard)
            .await
    }

    pub(crate) async fn reset_topic_consumer_offset_with_guard(
        &self,
        topic: &str,
        request: TopicResetOffsetRequest,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOffsetResult, DashboardError> {
        let topic = topic.trim().to_string();
        validate_name(&topic, "Topic")?;
        let request = normalize_reset_offset_request(request)?;
        run_topic_admin_rpc!(self, Some(mutation_guard), |admin| async {
            let catalog = authoritative_topic_catalog(admin).await?;
            let mut executor = TopicAdminOffsetExecutor { admin };
            run_topic_offset(
                catalog,
                "RESET_OFFSET",
                topic,
                request.consumer_group,
                request.reset_timestamp,
                request.force,
                &mut executor,
            )
            .await
        })
    }

    pub async fn skip_topic_consumer_offset(
        &self,
        topic: &str,
        request: TopicSkipOffsetRequest,
    ) -> Result<TopicOffsetResult, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.skip_topic_consumer_offset_with_guard(topic, request, mutation_guard)
            .await
    }

    pub(crate) async fn skip_topic_consumer_offset_with_guard(
        &self,
        topic: &str,
        request: TopicSkipOffsetRequest,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOffsetResult, DashboardError> {
        let topic = topic.trim().to_string();
        validate_name(&topic, "Topic")?;
        let consumer_group = normalize_consumer_group(request.consumer_group)?;
        let applied_timestamp = epoch_millis(std::time::SystemTime::now())?;
        run_topic_admin_rpc!(self, Some(mutation_guard), |admin| async {
            let catalog = authoritative_topic_catalog(admin).await?;
            let mut executor = TopicAdminOffsetExecutor { admin };
            run_topic_offset(
                catalog,
                "SKIP_BACKLOG",
                topic,
                consumer_group,
                applied_timestamp,
                true,
                &mut executor,
            )
            .await
        })
    }

    pub async fn delete_topic_from_broker(
        &self,
        topic: &str,
        broker_name: &str,
    ) -> Result<TopicOperationResult, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.delete_topic_from_broker_with_guard(topic, broker_name, mutation_guard)
            .await
    }

    pub(crate) async fn delete_topic_from_broker_with_guard(
        &self,
        topic: &str,
        broker_name: &str,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOperationResult, DashboardError> {
        let topic = topic.trim().to_string();
        let broker_name = broker_name.trim().to_string();
        validate_name(&topic, "Topic")?;
        validate_name(&broker_name, "Broker")?;
        run_topic_admin_rpc!(self, Some(mutation_guard), |admin| async {
            let catalog = authoritative_topic_catalog(admin).await?;
            let mut executor = TopicAdminDeleteExecutor { admin };
            run_topic_delete(catalog, topic, Some(broker_name), &mut executor).await
        })
    }

    pub async fn delete_topic(&self, topic: &str) -> Result<TopicOperationResult, DashboardError> {
        let mutation_guard = self.acquire_topic_mutation_lock().await;
        self.delete_topic_with_guard(topic, mutation_guard).await
    }

    pub(crate) async fn delete_topic_with_guard(
        &self,
        topic: &str,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOperationResult, DashboardError> {
        let topic = topic.trim().to_string();
        validate_name(&topic, "Topic")?;
        run_topic_admin_rpc!(self, Some(mutation_guard), |admin| async {
            let catalog = authoritative_topic_catalog(admin).await?;
            run_topic_batch_delete(catalog, topic, admin).await
        })
    }
}

#[derive(Clone, Copy)]
enum TopicMutationKind {
    Create,
    Update,
}

impl DashboardAdminClient {
    async fn upsert_topic(
        &self,
        request: TopicMutationRequest,
        kind: TopicMutationKind,
        mutation_guard: OwnedMutexGuard<()>,
    ) -> Result<TopicOperationResult, DashboardError> {
        let operation = match kind {
            TopicMutationKind::Create => "CREATE",
            TopicMutationKind::Update => "UPDATE",
        };
        run_topic_admin_rpc!(self, Some(mutation_guard), |admin| async {
            let catalog = map_topic_catalog(
                admin
                    .get_topic_catalog(&TopicCatalogRequest {
                        skip_system_topics: false,
                        skip_retry_and_dlq_topics: false,
                    })
                    .await?,
            );
            let mut executor = TopicAdminUpsertExecutor { admin };
            run_topic_upserts(catalog, request, kind, operation, &mut executor).await
        })
    }
}

trait TopicUpsertExecutor {
    async fn upsert_batch(
        &mut self,
        request: &TopicBatchUpsertRequest,
    ) -> Result<topic::TopicBatchMutationOutcome, AdminError>;
}

struct TopicAdminUpsertExecutor<'a, T> {
    admin: &'a mut T,
}

trait TopicSendExecutor {
    async fn send(&mut self, request: &topic::TopicSendRequest) -> Result<topic::TopicSendResult, AdminError>;
}

struct TopicAdminSendExecutor<'a, T> {
    admin: &'a mut T,
}

impl<T> TopicSendExecutor for TopicAdminSendExecutor<'_, T>
where
    T: TopicAdmin,
{
    async fn send(&mut self, request: &topic::TopicSendRequest) -> Result<topic::TopicSendResult, AdminError> {
        self.admin.send_topic_test_message(request).await
    }
}

trait TopicDeleteExecutor {
    async fn delete(&mut self, request: &DeleteTopicAdminRequest) -> Result<topic::TopicMutationOutcome, AdminError>;
}

struct TopicAdminDeleteExecutor<'a, T> {
    admin: &'a mut T,
}

impl<T> TopicDeleteExecutor for TopicAdminDeleteExecutor<'_, T>
where
    T: TopicAdmin,
{
    async fn delete(&mut self, request: &DeleteTopicAdminRequest) -> Result<topic::TopicMutationOutcome, AdminError> {
        self.admin.delete_topic(request).await
    }
}

trait TopicOffsetExecutor {
    async fn consumer_groups(&mut self, topic: &str) -> Result<topic::TopicConsumerGroups, AdminError>;

    async fn reset_offset(
        &mut self,
        request: &ResetTopicConsumerOffsetRequest,
    ) -> Result<topic::TopicMutationOutcome, AdminError>;
}

struct TopicAdminOffsetExecutor<'a, T> {
    admin: &'a mut T,
}

impl<T> TopicOffsetExecutor for TopicAdminOffsetExecutor<'_, T>
where
    T: TopicAdmin,
{
    async fn consumer_groups(&mut self, topic: &str) -> Result<topic::TopicConsumerGroups, AdminError> {
        self.admin.get_topic_consumer_groups(topic).await
    }

    async fn reset_offset(
        &mut self,
        request: &ResetTopicConsumerOffsetRequest,
    ) -> Result<topic::TopicMutationOutcome, AdminError> {
        self.admin.reset_topic_consumer_offset(request).await
    }
}

async fn run_topic_send<E>(
    catalog: TopicListView,
    topic: String,
    request: TopicTestMessageRequest,
    executor: &mut E,
) -> Result<TopicSendResultView, DashboardError>
where
    E: TopicSendExecutor,
{
    ensure_topic_operation_allowed(authoritative_topic(&catalog, &topic)?, "SEND")?;
    let result = executor
        .send(&topic::TopicSendRequest {
            topic,
            key: request.key,
            tag: request.tag,
            message_body: request.message_body,
            trace_enabled: request.trace_enabled,
        })
        .await?;
    Ok(map_topic_send_result(result))
}

async fn run_topic_delete<E>(
    catalog: TopicListView,
    topic: String,
    broker_name: Option<String>,
    executor: &mut E,
) -> Result<TopicOperationResult, DashboardError>
where
    E: TopicDeleteExecutor,
{
    let item = authoritative_topic(&catalog, &topic)?;
    let operation = if broker_name.is_some() {
        "DELETE_BROKER"
    } else {
        "DELETE_TOPIC"
    };
    ensure_topic_operation_allowed(item, operation)?;
    if let Some(broker_name) = broker_name {
        if !item.brokers.iter().any(|broker| broker.trim() == broker_name) {
            return Err(DashboardError::Validation(format!(
                "Broker `{broker_name}` is not an authoritative target for topic `{topic}`"
            )));
        }
        let outcome = executor
            .delete(&DeleteTopicAdminRequest {
                topic: topic.clone(),
                cluster_name: None,
                broker_name: Some(broker_name.clone()),
            })
            .await?;
        return Ok(build_operation_result(
            operation,
            topic,
            vec![TopicTargetResult::success(broker_name, outcome.message)],
        ));
    }
    let clusters = item
        .clusters
        .iter()
        .map(|cluster| cluster.trim())
        .filter(|cluster| !cluster.is_empty())
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    if clusters.is_empty() {
        return Err(DashboardError::Validation(format!(
            "Topic `{topic}` has no authoritative clusters to delete"
        )));
    }
    let mut targets = Vec::with_capacity(clusters.len());
    for cluster_name in clusters {
        match executor
            .delete(&DeleteTopicAdminRequest {
                topic: topic.clone(),
                cluster_name: Some(cluster_name.clone()),
                broker_name: None,
            })
            .await
        {
            Ok(outcome) => targets.push(TopicTargetResult::success(cluster_name, outcome.message)),
            Err(_) => targets.push(TopicTargetResult::failure(cluster_name, "Cluster deletion failed")),
        }
    }
    Ok(build_operation_result(operation, topic, targets))
}

async fn run_topic_offset<E>(
    catalog: TopicListView,
    operation: &str,
    topic: String,
    consumer_group: String,
    applied_timestamp: u64,
    force: bool,
    executor: &mut E,
) -> Result<TopicOffsetResult, DashboardError>
where
    E: TopicOffsetExecutor,
{
    ensure_topic_operation_allowed(authoritative_topic(&catalog, &topic)?, operation)?;
    let groups = executor.consumer_groups(&topic).await?;
    if !groups.groups.iter().any(|group| group.trim() == consumer_group) {
        return Err(DashboardError::Validation(format!(
            "Consumer group `{consumer_group}` does not consume topic `{topic}`"
        )));
    }
    let outcome = executor
        .reset_offset(&ResetTopicConsumerOffsetRequest {
            consumer_group: consumer_group.clone(),
            topic: topic.clone(),
            reset_timestamp: applied_timestamp,
            force,
        })
        .await?;
    Ok(TopicOffsetResult {
        operation: operation.into(),
        topic,
        consumer_group,
        success: true,
        affected_queue_count: outcome.target_count,
        applied_timestamp,
        message: outcome.message,
    })
}

impl<T> TopicUpsertExecutor for TopicAdminUpsertExecutor<'_, T>
where
    T: TopicBatchMutationAdmin,
{
    async fn upsert_batch(
        &mut self,
        request: &TopicBatchUpsertRequest,
    ) -> Result<topic::TopicBatchMutationOutcome, AdminError> {
        self.admin.upsert_topic_batch(request).await
    }
}

async fn run_topic_upserts<E>(
    catalog: TopicListView,
    request: TopicMutationRequest,
    kind: TopicMutationKind,
    operation: &str,
    executor: &mut E,
) -> Result<TopicOperationResult, DashboardError>
where
    E: TopicUpsertExecutor,
{
    match kind {
        TopicMutationKind::Create => ensure_topic_does_not_exist(&catalog, &request.topic)?,
        TopicMutationKind::Update => require_mutable_topic(&catalog, &request.topic)?,
    }
    let broker_names = resolve_topic_targets(&catalog.targets, &request.cluster_name_list, &request.broker_name_list)?;
    let batch_request = TopicBatchUpsertRequest::try_new(
        request.topic.clone(),
        broker_names,
        request.write_queue_count,
        request.read_queue_count,
        request.perm,
        request.order.unwrap_or(false),
        request.message_type.clone(),
    )?;
    let batch_outcome = executor.upsert_batch(&batch_request).await?;
    let mut targets = batch_outcome
        .targets
        .into_iter()
        .map(|target| {
            if target.success {
                TopicTargetResult::success(target.broker_name, target.message)
            } else {
                TopicTargetResult::failure(target.broker_name, target.message)
            }
        })
        .collect::<Vec<_>>();
    if let Some(order_config) = batch_outcome.order_config.filter(|outcome| !outcome.success) {
        targets.push(TopicTargetResult::failure("ORDER_TOPIC_CONFIG", order_config.message));
    }
    Ok(build_operation_result(operation, request.topic, targets))
}

pub(crate) fn resolve_topic_targets(
    targets: &[TopicTargetOptionView],
    cluster_names: &[String],
    broker_names: &[String],
) -> Result<Vec<String>, DashboardError> {
    let mut resolved = std::collections::BTreeSet::new();
    for cluster_name in cluster_names {
        let cluster_name = cluster_name.trim();
        let target = targets
            .iter()
            .find(|target| target.cluster_name == cluster_name)
            .ok_or_else(|| DashboardError::Validation(format!("Unknown cluster target `{cluster_name}`")))?;
        resolved.extend(
            target
                .broker_names
                .iter()
                .map(|broker_name| broker_name.trim())
                .filter(|broker_name| !broker_name.is_empty())
                .map(str::to_string),
        );
    }
    for broker_name in broker_names {
        let broker_name = broker_name.trim();
        let canonical_broker_name = targets
            .iter()
            .flat_map(|target| target.broker_names.iter())
            .map(|candidate| candidate.trim())
            .find(|candidate| *candidate == broker_name && !candidate.is_empty())
            .ok_or_else(|| DashboardError::Validation(format!("Unknown broker target `{broker_name}`")))?;
        resolved.insert(canonical_broker_name.to_string());
    }
    if resolved.is_empty() {
        return Err(DashboardError::Validation(
            "Select at least one cluster or broker before saving the topic".to_string(),
        ));
    }
    Ok(resolved.into_iter().collect())
}

fn ensure_topic_does_not_exist(catalog: &TopicListView, topic: &str) -> Result<(), DashboardError> {
    if catalog.items.iter().any(|item| item.topic == topic) {
        return Err(DashboardError::Validation(format!("Topic `{topic}` already exists")));
    }
    Ok(())
}

fn require_mutable_topic(catalog: &TopicListView, topic: &str) -> Result<(), DashboardError> {
    ensure_topic_operation_allowed(authoritative_topic(catalog, topic)?, "EDIT")?;
    Ok(())
}

async fn authoritative_topic_catalog<T>(admin: &mut T) -> Result<TopicListView, DashboardError>
where
    T: TopicAdmin,
{
    Ok(map_topic_catalog(
        admin
            .get_topic_catalog(&TopicCatalogRequest {
                skip_system_topics: false,
                skip_retry_and_dlq_topics: false,
            })
            .await?,
    ))
}

fn authoritative_topic<'a>(catalog: &'a TopicListView, topic: &str) -> Result<&'a TopicInfo, DashboardError> {
    catalog.items.iter().find(|item| item.topic == topic).ok_or_else(|| {
        DashboardError::NotFound(format!(
            "Topic `{topic}` was not found in the authoritative topic catalog"
        ))
    })
}

pub(crate) fn ensure_topic_operation_allowed(topic: &TopicInfo, operation: &str) -> Result<(), DashboardError> {
    if topic.system_topic {
        return Err(DashboardError::Validation(format!(
            "System topic `{}` cannot perform {operation}",
            topic.topic
        )));
    }
    Ok(())
}

fn normalize_test_message_request(
    mut request: TopicTestMessageRequest,
) -> Result<TopicTestMessageRequest, DashboardError> {
    request.key = request.key.trim().to_string();
    request.tag = request.tag.trim().to_string();
    if request.message_body.trim().is_empty() {
        return Err(DashboardError::Validation("Message body cannot be blank".to_string()));
    }
    Ok(request)
}

fn normalize_reset_offset_request(
    mut request: TopicResetOffsetRequest,
) -> Result<TopicResetOffsetRequest, DashboardError> {
    request.consumer_group = normalize_consumer_group(request.consumer_group)?;
    Ok(request)
}

fn normalize_consumer_group(consumer_group: String) -> Result<String, DashboardError> {
    let consumer_group = consumer_group.trim().to_string();
    validate_name(&consumer_group, "Consumer group")?;
    Ok(consumer_group)
}

pub(crate) fn epoch_millis(timestamp: std::time::SystemTime) -> Result<u64, DashboardError> {
    let millis = timestamp
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DashboardError::Validation("Timestamp must not be before the Unix epoch".to_string()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| DashboardError::Validation("Timestamp exceeds supported epoch milliseconds".to_string()))
}

pub(crate) fn canonical_send_status(status: &str) -> String {
    let status = status.trim();
    let split = status.find(char::is_whitespace).unwrap_or(status.len());
    let (prefix, suffix) = status.split_at(split);
    let canonical = match prefix.rsplit("::").next().unwrap_or(prefix) {
        "SendOk" | "SEND_OK" => "SEND_OK",
        "FlushDiskTimeout" | "FLUSH_DISK_TIMEOUT" => "FLUSH_DISK_TIMEOUT",
        "FlushSlaveTimeout" | "FLUSH_SLAVE_TIMEOUT" => "FLUSH_SLAVE_TIMEOUT",
        "SlaveNotAvailable" | "SLAVE_NOT_AVAILABLE" => "SLAVE_NOT_AVAILABLE",
        _ => prefix,
    };
    format!("{canonical}{suffix}")
}

pub(crate) fn is_successful_send_status(status: &str) -> bool {
    status == "SEND_OK"
        || status
            .strip_prefix("SEND_OK ")
            .is_some_and(|suffix| suffix.starts_with('('))
}

fn map_topic_send_result(result: topic::TopicSendResult) -> TopicSendResultView {
    let send_status = canonical_send_status(&result.send_status);
    TopicSendResultView {
        topic: result.topic,
        success: is_successful_send_status(&send_status),
        send_status,
        message_id: result.message_id,
        broker_name: result.broker_name,
        queue_id: result.queue_id,
        queue_offset: result.queue_offset,
        transaction_id: result.transaction_id,
        region_id: result.region_id,
        local_transaction_state: result.local_transaction_state,
    }
}

fn map_topic_catalog(catalog: topic::TopicCatalog) -> TopicListView {
    let mut items = catalog
        .items
        .into_iter()
        .map(|item| {
            let mut brokers = item.brokers;
            let mut clusters = item.clusters;
            brokers.sort_unstable();
            clusters.sort_unstable();
            TopicInfo {
                topic: item.topic,
                broker_name: brokers.first().cloned(),
                brokers,
                clusters,
                read_queue_count: item.read_queue_count,
                write_queue_count: item.write_queue_count,
                perm: item.perm.max(0) as u32,
                category: item.category,
                message_type: item.message_type,
                order: item.order,
                system_topic: item.system_topic,
            }
        })
        .collect::<Vec<_>>();
    let mut target_brokers_by_cluster = std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();
    for target in catalog.targets {
        let cluster_name = target.cluster_name.trim();
        if cluster_name.is_empty() {
            continue;
        }
        target_brokers_by_cluster
            .entry(cluster_name.to_string())
            .or_default()
            .extend(
                target
                    .broker_names
                    .into_iter()
                    .map(|broker_name| broker_name.trim().to_string())
                    .filter(|broker_name| !broker_name.is_empty()),
            );
    }
    let targets = target_brokers_by_cluster
        .into_iter()
        .map(|(cluster_name, broker_names)| TopicTargetOptionView {
            cluster_name,
            broker_names: broker_names.into_iter().collect(),
        })
        .collect::<Vec<_>>();
    items.sort_unstable_by(|left, right| left.topic.cmp(&right.topic));
    TopicListView {
        total: items.len(),
        items,
        targets,
    }
}

fn map_topic_route(route: core::DashboardTopicRoute) -> TopicRouteInfo {
    TopicRouteInfo {
        topic: route.topic,
        brokers: route
            .brokers
            .into_iter()
            .map(|broker| TopicRouteBroker {
                broker_name: broker.broker_name,
                broker_addrs: broker.broker_addrs,
            })
            .collect(),
        queues: route
            .queues
            .into_iter()
            .map(|queue| TopicRouteQueue {
                broker_name: queue.broker_name,
                read_queue_nums: queue.read_queue_nums,
                write_queue_nums: queue.write_queue_nums,
                perm: queue.perm,
            })
            .collect(),
    }
}

fn topic_info_from_catalog(catalog: &TopicListView, topic: &str) -> Option<TopicInfo> {
    catalog.items.iter().find(|item| item.topic == topic).cloned()
}

fn map_topic_stats(stats: topic::TopicStats) -> TopicStatsInfo {
    let mut offsets = stats
        .offsets
        .into_iter()
        .map(|offset| TopicQueueOffsetView {
            broker_name: offset.broker_name,
            queue_id: offset.queue_id,
            min_offset: offset.min_offset,
            max_offset: offset.max_offset,
            last_update_timestamp: offset.last_update_timestamp,
        })
        .collect::<Vec<_>>();
    offsets.sort_unstable_by(|left, right| {
        left.broker_name
            .cmp(&right.broker_name)
            .then(left.queue_id.cmp(&right.queue_id))
    });
    let total_min_offset = offsets.iter().map(|offset| offset.min_offset).sum();
    let total_max_offset = offsets.iter().map(|offset| offset.max_offset).sum();
    TopicStatsInfo {
        topic: stats.topic,
        queue_count: stats.queue_count,
        total_message_count: stats.total_message_count,
        total_min_offset,
        total_max_offset,
        offsets,
    }
}

fn map_topic_config(config: topic::TopicConfigDetail) -> TopicConfigView {
    let mut broker_name_list = config.broker_name_list;
    let mut cluster_name_list = config.cluster_name_list;
    broker_name_list.sort_unstable();
    cluster_name_list.sort_unstable();
    TopicConfigView {
        topic_name: config.topic_name,
        broker_name: config.broker_name,
        cluster_name: config.cluster_name,
        broker_name_list,
        cluster_name_list,
        read_queue_nums: config.read_queue_nums,
        write_queue_nums: config.write_queue_nums,
        perm: config.perm,
        order: config.order,
        message_type: config.message_type,
        attributes: config.attributes,
        inconsistent_fields: config.inconsistent_fields,
    }
}

fn map_topic_consumers(consumers: topic::TopicConsumers) -> TopicConsumersView {
    let mut items = consumers
        .items
        .into_iter()
        .map(|consumer| TopicConsumerView {
            consumer_group: consumer.consumer_group,
            total_diff: consumer.total_diff,
            inflight_diff: consumer.inflight_diff,
            consume_tps: consumer.consume_tps,
        })
        .collect::<Vec<_>>();
    items.sort_unstable_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
    TopicConsumersView { items }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use super::TopicBatchUpsertRequest;
    use super::TopicDeleteExecutor;
    use super::TopicMutationKind;
    use super::TopicOffsetExecutor;
    use super::TopicSendExecutor;
    use super::TopicUpsertExecutor;
    use super::canonical_send_status;
    use super::ensure_topic_operation_allowed;
    use super::epoch_millis;
    use super::is_successful_send_status;
    use super::map_topic_catalog;
    use super::map_topic_stats;
    use super::resolve_topic_targets;
    use super::run_topic_batch_delete;
    use super::run_topic_delete;
    use super::run_topic_offset;
    use super::run_topic_send;
    use super::run_topic_upserts;
    use super::topic_info_from_catalog;
    use super::topic_operations::TopicBatchDeleteExecutor;
    use crate::model::TopicInfo;
    use crate::model::TopicListView;
    use crate::model::TopicMutationRequest;
    use crate::model::TopicTargetOptionView;
    use crate::model::TopicTestMessageRequest;
    use rocketmq_admin_core::core::AdminError;
    use rocketmq_admin_core::core::topic as core_topic;

    #[test]
    fn send_ok_is_the_only_successful_send_status() {
        assert_eq!(canonical_send_status("SendOk"), "SEND_OK");
        assert_eq!(canonical_send_status("FlushDiskTimeout"), "FLUSH_DISK_TIMEOUT");
        assert!(is_successful_send_status("SEND_OK"));
        assert!(is_successful_send_status("SEND_OK (COMMIT_MESSAGE)"));
        assert!(!is_successful_send_status("FLUSH_DISK_TIMEOUT"));
    }

    #[test]
    fn system_topic_rejects_every_mutating_operation() {
        let topic = TopicInfo {
            topic: "RMQ_SYS_TRACE_TOPIC".into(),
            broker_name: Some("broker-a".into()),
            brokers: vec!["broker-a".into()],
            clusters: vec!["DefaultCluster".into()],
            read_queue_count: 1,
            write_queue_count: 1,
            perm: 6,
            category: "SYSTEM".into(),
            message_type: "SYSTEM".into(),
            order: false,
            system_topic: true,
        };
        for operation in [
            "EDIT",
            "SEND",
            "RESET_OFFSET",
            "SKIP_BACKLOG",
            "DELETE_BROKER",
            "DELETE_TOPIC",
        ] {
            assert!(ensure_topic_operation_allowed(&topic, operation).is_err());
        }
    }

    #[test]
    fn skip_timestamp_uses_current_epoch_millis() {
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        assert_eq!(
            epoch_millis(now).expect("timestamp is after the epoch"),
            1_700_000_000_123
        );
    }

    #[tokio::test]
    async fn send_non_send_ok_returns_an_unsuccessful_view() {
        let mut executor = FakeTopicOperations::with_send_status("FlushDiskTimeout");

        let result = run_topic_send(
            mutable_catalog("orders"),
            "orders".into(),
            TopicTestMessageRequest {
                key: "key".into(),
                tag: "tag".into(),
                message_body: "body".into(),
                trace_enabled: false,
            },
            &mut executor,
        )
        .await
        .expect("send returns a broker result");

        assert!(!result.success);
        assert_eq!(result.send_status, "FLUSH_DISK_TIMEOUT");
        assert_eq!(executor.sent_request_count, 1);
    }

    #[tokio::test]
    async fn system_topic_delete_makes_no_executor_calls() {
        let mut executor = FakeTopicOperations::default();

        let error = run_topic_delete(system_catalog(), "RMQ_SYS_TRACE_TOPIC".into(), None, &mut executor)
            .await
            .expect_err("system topic is protected");

        assert!(matches!(error, crate::error::DashboardError::Validation(_)));
        assert!(executor.delete_targets.is_empty());
    }

    #[tokio::test]
    async fn system_topic_send_and_offset_make_no_executor_calls() {
        let mut executor = FakeTopicOperations::with_send_status("SendOk");
        let request = TopicTestMessageRequest {
            key: String::new(),
            tag: String::new(),
            message_body: "body".into(),
            trace_enabled: false,
        };

        run_topic_send(system_catalog(), "RMQ_SYS_TRACE_TOPIC".into(), request, &mut executor)
            .await
            .expect_err("system topic is protected from sends");
        run_topic_offset(
            system_catalog(),
            "SKIP_BACKLOG",
            "RMQ_SYS_TRACE_TOPIC".into(),
            "known-group".into(),
            1_700_000_000_123,
            true,
            &mut executor,
        )
        .await
        .expect_err("system topic is protected from offset changes");

        assert_eq!(executor.sent_request_count, 0);
        assert_eq!(executor.offset_reset_count, 0);
    }

    #[tokio::test]
    async fn unknown_broker_delete_makes_no_executor_calls() {
        let mut executor = FakeTopicOperations::default();

        let error = run_topic_delete(
            mutable_catalog("orders"),
            "orders".into(),
            Some("broker-z".into()),
            &mut executor,
        )
        .await
        .expect_err("unknown broker is rejected before deletion");

        assert!(matches!(error, crate::error::DashboardError::Validation(_)));
        assert!(executor.delete_targets.is_empty());
    }

    #[tokio::test]
    async fn cluster_delete_continues_after_a_failure() {
        let mut executor = FakeTopicOperations {
            delete_failures: BTreeMap::from([("cluster-a".into(), true)]),
            ..Default::default()
        };
        let catalog = TopicListView {
            items: vec![topic_info(
                "orders",
                false,
                vec!["broker-a"],
                vec!["cluster-b", "cluster-a"],
            )],
            total: 1,
            targets: vec![],
        };

        let result = run_topic_delete(catalog, "orders".into(), None, &mut executor)
            .await
            .expect("partial delete is a structured result");

        assert!(!result.success);
        assert_eq!(executor.delete_targets, vec!["cluster-a", "cluster-b"]);
        assert_eq!(result.targets.len(), 2);
        assert!(!result.targets[0].success);
        assert!(result.targets[1].success);
    }

    #[tokio::test]
    async fn batch_delete_order_cleanup_failure_is_a_structured_global_failure() {
        let mut executor = FakeBatchDeleteExecutor;

        let result = run_topic_batch_delete(mutable_catalog("orders"), "orders".into(), &mut executor)
            .await
            .expect("structured batch outcome");

        assert!(!result.success);
        assert!(
            result
                .targets
                .iter()
                .any(|target| target.target == "ORDER_TOPIC_CONFIG" && !target.success)
        );
    }

    #[tokio::test]
    async fn unknown_consumer_group_makes_no_offset_reset_call() {
        let mut executor = FakeTopicOperations::default();

        let error = run_topic_offset(
            mutable_catalog("orders"),
            "RESET_OFFSET",
            "orders".into(),
            "missing-group".into(),
            1_700_000_000_123,
            true,
            &mut executor,
        )
        .await
        .expect_err("unknown group is rejected before reset");

        assert!(matches!(error, crate::error::DashboardError::Validation(_)));
        assert_eq!(executor.offset_reset_count, 0);
    }

    fn mutable_catalog(topic: &str) -> TopicListView {
        TopicListView {
            items: vec![topic_info(topic, false, vec!["broker-a"], vec!["cluster-a"])],
            total: 1,
            targets: vec![],
        }
    }

    fn system_catalog() -> TopicListView {
        TopicListView {
            items: vec![topic_info(
                "RMQ_SYS_TRACE_TOPIC",
                true,
                vec!["broker-a"],
                vec!["cluster-a"],
            )],
            total: 1,
            targets: vec![],
        }
    }

    fn topic_info(topic: &str, system_topic: bool, brokers: Vec<&str>, clusters: Vec<&str>) -> TopicInfo {
        TopicInfo {
            topic: topic.into(),
            broker_name: brokers.first().map(|broker| (*broker).to_string()),
            brokers: brokers.into_iter().map(str::to_string).collect(),
            clusters: clusters.into_iter().map(str::to_string).collect(),
            read_queue_count: 1,
            write_queue_count: 1,
            perm: 6,
            category: if system_topic { "SYSTEM" } else { "NORMAL" }.into(),
            message_type: "NORMAL".into(),
            order: false,
            system_topic,
        }
    }

    #[derive(Default)]
    struct FakeTopicOperations {
        send_status: String,
        sent_request_count: usize,
        delete_targets: Vec<String>,
        delete_failures: BTreeMap<String, bool>,
        offset_reset_count: usize,
    }

    impl FakeTopicOperations {
        fn with_send_status(send_status: &str) -> Self {
            Self {
                send_status: send_status.into(),
                ..Self::default()
            }
        }
    }

    impl TopicSendExecutor for FakeTopicOperations {
        async fn send(
            &mut self,
            request: &core_topic::TopicSendRequest,
        ) -> Result<core_topic::TopicSendResult, AdminError> {
            self.sent_request_count += 1;
            Ok(core_topic::TopicSendResult {
                topic: request.topic.clone(),
                send_status: self.send_status.clone(),
                message_id: Some("message-1".into()),
                broker_name: Some("broker-a".into()),
                queue_id: Some(0),
                queue_offset: 1,
                transaction_id: None,
                region_id: None,
                local_transaction_state: None,
            })
        }
    }

    impl TopicDeleteExecutor for FakeTopicOperations {
        async fn delete(
            &mut self,
            request: &core_topic::DeleteTopicAdminRequest,
        ) -> Result<core_topic::TopicMutationOutcome, AdminError> {
            let target = request
                .broker_name
                .clone()
                .or_else(|| request.cluster_name.clone())
                .expect("test request has one target");
            self.delete_targets.push(target.clone());
            if self.delete_failures.get(&target).copied().unwrap_or(false) {
                return Err(AdminError::backend("delete", "planned failure"));
            }
            Ok(core_topic::TopicMutationOutcome {
                message: format!("deleted {target}"),
                target_count: 1,
            })
        }
    }

    impl TopicOffsetExecutor for FakeTopicOperations {
        async fn consumer_groups(&mut self, _: &str) -> Result<core_topic::TopicConsumerGroups, AdminError> {
            Ok(core_topic::TopicConsumerGroups {
                groups: vec!["known-group".into()],
            })
        }

        async fn reset_offset(
            &mut self,
            _: &core_topic::ResetTopicConsumerOffsetRequest,
        ) -> Result<core_topic::TopicMutationOutcome, AdminError> {
            self.offset_reset_count += 1;
            Ok(core_topic::TopicMutationOutcome {
                message: "reset".into(),
                target_count: 1,
            })
        }
    }

    struct FakeBatchDeleteExecutor;

    impl TopicBatchDeleteExecutor for FakeBatchDeleteExecutor {
        async fn delete_batch(
            &mut self,
            request: &core_topic::TopicBatchDeleteRequest,
        ) -> Result<core_topic::TopicBatchDeleteOutcome, AdminError> {
            Ok(core_topic::TopicBatchDeleteOutcome {
                targets: request
                    .cluster_names()
                    .iter()
                    .map(|cluster| core_topic::TopicBatchTargetOutcome {
                        broker_name: cluster.clone(),
                        success: true,
                        message: "deleted".into(),
                    })
                    .collect(),
                order_config: Some(core_topic::TopicBatchOrderConfigOutcome {
                    success: false,
                    message: "cleanup failed".into(),
                }),
            })
        }
    }

    #[test]
    fn maps_core_stats_without_losing_queue_identity() {
        let view = map_topic_stats(core_topic::TopicStats {
            topic: "orders".into(),
            total_message_count: 9,
            queue_count: 1,
            offsets: vec![core_topic::TopicQueueOffset {
                broker_name: "broker-a".into(),
                queue_id: 2,
                min_offset: 3,
                max_offset: 12,
                last_update_timestamp: 1_700_000_000_000,
            }],
        });
        assert_eq!(view.total_message_count, 9);
        assert_eq!(view.offsets[0].queue_id, 2);
    }

    #[test]
    fn topic_detail_reuses_authoritative_catalog_metadata() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".to_string(),
                category: "NORMAL".to_string(),
                message_type: "MIXED".to_string(),
                clusters: vec!["DefaultCluster".to_string()],
                brokers: vec!["broker-a".to_string()],
                read_queue_count: 8,
                write_queue_count: 12,
                perm: 6,
                order: true,
                system_topic: false,
            }],
            targets: Vec::new(),
        });

        let detail = topic_info_from_catalog(&catalog, "orders").expect("topic catalog entry");

        assert_eq!(detail.clusters, ["DefaultCluster"]);
        assert_eq!(detail.message_type, "MIXED");
        assert!(detail.order);
        assert_eq!(detail.category, "NORMAL");
    }

    #[test]
    fn resolves_clusters_and_brokers_to_unique_canonical_brokers() {
        let targets = vec![TopicTargetOptionView {
            cluster_name: "DefaultCluster".into(),
            broker_names: vec!["broker-a".into(), "broker-b".into()],
        }];

        assert_eq!(
            resolve_topic_targets(&targets, &[" DefaultCluster ".into()], &["broker-a".into()]).unwrap(),
            vec!["broker-a", "broker-b"]
        );
    }

    #[test]
    fn canonicalizes_catalog_broker_names_when_resolving_targets() {
        let targets = vec![TopicTargetOptionView {
            cluster_name: "DefaultCluster".into(),
            broker_names: vec![" broker-a ".into(), "broker-a".into(), " broker-b".into()],
        }];

        assert_eq!(
            resolve_topic_targets(&targets, &["DefaultCluster".into()], &[]).unwrap(),
            vec!["broker-a", "broker-b"]
        );
    }

    #[test]
    fn catalog_mapping_merges_equivalent_cluster_target_entries() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: Vec::new(),
            targets: vec![
                core_topic::TopicTargetOption {
                    cluster_name: " DefaultCluster ".into(),
                    broker_names: vec![" broker-b ".into(), "broker-a".into()],
                },
                core_topic::TopicTargetOption {
                    cluster_name: "DefaultCluster".into(),
                    broker_names: vec!["broker-b".into()],
                },
            ],
        });

        assert_eq!(
            catalog.targets,
            vec![TopicTargetOptionView {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-a".into(), "broker-b".into()],
            }]
        );
    }

    #[tokio::test]
    async fn create_catalog_collision_runs_no_target_upserts() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".into(),
                category: "NORMAL".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: vec!["broker-a".into()],
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: false,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-a".into()],
            }],
        });
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-a".into()]),
            TopicMutationKind::Create,
            "CREATE",
            &mut executor,
        )
        .await;

        assert!(result.is_err());
        assert!(executor.calls.is_empty());
    }

    #[tokio::test]
    async fn target_upserts_continue_after_a_failure_in_canonical_order() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".into(),
                category: "NORMAL".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: vec!["broker-a".into(), "broker-b".into()],
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: false,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-b".into(), "broker-a".into()],
            }],
        });
        let mut executor = RecordingExecutor {
            failing_target: Some("broker-a".into()),
            ..RecordingExecutor::default()
        };

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-b".into(), "broker-a".into(), "broker-a".into()]),
            TopicMutationKind::Update,
            "UPDATE",
            &mut executor,
        )
        .await
        .expect("per-target failures return a structured result");

        assert_eq!(executor.calls, vec!["broker-a", "broker-b"]);
        assert!(!result.success);
        assert_eq!(result.target_count, 2);
        assert_eq!(result.targets[0].target, "broker-a");
        assert!(!result.targets[0].success);
        assert_eq!(result.targets[1].target, "broker-b");
        assert!(result.targets[1].success);
    }

    #[tokio::test]
    async fn system_topic_update_runs_no_target_upserts() {
        let catalog = map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "TBW102".into(),
                category: "SYSTEM".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: vec!["broker-a".into()],
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: true,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: vec!["broker-a".into()],
            }],
        });
        let mut request = mutation_request(vec!["broker-a".into()]);
        request.topic = "TBW102".into();
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(catalog, request, TopicMutationKind::Update, "UPDATE", &mut executor).await;

        assert!(result.is_err());
        assert!(executor.calls.is_empty());
    }

    #[tokio::test]
    async fn ordered_topic_reconciles_all_successful_brokers_once() {
        let catalog = catalog_with_targets(&["broker-b", "broker-a"]);
        let mut request = mutation_request(vec!["broker-a".into(), "broker-b".into()]);
        request.order = Some(true);
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(catalog, request, TopicMutationKind::Update, "UPDATE", &mut executor)
            .await
            .expect("ordered topic mutation");

        assert!(result.success);
        assert_eq!(executor.batch_requests.len(), 1);
        assert_eq!(executor.batch_requests[0].broker_names(), ["broker-a", "broker-b"]);
        assert!(executor.batch_requests[0].order());
    }

    #[tokio::test]
    async fn unordered_topic_removes_the_global_order_configuration() {
        let catalog = catalog_with_targets(&["broker-a"]);
        let mut executor = RecordingExecutor::default();

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-a".into()]),
            TopicMutationKind::Update,
            "UPDATE",
            &mut executor,
        )
        .await
        .expect("unordered topic mutation");

        assert!(result.success);
        assert_eq!(executor.batch_requests.len(), 1);
        assert!(!executor.batch_requests[0].order());
    }

    #[tokio::test]
    async fn order_reconciliation_failure_is_a_structured_global_failure() {
        let catalog = catalog_with_targets(&["broker-a"]);
        let mut executor = RecordingExecutor {
            reconcile_error: Some(AdminError::backend("order_reconcile", "unavailable")),
            ..RecordingExecutor::default()
        };

        let result = run_topic_upserts(
            catalog,
            mutation_request(vec!["broker-a".into()]),
            TopicMutationKind::Update,
            "UPDATE",
            &mut executor,
        )
        .await
        .expect("broker mutation remains structured");

        assert!(!result.success);
        assert!(
            result.targets.iter().any(|target| {
                target.target == "ORDER_TOPIC_CONFIG" && !target.success && !target.message.is_empty()
            })
        );
    }

    fn mutation_request(broker_name_list: Vec<String>) -> TopicMutationRequest {
        TopicMutationRequest {
            topic: "orders".into(),
            read_queue_count: 8,
            write_queue_count: 8,
            perm: 6,
            broker_name_list,
            cluster_name_list: Vec::new(),
            order: Some(false),
            message_type: Some("NORMAL".into()),
        }
    }

    fn catalog_with_targets(broker_names: &[&str]) -> TopicListView {
        map_topic_catalog(core_topic::TopicCatalog {
            items: vec![core_topic::TopicCatalogItem {
                topic: "orders".into(),
                category: "NORMAL".into(),
                message_type: "NORMAL".into(),
                clusters: vec!["DefaultCluster".into()],
                brokers: broker_names.iter().map(|broker| (*broker).to_string()).collect(),
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                order: false,
                system_topic: false,
            }],
            targets: vec![core_topic::TopicTargetOption {
                cluster_name: "DefaultCluster".into(),
                broker_names: broker_names.iter().map(|broker| (*broker).to_string()).collect(),
            }],
        })
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: Vec<String>,
        failing_target: Option<String>,
        batch_requests: Vec<TopicBatchUpsertRequest>,
        reconcile_error: Option<AdminError>,
    }

    impl TopicUpsertExecutor for RecordingExecutor {
        async fn upsert_batch(
            &mut self,
            request: &TopicBatchUpsertRequest,
        ) -> Result<core_topic::TopicBatchMutationOutcome, AdminError> {
            self.batch_requests.push(request.clone());
            let targets = request
                .broker_names()
                .iter()
                .map(|broker_name| {
                    self.calls.push(broker_name.clone());
                    let success = self.failing_target.as_deref() != Some(broker_name.as_str());
                    core_topic::TopicBatchTargetOutcome {
                        broker_name: broker_name.clone(),
                        success,
                        message: if success { "saved" } else { "unavailable" }.to_string(),
                    }
                })
                .collect::<Vec<_>>();
            let order_config = targets.iter().any(|target| target.success).then(|| {
                let success = self.reconcile_error.is_none();
                core_topic::TopicBatchOrderConfigOutcome {
                    success,
                    message: self
                        .reconcile_error
                        .take()
                        .map_or_else(|| "reconciled".to_string(), |error| error.to_string()),
                }
            });
            Ok(core_topic::TopicBatchMutationOutcome { targets, order_config })
        }
    }
}
