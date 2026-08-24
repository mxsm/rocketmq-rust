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

//! Private Topic application seam and authoritative post-mutation reload rules.

#[cfg(test)]
#[path = "topics_test_support.rs"]
pub(crate) mod test_support;

use std::{fmt, future::Future, sync::Arc};

use rocketmq_admin_core::core::topic::TopicConfigCasState;
use rocketmq_dashboard_common::{
    TopicConfigView, TopicConsumersView, TopicIdentity, TopicInventory, TopicMessageType, TopicPartialOutcome,
    TopicPermission, TopicRouteView, TopicStatsView, TopicTargetIdentity,
};

use crate::{
    infrastructure::admin_provider::{
        GpuiAdminProvider, SafeTopicCreateRequest, SafeTopicDeleteBrokerRequest, SafeTopicDeleteRequest,
        SafeTopicOffsetRequest, SafeTopicPatchOutcome, SafeTopicQueuePatchRequest, SafeTopicSendRequest,
    },
    state::{UiError, UiErrorCode},
};

use super::{AppServices, ServiceFuture};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TopicRequestScope {
    pub revision: u64,
    pub epoch: u64,
}

pub(crate) struct TopicCreateCommand {
    pub topic: TopicIdentity,
    pub targets: Vec<TopicTargetIdentity>,
    pub read_queue_count: u32,
    pub write_queue_count: u32,
    pub permission: TopicPermission,
    pub ordered: bool,
    pub message_type: TopicMessageType,
}

impl fmt::Debug for TopicCreateCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicCreateCommand")
            .field("target_count", &self.targets.len())
            .field("read_queue_count", &self.read_queue_count)
            .field("write_queue_count", &self.write_queue_count)
            .field("ordered", &self.ordered)
            .field("message_type", &self.message_type)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct TopicQueuePatchCommand {
    pub topic: TopicIdentity,
    pub target: TopicTargetIdentity,
    pub expected_version: u64,
    pub read_queue_count: Option<u32>,
    pub write_queue_count: Option<u32>,
}

impl fmt::Debug for TopicQueuePatchCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicQueuePatchCommand")
            .field("expected_version", &self.expected_version)
            .field("read_queue_count", &self.read_queue_count)
            .field("write_queue_count", &self.write_queue_count)
            .finish_non_exhaustive()
    }
}

pub(crate) struct TopicDeleteCommand {
    pub topic: TopicIdentity,
    pub cluster_names: Vec<String>,
}

pub(crate) struct TopicDeleteBrokerCommand {
    pub topic: TopicIdentity,
    pub target: TopicTargetIdentity,
}

pub(crate) struct TopicSendCommand {
    pub topic: TopicIdentity,
    key: String,
    tag: String,
    body: String,
    trace_enabled: bool,
}

impl TopicSendCommand {
    pub(crate) fn new(
        topic: TopicIdentity,
        key: String,
        tag: String,
        body: String,
        trace_enabled: bool,
    ) -> Result<Self, UiError> {
        if body.is_empty() || body.len() > 1024 * 1024 {
            return Err(UiError::new(
                "Message body must contain between 1 byte and 1 MiB.",
                UiErrorCode::Validation,
                false,
            ));
        }
        Ok(Self {
            topic,
            key,
            tag,
            body,
            trace_enabled,
        })
    }

    fn into_safe(mut self) -> SafeTopicSendRequest {
        SafeTopicSendRequest {
            topic: self.topic.clone(),
            key: std::mem::take(&mut self.key),
            tag: std::mem::take(&mut self.tag),
            body: std::mem::take(&mut self.body),
            trace_enabled: self.trace_enabled,
        }
    }
}

impl fmt::Debug for TopicSendCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicSendCommand")
            .field("has_key", &!self.key.is_empty())
            .field("has_tag", &!self.tag.is_empty())
            .field("body_length", &self.body.len())
            .field("trace_enabled", &self.trace_enabled)
            .finish_non_exhaustive()
    }
}

impl Drop for TopicSendCommand {
    fn drop(&mut self) {
        self.body.clear();
    }
}

pub(crate) struct TopicOffsetCommand {
    pub topic: TopicIdentity,
    pub consumer_group: String,
    pub cluster_name: String,
    pub timestamp: Option<u64>,
    pub force: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TopicCacheInvalidation {
    Inventory,
    Overview(TopicIdentity),
    Stats(TopicIdentity),
    Route(TopicIdentity),
    Configuration(TopicIdentity),
    Consumers(TopicIdentity),
}

pub(crate) enum TopicMutationResult {
    Rejected(TopicPartialOutcome),
    Applied {
        outcome: TopicPartialOutcome,
        inventory: Option<TopicInventory>,
        consumers: Option<TopicConsumersView>,
        invalidations: Vec<TopicCacheInvalidation>,
    },
    AppliedReloadFailed {
        outcome: TopicPartialOutcome,
        invalidations: Vec<TopicCacheInvalidation>,
        error: UiError,
    },
}

impl fmt::Debug for TopicMutationResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(outcome) => formatter
                .debug_struct("Rejected")
                .field("kind", &outcome.kind)
                .field("applied_count", &outcome.applied_count())
                .field("failed_count", &outcome.failed_count())
                .finish_non_exhaustive(),
            Self::Applied {
                outcome,
                inventory,
                consumers,
                invalidations,
            } => formatter
                .debug_struct("Applied")
                .field("kind", &outcome.kind)
                .field("applied_count", &outcome.applied_count())
                .field("failed_count", &outcome.failed_count())
                .field("inventory_loaded", &inventory.is_some())
                .field("consumers_loaded", &consumers.is_some())
                .field("invalidation_count", &invalidations.len())
                .finish_non_exhaustive(),
            Self::AppliedReloadFailed {
                outcome,
                invalidations,
                error,
            } => formatter
                .debug_struct("AppliedReloadFailed")
                .field("kind", &outcome.kind)
                .field("applied_count", &outcome.applied_count())
                .field("failed_count", &outcome.failed_count())
                .field("invalidation_count", &invalidations.len())
                .field("retryable", &error.is_retryable())
                .finish_non_exhaustive(),
        }
    }
}

pub(crate) enum TopicQueuePatchResult {
    Applied {
        previous_version: u64,
        version: u64,
        configuration: TopicConfigView,
        inventory: TopicInventory,
        invalidations: Vec<TopicCacheInvalidation>,
    },
    AppliedReloadFailed {
        previous_version: u64,
        version: u64,
        invalidations: Vec<TopicCacheInvalidation>,
        error: UiError,
    },
    VersionConflict {
        expected_version: u64,
        actual_version: u64,
        latest: TopicConfigCasState,
    },
}

impl fmt::Debug for TopicQueuePatchResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Applied {
                previous_version,
                version,
                ..
            } => formatter
                .debug_struct("Applied")
                .field("previous_version", previous_version)
                .field("version", version)
                .finish_non_exhaustive(),
            Self::AppliedReloadFailed {
                previous_version,
                version,
                error,
                ..
            } => formatter
                .debug_struct("AppliedReloadFailed")
                .field("previous_version", previous_version)
                .field("version", version)
                .field("retryable", &error.is_retryable())
                .finish_non_exhaustive(),
            Self::VersionConflict {
                expected_version,
                actual_version,
                ..
            } => formatter
                .debug_struct("VersionConflict")
                .field("expected_version", expected_version)
                .field("actual_version", actual_version)
                .finish_non_exhaustive(),
        }
    }
}

pub(crate) enum BackendTopicQueuePatchResult {
    Applied {
        previous_version: u64,
        version: u64,
    },
    VersionConflict {
        expected_version: u64,
        actual_version: u64,
        latest: TopicConfigCasState,
    },
}

pub(crate) trait TopicBackend: Send + Sync {
    fn inventory(&self, scope: TopicRequestScope) -> ServiceFuture<'_, Result<TopicInventory, UiError>>;
    fn route(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicRouteView, UiError>>;
    fn stats(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicStatsView, UiError>>;
    fn config(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicConfigView, UiError>>;
    fn consumers(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicConsumersView, UiError>>;
    fn create(
        &self,
        scope: TopicRequestScope,
        command: TopicCreateCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>>;
    fn patch_queue_counts(
        &self,
        scope: TopicRequestScope,
        command: TopicQueuePatchCommand,
    ) -> ServiceFuture<'_, Result<BackendTopicQueuePatchResult, UiError>>;
    fn delete(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>>;
    fn delete_broker(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteBrokerCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>>;
    fn send(&self, scope: TopicRequestScope, command: TopicSendCommand) -> ServiceFuture<'_, Result<(), UiError>>;
    fn reset(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>>;
    fn skip(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>>;
}

pub(super) struct RealTopicBackend {
    provider: Option<Arc<GpuiAdminProvider>>,
}

impl RealTopicBackend {
    pub(super) fn new(provider: Arc<GpuiAdminProvider>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
        })
    }

    pub(super) fn unavailable() -> Arc<Self> {
        Arc::new(Self { provider: None })
    }

    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(|| {
            UiError::new(
                "Topic operations are unavailable in this application configuration.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })
    }
}

impl TopicBackend for RealTopicBackend {
    fn inventory(&self, scope: TopicRequestScope) -> ServiceFuture<'_, Result<TopicInventory, UiError>> {
        Box::pin(async move {
            self.provider()?
                .topic_inventory(scope.revision)
                .await
                .map_err(query_error)
        })
    }

    fn route(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicRouteView, UiError>> {
        Box::pin(async move {
            self.provider()?
                .topic_route(scope.revision, topic)
                .await
                .map_err(query_error)
        })
    }

    fn stats(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicStatsView, UiError>> {
        Box::pin(async move {
            self.provider()?
                .detailed_topic_stats(scope.revision, topic)
                .await
                .map_err(query_error)
        })
    }

    fn config(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicConfigView, UiError>> {
        Box::pin(async move {
            self.provider()?
                .detailed_topic_config(scope.revision, topic)
                .await
                .map_err(query_error)
        })
    }

    fn consumers(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> ServiceFuture<'_, Result<TopicConsumersView, UiError>> {
        Box::pin(async move {
            self.provider()?
                .detailed_topic_consumers(scope.revision, topic)
                .await
                .map_err(query_error)
        })
    }

    fn create(
        &self,
        scope: TopicRequestScope,
        command: TopicCreateCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .create_topic(
                    scope.revision,
                    SafeTopicCreateRequest {
                        topic: command.topic,
                        targets: command.targets,
                        read_queue_count: command.read_queue_count,
                        write_queue_count: command.write_queue_count,
                        permission: command.permission,
                        ordered: command.ordered,
                        message_type: command.message_type,
                    },
                )
                .await
                .map_err(mutation_error)
        })
    }

    fn patch_queue_counts(
        &self,
        scope: TopicRequestScope,
        command: TopicQueuePatchCommand,
    ) -> ServiceFuture<'_, Result<BackendTopicQueuePatchResult, UiError>> {
        Box::pin(async move {
            let topic = command.topic.clone();
            let target = command.target.clone();
            let result = self
                .provider()?
                .patch_topic_queue_counts(
                    scope.revision,
                    SafeTopicQueuePatchRequest {
                        topic,
                        target: target.clone(),
                        expected_version: command.expected_version,
                        read_queue_count: command.read_queue_count,
                        write_queue_count: command.write_queue_count,
                    },
                )
                .await
                .map_err(mutation_error)?;
            Ok(match result {
                SafeTopicPatchOutcome::Applied {
                    previous_version,
                    version,
                } => BackendTopicQueuePatchResult::Applied {
                    previous_version,
                    version,
                },
                SafeTopicPatchOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } => {
                    let latest = self
                        .provider()?
                        .topic_config_preflight(scope.revision, command.topic, target)
                        .await
                        .map_err(query_error)?;
                    BackendTopicQueuePatchResult::VersionConflict {
                        expected_version,
                        actual_version,
                        latest,
                    }
                }
            })
        })
    }

    fn delete(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .delete_topic(
                    scope.revision,
                    SafeTopicDeleteRequest {
                        topic: command.topic,
                        cluster_names: command.cluster_names,
                    },
                )
                .await
                .map_err(mutation_error)
        })
    }

    fn delete_broker(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteBrokerCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .delete_topic_from_broker(
                    scope.revision,
                    SafeTopicDeleteBrokerRequest {
                        topic: command.topic,
                        target: command.target,
                    },
                )
                .await
                .map_err(mutation_error)
        })
    }

    fn send(&self, scope: TopicRequestScope, command: TopicSendCommand) -> ServiceFuture<'_, Result<(), UiError>> {
        Box::pin(async move {
            let receipt = self
                .provider()?
                .send_topic_message(scope.revision, command.into_safe())
                .await
                .map_err(mutation_error)?;
            if receipt.delivered {
                Ok(())
            } else {
                Err(mutation_error("message delivery was not acknowledged"))
            }
        })
    }

    fn reset(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .reset_topic_offset(scope.revision, offset_request(command))
                .await
                .map_err(mutation_error)
        })
    }

    fn skip(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> ServiceFuture<'_, Result<TopicPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .skip_topic_accumulated(scope.revision, offset_request(command))
                .await
                .map_err(mutation_error)
        })
    }
}

impl AppServices {
    #[cfg(test)]
    pub(crate) fn with_topic_backend(mut self, backend: Arc<dyn TopicBackend>) -> Self {
        self.topics = backend;
        self
    }

    pub async fn topic_inventory(&self, scope: TopicRequestScope) -> Result<TopicInventory, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic(
            "gpui-service-topic-inventory",
            async move { backend.inventory(scope).await },
        )
        .await
    }

    pub async fn topic_route(&self, scope: TopicRequestScope, topic: TopicIdentity) -> Result<TopicRouteView, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic(
            "gpui-service-topic-route",
            async move { backend.route(scope, topic).await },
        )
        .await
    }

    pub async fn topic_stats(&self, scope: TopicRequestScope, topic: TopicIdentity) -> Result<TopicStatsView, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic(
            "gpui-service-topic-stats",
            async move { backend.stats(scope, topic).await },
        )
        .await
    }

    pub async fn topic_config(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> Result<TopicConfigView, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic("gpui-service-topic-config", async move {
            backend.config(scope, topic).await
        })
        .await
    }

    pub async fn topic_consumers(
        &self,
        scope: TopicRequestScope,
        topic: TopicIdentity,
    ) -> Result<TopicConsumersView, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic("gpui-service-topic-consumers", async move {
            backend.consumers(scope, topic).await
        })
        .await
    }

    pub async fn create_topic(
        &self,
        scope: TopicRequestScope,
        command: TopicCreateCommand,
    ) -> Result<TopicMutationResult, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic_mutation("gpui-service-topic-create", backend, scope, async move |backend| {
            backend.create(scope, command).await
        })
        .await
    }

    pub async fn delete_topic(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteCommand,
    ) -> Result<TopicMutationResult, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic_mutation("gpui-service-topic-delete", backend, scope, async move |backend| {
            backend.delete(scope, command).await
        })
        .await
    }

    pub async fn delete_topic_from_broker(
        &self,
        scope: TopicRequestScope,
        command: TopicDeleteBrokerCommand,
    ) -> Result<TopicMutationResult, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic_mutation(
            "gpui-service-topic-delete-broker",
            backend,
            scope,
            async move |backend| backend.delete_broker(scope, command).await,
        )
        .await
    }

    pub async fn patch_topic_queue_counts(
        &self,
        scope: TopicRequestScope,
        command: TopicQueuePatchCommand,
    ) -> Result<TopicQueuePatchResult, UiError> {
        let backend = Arc::clone(&self.topics);
        let topic = command.topic.clone();
        let invalidations = mutation_invalidations(rocketmq_dashboard_common::TopicMutationKind::Edit, topic.clone());
        self.run_topic("gpui-service-topic-edit", async move {
            match backend.patch_queue_counts(scope, command).await? {
                BackendTopicQueuePatchResult::Applied {
                    previous_version,
                    version,
                } => {
                    let configuration = backend.config(scope, topic).await;
                    let inventory = backend.inventory(scope).await;
                    match (configuration, inventory) {
                        (Ok(configuration), Ok(inventory)) => Ok(TopicQueuePatchResult::Applied {
                            previous_version,
                            version,
                            configuration,
                            inventory,
                            invalidations,
                        }),
                        (Err(error), _) | (_, Err(error)) => Ok(TopicQueuePatchResult::AppliedReloadFailed {
                            previous_version,
                            version,
                            invalidations,
                            error,
                        }),
                    }
                }
                BackendTopicQueuePatchResult::VersionConflict {
                    expected_version,
                    actual_version,
                    latest,
                } => Ok(TopicQueuePatchResult::VersionConflict {
                    expected_version,
                    actual_version,
                    latest,
                }),
            }
        })
        .await
    }

    pub async fn send_topic_message(&self, scope: TopicRequestScope, command: TopicSendCommand) -> Result<(), UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic(
            "gpui-service-topic-send",
            async move { backend.send(scope, command).await },
        )
        .await
    }

    pub async fn reset_topic_offset(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> Result<TopicMutationResult, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic_mutation("gpui-service-topic-reset", backend, scope, async move |backend| {
            backend.reset(scope, command).await
        })
        .await
    }

    pub async fn skip_topic_accumulated(
        &self,
        scope: TopicRequestScope,
        command: TopicOffsetCommand,
    ) -> Result<TopicMutationResult, UiError> {
        let backend = Arc::clone(&self.topics);
        self.run_topic_mutation("gpui-service-topic-skip", backend, scope, async move |backend| {
            backend.skip(scope, command).await
        })
        .await
    }

    async fn run_topic<T>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<T, UiError>> + Send + 'static,
    ) -> Result<T, UiError>
    where
        T: Send + 'static,
    {
        match &self.runtime_bridge {
            Some(runtime) => runtime.run(name, future).await,
            None => future.await,
        }
    }

    async fn run_topic_mutation<F, Fut>(
        &self,
        name: &'static str,
        backend: Arc<dyn TopicBackend>,
        scope: TopicRequestScope,
        mutate: F,
    ) -> Result<TopicMutationResult, UiError>
    where
        F: FnOnce(Arc<dyn TopicBackend>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<TopicPartialOutcome, UiError>> + Send + 'static,
    {
        self.run_topic(name, async move {
            let outcome = mutate(Arc::clone(&backend)).await?;
            if outcome.applied_count() == 0 {
                return Ok(TopicMutationResult::Rejected(outcome));
            }
            let topic = outcome.topic.clone();
            let kind = outcome.kind;
            let invalidations = mutation_invalidations(kind, topic.clone());
            let reload = if matches!(
                kind,
                rocketmq_dashboard_common::TopicMutationKind::ResetOffset
                    | rocketmq_dashboard_common::TopicMutationKind::SkipBacklog
            ) {
                backend
                    .consumers(scope, topic)
                    .await
                    .map(|consumers| (None, Some(consumers)))
            } else {
                backend.inventory(scope).await.map(|inventory| (Some(inventory), None))
            };
            match reload {
                Ok((inventory, consumers)) => Ok(TopicMutationResult::Applied {
                    outcome,
                    inventory,
                    consumers,
                    invalidations,
                }),
                Err(error) => {
                    let mut outcome = outcome;
                    outcome.reload_failed = true;
                    Ok(TopicMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error,
                    })
                }
            }
        })
        .await
    }
}

pub(crate) fn mutation_invalidations(
    kind: rocketmq_dashboard_common::TopicMutationKind,
    topic: TopicIdentity,
) -> Vec<TopicCacheInvalidation> {
    use rocketmq_dashboard_common::TopicMutationKind;

    match kind {
        TopicMutationKind::Create => vec![TopicCacheInvalidation::Inventory],
        TopicMutationKind::Edit => vec![
            TopicCacheInvalidation::Inventory,
            TopicCacheInvalidation::Overview(topic.clone()),
            TopicCacheInvalidation::Stats(topic.clone()),
            TopicCacheInvalidation::Route(topic.clone()),
            TopicCacheInvalidation::Configuration(topic),
        ],
        TopicMutationKind::DeleteTopic => vec![
            TopicCacheInvalidation::Inventory,
            TopicCacheInvalidation::Overview(topic.clone()),
            TopicCacheInvalidation::Stats(topic.clone()),
            TopicCacheInvalidation::Route(topic.clone()),
            TopicCacheInvalidation::Configuration(topic.clone()),
            TopicCacheInvalidation::Consumers(topic),
        ],
        TopicMutationKind::DeleteBroker => vec![
            TopicCacheInvalidation::Inventory,
            TopicCacheInvalidation::Overview(topic.clone()),
            TopicCacheInvalidation::Stats(topic.clone()),
            TopicCacheInvalidation::Route(topic.clone()),
            TopicCacheInvalidation::Configuration(topic),
        ],
        TopicMutationKind::ResetOffset | TopicMutationKind::SkipBacklog => {
            vec![TopicCacheInvalidation::Consumers(topic)]
        }
        TopicMutationKind::Send => Vec::new(),
    }
}

fn offset_request(command: TopicOffsetCommand) -> SafeTopicOffsetRequest {
    SafeTopicOffsetRequest {
        topic: command.topic,
        consumer_group: command.consumer_group,
        cluster_name: command.cluster_name,
        timestamp: command.timestamp,
        force: command.force,
    }
}

fn query_error(_error: impl fmt::Display) -> UiError {
    UiError::new(
        "Unable to load Topic data from the selected connection.",
        UiErrorCode::Connection,
        true,
    )
}

fn mutation_error(_error: impl fmt::Display) -> UiError {
    UiError::new("Unable to apply the Topic operation.", UiErrorCode::Connection, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_body_is_absent_from_debug_and_cleared_by_owned_command() {
        let command = TopicSendCommand::new(
            TopicIdentity::parse("orders").expect("topic"),
            String::new(),
            String::new(),
            "private-message-body".into(),
            false,
        )
        .expect("command");
        assert!(!format!("{command:?}").contains("private-message-body"));
    }

    #[test]
    fn every_mutation_kind_has_the_exact_targeted_invalidation_set() {
        use rocketmq_dashboard_common::TopicMutationKind;

        let topic = TopicIdentity::parse("orders").expect("topic");
        let inventory = TopicCacheInvalidation::Inventory;
        let overview = TopicCacheInvalidation::Overview(topic.clone());
        let stats = TopicCacheInvalidation::Stats(topic.clone());
        let route = TopicCacheInvalidation::Route(topic.clone());
        let configuration = TopicCacheInvalidation::Configuration(topic.clone());
        let consumers = TopicCacheInvalidation::Consumers(topic.clone());

        assert_eq!(
            mutation_invalidations(TopicMutationKind::Create, topic.clone()),
            vec![inventory.clone()]
        );
        assert_eq!(
            mutation_invalidations(TopicMutationKind::Edit, topic.clone()),
            vec![
                inventory.clone(),
                overview.clone(),
                stats.clone(),
                route.clone(),
                configuration.clone(),
            ]
        );
        assert_eq!(
            mutation_invalidations(TopicMutationKind::DeleteTopic, topic.clone()),
            vec![
                inventory.clone(),
                overview.clone(),
                stats.clone(),
                route.clone(),
                configuration.clone(),
                consumers.clone(),
            ]
        );
        assert_eq!(
            mutation_invalidations(TopicMutationKind::DeleteBroker, topic.clone()),
            vec![inventory, overview, stats, route, configuration,]
        );
        assert_eq!(
            mutation_invalidations(TopicMutationKind::ResetOffset, topic.clone()),
            vec![consumers.clone()]
        );
        assert_eq!(
            mutation_invalidations(TopicMutationKind::SkipBacklog, topic.clone()),
            vec![consumers]
        );
        assert!(mutation_invalidations(TopicMutationKind::Send, topic).is_empty());
    }
}
