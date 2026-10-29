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

//! Consumer and Producer workspace service boundary.

#[cfg(test)]
#[path = "consumers_test_support.rs"]
pub(crate) mod test_support;

use std::{future::Future, sync::Arc};

use rocketmq_dashboard_common::{
    AuditAction, AuditConsumerField, ConsumerClients, ConsumerConfigPatchCommand, ConsumerConfigPatchOutcome,
    ConsumerConfiguration, ConsumerCreateCommand, ConsumerDeleteCommand, ConsumerDiagnosticPayload,
    ConsumerDiagnosticRequest, ConsumerIdentity, ConsumerInventory, ConsumerObservation, ConsumerObservationState,
    ConsumerPartialOutcome, ConsumerProgress, OperationResource, ProducerConnectionQuery, ProducerConnections,
    ProducerInventory,
};

use super::{AppServices, ServiceFuture};
use crate::{
    infrastructure::admin_provider::GpuiAdminProvider,
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerRequestScope {
    pub revision: u64,
    pub epoch: u64,
    pub query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumerCacheInvalidation {
    Inventory,
    Overview(ConsumerIdentity),
    Progress(ConsumerIdentity),
    Dashboard,
    TopicConsumers,
}

pub enum ConsumerMutationResult {
    Rejected(ConsumerPartialOutcome),
    Applied {
        outcome: ConsumerPartialOutcome,
        inventory: ConsumerInventory,
        invalidations: Vec<ConsumerCacheInvalidation>,
    },
    AppliedReloadFailed {
        outcome: ConsumerPartialOutcome,
        invalidations: Vec<ConsumerCacheInvalidation>,
        error: UiError,
    },
}

pub enum ConsumerConfigMutationResult {
    Applied {
        previous_generation: u64,
        generation: u64,
        configuration: ConsumerConfiguration,
        inventory: ConsumerInventory,
        invalidations: Vec<ConsumerCacheInvalidation>,
    },
    AppliedReloadFailed {
        previous_generation: u64,
        generation: u64,
        invalidations: Vec<ConsumerCacheInvalidation>,
        error: UiError,
    },
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
    PersistenceIncomplete {
        /// Whether this command was confirmed applied in memory, independently of durability.
        applied: bool,
        previous_generation: u64,
        generation: u64,
        persistence: rocketmq_dashboard_common::ConsumerConfigPersistence,
        invalidations: Vec<ConsumerCacheInvalidation>,
    },
}

pub(crate) trait ConsumerBackend: Send + Sync {
    fn inventory(&self, scope: ConsumerRequestScope) -> ServiceFuture<'_, Result<ConsumerInventory, UiError>>;
    fn clients(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ConsumerClients>, UiError>>;
    fn progress(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ConsumerProgress>, UiError>>;
    fn configuration(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerConfiguration, UiError>>;
    fn diagnostic(
        &self,
        scope: ConsumerRequestScope,
        request: ConsumerDiagnosticRequest,
    ) -> ServiceFuture<'_, Result<ConsumerDiagnosticPayload, UiError>>;
    fn producer_inventory(&self, scope: ConsumerRequestScope) -> ServiceFuture<'_, Result<ProducerInventory, UiError>>;
    fn producer_connections(
        &self,
        scope: ConsumerRequestScope,
        query: ProducerConnectionQuery,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ProducerConnections>, UiError>>;
    fn create(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerCreateCommand,
    ) -> ServiceFuture<'_, Result<ConsumerPartialOutcome, UiError>>;
    fn patch_configuration(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerConfigPatchCommand,
    ) -> ServiceFuture<'_, Result<ConsumerConfigPatchOutcome, UiError>>;
    fn delete(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerDeleteCommand,
    ) -> ServiceFuture<'_, Result<ConsumerPartialOutcome, UiError>>;
}

pub(super) struct RealConsumerBackend {
    provider: Option<Arc<GpuiAdminProvider>>,
}

impl RealConsumerBackend {
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
                "Consumer operations are unavailable in this application configuration.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })
    }
}

impl ConsumerBackend for RealConsumerBackend {
    fn inventory(&self, scope: ConsumerRequestScope) -> ServiceFuture<'_, Result<ConsumerInventory, UiError>> {
        Box::pin(async move {
            self.provider()?
                .consumer_inventory(scope.revision, scope.query_scope)
                .await
                .map_err(query_error)
        })
    }

    fn clients(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ConsumerClients>, UiError>> {
        Box::pin(async move {
            self.provider()?
                .consumer_clients(scope.revision, scope.query_scope, group)
                .await
                .map_err(query_error)
        })
    }

    fn progress(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ConsumerProgress>, UiError>> {
        Box::pin(async move {
            self.provider()?
                .consumer_progress(scope.revision, scope.query_scope, group)
                .await
                .map_err(query_error)
        })
    }

    fn configuration(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerConfiguration, UiError>> {
        Box::pin(async move {
            self.provider()?
                .consumer_configuration(scope.revision, group)
                .await
                .map_err(query_error)
        })
    }

    fn diagnostic(
        &self,
        scope: ConsumerRequestScope,
        request: ConsumerDiagnosticRequest,
    ) -> ServiceFuture<'_, Result<ConsumerDiagnosticPayload, UiError>> {
        Box::pin(async move {
            self.provider()?
                .consumer_diagnostic(scope.revision, request)
                .await
                .map_err(query_error)
        })
    }

    fn producer_inventory(&self, scope: ConsumerRequestScope) -> ServiceFuture<'_, Result<ProducerInventory, UiError>> {
        Box::pin(async move {
            self.provider()?
                .producer_inventory(scope.revision)
                .await
                .map_err(query_error)
        })
    }

    fn producer_connections(
        &self,
        scope: ConsumerRequestScope,
        query: ProducerConnectionQuery,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ProducerConnections>, UiError>> {
        Box::pin(async move {
            self.provider()?
                .producer_connections(scope.revision, query)
                .await
                .map_err(query_error)
        })
    }

    fn create(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerCreateCommand,
    ) -> ServiceFuture<'_, Result<ConsumerPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .create_consumer_group(scope.revision, command)
                .await
                .map_err(mutation_error)
        })
    }

    fn patch_configuration(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerConfigPatchCommand,
    ) -> ServiceFuture<'_, Result<ConsumerConfigPatchOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .patch_consumer_configuration(scope.revision, command)
                .await
                .map_err(mutation_error)
        })
    }

    fn delete(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerDeleteCommand,
    ) -> ServiceFuture<'_, Result<ConsumerPartialOutcome, UiError>> {
        Box::pin(async move {
            self.provider()?
                .delete_consumer_group(scope.revision, command)
                .await
                .map_err(mutation_error)
        })
    }
}

impl AppServices {
    #[cfg(test)]
    pub(crate) fn with_consumer_backend(mut self, backend: Arc<dyn ConsumerBackend>) -> Self {
        self.consumers = backend;
        self
    }

    pub async fn consumer_inventory(&self, scope: ConsumerRequestScope) -> Result<ConsumerInventory, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-consumer-inventory", async move {
            backend.inventory(scope).await
        })
        .await
    }

    pub async fn consumer_clients(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> Result<ConsumerObservation<ConsumerClients>, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-consumer-clients", async move {
            backend.clients(scope, group).await
        })
        .await
    }

    pub async fn consumer_progress(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> Result<ConsumerObservation<ConsumerProgress>, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-consumer-progress", async move {
            backend.progress(scope, group).await
        })
        .await
    }

    pub async fn consumer_configuration(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> Result<ConsumerConfiguration, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-consumer-configuration", async move {
            backend.configuration(scope, group).await
        })
        .await
    }

    pub async fn consumer_diagnostic(
        &self,
        scope: ConsumerRequestScope,
        request: ConsumerDiagnosticRequest,
    ) -> Result<ConsumerDiagnosticPayload, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-consumer-diagnostic", async move {
            backend.diagnostic(scope, request).await
        })
        .await
    }

    pub async fn producer_inventory(&self, scope: ConsumerRequestScope) -> Result<ProducerInventory, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-producer-inventory", async move {
            backend.producer_inventory(scope).await
        })
        .await
    }

    pub async fn producer_connections(
        &self,
        scope: ConsumerRequestScope,
        query: ProducerConnectionQuery,
    ) -> Result<ConsumerObservation<ProducerConnections>, UiError> {
        let backend = Arc::clone(&self.consumers);
        self.run_consumer("gpui-service-producer-connections", async move {
            backend.producer_connections(scope, query).await
        })
        .await
    }

    pub async fn create_consumer(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerCreateCommand,
    ) -> Result<ConsumerMutationResult, UiError> {
        let backend = Arc::clone(&self.consumers);
        let group = command.group.clone();
        let mut operation = self.accept_operation(
            scope.revision,
            AuditAction::ConsumerCreated,
            OperationResource::ConsumerGroup(group.as_str().to_owned()),
        )?;
        if let Some(operation) = &mut operation {
            operation.consumer_fields = [
                AuditConsumerField::RetryMaxTimes,
                AuditConsumerField::RetryQueueNums,
                AuditConsumerField::ConsumeTimeoutMinutes,
                AuditConsumerField::ConsumeEnable,
                AuditConsumerField::ConsumeFromMinEnable,
                AuditConsumerField::ConsumeBroadcastEnable,
                AuditConsumerField::ConsumeMessageOrderly,
                AuditConsumerField::BrokerId,
                AuditConsumerField::WhichBrokerWhenConsumeSlowly,
            ]
            .into_iter()
            .collect();
        }
        self.run_mutation("gpui-service-consumer-create", operation, async move {
            let mut outcome = backend.create(scope, command).await?;
            if !outcome.has_applied_effects() {
                return Ok(ConsumerMutationResult::Rejected(outcome));
            }
            let invalidations = consumer_invalidations(group);
            match backend.inventory(scope).await {
                Ok(inventory) if inventory.group_observation == ConsumerObservationState::Complete => {
                    Ok(ConsumerMutationResult::Applied {
                        outcome,
                        inventory,
                        invalidations,
                    })
                }
                Ok(_) => {
                    outcome.reload_failed = true;
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error: incomplete_reload_error(),
                    })
                }
                Err(error) => {
                    outcome.reload_failed = true;
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error,
                    })
                }
            }
        })
        .await
    }

    pub async fn patch_consumer_config(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerConfigPatchCommand,
    ) -> Result<ConsumerConfigMutationResult, UiError> {
        let backend = Arc::clone(&self.consumers);
        let group = command.snapshot.identity.group.clone();
        let mut operation = self.accept_operation(
            scope.revision,
            AuditAction::ConsumerUpdated,
            OperationResource::ConsumerGroup(group.as_str().to_owned()),
        )?;
        if let Some(operation) = &mut operation {
            operation.consumer_fields = [
                (
                    command.patch.retry_max_times.is_some(),
                    AuditConsumerField::RetryMaxTimes,
                ),
                (
                    command.patch.retry_queue_nums.is_some(),
                    AuditConsumerField::RetryQueueNums,
                ),
                (
                    command.patch.consume_timeout_minutes.is_some(),
                    AuditConsumerField::ConsumeTimeoutMinutes,
                ),
                (
                    command.patch.consume_enable.is_some(),
                    AuditConsumerField::ConsumeEnable,
                ),
                (
                    command.patch.consume_from_min_enable.is_some(),
                    AuditConsumerField::ConsumeFromMinEnable,
                ),
                (
                    command.patch.consume_broadcast_enable.is_some(),
                    AuditConsumerField::ConsumeBroadcastEnable,
                ),
                (
                    command.patch.consume_message_orderly.is_some(),
                    AuditConsumerField::ConsumeMessageOrderly,
                ),
                (command.patch.broker_id.is_some(), AuditConsumerField::BrokerId),
                (
                    command.patch.which_broker_when_consume_slowly.is_some(),
                    AuditConsumerField::WhichBrokerWhenConsumeSlowly,
                ),
            ]
            .into_iter()
            .filter_map(|(present, field)| present.then_some(field))
            .collect();
        }
        self.run_mutation("gpui-service-consumer-config-patch", operation, async move {
            match backend.patch_configuration(scope, command).await? {
                ConsumerConfigPatchOutcome::PersistenceIncomplete {
                    applied,
                    previous_generation,
                    generation,
                    persistence,
                } => Ok(ConsumerConfigMutationResult::PersistenceIncomplete {
                    applied,
                    previous_generation,
                    generation,
                    persistence,
                    invalidations: consumer_invalidations(group),
                }),
                ConsumerConfigPatchOutcome::GenerationConflict {
                    expected_generation,
                    actual_generation,
                } => Ok(ConsumerConfigMutationResult::GenerationConflict {
                    expected_generation,
                    actual_generation,
                }),
                ConsumerConfigPatchOutcome::Applied {
                    previous_generation,
                    generation,
                } => {
                    let invalidations = consumer_invalidations(group.clone());
                    let reload = match backend.configuration(scope, group).await {
                        Ok(configuration) if configuration.observation == ConsumerObservationState::Complete => {
                            backend.inventory(scope).await.and_then(|inventory| {
                                (inventory.group_observation == ConsumerObservationState::Complete)
                                    .then_some((configuration, inventory))
                                    .ok_or_else(incomplete_reload_error)
                            })
                        }
                        Ok(_) => Err(incomplete_reload_error()),
                        Err(error) => Err(error),
                    };
                    match reload {
                        Ok((configuration, inventory)) => Ok(ConsumerConfigMutationResult::Applied {
                            previous_generation,
                            generation,
                            configuration,
                            inventory,
                            invalidations,
                        }),
                        Err(error) => Ok(ConsumerConfigMutationResult::AppliedReloadFailed {
                            previous_generation,
                            generation,
                            invalidations,
                            error,
                        }),
                    }
                }
            }
        })
        .await
    }

    pub async fn delete_consumer(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerDeleteCommand,
    ) -> Result<ConsumerMutationResult, UiError> {
        let backend = Arc::clone(&self.consumers);
        let group = command.group.clone();
        let operation = self.accept_operation(
            scope.revision,
            AuditAction::ConsumerDeleted,
            OperationResource::ConsumerGroup(group.as_str().to_owned()),
        )?;
        self.run_mutation("gpui-service-consumer-delete", operation, async move {
            let mut outcome = backend.delete(scope, command).await?;
            if !outcome.has_applied_effects() {
                return Ok(ConsumerMutationResult::Rejected(outcome));
            }
            let invalidations = consumer_invalidations(group);
            match backend.inventory(scope).await {
                Ok(inventory) if inventory.group_observation == ConsumerObservationState::Complete => {
                    Ok(ConsumerMutationResult::Applied {
                        outcome,
                        inventory,
                        invalidations,
                    })
                }
                Ok(_) => {
                    outcome.reload_failed = true;
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error: incomplete_reload_error(),
                    })
                }
                Err(error) => {
                    outcome.reload_failed = true;
                    Ok(ConsumerMutationResult::AppliedReloadFailed {
                        outcome,
                        invalidations,
                        error,
                    })
                }
            }
        })
        .await
    }

    async fn run_consumer<T>(
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
}

fn query_error(source: crate::infrastructure::admin_provider::ProviderFailure) -> UiError {
    provider_failure(source, "Unable to load Consumer data from the selected connection.")
}

fn mutation_error(source: crate::infrastructure::admin_provider::ProviderFailure) -> UiError {
    provider_failure(source, "Unable to apply the exact-target Consumer operation.")
}

fn provider_failure(source: crate::infrastructure::admin_provider::ProviderFailure, summary: &'static str) -> UiError {
    let retryable = source.is_retryable();
    match source.into_operational() {
        Some(source) => UiError::caused_by(summary, UiErrorCode::Connection, retryable, source),
        None => UiError::new(summary, UiErrorCode::Connection, retryable),
    }
}

fn incomplete_reload_error() -> UiError {
    UiError::new(
        "The mutation was applied, but the authoritative Consumer reload was incomplete.",
        UiErrorCode::Connection,
        true,
    )
}

fn consumer_invalidations(group: ConsumerIdentity) -> Vec<ConsumerCacheInvalidation> {
    vec![
        ConsumerCacheInvalidation::Inventory,
        ConsumerCacheInvalidation::Overview(group.clone()),
        ConsumerCacheInvalidation::Progress(group),
        ConsumerCacheInvalidation::Dashboard,
        ConsumerCacheInvalidation::TopicConsumers,
    ]
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::pin,
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
    };

    use rocketmq_dashboard_common::{
        ConnectionScope, ConsumerAclClassification, ConsumerCapabilities, ConsumerConfigEntries,
        ConsumerConfigIdentity, ConsumerConfigPatch, ConsumerConfigSnapshot, ConsumerFailureCode, ConsumerFailureStage,
        ConsumerMutationGuarantee, ConsumerMutationKind, ConsumerTargetIdentity, ConsumerTargetOutcome,
    };

    use super::{
        AppServices, ConsumerConfigMutationResult, ConsumerMutationResult, ConsumerRequestScope,
        test_support::FakeConsumerBackend,
    };

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn ready<T>(future: impl Future<Output = T>) -> T {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("deterministic Consumer service future unexpectedly pending"),
        }
    }

    fn group() -> rocketmq_dashboard_common::ConsumerIdentity {
        rocketmq_dashboard_common::ConsumerIdentity::parse("orders-consumer").expect("group")
    }

    fn target() -> ConsumerTargetIdentity {
        ConsumerTargetIdentity::parse("cluster-a", "broker-a", "10.0.0.1:10911").expect("target")
    }

    fn outcome(kind: ConsumerMutationKind) -> rocketmq_dashboard_common::ConsumerPartialOutcome {
        rocketmq_dashboard_common::ConsumerPartialOutcome {
            group: group(),
            kind,
            guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
            targets: vec![ConsumerTargetOutcome {
                target: "cluster-a/broker-a/10.0.0.1:10911".into(),
                stage: ConsumerFailureStage::Mutation,
                applied: true,
                failure: None,
                retryable: false,
            }],
            reload_failed: false,
        }
    }

    fn incomplete_inventory(
        observation: rocketmq_dashboard_common::ConsumerObservationState,
    ) -> rocketmq_dashboard_common::ConsumerInventory {
        rocketmq_dashboard_common::ConsumerInventory {
            groups: Vec::new(),
            targets: vec![target()],
            target_observation: rocketmq_dashboard_common::ConsumerObservationState::Complete,
            group_observation: observation,
            observation,
            failures: vec![rocketmq_dashboard_common::ConsumerTargetFailure {
                target: "broker-a".into(),
                stage: ConsumerFailureStage::Reload,
                code: ConsumerFailureCode::Unavailable,
                retryable: true,
            }],
            capabilities: ConsumerCapabilities::for_scope(ConnectionScope::NameServer),
        }
    }

    #[test]
    fn consumer_mutations_reload_complete_group_catalog_without_hiding_partial_diagnostics() {
        use rocketmq_dashboard_common::{
            ConsumerCategory, ConsumerGroupObservation, ConsumerObservation, ConsumerObservationState,
            ConsumerUnknownReason,
        };
        let row = ConsumerGroupObservation {
            identity: group(),
            category: ConsumerCategory::Application,
            connection_state: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
            client_count: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
            lag: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
            consume_type: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
            message_model: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::Unavailable,
            },
            targets: vec![target()],
        };
        let mut unrelated = row.clone();
        unrelated.identity = rocketmq_dashboard_common::ConsumerIdentity::parse("unrelated-offline-group").unwrap();
        let mut catalog = incomplete_inventory(ConsumerObservationState::Partial);
        catalog.groups = vec![row, unrelated];
        catalog.group_observation = ConsumerObservationState::Complete;
        catalog.failures = [ConsumerFailureStage::Clients, ConsumerFailureStage::Progress]
            .into_iter()
            .map(|stage| rocketmq_dashboard_common::ConsumerTargetFailure {
                target: "unrelated-offline-group".into(),
                stage,
                code: ConsumerFailureCode::Unavailable,
                retryable: true,
            })
            .collect();
        let snapshot = ConsumerConfigSnapshot {
            identity: ConsumerConfigIdentity {
                group: group(),
                target: target(),
            },
            generation: 7,
            entries: ConsumerConfigEntries::default(),
            read_only: Default::default(),
        };
        let fake = Arc::new(FakeConsumerBackend::default());
        fake.queue_create(Ok(outcome(ConsumerMutationKind::Create)));
        fake.queue_inventory(Ok(catalog.clone()));
        fake.queue_patch(Ok(rocketmq_dashboard_common::ConsumerConfigPatchOutcome::Applied {
            previous_generation: 7,
            generation: 8,
        }));
        let mut updated = snapshot.clone();
        updated.generation = 8;
        updated.entries.retry_max_times = 2;
        fake.queue_configuration(Ok(rocketmq_dashboard_common::ConsumerConfiguration {
            group: group(),
            snapshots: vec![updated],
            observation: ConsumerObservationState::Complete,
            failures: Vec::new(),
        }));
        fake.queue_inventory(Ok(catalog.clone()));
        catalog.groups.retain(|row| row.identity != group());
        fake.queue_delete(Ok(outcome(ConsumerMutationKind::Delete)));
        fake.queue_inventory(Ok(catalog));
        let services = AppServices::default().with_consumer_backend(fake.clone());
        let scope = ConsumerRequestScope {
            revision: 1,
            epoch: 1,
            query_scope: Default::default(),
        };
        let assert_partial = |inventory: &rocketmq_dashboard_common::ConsumerInventory| {
            assert_eq!(inventory.group_observation, ConsumerObservationState::Complete);
            assert_eq!(inventory.observation, ConsumerObservationState::Partial);
            assert_eq!(inventory.failures.len(), 2);
            assert!(
                inventory
                    .groups
                    .iter()
                    .all(|row| matches!(row.client_count, ConsumerObservation::Unknown { .. })
                        && matches!(row.lag, ConsumerObservation::Unknown { .. }))
            );
        };
        let create = ready(services.create_consumer(
            scope,
            rocketmq_dashboard_common::ConsumerCreateCommand {
                group: group(),
                targets: vec![target()],
                entries: ConsumerConfigEntries::default(),
                authorization: ConsumerAclClassification::Authorized,
            },
        ))
        .unwrap();
        let ConsumerMutationResult::Applied { outcome, inventory, .. } = create else {
            panic!("complete Group catalog must reload after create")
        };
        assert!(!outcome.reload_failed);
        assert_partial(&inventory);
        assert!(inventory.groups.iter().any(|row| row.identity == group()));
        let edit = ready(services.patch_consumer_config(
            scope,
            rocketmq_dashboard_common::ConsumerConfigPatchCommand {
                snapshot,
                patch: ConsumerConfigPatch {
                    retry_max_times: Some(2),
                    ..Default::default()
                },
                authorization: ConsumerAclClassification::Authorized,
            },
        ))
        .unwrap();
        let ConsumerConfigMutationResult::Applied {
            configuration,
            inventory,
            ..
        } = edit
        else {
            panic!("complete configuration and Group catalog must reload after edit")
        };
        assert_eq!(configuration.snapshots[0].entries.retry_max_times, 2);
        assert_partial(&inventory);
        let delete = ready(services.delete_consumer(
            scope,
            rocketmq_dashboard_common::ConsumerDeleteCommand {
                group: group(),
                selected_targets: vec![target()],
                authoritative_targets: vec![target()],
                authorization: ConsumerAclClassification::Authorized,
            },
        ))
        .unwrap();
        let ConsumerMutationResult::Applied { outcome, inventory, .. } = delete else {
            panic!("complete Group catalog must reload after delete")
        };
        assert!(!outcome.reload_failed);
        assert_partial(&inventory);
        assert!(!inventory.groups.iter().any(|row| row.identity == group()));
        let calls = fake.calls();
        assert_eq!((calls.create.len(), calls.patch.len(), calls.delete.len()), (1, 1, 1));
    }

    #[test]
    fn successful_mutations_reject_partial_or_unknown_authoritative_reloads() {
        let fake = Arc::new(FakeConsumerBackend::default());
        fake.queue_create(Ok(outcome(ConsumerMutationKind::Create)));
        fake.queue_inventory(Ok(incomplete_inventory(
            rocketmq_dashboard_common::ConsumerObservationState::Partial,
        )));
        fake.queue_delete(Ok(outcome(ConsumerMutationKind::Delete)));
        fake.queue_inventory(Ok(incomplete_inventory(
            rocketmq_dashboard_common::ConsumerObservationState::Unknown,
        )));
        fake.queue_patch(Ok(rocketmq_dashboard_common::ConsumerConfigPatchOutcome::Applied {
            previous_generation: 7,
            generation: 8,
        }));
        fake.queue_configuration(Ok(rocketmq_dashboard_common::ConsumerConfiguration {
            group: group(),
            snapshots: Vec::new(),
            observation: rocketmq_dashboard_common::ConsumerObservationState::Partial,
            failures: Vec::new(),
        }));
        let services = AppServices::default().with_consumer_backend(fake);
        let scope = ConsumerRequestScope {
            revision: 1,
            epoch: 1,
            query_scope: Default::default(),
        };

        let create = ready(services.create_consumer(
            scope,
            rocketmq_dashboard_common::ConsumerCreateCommand {
                group: group(),
                targets: vec![target()],
                entries: ConsumerConfigEntries {
                    retry_max_times: 16,
                    retry_queue_nums: 1,
                    consume_timeout_minutes: 15,
                    ..ConsumerConfigEntries::default()
                },
                authorization: ConsumerAclClassification::Authorized,
            },
        ))
        .expect("create result");
        assert!(matches!(
            create,
            ConsumerMutationResult::AppliedReloadFailed { outcome, .. } if outcome.reload_failed
        ));

        let delete = ready(services.delete_consumer(
            scope,
            rocketmq_dashboard_common::ConsumerDeleteCommand {
                group: group(),
                selected_targets: vec![target()],
                authoritative_targets: vec![target()],
                authorization: ConsumerAclClassification::Authorized,
            },
        ))
        .expect("delete result");
        assert!(matches!(
            delete,
            ConsumerMutationResult::AppliedReloadFailed { outcome, .. } if outcome.reload_failed
        ));

        let config = ready(services.patch_consumer_config(
            scope,
            rocketmq_dashboard_common::ConsumerConfigPatchCommand {
                snapshot: ConsumerConfigSnapshot {
                    identity: ConsumerConfigIdentity {
                        group: group(),
                        target: target(),
                    },
                    generation: 7,
                    entries: ConsumerConfigEntries {
                        retry_max_times: 16,
                        retry_queue_nums: 1,
                        consume_timeout_minutes: 15,
                        ..ConsumerConfigEntries::default()
                    },
                    read_only: Default::default(),
                },
                patch: ConsumerConfigPatch {
                    retry_max_times: Some(15),
                    ..Default::default()
                },
                authorization: ConsumerAclClassification::Authorized,
            },
        ))
        .expect("config result");
        assert!(matches!(
            config,
            ConsumerConfigMutationResult::AppliedReloadFailed { .. }
        ));
    }
}
