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

//! Consumer and Producer read-only workspace mapping.

use std::sync::Arc;

use rocketmq_admin_core::core::{
    consumer::DashboardConsumerRunningInfoRequest,
    consumer_workspace::{
        ConsumerConfigPresence, ConsumerConfigPresenceResult, ConsumerConfigTarget, ConsumerConnectionsAtTargetsResult,
        ConsumerExactTargetsRequest, ConsumerInventoryResult, ConsumerResourceRequest, ConsumerWorkspaceTarget,
        ProducerConnectionsRequest, WorkspaceFailureCode, WorkspaceFailureStage, WorkspaceObservation,
        WorkspaceObservationState, WorkspaceTargetFailure, WorkspaceUnknownReason,
    },
};
use rocketmq_dashboard_common::{
    ConnectionScope, ConsumerCapabilities, ConsumerCategory, ConsumerClientIdentity, ConsumerClientObservation,
    ConsumerClients, ConsumerConfigEntries, ConsumerConfigIdentity, ConsumerConfigPatchCommand,
    ConsumerConfigPatchOutcome, ConsumerConfigSnapshot, ConsumerConfiguration, ConsumerCreateCommand,
    ConsumerDeleteCommand, ConsumerDiagnosticPayload, ConsumerDiagnosticRequest, ConsumerFailureCode,
    ConsumerFailureStage, ConsumerGroupObservation, ConsumerIdentity, ConsumerInventory, ConsumerMutationGuarantee,
    ConsumerMutationKind, ConsumerObservation, ConsumerObservationState, ConsumerPartialOutcome, ConsumerProgress,
    ConsumerProgressRow, ConsumerSubscription, ConsumerTargetFailure, ConsumerTargetIdentity, ConsumerTargetOutcome,
    ConsumerUnknownReason, ProducerConnectionQuery, ProducerConnections, ProducerGroupObservation, ProducerIdentity,
    ProducerInventory,
};

use super::{GpuiAdminProvider, ProviderError, ProviderErrorCode, query_for_revision, select_admin};

impl GpuiAdminProvider {
    pub async fn consumer_inventory(self: &Arc<Self>, revision: u64) -> Result<ConsumerInventory, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-inventory", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            let request = rocketmq_admin_core::core::consumer_workspace::ConsumerInventoryRequest {
                skip_system_groups: false,
                forwarded_address: forwarded_address(&snapshot)?,
            };
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.consumer_inventory(&request)).await?;
            map_inventory(snapshot.scope, response)
        })
        .await
    }

    pub async fn consumer_clients(
        self: &Arc<Self>,
        revision: u64,
        group: ConsumerIdentity,
    ) -> Result<ConsumerObservation<ConsumerClients>, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-clients", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            let request = ConsumerResourceRequest {
                group: group.as_str().to_owned(),
                forwarded_address: forwarded_address(&snapshot)?,
            };
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.consumer_clients(&request)).await?;
            map_fallible_observation(response.observation, |connection| map_clients(group, connection))
        })
        .await
    }

    pub async fn consumer_progress(
        self: &Arc<Self>,
        revision: u64,
        group: ConsumerIdentity,
    ) -> Result<ConsumerObservation<ConsumerProgress>, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-progress", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            let request = ConsumerResourceRequest {
                group: group.as_str().to_owned(),
                forwarded_address: forwarded_address(&snapshot)?,
            };
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.consumer_progress(&request)).await?;
            Ok(map_observation(response.observation, |progress| {
                map_progress(group, progress)
            }))
        })
        .await
    }

    pub async fn consumer_configuration(
        self: &Arc<Self>,
        revision: u64,
        group: ConsumerIdentity,
    ) -> Result<ConsumerConfiguration, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-configuration", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.consumer_configuration(group.as_str())).await?;
            map_configuration(group, response.targets, response.observation, response.failures)
        })
        .await
    }

    pub async fn consumer_diagnostic(
        self: &Arc<Self>,
        revision: u64,
        request: ConsumerDiagnosticRequest,
    ) -> Result<ConsumerDiagnosticPayload, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-diagnostic", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let request = DashboardConsumerRunningInfoRequest::try_new(
                request.group.as_str(),
                request.client.as_str(),
                matches!(request.kind, rocketmq_dashboard_common::ConsumerDiagnosticKind::Jstack),
                request.max_output_bytes,
            )
            .map_err(|_| invalid_data())?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.consumer_diagnostic(&request)).await?;
            let (properties, jstack, truncated) = response.into_diagnostic_parts();
            let payload = ConsumerDiagnosticPayload::new(
                properties.into_iter().map(|entry| (entry.key, entry.value)).collect(),
                jstack,
                truncated,
            );
            Ok(payload)
        })
        .await
    }

    pub async fn producer_inventory(self: &Arc<Self>, revision: u64) -> Result<ProducerInventory, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-producer-inventory", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.producer_inventory()).await?;
            let mut groups = response
                .items
                .into_iter()
                .map(|item| {
                    Ok(ProducerGroupObservation {
                        identity: ProducerIdentity::parse(item.group).map_err(|_| invalid_data())?,
                        client_count: map_observation(item.client_count, std::convert::identity),
                    })
                })
                .collect::<Result<Vec<_>, ProviderError>>()?;
            groups.sort_by(|left, right| left.identity.cmp(&right.identity));
            Ok(ProducerInventory {
                groups,
                observation: map_state(response.observation),
                failures: response.failures.into_iter().map(map_failure).collect(),
                capabilities: rocketmq_dashboard_common::ProducerCapabilities::for_scope(snapshot.scope),
            })
        })
        .await
    }

    pub async fn producer_connections(
        self: &Arc<Self>,
        revision: u64,
        query: ProducerConnectionQuery,
    ) -> Result<ConsumerObservation<ProducerConnections>, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-producer-connections", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let request = ProducerConnectionsRequest {
                topic: query.topic().to_owned(),
                group: query.group().as_str().to_owned(),
            };
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.producer_connections(&request)).await?;
            map_fallible_observation(response.observation, |connections| {
                let clients = connections
                    .connections
                    .into_iter()
                    .map(map_producer_client)
                    .collect::<Result<Vec<_>, ProviderError>>()?;
                Ok(ProducerConnections { query, clients })
            })
        })
        .await
    }

    pub async fn create_consumer_group(
        self: &Arc<Self>,
        revision: u64,
        command: ConsumerCreateCommand,
    ) -> Result<ConsumerPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-create", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let command = command.validate().map_err(|_| invalid_request())?;
            let exact = exact_targets_request(&command.group, &command.targets);
            let preflight = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                select_admin(cancellation.clone(), session.consumer_config_presence(&exact)).await?
            };
            if let Some(targets) = create_preflight_rejection(&command.targets, &preflight) {
                return Ok(ConsumerPartialOutcome {
                    group: command.group,
                    kind: ConsumerMutationKind::Create,
                    guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
                    targets,
                    reload_failed: false,
                });
            }
            let targets = command
                .targets
                .iter()
                .map(admin_exact_upsert_target)
                .collect::<Result<Vec<_>, ProviderError>>()?;
            let request = rocketmq_admin_core::core::consumer::ConsumerExactBatchUpsertRequest::try_new(
                rocketmq_admin_core::core::consumer::DashboardConsumerUpsertRequest {
                    cluster_name_list: Vec::new(),
                    broker_name_list: Vec::new(),
                    consumer_group: command.group.as_str().to_owned(),
                    consume_enable: true,
                    consume_from_min_enable: false,
                    consume_broadcast_enable: false,
                    consume_message_orderly: false,
                    retry_queue_nums: i32::try_from(command.entries.retry_queue_nums).map_err(|_| invalid_request())?,
                    retry_max_times: i32::try_from(command.entries.retry_max_times).map_err(|_| invalid_request())?,
                    broker_id: 0,
                    which_broker_when_consume_slowly: 1,
                    notify_consumer_ids_changed_enable: true,
                    group_sys_flag: 0,
                    consume_timeout_minute: i32::try_from(command.entries.consume_timeout_minutes)
                        .map_err(|_| invalid_request())?,
                },
                targets,
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = super::mutation_for_revision(&mut guard, revision)?;
            let outcome = select_admin(cancellation, session.upsert_consumer_group_exact_batch(&request)).await?;
            Ok(map_batch_outcome(command.group, ConsumerMutationKind::Create, outcome))
        })
        .await
    }

    pub async fn patch_consumer_configuration(
        self: &Arc<Self>,
        revision: u64,
        command: ConsumerConfigPatchCommand,
    ) -> Result<ConsumerConfigPatchOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-config-patch", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let command = command.validate().map_err(|_| invalid_request())?;
            let request = rocketmq_admin_core::core::consumer::PatchSubscriptionGroupConfigRequest::try_new(
                command.snapshot.identity.target.broker_address(),
                command.snapshot.identity.group.as_str(),
                command.snapshot.generation,
                rocketmq_admin_core::core::consumer::SubscriptionGroupConfigCasPatch {
                    retry_max_times: command.patch.retry_max_times,
                    retry_queue_nums: command.patch.retry_queue_nums,
                    consume_timeout_minutes: command.patch.consume_timeout_minutes,
                },
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = super::mutation_for_revision(&mut guard, revision)?;
            let outcome = select_admin(cancellation, session.patch_consumer_config(&request)).await?;
            Ok(match outcome {
                rocketmq_admin_core::core::consumer::PatchSubscriptionGroupConfigOutcome::Applied {
                    previous_version,
                    version,
                } => ConsumerConfigPatchOutcome::Applied {
                    previous_generation: previous_version,
                    generation: version,
                },
                rocketmq_admin_core::core::consumer::PatchSubscriptionGroupConfigOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } => ConsumerConfigPatchOutcome::GenerationConflict {
                    expected_generation: expected_version,
                    actual_generation: actual_version,
                },
            })
        })
        .await
    }

    pub async fn delete_consumer_group(
        self: &Arc<Self>,
        revision: u64,
        command: ConsumerDeleteCommand,
    ) -> Result<ConsumerPartialOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-delete", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let command = command.validate().map_err(|_| invalid_request())?;
            let exact = exact_targets_request(&command.group, &command.authoritative_targets);
            let (inventory, connections) = {
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                let inventory_request = rocketmq_admin_core::core::consumer_workspace::ConsumerInventoryRequest {
                    skip_system_groups: false,
                    forwarded_address: None,
                };
                let inventory =
                    select_admin(cancellation.clone(), session.consumer_inventory(&inventory_request)).await?;
                let connections =
                    select_admin(cancellation.clone(), session.consumer_connections_at_targets(&exact)).await?;
                (inventory, connections)
            };
            if let Some(targets) = delete_preflight_rejection(
                &command.authoritative_targets,
                &inventory,
                &connections,
                command.group.as_str(),
            ) {
                return Ok(ConsumerPartialOutcome {
                    group: command.group,
                    kind: ConsumerMutationKind::Delete,
                    guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
                    targets,
                    reload_failed: false,
                });
            }
            let selected_targets = command
                .selected_targets
                .iter()
                .map(admin_exact_delete_target)
                .collect::<Result<Vec<_>, ProviderError>>()?;
            let authoritative_targets = command
                .authoritative_targets
                .iter()
                .map(admin_exact_delete_target)
                .collect::<Result<Vec<_>, ProviderError>>()?;
            let request = rocketmq_admin_core::core::consumer::ConsumerExactBatchDeleteRequest::try_new(
                command.group.as_str(),
                selected_targets,
                authoritative_targets,
            )
            .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = super::mutation_for_revision(&mut guard, revision)?;
            let outcome = select_admin(cancellation, session.delete_consumer_group_exact_batch(&request)).await?;
            Ok(map_batch_outcome(command.group, ConsumerMutationKind::Delete, outcome))
        })
        .await
    }
}

fn admin_exact_delete_target(
    target: &ConsumerTargetIdentity,
) -> Result<rocketmq_admin_core::core::consumer::ConsumerExactBatchDeleteTarget, ProviderError> {
    rocketmq_admin_core::core::consumer::ConsumerExactBatchDeleteTarget::try_new(
        target.cluster_name(),
        target.broker_name(),
        target.broker_address(),
    )
    .map_err(|_| invalid_request())
}

fn admin_exact_upsert_target(
    target: &ConsumerTargetIdentity,
) -> Result<rocketmq_admin_core::core::consumer::ConsumerExactBatchUpsertTarget, ProviderError> {
    rocketmq_admin_core::core::consumer::ConsumerExactBatchUpsertTarget::try_new(
        target.cluster_name(),
        target.broker_name(),
        target.broker_address(),
    )
    .map_err(|_| invalid_request())
}

fn exact_targets_request(group: &ConsumerIdentity, targets: &[ConsumerTargetIdentity]) -> ConsumerExactTargetsRequest {
    ConsumerExactTargetsRequest {
        group: group.as_str().to_owned(),
        targets: targets
            .iter()
            .map(|target| ConsumerWorkspaceTarget {
                cluster_name: target.cluster_name().to_owned(),
                broker_name: target.broker_name().to_owned(),
                broker_address: target.broker_address().to_owned(),
            })
            .collect(),
    }
}

fn workspace_targets_equal(left: &[ConsumerWorkspaceTarget], right: &[ConsumerTargetIdentity]) -> bool {
    let mut left = left
        .iter()
        .map(|target| {
            (
                target.cluster_name.as_str(),
                target.broker_name.as_str(),
                target.broker_address.as_str(),
            )
        })
        .collect::<Vec<_>>();
    let mut right = right
        .iter()
        .map(|target| (target.cluster_name(), target.broker_name(), target.broker_address()))
        .collect::<Vec<_>>();
    left.sort_unstable();
    right.sort_unstable();
    left == right
}

fn create_preflight_rejection(
    requested: &[ConsumerTargetIdentity],
    response: &ConsumerConfigPresenceResult,
) -> Option<Vec<ConsumerTargetOutcome>> {
    let mut outcomes = Vec::with_capacity(requested.len() + response.targets.len());
    let mut unsafe_preflight = !response.failures.is_empty();
    for target in requested {
        let matching = response
            .targets
            .iter()
            .filter(|observed| workspace_target_matches(&observed.target, target))
            .collect::<Vec<_>>();
        let outcome = match matching.as_slice() {
            [observed] => match observed.presence {
                ConsumerConfigPresence::Absent => preflight_safe(target),
                ConsumerConfigPresence::Present => {
                    unsafe_preflight = true;
                    preflight_failure(
                        exact_target_label(target),
                        ConsumerFailureStage::Configuration,
                        ConsumerFailureCode::Conflict,
                        false,
                    )
                }
                ConsumerConfigPresence::Unknown => {
                    unsafe_preflight = true;
                    preflight_failure(
                        exact_target_label(target),
                        ConsumerFailureStage::Configuration,
                        ConsumerFailureCode::Unavailable,
                        true,
                    )
                }
            },
            _ => {
                unsafe_preflight = true;
                preflight_failure(
                    exact_target_label(target),
                    ConsumerFailureStage::Configuration,
                    ConsumerFailureCode::InvalidData,
                    false,
                )
            }
        };
        outcomes.push(outcome);
    }
    for observed in &response.targets {
        if !requested
            .iter()
            .any(|target| workspace_target_matches(&observed.target, target))
        {
            unsafe_preflight = true;
            outcomes.push(preflight_failure(
                workspace_target_label(&observed.target),
                ConsumerFailureStage::Configuration,
                ConsumerFailureCode::InvalidData,
                false,
            ));
        }
    }
    if !unsafe_preflight {
        return None;
    }
    abort_safe_preflight_targets(&mut outcomes);
    Some(outcomes)
}

fn delete_preflight_rejection(
    authoritative: &[ConsumerTargetIdentity],
    inventory: &ConsumerInventoryResult,
    connections: &ConsumerConnectionsAtTargetsResult,
    group: &str,
) -> Option<Vec<ConsumerTargetOutcome>> {
    let matching_inventory = inventory
        .items
        .iter()
        .filter(|item| item.group == group)
        .collect::<Vec<_>>();
    let fresh_targets = match matching_inventory.as_slice() {
        [item] => item.targets.as_slice(),
        _ => &[],
    };
    let inventory_complete = inventory.observation == WorkspaceObservationState::Complete
        && matching_inventory.len() == 1
        && workspace_targets_equal(fresh_targets, authoritative);
    let connection_identity_complete = connections.targets.len() == authoritative.len()
        && authoritative.iter().all(|target| {
            connections
                .targets
                .iter()
                .filter(|observed| workspace_target_matches(&observed.target, target))
                .count()
                == 1
        });
    let mut unsafe_preflight = !inventory_complete || !connection_identity_complete || !connections.failures.is_empty();
    let mut outcomes = Vec::with_capacity(authoritative.len() + connections.targets.len());
    for target in authoritative {
        if !inventory_complete
            && !fresh_targets
                .iter()
                .any(|fresh| workspace_target_matches(fresh, target))
        {
            outcomes.push(preflight_failure(
                exact_target_label(target),
                ConsumerFailureStage::Inventory,
                ConsumerFailureCode::Conflict,
                false,
            ));
            continue;
        }
        let matching = connections
            .targets
            .iter()
            .filter(|observed| workspace_target_matches(&observed.target, target))
            .collect::<Vec<_>>();
        let outcome = match matching.as_slice() {
            [observed] => match &observed.observation {
                WorkspaceObservation::Complete { value } if value.connections.is_empty() => preflight_safe(target),
                WorkspaceObservation::Complete { .. } => {
                    unsafe_preflight = true;
                    preflight_failure(
                        exact_target_label(target),
                        ConsumerFailureStage::ConnectionObservation,
                        ConsumerFailureCode::Conflict,
                        false,
                    )
                }
                WorkspaceObservation::Partial { .. } | WorkspaceObservation::Unknown { .. } => {
                    unsafe_preflight = true;
                    preflight_failure(
                        exact_target_label(target),
                        ConsumerFailureStage::ConnectionObservation,
                        ConsumerFailureCode::Unavailable,
                        true,
                    )
                }
            },
            _ => {
                unsafe_preflight = true;
                preflight_failure(
                    exact_target_label(target),
                    ConsumerFailureStage::ConnectionObservation,
                    ConsumerFailureCode::InvalidData,
                    false,
                )
            }
        };
        outcomes.push(outcome);
    }
    for observed in &connections.targets {
        if !authoritative
            .iter()
            .any(|target| workspace_target_matches(&observed.target, target))
        {
            unsafe_preflight = true;
            outcomes.push(preflight_failure(
                workspace_target_label(&observed.target),
                ConsumerFailureStage::ConnectionObservation,
                ConsumerFailureCode::InvalidData,
                false,
            ));
        }
    }
    if !unsafe_preflight {
        return None;
    }
    abort_safe_preflight_targets(&mut outcomes);
    Some(outcomes)
}

fn workspace_target_matches(left: &ConsumerWorkspaceTarget, right: &ConsumerTargetIdentity) -> bool {
    left.cluster_name == right.cluster_name()
        && left.broker_name == right.broker_name()
        && left.broker_address == right.broker_address()
}

fn preflight_safe(target: &ConsumerTargetIdentity) -> ConsumerTargetOutcome {
    ConsumerTargetOutcome {
        target: exact_target_label(target),
        stage: ConsumerFailureStage::PreflightAborted,
        applied: false,
        failure: None,
        retryable: false,
    }
}

fn preflight_failure(
    target: String,
    stage: ConsumerFailureStage,
    failure: ConsumerFailureCode,
    retryable: bool,
) -> ConsumerTargetOutcome {
    ConsumerTargetOutcome {
        target,
        stage,
        applied: false,
        failure: Some(failure),
        retryable,
    }
}

fn abort_safe_preflight_targets(outcomes: &mut [ConsumerTargetOutcome]) {
    for outcome in outcomes.iter_mut().filter(|outcome| outcome.failure.is_none()) {
        outcome.stage = ConsumerFailureStage::PreflightAborted;
        outcome.failure = Some(ConsumerFailureCode::NotApplied);
    }
}

fn exact_target_label(target: &ConsumerTargetIdentity) -> String {
    format!(
        "{}/{}/{}",
        target.cluster_name(),
        target.broker_name(),
        target.broker_address()
    )
}

fn workspace_target_label(target: &ConsumerWorkspaceTarget) -> String {
    format!(
        "{}/{}/{}",
        target.cluster_name, target.broker_name, target.broker_address
    )
}

fn map_batch_outcome(
    group: ConsumerIdentity,
    kind: ConsumerMutationKind,
    outcome: rocketmq_admin_core::core::consumer::DashboardConsumerBatchResult,
) -> ConsumerPartialOutcome {
    ConsumerPartialOutcome {
        group,
        kind,
        guarantee: ConsumerMutationGuarantee::PreflightBestEffort,
        targets: outcome
            .targets
            .into_iter()
            .map(|target| ConsumerTargetOutcome {
                target: target.target,
                stage: if target.kind == "INTERNAL_TOPIC_CLEANUP" {
                    ConsumerFailureStage::Cleanup
                } else {
                    ConsumerFailureStage::Mutation
                },
                applied: target.success,
                failure: (!target.success).then_some(ConsumerFailureCode::Unavailable),
                retryable: !target.success,
            })
            .collect(),
        reload_failed: false,
    }
}

fn forwarded_address(
    snapshot: &rocketmq_dashboard_common::ConnectionSnapshot,
) -> Result<Option<String>, ProviderError> {
    match snapshot.scope {
        ConnectionScope::NameServer => Ok(None),
        ConnectionScope::Proxy => snapshot.proxy.clone().map(Some).ok_or_else(not_configured),
    }
}

fn require_direct(scope: ConnectionScope) -> Result<(), ProviderError> {
    if scope == ConnectionScope::NameServer {
        Ok(())
    } else {
        Err(ProviderError::new(
            ProviderErrorCode::Unavailable,
            "This Consumer capability requires NameServer Direct scope.",
            false,
        ))
    }
}

fn map_inventory(
    scope: ConnectionScope,
    response: ConsumerInventoryResult,
) -> Result<ConsumerInventory, ProviderError> {
    let mut groups = response
        .items
        .into_iter()
        .map(|item| {
            let client_count = map_observation(item.client_count, std::convert::identity);
            let connection_state = map_connection_state(&client_count);
            Ok(ConsumerGroupObservation {
                identity: ConsumerIdentity::parse(item.group).map_err(|_| invalid_data())?,
                category: map_category(&item.category),
                connection_state,
                client_count,
                lag: map_observation(item.diff_total, std::convert::identity),
                consume_type: map_observation(item.consume_type, std::convert::identity),
                message_model: map_observation(item.message_model, std::convert::identity),
                targets: item
                    .targets
                    .into_iter()
                    .map(map_target)
                    .collect::<Result<Vec<_>, ProviderError>>()?,
            })
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    groups.sort_by(|left, right| left.identity.cmp(&right.identity));
    Ok(ConsumerInventory {
        groups,
        targets: response
            .targets
            .into_iter()
            .map(map_target)
            .collect::<Result<Vec<_>, ProviderError>>()?,
        observation: map_state(response.observation),
        failures: response.failures.into_iter().map(map_failure).collect(),
        capabilities: ConsumerCapabilities::for_scope(scope),
    })
}

fn map_connection_state(
    count: &ConsumerObservation<usize>,
) -> ConsumerObservation<rocketmq_dashboard_common::ConsumerConnectionState> {
    use rocketmq_dashboard_common::ConsumerConnectionState::{Connected, Disconnected};
    match count {
        ConsumerObservation::Complete(count) => {
            ConsumerObservation::Complete(if *count == 0 { Disconnected } else { Connected })
        }
        ConsumerObservation::Partial {
            value,
            successful_target_count,
            failures,
        } if *value > 0 => ConsumerObservation::Partial {
            value: Connected,
            successful_target_count: *successful_target_count,
            failures: failures.clone(),
        },
        ConsumerObservation::Partial { .. } | ConsumerObservation::Unknown { .. } => ConsumerObservation::Unknown {
            reason: ConsumerUnknownReason::Unavailable,
        },
    }
}

fn map_clients(
    group: ConsumerIdentity,
    connection: rocketmq_admin_core::core::consumer::DashboardConsumerConnection,
) -> Result<ConsumerClients, ProviderError> {
    Ok(ConsumerClients {
        group,
        clients: connection
            .connections
            .into_iter()
            .map(map_client)
            .collect::<Result<Vec<_>, ProviderError>>()?,
        consume_type: text_observation(connection.consume_type),
        message_model: text_observation(connection.message_model),
        subscriptions: connection
            .subscriptions
            .into_iter()
            .map(|subscription| ConsumerSubscription {
                topic: subscription.topic,
                expression: subscription.sub_string,
                expression_type: subscription.expression_type,
            })
            .collect(),
    })
}

fn map_client(
    client: rocketmq_admin_core::core::consumer::DashboardConsumerConnectionItem,
) -> Result<ConsumerClientObservation, ProviderError> {
    Ok(ConsumerClientObservation {
        identity: ConsumerClientIdentity::parse(client.client_id).map_err(|_| invalid_data())?,
        address: client.client_addr,
        language: client.language,
        version: client.version,
        version_description: client.version_desc,
    })
}

fn map_producer_client(
    client: rocketmq_admin_core::core::dashboard::DashboardProducerConnection,
) -> Result<ConsumerClientObservation, ProviderError> {
    Ok(ConsumerClientObservation {
        identity: ConsumerClientIdentity::parse(client.client_id).map_err(|_| invalid_data())?,
        address: client.client_addr,
        language: client.language,
        version: client.version,
        version_description: client.version_desc,
    })
}

fn map_progress(
    group: ConsumerIdentity,
    progress: rocketmq_admin_core::core::consumer::DashboardConsumerProgress,
) -> ConsumerProgress {
    let rows = progress
        .topics
        .into_iter()
        .flat_map(|topic| {
            let topic_name = topic.topic;
            topic.queues.into_iter().map(move |queue| ConsumerProgressRow {
                topic: topic_name.clone(),
                broker_name: queue.broker_name,
                queue_id: queue.queue_id,
                broker_offset: queue.broker_offset,
                consumer_offset: queue.consumer_offset,
                delta: queue.diff_total,
                last_timestamp: queue.last_timestamp,
            })
        })
        .collect();
    ConsumerProgress::from_rows(group, rows)
}

fn map_configuration(
    group: ConsumerIdentity,
    targets: Vec<ConsumerConfigTarget>,
    observation: WorkspaceObservationState,
    failures: Vec<WorkspaceTargetFailure>,
) -> Result<ConsumerConfiguration, ProviderError> {
    let snapshots = targets
        .into_iter()
        .filter_map(|target| match target.observation {
            WorkspaceObservation::Complete { value } | WorkspaceObservation::Partial { value, .. } => {
                Some((target.target, value))
            }
            WorkspaceObservation::Unknown { .. } => None,
        })
        .map(|(target, state)| {
            Ok(ConsumerConfigSnapshot {
                identity: ConsumerConfigIdentity {
                    group: group.clone(),
                    target: map_target(target)?,
                },
                generation: state.version,
                entries: ConsumerConfigEntries {
                    retry_max_times: state.retry_max_times,
                    retry_queue_nums: state.retry_queue_nums,
                    consume_timeout_minutes: state.consume_timeout_minutes,
                },
            })
        })
        .collect::<Result<Vec<_>, ProviderError>>()?;
    Ok(ConsumerConfiguration {
        group,
        snapshots,
        observation: map_state(observation),
        failures: failures.into_iter().map(map_failure).collect(),
    })
}

fn map_observation<T, U>(observation: WorkspaceObservation<T>, map: impl FnOnce(T) -> U) -> ConsumerObservation<U> {
    match observation {
        WorkspaceObservation::Complete { value } => ConsumerObservation::Complete(map(value)),
        WorkspaceObservation::Partial {
            value,
            successful_target_count,
            failures,
        } => ConsumerObservation::Partial {
            value: map(value),
            successful_target_count,
            failures: failures.into_iter().map(map_failure).collect(),
        },
        WorkspaceObservation::Unknown { reason } => ConsumerObservation::Unknown {
            reason: map_unknown(reason),
        },
    }
}

fn map_fallible_observation<T, U>(
    observation: WorkspaceObservation<T>,
    map: impl FnOnce(T) -> Result<U, ProviderError>,
) -> Result<ConsumerObservation<U>, ProviderError> {
    match observation {
        WorkspaceObservation::Complete { value } => Ok(ConsumerObservation::Complete(map(value)?)),
        WorkspaceObservation::Partial {
            value,
            successful_target_count,
            failures,
        } => Ok(ConsumerObservation::Partial {
            value: map(value)?,
            successful_target_count,
            failures: failures.into_iter().map(map_failure).collect(),
        }),
        WorkspaceObservation::Unknown { reason } => Ok(ConsumerObservation::Unknown {
            reason: map_unknown(reason),
        }),
    }
}

fn text_observation(value: String) -> ConsumerObservation<String> {
    if value.trim().is_empty() || value.eq_ignore_ascii_case("unknown") {
        ConsumerObservation::Unknown {
            reason: ConsumerUnknownReason::InvalidResponse,
        }
    } else {
        ConsumerObservation::Complete(value)
    }
}

fn map_target(target: ConsumerWorkspaceTarget) -> Result<ConsumerTargetIdentity, ProviderError> {
    ConsumerTargetIdentity::parse(target.cluster_name, target.broker_name, target.broker_address)
        .map_err(|_| invalid_data())
}

fn map_failure(failure: WorkspaceTargetFailure) -> ConsumerTargetFailure {
    ConsumerTargetFailure {
        target: failure.target,
        stage: match failure.stage {
            WorkspaceFailureStage::Inventory => ConsumerFailureStage::Inventory,
            WorkspaceFailureStage::Clients => ConsumerFailureStage::Clients,
            WorkspaceFailureStage::Progress => ConsumerFailureStage::Progress,
            WorkspaceFailureStage::Configuration => ConsumerFailureStage::Configuration,
            WorkspaceFailureStage::Diagnostics => ConsumerFailureStage::ConnectionObservation,
        },
        code: match failure.code {
            WorkspaceFailureCode::NotFound => ConsumerFailureCode::NotFound,
            WorkspaceFailureCode::Unavailable => ConsumerFailureCode::Unavailable,
            WorkspaceFailureCode::Unsupported => ConsumerFailureCode::Unsupported,
            WorkspaceFailureCode::InvalidData => ConsumerFailureCode::InvalidData,
        },
        retryable: failure.retryable,
    }
}

const fn map_state(state: WorkspaceObservationState) -> ConsumerObservationState {
    match state {
        WorkspaceObservationState::Complete => ConsumerObservationState::Complete,
        WorkspaceObservationState::Partial => ConsumerObservationState::Partial,
        WorkspaceObservationState::Unknown => ConsumerObservationState::Unknown,
    }
}

const fn map_unknown(reason: WorkspaceUnknownReason) -> ConsumerUnknownReason {
    match reason {
        WorkspaceUnknownReason::Unsupported => ConsumerUnknownReason::Unsupported,
        WorkspaceUnknownReason::Unavailable => ConsumerUnknownReason::Unavailable,
        WorkspaceUnknownReason::InvalidResponse => ConsumerUnknownReason::InvalidResponse,
    }
}

fn map_category(category: &str) -> ConsumerCategory {
    match category.trim().to_ascii_uppercase().as_str() {
        "SYSTEM" => ConsumerCategory::System,
        "NORMAL" | "FIFO" => ConsumerCategory::Application,
        _ => ConsumerCategory::Unknown,
    }
}

fn invalid_data() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::Unavailable,
        "The Admin response did not contain a valid Consumer identity.",
        false,
    )
}

fn invalid_request() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::Unavailable,
        "The Consumer operation did not pass its exact-target preflight.",
        false,
    )
}

fn not_configured() -> ProviderError {
    ProviderError::new(
        ProviderErrorCode::NotConfigured,
        "A Proxy endpoint is required for Proxy scope.",
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_dashboard_common::CapabilityAvailability;

    #[test]
    fn partial_zero_clients_never_becomes_disconnected() {
        let state = map_connection_state(&ConsumerObservation::Partial {
            value: 0,
            successful_target_count: 1,
            failures: vec![ConsumerTargetFailure {
                target: "broker-b".into(),
                stage: ConsumerFailureStage::Clients,
                code: ConsumerFailureCode::Unavailable,
                retryable: true,
            }],
        });
        assert!(matches!(state, ConsumerObservation::Unknown { .. }));
    }

    #[test]
    fn diagnostic_payload_takes_ownership_without_cloning_admin_value() {
        let response = rocketmq_admin_core::core::consumer::DashboardConsumerRunningInfo::new(
            "orders".into(),
            "client-a".into(),
            vec![rocketmq_admin_core::core::consumer::DashboardConsumerConfigAttribute {
                key: "PROP_CONSUME_TYPE".into(),
                value: "PUSH".into(),
            }],
            Vec::new(),
            Vec::new(),
            Some("sensitive-stack".into()),
            false,
        );
        let (properties, jstack, truncated) = response.into_diagnostic_parts();
        assert_eq!(properties.len(), 1);
        assert_eq!(jstack.as_deref(), Some("sensitive-stack"));
        assert!(!truncated);
    }

    #[test]
    fn proxy_direct_only_matrix_is_fail_closed() {
        assert!(require_direct(ConnectionScope::NameServer).is_ok());
        assert!(require_direct(ConnectionScope::Proxy).is_err());
        let capabilities = ConsumerCapabilities::for_scope(ConnectionScope::Proxy);
        assert_eq!(capabilities.configuration, CapabilityAvailability::Unavailable);
        assert_eq!(capabilities.diagnostics, CapabilityAvailability::Unavailable);
        let producer = rocketmq_dashboard_common::ProducerCapabilities::for_scope(ConnectionScope::Proxy);
        assert_eq!(producer.inventory, CapabilityAvailability::Unavailable);
        assert_eq!(producer.connections, CapabilityAvailability::Unavailable);
    }

    #[test]
    fn create_preflight_maps_present_and_safe_absent_targets_independently() {
        let requested = [
            common_target("broker-a", "10.0.0.1:10911"),
            common_target("broker-b", "10.0.0.2:10911"),
        ];
        let response = ConsumerConfigPresenceResult {
            targets: vec![
                rocketmq_admin_core::core::consumer_workspace::ConsumerConfigPresenceTarget {
                    target: workspace_target("broker-a", "10.0.0.1:10911"),
                    presence: ConsumerConfigPresence::Absent,
                },
                rocketmq_admin_core::core::consumer_workspace::ConsumerConfigPresenceTarget {
                    target: workspace_target("broker-b", "10.0.0.2:10911"),
                    presence: ConsumerConfigPresence::Present,
                },
            ],
            failures: Vec::new(),
        };

        let outcomes = create_preflight_rejection(&requested, &response).expect("collision rejects all writes");
        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].stage, ConsumerFailureStage::PreflightAborted);
        assert_eq!(outcomes[0].failure, Some(ConsumerFailureCode::NotApplied));
        assert_eq!(outcomes[1].stage, ConsumerFailureStage::Configuration);
        assert_eq!(outcomes[1].failure, Some(ConsumerFailureCode::Conflict));
        assert!(outcomes.iter().all(|outcome| !outcome.applied));
    }

    #[test]
    fn delete_preflight_checks_every_authoritative_target_and_keeps_unknown_distinct() {
        let authoritative = [
            common_target("broker-a", "10.0.0.1:10911"),
            common_target("broker-b", "10.0.0.2:10911"),
        ];
        let inventory = ConsumerInventoryResult {
            items: vec![rocketmq_admin_core::core::consumer_workspace::ConsumerInventoryItem {
                group: "orders".into(),
                category: "NORMAL".into(),
                client_count: WorkspaceObservation::Complete { value: 0 },
                diff_total: WorkspaceObservation::Complete { value: 0 },
                consume_type: WorkspaceObservation::Complete { value: "PUSH".into() },
                message_model: WorkspaceObservation::Complete {
                    value: "CLUSTERING".into(),
                },
                targets: authoritative
                    .iter()
                    .map(|target| workspace_target(target.broker_name(), target.broker_address()))
                    .collect(),
            }],
            targets: Vec::new(),
            observation: WorkspaceObservationState::Complete,
            failures: Vec::new(),
        };
        let empty_connection = rocketmq_admin_core::core::consumer::DashboardConsumerConnection {
            consumer_group: "orders".into(),
            connection_count: 0,
            consume_type: "PUSH".into(),
            message_model: "CLUSTERING".into(),
            consume_from_where: "LAST".into(),
            connections: Vec::new(),
            subscriptions: Vec::new(),
        };
        let connections = ConsumerConnectionsAtTargetsResult {
            targets: vec![
                rocketmq_admin_core::core::consumer_workspace::ConsumerConnectionTarget {
                    target: workspace_target("broker-a", "10.0.0.1:10911"),
                    observation: WorkspaceObservation::Complete {
                        value: empty_connection,
                    },
                },
                rocketmq_admin_core::core::consumer_workspace::ConsumerConnectionTarget {
                    target: workspace_target("broker-b", "10.0.0.2:10911"),
                    observation: WorkspaceObservation::Unknown {
                        reason: WorkspaceUnknownReason::Unavailable,
                    },
                },
            ],
            failures: Vec::new(),
        };

        let outcomes = delete_preflight_rejection(&authoritative, &inventory, &connections, "orders")
            .expect("Unknown connection fails closed");
        assert_eq!(outcomes[0].failure, Some(ConsumerFailureCode::NotApplied));
        assert_eq!(outcomes[1].failure, Some(ConsumerFailureCode::Unavailable));
        assert!(outcomes[1].retryable);
    }

    fn common_target(broker: &str, address: &str) -> ConsumerTargetIdentity {
        ConsumerTargetIdentity::parse("cluster-a", broker, address).expect("target")
    }

    fn workspace_target(broker: &str, address: &str) -> ConsumerWorkspaceTarget {
        ConsumerWorkspaceTarget {
            cluster_name: "cluster-a".into(),
            broker_name: broker.into(),
            broker_address: address.into(),
        }
    }
}
