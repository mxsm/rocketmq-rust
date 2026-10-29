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

//! Consumer and Producer session routing and exact-target mutation preflight.

#[path = "consumers_mapping.rs"]
mod mapping;
use mapping::*;

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

use super::{GpuiAdminProvider, ProviderFailure, query_for_revision, select_admin};

impl GpuiAdminProvider {
    pub async fn consumer_inventory(
        self: &Arc<Self>,
        revision: u64,
        query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
    ) -> Result<ConsumerInventory, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-inventory", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            let request = rocketmq_admin_core::core::consumer_workspace::ConsumerInventoryRequest {
                skip_system_groups: false,
                forwarded_address: forwarded_address(&snapshot, query_scope)?,
            };
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let response = select_admin(cancellation, session.consumer_inventory(&request)).await?;
            let evidence = snapshot.nameserver.is_some()
                && response.target_observation == WorkspaceObservationState::Complete
                && !response.targets.is_empty();
            let scope = match query_scope {
                rocketmq_dashboard_common::ConsumerQueryScope::Configured => snapshot.scope,
                rocketmq_dashboard_common::ConsumerQueryScope::BrokerDirect => ConnectionScope::NameServer,
            };
            let mut inventory = map_inventory(scope, response)?;
            // Query routing never changes mutation capability of the global connection.
            inventory.capabilities =
                ConsumerCapabilities::for_scope(snapshot.scope).with_direct_read_evidence(evidence);
            Ok(inventory)
        })
        .await
    }

    pub async fn consumer_clients(
        self: &Arc<Self>,
        revision: u64,
        query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
        group: ConsumerIdentity,
    ) -> Result<ConsumerObservation<ConsumerClients>, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-clients", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            let request = ConsumerResourceRequest {
                group: group.as_str().to_owned(),
                forwarded_address: forwarded_address(&snapshot, query_scope)?,
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
        query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
        group: ConsumerIdentity,
    ) -> Result<ConsumerObservation<ConsumerProgress>, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-progress", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            let request = ConsumerResourceRequest {
                group: group.as_str().to_owned(),
                forwarded_address: forwarded_address(&snapshot, query_scope)?,
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
    ) -> Result<ConsumerConfiguration, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-configuration", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct_read(&snapshot)?;
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
    ) -> Result<ConsumerDiagnosticPayload, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-diagnostic", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct_read(&snapshot)?;
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
            let parts = response.into_parts();
            let payload = ConsumerDiagnosticPayload::new(
                parts
                    .properties
                    .into_iter()
                    .map(|entry| (entry.key, entry.value))
                    .collect(),
                parts.jstack,
                parts.truncated,
            )
            .with_running_details(
                parts
                    .subscriptions
                    .into_iter()
                    .map(|subscription| ConsumerSubscription {
                        topic: subscription.topic,
                        expression: subscription.sub_string,
                        expression_type: subscription.expression_type,
                    })
                    .collect(),
                parts
                    .process_queues
                    .into_iter()
                    .map(|queue| rocketmq_dashboard_common::ConsumerProcessQueue {
                        topic: queue.topic,
                        broker_name: queue.broker_name,
                        queue_id: queue.queue_id,
                        cached_message_count: queue.cached_message_count,
                        cached_message_size_in_mib: queue.cached_message_size_in_mib,
                        commit_offset: queue.commit_offset,
                        dropped: queue.dropped,
                        last_consume_timestamp: queue.last_consume_timestamp,
                    })
                    .collect(),
            );
            Ok(payload)
        })
        .await
    }

    pub async fn producer_inventory(self: &Arc<Self>, revision: u64) -> Result<ProducerInventory, ProviderFailure> {
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
                .collect::<Result<Vec<_>, ProviderFailure>>()?;
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
    ) -> Result<ConsumerObservation<ProducerConnections>, ProviderFailure> {
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
                    .collect::<Result<Vec<_>, ProviderFailure>>()?;
                Ok(ProducerConnections { query, clients })
            })
        })
        .await
    }

    pub async fn create_consumer_group(
        self: &Arc<Self>,
        revision: u64,
        command: ConsumerCreateCommand,
    ) -> Result<ConsumerPartialOutcome, ProviderFailure> {
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
                .collect::<Result<Vec<_>, ProviderFailure>>()?;
            let request = rocketmq_admin_core::core::consumer::ConsumerExactBatchUpsertRequest::try_new(
                create_values(&command)?,
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
    ) -> Result<ConsumerConfigPatchOutcome, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-consumer-config-patch", move |cancellation| async move {
            let snapshot = this.snapshot_for_revision(revision)?;
            require_direct(snapshot.scope)?;
            let command = command.validate().map_err(|_| invalid_request())?;
            let request =
                rocketmq_admin_core::core::consumer_configuration::PatchConsumerWorkspaceConfigRequest::try_new(
                    command.snapshot.identity.target.broker_address(),
                    command.snapshot.identity.group.as_str(),
                    command.snapshot.generation,
                    rocketmq_admin_core::core::consumer_configuration::ConsumerWorkspaceConfigPatch {
                        retry_max_times: command.patch.retry_max_times,
                        retry_queue_nums: command.patch.retry_queue_nums,
                        consume_timeout_minutes: command.patch.consume_timeout_minutes,
                        consume_enable: command.patch.consume_enable,
                        consume_from_min_enable: command.patch.consume_from_min_enable,
                        consume_broadcast_enable: command.patch.consume_broadcast_enable,
                        consume_message_orderly: command.patch.consume_message_orderly,
                        broker_id: command.patch.broker_id,
                        which_broker_when_consume_slowly: command.patch.which_broker_when_consume_slowly,
                    },
                )
                .map_err(|_| invalid_request())?;
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = super::mutation_for_revision(&mut guard, revision)?;
            let outcome = select_admin(cancellation, session.patch_consumer_config(&request)).await?;
            Ok(match outcome {
                rocketmq_admin_core::core::consumer_configuration::ConsumerWorkspaceConfigPatchResult::Applied {
                    previous_version,
                    version,
                } => ConsumerConfigPatchOutcome::Applied {
                    previous_generation: previous_version,
                    generation: version,
                },
                rocketmq_admin_core::core::consumer_configuration::ConsumerWorkspaceConfigPatchResult::VersionConflict {
                    expected_version,
                    actual_version,
                } => ConsumerConfigPatchOutcome::GenerationConflict {
                    expected_generation: expected_version,
                    actual_generation: actual_version,
                },
                rocketmq_admin_core::core::consumer_configuration::ConsumerWorkspaceConfigPatchResult::PersistenceIncomplete {applied,previous_version,version,persistence} => ConsumerConfigPatchOutcome::PersistenceIncomplete {
                    applied,previous_generation:previous_version,generation:version,persistence:match persistence {
                        rocketmq_admin_core::core::consumer_configuration::ConsumerConfigPersistence::Failed=>rocketmq_dashboard_common::ConsumerConfigPersistence::Failed,
                        rocketmq_admin_core::core::consumer_configuration::ConsumerConfigPersistence::Unconfirmed=>rocketmq_dashboard_common::ConsumerConfigPersistence::Unconfirmed,
                    },
                },
            })
        })
        .await
    }

    pub async fn delete_consumer_group(
        self: &Arc<Self>,
        revision: u64,
        command: ConsumerDeleteCommand,
    ) -> Result<ConsumerPartialOutcome, ProviderFailure> {
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
                .collect::<Result<Vec<_>, ProviderFailure>>()?;
            let authoritative_targets = command
                .authoritative_targets
                .iter()
                .map(admin_exact_delete_target)
                .collect::<Result<Vec<_>, ProviderFailure>>()?;
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
) -> Result<rocketmq_admin_core::core::consumer::ConsumerExactBatchDeleteTarget, ProviderFailure> {
    rocketmq_admin_core::core::consumer::ConsumerExactBatchDeleteTarget::try_new(
        target.cluster_name(),
        target.broker_name(),
        target.broker_address(),
    )
    .map_err(|_| invalid_request())
}

fn admin_exact_upsert_target(
    target: &ConsumerTargetIdentity,
) -> Result<rocketmq_admin_core::core::consumer::ConsumerExactBatchUpsertTarget, ProviderFailure> {
    rocketmq_admin_core::core::consumer::ConsumerExactBatchUpsertTarget::try_new(
        target.cluster_name(),
        target.broker_name(),
        target.broker_address(),
    )
    .map_err(|_| invalid_request())
}

fn create_values(
    command: &ConsumerCreateCommand,
) -> Result<rocketmq_admin_core::core::consumer::DashboardConsumerUpsertRequest, ProviderFailure> {
    Ok(rocketmq_admin_core::core::consumer::DashboardConsumerUpsertRequest {
        cluster_name_list: Vec::new(),
        broker_name_list: Vec::new(),
        consumer_group: command.group.as_str().to_owned(),
        consume_enable: command.entries.consume_enable,
        consume_from_min_enable: command.entries.consume_from_min_enable,
        consume_broadcast_enable: command.entries.consume_broadcast_enable,
        consume_message_orderly: command.entries.consume_message_orderly,
        retry_queue_nums: i32::try_from(command.entries.retry_queue_nums).map_err(|_| invalid_request())?,
        retry_max_times: command.entries.retry_max_times,
        broker_id: command.entries.broker_id,
        which_broker_when_consume_slowly: command.entries.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: command.entries.notify_consumer_ids_changed_enable,
        group_sys_flag: command.entries.group_sys_flag,
        consume_timeout_minute: i32::try_from(command.entries.consume_timeout_minutes)
            .map_err(|_| invalid_request())?,
    })
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
    let inventory_complete = inventory.group_observation == WorkspaceObservationState::Complete
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
    query_scope: rocketmq_dashboard_common::ConsumerQueryScope,
) -> Result<Option<String>, ProviderFailure> {
    if query_scope == rocketmq_dashboard_common::ConsumerQueryScope::BrokerDirect {
        require_direct_read(snapshot)?;
        return Ok(None);
    }
    match snapshot.scope {
        ConnectionScope::NameServer => Ok(None),
        ConnectionScope::Proxy => snapshot.proxy.clone().map(Some).ok_or_else(not_configured),
    }
}

fn require_direct_read(snapshot: &rocketmq_dashboard_common::ConnectionSnapshot) -> Result<(), ProviderFailure> {
    if snapshot
        .nameserver
        .as_ref()
        .is_some_and(|address| !address.trim().is_empty())
    {
        Ok(())
    } else {
        Err(ProviderFailure::not_configured(
            "Select a NameServer to establish Broker Direct read evidence.",
        ))
    }
}

fn require_direct(scope: ConnectionScope) -> Result<(), ProviderFailure> {
    if scope == ConnectionScope::NameServer {
        Ok(())
    } else {
        Err(ProviderFailure::invalid_request(
            "This Consumer capability requires NameServer Direct scope.",
        ))
    }
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
    fn consumer_target_discovery_survives_unrelated_group_read_failures_and_keeps_exact_create_preflight() {
        use rocketmq_admin_core::core::consumer_workspace::{ConsumerConfigPresenceTarget, ConsumerInventoryItem};
        let result = ConsumerInventoryResult {
            items: vec![ConsumerInventoryItem {
                group: "offline-unrelated-group".into(),
                category: "NORMAL".into(),
                client_count: WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
                diff_total: WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
                consume_type: WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
                message_model: WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
                targets: vec![workspace_target("broker-a", "a:10911")],
            }],
            targets: vec![
                workspace_target("broker-a", "a:10911"),
                workspace_target("broker-b", "b:10911"),
            ],
            target_observation: WorkspaceObservationState::Complete,
            group_observation: WorkspaceObservationState::Complete,
            observation: WorkspaceObservationState::Partial,
            failures: [WorkspaceFailureStage::Clients, WorkspaceFailureStage::Progress]
                .into_iter()
                .map(|stage| WorkspaceTargetFailure {
                    target: "offline-unrelated-group".into(),
                    stage,
                    code: WorkspaceFailureCode::Unavailable,
                    retryable: true,
                })
                .collect(),
        };
        let inventory = map_inventory(ConnectionScope::NameServer, result).unwrap();
        assert_eq!(inventory.target_observation, ConsumerObservationState::Complete);
        assert_eq!(inventory.group_observation, ConsumerObservationState::Complete);
        assert_eq!(inventory.observation, ConsumerObservationState::Partial);
        assert_eq!(inventory.failures.len(), 2);
        assert!(matches!(
            inventory.groups[0].client_count,
            ConsumerObservation::Unknown { .. }
        ));
        assert!(matches!(inventory.groups[0].lag, ConsumerObservation::Unknown { .. }));
        let selected = vec![inventory.targets[1].clone()];
        let mut presence = ConsumerConfigPresenceResult {
            targets: vec![ConsumerConfigPresenceTarget {
                target: workspace_target("broker-b", "b:10911"),
                presence: ConsumerConfigPresence::Absent,
            }],
            failures: Vec::new(),
        };
        assert!(create_preflight_rejection(&selected, &presence).is_none());
        presence.targets[0].presence = ConsumerConfigPresence::Unknown;
        let rejected = create_preflight_rejection(&selected, &presence).unwrap();
        assert!(rejected.iter().all(|target| !target.applied));
        presence.targets.clear();
        assert!(create_preflight_rejection(&selected, &presence).is_some());
    }

    #[test]
    fn delete_preflight_uses_group_catalog_and_checks_every_authoritative_connection() {
        let authoritative = [
            common_target("broker-a", "10.0.0.1:10911"),
            common_target("broker-b", "10.0.0.2:10911"),
        ];
        let mut inventory = ConsumerInventoryResult {
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
            targets: authoritative
                .iter()
                .map(|target| workspace_target(target.broker_name(), target.broker_address()))
                .collect(),
            target_observation: WorkspaceObservationState::Complete,
            group_observation: WorkspaceObservationState::Complete,
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
        let mut connections = ConsumerConnectionsAtTargetsResult {
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

        let offline = connections.targets[0].observation.value().unwrap().clone();
        connections.targets[1].observation = WorkspaceObservation::Complete { value: offline.clone() };
        inventory.observation = WorkspaceObservationState::Partial;
        inventory.items[0].client_count = WorkspaceObservation::Unknown {
            reason: WorkspaceUnknownReason::Unavailable,
        };
        inventory.items[0].diff_total = WorkspaceObservation::Unknown {
            reason: WorkspaceUnknownReason::Unavailable,
        };
        inventory.failures = [WorkspaceFailureStage::Clients, WorkspaceFailureStage::Progress]
            .into_iter()
            .map(|stage| WorkspaceTargetFailure {
                target: "unrelated-offline-group".into(),
                stage,
                code: WorkspaceFailureCode::Unavailable,
                retryable: true,
            })
            .collect();
        assert!(delete_preflight_rejection(&authoritative, &inventory, &connections, "orders").is_none());
        for state in [WorkspaceObservationState::Partial, WorkspaceObservationState::Unknown] {
            let mut incomplete = inventory.clone();
            incomplete.group_observation = state;
            let rejected = delete_preflight_rejection(&authoritative, &incomplete, &connections, "orders").unwrap();
            assert!(rejected.iter().all(|outcome| !outcome.applied));
        }
        let mut changed_membership = inventory.clone();
        changed_membership.items[0].targets.pop();
        let rejected = delete_preflight_rejection(&authoritative, &changed_membership, &connections, "orders").unwrap();
        assert!(
            rejected
                .iter()
                .any(|outcome| outcome.stage == ConsumerFailureStage::Inventory
                    && outcome.failure == Some(ConsumerFailureCode::Conflict))
        );

        let mut online = offline;
        online.connection_count = 1;
        online
            .connections
            .push(rocketmq_admin_core::core::consumer::DashboardConsumerConnectionItem {
                client_id: "live-client".into(),
                client_addr: "client:40000".into(),
                language: "RUST".into(),
                version: 403,
                version_desc: "live".into(),
            });
        connections.targets[0].observation = WorkspaceObservation::Complete { value: online };
        let rejected = delete_preflight_rejection(&authoritative, &inventory, &connections, "orders").unwrap();
        assert!(
            rejected
                .iter()
                .any(|outcome| outcome.stage == ConsumerFailureStage::ConnectionObservation
                    && outcome.failure == Some(ConsumerFailureCode::Conflict))
        );
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
    #[test]
    fn proxy_query_scope_and_direct_read_evidence_do_not_enable_mutations_or_default_endpoints() {
        use rocketmq_dashboard_common::{ConnectionSnapshot, ConsumerQueryScope};
        let mut snapshot = ConnectionSnapshot {
            revision: 7,
            nameserver: Some("ns-selected:9876".into()),
            proxy: Some("proxy-selected:8081".into()),
            scope: ConnectionScope::Proxy,
            transport: Default::default(),
            credential_source: Default::default(),
        };
        assert_eq!(
            forwarded_address(&snapshot, ConsumerQueryScope::Configured)
                .unwrap()
                .as_deref(),
            Some("proxy-selected:8081")
        );
        assert_eq!(
            forwarded_address(&snapshot, ConsumerQueryScope::BrokerDirect).unwrap(),
            None
        );
        let evidence = ConsumerCapabilities::for_scope(ConnectionScope::Proxy).with_direct_read_evidence(true);
        assert_eq!(evidence.configuration, CapabilityAvailability::Available);
        assert_eq!(evidence.diagnostics, CapabilityAvailability::Available);
        assert_eq!(evidence.create, CapabilityAvailability::Unavailable);
        assert_eq!(evidence.delete, CapabilityAvailability::Unavailable);
        snapshot.proxy = None;
        assert!(forwarded_address(&snapshot, ConsumerQueryScope::Configured).is_err());
        snapshot.nameserver = None;
        assert!(forwarded_address(&snapshot, ConsumerQueryScope::BrokerDirect).is_err());
    }

    #[test]
    fn consumer_delete_batch_keeps_cleanup_steps_out_of_broker_target_counts() {
        use rocketmq_admin_core::core::consumer::{DashboardConsumerBatchResult, DashboardConsumerTargetResult};
        let result = |target: &str, kind: &str| DashboardConsumerTargetResult {
            target: target.into(),
            kind: kind.into(),
            success: true,
            message: String::new(),
        };
        let mut batch = DashboardConsumerBatchResult {
            consumer_group: "orders".into(),
            success: true,
            targets: vec![
                result("broker-a", "BROKER"),
                result("retry-topic", "INTERNAL_TOPIC_CLEANUP"),
                result("dlq-topic", "INTERNAL_TOPIC_CLEANUP"),
            ],
        };
        let outcome = map_batch_outcome(
            ConsumerIdentity::parse("orders").unwrap(),
            ConsumerMutationKind::Delete,
            batch.clone(),
        );
        assert_eq!(outcome.target_count(), 1);
        assert_eq!(outcome.applied_count(), 1);
        assert_eq!(outcome.counts().cleanup_steps.applied, 2);
        assert_eq!(outcome.targets.len(), 3);
        assert!(outcome.is_complete_success());
        batch.success = false;
        batch.targets[2].success = false;
        let partial = map_batch_outcome(
            ConsumerIdentity::parse("orders").unwrap(),
            ConsumerMutationKind::Delete,
            batch,
        );
        assert_eq!(partial.target_count(), 1);
        assert_eq!(partial.applied_count(), 1);
        assert_eq!(partial.counts().cleanup_steps.applied, 1);
        assert_eq!(partial.counts().cleanup_steps.unconfirmed, 1);
        assert_eq!(partial.targets.len(), 3);
        assert!(!partial.is_complete_success());
    }

    #[test]
    fn consumer_create_values_preserve_the_reviewed_flags_routing_and_nondefault_limits() {
        let command = ConsumerCreateCommand {
            group: ConsumerIdentity::parse("orders").unwrap(),
            targets: vec![common_target("a", "a:10911")],
            entries: ConsumerConfigEntries {
                retry_max_times: 3,
                retry_queue_nums: 0,
                consume_timeout_minutes: 5,
                consume_enable: false,
                consume_from_min_enable: true,
                consume_broadcast_enable: true,
                consume_message_orderly: true,
                broker_id: 7,
                which_broker_when_consume_slowly: 9,
                notify_consumer_ids_changed_enable: false,
                group_sys_flag: 23,
            },
            authorization: rocketmq_dashboard_common::ConsumerAclClassification::Authorized,
        };
        let mapped = create_values(&command).unwrap();
        assert_eq!(mapped.retry_max_times, 3);
        assert_eq!(mapped.retry_queue_nums, 0);
        assert_eq!(mapped.consume_timeout_minute, 5);
        assert!(!mapped.consume_enable);
        assert!(mapped.consume_from_min_enable);
        assert!(mapped.consume_broadcast_enable);
        assert!(mapped.consume_message_orderly);
        assert_eq!(mapped.broker_id, 7);
        assert_eq!(mapped.which_broker_when_consume_slowly, 9);
        assert!(!mapped.notify_consumer_ids_changed_enable);
        assert_eq!(mapped.group_sys_flag, 23);
        assert!(mapped.cluster_name_list.is_empty());
        assert!(mapped.broker_name_list.is_empty());
    }

    #[test]
    fn consumer_configuration_mapping_keeps_heterogeneous_and_missing_targets_distinct() {
        use rocketmq_admin_core::core::consumer_configuration::{
            ConsumerConfigDetails, ConsumerConfigValue, ConsumerRetryPolicy,
        };
        let value = ConsumerConfigValue {
            version: 7,
            retry_max_times: 16,
            retry_queue_nums: 1,
            consume_timeout_minutes: 15,
            consume_enable: true,
            consume_from_min_enable: false,
            consume_broadcast_enable: false,
            consume_message_orderly: false,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 19,
        };
        let group = ConsumerIdentity::parse("orders").unwrap();
        let targets = vec![
            ConsumerConfigTarget {
                target: workspace_target("a", "a:10911"),
                observation: WorkspaceObservation::Complete { value },
                details: ConsumerConfigDetails::default(),
                entries: Vec::new(),
            },
            ConsumerConfigTarget {
                target: workspace_target("b", "b:10911"),
                observation: WorkspaceObservation::Complete {
                    value: ConsumerConfigValue {
                        retry_max_times: 3,
                        ..value
                    },
                },
                details: ConsumerConfigDetails {
                    retry_policy: ConsumerRetryPolicy::Customized {
                        delays_ms: vec![1000, 5000],
                    },
                    subscribed_topics: vec!["orders".into()],
                    attributes: vec![("custom".into(), "kept".into())],
                    truncated: false,
                },
                entries: Vec::new(),
            },
            ConsumerConfigTarget {
                target: workspace_target("c", "c:10911"),
                observation: WorkspaceObservation::Unknown {
                    reason: WorkspaceUnknownReason::Unavailable,
                },
                details: ConsumerConfigDetails::default(),
                entries: Vec::new(),
            },
        ];
        let failure = WorkspaceTargetFailure {
            target: "c".into(),
            stage: WorkspaceFailureStage::Configuration,
            code: WorkspaceFailureCode::Unavailable,
            retryable: true,
        };
        let mapped = map_configuration(group, targets, WorkspaceObservationState::Partial, vec![failure]).unwrap();
        assert_eq!(mapped.snapshots.len(), 2);
        assert_eq!(mapped.failures.len(), 1);
        assert_eq!(mapped.observation, ConsumerObservationState::Partial);
        assert_eq!(
            mapped.differing_fields(),
            ["retry_max_times", "retry_policy", "subscribed_topics", "attributes"]
        );
        assert_eq!(mapped.snapshots[1].entries.group_sys_flag, 19);
    }
}
