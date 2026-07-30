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
use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::Utc;
use rocketmq_admin_core::core::consumer::ConsumerMutationAdmin;
use rocketmq_admin_core::core::consumer::ConsumerQueryAdmin;
use rocketmq_admin_core::core::consumer::PatchSubscriptionGroupConfigOutcome;
use rocketmq_admin_core::core::consumer::PatchSubscriptionGroupConfigRequest;
use rocketmq_admin_core::core::consumer::QuerySubscriptionGroupConfigCasRequest;
use rocketmq_admin_core::core::consumer::SubscriptionGroupConfigCasPatch;
use rocketmq_admin_core::core::consumer::SubscriptionGroupConfigCasState;
use rocketmq_admin_core::core::topic::GetTopicRouteRequest;
use rocketmq_admin_core::core::topic::TopicQueryAdmin;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminBuilder;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;
use rocketmq_admin_core::read_client_adapter::TelemetryHandle;
use rocketmq_runtime::ChildServiceContext;
use serde::Deserialize;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::Mutex;

use self::journal::OperationDirection;
use self::journal::SubscriptionGroupBeforeBroker;
use self::journal::SubscriptionGroupBeforeState;
use self::journal::SubscriptionGroupJournal;
use super::DriverFuture;
use super::SubscriptionGroupPatch;
use super::SubscriptionGroupPatchApplyOutcome;
use super::SubscriptionGroupPatchClient;
use super::SubscriptionGroupPatchRestore;
use super::SubscriptionGroupPatchState;
use super::SubscriptionGroupPatchWrite;
use crate::AgentStoreError;
use crate::ExecutionAgentError;
use crate::config::BrokerAdminDriverConfig;

mod journal;

const MASTER_BROKER_ID: u64 = 0;
const RETRY_TOPIC_PREFIX: &str = "%RETRY%";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(super) struct SubscriptionGroupSafetyState {
    consume_enable: bool,
    consume_from_min_enable: bool,
    consume_broadcast_enable: bool,
    consume_message_orderly: bool,
    broker_id: u64,
    which_broker_when_consume_slowly: u64,
    notify_consumer_ids_changed_enable: bool,
    group_sys_flag: i32,
}

#[derive(Clone, Debug)]
struct LiveSubscriptionGroupBroker {
    broker_addr: String,
    state: SubscriptionGroupConfigCasState,
}

#[derive(Clone, Debug)]
struct LiveSubscriptionGroup {
    brokers: Vec<LiveSubscriptionGroupBroker>,
    aggregate: SubscriptionGroupPatchState,
}

/// Production Admin Core adapter for version-checked Subscription Group
/// changes.
///
/// Targets are resolved through the group's retry Topic. Every Broker must
/// expose the same version, retry values, and safety fields before the
/// operation is eligible. Before states and per-Broker outcomes are persisted
/// before any inverse operation can run.
pub(crate) struct ProductionSubscriptionGroupPatchClient {
    read_admin: Mutex<ReadAdminSession>,
    mutation_admin: Mutex<MutationAdminSession>,
    journal: SubscriptionGroupJournal,
    _client_runtime: Arc<ClientRuntime>,
}

impl ProductionSubscriptionGroupPatchClient {
    pub(crate) async fn start(
        config: &BrokerAdminDriverConfig,
        pool: PgPool,
        context: ChildServiceContext,
    ) -> Result<Self, ExecutionAgentError> {
        let client_runtime = ClientRuntime::try_new(
            context.child("subscription-group-admin-client"),
            ClientRuntimeConfig {
                shutdown_timeout: config.shutdown_timeout,
                ..ClientRuntimeConfig::default()
            },
            TelemetryHandle::noop(),
        )
        .map_err(|_| ExecutionAgentError::Configuration)?;
        let timeout_millis = duration_millis(config.request_timeout)?;
        let mut read_builder = ReadAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-subscription-group-read")
            .instance_name("rocketmq-sre-execution-agent-subscription-group-read")
            .timeout_millis(timeout_millis)
            .use_tls(config.use_tls);
        if let Some(credentials) = &config.read_credentials {
            read_builder = read_builder.credentials(credentials.clone());
        }
        let mut read_admin = read_builder
            .build_and_start()
            .await
            .map_err(|_| ExecutionAgentError::Configuration)?;

        let mut mutation_builder = MutationAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-subscription-group-mutation")
            .instance_name("rocketmq-sre-execution-agent-subscription-group-mutation")
            .timeout_millis(timeout_millis)
            .use_tls(config.use_tls);
        if let Some(credentials) = &config.mutation_credentials {
            mutation_builder = mutation_builder.credentials(credentials.clone());
        }
        let mutation_admin = match mutation_builder.build_and_start().await {
            Ok(session) => session,
            Err(_) => {
                read_admin.shutdown().await;
                return Err(ExecutionAgentError::Configuration);
            }
        };
        Ok(Self {
            read_admin: Mutex::new(read_admin),
            mutation_admin: Mutex::new(mutation_admin),
            journal: SubscriptionGroupJournal::new(pool),
            _client_runtime: client_runtime,
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.read_admin.lock().await.shutdown().await;
        self.mutation_admin.lock().await.shutdown().await;
    }

    async fn live_state(&self, group: &str) -> Result<LiveSubscriptionGroup, ExecutionAgentError> {
        let retry_topic = format!("{RETRY_TOPIC_PREFIX}{group}");
        let route_request =
            GetTopicRouteRequest::try_new(&retry_topic).map_err(|_| ExecutionAgentError::InvalidRequest)?;
        let (targets, broker_states) = {
            let mut admin = self.read_admin.lock().await;
            let route = admin
                .get_topic_route(&route_request)
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?
                .ok_or(ExecutionAgentError::DriverFailed)?;
            let mut targets = BTreeSet::new();
            for broker in route.brokers {
                let master_addr = broker
                    .broker_addrs
                    .get(&MASTER_BROKER_ID)
                    .ok_or(ExecutionAgentError::DriverFailed)?;
                targets.insert(master_addr.clone());
            }
            if targets.is_empty() {
                return Err(ExecutionAgentError::DriverFailed);
            }
            let mut broker_states = Vec::with_capacity(targets.len());
            for broker_addr in &targets {
                let state = ConsumerQueryAdmin::query_config_cas_state(
                    &mut *admin,
                    &QuerySubscriptionGroupConfigCasRequest::try_new(broker_addr, group)
                        .map_err(|_| ExecutionAgentError::InvalidRequest)?,
                )
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?;
                broker_states.push(LiveSubscriptionGroupBroker {
                    broker_addr: broker_addr.clone(),
                    state,
                });
            }
            (targets, broker_states)
        };

        let version = broker_states
            .iter()
            .map(|broker| broker.state.version)
            .max()
            .ok_or(ExecutionAgentError::DriverFailed)?;
        let first = broker_states.first().ok_or(ExecutionAgentError::DriverFailed)?;
        let values = state_patch(first.state);
        let safety = safety_state(first.state);
        let configuration_consistent = broker_states.iter().all(|broker| {
            broker.state.version == first.state.version
                && state_patch(broker.state) == values
                && safety_state(broker.state) == safety
        });
        let last_operation_id = if configuration_consistent {
            self.journal.last_applied_operation(group, version, &targets).await?
        } else {
            None
        };
        let permissions_unchanged = match &last_operation_id {
            Some(operation_id) => match self.journal.load_before_by_operation(operation_id).await {
                Ok(before) => safety_matches_before(&broker_states, &before),
                Err(AgentStoreError::NotFound) => configuration_consistent,
                Err(error) => return Err(error.into()),
            },
            None => configuration_consistent,
        };
        Ok(LiveSubscriptionGroup {
            brokers: broker_states,
            aggregate: SubscriptionGroupPatchState {
                version,
                values,
                retry_semantics_known: configuration_consistent,
                permissions_unchanged,
                last_operation_id,
            },
        })
    }

    async fn apply_one(
        &self,
        broker_addr: &str,
        group: &str,
        expected_version: u64,
        patch: &SubscriptionGroupPatch,
    ) -> Result<SubscriptionGroupPatchApplyOutcome, ExecutionAgentError> {
        let request = PatchSubscriptionGroupConfigRequest::try_new(
            broker_addr,
            group,
            expected_version,
            SubscriptionGroupConfigCasPatch {
                retry_max_times: patch.retry_max_times,
                retry_queue_nums: patch.retry_queue_nums,
                consume_timeout_minutes: patch.consume_timeout_minutes,
            },
        )
        .map_err(|_| ExecutionAgentError::InvalidRequest)?;
        let outcome = {
            let mut admin = self.mutation_admin.lock().await;
            admin
                .patch_config_if_version(&request)
                .await
                .map_err(|_| ExecutionAgentError::DriverFailed)?
        };
        Ok(match outcome {
            PatchSubscriptionGroupConfigOutcome::Applied {
                previous_version,
                version,
            } => SubscriptionGroupPatchApplyOutcome::Applied {
                previous_version,
                version,
            },
            PatchSubscriptionGroupConfigOutcome::VersionConflict {
                expected_version,
                actual_version,
            } => SubscriptionGroupPatchApplyOutcome::VersionConflict {
                expected_version,
                actual_version,
            },
        })
    }

    async fn rollback_known_forward_effects(
        &self,
        request: &SubscriptionGroupPatchWrite,
        applied: &[(SubscriptionGroupBeforeBroker, u64)],
    ) -> Result<(), ExecutionAgentError> {
        for (broker, current_version) in applied.iter().rev() {
            let outcome = self
                .apply_one(&broker.broker_addr, &request.group, *current_version, &broker.before)
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            self.journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &request.group,
                    &broker.broker_addr,
                    &request.operation_id,
                    OperationDirection::Compensation,
                    *current_version,
                    outcome,
                    Utc::now(),
                )
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            if !matches!(outcome, SubscriptionGroupPatchApplyOutcome::Applied { .. }) {
                return Err(ExecutionAgentError::DriverUnknown);
            }
        }
        Ok(())
    }

    async fn reapply_known_compensation_effects(
        &self,
        request: &SubscriptionGroupPatchRestore,
        before: &SubscriptionGroupBeforeState,
        applied: &[(String, u64)],
    ) -> Result<(), ExecutionAgentError> {
        for (broker_addr, current_version) in applied.iter().rev() {
            let outcome = self
                .apply_one(broker_addr, &request.group, *current_version, &before.forward_patch)
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            self.journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &request.group,
                    broker_addr,
                    &request.operation_id,
                    OperationDirection::Forward,
                    *current_version,
                    outcome,
                    Utc::now(),
                )
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            if !matches!(outcome, SubscriptionGroupPatchApplyOutcome::Applied { .. }) {
                return Err(ExecutionAgentError::DriverUnknown);
            }
        }
        Ok(())
    }
}

impl SubscriptionGroupPatchClient for ProductionSubscriptionGroupPatchClient {
    fn subscription_group_patch_state<'a>(&'a self, group: &'a str) -> DriverFuture<'a, SubscriptionGroupPatchState> {
        Box::pin(async move { self.live_state(group).await.map(|live| live.aggregate) })
    }

    fn patch_subscription_group<'a>(
        &'a self,
        request: &'a SubscriptionGroupPatchWrite,
    ) -> DriverFuture<'a, SubscriptionGroupPatchApplyOutcome> {
        Box::pin(async move {
            let live = self.live_state(&request.group).await?;
            if !live.aggregate.retry_semantics_known || !live.aggregate.permissions_unchanged {
                return Err(ExecutionAgentError::DriverFailed);
            }
            if live.aggregate.version != request.expected_version {
                return Ok(SubscriptionGroupPatchApplyOutcome::VersionConflict {
                    expected_version: request.expected_version,
                    actual_version: live.aggregate.version,
                });
            }
            if patch_matches(&request.patch, &live.aggregate.values) {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let brokers = live
                .brokers
                .iter()
                .map(|broker| {
                    Ok(SubscriptionGroupBeforeBroker {
                        broker_addr: broker.broker_addr.clone(),
                        version: broker.state.version,
                        before: select_before_values(state_patch(broker.state), &request.patch)?,
                        safety: safety_state(broker.state),
                    })
                })
                .collect::<Result<Vec<_>, ExecutionAgentError>>()?;
            let before = SubscriptionGroupBeforeState {
                group: request.group.clone(),
                operation_id: request.operation_id.clone(),
                expected_version: request.expected_version,
                brokers,
                forward_patch: request.patch.clone(),
            };
            let before = self
                .journal
                .persist_before(request.execution_id, request.plan_step_id, &before, Utc::now())
                .await?;
            let mut applied = Vec::new();
            for broker in &before.brokers {
                let outcome = match self
                    .apply_one(&broker.broker_addr, &request.group, broker.version, &request.patch)
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        let _ = self.rollback_known_forward_effects(request, &applied).await;
                        return Err(ExecutionAgentError::DriverUnknown);
                    }
                };
                if let SubscriptionGroupPatchApplyOutcome::Applied { version, .. } = outcome {
                    applied.push((broker.clone(), version));
                }
                if self
                    .journal
                    .append_result(
                        request.execution_id,
                        request.plan_step_id,
                        &request.group,
                        &broker.broker_addr,
                        &request.operation_id,
                        OperationDirection::Forward,
                        broker.version,
                        outcome,
                        Utc::now(),
                    )
                    .await
                    .is_err()
                {
                    let _ = self.rollback_known_forward_effects(request, &applied).await;
                    return Err(ExecutionAgentError::DriverUnknown);
                }
                if let SubscriptionGroupPatchApplyOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } = outcome
                {
                    if applied.is_empty() {
                        return Ok(SubscriptionGroupPatchApplyOutcome::VersionConflict {
                            expected_version,
                            actual_version,
                        });
                    }
                    let _ = self.rollback_known_forward_effects(request, &applied).await;
                    return Err(ExecutionAgentError::DriverUnknown);
                }
            }
            let version = request
                .expected_version
                .checked_add(1)
                .ok_or(ExecutionAgentError::DriverUnknown)?;
            Ok(SubscriptionGroupPatchApplyOutcome::Applied {
                previous_version: request.expected_version,
                version,
            })
        })
    }

    fn restore_subscription_group<'a>(
        &'a self,
        request: &'a SubscriptionGroupPatchRestore,
    ) -> DriverFuture<'a, SubscriptionGroupPatchApplyOutcome> {
        Box::pin(async move {
            let before = self
                .journal
                .load_before(request.execution_id, request.plan_step_id)
                .await?;
            if before.group != request.group {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let live = self.live_state(&request.group).await?;
            let current_targets = live
                .brokers
                .iter()
                .map(|broker| broker.broker_addr.clone())
                .collect::<BTreeSet<_>>();
            let before_targets = before
                .brokers
                .iter()
                .map(|broker| broker.broker_addr.clone())
                .collect::<BTreeSet<_>>();
            if current_targets != before_targets
                || !live.aggregate.retry_semantics_known
                || !live.aggregate.permissions_unchanged
                || !patch_matches(&before.forward_patch, &live.aggregate.values)
            {
                return Ok(SubscriptionGroupPatchApplyOutcome::VersionConflict {
                    expected_version: before.expected_version.saturating_add(1),
                    actual_version: live.aggregate.version,
                });
            }
            let before_by_addr = before
                .brokers
                .iter()
                .map(|broker| (broker.broker_addr.as_str(), &broker.before))
                .collect::<BTreeMap<_, _>>();
            let previous_version = live.aggregate.version;
            let mut applied = Vec::new();
            for broker in &live.brokers {
                let inverse = before_by_addr
                    .get(broker.broker_addr.as_str())
                    .ok_or(ExecutionAgentError::DriverFailed)?;
                let outcome = match self
                    .apply_one(&broker.broker_addr, &request.group, broker.state.version, inverse)
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        let _ = self
                            .reapply_known_compensation_effects(request, &before, &applied)
                            .await;
                        return Err(ExecutionAgentError::DriverUnknown);
                    }
                };
                if let SubscriptionGroupPatchApplyOutcome::Applied { version, .. } = outcome {
                    applied.push((broker.broker_addr.clone(), version));
                }
                if self
                    .journal
                    .append_result(
                        request.execution_id,
                        request.plan_step_id,
                        &request.group,
                        &broker.broker_addr,
                        &request.operation_id,
                        OperationDirection::Compensation,
                        broker.state.version,
                        outcome,
                        Utc::now(),
                    )
                    .await
                    .is_err()
                {
                    let _ = self
                        .reapply_known_compensation_effects(request, &before, &applied)
                        .await;
                    return Err(ExecutionAgentError::DriverUnknown);
                }
                if let SubscriptionGroupPatchApplyOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } = outcome
                {
                    if applied.is_empty() {
                        return Ok(SubscriptionGroupPatchApplyOutcome::VersionConflict {
                            expected_version,
                            actual_version,
                        });
                    }
                    let _ = self
                        .reapply_known_compensation_effects(request, &before, &applied)
                        .await;
                    return Err(ExecutionAgentError::DriverUnknown);
                }
            }
            let version = previous_version
                .checked_add(1)
                .ok_or(ExecutionAgentError::DriverUnknown)?;
            Ok(SubscriptionGroupPatchApplyOutcome::Applied {
                previous_version,
                version,
            })
        })
    }
}

fn state_patch(state: SubscriptionGroupConfigCasState) -> SubscriptionGroupPatch {
    SubscriptionGroupPatch {
        retry_max_times: Some(state.retry_max_times),
        retry_queue_nums: Some(state.retry_queue_nums),
        consume_timeout_minutes: Some(state.consume_timeout_minutes),
    }
}

const fn safety_state(state: SubscriptionGroupConfigCasState) -> SubscriptionGroupSafetyState {
    SubscriptionGroupSafetyState {
        consume_enable: state.consume_enable,
        consume_from_min_enable: state.consume_from_min_enable,
        consume_broadcast_enable: state.consume_broadcast_enable,
        consume_message_orderly: state.consume_message_orderly,
        broker_id: state.broker_id,
        which_broker_when_consume_slowly: state.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: state.notify_consumer_ids_changed_enable,
        group_sys_flag: state.group_sys_flag,
    }
}

fn safety_matches_before(live: &[LiveSubscriptionGroupBroker], before: &SubscriptionGroupBeforeState) -> bool {
    let before_by_addr = before
        .brokers
        .iter()
        .map(|broker| (broker.broker_addr.as_str(), broker.safety))
        .collect::<BTreeMap<_, _>>();
    live.iter().all(|broker| {
        before_by_addr
            .get(broker.broker_addr.as_str())
            .is_some_and(|expected| *expected == safety_state(broker.state))
    })
}

fn select_before_values(
    live: SubscriptionGroupPatch,
    requested: &SubscriptionGroupPatch,
) -> Result<SubscriptionGroupPatch, ExecutionAgentError> {
    Ok(SubscriptionGroupPatch {
        retry_max_times: requested
            .retry_max_times
            .map(|_| live.retry_max_times)
            .transpose_required()?,
        retry_queue_nums: requested
            .retry_queue_nums
            .map(|_| live.retry_queue_nums)
            .transpose_required()?,
        consume_timeout_minutes: requested
            .consume_timeout_minutes
            .map(|_| live.consume_timeout_minutes)
            .transpose_required()?,
    })
}

trait RequiredOption<T> {
    fn transpose_required(self) -> Result<Option<T>, ExecutionAgentError>;
}

impl<T> RequiredOption<T> for Option<Option<T>> {
    fn transpose_required(self) -> Result<Option<T>, ExecutionAgentError> {
        match self {
            Some(Some(value)) => Ok(Some(value)),
            Some(None) => Err(ExecutionAgentError::DriverFailed),
            None => Ok(None),
        }
    }
}

fn patch_matches(patch: &SubscriptionGroupPatch, state: &SubscriptionGroupPatch) -> bool {
    patch
        .retry_max_times
        .is_none_or(|value| state.retry_max_times == Some(value))
        && patch
            .retry_queue_nums
            .is_none_or(|value| state.retry_queue_nums == Some(value))
        && patch
            .consume_timeout_minutes
            .is_none_or(|value| state.consume_timeout_minutes == Some(value))
}

fn duration_millis(duration: std::time::Duration) -> Result<u64, ExecutionAgentError> {
    u64::try_from(duration.as_millis()).map_err(|_| ExecutionAgentError::Configuration)
}

#[cfg(test)]
#[path = "production_subscription_group_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "production_subscription_group_e2e_tests.rs"]
mod e2e_tests;
