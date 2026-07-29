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
use rocketmq_admin_core::core::topic::GetTopicRouteRequest;
use rocketmq_admin_core::core::topic::PatchTopicConfigOutcome;
use rocketmq_admin_core::core::topic::PatchTopicConfigRequest;
use rocketmq_admin_core::core::topic::QueryTopicConfigCasRequest;
use rocketmq_admin_core::core::topic::TopicConfigCasPatch;
use rocketmq_admin_core::core::topic::TopicConfigCasState;
use rocketmq_admin_core::core::topic::TopicMutationAdmin;
use rocketmq_admin_core::core::topic::TopicQueryAdmin;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminBuilder;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;
use rocketmq_admin_core::read_client_adapter::ClientRuntime;
use rocketmq_admin_core::read_client_adapter::ClientRuntimeConfig;
use rocketmq_admin_core::read_client_adapter::ReadAdminBuilder;
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;
use rocketmq_runtime::ChildServiceContext;
use sqlx::PgPool;
use tokio::sync::Mutex;

use self::journal::OperationDirection;
use self::journal::TopicBeforeBroker;
use self::journal::TopicBeforeState;
use self::journal::TopicConfigJournal;
use super::DriverFuture;
use super::TopicConfigPatch;
use super::TopicConfigPatchApplyOutcome;
use super::TopicConfigPatchClient;
use super::TopicConfigPatchRestore;
use super::TopicConfigPatchState;
use super::TopicConfigPatchWrite;
use crate::ExecutionAgentError;
use crate::config::BrokerAdminDriverConfig;

mod journal;

const MASTER_BROKER_ID: u64 = 0;

#[derive(Clone, Debug)]
struct LiveTopicBroker {
    broker_addr: String,
    state: TopicConfigCasState,
}

#[derive(Clone, Debug)]
struct LiveTopicConfig {
    brokers: Vec<LiveTopicBroker>,
    aggregate: TopicConfigPatchState,
}

/// Production RocketMQ Admin Core adapter for version-checked Topic changes.
///
/// The adapter resolves the complete Topic route through the read identity,
/// requires every Broker to expose the same closed configuration and version,
/// persists every before state, and then calls only the dedicated CAS method.
/// If a multi-Broker operation partially commits, known effects are inverted
/// and the result remains unknown because independent version counters cannot
/// be rolled back.
pub(crate) struct ProductionTopicConfigPatchClient {
    read_admin: Mutex<ReadAdminSession>,
    mutation_admin: Mutex<MutationAdminSession>,
    journal: TopicConfigJournal,
    _client_runtime: Arc<ClientRuntime>,
}

impl ProductionTopicConfigPatchClient {
    pub(crate) async fn start(
        config: &BrokerAdminDriverConfig,
        pool: PgPool,
        context: ChildServiceContext,
    ) -> Result<Self, ExecutionAgentError> {
        let client_runtime = ClientRuntime::new(
            context.child("topic-admin-client"),
            ClientRuntimeConfig {
                shutdown_timeout: config.shutdown_timeout,
            },
        );
        let timeout_millis = duration_millis(config.request_timeout)?;
        let mut read_builder = ReadAdminBuilder::new(Arc::clone(&client_runtime))
            .namesrv_addr(config.namesrv_addr.clone())
            .admin_group("rocketmq-sre-agent-topic-read")
            .instance_name("rocketmq-sre-execution-agent-topic-read")
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
            .admin_group("rocketmq-sre-agent-topic-mutation")
            .instance_name("rocketmq-sre-execution-agent-topic-mutation")
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
            journal: TopicConfigJournal::new(pool),
            _client_runtime: client_runtime,
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.read_admin.lock().await.shutdown().await;
        self.mutation_admin.lock().await.shutdown().await;
    }

    async fn live_state(&self, topic: &str) -> Result<LiveTopicConfig, ExecutionAgentError> {
        let request = GetTopicRouteRequest::try_new(topic).map_err(|_| ExecutionAgentError::InvalidRequest)?;
        let (targets, broker_states) = {
            let mut admin = self.read_admin.lock().await;
            let route = admin
                .get_topic_route(&request)
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
                let state = admin
                    .query_config_cas_state(
                        &QueryTopicConfigCasRequest::try_new(broker_addr, topic)
                            .map_err(|_| ExecutionAgentError::InvalidRequest)?,
                    )
                    .await
                    .map_err(|_| ExecutionAgentError::DriverFailed)?;
                broker_states.push(LiveTopicBroker {
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
        let configuration_consistent = broker_states
            .iter()
            .all(|broker| broker.state.version == first.state.version && state_patch(broker.state) == values);
        let last_operation_id = if configuration_consistent {
            self.journal.last_applied_operation(topic, version, &targets).await?
        } else {
            None
        };
        Ok(LiveTopicConfig {
            brokers: broker_states,
            aggregate: TopicConfigPatchState {
                version,
                values,
                configuration_consistent,
                last_operation_id,
            },
        })
    }

    async fn apply_one(
        &self,
        broker_addr: &str,
        topic: &str,
        expected_version: u64,
        patch: &TopicConfigPatch,
    ) -> Result<TopicConfigPatchApplyOutcome, ExecutionAgentError> {
        let request = PatchTopicConfigRequest::try_new(
            broker_addr,
            topic,
            expected_version,
            TopicConfigCasPatch {
                read_queue_nums: patch.read_queue_nums,
                write_queue_nums: patch.write_queue_nums,
                order: patch.order,
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
            PatchTopicConfigOutcome::Applied {
                previous_version,
                version,
            } => TopicConfigPatchApplyOutcome::Applied {
                previous_version,
                version,
            },
            PatchTopicConfigOutcome::VersionConflict {
                expected_version,
                actual_version,
            } => TopicConfigPatchApplyOutcome::VersionConflict {
                expected_version,
                actual_version,
            },
        })
    }

    async fn rollback_known_forward_effects(
        &self,
        request: &TopicConfigPatchWrite,
        applied: &[(TopicBeforeBroker, u64)],
    ) -> Result<(), ExecutionAgentError> {
        for (broker, current_version) in applied.iter().rev() {
            let outcome = self
                .apply_one(&broker.broker_addr, &request.topic, *current_version, &broker.before)
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            self.journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &request.topic,
                    &broker.broker_addr,
                    &request.operation_id,
                    OperationDirection::Compensation,
                    *current_version,
                    outcome,
                    Utc::now(),
                )
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            if !matches!(outcome, TopicConfigPatchApplyOutcome::Applied { .. }) {
                return Err(ExecutionAgentError::DriverUnknown);
            }
        }
        Ok(())
    }

    async fn reapply_known_compensation_effects(
        &self,
        request: &TopicConfigPatchRestore,
        before: &TopicBeforeState,
        applied: &[(String, u64)],
    ) -> Result<(), ExecutionAgentError> {
        for (broker_addr, current_version) in applied.iter().rev() {
            let outcome = self
                .apply_one(broker_addr, &request.topic, *current_version, &before.forward_patch)
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            self.journal
                .append_result(
                    request.execution_id,
                    request.plan_step_id,
                    &request.topic,
                    broker_addr,
                    &request.operation_id,
                    OperationDirection::Forward,
                    *current_version,
                    outcome,
                    Utc::now(),
                )
                .await
                .map_err(|_| ExecutionAgentError::DriverUnknown)?;
            if !matches!(outcome, TopicConfigPatchApplyOutcome::Applied { .. }) {
                return Err(ExecutionAgentError::DriverUnknown);
            }
        }
        Ok(())
    }
}

impl TopicConfigPatchClient for ProductionTopicConfigPatchClient {
    fn topic_config_patch_state<'a>(&'a self, topic: &'a str) -> DriverFuture<'a, TopicConfigPatchState> {
        Box::pin(async move { self.live_state(topic).await.map(|live| live.aggregate) })
    }

    fn patch_topic_config<'a>(
        &'a self,
        request: &'a TopicConfigPatchWrite,
    ) -> DriverFuture<'a, TopicConfigPatchApplyOutcome> {
        Box::pin(async move {
            let live = self.live_state(&request.topic).await?;
            if !live.aggregate.configuration_consistent {
                return Err(ExecutionAgentError::DriverFailed);
            }
            if live.aggregate.version != request.expected_version {
                return Ok(TopicConfigPatchApplyOutcome::VersionConflict {
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
                    Ok(TopicBeforeBroker {
                        broker_addr: broker.broker_addr.clone(),
                        version: broker.state.version,
                        before: select_before_values(state_patch(broker.state), &request.patch)?,
                    })
                })
                .collect::<Result<Vec<_>, ExecutionAgentError>>()?;
            let before = TopicBeforeState {
                topic: request.topic.clone(),
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
                    .apply_one(&broker.broker_addr, &request.topic, broker.version, &request.patch)
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        let _ = self.rollback_known_forward_effects(request, &applied).await;
                        return Err(ExecutionAgentError::DriverUnknown);
                    }
                };
                if let TopicConfigPatchApplyOutcome::Applied { version, .. } = outcome {
                    applied.push((broker.clone(), version));
                }
                if self
                    .journal
                    .append_result(
                        request.execution_id,
                        request.plan_step_id,
                        &request.topic,
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
                if let TopicConfigPatchApplyOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } = outcome
                {
                    if applied.is_empty() {
                        return Ok(TopicConfigPatchApplyOutcome::VersionConflict {
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
            Ok(TopicConfigPatchApplyOutcome::Applied {
                previous_version: request.expected_version,
                version,
            })
        })
    }

    fn restore_topic_config<'a>(
        &'a self,
        request: &'a TopicConfigPatchRestore,
    ) -> DriverFuture<'a, TopicConfigPatchApplyOutcome> {
        Box::pin(async move {
            let before = self
                .journal
                .load_before(request.execution_id, request.plan_step_id)
                .await?;
            if before.topic != request.topic {
                return Err(ExecutionAgentError::InvalidRequest);
            }
            let live = self.live_state(&request.topic).await?;
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
                || !live.aggregate.configuration_consistent
                || !patch_matches(&before.forward_patch, &live.aggregate.values)
            {
                return Ok(TopicConfigPatchApplyOutcome::VersionConflict {
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
                    .apply_one(&broker.broker_addr, &request.topic, broker.state.version, inverse)
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
                if let TopicConfigPatchApplyOutcome::Applied { version, .. } = outcome {
                    applied.push((broker.broker_addr.clone(), version));
                }
                if self
                    .journal
                    .append_result(
                        request.execution_id,
                        request.plan_step_id,
                        &request.topic,
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
                if let TopicConfigPatchApplyOutcome::VersionConflict {
                    expected_version,
                    actual_version,
                } = outcome
                {
                    if applied.is_empty() {
                        return Ok(TopicConfigPatchApplyOutcome::VersionConflict {
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
            Ok(TopicConfigPatchApplyOutcome::Applied {
                previous_version,
                version,
            })
        })
    }
}

fn state_patch(state: TopicConfigCasState) -> TopicConfigPatch {
    TopicConfigPatch {
        read_queue_nums: Some(state.read_queue_nums),
        write_queue_nums: Some(state.write_queue_nums),
        order: Some(state.order),
    }
}

fn select_before_values(
    live: TopicConfigPatch,
    requested: &TopicConfigPatch,
) -> Result<TopicConfigPatch, ExecutionAgentError> {
    Ok(TopicConfigPatch {
        read_queue_nums: requested
            .read_queue_nums
            .map(|_| live.read_queue_nums)
            .transpose_required()?,
        write_queue_nums: requested
            .write_queue_nums
            .map(|_| live.write_queue_nums)
            .transpose_required()?,
        order: requested.order.map(|_| live.order).transpose_required()?,
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

fn patch_matches(patch: &TopicConfigPatch, state: &TopicConfigPatch) -> bool {
    patch
        .read_queue_nums
        .is_none_or(|value| state.read_queue_nums == Some(value))
        && patch
            .write_queue_nums
            .is_none_or(|value| state.write_queue_nums == Some(value))
        && patch.order.is_none_or(|value| state.order == Some(value))
}

fn duration_millis(duration: std::time::Duration) -> Result<u64, ExecutionAgentError> {
    u64::try_from(duration.as_millis()).map_err(|_| ExecutionAgentError::Configuration)
}

#[cfg(test)]
#[path = "production_topic_config_tests.rs"]
mod tests;
