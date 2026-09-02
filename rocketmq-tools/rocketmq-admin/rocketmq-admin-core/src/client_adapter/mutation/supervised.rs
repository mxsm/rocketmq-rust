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

//! Supervised, preflight-bound mutation adapter operations.

use super::*;
use std::collections::BTreeMap;

impl SupervisedMutationAdmin for MutationAdminSession {
    fn preflight_topic<'a>(
        &'a mut self,
        request: &'a TopicMutationPreflightRequest,
    ) -> AdminFuture<'a, TopicMutationPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_topic_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request).await
        })
    }

    fn preflight_topic_targets<'a>(
        &'a mut self,
        request: &'a TopicMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> AdminFuture<'a, TopicMutationPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_topic_targets_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request, broker_names)
                .await
        })
    }

    fn execute_topic<'a>(&'a mut self, plan: &'a TopicMutationPlan) -> AdminFuture<'a, MetadataMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_topic_checked(&self.inner.inner, &self.plan_seal, plan).await
        })
    }

    fn preflight_subscription_group<'a>(
        &'a mut self,
        request: &'a SubscriptionGroupMutationPreflightRequest,
    ) -> AdminFuture<'a, SubscriptionGroupMutationPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_subscription_group_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request).await
        })
    }

    fn preflight_subscription_group_targets<'a>(
        &'a mut self,
        request: &'a SubscriptionGroupMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> AdminFuture<'a, SubscriptionGroupMutationPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_subscription_group_targets_with_admin(
                &self.inner.inner,
                Arc::clone(&self.plan_seal),
                request,
                broker_names,
            )
            .await
        })
    }

    fn execute_subscription_group<'a>(
        &'a mut self,
        plan: &'a SubscriptionGroupMutationPlan,
    ) -> AdminFuture<'a, MetadataMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_subscription_group_checked(&self.inner.inner, &self.plan_seal, plan).await
        })
    }

    fn preview_offset_reset<'a>(
        &'a mut self,
        request: &'a OffsetResetPreviewRequest,
    ) -> AdminFuture<'a, OffsetResetPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preview_offset_reset_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request).await
        })
    }

    fn execute_offset_reset<'a>(&'a mut self, plan: &'a OffsetResetPlan) -> AdminFuture<'a, OffsetResetOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_offset_reset_checked(&self.inner.inner, &self.plan_seal, plan).await
        })
    }

    fn preflight_broker_config<'a>(&'a mut self, cluster: &'a str) -> AdminFuture<'a, BrokerMutationConfigPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_broker_config_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), cluster).await
        })
    }

    fn preflight_broker_config_target<'a>(
        &'a mut self,
        cluster: &'a str,
        broker_name: &'a str,
    ) -> AdminFuture<'a, BrokerMutationConfigPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_broker_config_target_with_admin(
                &self.inner.inner,
                Arc::clone(&self.plan_seal),
                cluster,
                broker_name,
            )
            .await
        })
    }

    fn execute_broker_config_patch<'a>(
        &'a mut self,
        plan: &'a BrokerMutationConfigPlan,
        patch: BrokerMutationConfigPatch,
    ) -> AdminFuture<'a, MetadataMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            self.ensure_plan_owned(&plan.seal)?;
            let properties = broker_patch_properties(patch)?;
            let mut outcome = MetadataMutationOutcome {
                failures: plan.failures.clone(),
                ..MetadataMutationOutcome::default()
            };
            for (broker_name, broker_addr, state) in &plan.targets {
                match self
                    .inner
                    .inner
                    .patch_broker_config_if_generation(
                        broker_addr.as_str().into(),
                        state.generation,
                        properties.clone(),
                    )
                    .await
                {
                    Ok(ClientBrokerConfigPatchOutcome::Applied { generation, .. }) => {
                        outcome.targets.push(MetadataMutationTargetOutcome {
                            broker_name: broker_name.clone(),
                            expected_state: ExpectedState::Present {
                                version: state.generation,
                            },
                            resulting_state: Some(ExpectedState::Present { version: generation }),
                            applied: true,
                            changed: true,
                            persistence: MutationPersistenceState::Persisted,
                            verification: MutationVerificationState::NotPerformed,
                            failure: None,
                            retryable: false,
                        });
                    }
                    Ok(ClientBrokerConfigPatchOutcome::GenerationConflict { actual_generation, .. }) => {
                        outcome.targets.push(MetadataMutationTargetOutcome {
                            broker_name: broker_name.clone(),
                            expected_state: ExpectedState::Present {
                                version: state.generation,
                            },
                            resulting_state: Some(ExpectedState::Present {
                                version: actual_generation,
                            }),
                            applied: false,
                            changed: false,
                            persistence: MutationPersistenceState::NotRequired,
                            verification: MutationVerificationState::NotPerformed,
                            failure: Some(MutationFailureCode::Conflict),
                            retryable: false,
                        })
                    }
                    Err(error) => outcome.targets.push(MetadataMutationTargetOutcome {
                        broker_name: broker_name.clone(),
                        expected_state: ExpectedState::Present {
                            version: state.generation,
                        },
                        resulting_state: None,
                        applied: false,
                        changed: false,
                        persistence: MutationPersistenceState::NotRequired,
                        verification: MutationVerificationState::NotPerformed,
                        failure: Some(MutationFailureCode::Unavailable),
                        retryable: error.boundary_view().is_retryable(),
                    }),
                }
            }
            Ok(outcome)
        })
    }

    fn execute_broker_config_patch_verified<'a>(
        &'a mut self,
        plan: &'a BrokerMutationConfigPlan,
        patch: BrokerMutationConfigPatch,
    ) -> AdminFuture<'a, BrokerMutationConfigOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_broker_config_patch_verified_with_admin(&self.inner.inner, &self.plan_seal, plan, patch).await
        })
    }

    fn preflight_request_mode<'a>(
        &'a mut self,
        request: &'a RequestModePreflightRequest,
    ) -> AdminFuture<'a, RequestModeMutationPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_request_mode_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request).await
        })
    }

    fn execute_request_mode<'a>(
        &'a mut self,
        plan: &'a RequestModeMutationPlan,
    ) -> AdminFuture<'a, RequestModeMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_request_mode_checked(&self.inner.inner, &self.plan_seal, plan).await
        })
    }

    fn execute_request_mode_with_timeout<'a>(
        &'a mut self,
        plan: &'a RequestModeMutationPlan,
        timeout_millis: u64,
    ) -> AdminFuture<'a, RequestModeMutationOutcome> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            if !(1..=24_000).contains(&timeout_millis) {
                return Err(AdminError::invalid_argument(
                    "timeoutMillis",
                    "must be between 1 and 24000",
                ));
            }
            execute_request_mode_checked_with_timeout(&self.inner.inner, &self.plan_seal, plan, timeout_millis).await
        })
    }
}

async fn preflight_topic_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &TopicMutationPreflightRequest,
) -> AdminResult<TopicMutationPlan> {
    preflight_topic_with_targets(admin, seal, request, None).await
}

async fn preflight_topic_targets_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &TopicMutationPreflightRequest,
    broker_names: &[String],
) -> AdminResult<TopicMutationPlan> {
    preflight_topic_with_targets(admin, seal, request, Some(broker_names)).await
}

async fn preflight_topic_with_targets<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &TopicMutationPreflightRequest,
    broker_names: Option<&[String]>,
) -> AdminResult<TopicMutationPlan> {
    let cluster = require_non_empty("cluster", &request.cluster)?.to_owned();
    let topic = validate_supervised_topic(&request.topic)?;
    validate_topic_replacement(&request.replacement)?;
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let mut targets = Vec::new();
    let mut failures = Vec::new();
    let master_targets = master_targets_by_cluster_name(&cluster_info, &cluster)?;
    let master_targets = select_metadata_targets(master_targets, broker_names)?;
    let targeted_order_guard = if broker_names.is_some() {
        let current = admin
            .mutation_order_topic_config(topic.clone().into())
            .await
            .map_err(|error| backend_error("mutation_order_topic_config", error))?;
        let expected = parse_order_topic_config(current.as_deref())?;
        validate_targeted_order_state(
            &expected,
            master_targets.iter().map(|(broker_name, _)| broker_name.as_str()),
            &request.replacement,
        )?;
        Some(TargetedTopicOrderGuard { expected })
    } else {
        None
    };
    for (broker_name, broker_addr) in master_targets {
        match admin
            .mutation_topic_config_state(broker_addr.clone(), topic.clone().into())
            .await
        {
            Ok(state) => targets.push(ResolvedMetadataTarget {
                broker_name,
                broker_addr: broker_addr.to_string(),
                state: map_client_expected_state(state.state),
                current: state.config.map(map_client_topic_config),
            }),
            Err(error) => failures.push(client_failure(broker_name, None, &error)),
        }
    }
    Ok(TopicMutationPlan {
        seal,
        cluster,
        topic,
        replacement: request.replacement.clone(),
        targets,
        failures,
        targeted_order_guard,
    })
}

async fn execute_topic_checked<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &TopicMutationPlan,
) -> AdminResult<MetadataMutationOutcome> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    if let Some(guard) = &plan.targeted_order_guard {
        let current = admin
            .mutation_order_topic_config(plan.topic.as_str().into())
            .await
            .map_err(|error| backend_error("mutation_order_topic_config", error))?;
        let current = parse_order_topic_config(current.as_deref())?;
        if current != guard.expected {
            return Ok(targeted_order_conflict(plan));
        }
    }
    let mut outcome = MetadataMutationOutcome {
        failures: plan.failures.clone(),
        ..MetadataMutationOutcome::default()
    };
    for target in &plan.targets {
        let expected_state = map_expected_state_to_client(target.state);
        match admin
            .replace_topic_config_if_state(
                target.broker_addr.as_str().into(),
                plan.topic.as_str().into(),
                expected_state,
                map_topic_replacement_to_client(&plan.replacement),
            )
            .await
        {
            Ok(result) => {
                let verification = if result.persistence == ClientMutationPersistenceState::Failed {
                    match admin
                        .mutation_topic_config_state(target.broker_addr.as_str().into(), plan.topic.as_str().into())
                        .await
                    {
                        Ok(observed)
                            if observed.state == result.state
                                && observed.config == Some(map_topic_replacement_to_client(&plan.replacement)) =>
                        {
                            MutationVerificationState::Verified
                        }
                        _ => MutationVerificationState::Failed,
                    }
                } else {
                    MutationVerificationState::NotPerformed
                };
                outcome.targets.push(MetadataMutationTargetOutcome {
                    broker_name: target.broker_name.clone(),
                    expected_state: target.state,
                    resulting_state: Some(map_client_expected_state(result.state)),
                    applied: result.applied,
                    changed: result.changed,
                    persistence: map_client_persistence(result.persistence),
                    verification,
                    failure: if result.persistence == ClientMutationPersistenceState::Failed {
                        Some(MutationFailureCode::PersistenceFailed)
                    } else if !result.applied {
                        Some(MutationFailureCode::Conflict)
                    } else {
                        None
                    },
                    retryable: false,
                });
            }
            Err(error) => outcome.targets.push(metadata_client_failure(target, &error)),
        }
    }
    if let Some(guard) = &plan.targeted_order_guard {
        let postread = admin
            .mutation_order_topic_config(plan.topic.as_str().into())
            .await
            .ok()
            .and_then(|current| parse_order_topic_config(current.as_deref()).ok());
        if postread.as_ref() == Some(&guard.expected) {
            outcome.order_reconciled = Some(true);
        } else {
            outcome.order_reconciled = Some(false);
            for target in &plan.targets {
                outcome.failures.push(MutationTargetFailure {
                    broker_name: target.broker_name.clone(),
                    queue_id: None,
                    code: MutationFailureCode::OrderReconciliationFailed,
                    retryable: false,
                });
            }
        }
        return Ok(outcome);
    }

    let all_applied = !plan.targets.is_empty()
        && outcome.failures.is_empty()
        && outcome.targets.iter().all(|target| {
            target.applied
                && target.failure.is_none()
                && matches!(
                    (target.changed, target.persistence),
                    (true, MutationPersistenceState::Persisted) | (false, MutationPersistenceState::NotRequired)
                )
        });
    if all_applied {
        let order_result = if plan.replacement.order {
            let broker_names = plan
                .targets
                .iter()
                .map(|target| target.broker_name.clone())
                .collect::<HashSet<_>>();
            admin
                .upsert_order_topic_config(
                    plan.topic.as_str().into(),
                    build_order_conf(&broker_names, plan.replacement.write_queue_nums).into(),
                    false,
                )
                .await
        } else {
            admin.delete_order_topic_config(plan.topic.as_str().into()).await
        };
        match order_result {
            Ok(()) => outcome.order_reconciled = Some(true),
            Err(error) => {
                outcome.order_reconciled = Some(false);
                outcome
                    .failures
                    .push(client_failure("order_topic_config".to_owned(), None, &error));
            }
        }
    } else {
        outcome.order_reconciled = Some(false);
    }
    Ok(outcome)
}

fn parse_order_topic_config(value: Option<&str>) -> AdminResult<Option<BTreeMap<String, u32>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        return Err(AdminError::invalid_argument("orderTopicConfig", "must not be empty"));
    }
    let mut entries = BTreeMap::new();
    for entry in value.split(';') {
        let Some((broker_name, queues)) = entry.split_once(':') else {
            return Err(AdminError::invalid_argument(
                "orderTopicConfig",
                "contains a malformed broker entry",
            ));
        };
        if broker_name.is_empty()
            || broker_name.len() > 127
            || !broker_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'%' | b'|' | b'-' | b'_'))
            || broker_name
                .as_bytes()
                .windows(3)
                .any(|window| window[0] == b'%' && window[1].is_ascii_hexdigit() && window[2].is_ascii_hexdigit())
            || queues.is_empty()
            || (queues.len() > 1 && queues.starts_with('0'))
        {
            return Err(AdminError::invalid_argument(
                "orderTopicConfig",
                "contains a non-canonical broker entry",
            ));
        }
        let queues = queues
            .parse::<u32>()
            .ok()
            .filter(|queues| *queues > 0)
            .ok_or_else(|| AdminError::invalid_argument("orderTopicConfig", "contains an invalid queue count"))?;
        if entries.insert(broker_name.to_owned(), queues).is_some() {
            return Err(AdminError::invalid_argument(
                "orderTopicConfig",
                "contains a duplicate broker entry",
            ));
        }
    }
    Ok(Some(entries))
}

fn validate_targeted_order_state<'a>(
    expected: &Option<BTreeMap<String, u32>>,
    selected_brokers: impl Iterator<Item = &'a str>,
    replacement: &TopicReplacement,
) -> AdminResult<()> {
    let entries = expected.as_ref();
    let compatible = if replacement.order {
        selected_brokers
            .into_iter()
            .all(|broker| entries.and_then(|entries| entries.get(broker)) == Some(&replacement.write_queue_nums))
    } else {
        selected_brokers
            .into_iter()
            .all(|broker| entries.is_none_or(|entries| !entries.contains_key(broker)))
    };
    if !compatible {
        return Err(AdminError::invalid_argument(
            "orderTopicConfig",
            "targeted mutation would require a NameServer-wide order configuration write",
        ));
    }
    Ok(())
}

fn targeted_order_conflict(plan: &TopicMutationPlan) -> MetadataMutationOutcome {
    MetadataMutationOutcome {
        targets: plan
            .targets
            .iter()
            .map(|target| MetadataMutationTargetOutcome {
                broker_name: target.broker_name.clone(),
                expected_state: target.state,
                resulting_state: None,
                applied: false,
                changed: false,
                persistence: MutationPersistenceState::NotRequired,
                verification: MutationVerificationState::NotPerformed,
                failure: Some(MutationFailureCode::Conflict),
                retryable: false,
            })
            .collect(),
        failures: Vec::new(),
        order_reconciled: Some(false),
    }
}

async fn preflight_subscription_group_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &SubscriptionGroupMutationPreflightRequest,
) -> AdminResult<SubscriptionGroupMutationPlan> {
    preflight_subscription_group_with_targets(admin, seal, request, None).await
}

async fn preflight_subscription_group_targets_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &SubscriptionGroupMutationPreflightRequest,
    broker_names: &[String],
) -> AdminResult<SubscriptionGroupMutationPlan> {
    preflight_subscription_group_with_targets(admin, seal, request, Some(broker_names)).await
}

async fn preflight_subscription_group_with_targets<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &SubscriptionGroupMutationPreflightRequest,
    broker_names: Option<&[String]>,
) -> AdminResult<SubscriptionGroupMutationPlan> {
    let cluster = require_non_empty("cluster", &request.cluster)?.to_owned();
    let group = validate_supervised_group(&request.consumer_group)?;
    validate_group_replacement(&request.replacement)?;
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let mut targets = Vec::new();
    let mut failures = Vec::new();
    let master_targets = master_targets_by_cluster_name(&cluster_info, &cluster)?;
    let master_targets = select_metadata_targets(master_targets, broker_names)?;
    for (broker_name, broker_addr) in master_targets {
        match admin
            .mutation_subscription_group_config_state(broker_addr.clone(), group.clone().into())
            .await
        {
            Ok(state) => targets.push(ResolvedMetadataTarget {
                broker_name,
                broker_addr: broker_addr.to_string(),
                state: map_client_expected_state(state.state),
                current: state.config.map(map_client_group_config),
            }),
            Err(error) => failures.push(client_failure(broker_name, None, &error)),
        }
    }
    Ok(SubscriptionGroupMutationPlan {
        seal,
        cluster,
        consumer_group: group,
        replacement: request.replacement.clone(),
        targets,
        failures,
    })
}

async fn execute_subscription_group_checked<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &SubscriptionGroupMutationPlan,
) -> AdminResult<MetadataMutationOutcome> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    let mut outcome = MetadataMutationOutcome {
        failures: plan.failures.clone(),
        ..MetadataMutationOutcome::default()
    };
    for target in &plan.targets {
        match admin
            .replace_subscription_group_config_if_state(
                target.broker_addr.as_str().into(),
                plan.consumer_group.as_str().into(),
                map_expected_state_to_client(target.state),
                map_group_replacement_to_client(&plan.replacement),
            )
            .await
        {
            Ok(result) => {
                let verification = if result.persistence == ClientMutationPersistenceState::Failed {
                    match admin
                        .mutation_subscription_group_config_state(
                            target.broker_addr.as_str().into(),
                            plan.consumer_group.as_str().into(),
                        )
                        .await
                    {
                        Ok(observed)
                            if observed.state == result.state
                                && observed.config == Some(map_group_replacement_to_client(&plan.replacement)) =>
                        {
                            MutationVerificationState::Verified
                        }
                        _ => MutationVerificationState::Failed,
                    }
                } else {
                    MutationVerificationState::NotPerformed
                };
                outcome.targets.push(MetadataMutationTargetOutcome {
                    broker_name: target.broker_name.clone(),
                    expected_state: target.state,
                    resulting_state: Some(map_client_expected_state(result.state)),
                    applied: result.applied,
                    changed: result.changed,
                    persistence: map_client_persistence(result.persistence),
                    verification,
                    failure: if result.persistence == ClientMutationPersistenceState::Failed {
                        Some(MutationFailureCode::PersistenceFailed)
                    } else if !result.applied {
                        Some(MutationFailureCode::Conflict)
                    } else {
                        None
                    },
                    retryable: false,
                });
            }
            Err(error) => outcome.targets.push(metadata_client_failure(target, &error)),
        }
    }
    Ok(outcome)
}

async fn preflight_request_mode_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &RequestModePreflightRequest,
) -> AdminResult<RequestModeMutationPlan> {
    let cluster = require_non_empty("cluster", &request.cluster)?.to_owned();
    let topic = validate_supervised_topic(&request.topic)?;
    let group = validate_supervised_group(&request.consumer_group)?;
    if request.replacement.pop_share_queue_num < 0 {
        return Err(AdminError::invalid_argument("popShareQueueNum", "must be non-negative"));
    }
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let mut targets = Vec::new();
    let mut failures = Vec::new();
    for (broker_name, broker_addr) in master_targets_by_cluster_name(&cluster_info, &cluster)? {
        let topic_state = match admin
            .mutation_topic_config_state(broker_addr.clone(), topic.clone().into())
            .await
        {
            Ok(state) => state,
            Err(error) => {
                failures.push(client_failure(broker_name, None, &error));
                continue;
            }
        };
        if !matches!(
            topic_state,
            rocketmq_client_rust::MutationTopicConfigState {
                state: ClientExpectedState::Present { .. },
                config: Some(_),
            }
        ) {
            failures.push(MutationTargetFailure {
                broker_name,
                queue_id: None,
                code: MutationFailureCode::InvalidData,
                retryable: false,
            });
            continue;
        }
        let group_state = match admin
            .mutation_subscription_group_config_state(broker_addr.clone(), group.clone().into())
            .await
        {
            Ok(state) => state,
            Err(error) => {
                failures.push(client_failure(broker_name, None, &error));
                continue;
            }
        };
        if !matches!(
            group_state,
            rocketmq_client_rust::MutationSubscriptionGroupConfigState {
                state: ClientExpectedState::Present { .. },
                config: Some(_),
            }
        ) {
            failures.push(MutationTargetFailure {
                broker_name,
                queue_id: None,
                code: MutationFailureCode::InvalidData,
                retryable: false,
            });
            continue;
        }
        match admin
            .mutation_message_request_mode(broker_addr.clone(), topic.clone().into(), group.clone().into())
            .await
        {
            Ok(current) => targets.push((
                broker_name,
                broker_addr.to_string(),
                current.map(map_client_request_mode),
            )),
            Err(error) => failures.push(client_failure(broker_name, None, &error)),
        }
    }
    Ok(RequestModeMutationPlan {
        seal,
        cluster,
        topic,
        consumer_group: group,
        replacement: request.replacement,
        targets,
        failures,
    })
}

async fn preflight_broker_config_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    cluster: &str,
) -> AdminResult<BrokerMutationConfigPlan> {
    let cluster = require_non_empty("cluster", cluster)?.to_owned();
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let mut targets = Vec::new();
    let mut failures = Vec::new();
    for (broker_name, broker_addr) in master_targets_by_cluster_name(&cluster_info, &cluster)? {
        match admin.broker_mutation_config_state(broker_addr.clone()).await {
            Ok(state) => targets.push((broker_name, broker_addr.to_string(), map_client_broker_state(state))),
            Err(error) => failures.push(client_failure(broker_name, None, &error)),
        }
    }
    Ok(BrokerMutationConfigPlan {
        seal,
        cluster,
        targets,
        failures,
    })
}

async fn preflight_broker_config_target_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    cluster: &str,
    broker_name: &str,
) -> AdminResult<BrokerMutationConfigPlan> {
    let cluster = require_non_empty("cluster", cluster)?.to_owned();
    let broker_name = require_non_empty("brokerName", broker_name)?.to_owned();
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let all_targets = master_targets_by_cluster_name(&cluster_info, &cluster)?;
    let selected = vec![broker_name];
    let selected_targets = select_metadata_targets(all_targets, Some(&selected))?;
    let mut targets = Vec::with_capacity(1);
    let mut failures = Vec::new();
    for (broker_name, broker_addr) in selected_targets {
        match admin.broker_mutation_config_state(broker_addr.clone()).await {
            Ok(state) => targets.push((broker_name, broker_addr.to_string(), map_client_broker_state(state))),
            Err(error) => failures.push(client_failure(broker_name, None, &error)),
        }
    }
    Ok(BrokerMutationConfigPlan {
        seal,
        cluster,
        targets,
        failures,
    })
}

async fn execute_broker_config_patch_verified_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &BrokerMutationConfigPlan,
    patch: BrokerMutationConfigPatch,
) -> AdminResult<BrokerMutationConfigOutcome> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    let properties = broker_patch_properties(patch)?;
    let mut outcome = BrokerMutationConfigOutcome {
        failures: plan.failures.clone(),
        ..BrokerMutationConfigOutcome::default()
    };
    for (broker_name, broker_addr, before) in &plan.targets {
        let planned_changed = broker_patch_changes(*before, patch);
        match admin
            .patch_broker_config_if_generation(broker_addr.as_str().into(), before.generation, properties.clone())
            .await
        {
            Ok(ClientBrokerConfigPatchOutcome::Applied { .. }) => {
                match admin.broker_mutation_config_state(broker_addr.as_str().into()).await {
                    Ok(observed) => {
                        let observed = map_client_broker_state(observed);
                        let verified = broker_patch_matches(observed, patch);
                        outcome.targets.push(BrokerMutationConfigTargetOutcome {
                            broker_name: broker_name.clone(),
                            before: *before,
                            after: Some(observed),
                            applied: true,
                            changed: planned_changed,
                            persistence: MutationPersistenceState::Persisted,
                            verification: if verified {
                                MutationVerificationState::Verified
                            } else {
                                MutationVerificationState::Failed
                            },
                            failure: (!verified).then_some(MutationFailureCode::VerificationFailed),
                            retryable: false,
                        });
                    }
                    Err(error) => outcome.targets.push(BrokerMutationConfigTargetOutcome {
                        broker_name: broker_name.clone(),
                        before: *before,
                        after: None,
                        applied: true,
                        changed: planned_changed,
                        persistence: MutationPersistenceState::Persisted,
                        verification: MutationVerificationState::Failed,
                        failure: Some(MutationFailureCode::VerificationFailed),
                        retryable: error.boundary_view().is_retryable(),
                    }),
                }
            }
            Ok(ClientBrokerConfigPatchOutcome::GenerationConflict { .. }) => {
                outcome.targets.push(BrokerMutationConfigTargetOutcome {
                    broker_name: broker_name.clone(),
                    before: *before,
                    after: None,
                    applied: false,
                    changed: false,
                    persistence: MutationPersistenceState::NotRequired,
                    verification: MutationVerificationState::NotPerformed,
                    failure: Some(MutationFailureCode::Conflict),
                    retryable: false,
                });
            }
            Err(error) => outcome.targets.push(BrokerMutationConfigTargetOutcome {
                broker_name: broker_name.clone(),
                before: *before,
                after: None,
                applied: false,
                changed: false,
                persistence: MutationPersistenceState::NotRequired,
                verification: MutationVerificationState::NotPerformed,
                failure: Some(MutationFailureCode::Unavailable),
                retryable: error.boundary_view().is_retryable(),
            }),
        }
    }
    Ok(outcome)
}

async fn execute_request_mode_checked<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &RequestModeMutationPlan,
) -> AdminResult<RequestModeMutationOutcome> {
    execute_request_mode_checked_inner(admin, session_seal, plan, None).await
}

async fn execute_request_mode_checked_with_timeout<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &RequestModeMutationPlan,
    timeout_millis: u64,
) -> AdminResult<RequestModeMutationOutcome> {
    execute_request_mode_checked_inner(admin, session_seal, plan, Some(timeout_millis)).await
}

async fn execute_request_mode_checked_inner<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &RequestModeMutationPlan,
    timeout_millis: Option<u64>,
) -> AdminResult<RequestModeMutationOutcome> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    let mut outcome = RequestModeMutationOutcome {
        failures: plan.failures.clone(),
        ..RequestModeMutationOutcome::default()
    };
    for (broker_name, broker_addr, current) in &plan.targets {
        let expected = current.map_or(ClientExpectedMessageRequestMode::Absent, |value| {
            ClientExpectedMessageRequestMode::Present(map_request_mode_to_client(value))
        });
        let replacement = map_request_mode_to_client(plan.replacement);
        let result = if let Some(timeout_millis) = timeout_millis {
            admin
                .replace_message_request_mode_if_current_with_timeout(
                    broker_addr.as_str().into(),
                    plan.topic.as_str().into(),
                    plan.consumer_group.as_str().into(),
                    expected,
                    replacement,
                    timeout_millis,
                )
                .await
        } else {
            admin
                .replace_message_request_mode_if_current(
                    broker_addr.as_str().into(),
                    plan.topic.as_str().into(),
                    plan.consumer_group.as_str().into(),
                    expected,
                    replacement,
                )
                .await
        };
        match result {
            Ok(result) if result.applied || result.persistence == ClientMutationPersistenceState::Failed => {
                let observed = admin
                    .mutation_message_request_mode(
                        broker_addr.as_str().into(),
                        plan.topic.as_str().into(),
                        plan.consumer_group.as_str().into(),
                    )
                    .await;
                let expected_result = result.current.map(map_client_request_mode);
                let (current_result, verification, verification_failed, retryable) = match observed {
                    Ok(value) => {
                        let value = value.map(map_client_request_mode);
                        let failed = value != expected_result;
                        (
                            value,
                            if failed {
                                MutationVerificationState::Failed
                            } else {
                                MutationVerificationState::Verified
                            },
                            failed,
                            false,
                        )
                    }
                    Err(error) => (
                        None,
                        MutationVerificationState::Failed,
                        true,
                        error.boundary_view().is_retryable(),
                    ),
                };
                let persistence = map_client_persistence(result.persistence);
                outcome.targets.push(RequestModeTargetOutcome {
                    broker_name: broker_name.clone(),
                    expected: *current,
                    current: current_result,
                    applied: result.applied,
                    changed: result.changed,
                    persistence,
                    verification,
                    failure: if verification_failed {
                        Some(MutationFailureCode::VerificationFailed)
                    } else if persistence == MutationPersistenceState::Failed {
                        Some(MutationFailureCode::PersistenceFailed)
                    } else {
                        None
                    },
                    retryable,
                });
            }
            Ok(result) => outcome.targets.push(RequestModeTargetOutcome {
                broker_name: broker_name.clone(),
                expected: *current,
                current: result.current.map(map_client_request_mode),
                applied: false,
                changed: false,
                persistence: map_client_persistence(result.persistence),
                verification: MutationVerificationState::NotPerformed,
                failure: Some(MutationFailureCode::Conflict),
                retryable: false,
            }),
            Err(error) => outcome.targets.push(RequestModeTargetOutcome {
                broker_name: broker_name.clone(),
                expected: *current,
                current: None,
                applied: false,
                changed: false,
                persistence: MutationPersistenceState::NotRequired,
                verification: MutationVerificationState::NotPerformed,
                failure: Some(MutationFailureCode::Unavailable),
                retryable: error.boundary_view().is_retryable(),
            }),
        }
    }
    Ok(outcome)
}

async fn preview_offset_reset_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &OffsetResetPreviewRequest,
) -> AdminResult<OffsetResetPlan> {
    let cluster = require_non_empty("cluster", &request.cluster)?.to_owned();
    let topic = validate_supervised_topic(&request.topic)?;
    let group = validate_supervised_group(&request.consumer_group)?;
    if request.timestamp < 0 {
        return Err(AdminError::invalid_argument(
            "timestamp",
            "must be a non-negative RocketMQ timestamp",
        ));
    }
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let selected_masters = master_targets_by_cluster_name(&cluster_info, &cluster)?;
    let route = require_topic_route(admin, &topic).await?;
    let selected_names = selected_masters
        .iter()
        .map(|(broker_name, _)| broker_name.as_str())
        .collect::<HashSet<_>>();
    let mut route_masters = HashMap::with_capacity(selected_masters.len());
    for broker in &route.broker_datas {
        if !selected_names.contains(broker.broker_name().as_str()) {
            continue;
        }
        if broker.cluster() != cluster || route_masters.contains_key(broker.broker_name().as_str()) {
            return Err(AdminError::backend(
                "mutation_topic_route",
                "selected Topic route has inconsistent broker identity",
            ));
        }
        let master = broker
            .broker_addrs()
            .get(&MASTER_ID)
            .ok_or_else(|| AdminError::backend("mutation_topic_route", "selected Topic route broker has no master"))?;
        route_masters.insert(broker.broker_name().to_string(), master.clone());
    }
    let mut queue_data = HashMap::with_capacity(selected_masters.len());
    for queue in &route.queue_datas {
        let broker_name = queue.broker_name().to_string();
        if !selected_names.contains(broker_name.as_str()) {
            continue;
        }
        if queue_data.insert(broker_name, queue.read_queue_nums()).is_some() {
            return Err(AdminError::backend(
                "mutation_topic_route",
                "selected Topic route has duplicate queue ownership",
            ));
        }
    }
    for (broker_name, master_addr) in &selected_masters {
        if route_masters.get(broker_name) != Some(master_addr) {
            return Err(AdminError::backend(
                "mutation_topic_route",
                "selected Topic route does not match cluster topology",
            ));
        }
        queue_data.get(broker_name).ok_or_else(|| {
            AdminError::backend("mutation_topic_route", "selected Topic route has no queue ownership")
        })?;
    }
    let target_budget = checked_offset_target_budget(queue_data.values().copied())?;
    let mut targets = Vec::with_capacity(target_budget);
    let mut failures = Vec::new();
    for (broker_name, master_addr) in selected_masters {
        let read_queue_nums = queue_data[&broker_name];
        match admin
            .preview_consumer_offset_reset_on_broker(
                master_addr.clone(),
                broker_name.clone().into(),
                read_queue_nums,
                group.clone().into(),
                topic.clone().into(),
                request.timestamp,
            )
            .await
        {
            Ok(rows) => {
                let mut queue_ids = rows.iter().map(|row| row.queue_id).collect::<Vec<_>>();
                queue_ids.sort_unstable();
                if rows.len() != read_queue_nums as usize
                    || rows.iter().any(|row| {
                        row.broker_name != broker_name || row.queue_id < 0 || row.queue_id >= read_queue_nums as i32
                    })
                    || queue_ids
                        .iter()
                        .enumerate()
                        .any(|(expected, actual)| *actual != expected as i32)
                {
                    return Err(AdminError::backend(
                        "preview_consumer_offset_reset_on_broker",
                        "Broker returned rows outside the selected Topic queue range",
                    ));
                }
                if targets.len().saturating_add(rows.len()) > target_budget {
                    return Err(AdminError::invalid_argument(
                        "offsetTargets",
                        "Broker returned more queue targets than the selected Topic route",
                    ));
                }
                for row in rows {
                    let planned_offset =
                        planned_offset_for_force(row.current_offset, row.planned_offset, request.force);
                    let delta = planned_offset
                        .checked_sub(row.current_offset)
                        .ok_or_else(|| AdminError::invalid_argument("offset", "planned offset delta overflowed"))?;
                    targets.push(ResolvedOffsetResetTarget {
                        broker_addr: master_addr.to_string(),
                        row: OffsetResetPreviewRow {
                            broker_name: row.broker_name,
                            queue_id: row.queue_id,
                            current_offset: row.current_offset,
                            planned_offset,
                            delta,
                            changed: planned_offset != row.current_offset,
                        },
                    });
                }
            }
            Err(error) => failures.push(client_failure(broker_name, None, &error)),
        }
    }
    OffsetResetPlan::try_new(
        seal,
        cluster,
        topic,
        group,
        request.timestamp,
        request.force,
        targets,
        failures,
    )
}

async fn execute_offset_reset_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    plan: &OffsetResetPlan,
) -> AdminResult<OffsetResetOutcome> {
    let mut outcome = OffsetResetOutcome {
        failures: plan.failures().to_vec(),
        ..OffsetResetOutcome::default()
    };
    for target in &plan.targets {
        let row = &target.row;
        if !row.changed {
            outcome.targets.push(OffsetResetTargetOutcome {
                broker_name: row.broker_name.clone(),
                queue_id: row.queue_id,
                expected_offset: row.current_offset,
                planned_offset: row.planned_offset,
                observed_offset: Some(row.current_offset),
                applied: true,
                changed: false,
                failure: None,
                retryable: false,
            });
            continue;
        }
        let result = admin
            .reset_consumer_offset_if_current(
                target.broker_addr.as_str().into(),
                plan.consumer_group.as_str().into(),
                plan.topic.as_str().into(),
                row.queue_id,
                row.current_offset,
                row.planned_offset,
            )
            .await;
        match result {
            Ok(result) if result.applied => {
                let verified = admin
                    .mutation_consumer_offset(
                        target.broker_addr.as_str().into(),
                        plan.consumer_group.as_str().into(),
                        plan.topic.as_str().into(),
                        row.queue_id,
                    )
                    .await;
                match verified {
                    Ok(observed) => outcome.targets.push(OffsetResetTargetOutcome {
                        broker_name: row.broker_name.clone(),
                        queue_id: row.queue_id,
                        expected_offset: row.current_offset,
                        planned_offset: row.planned_offset,
                        observed_offset: Some(observed),
                        applied: true,
                        changed: true,
                        failure: (observed != row.planned_offset).then_some(MutationFailureCode::VerificationFailed),
                        retryable: false,
                    }),
                    Err(error) => outcome.targets.push(OffsetResetTargetOutcome {
                        broker_name: row.broker_name.clone(),
                        queue_id: row.queue_id,
                        expected_offset: row.current_offset,
                        planned_offset: row.planned_offset,
                        observed_offset: None,
                        applied: true,
                        changed: true,
                        failure: Some(MutationFailureCode::VerificationFailed),
                        retryable: error.boundary_view().is_retryable(),
                    }),
                }
            }
            Ok(result) => outcome.targets.push(OffsetResetTargetOutcome {
                broker_name: row.broker_name.clone(),
                queue_id: row.queue_id,
                expected_offset: row.current_offset,
                planned_offset: row.planned_offset,
                observed_offset: Some(result.actual_offset),
                applied: false,
                changed: false,
                failure: Some(MutationFailureCode::Conflict),
                retryable: false,
            }),
            Err(error) => outcome.targets.push(OffsetResetTargetOutcome {
                broker_name: row.broker_name.clone(),
                queue_id: row.queue_id,
                expected_offset: row.current_offset,
                planned_offset: row.planned_offset,
                observed_offset: None,
                applied: false,
                changed: false,
                failure: Some(MutationFailureCode::Unavailable),
                retryable: error.boundary_view().is_retryable(),
            }),
        }
    }
    Ok(outcome)
}

async fn execute_offset_reset_checked<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &OffsetResetPlan,
) -> AdminResult<OffsetResetOutcome> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    execute_offset_reset_with_admin(admin, plan).await
}

fn validate_supervised_topic(topic: &str) -> AdminResult<String> {
    let topic = require_non_empty("topic", topic)?;
    let validation = rocketmq_model::common::topic::TopicValidator::validate_topic(topic);
    if !validation.valid() || rocketmq_model::common::topic::TopicValidator::is_system_topic(topic) {
        return Err(AdminError::invalid_argument(
            "topic",
            "must be a non-system RocketMQ Topic name",
        ));
    }
    Ok(topic.to_owned())
}

fn planned_offset_for_force(current: i64, candidate: i64, force: bool) -> i64 {
    if force || candidate <= current {
        candidate
    } else {
        current
    }
}

fn checked_offset_target_budget(queue_counts: impl IntoIterator<Item = u32>) -> AdminResult<usize> {
    let mut total = 0usize;
    for queue_count in queue_counts {
        total = total
            .checked_add(queue_count as usize)
            .ok_or_else(|| AdminError::invalid_argument("offsetTargets", "queue target count overflowed"))?;
        if total > MAX_OFFSET_RESET_TARGETS {
            return Err(AdminError::invalid_argument(
                "offsetTargets",
                format!("must contain at most {MAX_OFFSET_RESET_TARGETS} unique queue targets"),
            ));
        }
    }
    Ok(total)
}

fn validate_supervised_group(group: &str) -> AdminResult<String> {
    let group = require_non_empty("consumerGroup", group)?;
    rocketmq_protocol::protocol::subscription::subscription_group_config::validate_subscription_group_name(group)
        .map_err(|error| AdminError::invalid_argument("consumerGroup", error.to_string()))?;
    if crate::core::consumer::is_protected_consumer_group(group) {
        return Err(AdminError::invalid_argument(
            "consumerGroup",
            "must not be a system consumer group",
        ));
    }
    Ok(group.to_owned())
}

fn validate_topic_replacement(replacement: &TopicReplacement) -> AdminResult<()> {
    if !(1..=128).contains(&replacement.read_queue_nums) {
        return Err(AdminError::invalid_argument(
            "readQueueNums",
            "must be between 1 and 128",
        ));
    }
    if !(1..=128).contains(&replacement.write_queue_nums) {
        return Err(AdminError::invalid_argument(
            "writeQueueNums",
            "must be between 1 and 128",
        ));
    }
    if !(1..=7).contains(&replacement.perm) || replacement.perm & 0b110 == 0 {
        return Err(AdminError::invalid_argument(
            "perm",
            "must grant read or write permission",
        ));
    }
    Ok(())
}

fn validate_group_replacement(replacement: &SubscriptionGroupReplacement) -> AdminResult<()> {
    if replacement.retry_queue_nums < 0 {
        return Err(AdminError::invalid_argument("retryQueueNums", "must be non-negative"));
    }
    if replacement.retry_max_times < -1 {
        return Err(AdminError::invalid_argument("retryMaxTimes", "must be -1 or greater"));
    }
    if replacement.consume_timeout_minute <= 0 {
        return Err(AdminError::invalid_argument(
            "consumeTimeoutMinute",
            "must be greater than zero",
        ));
    }
    Ok(())
}

fn map_client_expected_state(state: ClientExpectedState) -> ExpectedState {
    match state {
        ClientExpectedState::Absent => ExpectedState::Absent,
        ClientExpectedState::Present { version } => ExpectedState::Present { version },
    }
}

fn map_expected_state_to_client(state: ExpectedState) -> ClientExpectedState {
    match state {
        ExpectedState::Absent => ClientExpectedState::Absent,
        ExpectedState::Present { version } => ClientExpectedState::Present { version },
    }
}

fn map_client_topic_config(config: ClientTopicConfig) -> TopicReplacement {
    TopicReplacement {
        read_queue_nums: config.read_queue_nums,
        write_queue_nums: config.write_queue_nums,
        perm: config.perm,
        order: config.order,
        message_type: match config.message_type {
            ClientTopicMessageType::Normal => TopicMessageType::Normal,
            ClientTopicMessageType::Fifo => TopicMessageType::Fifo,
            ClientTopicMessageType::Delay => TopicMessageType::Delay,
            ClientTopicMessageType::Transaction => TopicMessageType::Transaction,
            ClientTopicMessageType::Unspecified => TopicMessageType::Unspecified,
        },
    }
}

fn map_topic_replacement_to_client(config: &TopicReplacement) -> ClientTopicConfig {
    ClientTopicConfig {
        read_queue_nums: config.read_queue_nums,
        write_queue_nums: config.write_queue_nums,
        perm: config.perm,
        order: config.order,
        message_type: match config.message_type {
            TopicMessageType::Normal => ClientTopicMessageType::Normal,
            TopicMessageType::Fifo => ClientTopicMessageType::Fifo,
            TopicMessageType::Delay => ClientTopicMessageType::Delay,
            TopicMessageType::Transaction => ClientTopicMessageType::Transaction,
            TopicMessageType::Unspecified => ClientTopicMessageType::Unspecified,
        },
    }
}

fn map_client_group_config(config: ClientSubscriptionGroupConfig) -> SubscriptionGroupReplacement {
    SubscriptionGroupReplacement {
        consume_enable: config.consume_enable,
        consume_from_min_enable: config.consume_from_min_enable,
        consume_broadcast_enable: config.consume_broadcast_enable,
        consume_message_orderly: config.consume_message_orderly,
        retry_queue_nums: config.retry_queue_nums,
        retry_max_times: config.retry_max_times,
        broker_id: config.broker_id,
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable,
        group_sys_flag: config.group_sys_flag,
        consume_timeout_minute: config.consume_timeout_minute,
    }
}

fn map_group_replacement_to_client(config: &SubscriptionGroupReplacement) -> ClientSubscriptionGroupConfig {
    ClientSubscriptionGroupConfig {
        consume_enable: config.consume_enable,
        consume_from_min_enable: config.consume_from_min_enable,
        consume_broadcast_enable: config.consume_broadcast_enable,
        consume_message_orderly: config.consume_message_orderly,
        retry_queue_nums: config.retry_queue_nums,
        retry_max_times: config.retry_max_times,
        broker_id: config.broker_id,
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable,
        group_sys_flag: config.group_sys_flag,
        consume_timeout_minute: config.consume_timeout_minute,
    }
}

fn map_client_broker_state(state: ClientBrokerMutationConfigState) -> BrokerMutationConfigState {
    BrokerMutationConfigState {
        generation: state.generation,
        auto_create_topic_enable: state.auto_create_topic_enable,
        auto_create_subscription_group: state.auto_create_subscription_group,
        broker_permission: state.broker_permission,
        default_topic_queue_nums: state.default_topic_queue_nums,
        message_index_enable: state.message_index_enable,
        trace_topic_enable: state.trace_topic_enable,
    }
}

fn map_client_request_mode(mode: ClientMessageRequestMode) -> RequestModeValue {
    RequestModeValue {
        mode: match mode.mode {
            rocketmq_model::common::message::message_enum::MessageRequestMode::Pull => RequestMode::Pull,
            rocketmq_model::common::message::message_enum::MessageRequestMode::Pop => RequestMode::Pop,
        },
        pop_share_queue_num: mode.pop_share_queue_num,
    }
}

fn map_client_persistence(state: ClientMutationPersistenceState) -> MutationPersistenceState {
    match state {
        ClientMutationPersistenceState::NotRequired => MutationPersistenceState::NotRequired,
        ClientMutationPersistenceState::Persisted => MutationPersistenceState::Persisted,
        ClientMutationPersistenceState::Failed => MutationPersistenceState::Failed,
    }
}

fn map_request_mode_to_client(mode: RequestModeValue) -> ClientMessageRequestMode {
    ClientMessageRequestMode {
        mode: match mode.mode {
            RequestMode::Pull => rocketmq_model::common::message::message_enum::MessageRequestMode::Pull,
            RequestMode::Pop => rocketmq_model::common::message::message_enum::MessageRequestMode::Pop,
        },
        pop_share_queue_num: mode.pop_share_queue_num,
    }
}

fn client_failure(broker_name: String, queue_id: Option<i32>, error: &RocketMQError) -> MutationTargetFailure {
    MutationTargetFailure {
        broker_name,
        queue_id,
        code: MutationFailureCode::Unavailable,
        retryable: error.boundary_view().is_retryable(),
    }
}

fn metadata_client_failure<T>(
    target: &ResolvedMetadataTarget<T>,
    error: &RocketMQError,
) -> MetadataMutationTargetOutcome {
    MetadataMutationTargetOutcome {
        broker_name: target.broker_name.clone(),
        expected_state: target.state,
        resulting_state: None,
        applied: false,
        changed: false,
        persistence: MutationPersistenceState::NotRequired,
        verification: MutationVerificationState::NotPerformed,
        failure: Some(MutationFailureCode::Unavailable),
        retryable: error.boundary_view().is_retryable(),
    }
}

fn broker_patch_properties(patch: BrokerMutationConfigPatch) -> AdminResult<HashMap<CheetahString, CheetahString>> {
    if patch.is_empty() {
        return Err(AdminError::invalid_argument("patch", "must not be empty"));
    }
    if patch
        .broker_permission
        .is_some_and(|permission| !(1..=7).contains(&permission) || permission & 0b110 == 0)
    {
        return Err(AdminError::invalid_argument("brokerPermission", "is invalid"));
    }
    if patch
        .default_topic_queue_nums
        .is_some_and(|queues| !(1..=128).contains(&queues))
    {
        return Err(AdminError::invalid_argument(
            "defaultTopicQueueNums",
            "must be between 1 and 128",
        ));
    }
    let mut properties = HashMap::new();
    let mut insert = |key: &'static str, value: String| {
        properties.insert(CheetahString::from_static_str(key), CheetahString::from(value));
    };
    if let Some(value) = patch.auto_create_topic_enable {
        insert("autoCreateTopicEnable", value.to_string());
    }
    if let Some(value) = patch.auto_create_subscription_group {
        insert("autoCreateSubscriptionGroup", value.to_string());
    }
    if let Some(value) = patch.broker_permission {
        insert("brokerPermission", value.to_string());
    }
    if let Some(value) = patch.default_topic_queue_nums {
        insert("defaultTopicQueueNums", value.to_string());
    }
    if let Some(value) = patch.message_index_enable {
        insert("messageIndexEnable", value.to_string());
    }
    if let Some(value) = patch.trace_topic_enable {
        insert("traceTopicEnable", value.to_string());
    }
    Ok(properties)
}

fn broker_patch_matches(state: BrokerMutationConfigState, patch: BrokerMutationConfigPatch) -> bool {
    patch
        .auto_create_topic_enable
        .is_none_or(|value| state.auto_create_topic_enable == value)
        && patch
            .auto_create_subscription_group
            .is_none_or(|value| state.auto_create_subscription_group == value)
        && patch
            .broker_permission
            .is_none_or(|value| state.broker_permission == value)
        && patch
            .default_topic_queue_nums
            .is_none_or(|value| state.default_topic_queue_nums == value)
        && patch
            .message_index_enable
            .is_none_or(|value| state.message_index_enable == value)
        && patch
            .trace_topic_enable
            .is_none_or(|value| state.trace_topic_enable == value)
}

fn broker_patch_changes(state: BrokerMutationConfigState, patch: BrokerMutationConfigPatch) -> bool {
    !broker_patch_matches(state, patch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::time::Duration;

    use rocketmq_runtime::RuntimeContext;

    struct CountingMutationAdmin {
        cluster_info: ClusterInfo,
        route: TopicRouteData,
        preview_rows: Mutex<Vec<rocketmq_client_rust::MutationConsumerOffsetPreview>>,
        offsets: Mutex<HashMap<i32, i64>>,
        preview_calls: AtomicUsize,
        reset_calls: AtomicUsize,
        verify_calls: AtomicUsize,
        offset_fail_postread: AtomicBool,
        endpoint_calls: Mutex<Vec<(&'static str, String)>>,
        broker_state: Mutex<ClientBrokerMutationConfigState>,
        broker_reads: AtomicUsize,
        broker_writes: AtomicUsize,
        broker_conflict: AtomicBool,
        broker_fail_postread: AtomicBool,
        request_mode: Mutex<Option<ClientMessageRequestMode>>,
        request_mode_reads: AtomicUsize,
        request_mode_writes: AtomicUsize,
        request_mode_fail_postread: AtomicBool,
        request_mode_timeouts: Mutex<Vec<u64>>,
        request_mode_persistence: Mutex<ClientMutationPersistenceState>,
        request_mode_dirty: AtomicBool,
        topic_state: Mutex<rocketmq_client_rust::MutationTopicConfigState>,
        topic_reads: AtomicUsize,
        topic_writes: AtomicUsize,
        topic_persistence: Mutex<ClientMutationPersistenceState>,
        topic_dirty: AtomicBool,
        group_state: Mutex<rocketmq_client_rust::MutationSubscriptionGroupConfigState>,
        group_reads: AtomicUsize,
        group_writes: AtomicUsize,
        group_persistence: Mutex<ClientMutationPersistenceState>,
        group_dirty: AtomicBool,
        order_config: Mutex<Option<CheetahString>>,
        order_after_topic_write: Mutex<Option<CheetahString>>,
        order_reads: AtomicUsize,
        order_writes: AtomicUsize,
    }

    impl CountingMutationAdmin {
        fn new(read_queue_nums: u32) -> Self {
            Self::with_queue_counts(&[read_queue_nums])
        }

        fn with_queue_counts(read_queue_nums: &[u32]) -> Self {
            let mut broker_table = HashMap::new();
            let mut broker_names = HashSet::new();
            let mut broker_datas = Vec::new();
            let mut queue_datas = Vec::new();
            for (index, read_queue_nums) in read_queue_nums.iter().copied().enumerate() {
                let broker_name = CheetahString::from(format!("broker-{index}"));
                let broker_addr = CheetahString::from(format!("10.0.0.{}:10911", index + 1));
                let slave_addr = CheetahString::from(format!("10.0.0.{}:10912", index + 1));
                let broker_data = rocketmq_protocol::protocol::route::route_data_view::BrokerData::new(
                    CheetahString::from_static_str("cluster-a"),
                    broker_name.clone(),
                    HashMap::from([(MASTER_ID, broker_addr), (1, slave_addr)]),
                    None,
                );
                broker_table.insert(broker_name.clone(), broker_data.clone());
                broker_names.insert(broker_name.clone());
                broker_datas.push(broker_data);
                queue_datas.push(rocketmq_protocol::protocol::route::route_data_view::QueueData::new(
                    broker_name,
                    read_queue_nums,
                    read_queue_nums,
                    6,
                    0,
                ));
            }
            Self {
                cluster_info: ClusterInfo::new(
                    Some(broker_table),
                    Some(HashMap::from([(
                        CheetahString::from_static_str("cluster-a"),
                        broker_names,
                    )])),
                ),
                route: TopicRouteData {
                    queue_datas,
                    broker_datas,
                    ..TopicRouteData::default()
                },
                preview_rows: Mutex::new(
                    (0..read_queue_nums.first().copied().unwrap_or_default())
                        .map(|queue_id| rocketmq_client_rust::MutationConsumerOffsetPreview {
                            broker_name: "broker-0".to_owned(),
                            queue_id: queue_id as i32,
                            current_offset: -1,
                            planned_offset: 12,
                        })
                        .collect(),
                ),
                offsets: Mutex::new(HashMap::new()),
                preview_calls: AtomicUsize::new(0),
                reset_calls: AtomicUsize::new(0),
                verify_calls: AtomicUsize::new(0),
                offset_fail_postread: AtomicBool::new(false),
                endpoint_calls: Mutex::new(Vec::new()),
                broker_state: Mutex::new(ClientBrokerMutationConfigState {
                    generation: 1,
                    auto_create_topic_enable: true,
                    auto_create_subscription_group: true,
                    broker_permission: 6,
                    default_topic_queue_nums: 8,
                    message_index_enable: true,
                    trace_topic_enable: false,
                }),
                broker_reads: AtomicUsize::new(0),
                broker_writes: AtomicUsize::new(0),
                broker_conflict: AtomicBool::new(false),
                broker_fail_postread: AtomicBool::new(false),
                request_mode: Mutex::new(None),
                request_mode_reads: AtomicUsize::new(0),
                request_mode_writes: AtomicUsize::new(0),
                request_mode_fail_postread: AtomicBool::new(false),
                request_mode_timeouts: Mutex::new(Vec::new()),
                request_mode_persistence: Mutex::new(ClientMutationPersistenceState::Persisted),
                request_mode_dirty: AtomicBool::new(false),
                topic_state: Mutex::new(rocketmq_client_rust::MutationTopicConfigState {
                    state: ClientExpectedState::Absent,
                    config: None,
                }),
                topic_reads: AtomicUsize::new(0),
                topic_writes: AtomicUsize::new(0),
                topic_persistence: Mutex::new(ClientMutationPersistenceState::Persisted),
                topic_dirty: AtomicBool::new(false),
                group_state: Mutex::new(rocketmq_client_rust::MutationSubscriptionGroupConfigState {
                    state: ClientExpectedState::Absent,
                    config: None,
                }),
                group_reads: AtomicUsize::new(0),
                group_writes: AtomicUsize::new(0),
                group_persistence: Mutex::new(ClientMutationPersistenceState::Persisted),
                group_dirty: AtomicBool::new(false),
                order_config: Mutex::new(None),
                order_after_topic_write: Mutex::new(None),
                order_reads: AtomicUsize::new(0),
                order_writes: AtomicUsize::new(0),
            }
        }

        fn set_preview_rows(&self, rows: Vec<rocketmq_client_rust::MutationConsumerOffsetPreview>) {
            *self.preview_rows.lock().expect("preview rows") = rows;
        }

        fn record_endpoint(&self, operation: &'static str, broker_addr: &CheetahString) {
            self.endpoint_calls
                .lock()
                .expect("endpoint calls")
                .push((operation, broker_addr.to_string()));
        }

        fn enable_request_mode_target(&self) {
            *self.topic_state.lock().expect("topic state") = rocketmq_client_rust::MutationTopicConfigState {
                state: ClientExpectedState::Present { version: 1 },
                config: Some(map_topic_replacement_to_client(&TopicReplacement {
                    read_queue_nums: 1,
                    write_queue_nums: 1,
                    perm: 6,
                    order: false,
                    message_type: TopicMessageType::Normal,
                })),
            };
            *self.group_state.lock().expect("group state") =
                rocketmq_client_rust::MutationSubscriptionGroupConfigState {
                    state: ClientExpectedState::Present { version: 1 },
                    config: Some(map_group_replacement_to_client(&SubscriptionGroupReplacement {
                        consume_enable: true,
                        consume_from_min_enable: false,
                        consume_broadcast_enable: false,
                        consume_message_orderly: false,
                        retry_queue_nums: 1,
                        retry_max_times: 16,
                        broker_id: 0,
                        which_broker_when_consume_slowly: 1,
                        notify_consumer_ids_changed_enable: true,
                        group_sys_flag: 0,
                        consume_timeout_minute: 15,
                    })),
                };
        }
    }

    fn unsupported<T>() -> rocketmq_error::RocketMQResult<T> {
        Err(RocketMQError::illegal_argument("unused fake operation"))
    }

    impl MQAdminMutationExt for CountingMutationAdmin {
        async fn begin_proxy_drain(
            &self,
            _proxy_addr: CheetahString,
            _operation_id: CheetahString,
        ) -> rocketmq_error::RocketMQResult<rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody>
        {
            unsupported()
        }

        async fn cancel_proxy_drain(
            &self,
            _proxy_addr: CheetahString,
            _operation_id: CheetahString,
        ) -> rocketmq_error::RocketMQResult<rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody>
        {
            unsupported()
        }

        async fn broker_config_generation(&self, _broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<u64> {
            unsupported()
        }

        async fn patch_broker_config_if_generation(
            &self,
            broker_addr: CheetahString,
            expected_generation: u64,
            properties: HashMap<CheetahString, CheetahString>,
        ) -> rocketmq_error::RocketMQResult<ClientBrokerConfigPatchOutcome> {
            self.record_endpoint("broker_write", &broker_addr);
            self.broker_writes.fetch_add(1, Ordering::SeqCst);
            let mut state = self.broker_state.lock().expect("broker state");
            if self.broker_conflict.load(Ordering::SeqCst) || state.generation != expected_generation {
                return Ok(ClientBrokerConfigPatchOutcome::GenerationConflict {
                    expected_generation,
                    actual_generation: state.generation,
                });
            }
            for (key, value) in properties {
                match key.as_str() {
                    "autoCreateTopicEnable" => state.auto_create_topic_enable = value == "true",
                    "autoCreateSubscriptionGroup" => state.auto_create_subscription_group = value == "true",
                    "brokerPermission" => {
                        state.broker_permission = value
                            .parse()
                            .map_err(|_| RocketMQError::illegal_argument("invalid test broker permission"))?;
                    }
                    "defaultTopicQueueNums" => {
                        state.default_topic_queue_nums = value
                            .parse()
                            .map_err(|_| RocketMQError::illegal_argument("invalid test queue count"))?;
                    }
                    "messageIndexEnable" => state.message_index_enable = value == "true",
                    "traceTopicEnable" => state.trace_topic_enable = value == "true",
                    _ => return unsupported(),
                }
            }
            let previous_generation = state.generation;
            state.generation += 1;
            Ok(ClientBrokerConfigPatchOutcome::Applied {
                previous_generation,
                generation: state.generation,
            })
        }

        async fn patch_topic_config_if_version(
            &self,
            _broker_addr: CheetahString,
            _topic: CheetahString,
            _expected_version: u64,
            _patch: ClientTopicConfigPatch,
        ) -> rocketmq_error::RocketMQResult<ClientTopicConfigPatchOutcome> {
            unsupported()
        }

        async fn patch_subscription_group_config_if_version(
            &self,
            _broker_addr: CheetahString,
            _group: CheetahString,
            _expected_version: u64,
            _patch: ClientSubscriptionGroupConfigPatch,
        ) -> rocketmq_error::RocketMQResult<ClientSubscriptionGroupConfigPatchOutcome> {
            unsupported()
        }

        async fn upsert_topic_config(
            &self,
            _broker_addr: CheetahString,
            _config: TopicConfig,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn remove_topic(
            &self,
            _topic_name: CheetahString,
            _cluster_name: CheetahString,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn reset_consumer_offset(
            &self,
            _cluster_name: Option<CheetahString>,
            _topic: CheetahString,
            _consumer_group: CheetahString,
            _timestamp: u64,
            _force: bool,
        ) -> rocketmq_error::RocketMQResult<HashMap<rocketmq_model::message::MessageQueue, u64>> {
            unsupported()
        }

        async fn upsert_subscription_group(
            &self,
            _broker_addr: CheetahString,
            _config: SubscriptionGroupConfig,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn remove_subscription_group(
            &self,
            _broker_addr: CheetahString,
            _group_name: CheetahString,
            _remove_offset: Option<bool>,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn remove_subscription_groups(
            &self,
            _broker_addr: CheetahString,
            _group_names: Vec<CheetahString>,
            _clean_offset: bool,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn configure_message_request_mode(
            &self,
            _broker_addr: CheetahString,
            _topic: CheetahString,
            _consumer_group: CheetahString,
            _mode: rocketmq_model::common::message::message_enum::MessageRequestMode,
            _pop_work_group_size: i32,
            _timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn consume_directly(
            &self,
            _consumer_group: CheetahString,
            _client_id: CheetahString,
            _topic: CheetahString,
            _message_id: CheetahString,
        ) -> rocketmq_error::RocketMQResult<
            rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult,
        > {
            unsupported()
        }

        async fn clone_consumer_group_offset(
            &self,
            _source_group: CheetahString,
            _destination_group: CheetahString,
            _topic: CheetahString,
            _offline: bool,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn mutation_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
            Ok(self.cluster_info.clone())
        }

        async fn mutation_topic_route(
            &self,
            _topic: CheetahString,
        ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
            Ok(Some(self.route.clone()))
        }

        async fn mutation_topic_config(
            &self,
            _broker_addr: CheetahString,
            _topic: CheetahString,
        ) -> rocketmq_error::RocketMQResult<TopicConfig> {
            unsupported()
        }

        async fn remove_topic_from_brokers(
            &self,
            _broker_addrs: HashSet<CheetahString>,
            _topic: CheetahString,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn remove_topics_from_broker(
            &self,
            _broker_addr: CheetahString,
            _topics: Vec<CheetahString>,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn remove_topic_from_name_servers(
            &self,
            _namesrv_addrs: HashSet<CheetahString>,
            _cluster_name: Option<CheetahString>,
            _topic: CheetahString,
        ) -> rocketmq_error::RocketMQResult<()> {
            unsupported()
        }

        async fn mutation_name_server_addresses(&self) -> rocketmq_error::RocketMQResult<Vec<CheetahString>> {
            unsupported()
        }

        async fn upsert_order_topic_config(
            &self,
            _topic: CheetahString,
            _value: CheetahString,
            _cluster_wide: bool,
        ) -> rocketmq_error::RocketMQResult<()> {
            self.order_writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn mutation_order_topic_config(
            &self,
            _topic: CheetahString,
        ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
            self.order_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.order_config.lock().expect("order config").clone())
        }

        async fn delete_order_topic_config(&self, _topic: CheetahString) -> rocketmq_error::RocketMQResult<()> {
            self.order_writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn reset_consumer_offset_legacy(
            &self,
            _cluster_name: Option<CheetahString>,
            _consumer_group: CheetahString,
            _topic: CheetahString,
            _timestamp: u64,
            _force: bool,
        ) -> rocketmq_error::RocketMQResult<Vec<rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats>>
        {
            unsupported()
        }

        async fn view_message_for_mutation(
            &self,
            _topic: CheetahString,
            _message_id: CheetahString,
        ) -> rocketmq_error::RocketMQResult<MessageExt> {
            unsupported()
        }

        async fn preview_consumer_offset_reset_on_broker(
            &self,
            broker_addr: CheetahString,
            _broker_name: CheetahString,
            _read_queue_nums: u32,
            _consumer_group: CheetahString,
            _topic: CheetahString,
            _timestamp: i64,
        ) -> rocketmq_error::RocketMQResult<Vec<rocketmq_client_rust::MutationConsumerOffsetPreview>> {
            self.record_endpoint("offset", &broker_addr);
            self.preview_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.preview_rows.lock().expect("preview rows").clone())
        }

        async fn reset_consumer_offset_if_current(
            &self,
            _broker_addr: CheetahString,
            _consumer_group: CheetahString,
            _topic: CheetahString,
            queue_id: i32,
            _expected_offset: i64,
            new_offset: i64,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::ConditionalConsumerOffsetOutcome> {
            self.reset_calls.fetch_add(1, Ordering::SeqCst);
            self.offsets.lock().expect("offsets").insert(queue_id, new_offset);
            Ok(rocketmq_client_rust::ConditionalConsumerOffsetOutcome {
                applied: true,
                actual_offset: new_offset,
            })
        }

        async fn mutation_consumer_offset(
            &self,
            _broker_addr: CheetahString,
            _consumer_group: CheetahString,
            _topic: CheetahString,
            queue_id: i32,
        ) -> rocketmq_error::RocketMQResult<i64> {
            self.verify_calls.fetch_add(1, Ordering::SeqCst);
            if self.offset_fail_postread.load(Ordering::SeqCst) && self.reset_calls.load(Ordering::SeqCst) > 0 {
                return Err(RocketMQError::network_connection_failed(
                    "test-broker",
                    "test offset postread failure",
                ));
            }
            self.offsets
                .lock()
                .expect("offsets")
                .get(&queue_id)
                .copied()
                .ok_or_else(|| RocketMQError::illegal_argument("offset was not applied"))
        }

        async fn mutation_topic_config_state(
            &self,
            broker_addr: CheetahString,
            _topic: CheetahString,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::MutationTopicConfigState> {
            self.record_endpoint("topic", &broker_addr);
            self.topic_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.topic_state.lock().expect("topic state").clone())
        }

        async fn replace_topic_config_if_state(
            &self,
            _broker_addr: CheetahString,
            _topic: CheetahString,
            expected_state: ClientExpectedState,
            replacement: ClientTopicConfig,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::MutationStateCasOutcome> {
            self.topic_writes.fetch_add(1, Ordering::SeqCst);
            let mut current = self.topic_state.lock().expect("topic state");
            if self.topic_dirty.load(Ordering::SeqCst) {
                return Ok(rocketmq_client_rust::MutationStateCasOutcome {
                    applied: false,
                    changed: false,
                    state: current.state,
                    persistence: ClientMutationPersistenceState::Failed,
                });
            }
            if current.state != expected_state {
                return Ok(rocketmq_client_rust::MutationStateCasOutcome {
                    applied: false,
                    changed: false,
                    state: current.state,
                    persistence: ClientMutationPersistenceState::NotRequired,
                });
            }
            let changed = current.config.as_ref() != Some(&replacement);
            if changed {
                let next_version = match current.state {
                    ClientExpectedState::Absent => 1,
                    ClientExpectedState::Present { version } => version + 1,
                };
                current.state = ClientExpectedState::Present { version: next_version };
                current.config = Some(replacement);
            }
            let persistence = if changed {
                *self.topic_persistence.lock().expect("topic persistence")
            } else {
                ClientMutationPersistenceState::NotRequired
            };
            if persistence == ClientMutationPersistenceState::Failed {
                self.topic_dirty.store(true, Ordering::SeqCst);
            }
            if let Some(order) = self
                .order_after_topic_write
                .lock()
                .expect("order after Topic write")
                .take()
            {
                *self.order_config.lock().expect("order config") = Some(order);
            }
            Ok(rocketmq_client_rust::MutationStateCasOutcome {
                applied: true,
                changed,
                state: current.state,
                persistence,
            })
        }

        async fn mutation_subscription_group_config_state(
            &self,
            broker_addr: CheetahString,
            _group: CheetahString,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::MutationSubscriptionGroupConfigState> {
            self.record_endpoint("group", &broker_addr);
            self.group_reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.group_state.lock().expect("group state").clone())
        }

        async fn replace_subscription_group_config_if_state(
            &self,
            _broker_addr: CheetahString,
            _group: CheetahString,
            expected_state: ClientExpectedState,
            replacement: ClientSubscriptionGroupConfig,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::MutationStateCasOutcome> {
            self.group_writes.fetch_add(1, Ordering::SeqCst);
            let mut current = self.group_state.lock().expect("group state");
            if self.group_dirty.load(Ordering::SeqCst) {
                return Ok(rocketmq_client_rust::MutationStateCasOutcome {
                    applied: false,
                    changed: false,
                    state: current.state,
                    persistence: ClientMutationPersistenceState::Failed,
                });
            }
            if current.state != expected_state {
                return Ok(rocketmq_client_rust::MutationStateCasOutcome {
                    applied: false,
                    changed: false,
                    state: current.state,
                    persistence: ClientMutationPersistenceState::NotRequired,
                });
            }
            let changed = current.config.as_ref() != Some(&replacement);
            if changed {
                let next_version = match current.state {
                    ClientExpectedState::Absent => 1,
                    ClientExpectedState::Present { version } => version + 1,
                };
                current.state = ClientExpectedState::Present { version: next_version };
                current.config = Some(replacement);
            }
            let persistence = if changed {
                *self.group_persistence.lock().expect("group persistence")
            } else {
                ClientMutationPersistenceState::NotRequired
            };
            if persistence == ClientMutationPersistenceState::Failed {
                self.group_dirty.store(true, Ordering::SeqCst);
            }
            Ok(rocketmq_client_rust::MutationStateCasOutcome {
                applied: true,
                changed,
                state: current.state,
                persistence,
            })
        }

        async fn broker_mutation_config_state(
            &self,
            broker_addr: CheetahString,
        ) -> rocketmq_error::RocketMQResult<ClientBrokerMutationConfigState> {
            self.record_endpoint("broker", &broker_addr);
            self.broker_reads.fetch_add(1, Ordering::SeqCst);
            if self.broker_fail_postread.load(Ordering::SeqCst) && self.broker_writes.load(Ordering::SeqCst) > 0 {
                return Err(RocketMQError::illegal_argument("test postread failure"));
            }
            Ok(*self.broker_state.lock().expect("broker state"))
        }

        async fn mutation_message_request_mode(
            &self,
            broker_addr: CheetahString,
            _topic: CheetahString,
            _consumer_group: CheetahString,
        ) -> rocketmq_error::RocketMQResult<Option<ClientMessageRequestMode>> {
            self.record_endpoint("request_mode", &broker_addr);
            self.request_mode_reads.fetch_add(1, Ordering::SeqCst);
            if self.request_mode_fail_postread.load(Ordering::SeqCst)
                && self.request_mode_writes.load(Ordering::SeqCst) > 0
            {
                return Err(RocketMQError::network_connection_failed(
                    "test-broker",
                    "test request-mode postread failure",
                ));
            }
            Ok(*self.request_mode.lock().expect("request mode"))
        }

        async fn replace_message_request_mode_if_current(
            &self,
            _broker_addr: CheetahString,
            _topic: CheetahString,
            _consumer_group: CheetahString,
            expected: ClientExpectedMessageRequestMode,
            replacement: ClientMessageRequestMode,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::MutationMessageRequestModeOutcome> {
            self.request_mode_writes.fetch_add(1, Ordering::SeqCst);
            let mut current = self.request_mode.lock().expect("request mode");
            if self.request_mode_dirty.load(Ordering::SeqCst) {
                return Ok(rocketmq_client_rust::MutationMessageRequestModeOutcome {
                    applied: false,
                    changed: false,
                    current: *current,
                    persistence: ClientMutationPersistenceState::Failed,
                });
            }
            let matches = match (expected, *current) {
                (ClientExpectedMessageRequestMode::Absent, None) => true,
                (ClientExpectedMessageRequestMode::Present(expected), Some(actual)) => expected == actual,
                _ => false,
            };
            if !matches {
                return Ok(rocketmq_client_rust::MutationMessageRequestModeOutcome {
                    applied: false,
                    changed: false,
                    current: *current,
                    persistence: ClientMutationPersistenceState::NotRequired,
                });
            }
            let changed = *current != Some(replacement);
            *current = Some(replacement);
            let persistence = if changed {
                *self.request_mode_persistence.lock().expect("persistence")
            } else {
                ClientMutationPersistenceState::NotRequired
            };
            if persistence == ClientMutationPersistenceState::Failed {
                self.request_mode_dirty.store(true, Ordering::SeqCst);
            }
            Ok(rocketmq_client_rust::MutationMessageRequestModeOutcome {
                applied: true,
                changed,
                current: *current,
                persistence,
            })
        }

        async fn replace_message_request_mode_if_current_with_timeout(
            &self,
            broker_addr: CheetahString,
            topic: CheetahString,
            consumer_group: CheetahString,
            expected: ClientExpectedMessageRequestMode,
            replacement: ClientMessageRequestMode,
            timeout_millis: u64,
        ) -> rocketmq_error::RocketMQResult<rocketmq_client_rust::MutationMessageRequestModeOutcome> {
            self.request_mode_timeouts
                .lock()
                .expect("request mode timeouts")
                .push(timeout_millis);
            self.replace_message_request_mode_if_current(broker_addr, topic, consumer_group, expected, replacement)
                .await
        }
    }

    fn broker(
        cluster: &str,
        name: &str,
        addresses: impl IntoIterator<Item = (u64, &'static str)>,
    ) -> rocketmq_protocol::protocol::route::route_data_view::BrokerData {
        rocketmq_protocol::protocol::route::route_data_view::BrokerData::new(
            cluster.into(),
            name.into(),
            addresses
                .into_iter()
                .map(|(id, address)| (id, CheetahString::from(address)))
                .collect(),
            None,
        )
    }

    #[test]
    fn supervised_topology_selects_only_exact_cluster_masters() {
        let cluster_info = ClusterInfo::new(
            Some(HashMap::from([
                (
                    CheetahString::from("broker-b"),
                    broker("cluster-a", "broker-b", [(MASTER_ID, "10.0.0.2:10911")]),
                ),
                (
                    CheetahString::from("broker-a"),
                    broker(
                        "cluster-a",
                        "broker-a",
                        [(MASTER_ID, "10.0.0.1:10911"), (1, "10.0.0.1:10912")],
                    ),
                ),
                (
                    CheetahString::from("broker-slave-only"),
                    broker("cluster-a", "broker-slave-only", [(1, "10.0.0.3:10912")]),
                ),
                (
                    CheetahString::from("broker-other"),
                    broker("cluster-b", "broker-other", [(MASTER_ID, "10.0.1.1:10911")]),
                ),
            ])),
            Some(HashMap::from([
                (
                    CheetahString::from("cluster-a"),
                    HashSet::from([CheetahString::from("broker-a"), CheetahString::from("broker-b")]),
                ),
                (
                    CheetahString::from("cluster-b"),
                    HashSet::from([CheetahString::from("broker-other")]),
                ),
            ])),
        );

        let targets = master_targets_by_cluster_name(&cluster_info, "cluster-a").expect("exact cluster");
        assert_eq!(
            targets,
            vec![
                ("broker-a".to_owned(), CheetahString::from("10.0.0.1:10911")),
                ("broker-b".to_owned(), CheetahString::from("10.0.0.2:10911")),
            ]
        );
        assert!(master_targets_by_cluster_name(&cluster_info, "unknown").is_err());
    }

    #[test]
    fn supervised_topology_rejects_corrupt_selected_cluster_before_use() {
        let valid = broker("cluster-a", "broker-a", [(MASTER_ID, "10.0.0.1:10911")]);
        let cluster = |members: HashSet<CheetahString>, table| {
            ClusterInfo::new(
                Some(table),
                Some(HashMap::from([(CheetahString::from("cluster-a"), members)])),
            )
        };

        let missing = cluster(
            HashSet::from([CheetahString::from("missing")]),
            HashMap::from([(CheetahString::from("broker-a"), valid.clone())]),
        );
        assert!(master_targets_by_cluster_name(&missing, "cluster-a").is_err());

        let mismatched_key = cluster(
            HashSet::from([CheetahString::from("table-key")]),
            HashMap::from([(CheetahString::from("table-key"), valid.clone())]),
        );
        assert!(master_targets_by_cluster_name(&mismatched_key, "cluster-a").is_err());

        let mismatched_cluster = cluster(
            HashSet::from([CheetahString::from("broker-a")]),
            HashMap::from([(
                CheetahString::from("broker-a"),
                broker("cluster-b", "broker-a", [(MASTER_ID, "10.0.0.1:10911")]),
            )]),
        );
        assert!(master_targets_by_cluster_name(&mismatched_cluster, "cluster-a").is_err());

        let slave_only = cluster(
            HashSet::from([CheetahString::from("broker-a")]),
            HashMap::from([(
                CheetahString::from("broker-a"),
                broker("cluster-a", "broker-a", [(1, "10.0.0.1:10912")]),
            )]),
        );
        assert!(master_targets_by_cluster_name(&slave_only, "cluster-a").is_err());

        let duplicate_endpoint = cluster(
            HashSet::from([CheetahString::from("broker-a"), CheetahString::from("broker-b")]),
            HashMap::from([
                (
                    CheetahString::from("broker-a"),
                    broker("cluster-a", "broker-a", [(MASTER_ID, "10.0.0.1:10911")]),
                ),
                (
                    CheetahString::from("broker-b"),
                    broker("cluster-a", "broker-b", [(MASTER_ID, "10.0.0.1:10911")]),
                ),
            ]),
        );
        assert!(master_targets_by_cluster_name(&duplicate_endpoint, "cluster-a").is_err());
    }

    #[tokio::test]
    async fn production_preflights_use_only_selected_cluster_master_endpoints() {
        let fake = CountingMutationAdmin::new(1);
        fake.enable_request_mode_target();
        let seal = Arc::new(MutationPlanSeal);
        preflight_topic_with_admin(
            &fake,
            Arc::clone(&seal),
            &TopicMutationPreflightRequest {
                cluster: "cluster-a".to_owned(),
                topic: "orders".to_owned(),
                replacement: TopicReplacement {
                    read_queue_nums: 1,
                    write_queue_nums: 1,
                    perm: 6,
                    order: false,
                    message_type: TopicMessageType::Normal,
                },
            },
        )
        .await
        .expect("Topic preflight");
        preflight_subscription_group_with_admin(
            &fake,
            Arc::clone(&seal),
            &SubscriptionGroupMutationPreflightRequest {
                cluster: "cluster-a".to_owned(),
                consumer_group: "orders-consumer".to_owned(),
                replacement: SubscriptionGroupReplacement {
                    consume_enable: true,
                    consume_from_min_enable: false,
                    consume_broadcast_enable: true,
                    consume_message_orderly: false,
                    retry_queue_nums: 1,
                    retry_max_times: 16,
                    broker_id: 0,
                    which_broker_when_consume_slowly: 1,
                    notify_consumer_ids_changed_enable: true,
                    group_sys_flag: 0,
                    consume_timeout_minute: 15,
                },
            },
        )
        .await
        .expect("group preflight");
        preview_offset_reset_with_admin(&fake, Arc::clone(&seal), &offset_request(false))
            .await
            .expect("offset preflight");
        preflight_broker_config_with_admin(&fake, Arc::clone(&seal), "cluster-a")
            .await
            .expect("broker preflight");
        preflight_request_mode_with_admin(
            &fake,
            Arc::clone(&seal),
            &RequestModePreflightRequest {
                cluster: "cluster-a".to_owned(),
                topic: "orders".to_owned(),
                consumer_group: "orders-consumer".to_owned(),
                replacement: RequestModeValue {
                    mode: RequestMode::Pull,
                    pop_share_queue_num: 0,
                },
            },
        )
        .await
        .expect("request-mode preflight");

        let calls = fake.endpoint_calls.lock().expect("endpoint calls");
        assert_eq!(calls.len(), 7);
        assert_eq!(
            calls.iter().map(|(operation, _)| *operation).collect::<HashSet<_>>(),
            HashSet::from(["topic", "group", "offset", "broker", "request_mode"])
        );
        assert!(calls.iter().all(|(_, endpoint)| endpoint == "10.0.0.1:10911"));
        assert!(calls.iter().all(|(_, endpoint)| endpoint != "10.0.0.1:10912"));
    }

    #[tokio::test]
    async fn targeted_metadata_preflight_is_sorted_and_rejects_invalid_selection_before_state_rpc() {
        let fake = CountingMutationAdmin::with_queue_counts(&[1, 1, 1]);
        let seal = Arc::new(MutationPlanSeal);
        let topic_request = TopicMutationPreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            replacement: TopicReplacement {
                read_queue_nums: 1,
                write_queue_nums: 1,
                perm: 6,
                order: false,
                message_type: TopicMessageType::Normal,
            },
        };
        let group_request = SubscriptionGroupMutationPreflightRequest {
            cluster: "cluster-a".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: SubscriptionGroupReplacement {
                consume_enable: true,
                consume_from_min_enable: false,
                consume_broadcast_enable: false,
                consume_message_orderly: false,
                retry_queue_nums: 1,
                retry_max_times: 16,
                broker_id: 0,
                which_broker_when_consume_slowly: 1,
                notify_consumer_ids_changed_enable: true,
                group_sys_flag: 0,
                consume_timeout_minute: 15,
            },
        };

        let topic_plan = preflight_topic_targets_with_admin(
            &fake,
            Arc::clone(&seal),
            &topic_request,
            &["broker-2".to_owned(), "broker-0".to_owned()],
        )
        .await
        .expect("targeted topic preflight");
        assert_eq!(
            topic_plan
                .preflight_targets()
                .into_iter()
                .map(|target| target.broker_name)
                .collect::<Vec<_>>(),
            ["broker-0", "broker-2"]
        );
        assert_eq!(fake.topic_reads.load(Ordering::SeqCst), 2);

        let group_plan = preflight_subscription_group_targets_with_admin(
            &fake,
            Arc::clone(&seal),
            &group_request,
            &["broker-1".to_owned()],
        )
        .await
        .expect("targeted group preflight");
        assert_eq!(group_plan.preflight_targets()[0].broker_name, "broker-1");
        assert_eq!(fake.group_reads.load(Ordering::SeqCst), 1);

        for invalid in [
            Vec::new(),
            vec!["broker-0".to_owned(), "broker-0".to_owned()],
            vec!["broker-unknown".to_owned()],
            (0..=MAX_METADATA_MUTATION_TARGETS)
                .map(|index| format!("broker-{index}"))
                .collect(),
        ] {
            let topic_reads = fake.topic_reads.load(Ordering::SeqCst);
            let order_reads = fake.order_reads.load(Ordering::SeqCst);
            assert!(
                preflight_topic_targets_with_admin(&fake, Arc::clone(&seal), &topic_request, &invalid,)
                    .await
                    .is_err()
            );
            assert_eq!(fake.topic_reads.load(Ordering::SeqCst), topic_reads);
            assert_eq!(fake.order_reads.load(Ordering::SeqCst), order_reads);
        }

        let mut corrupt = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        corrupt
            .cluster_info
            .broker_addr_table
            .as_mut()
            .expect("broker table")
            .insert(
                CheetahString::from("broker-1"),
                broker("cluster-a", "broker-1", [(MASTER_ID, "10.0.0.1:10911")]),
            );
        assert!(preflight_topic_targets_with_admin(
            &corrupt,
            Arc::clone(&seal),
            &topic_request,
            &["broker-0".to_owned()],
        )
        .await
        .is_err());
        assert_eq!(corrupt.topic_reads.load(Ordering::SeqCst), 0);
        assert_eq!(corrupt.order_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn targeted_topic_order_guard_never_writes_global_kv_and_detects_pre_and_post_races() {
        let request = |order| TopicMutationPreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            replacement: TopicReplacement {
                read_queue_nums: 1,
                write_queue_nums: 1,
                perm: 6,
                order,
                message_type: TopicMessageType::Normal,
            },
        };
        let selected = ["broker-0".to_owned()];
        let seal = Arc::new(MutationPlanSeal);

        let mut unordered = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        unordered
            .cluster_info
            .broker_addr_table
            .as_mut()
            .expect("broker table")
            .insert(
                CheetahString::from_static_str("broker-other"),
                broker("cluster-b", "broker-other", [(MASTER_ID, "10.0.1.1:10911")]),
            );
        unordered
            .cluster_info
            .cluster_addr_table
            .as_mut()
            .expect("cluster table")
            .insert(
                CheetahString::from_static_str("cluster-b"),
                HashSet::from([CheetahString::from_static_str("broker-other")]),
            );
        *unordered.order_config.lock().expect("order config") = Some("broker-other:7;broker-1:9".into());
        let plan = preflight_topic_targets_with_admin(&unordered, Arc::clone(&seal), &request(false), &selected)
            .await
            .expect("unchanged unordered subset");
        let outcome = execute_topic_checked(&unordered, &seal, &plan)
            .await
            .expect("guarded execute");
        assert_eq!(outcome.order_reconciled, Some(true));
        assert_eq!(unordered.topic_writes.load(Ordering::SeqCst), 1);
        assert_eq!(unordered.order_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            unordered.order_config.lock().expect("order config").as_deref(),
            Some("broker-other:7;broker-1:9")
        );

        let ordered = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        *ordered.order_config.lock().expect("order config") = Some("broker-1:9;broker-0:1".into());
        let plan = preflight_topic_targets_with_admin(&ordered, Arc::clone(&seal), &request(true), &selected)
            .await
            .expect("unchanged ordered subset");
        let outcome = execute_topic_checked(&ordered, &seal, &plan)
            .await
            .expect("guarded execute");
        assert_eq!(outcome.order_reconciled, Some(true));
        assert_eq!(ordered.order_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            ordered.order_config.lock().expect("order config").as_deref(),
            Some("broker-1:9;broker-0:1")
        );

        for invalid in [
            "broker-0:1;broker-0:1",
            "broker-0:not-a-number",
            "broker-0:01",
            "broker-0:1;",
            "broker%2d0:1",
        ] {
            let fake = CountingMutationAdmin::with_queue_counts(&[1, 1]);
            *fake.order_config.lock().expect("order config") = Some(invalid.into());
            assert!(
                preflight_topic_targets_with_admin(&fake, Arc::clone(&seal), &request(true), &selected)
                    .await
                    .is_err()
            );
            assert_eq!(fake.topic_reads.load(Ordering::SeqCst), 0);
            assert_eq!(fake.topic_writes.load(Ordering::SeqCst), 0);
            assert_eq!(fake.order_writes.load(Ordering::SeqCst), 0);
        }

        let prechange = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        let plan = preflight_topic_targets_with_admin(&prechange, Arc::clone(&seal), &request(false), &selected)
            .await
            .expect("initial order guard");
        *prechange.order_config.lock().expect("order config") = Some("broker-0:1".into());
        let outcome = execute_topic_checked(&prechange, &seal, &plan)
            .await
            .expect("conflict outcome");
        assert_eq!(outcome.targets[0].failure, Some(MutationFailureCode::Conflict));
        assert_eq!(prechange.topic_writes.load(Ordering::SeqCst), 0);
        assert_eq!(prechange.order_writes.load(Ordering::SeqCst), 0);

        let postchange = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        let plan = preflight_topic_targets_with_admin(&postchange, Arc::clone(&seal), &request(false), &selected)
            .await
            .expect("initial order guard");
        *postchange
            .order_after_topic_write
            .lock()
            .expect("order after Topic write") = Some("broker-0:1".into());
        let outcome = execute_topic_checked(&postchange, &seal, &plan)
            .await
            .expect("partial outcome");
        assert_eq!(outcome.order_reconciled, Some(false));
        assert_eq!(outcome.failures[0].code, MutationFailureCode::OrderReconciliationFailed);
        assert!(outcome.targets[0].applied);
        assert_eq!(postchange.topic_writes.load(Ordering::SeqCst), 1);
        assert_eq!(postchange.order_writes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn broker_patch_mapping_has_exact_six_key_vocabulary() {
        let properties = broker_patch_properties(BrokerMutationConfigPatch {
            auto_create_topic_enable: Some(true),
            auto_create_subscription_group: Some(false),
            broker_permission: Some(6),
            default_topic_queue_nums: Some(8),
            message_index_enable: Some(true),
            trace_topic_enable: Some(false),
        })
        .expect("closed patch");
        let keys = properties.keys().map(CheetahString::as_str).collect::<HashSet<_>>();
        assert_eq!(
            keys,
            HashSet::from([
                "autoCreateTopicEnable",
                "autoCreateSubscriptionGroup",
                "brokerPermission",
                "defaultTopicQueueNums",
                "messageIndexEnable",
                "traceTopicEnable",
            ])
        );
        assert!(broker_patch_properties(BrokerMutationConfigPatch::default()).is_err());
    }

    #[tokio::test]
    async fn targeted_broker_patch_seals_one_validated_master_and_verifies_full_state() {
        let fake = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        let seal = Arc::new(MutationPlanSeal);
        let plan = preflight_broker_config_target_with_admin(&fake, Arc::clone(&seal), "cluster-a", "broker-1")
            .await
            .expect("targeted broker preflight");
        assert_eq!(plan.targets()[0].broker_name, "broker-1");
        assert_eq!(fake.broker_reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            fake.endpoint_calls.lock().expect("endpoint calls").as_slice(),
            [("broker", "10.0.0.2:10911".to_owned())]
        );

        let patch = BrokerMutationConfigPatch {
            auto_create_topic_enable: Some(false),
            auto_create_subscription_group: Some(false),
            broker_permission: Some(4),
            default_topic_queue_nums: Some(16),
            message_index_enable: Some(false),
            trace_topic_enable: Some(true),
        };
        let outcome = execute_broker_config_patch_verified_with_admin(&fake, &seal, &plan, patch)
            .await
            .expect("verified broker patch");
        assert_eq!(outcome.targets.len(), 1);
        let target = &outcome.targets[0];
        assert!(target.applied);
        assert!(target.changed);
        assert_eq!(target.verification, MutationVerificationState::Verified);
        assert_eq!(target.after.expect("postread").generation, 2);
        assert_eq!(target.after.expect("postread").broker_permission, 4);
        assert_eq!(fake.broker_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fake.broker_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            fake.endpoint_calls.lock().expect("endpoint calls").as_slice(),
            [
                ("broker", "10.0.0.2:10911".to_owned()),
                ("broker_write", "10.0.0.2:10911".to_owned()),
                ("broker", "10.0.0.2:10911".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn targeted_broker_patch_never_retries_conflict_and_retains_applied_postread_failure() {
        let conflict = CountingMutationAdmin::new(1);
        let seal = Arc::new(MutationPlanSeal);
        let plan = preflight_broker_config_target_with_admin(&conflict, Arc::clone(&seal), "cluster-a", "broker-0")
            .await
            .expect("broker preflight");
        conflict.broker_conflict.store(true, Ordering::SeqCst);
        let patch = BrokerMutationConfigPatch {
            trace_topic_enable: Some(true),
            ..BrokerMutationConfigPatch::default()
        };
        let outcome = execute_broker_config_patch_verified_with_admin(&conflict, &seal, &plan, patch)
            .await
            .expect("conflict outcome");
        assert_eq!(conflict.broker_writes.load(Ordering::SeqCst), 1);
        assert_eq!(conflict.broker_reads.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.targets[0].failure, Some(MutationFailureCode::Conflict));
        assert!(!outcome.targets[0].applied);

        let postread = CountingMutationAdmin::new(1);
        let seal = Arc::new(MutationPlanSeal);
        let plan = preflight_broker_config_target_with_admin(&postread, Arc::clone(&seal), "cluster-a", "broker-0")
            .await
            .expect("broker preflight");
        postread.broker_fail_postread.store(true, Ordering::SeqCst);
        let outcome = execute_broker_config_patch_verified_with_admin(&postread, &seal, &plan, patch)
            .await
            .expect("postread outcome");
        assert_eq!(postread.broker_writes.load(Ordering::SeqCst), 1);
        assert_eq!(postread.broker_reads.load(Ordering::SeqCst), 2);
        assert!(outcome.targets[0].applied);
        assert_eq!(outcome.targets[0].after, None);
        assert_eq!(outcome.targets[0].verification, MutationVerificationState::Failed);
        assert_eq!(
            outcome.targets[0].failure,
            Some(MutationFailureCode::VerificationFailed)
        );
    }

    #[tokio::test]
    async fn targeted_broker_preflight_validates_full_topology_before_state_read() {
        let mut corrupt = CountingMutationAdmin::with_queue_counts(&[1, 1]);
        corrupt
            .cluster_info
            .broker_addr_table
            .as_mut()
            .expect("broker table")
            .insert(
                CheetahString::from("broker-1"),
                broker("cluster-a", "broker-1", [(MASTER_ID, "10.0.0.1:10911")]),
            );
        assert!(preflight_broker_config_target_with_admin(
            &corrupt,
            Arc::new(MutationPlanSeal),
            "cluster-a",
            "broker-0",
        )
        .await
        .is_err());
        assert_eq!(corrupt.broker_reads.load(Ordering::SeqCst), 0);
        assert_eq!(corrupt.broker_writes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn partial_failures_expose_only_logical_identity() {
        let error = RocketMQError::illegal_argument("backend at 10.0.0.9:10911 with accessKey=secret");
        let failure = client_failure("broker-a".to_owned(), Some(3), &error);
        assert_eq!(failure.broker_name, "broker-a");
        assert_eq!(failure.queue_id, Some(3));
        let serialized = serde_json::to_string(&failure).expect("failure DTO");
        assert!(!serialized.contains("10.0.0.9"));
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn offset_preview_matches_java_force_semantics() {
        for (candidate, force, expected) in [
            (5, false, 5),
            (10, false, 10),
            (15, false, 10),
            (5, true, 5),
            (10, true, 10),
            (15, true, 15),
        ] {
            assert_eq!(planned_offset_for_force(10, candidate, force), expected);
        }
        assert_eq!(planned_offset_for_force(-1, 12, false), -1);
        assert_eq!(planned_offset_for_force(-1, 12, true), 12);
    }

    #[test]
    fn offset_budget_is_query_wide_before_preview_rpc() {
        assert_eq!(checked_offset_target_budget([1_000]).expect("exact limit"), 1_000);
        assert!(checked_offset_target_budget([1_001]).is_err());
        assert!(checked_offset_target_budget([600, 600]).is_err());
        assert!(checked_offset_target_budget(std::iter::repeat_n(16, 64)).is_err());
    }

    fn offset_request(force: bool) -> OffsetResetPreviewRequest {
        OffsetResetPreviewRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            timestamp: 1,
            force,
        }
    }

    #[tokio::test]
    async fn production_offset_workflow_skips_minus_one_without_force_and_executes_with_force() {
        let fake = CountingMutationAdmin::new(1);
        let seal = Arc::new(MutationPlanSeal);
        let plan = preview_offset_reset_with_admin(&fake, Arc::clone(&seal), &offset_request(false))
            .await
            .expect("non-force preflight");
        assert_eq!(plan.rows()[0].current_offset, -1);
        assert!(!plan.rows()[0].changed);
        let outcome = execute_offset_reset_checked(&fake, &seal, &plan)
            .await
            .expect("non-force execute");
        assert!(!outcome.targets[0].changed);
        assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 0);

        let force_plan = preview_offset_reset_with_admin(&fake, Arc::clone(&seal), &offset_request(true))
            .await
            .expect("force preflight");
        assert!(force_plan.rows()[0].changed);
        let outcome = execute_offset_reset_checked(&fake, &seal, &force_plan)
            .await
            .expect("force execute");
        assert!(outcome.targets[0].changed);
        assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn production_offset_postread_error_preserves_applied_truth_as_verification_failure() {
        let fake = CountingMutationAdmin::new(1);
        fake.offset_fail_postread.store(true, Ordering::SeqCst);
        let seal = Arc::new(MutationPlanSeal);
        let plan = preview_offset_reset_with_admin(&fake, Arc::clone(&seal), &offset_request(true))
            .await
            .expect("offset preflight");
        let outcome = execute_offset_reset_checked(&fake, &seal, &plan)
            .await
            .expect("offset execute");
        let target = &outcome.targets[0];
        assert!(target.applied);
        assert!(target.changed);
        assert_eq!(target.observed_offset, None);
        assert_eq!(target.failure, Some(MutationFailureCode::VerificationFailed));
        assert!(target.retryable);
        assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn production_offset_preflight_rejects_corrupt_and_incomplete_rows_before_execution() {
        for rows in [
            vec![rocketmq_client_rust::MutationConsumerOffsetPreview {
                broker_name: "wrong-broker".to_owned(),
                queue_id: 0,
                current_offset: 3,
                planned_offset: 1,
            }],
            vec![
                rocketmq_client_rust::MutationConsumerOffsetPreview {
                    broker_name: "broker-0".to_owned(),
                    queue_id: 0,
                    current_offset: 3,
                    planned_offset: 1,
                },
                rocketmq_client_rust::MutationConsumerOffsetPreview {
                    broker_name: "broker-0".to_owned(),
                    queue_id: 0,
                    current_offset: 3,
                    planned_offset: 1,
                },
            ],
            vec![rocketmq_client_rust::MutationConsumerOffsetPreview {
                broker_name: "broker-0".to_owned(),
                queue_id: 0,
                current_offset: 3,
                planned_offset: 1,
            }],
        ] {
            let fake = CountingMutationAdmin::new(2);
            fake.set_preview_rows(rows);
            assert!(
                preview_offset_reset_with_admin(&fake, Arc::new(MutationPlanSeal), &offset_request(false))
                    .await
                    .is_err()
            );
            assert_eq!(fake.preview_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 0);
            assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn production_offset_preflight_enforces_budget_and_topology_before_preview_rpc() {
        for queue_counts in [vec![1_001], vec![600, 600], vec![16; 64]] {
            let fake = CountingMutationAdmin::with_queue_counts(&queue_counts);
            assert!(
                preview_offset_reset_with_admin(&fake, Arc::new(MutationPlanSeal), &offset_request(false))
                    .await
                    .is_err()
            );
            assert_eq!(fake.preview_calls.load(Ordering::SeqCst), 0);
            assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 0);
        }

        let mut corrupt = CountingMutationAdmin::new(1);
        corrupt.route.broker_datas[0].set_cluster(CheetahString::from_static_str("other-cluster"));
        assert!(
            preview_offset_reset_with_admin(&corrupt, Arc::new(MutationPlanSeal), &offset_request(false))
                .await
                .is_err()
        );
        assert_eq!(corrupt.preview_calls.load(Ordering::SeqCst), 0);
        assert_eq!(corrupt.reset_calls.load(Ordering::SeqCst), 0);

        let exact = CountingMutationAdmin::new(1_000);
        let plan = preview_offset_reset_with_admin(&exact, Arc::new(MutationPlanSeal), &offset_request(false))
            .await
            .expect("exact 1000 target budget");
        assert_eq!(plan.target_count(), 1_000);
        assert_eq!(exact.preview_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn production_offset_execute_rejects_cross_session_plan_before_rpc() {
        let fake = CountingMutationAdmin::new(1);
        let owner = Arc::new(MutationPlanSeal);
        let plan = preview_offset_reset_with_admin(&fake, Arc::clone(&owner), &offset_request(true))
            .await
            .expect("preflight");
        assert!(execute_offset_reset_checked(&fake, &Arc::new(MutationPlanSeal), &plan)
            .await
            .is_err());
        assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 0);
        assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 0);
        assert!(execute_offset_reset_checked(&fake, &owner, &plan).await.is_ok());
        assert_eq!(fake.reset_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fake.verify_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn production_request_mode_workflow_checks_current_cas_and_verifies_once() {
        let fake = CountingMutationAdmin::new(1);
        fake.enable_request_mode_target();
        let seal = Arc::new(MutationPlanSeal);
        let request = RequestModePreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: RequestModeValue {
                mode: RequestMode::Pop,
                pop_share_queue_num: 4,
            },
        };
        let plan = preflight_request_mode_with_admin(&fake, Arc::clone(&seal), &request)
            .await
            .expect("request-mode preflight");
        assert_eq!(plan.targets(), vec![("broker-0".to_owned(), None)]);
        let outcome = execute_request_mode_checked(&fake, &seal, &plan)
            .await
            .expect("request-mode execute");
        assert_eq!(outcome.targets.len(), 1);
        assert!(outcome.targets[0].applied);
        assert_eq!(outcome.targets[0].verification, MutationVerificationState::Verified);
        assert_eq!(fake.request_mode_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fake.request_mode_reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn production_request_mode_postread_error_never_uses_cas_current_as_after() {
        let fake = CountingMutationAdmin::new(1);
        fake.enable_request_mode_target();
        fake.request_mode_fail_postread.store(true, Ordering::SeqCst);
        let seal = Arc::new(MutationPlanSeal);
        let request = RequestModePreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: RequestModeValue {
                mode: RequestMode::Pop,
                pop_share_queue_num: 4,
            },
        };
        let plan = preflight_request_mode_with_admin(&fake, Arc::clone(&seal), &request)
            .await
            .expect("request-mode preflight");
        let outcome = execute_request_mode_checked_with_timeout(&fake, &seal, &plan, 12_345)
            .await
            .expect("request-mode execute");
        let target = &outcome.targets[0];
        assert!(target.applied);
        assert!(target.changed);
        assert_eq!(target.current, None);
        assert_eq!(target.persistence, MutationPersistenceState::Persisted);
        assert_eq!(target.verification, MutationVerificationState::Failed);
        assert_eq!(target.failure, Some(MutationFailureCode::VerificationFailed));
        assert!(target.retryable);
        assert_eq!(fake.request_mode_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fake.request_mode_reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn production_request_mode_preflight_requires_topic_and_group_before_mode_query() {
        let fake = CountingMutationAdmin::new(1);
        let request = RequestModePreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: RequestModeValue {
                mode: RequestMode::Pull,
                pop_share_queue_num: 0,
            },
        };
        let plan = preflight_request_mode_with_admin(&fake, Arc::new(MutationPlanSeal), &request)
            .await
            .expect("missing topic is a target failure");
        assert!(plan.targets().is_empty());
        assert_eq!(plan.failures()[0].code, MutationFailureCode::InvalidData);
        assert_eq!(fake.topic_reads.load(Ordering::SeqCst), 1);
        assert_eq!(fake.group_reads.load(Ordering::SeqCst), 0);
        assert_eq!(fake.request_mode_reads.load(Ordering::SeqCst), 0);

        *fake.topic_state.lock().expect("topic state") = rocketmq_client_rust::MutationTopicConfigState {
            state: ClientExpectedState::Present { version: 1 },
            config: Some(map_topic_replacement_to_client(&TopicReplacement {
                read_queue_nums: 1,
                write_queue_nums: 1,
                perm: 6,
                order: false,
                message_type: TopicMessageType::Normal,
            })),
        };
        let plan = preflight_request_mode_with_admin(&fake, Arc::new(MutationPlanSeal), &request)
            .await
            .expect("missing group is a target failure");
        assert!(plan.targets().is_empty());
        assert_eq!(plan.failures()[0].code, MutationFailureCode::InvalidData);
        assert_eq!(fake.group_reads.load(Ordering::SeqCst), 1);
        assert_eq!(fake.request_mode_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn production_request_mode_timeout_is_forwarded_exactly() {
        let fake = CountingMutationAdmin::new(1);
        fake.enable_request_mode_target();
        let seal = Arc::new(MutationPlanSeal);
        let request = RequestModePreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: RequestModeValue {
                mode: RequestMode::Pop,
                pop_share_queue_num: 3,
            },
        };
        let plan = preflight_request_mode_with_admin(&fake, Arc::clone(&seal), &request)
            .await
            .expect("request-mode preflight");
        let outcome = execute_request_mode_checked_with_timeout(&fake, &seal, &plan, 12_345)
            .await
            .expect("timeout-aware request-mode execute");
        assert!(outcome.targets[0].applied);
        assert_eq!(
            fake.request_mode_timeouts
                .lock()
                .expect("request mode timeouts")
                .as_slice(),
            [12_345]
        );
    }

    #[tokio::test]
    async fn mutation_admin_session_uses_the_real_facade_timeout_dispatch() {
        let runtime_context = RuntimeContext::from_current("mutation-session-facade-dispatch-test");
        let client_runtime =
            create_mutation_client_runtime(runtime_context.service_context("client")).expect("client runtime");
        let facade = rocketmq_client_rust::DefaultMQAdminExt::new(Arc::clone(&client_runtime));
        let inner = crate::client_adapter::lifecycle::AdminSession::from_started(
            facade,
            Arc::new(crate::core::clock::SystemClock),
        );
        let mut session = MutationAdminSession {
            inner,
            plan_seal: Arc::new(MutationPlanSeal),
        };
        let plan = RequestModeMutationPlan {
            seal: Arc::clone(&session.plan_seal),
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: RequestModeValue {
                mode: RequestMode::Pop,
                pop_share_queue_num: 4,
            },
            targets: vec![("broker-a".to_owned(), "127.0.0.1:10911".to_owned(), None)],
            failures: Vec::new(),
        };
        let outcome = SupervisedMutationAdmin::execute_request_mode_with_timeout(&mut session, &plan, 12_345)
            .await
            .expect("the real mutation session maps facade failures into a typed target outcome");
        assert_eq!(outcome.targets.len(), 1);
        assert!(!outcome.targets[0].applied);
        assert_eq!(outcome.targets[0].failure, Some(MutationFailureCode::Unavailable));

        let error = MQAdminMutationExt::replace_message_request_mode_if_current_with_timeout(
            &session.inner.inner,
            "127.0.0.1:10911".into(),
            "orders".into(),
            "orders-consumer".into(),
            ClientExpectedMessageRequestMode::Absent,
            ClientMessageRequestMode {
                mode: rocketmq_model::common::message::message_enum::MessageRequestMode::Pop,
                pop_share_queue_num: 4,
            },
            12_345,
        )
        .await
        .expect_err("an unstarted real facade must reach its concrete inner implementation");
        assert!(matches!(error, RocketMQError::ClientNotStarted));

        session.shutdown().await;
        drop(session);
        client_runtime
            .shutdown()
            .await
            .assert_no_task_leak()
            .expect("client runtime tasks drained");
        runtime_context
            .shutdown_tasks(Duration::from_secs(5))
            .await
            .assert_no_task_leak()
            .expect("runtime tasks drained");
    }

    #[tokio::test]
    async fn production_request_mode_persistence_failure_retains_applied_truth_and_rereads_once() {
        let fake = CountingMutationAdmin::new(1);
        fake.enable_request_mode_target();
        *fake.request_mode_persistence.lock().expect("persistence") = ClientMutationPersistenceState::Failed;
        let seal = Arc::new(MutationPlanSeal);
        let request = RequestModePreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: RequestModeValue {
                mode: RequestMode::Pull,
                pop_share_queue_num: 0,
            },
        };
        let plan = preflight_request_mode_with_admin(&fake, Arc::clone(&seal), &request)
            .await
            .expect("request-mode preflight");
        let outcome = execute_request_mode_checked(&fake, &seal, &plan)
            .await
            .expect("request-mode execute");
        assert!(outcome.targets[0].applied);
        assert!(outcome.targets[0].changed);
        assert_eq!(outcome.targets[0].persistence, MutationPersistenceState::Failed);
        assert_eq!(outcome.targets[0].verification, MutationVerificationState::Verified);
        assert_eq!(outcome.targets[0].failure, Some(MutationFailureCode::PersistenceFailed));
        assert_eq!(fake.request_mode_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fake.request_mode_reads.load(Ordering::SeqCst), 2);

        for replacement in [
            request.replacement,
            RequestModeValue {
                mode: RequestMode::Pop,
                pop_share_queue_num: 4,
            },
        ] {
            let follow_up = RequestModePreflightRequest {
                replacement,
                ..request.clone()
            };
            let plan = preflight_request_mode_with_admin(&fake, Arc::clone(&seal), &follow_up)
                .await
                .expect("dirty request-mode preflight");
            let blocked = execute_request_mode_checked(&fake, &seal, &plan)
                .await
                .expect("dirty request-mode execute");
            let blocked = &blocked.targets[0];
            assert!(!blocked.applied);
            assert!(!blocked.changed);
            assert_eq!(blocked.current, Some(request.replacement));
            assert_eq!(blocked.persistence, MutationPersistenceState::Failed);
            assert_eq!(blocked.verification, MutationVerificationState::Verified);
            assert_eq!(blocked.failure, Some(MutationFailureCode::PersistenceFailed));
        }
        assert_eq!(fake.request_mode_writes.load(Ordering::SeqCst), 3);
        assert_eq!(fake.request_mode_reads.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn production_topic_persistence_failure_rereads_once_and_blocks_order_reconciliation() {
        let fake = CountingMutationAdmin::new(1);
        *fake.topic_persistence.lock().expect("topic persistence") = ClientMutationPersistenceState::Failed;
        let seal = Arc::new(MutationPlanSeal);
        let request = TopicMutationPreflightRequest {
            cluster: "cluster-a".to_owned(),
            topic: "orders".to_owned(),
            replacement: TopicReplacement {
                read_queue_nums: 4,
                write_queue_nums: 4,
                perm: 6,
                order: true,
                message_type: TopicMessageType::Normal,
            },
        };
        let plan = preflight_topic_with_admin(&fake, Arc::clone(&seal), &request)
            .await
            .expect("Topic preflight");
        let outcome = execute_topic_checked(&fake, &seal, &plan).await.expect("Topic execute");
        assert_eq!(outcome.targets.len(), 1);
        assert!(outcome.targets[0].applied);
        assert!(outcome.targets[0].changed);
        assert_eq!(outcome.targets[0].persistence, MutationPersistenceState::Failed);
        assert_eq!(outcome.targets[0].verification, MutationVerificationState::Verified);
        assert_eq!(outcome.targets[0].failure, Some(MutationFailureCode::PersistenceFailed));
        assert_eq!(outcome.order_reconciled, Some(false));
        assert_eq!(fake.topic_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fake.topic_reads.load(Ordering::SeqCst), 2);
        assert_eq!(fake.order_writes.load(Ordering::SeqCst), 0);

        for queues in [4, 5] {
            let follow_up = TopicMutationPreflightRequest {
                replacement: TopicReplacement {
                    read_queue_nums: queues,
                    write_queue_nums: queues,
                    ..request.replacement.clone()
                },
                ..request.clone()
            };
            let plan = preflight_topic_with_admin(&fake, Arc::clone(&seal), &follow_up)
                .await
                .expect("dirty Topic preflight");
            let blocked = execute_topic_checked(&fake, &seal, &plan)
                .await
                .expect("dirty Topic execute");
            let blocked = &blocked.targets[0];
            assert!(!blocked.applied);
            assert!(!blocked.changed);
            assert_eq!(blocked.resulting_state, Some(ExpectedState::Present { version: 1 }));
            assert_eq!(blocked.persistence, MutationPersistenceState::Failed);
            assert_eq!(blocked.failure, Some(MutationFailureCode::PersistenceFailed));
            assert_eq!(
                blocked.verification,
                if queues == 4 {
                    MutationVerificationState::Verified
                } else {
                    MutationVerificationState::Failed
                }
            );
        }
        assert_eq!(fake.topic_writes.load(Ordering::SeqCst), 3);
        assert_eq!(fake.topic_reads.load(Ordering::SeqCst), 6);
        assert_eq!(fake.order_writes.load(Ordering::SeqCst), 0);
        let state = fake.topic_state.lock().expect("topic state");
        assert_eq!(state.state, ClientExpectedState::Present { version: 1 });
        assert_eq!(state.config.as_ref().expect("Topic config").read_queue_nums, 4);
    }

    #[tokio::test]
    async fn production_group_persistence_failure_retains_first_apply_and_blocks_followups() {
        let fake = CountingMutationAdmin::new(1);
        *fake.group_persistence.lock().expect("group persistence") = ClientMutationPersistenceState::Failed;
        let seal = Arc::new(MutationPlanSeal);
        let request = SubscriptionGroupMutationPreflightRequest {
            cluster: "cluster-a".to_owned(),
            consumer_group: "orders-consumer".to_owned(),
            replacement: SubscriptionGroupReplacement {
                consume_enable: true,
                consume_from_min_enable: false,
                consume_broadcast_enable: true,
                consume_message_orderly: false,
                retry_queue_nums: 1,
                retry_max_times: 16,
                broker_id: 0,
                which_broker_when_consume_slowly: 1,
                notify_consumer_ids_changed_enable: true,
                group_sys_flag: 0,
                consume_timeout_minute: 15,
            },
        };
        let plan = preflight_subscription_group_with_admin(&fake, Arc::clone(&seal), &request)
            .await
            .expect("group preflight");
        let outcome = execute_subscription_group_checked(&fake, &seal, &plan)
            .await
            .expect("group execute");
        let first = &outcome.targets[0];
        assert!(first.applied);
        assert!(first.changed);
        assert_eq!(first.resulting_state, Some(ExpectedState::Present { version: 1 }));
        assert_eq!(first.persistence, MutationPersistenceState::Failed);
        assert_eq!(first.verification, MutationVerificationState::Verified);
        assert_eq!(first.failure, Some(MutationFailureCode::PersistenceFailed));

        for retry_max_times in [16, 17] {
            let follow_up = SubscriptionGroupMutationPreflightRequest {
                replacement: SubscriptionGroupReplacement {
                    retry_max_times,
                    ..request.replacement.clone()
                },
                ..request.clone()
            };
            let plan = preflight_subscription_group_with_admin(&fake, Arc::clone(&seal), &follow_up)
                .await
                .expect("dirty group preflight");
            let blocked = execute_subscription_group_checked(&fake, &seal, &plan)
                .await
                .expect("dirty group execute");
            let blocked = &blocked.targets[0];
            assert!(!blocked.applied);
            assert!(!blocked.changed);
            assert_eq!(blocked.resulting_state, Some(ExpectedState::Present { version: 1 }));
            assert_eq!(blocked.persistence, MutationPersistenceState::Failed);
            assert_eq!(blocked.failure, Some(MutationFailureCode::PersistenceFailed));
            assert_eq!(
                blocked.verification,
                if retry_max_times == 16 {
                    MutationVerificationState::Verified
                } else {
                    MutationVerificationState::Failed
                }
            );
        }
        assert_eq!(fake.group_writes.load(Ordering::SeqCst), 3);
        assert_eq!(fake.group_reads.load(Ordering::SeqCst), 6);
        let state = fake.group_state.lock().expect("group state");
        assert_eq!(state.state, ClientExpectedState::Present { version: 1 });
        assert_eq!(state.config.as_ref().expect("group config").retry_max_times, 16);
    }
}
