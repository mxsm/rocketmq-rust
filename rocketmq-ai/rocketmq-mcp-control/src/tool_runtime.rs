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

#![cfg(feature = "write-tools")]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_admin_core::core::supervised_mutation as admin;
use rocketmq_runtime::TaskGroup;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::audit::AuditTrail;
use crate::error::ControlError;
use crate::guard::AuthorizedMutation;
use crate::model::ClusterName;
use crate::model::ControlOperation;
use crate::model::Principal;
use crate::tools;

const IDEMPOTENCY_TTL: Duration = Duration::from_secs(10 * 60);
const IDEMPOTENCY_CAPACITY: usize = 4096;
const SESSION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) type RuntimeFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone)]
pub(crate) enum UpsertRequest {
    Topic(tools::UpsertTopicArgs),
    ConsumerGroup(tools::UpsertConsumerGroupArgs),
}

impl UpsertRequest {
    fn operation(&self) -> ControlOperation {
        match self {
            Self::Topic(_) => ControlOperation::TopicUpsert,
            Self::ConsumerGroup(_) => ControlOperation::ConsumerGroupUpsert,
        }
    }

    fn dry_run(&self) -> bool {
        match self {
            Self::Topic(args) => args.dry_run,
            Self::ConsumerGroup(args) => args.dry_run,
        }
    }

    fn request_key(&self) -> Option<&str> {
        match self {
            Self::Topic(args) => args.request_key.as_deref(),
            Self::ConsumerGroup(args) => args.request_key.as_deref(),
        }
    }

    fn target_names(&self) -> &[String] {
        match self {
            Self::Topic(args) => &args.broker_names,
            Self::ConsumerGroup(args) => &args.broker_names,
        }
    }

    fn canonical_payload(&self) -> Result<String, ControlError> {
        let mut canonical = self.clone();
        match &mut canonical {
            Self::Topic(args) => args.broker_names.sort(),
            Self::ConsumerGroup(args) => args.broker_names.sort(),
        }
        serde_json::to_string(&canonical).map_err(|_| ControlError::invalid_arguments())
    }
}

impl Serialize for UpsertRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Topic(args) => args.serialize(serializer),
            Self::ConsumerGroup(args) => args.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(crate) enum UpsertResponse {
    Topic(tools::TopicMutationToolResponse),
    ConsumerGroup(tools::ConsumerGroupMutationToolResponse),
}

impl UpsertResponse {
    pub(crate) fn is_error(&self) -> bool {
        match self {
            Self::Topic(response) => response.is_error(),
            Self::ConsumerGroup(response) => response.is_error(),
        }
    }

    fn audit_error(&self) -> Option<crate::error::ControlErrorCode> {
        let status = match self {
            Self::Topic(response) => response.status,
            Self::ConsumerGroup(response) => response.status,
        };
        match status {
            tools::MutationStatus::Conflict => Some(crate::error::ControlErrorCode::Conflict),
            tools::MutationStatus::Partial | tools::MutationStatus::Failed => {
                Some(crate::error::ControlErrorCode::ExecutionFailed)
            }
            tools::MutationStatus::Planned | tools::MutationStatus::Applied => None,
        }
    }
}

pub(crate) trait UpsertSession: Send {
    fn run<'a>(&'a mut self, request: UpsertRequest) -> RuntimeFuture<'a, Result<UpsertResponse, ControlError>>;
    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>>;
}

pub(crate) trait UpsertSessionFactory: Send + Sync {
    fn open<'a>(&'a self, cluster: &'a ClusterName) -> RuntimeFuture<'a, Result<Box<dyn UpsertSession>, ControlError>>;
}

#[derive(Clone)]
pub(crate) struct ToolRuntime {
    audit: AuditTrail,
    factory: Arc<dyn UpsertSessionFactory>,
    operation_timeout: Duration,
    owner: TaskGroup,
    idempotency: Arc<Mutex<IdempotencyState>>,
}

impl ToolRuntime {
    pub(crate) fn new(
        audit: AuditTrail,
        factory: Arc<dyn UpsertSessionFactory>,
        operation_timeout: Duration,
        owner: TaskGroup,
    ) -> Self {
        Self {
            audit,
            factory,
            operation_timeout,
            owner,
            idempotency: Arc::new(Mutex::new(IdempotencyState::default())),
        }
    }

    pub(crate) async fn execute(
        &self,
        principal: &Principal,
        authorized: &AuthorizedMutation,
        request: UpsertRequest,
        cancellation: CancellationToken,
    ) -> Result<UpsertResponse, ControlError> {
        let cluster = authorized.cluster().clone();
        let identity = IdempotencyIdentity::from_request(principal, &cluster, &request)?;
        let admission =
            match idempotency::admit_cache(&self.idempotency, identity.key.as_ref(), &identity.payload).await {
                Ok(admission) => admission,
                Err(idempotency::AdmissionError::Capacity) => return Err(ControlError::operation_unavailable()),
                Err(idempotency::AdmissionError::Collision) => {
                    let invocation = self
                        .audit
                        .start(authorized.operation(), authorized.cluster(), request.dry_run())
                        .await?;
                    let error = ControlError::invalid_arguments();
                    self.audit.terminal(&invocation, Some(error.code())).await?;
                    return Err(error);
                }
            };
        let is_leader = matches!(&admission, idempotency::CacheAdmission::Leader);
        let invocation = match self
            .audit
            .start(authorized.operation(), authorized.cluster(), request.dry_run())
            .await
        {
            Ok(invocation) => invocation,
            Err(error) => {
                if is_leader {
                    idempotency::abort_cache_reservation(&self.idempotency, &identity, error.clone()).await;
                }
                return Err(error);
            }
        };
        let (sender, receiver) = oneshot::channel();
        let audit = self.audit.clone();
        let factory = self.factory.clone();
        let operation_timeout = self.operation_timeout;
        let owner_cancellation = self.owner.cancellation_token();
        let cache = self.idempotency.clone();
        let cleanup_identity = identity.clone();
        let task_invocation = invocation.clone();
        let spawn = self.owner.spawn_service("mcp-control-upsert-supervisor", async move {
            let result = execute_admitted(
                cache,
                identity,
                admission,
                factory,
                cluster,
                request,
                operation_timeout,
                cancellation,
                owner_cancellation,
            )
            .await;
            let error_code = match &result {
                Ok(response) => response.audit_error(),
                Err(error) => Some(error.code()),
            };
            let terminal = audit.terminal(&task_invocation, error_code).await;
            let delivered = terminal.map_or_else(Err, |_| result);
            let _ = sender.send(delivered);
        });
        if spawn.is_err() {
            let error = ControlError::execution_failed();
            if is_leader {
                idempotency::abort_cache_reservation(&self.idempotency, &cleanup_identity, error.clone()).await;
            }
            self.audit.terminal(&invocation, Some(error.code())).await?;
            return Err(error);
        }
        receiver.await.map_err(|_| ControlError::audit_unavailable())?
    }
}

pub(crate) mod admin_session;
mod execution;
mod idempotency;

pub(crate) use admin_session::AdminUpsertFactory;
use idempotency::execute_admitted;
use idempotency::IdempotencyIdentity;
use idempotency::IdempotencyState;

fn topic_before(
    args: &tools::UpsertTopicArgs,
    targets: Vec<admin::MetadataPreflightTarget<admin::TopicReplacement>>,
) -> BTreeMap<String, tools::VisibleState<tools::TopicReplacement>> {
    let mut states = args
        .broker_names
        .iter()
        .cloned()
        .map(|broker| (broker, tools::VisibleState::Unknown))
        .collect::<BTreeMap<_, _>>();
    for target in targets {
        states.insert(target.broker_name, topic_visible_state(target.state, target.current));
    }
    states
}

fn group_before(
    args: &tools::UpsertConsumerGroupArgs,
    targets: Vec<admin::MetadataPreflightTarget<admin::SubscriptionGroupReplacement>>,
) -> BTreeMap<String, tools::VisibleState<tools::ConsumerGroupReplacement>> {
    let mut states = args
        .broker_names
        .iter()
        .cloned()
        .map(|broker| (broker, tools::VisibleState::Unknown))
        .collect::<BTreeMap<_, _>>();
    for target in targets {
        states.insert(target.broker_name, group_visible_state(target.state, target.current));
    }
    states
}

fn topic_dry_run(
    args: &tools::UpsertTopicArgs,
    before: BTreeMap<String, tools::VisibleState<tools::TopicReplacement>>,
    failures: &[admin::MutationTargetFailure],
) -> tools::TopicMutationToolResponse {
    let failures = failure_map(failures);
    let aggregate_before = before.clone();
    let targets: Vec<_> = before
        .into_iter()
        .map(|(broker_name, before)| {
            let failure = failures.get(&broker_name).copied();
            tools::MutationTarget {
                target: tools::LogicalMutationTarget { broker_name },
                before,
                requested: args.replacement.clone(),
                after: None,
                applied: false,
                changed: false,
                persistence: tools::PersistenceState::NotRequired,
                verification: tools::VerificationState::NotPerformed,
                failure: failure.map(|(code, _)| code),
                retryable: failure.is_some_and(|(_, retryable)| retryable),
            }
        })
        .collect();
    let status = dry_run_status(targets.len(), failures.len());
    topic_response(
        args,
        tools::MutationMode::DryRun,
        status,
        aggregate_before,
        None,
        targets,
        Vec::new(),
    )
}

fn group_dry_run(
    args: &tools::UpsertConsumerGroupArgs,
    before: BTreeMap<String, tools::VisibleState<tools::ConsumerGroupReplacement>>,
    failures: &[admin::MutationTargetFailure],
) -> tools::ConsumerGroupMutationToolResponse {
    let failures = failure_map(failures);
    let aggregate_before = before.clone();
    let targets: Vec<_> = before
        .into_iter()
        .map(|(broker_name, before)| {
            let failure = failures.get(&broker_name).copied();
            tools::MutationTarget {
                target: tools::LogicalMutationTarget { broker_name },
                before,
                requested: args.replacement.clone(),
                after: None,
                applied: false,
                changed: false,
                persistence: tools::PersistenceState::NotRequired,
                verification: tools::VerificationState::NotPerformed,
                failure: failure.map(|(code, _)| code),
                retryable: failure.is_some_and(|(_, retryable)| retryable),
            }
        })
        .collect();
    let status = dry_run_status(targets.len(), failures.len());
    group_response(
        args,
        tools::MutationMode::DryRun,
        status,
        aggregate_before,
        None,
        targets,
        Vec::new(),
    )
}

fn topic_executed(
    args: &tools::UpsertTopicArgs,
    before: BTreeMap<String, tools::VisibleState<tools::TopicReplacement>>,
    outcome: admin::MetadataMutationOutcome,
    observed: Option<Vec<admin::MetadataPreflightTarget<admin::TopicReplacement>>>,
) -> tools::TopicMutationToolResponse {
    let aggregate_before = before.clone();
    let after = observed.map(|targets| topic_before(args, targets));
    let outcomes = outcome
        .targets
        .into_iter()
        .map(|target| (target.broker_name.clone(), target))
        .collect::<BTreeMap<_, _>>();
    let failures = failure_map(&outcome.failures);
    let targets = build_executed_targets(
        before,
        after.clone().unwrap_or_default(),
        outcomes,
        failures,
        args.replacement.clone(),
    );
    let mut warnings = Vec::new();
    if outcome.order_reconciled == Some(false) {
        warnings.push("topic order configuration was not reconciled".to_owned());
    }
    let mut status = execution_status(&targets);
    if !warnings.is_empty() && status == tools::MutationStatus::Applied {
        status = tools::MutationStatus::Partial;
    }
    topic_response(
        args,
        tools::MutationMode::Execute,
        status,
        aggregate_before,
        after,
        targets,
        warnings,
    )
}

fn group_executed(
    args: &tools::UpsertConsumerGroupArgs,
    before: BTreeMap<String, tools::VisibleState<tools::ConsumerGroupReplacement>>,
    outcome: admin::MetadataMutationOutcome,
    observed: Option<Vec<admin::MetadataPreflightTarget<admin::SubscriptionGroupReplacement>>>,
) -> tools::ConsumerGroupMutationToolResponse {
    let aggregate_before = before.clone();
    let after = observed.map(|targets| group_before(args, targets));
    let outcomes = outcome
        .targets
        .into_iter()
        .map(|target| (target.broker_name.clone(), target))
        .collect::<BTreeMap<_, _>>();
    let failures = failure_map(&outcome.failures);
    let targets = build_executed_targets(
        before,
        after.clone().unwrap_or_default(),
        outcomes,
        failures,
        args.replacement.clone(),
    );
    let status = execution_status(&targets);
    group_response(
        args,
        tools::MutationMode::Execute,
        status,
        aggregate_before,
        after,
        targets,
        Vec::new(),
    )
}

fn build_executed_targets<T: Clone + PartialEq>(
    before: BTreeMap<String, tools::VisibleState<T>>,
    after: BTreeMap<String, tools::VisibleState<T>>,
    mut outcomes: BTreeMap<String, admin::MetadataMutationTargetOutcome>,
    failures: BTreeMap<String, (tools::FailureCode, bool)>,
    requested: T,
) -> Vec<tools::MutationTarget<T>> {
    before
        .into_iter()
        .map(|(broker_name, before)| {
            let outcome = outcomes.remove(&broker_name);
            let applied = outcome.as_ref().is_some_and(|target| target.applied);
            let observed_matches = matches!(
                after.get(&broker_name),
                Some(tools::VisibleState::Present { value, .. }) if value == &requested
            );
            let failure = outcome
                .as_ref()
                .and_then(|target| target.failure.map(map_failure))
                .or_else(|| failures.get(&broker_name).map(|(code, _)| *code))
                .or_else(|| (applied && !observed_matches).then_some(tools::FailureCode::VerificationFailed));
            tools::MutationTarget {
                target: tools::LogicalMutationTarget {
                    broker_name: broker_name.clone(),
                },
                before,
                requested: requested.clone(),
                after: after.get(&broker_name).cloned(),
                applied,
                changed: outcome.as_ref().is_some_and(|target| target.changed),
                persistence: outcome
                    .as_ref()
                    .map(|target| map_persistence(target.persistence))
                    .unwrap_or(tools::PersistenceState::NotRequired),
                verification: if applied {
                    if observed_matches {
                        tools::VerificationState::Verified
                    } else {
                        tools::VerificationState::Failed
                    }
                } else {
                    outcome
                        .as_ref()
                        .map(|target| map_verification(target.verification))
                        .unwrap_or(tools::VerificationState::Failed)
                },
                failure,
                retryable: outcome.as_ref().is_some_and(|target| target.retryable)
                    || failures.get(&broker_name).is_some_and(|(_, retryable)| *retryable),
            }
        })
        .collect()
}

fn execution_status<T>(targets: &[tools::MutationTarget<T>]) -> tools::MutationStatus {
    let succeeded = targets.iter().filter(|target| target.failure.is_none()).count();
    let failed = targets.len().saturating_sub(succeeded);
    if failed == 0 {
        tools::MutationStatus::Applied
    } else if succeeded > 0 {
        tools::MutationStatus::Partial
    } else if targets
        .iter()
        .all(|target| target.failure == Some(tools::FailureCode::Conflict))
    {
        tools::MutationStatus::Conflict
    } else {
        tools::MutationStatus::Failed
    }
}

fn dry_run_status(target_count: usize, failure_count: usize) -> tools::MutationStatus {
    let succeeded = target_count.saturating_sub(failure_count);
    if failure_count == 0 {
        tools::MutationStatus::Planned
    } else if succeeded == 0 {
        tools::MutationStatus::Failed
    } else {
        tools::MutationStatus::Partial
    }
}

fn topic_response(
    args: &tools::UpsertTopicArgs,
    mode: tools::MutationMode,
    status: tools::MutationStatus,
    before: BTreeMap<String, tools::VisibleState<tools::TopicReplacement>>,
    after: Option<BTreeMap<String, tools::VisibleState<tools::TopicReplacement>>>,
    targets: Vec<tools::MutationTarget<tools::TopicReplacement>>,
    warnings: Vec<String>,
) -> tools::TopicMutationToolResponse {
    let mut brokers = args.broker_names.clone();
    brokers.sort();
    tools::MutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::TopicUpsertOperation::TopicUpsert,
        cluster: args.cluster.clone(),
        mode,
        status,
        target: tools::TopicMutationResource {
            topic: args.topic.clone(),
            brokers,
        },
        before,
        requested: args.replacement.clone(),
        after,
        targets,
        warnings,
    }
}

fn group_response(
    args: &tools::UpsertConsumerGroupArgs,
    mode: tools::MutationMode,
    status: tools::MutationStatus,
    before: BTreeMap<String, tools::VisibleState<tools::ConsumerGroupReplacement>>,
    after: Option<BTreeMap<String, tools::VisibleState<tools::ConsumerGroupReplacement>>>,
    targets: Vec<tools::MutationTarget<tools::ConsumerGroupReplacement>>,
    warnings: Vec<String>,
) -> tools::ConsumerGroupMutationToolResponse {
    let mut brokers = args.broker_names.clone();
    brokers.sort();
    tools::MutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::ConsumerGroupUpsertOperation::ConsumerGroupUpsert,
        cluster: args.cluster.clone(),
        mode,
        status,
        target: tools::ConsumerGroupMutationResource {
            consumer_group: args.consumer_group.clone(),
            brokers,
        },
        before,
        requested: args.replacement.clone(),
        after,
        targets,
        warnings,
    }
}

fn topic_visible_state(
    state: admin::ExpectedState,
    current: Option<admin::TopicReplacement>,
) -> tools::VisibleState<tools::TopicReplacement> {
    match (state, current) {
        (admin::ExpectedState::Absent, None) => tools::VisibleState::Absent,
        (admin::ExpectedState::Present { version }, Some(value)) => tools::VisibleState::Present {
            version,
            value: map_topic_from_admin(value),
        },
        _ => tools::VisibleState::Unknown,
    }
}

fn group_visible_state(
    state: admin::ExpectedState,
    current: Option<admin::SubscriptionGroupReplacement>,
) -> tools::VisibleState<tools::ConsumerGroupReplacement> {
    match (state, current) {
        (admin::ExpectedState::Absent, None) => tools::VisibleState::Absent,
        (admin::ExpectedState::Present { version }, Some(value)) => tools::VisibleState::Present {
            version,
            value: map_group_from_admin(value),
        },
        _ => tools::VisibleState::Unknown,
    }
}

fn failure_map(failures: &[admin::MutationTargetFailure]) -> BTreeMap<String, (tools::FailureCode, bool)> {
    failures
        .iter()
        .map(|failure| {
            (
                failure.broker_name.clone(),
                (map_failure(failure.code), failure.retryable),
            )
        })
        .collect()
}

fn map_failure(code: admin::MutationFailureCode) -> tools::FailureCode {
    match code {
        admin::MutationFailureCode::Conflict => tools::FailureCode::Conflict,
        admin::MutationFailureCode::InvalidData => tools::FailureCode::InvalidData,
        admin::MutationFailureCode::Unavailable => tools::FailureCode::Unavailable,
        admin::MutationFailureCode::PersistenceFailed => tools::FailureCode::PersistenceFailed,
        admin::MutationFailureCode::VerificationFailed => tools::FailureCode::VerificationFailed,
        admin::MutationFailureCode::OrderReconciliationFailed => tools::FailureCode::OrderReconciliationFailed,
    }
}

fn map_persistence(state: admin::MutationPersistenceState) -> tools::PersistenceState {
    match state {
        admin::MutationPersistenceState::NotRequired => tools::PersistenceState::NotRequired,
        admin::MutationPersistenceState::Persisted => tools::PersistenceState::Persisted,
        admin::MutationPersistenceState::Failed => tools::PersistenceState::Failed,
    }
}

fn map_verification(state: admin::MutationVerificationState) -> tools::VerificationState {
    match state {
        admin::MutationVerificationState::NotPerformed => tools::VerificationState::NotPerformed,
        admin::MutationVerificationState::Verified => tools::VerificationState::Verified,
        admin::MutationVerificationState::Failed => tools::VerificationState::Failed,
    }
}

fn map_topic_to_admin(value: &tools::TopicReplacement) -> admin::TopicReplacement {
    admin::TopicReplacement {
        read_queue_nums: value.read_queue_nums,
        write_queue_nums: value.write_queue_nums,
        perm: value.perm,
        order: value.order,
        message_type: match value.message_type {
            tools::TopicMessageType::Normal => admin::TopicMessageType::Normal,
            tools::TopicMessageType::Fifo => admin::TopicMessageType::Fifo,
            tools::TopicMessageType::Delay => admin::TopicMessageType::Delay,
            tools::TopicMessageType::Transaction => admin::TopicMessageType::Transaction,
            tools::TopicMessageType::Unspecified => admin::TopicMessageType::Unspecified,
        },
    }
}

fn map_topic_from_admin(value: admin::TopicReplacement) -> tools::TopicReplacement {
    tools::TopicReplacement {
        read_queue_nums: value.read_queue_nums,
        write_queue_nums: value.write_queue_nums,
        perm: value.perm,
        order: value.order,
        message_type: match value.message_type {
            admin::TopicMessageType::Normal => tools::TopicMessageType::Normal,
            admin::TopicMessageType::Fifo => tools::TopicMessageType::Fifo,
            admin::TopicMessageType::Delay => tools::TopicMessageType::Delay,
            admin::TopicMessageType::Transaction => tools::TopicMessageType::Transaction,
            admin::TopicMessageType::Unspecified => tools::TopicMessageType::Unspecified,
        },
    }
}

fn map_group_to_admin(value: &tools::ConsumerGroupReplacement) -> admin::SubscriptionGroupReplacement {
    admin::SubscriptionGroupReplacement {
        consume_enable: value.consume_enable,
        consume_from_min_enable: value.consume_from_min_enable,
        consume_broadcast_enable: value.consume_broadcast_enable,
        consume_message_orderly: value.consume_message_orderly,
        retry_queue_nums: value.retry_queue_nums,
        retry_max_times: value.retry_max_times,
        broker_id: value.broker_id,
        which_broker_when_consume_slowly: value.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: value.notify_consumer_ids_changed_enable,
        group_sys_flag: value.group_sys_flag,
        consume_timeout_minute: value.consume_timeout_minute,
    }
}

fn map_group_from_admin(value: admin::SubscriptionGroupReplacement) -> tools::ConsumerGroupReplacement {
    tools::ConsumerGroupReplacement {
        consume_enable: value.consume_enable,
        consume_from_min_enable: value.consume_from_min_enable,
        consume_broadcast_enable: value.consume_broadcast_enable,
        consume_message_orderly: value.consume_message_orderly,
        retry_queue_nums: value.retry_queue_nums,
        retry_max_times: value.retry_max_times,
        broker_id: value.broker_id,
        which_broker_when_consume_slowly: value.which_broker_when_consume_slowly,
        notify_consumer_ids_changed_enable: value.notify_consumer_ids_changed_enable,
        group_sys_flag: value.group_sys_flag,
        consume_timeout_minute: value.consume_timeout_minute,
    }
}

#[cfg(test)]
mod tests;
