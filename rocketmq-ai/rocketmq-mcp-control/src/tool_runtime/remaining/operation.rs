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

//! Proxy drains and temporary Broker log filters: changes that one operation starts and the same
//! operation ends, each on a single target.

use std::collections::BTreeMap;

use rocketmq_admin_core::core::broker::BrokerLogLevel as AdminLogLevel;
use rocketmq_admin_core::core::proxy::ProxyDrainPhase as AdminDrainPhase;
use rocketmq_admin_core::core::supervised_operation as admin;
use rocketmq_admin_core::core::supervised_operation::SupervisedOperationAdmin;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;

use super::status_from_failures;
use crate::error::ControlError;
use crate::tool_runtime::{map_failure, map_verification, RuntimeFuture};
use crate::tools;

/// What a preflight read and decided, taken out of its sealed plan.
pub(crate) struct Planned<S> {
    pub(crate) before: Option<S>,
    pub(crate) failure: Option<admin::OperationFailure>,
    pub(crate) changes: bool,
}

impl<S: Copy> Planned<S> {
    fn of<A: admin::OperationAction<Observation = S>>(plan: &admin::OperationPlan<A>) -> Self {
        Self {
            before: plan.before(),
            failure: plan.failure(),
            changes: plan.changes(),
        }
    }
}

/// The operation-bound part of a mutation session.
///
/// A plan is an associated type so that it stays sealed to the session that made it.
pub(crate) trait OperationBackend: Send {
    type ProxyDrainPlan: Send;
    type LogFilterPlan: Send;

    fn preflight_proxy_drain<'a>(
        &'a mut self,
        request: &'a admin::ProxyDrainPreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::ProxyDrainPlan, ControlError>>;

    fn planned_proxy_drain(plan: &Self::ProxyDrainPlan) -> Planned<admin::ProxyDrainObservation>;

    fn execute_proxy_drain<'a>(
        &'a mut self,
        plan: &'a Self::ProxyDrainPlan,
    ) -> RuntimeFuture<'a, Result<admin::ProxyDrainReport, ControlError>>;

    fn preflight_log_filter<'a>(
        &'a mut self,
        request: &'a admin::BrokerLogFilterPreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::LogFilterPlan, ControlError>>;

    fn planned_log_filter(plan: &Self::LogFilterPlan) -> Planned<admin::BrokerLogFilterObservation>;

    fn execute_log_filter<'a>(
        &'a mut self,
        plan: &'a Self::LogFilterPlan,
    ) -> RuntimeFuture<'a, Result<admin::BrokerLogFilterReport, ControlError>>;
}

impl OperationBackend for MutationAdminSession {
    type ProxyDrainPlan = admin::ProxyDrainPlan;
    type LogFilterPlan = admin::BrokerLogFilterPlan;

    fn preflight_proxy_drain<'a>(
        &'a mut self,
        request: &'a admin::ProxyDrainPreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::ProxyDrainPlan, ControlError>> {
        Box::pin(async move {
            SupervisedOperationAdmin::preflight_proxy_drain(self, request)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn planned_proxy_drain(plan: &Self::ProxyDrainPlan) -> Planned<admin::ProxyDrainObservation> {
        Planned::of(plan)
    }

    fn execute_proxy_drain<'a>(
        &'a mut self,
        plan: &'a Self::ProxyDrainPlan,
    ) -> RuntimeFuture<'a, Result<admin::ProxyDrainReport, ControlError>> {
        Box::pin(async move {
            SupervisedOperationAdmin::execute_proxy_drain(self, plan)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn preflight_log_filter<'a>(
        &'a mut self,
        request: &'a admin::BrokerLogFilterPreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::LogFilterPlan, ControlError>> {
        Box::pin(async move {
            SupervisedOperationAdmin::preflight_broker_log_filter(self, request)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn planned_log_filter(plan: &Self::LogFilterPlan) -> Planned<admin::BrokerLogFilterObservation> {
        Planned::of(plan)
    }

    fn execute_log_filter<'a>(
        &'a mut self,
        plan: &'a Self::LogFilterPlan,
    ) -> RuntimeFuture<'a, Result<admin::BrokerLogFilterReport, ControlError>> {
        Box::pin(async move {
            SupervisedOperationAdmin::execute_broker_log_filter(self, plan)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }
}

/// What one call planned or did to its single target, before it is shaped for a Tool.
struct Outcome<S> {
    before: Option<S>,
    after: Option<S>,
    applied: bool,
    changed: bool,
    verification: tools::VerificationState,
    failure: Option<tools::FailureCode>,
    retryable: bool,
}

impl<S> Outcome<S> {
    /// A dry run: what the plan read, and whether executing it would change the target.
    fn planned<O>(planned: Planned<O>, state: impl Fn(O) -> S) -> Self {
        Self {
            before: planned.before.map(state),
            after: None,
            applied: false,
            changed: planned.changes,
            verification: tools::VerificationState::NotPerformed,
            failure: planned.failure.map(|failure| map_failure(failure.code)),
            retryable: planned.failure.is_some_and(|failure| failure.retryable),
        }
    }

    fn executed<O>(report: admin::OperationReport<O>, state: impl Fn(O) -> S) -> Self {
        Self {
            before: report.before.map(&state),
            after: report.after.map(&state),
            applied: report.applied,
            changed: report.changed,
            verification: map_verification(report.verification),
            failure: report.failure.map(map_failure),
            retryable: report.retryable,
        }
    }

    /// A target that cannot be planned is a conflict or a failure for a dry run as well, so that
    /// a plan is never read as executable when it is not.
    fn status(&self, dry_run: bool) -> tools::MutationStatus {
        match self.failure {
            None if dry_run => tools::MutationStatus::Planned,
            failure => status_from_failures(std::iter::once(failure)),
        }
    }
}

pub(crate) async fn run_proxy_drain<B: OperationBackend>(
    backend: &mut B,
    operation: tools::ProxyDrainOperation,
    args: tools::ProxyDrainArgs,
    proxy_addr: &str,
) -> Result<tools::ProxyDrainToolResponse, ControlError> {
    let (action, requested) = match operation {
        tools::ProxyDrainOperation::ProxyDrainBegin => (admin::ProxyDrainAction::Begin, tools::ProxyDrainAction::Begin),
        tools::ProxyDrainOperation::ProxyDrainCancel => {
            (admin::ProxyDrainAction::Cancel, tools::ProxyDrainAction::Cancel)
        }
    };
    let request = admin::ProxyDrainPreflightRequest {
        cluster: args.cluster.clone(),
        proxy_addr: proxy_addr.to_owned(),
        operation_id: args.operation_id.clone(),
        action,
    };
    let plan = backend.preflight_proxy_drain(&request).await?;
    let outcome = if args.dry_run {
        Outcome::planned(B::planned_proxy_drain(&plan), drain_state)
    } else {
        Outcome::executed(backend.execute_proxy_drain(&plan).await?, drain_state)
    };
    let status = outcome.status(args.dry_run);
    let requested = tools::ProxyDrainRequested { action: requested };
    let by_name = |state: tools::ProxyDrainState| BTreeMap::from([(args.proxy_name.clone(), state)]);
    Ok(tools::ProxyDrainToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation,
        cluster: args.cluster.clone(),
        mode: mode(args.dry_run),
        status,
        error_code: tools::response_error_code(status, [outcome.failure]),
        target: tools::ProxyDrainResource {
            proxy_name: args.proxy_name.clone(),
        },
        before: outcome.before.map(by_name).unwrap_or_default(),
        requested,
        after: outcome.after.map(by_name),
        targets: vec![tools::ProxyDrainTarget {
            proxy_name: args.proxy_name.clone(),
            before: outcome.before,
            requested,
            after: outcome.after,
            applied: outcome.applied,
            changed: outcome.changed,
            verification: outcome.verification,
            failure: outcome.failure,
            retryable: outcome.retryable,
        }],
        warnings: Vec::new(),
    })
}

pub(crate) async fn run_set_log_filter<B: OperationBackend>(
    backend: &mut B,
    args: tools::SetBrokerLogFilterTtlArgs,
) -> Result<tools::BrokerLogFilterToolResponse, ControlError> {
    let level = match args.level {
        tools::BrokerLogLevel::Info => AdminLogLevel::Info,
        tools::BrokerLogLevel::Debug => AdminLogLevel::Debug,
    };
    let filter = admin::BrokerLogFilterOverride::try_new(args.logger.as_str(), level, args.ttl_seconds)
        .map_err(|_| ControlError::invalid_argument())?;
    let call = LogFilterCall {
        operation: tools::BrokerLogFilterOperation::BrokerLogFilterSet,
        cluster: &args.cluster,
        broker_name: &args.broker_name,
        operation_id: &args.operation_id,
        dry_run: args.dry_run,
        action: admin::BrokerLogFilterAction::Set(filter),
        requested: tools::BrokerLogFilterRequested::Set {
            logger: args.logger.clone(),
            level: args.level,
            ttl_seconds: args.ttl_seconds,
        },
    };
    run_log_filter(backend, call).await
}

pub(crate) async fn run_restore_log_filter<B: OperationBackend>(
    backend: &mut B,
    args: tools::RestoreBrokerLogFilterArgs,
) -> Result<tools::BrokerLogFilterToolResponse, ControlError> {
    let call = LogFilterCall {
        operation: tools::BrokerLogFilterOperation::BrokerLogFilterRestore,
        cluster: &args.cluster,
        broker_name: &args.broker_name,
        operation_id: &args.operation_id,
        dry_run: args.dry_run,
        action: admin::BrokerLogFilterAction::Restore,
        requested: tools::BrokerLogFilterRequested::Restore,
    };
    run_log_filter(backend, call).await
}

/// The parts of a set or a restore call that the shared log-filter flow needs.
struct LogFilterCall<'a> {
    operation: tools::BrokerLogFilterOperation,
    cluster: &'a str,
    broker_name: &'a str,
    operation_id: &'a str,
    dry_run: bool,
    action: admin::BrokerLogFilterAction,
    requested: tools::BrokerLogFilterRequested,
}

async fn run_log_filter<B: OperationBackend>(
    backend: &mut B,
    call: LogFilterCall<'_>,
) -> Result<tools::BrokerLogFilterToolResponse, ControlError> {
    let request = admin::BrokerLogFilterPreflightRequest {
        cluster: call.cluster.to_owned(),
        broker_name: call.broker_name.to_owned(),
        operation_id: call.operation_id.to_owned(),
        action: call.action,
    };
    let plan = backend.preflight_log_filter(&request).await?;
    let outcome = if call.dry_run {
        Outcome::planned(B::planned_log_filter(&plan), filter_state)
    } else {
        Outcome::executed(backend.execute_log_filter(&plan).await?, filter_state)
    };
    let status = outcome.status(call.dry_run);
    let by_name = |state: tools::BrokerLogFilterState| BTreeMap::from([(call.broker_name.to_owned(), state)]);
    Ok(tools::BrokerLogFilterToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: call.operation,
        cluster: call.cluster.to_owned(),
        mode: mode(call.dry_run),
        status,
        error_code: tools::response_error_code(status, [outcome.failure]),
        target: tools::BrokerLogFilterResource {
            broker_name: call.broker_name.to_owned(),
        },
        before: outcome.before.map(by_name).unwrap_or_default(),
        requested: call.requested.clone(),
        after: outcome.after.map(by_name),
        targets: vec![tools::BrokerLogFilterTarget {
            broker_name: call.broker_name.to_owned(),
            before: outcome.before,
            requested: call.requested,
            after: outcome.after,
            applied: outcome.applied,
            changed: outcome.changed,
            verification: outcome.verification,
            failure: outcome.failure,
            retryable: outcome.retryable,
        }],
        warnings: Vec::new(),
    })
}

const fn mode(dry_run: bool) -> tools::MutationMode {
    if dry_run {
        tools::MutationMode::DryRun
    } else {
        tools::MutationMode::Execute
    }
}

const fn holder(operation: admin::ActiveOperation) -> tools::OperationHolder {
    match operation {
        admin::ActiveOperation::None => tools::OperationHolder::None,
        admin::ActiveOperation::Requested => tools::OperationHolder::ThisOperation,
        admin::ActiveOperation::Other => tools::OperationHolder::OtherOperation,
    }
}

fn drain_state(observed: admin::ProxyDrainObservation) -> tools::ProxyDrainState {
    let count = |pending: usize| u64::try_from(pending).unwrap_or(u64::MAX);
    tools::ProxyDrainState {
        phase: match observed.phase {
            AdminDrainPhase::Accepting => tools::ProxyDrainPhase::Accepting,
            AdminDrainPhase::Draining => tools::ProxyDrainPhase::Draining,
            AdminDrainPhase::Drained => tools::ProxyDrainPhase::Drained,
        },
        held_by: holder(observed.operation),
        admission_open: observed.admission_open,
        routing_open: observed.routing_open,
        readiness_published: observed.readiness_published,
        zero_pending: observed.zero_pending,
        pending: tools::ProxyDrainPending {
            active_connections: count(observed.pending.active_connections),
            sessions: count(observed.pending.sessions),
            receipt_handles: count(observed.pending.receipt_handles),
            prepared_transactions: count(observed.pending.prepared_transactions),
            telemetry_links: count(observed.pending.telemetry_links),
            remoting_channels: count(observed.pending.remoting_channels),
            telemetry_commands: count(observed.pending.telemetry_commands),
            rpc_in_flight: count(observed.pending.rpc_in_flight),
        },
    }
}

fn filter_state(observed: admin::BrokerLogFilterObservation) -> tools::BrokerLogFilterState {
    tools::BrokerLogFilterState {
        level: observed.level.map(|level| match level {
            AdminLogLevel::Info => tools::BrokerLogLevel::Info,
            AdminLogLevel::Debug => tools::BrokerLogLevel::Debug,
        }),
        held_by: holder(observed.operation),
        expires_at_millis: observed.expires_at_millis,
    }
}

#[cfg(test)]
#[path = "operation/tests.rs"]
mod tests;
