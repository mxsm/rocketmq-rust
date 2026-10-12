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

use rocketmq_admin_core::core::proxy::ProxyDrainPending;
use rocketmq_admin_core::core::supervised_mutation::MutationFailureCode;
use rocketmq_admin_core::core::supervised_mutation::MutationVerificationState;
use rocketmq_admin_core::core::supervised_operation::OperationAction;
use serde_json::json;

use super::*;
use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;

const PROXY_ENDPOINT: &str = "192.0.2.7:8080";
const DRAIN: &str = "drain-0001";
const OTHER_DRAIN: &str = "other-drain-7";
const FILTER: &str = "debug-0001";
const OTHER_FILTER: &str = "other-debug-7";
const EXPIRES_AT_MILLIS: u64 = 1_788_048_300_000;

const UNAVAILABLE: admin::OperationFailure = admin::OperationFailure {
    code: MutationFailureCode::Unavailable,
    retryable: true,
};

/// One Proxy and one Broker master behind the operation-bound part of a session.
#[derive(Default)]
struct Targets {
    /// The operation that drains the Proxy; `None` while it accepts work.
    draining: Option<String>,
    /// The operation, logger and level of the active log-filter override.
    filter: Option<(String, String, AdminLogLevel)>,
    reads: usize,
    writes: usize,
    endpoints: Vec<String>,
    unreadable: bool,
    lose_verification: bool,
}

struct DrainPlan {
    request: admin::ProxyDrainPreflightRequest,
    planned: Planned<admin::ProxyDrainObservation>,
}

struct FilterPlan {
    request: admin::BrokerLogFilterPreflightRequest,
    planned: Planned<admin::BrokerLogFilterObservation>,
}

impl Targets {
    fn observe_drain(&self, operation_id: &str) -> admin::ProxyDrainObservation {
        let accepting = self.draining.is_none();
        admin::ProxyDrainObservation {
            phase: if accepting {
                AdminDrainPhase::Accepting
            } else {
                AdminDrainPhase::Draining
            },
            operation: admin::ActiveOperation::classify(self.draining.as_deref(), operation_id),
            admission_open: accepting,
            routing_open: accepting,
            readiness_published: accepting,
            zero_pending: accepting,
            pending: ProxyDrainPending {
                sessions: if accepting { 0 } else { 3 },
                ..ProxyDrainPending::default()
            },
        }
    }

    fn observe_filter(
        &self,
        operation_id: &str,
        action: &admin::BrokerLogFilterAction,
    ) -> admin::BrokerLogFilterObservation {
        let holder = self.filter.as_ref().map(|(operation, ..)| operation.as_str());
        admin::BrokerLogFilterObservation {
            level: match action {
                admin::BrokerLogFilterAction::Set(requested) => Some(match &self.filter {
                    Some((_, logger, level)) if requested.logger().starts_with(logger.as_str()) => *level,
                    _ => AdminLogLevel::Info,
                }),
                admin::BrokerLogFilterAction::Restore => None,
            },
            operation: admin::ActiveOperation::classify(holder, operation_id),
            expires_at_millis: holder.map(|_| EXPIRES_AT_MILLIS),
        }
    }

    /// Reads a target for a plan, unless the scenario keeps it out of reach.
    fn read<S>(&mut self, observe: impl FnOnce(&Self) -> S) -> Result<S, admin::OperationFailure> {
        self.reads += 1;
        if self.unreadable {
            Err(UNAVAILABLE)
        } else {
            Ok(observe(self))
        }
    }

    /// Reads a target after a write, unless the scenario loses that read.
    fn read_again<S>(&mut self, observe: impl FnOnce(&Self) -> S) -> Option<S> {
        self.reads += 1;
        (!self.lose_verification).then(|| observe(self))
    }
}

/// Plans like a real session: the action decides what a read state asks for.
fn plan<A: OperationAction>(
    action: &A,
    read: Result<A::Observation, admin::OperationFailure>,
) -> Planned<A::Observation> {
    match read {
        Ok(observed) => {
            let step = action.step(observed);
            Planned {
                before: Some(observed),
                failure: (step == admin::OperationStep::Conflict).then_some(admin::OperationFailure {
                    code: MutationFailureCode::Conflict,
                    retryable: false,
                }),
                changes: step == admin::OperationStep::Change,
            }
        }
        Err(failure) => Planned {
            before: None,
            failure: Some(failure),
            changes: false,
        },
    }
}

fn copy<S: Copy>(planned: &Planned<S>) -> Planned<S> {
    Planned {
        before: planned.before,
        failure: planned.failure,
        changes: planned.changes,
    }
}

/// The report of a plan that sends nothing: it was refused, or nothing has to change.
fn settled<S: Copy>(planned: &Planned<S>) -> Option<admin::OperationReport<S>> {
    if let Some(failure) = planned.failure {
        return Some(admin::OperationReport {
            before: planned.before,
            after: None,
            applied: false,
            changed: false,
            verification: MutationVerificationState::NotPerformed,
            failure: Some(failure.code),
            retryable: failure.retryable,
        });
    }
    (!planned.changes).then_some(admin::OperationReport {
        before: planned.before,
        after: planned.before,
        applied: true,
        changed: false,
        verification: MutationVerificationState::NotPerformed,
        failure: None,
        retryable: false,
    })
}

/// The report of an acknowledged write, verified by the state read after it.
fn written<A: OperationAction>(
    action: &A,
    planned: &Planned<A::Observation>,
    after: Option<A::Observation>,
) -> admin::OperationReport<A::Observation> {
    let verified = after.is_some_and(|after| action.satisfied_by(after));
    admin::OperationReport {
        before: planned.before,
        after,
        applied: true,
        changed: true,
        verification: if verified {
            MutationVerificationState::Verified
        } else {
            MutationVerificationState::Failed
        },
        failure: (!verified).then_some(MutationFailureCode::VerificationFailed),
        retryable: after.is_none(),
    }
}

impl OperationBackend for Targets {
    type ProxyDrainPlan = DrainPlan;
    type LogFilterPlan = FilterPlan;

    fn preflight_proxy_drain<'a>(
        &'a mut self,
        request: &'a admin::ProxyDrainPreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::ProxyDrainPlan, ControlError>> {
        Box::pin(async move {
            self.endpoints.push(request.proxy_addr.clone());
            let read = self.read(|targets| targets.observe_drain(&request.operation_id));
            Ok(DrainPlan {
                request: request.clone(),
                planned: plan(&request.action, read),
            })
        })
    }

    fn planned_proxy_drain(plan: &Self::ProxyDrainPlan) -> Planned<admin::ProxyDrainObservation> {
        copy(&plan.planned)
    }

    fn execute_proxy_drain<'a>(
        &'a mut self,
        plan: &'a Self::ProxyDrainPlan,
    ) -> RuntimeFuture<'a, Result<admin::ProxyDrainReport, ControlError>> {
        Box::pin(async move {
            if let Some(report) = settled(&plan.planned) {
                return Ok(report);
            }
            self.writes += 1;
            self.draining = match plan.request.action {
                admin::ProxyDrainAction::Begin => Some(plan.request.operation_id.clone()),
                admin::ProxyDrainAction::Cancel => None,
            };
            let after = self.read_again(|targets| targets.observe_drain(&plan.request.operation_id));
            Ok(written(&plan.request.action, &plan.planned, after))
        })
    }

    fn preflight_log_filter<'a>(
        &'a mut self,
        request: &'a admin::BrokerLogFilterPreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::LogFilterPlan, ControlError>> {
        Box::pin(async move {
            let read = self.read(|targets| targets.observe_filter(&request.operation_id, &request.action));
            Ok(FilterPlan {
                request: request.clone(),
                planned: plan(&request.action, read),
            })
        })
    }

    fn planned_log_filter(plan: &Self::LogFilterPlan) -> Planned<admin::BrokerLogFilterObservation> {
        copy(&plan.planned)
    }

    fn execute_log_filter<'a>(
        &'a mut self,
        plan: &'a Self::LogFilterPlan,
    ) -> RuntimeFuture<'a, Result<admin::BrokerLogFilterReport, ControlError>> {
        Box::pin(async move {
            if let Some(report) = settled(&plan.planned) {
                return Ok(report);
            }
            self.writes += 1;
            let request = &plan.request;
            self.filter = match &request.action {
                admin::BrokerLogFilterAction::Set(requested) => Some((
                    request.operation_id.clone(),
                    requested.logger().to_owned(),
                    requested.level(),
                )),
                admin::BrokerLogFilterAction::Restore => None,
            };
            let after = self.read_again(|targets| targets.observe_filter(&request.operation_id, &request.action));
            Ok(written(&request.action, &plan.planned, after))
        })
    }
}

fn drain_args(dry_run: bool, operation_id: &str) -> tools::ProxyDrainArgs {
    serde_json::from_value(json!({
        "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
        "cluster": "cluster-a",
        "proxy_name": "proxy-a",
        "operation_id": operation_id,
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": "planned proxy restart"
    }))
    .unwrap()
}

fn set_args(dry_run: bool, operation_id: &str) -> tools::SetBrokerLogFilterTtlArgs {
    serde_json::from_value(json!({
        "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
        "cluster": "cluster-a",
        "broker_name": "broker-a",
        "logger": "rocketmq_broker::processor",
        "level": "DEBUG",
        "ttl_seconds": 300,
        "operation_id": operation_id,
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": "incident diagnosis"
    }))
    .unwrap()
}

fn restore_args(dry_run: bool, operation_id: &str) -> tools::RestoreBrokerLogFilterArgs {
    serde_json::from_value(json!({
        "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
        "cluster": "cluster-a",
        "broker_name": "broker-a",
        "operation_id": operation_id,
        "dry_run": dry_run,
        "confirm": !dry_run,
        "reason": "incident diagnosis"
    }))
    .unwrap()
}

async fn drain(
    targets: &mut Targets,
    operation: tools::ProxyDrainOperation,
    dry_run: bool,
    operation_id: &str,
) -> serde_json::Value {
    let response = run_proxy_drain(targets, operation, drain_args(dry_run, operation_id), PROXY_ENDPOINT)
        .await
        .unwrap();
    serde_json::to_value(response).unwrap()
}

async fn set(targets: &mut Targets, dry_run: bool, operation_id: &str) -> serde_json::Value {
    let response = run_set_log_filter(targets, set_args(dry_run, operation_id))
        .await
        .unwrap();
    serde_json::to_value(response).unwrap()
}

async fn restore(targets: &mut Targets, dry_run: bool, operation_id: &str) -> serde_json::Value {
    let response = run_restore_log_filter(targets, restore_args(dry_run, operation_id))
        .await
        .unwrap();
    serde_json::to_value(response).unwrap()
}

fn accepting() -> serde_json::Value {
    json!({
        "phase": "accepting",
        "held_by": "none",
        "admission_open": true,
        "routing_open": true,
        "readiness_published": true,
        "zero_pending": true,
        "pending": {
            "active_connections": 0,
            "sessions": 0,
            "receipt_handles": 0,
            "prepared_transactions": 0,
            "telemetry_links": 0,
            "remoting_channels": 0,
            "telemetry_commands": 0,
            "rpc_in_flight": 0
        }
    })
}

fn draining(held_by: &str) -> serde_json::Value {
    let mut state = accepting();
    state["phase"] = json!("draining");
    state["held_by"] = json!(held_by);
    for closed in ["admission_open", "routing_open", "readiness_published", "zero_pending"] {
        state[closed] = json!(false);
    }
    state["pending"]["sessions"] = json!(3);
    state
}

/// Asserts that a response names its target only by alias and never repeats an identifier.
fn assert_closed(response: &serde_json::Value) {
    let rendered = response.to_string();
    for forbidden in [PROXY_ENDPOINT, "192.0.2.7", DRAIN, OTHER_DRAIN, FILTER, OTHER_FILTER] {
        assert!(!rendered.contains(forbidden), "{forbidden} in {rendered}");
    }
}

#[tokio::test]
async fn begin_drain_dry_run_reads_state_without_mutating() {
    let mut targets = Targets::default();

    let response = drain(&mut targets, tools::ProxyDrainOperation::ProxyDrainBegin, true, DRAIN).await;

    assert_eq!(
        response,
        json!({
            "schema_version": "rocketmq-mcp-mutation.v1",
            "operation": "proxy_drain_begin",
            "cluster": "cluster-a",
            "mode": "dry_run",
            "status": "planned",
            "error_code": null,
            "target": {"proxy_name": "proxy-a"},
            "before": {"proxy-a": accepting()},
            "requested": {"action": "begin"},
            "after": null,
            "targets": [{
                "proxy_name": "proxy-a",
                "before": accepting(),
                "requested": {"action": "begin"},
                "after": null,
                "applied": false,
                "changed": true,
                "verification": "not_performed",
                "failure": null,
                "retryable": false
            }],
            "warnings": []
        })
    );
    assert_closed(&response);
    assert_eq!((targets.reads, targets.writes), (1, 0));
    assert_eq!(targets.draining, None);
    // The session reads the endpoint it was given for the alias and nothing else.
    assert_eq!(targets.endpoints, [PROXY_ENDPOINT]);
}

#[tokio::test]
async fn begin_then_cancel_restores_accepting_state() {
    let mut targets = Targets::default();

    let begun = drain(&mut targets, tools::ProxyDrainOperation::ProxyDrainBegin, false, DRAIN).await;
    assert_eq!(begun["mode"], "execute");
    assert_eq!(begun["status"], "applied");
    assert_eq!(begun["error_code"], json!(null));
    assert_eq!(begun["before"], json!({"proxy-a": accepting()}));
    assert_eq!(begun["after"], json!({"proxy-a": draining("this_operation")}));
    let target = &begun["targets"][0];
    assert_eq!(target["applied"], true);
    assert_eq!(target["changed"], true);
    assert_eq!(target["verification"], "verified");
    assert_eq!(target["failure"], json!(null));
    assert_closed(&begun);
    assert_eq!(targets.draining.as_deref(), Some(DRAIN));

    // A repeated begin finds its own drain and writes nothing.
    let repeated = drain(&mut targets, tools::ProxyDrainOperation::ProxyDrainBegin, false, DRAIN).await;
    assert_eq!(repeated["status"], "applied");
    assert_eq!(repeated["targets"][0]["changed"], false);
    assert_eq!(repeated["after"], repeated["before"]);
    assert_eq!(targets.writes, 1);

    let planned = drain(&mut targets, tools::ProxyDrainOperation::ProxyDrainCancel, true, DRAIN).await;
    assert_eq!(planned["operation"], "proxy_drain_cancel");
    assert_eq!(planned["status"], "planned");
    assert_eq!(planned["requested"], json!({"action": "cancel"}));
    assert_eq!(planned["targets"][0]["changed"], true);
    assert_eq!(targets.draining.as_deref(), Some(DRAIN));

    let cancelled = drain(&mut targets, tools::ProxyDrainOperation::ProxyDrainCancel, false, DRAIN).await;
    assert_eq!(cancelled["status"], "applied");
    assert_eq!(cancelled["before"], json!({"proxy-a": draining("this_operation")}));
    assert_eq!(cancelled["after"], json!({"proxy-a": accepting()}));
    assert_eq!(cancelled["targets"][0]["verification"], "verified");
    assert_closed(&cancelled);
    assert_eq!(targets.draining, None);
    assert_eq!(targets.writes, 2);

    // Nothing is left to cancel, and saying so again changes nothing.
    let idle = drain(&mut targets, tools::ProxyDrainOperation::ProxyDrainCancel, false, DRAIN).await;
    assert_eq!(idle["status"], "applied");
    assert_eq!(idle["targets"][0]["changed"], false);
    assert_eq!(targets.writes, 2);
}

#[tokio::test]
async fn a_drain_another_operation_holds_is_a_conflict_for_a_plan_and_an_execution() {
    for dry_run in [true, false] {
        for operation in [
            tools::ProxyDrainOperation::ProxyDrainBegin,
            tools::ProxyDrainOperation::ProxyDrainCancel,
        ] {
            let mut targets = Targets {
                draining: Some(OTHER_DRAIN.to_owned()),
                ..Targets::default()
            };

            let response = drain(&mut targets, operation, dry_run, DRAIN).await;

            assert_eq!(response["status"], "conflict", "{operation:?} dry_run={dry_run}");
            assert_eq!(response["error_code"], "precondition_conflict");
            assert_eq!(response["before"], json!({"proxy-a": draining("other_operation")}));
            assert_eq!(response["after"], json!(null));
            let target = &response["targets"][0];
            assert_eq!(target["failure"], "conflict");
            assert_eq!(target["applied"], false);
            assert_eq!(target["changed"], false);
            assert_eq!(target["retryable"], false);
            // The drain of the other operation is reported, never named.
            assert_closed(&response);
            assert_eq!(targets.writes, 0);
            assert_eq!(targets.draining.as_deref(), Some(OTHER_DRAIN));
        }
    }
}

#[tokio::test]
async fn a_proxy_out_of_reach_fails_the_plan_and_an_unverified_drain_stays_applied() {
    let mut unreadable = Targets {
        unreadable: true,
        ..Targets::default()
    };
    let response = drain(
        &mut unreadable,
        tools::ProxyDrainOperation::ProxyDrainBegin,
        true,
        DRAIN,
    )
    .await;
    assert_eq!(response["status"], "failed");
    assert_eq!(response["error_code"], "execution_failed");
    assert_eq!(response["before"], json!({}));
    let target = &response["targets"][0];
    assert_eq!(target["before"], json!(null));
    assert_eq!(target["failure"], "unavailable");
    assert_eq!(target["retryable"], true);
    assert_eq!(target["changed"], false);
    assert_eq!(unreadable.writes, 0);

    let mut unverified = Targets {
        lose_verification: true,
        ..Targets::default()
    };
    let response = drain(
        &mut unverified,
        tools::ProxyDrainOperation::ProxyDrainBegin,
        false,
        DRAIN,
    )
    .await;
    assert_eq!(response["status"], "failed");
    assert_eq!(response["error_code"], "verification_failed");
    assert_eq!(response["after"], json!(null));
    let target = &response["targets"][0];
    assert_eq!(target["applied"], true);
    assert_eq!(target["verification"], "failed");
    assert_eq!(target["failure"], "verification_failed");
    assert_eq!(target["retryable"], true);
    assert_eq!(unverified.writes, 1);
}

#[tokio::test]
async fn set_then_restore_returns_to_the_previous_filter() {
    let mut targets = Targets::default();
    let idle = json!({"level": "INFO", "held_by": "none", "expires_at_millis": null});
    let overridden = json!({"level": "DEBUG", "held_by": "this_operation", "expires_at_millis": EXPIRES_AT_MILLIS});
    let requested = json!({
        "action": "set",
        "logger": "rocketmq_broker::processor",
        "level": "DEBUG",
        "ttl_seconds": 300
    });

    let planned = set(&mut targets, true, FILTER).await;
    assert_eq!(
        planned,
        json!({
            "schema_version": "rocketmq-mcp-mutation.v1",
            "operation": "broker_log_filter_set",
            "cluster": "cluster-a",
            "mode": "dry_run",
            "status": "planned",
            "error_code": null,
            "target": {"broker_name": "broker-a"},
            "before": {"broker-a": idle},
            "requested": requested,
            "after": null,
            "targets": [{
                "broker_name": "broker-a",
                "before": idle,
                "requested": requested,
                "after": null,
                "applied": false,
                "changed": true,
                "verification": "not_performed",
                "failure": null,
                "retryable": false
            }],
            "warnings": []
        })
    );
    assert_eq!((targets.reads, targets.writes), (1, 0));
    assert_eq!(targets.filter, None);

    let applied = set(&mut targets, false, FILTER).await;
    assert_eq!(applied["status"], "applied");
    assert_eq!(applied["before"], json!({"broker-a": idle}));
    assert_eq!(applied["after"], json!({"broker-a": overridden}));
    assert_eq!(applied["targets"][0]["verification"], "verified");
    assert_closed(&applied);

    let restored = restore(&mut targets, false, FILTER).await;
    assert_eq!(restored["operation"], "broker_log_filter_restore");
    assert_eq!(restored["status"], "applied");
    assert_eq!(restored["requested"], json!({"action": "restore"}));
    // A restore names no logger, so it reports who holds the override and no level.
    assert_eq!(
        restored["before"],
        json!({"broker-a": {"level": null, "held_by": "this_operation", "expires_at_millis": EXPIRES_AT_MILLIS}})
    );
    assert_eq!(
        restored["after"],
        json!({"broker-a": {"level": null, "held_by": "none", "expires_at_millis": null}})
    );
    assert_eq!(restored["targets"][0]["changed"], true);
    assert_closed(&restored);
    assert_eq!(targets.filter, None);
    assert_eq!(targets.writes, 2);

    // The logger is back at the level it had before the set.
    let replanned = set(&mut targets, true, FILTER).await;
    assert_eq!(replanned["before"], json!({"broker-a": idle}));

    // Once the override has ended, by a restore or by its time to live, a restore changes nothing.
    let idle_restore = restore(&mut targets, false, FILTER).await;
    assert_eq!(idle_restore["status"], "applied");
    assert_eq!(idle_restore["targets"][0]["changed"], false);
    assert_eq!(targets.writes, 2);
}

#[tokio::test]
async fn restore_with_a_mismatched_operation_id_is_a_conflict() {
    for dry_run in [true, false] {
        let held = Some((
            OTHER_FILTER.to_owned(),
            "rocketmq_broker::processor".to_owned(),
            AdminLogLevel::Debug,
        ));
        let mut targets = Targets {
            filter: held.clone(),
            ..Targets::default()
        };

        let response = restore(&mut targets, dry_run, FILTER).await;

        assert_eq!(response["status"], "conflict", "dry_run={dry_run}");
        assert_eq!(response["error_code"], "precondition_conflict");
        assert_eq!(response["before"]["broker-a"]["held_by"], "other_operation");
        assert_eq!(response["targets"][0]["failure"], "conflict");
        assert_eq!(response["targets"][0]["applied"], false);
        assert_closed(&response);
        // The Broker would end the override for any caller, so nothing may be sent.
        assert_eq!(targets.writes, 0);
        assert_eq!(targets.filter, held);

        // A set does not take the override over either.
        let response = set(&mut targets, dry_run, FILTER).await;
        assert_eq!(response["status"], "conflict");
        assert_eq!(targets.writes, 0);
        assert_eq!(targets.filter, held);
    }
}

#[tokio::test]
async fn a_logger_the_adapter_keeps_closed_is_refused_before_anything_is_read() {
    let mut targets = Targets::default();
    let mut args = set_args(false, FILTER);
    args.logger = "rocketmq_broker::message".to_owned();

    assert_eq!(
        run_set_log_filter(&mut targets, args).await.unwrap_err(),
        ControlError::invalid_argument()
    );
    assert_eq!((targets.reads, targets.writes), (0, 0));
}
