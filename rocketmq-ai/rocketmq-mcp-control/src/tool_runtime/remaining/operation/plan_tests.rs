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

//! The plans of operation-bound calls: what their digest covers, and that an execution compares
//! it before it writes.

use super::*;
use crate::tool_runtime::plan_binding::plan_digest;
use crate::tool_runtime::MutationToolRequest;

const BEGIN: tools::ProxyDrainOperation = tools::ProxyDrainOperation::ProxyDrainBegin;
const CANCEL: tools::ProxyDrainOperation = tools::ProxyDrainOperation::ProxyDrainCancel;

fn drain_request(operation: tools::ProxyDrainOperation, args: tools::ProxyDrainArgs) -> MutationToolRequest {
    match operation {
        tools::ProxyDrainOperation::ProxyDrainBegin => MutationToolRequest::ProxyDrainBegin(args),
        tools::ProxyDrainOperation::ProxyDrainCancel => MutationToolRequest::ProxyDrainCancel(args),
    }
}

/// Runs one drain call as a session does: bound to the plan that its request names.
async fn run_drain(
    targets: &mut Targets,
    operation: tools::ProxyDrainOperation,
    args: tools::ProxyDrainArgs,
) -> Result<tools::ProxyDrainToolResponse, ControlError> {
    let binding = PlanBinding::of(&drain_request(operation, args.clone())).unwrap();
    run_proxy_drain(targets, operation, args, PROXY_ENDPOINT, &binding).await
}

/// The digest of the plan that a dry run reports for the Proxy as it is.
async fn planned_drain(targets: &mut Targets, operation: tools::ProxyDrainOperation, operation_id: &str) -> String {
    let args = drain_args(true, operation_id);
    let requested = drain_request(operation, args.clone()).requested_digest().unwrap();
    let planned = run_drain(targets, operation, args).await.unwrap();
    plan_digest(&requested, &MutationToolResponse::ProxyDrain(planned)).unwrap()
}

/// The arguments that execute the drain plan `expected`.
fn drain_execution(expected: &str) -> tools::ProxyDrainArgs {
    let mut args = drain_args(false, DRAIN);
    args.expected_plan_digest = Some(expected.to_owned());
    args
}

#[tokio::test]
async fn plan_digest_is_stable_for_an_unchanged_state() {
    let mut targets = Targets::default();
    let plan = planned_drain(&mut targets, BEGIN, DRAIN).await;
    assert!(tools::is_plan_digest(&plan));

    // The Proxy serves between two plans: the work it holds moves, its state does not.
    targets.busy = 5;
    assert_eq!(planned_drain(&mut targets, BEGIN, DRAIN).await, plan);

    // Another operation asks for another drain of the same Proxy, and a cancel is not a begin.
    assert_ne!(planned_drain(&mut targets, BEGIN, OTHER_DRAIN).await, plan);
    assert_ne!(planned_drain(&mut targets, CANCEL, DRAIN).await, plan);

    // A drain that began in the meantime is another state, and so another plan ...
    targets.draining = Some(OTHER_DRAIN.to_owned());
    let held = planned_drain(&mut targets, BEGIN, DRAIN).await;
    assert_ne!(held, plan);
    // ... which stays that plan for as long as the drain goes on.
    assert_eq!(planned_drain(&mut targets, BEGIN, DRAIN).await, held);
    assert_eq!(targets.writes, 0);
}

#[tokio::test]
async fn execute_with_a_stale_plan_digest_is_a_conflict() {
    let mut targets = Targets::default();
    // The plan that was reviewed: a cancel changes nothing, because the Proxy does not drain.
    let reviewed = planned_drain(&mut targets, CANCEL, DRAIN).await;

    // The drain begins after the review. Executing the cancel would now end it.
    targets.draining = Some(DRAIN.to_owned());
    let reads = targets.reads;
    let refused = run_drain(&mut targets, CANCEL, drain_execution(&reviewed)).await;
    assert_eq!(refused.unwrap_err(), ControlError::precondition_conflict());
    // The Proxy was read once and not written: the drain that the plan did not show goes on.
    assert_eq!((targets.reads, targets.writes), (reads + 1, 0));
    assert_eq!(targets.draining.as_deref(), Some(DRAIN));

    // The same execution without the reviewed plan runs on what it reads, and ends the drain.
    let unbound = run_drain(&mut targets, CANCEL, drain_args(false, DRAIN)).await.unwrap();
    assert_eq!(unbound.status, tools::MutationStatus::Applied);
    assert_eq!(targets.writes, 1);
    assert_eq!(targets.draining, None);

    // The Proxy is where the plan saw it again, though busier, and the plan can be executed.
    targets.busy = 9;
    let executed = run_drain(&mut targets, CANCEL, drain_execution(&reviewed))
        .await
        .unwrap();
    assert_eq!(executed.status, tools::MutationStatus::Applied);
    assert!(!executed.targets[0].changed);
    assert_eq!(targets.writes, 1);

    // A plan that changes the Proxy is spent once it is executed: the state it was made for is gone.
    let begin = planned_drain(&mut targets, BEGIN, DRAIN).await;
    let begun = run_drain(&mut targets, BEGIN, drain_execution(&begin)).await.unwrap();
    assert_eq!(begun.status, tools::MutationStatus::Applied);
    assert_eq!(targets.writes, 2);
    let again = run_drain(&mut targets, BEGIN, drain_execution(&begin)).await;
    assert_eq!(again.unwrap_err(), ControlError::precondition_conflict());
    assert_eq!(targets.writes, 2);

    // A dry run compares nothing: it reports the plan of the state it reads.
    let mut dry_run = drain_args(true, DRAIN);
    dry_run.expected_plan_digest = Some(begin);
    let reported = run_drain(&mut targets, BEGIN, dry_run).await.unwrap();
    assert_eq!(reported.status, tools::MutationStatus::Planned);
}

#[tokio::test]
async fn a_log_filter_execution_compares_its_plan_before_it_writes() {
    let mut targets = Targets::default();
    let override_of = |operation: &str| {
        Some((
            operation.to_owned(),
            "rocketmq_broker::processor".to_owned(),
            AdminLogLevel::Debug,
        ))
    };

    // A set planned while no override is active.
    let plan = set_args(true, FILTER);
    let requested = MutationToolRequest::BrokerLogFilterSet(plan.clone())
        .requested_digest()
        .unwrap();
    let planned = run_set_log_filter(&mut targets, plan, &PlanBinding::unbound())
        .await
        .unwrap();
    let reviewed = plan_digest(&requested, &MutationToolResponse::BrokerLogFilter(planned)).unwrap();
    let mut set = set_args(false, FILTER);
    set.expected_plan_digest = Some(reviewed);
    let bound = PlanBinding::of(&MutationToolRequest::BrokerLogFilterSet(set.clone())).unwrap();

    // Another operation sets an override after the review.
    targets.filter = override_of(OTHER_FILTER);
    let refused = run_set_log_filter(&mut targets, set.clone(), &bound).await;
    assert_eq!(refused.unwrap_err(), ControlError::precondition_conflict());
    assert_eq!(targets.writes, 0);

    // Once that override is gone the plan is the current one again.
    targets.filter = None;
    let applied = run_set_log_filter(&mut targets, set, &bound).await.unwrap();
    assert_eq!(applied.status, tools::MutationStatus::Applied);
    assert_eq!(targets.writes, 1);

    // A restore planned while the override is active is stale once the Broker ended it.
    let plan = restore_args(true, FILTER);
    let requested = MutationToolRequest::BrokerLogFilterRestore(plan.clone())
        .requested_digest()
        .unwrap();
    let planned = run_restore_log_filter(&mut targets, plan, &PlanBinding::unbound())
        .await
        .unwrap();
    let reviewed = plan_digest(&requested, &MutationToolResponse::BrokerLogFilter(planned)).unwrap();
    let mut restore = restore_args(false, FILTER);
    restore.expected_plan_digest = Some(reviewed);
    let bound = PlanBinding::of(&MutationToolRequest::BrokerLogFilterRestore(restore.clone())).unwrap();
    targets.filter = None;
    let refused = run_restore_log_filter(&mut targets, restore.clone(), &bound).await;
    assert_eq!(refused.unwrap_err(), ControlError::precondition_conflict());
    targets.filter = override_of(FILTER);
    let restored = run_restore_log_filter(&mut targets, restore, &bound).await.unwrap();
    assert_eq!(restored.status, tools::MutationStatus::Applied);
    assert_eq!(targets.writes, 2);
    assert_eq!(targets.filter, None);
}
