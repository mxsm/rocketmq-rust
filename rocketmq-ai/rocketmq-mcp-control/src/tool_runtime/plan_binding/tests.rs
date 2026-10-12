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

use serde_json::json;

use super::*;
use crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION;
use crate::tools;

type TopicStates = BTreeMap<String, tools::VisibleState<tools::TopicReplacement>>;

fn topic_args(topic: &str) -> tools::UpsertTopicArgs {
    serde_json::from_value(json!({
        "schema_version": MUTATION_ARGUMENTS_SCHEMA_VERSION,
        "cluster": "cluster-a",
        "topic": topic,
        "broker_names": ["broker-a", "broker-b"],
        "read_queue_nums": 8,
        "write_queue_nums": 8,
        "perm": 6,
        "order": false,
        "message_type": "NORMAL"
    }))
    .unwrap()
}

/// Two Brokers without the Topic.
fn absent() -> TopicStates {
    ["broker-a", "broker-b"]
        .into_iter()
        .map(|broker| (broker.to_owned(), tools::VisibleState::Absent))
        .collect()
}

/// What a dry run of `args` reports after reading `before`.
fn planned(args: &tools::UpsertTopicArgs, before: TopicStates) -> tools::TopicMutationToolResponse {
    crate::tool_runtime::topic_dry_run(args, before, &[])
}

fn digest(requested: &tools::UpsertTopicArgs, planned: tools::TopicMutationToolResponse) -> String {
    let requested = MutationToolRequest::Topic(requested.clone())
        .requested_digest()
        .unwrap();
    plan_digest(&requested, &MutationToolResponse::Topic(planned)).unwrap()
}

fn drain_state(phase: tools::ProxyDrainPhase, sessions: u64) -> tools::ProxyDrainState {
    let accepting = phase == tools::ProxyDrainPhase::Accepting;
    tools::ProxyDrainState {
        phase,
        held_by: if accepting {
            tools::OperationHolder::None
        } else {
            tools::OperationHolder::ThisOperation
        },
        admission_open: accepting,
        routing_open: accepting,
        readiness_published: accepting,
        zero_pending: sessions == 0,
        pending: tools::ProxyDrainPending {
            active_connections: sessions,
            sessions,
            receipt_handles: 0,
            prepared_transactions: 0,
            telemetry_links: 0,
            remoting_channels: 0,
            telemetry_commands: 0,
            rpc_in_flight: sessions * 2,
        },
    }
}

/// What a dry run of a drain reports for the Proxy `proxy_name` in `state`.
fn planned_drain(proxy_name: &str, state: tools::ProxyDrainState) -> MutationToolResponse {
    let requested = tools::ProxyDrainRequested {
        action: tools::ProxyDrainAction::Begin,
    };
    MutationToolResponse::ProxyDrain(tools::ProxyDrainToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::ProxyDrainOperation::ProxyDrainBegin,
        cluster: "cluster-a".to_owned(),
        mode: tools::MutationMode::DryRun,
        status: tools::MutationStatus::Planned,
        error_code: None,
        target: tools::ProxyDrainResource {
            proxy_name: proxy_name.to_owned(),
        },
        before: BTreeMap::from([(proxy_name.to_owned(), state)]),
        requested,
        after: None,
        targets: vec![tools::ProxyDrainTarget {
            proxy_name: proxy_name.to_owned(),
            before: Some(state),
            requested,
            after: None,
            applied: false,
            changed: true,
            verification: tools::VerificationState::NotPerformed,
            failure: None,
            retryable: false,
        }],
        warnings: Vec::new(),
    })
}

#[test]
fn a_plan_is_what_was_asked_and_read_and_not_how_the_call_went() {
    let args = topic_args("orders");
    let plan = digest(&args, planned(&args, absent()));
    assert!(tools::is_plan_digest(&plan));

    // The members that say how a call went are not part of the plan.
    let mut reported = planned(&args, absent());
    reported.mode = tools::MutationMode::Execute;
    reported.status = tools::MutationStatus::Failed;
    reported.error_code = Some(crate::error::ControlErrorCode::ExecutionFailed);
    reported.warnings.push("a warning of this call".to_owned());
    assert_eq!(digest(&args, reported), plan);

    // The arguments that say how a call is made are not part of what it asks for.
    let mut restated = args.clone();
    restated.dry_run = false;
    restated.confirm = true;
    restated.reason = Some("approved topic rollout".to_owned());
    restated.request_key = Some("topic-request-0001".to_owned());
    restated.expected_plan_digest = Some(plan.clone());
    restated.broker_names.reverse();
    assert_eq!(digest(&restated, planned(&restated, absent())), plan);

    // The state that was read is, for every target.
    let mut found = absent();
    found.insert(
        "broker-b".to_owned(),
        tools::VisibleState::Present {
            version: 3,
            value: args.replacement.clone(),
        },
    );
    let present = digest(&args, planned(&args, found.clone()));
    assert_ne!(present, plan);
    let Some(tools::VisibleState::Present { version, .. }) = found.get_mut("broker-b") else {
        unreachable!();
    };
    *version = 4;
    assert_ne!(digest(&args, planned(&args, found)), present);
    let mut unread = absent();
    unread.insert("broker-b".to_owned(), tools::VisibleState::Unknown);
    assert_ne!(digest(&args, planned(&args, unread)), plan);

    // So is what was asked, and of which object.
    let mut wider = args.clone();
    wider.replacement.write_queue_nums = 16;
    assert_ne!(digest(&wider, planned(&wider, absent())), plan);
    let payments = topic_args("payments");
    assert_ne!(digest(&payments, planned(&payments, absent())), plan);
    let mut elsewhere = args.clone();
    elsewhere.cluster = "cluster-b".to_owned();
    assert_ne!(digest(&elsewhere, planned(&elsewhere, absent())), plan);

    // A plan belongs to its request: the same response is another plan for another request.
    let requested = MutationToolRequest::Topic(payments).requested_digest().unwrap();
    let response = MutationToolResponse::Topic(planned(&args, absent()));
    assert_ne!(plan_digest(&requested, &response).unwrap(), plan);
}

#[test]
fn the_work_a_proxy_holds_is_not_part_of_its_plan() {
    let requested = "a".repeat(64);
    let accepting = |sessions| planned_drain("proxy-a", drain_state(tools::ProxyDrainPhase::Accepting, sessions));
    let plan = plan_digest(&requested, &accepting(0)).unwrap();

    // A Proxy that serves holds more or less work from one read to the next.
    assert_eq!(plan_digest(&requested, &accepting(12)).unwrap(), plan);

    // Its phase, its holder and its gates are the state that a drain changes.
    let draining = planned_drain("proxy-a", drain_state(tools::ProxyDrainPhase::Draining, 12));
    assert_ne!(plan_digest(&requested, &draining).unwrap(), plan);
    let mut half_open = drain_state(tools::ProxyDrainPhase::Accepting, 0);
    half_open.routing_open = false;
    assert_ne!(
        plan_digest(&requested, &planned_drain("proxy-a", half_open)).unwrap(),
        plan
    );

    // Only members of a state are left out: an alias with one of their names stays a target.
    let state = serde_json::to_value(drain_state(tools::ProxyDrainPhase::Accepting, 0)).unwrap();
    for member in PROXY_WORK_MEMBERS {
        assert!(state.get(member).is_some(), "a Proxy state has no member {member}");
        let idle = planned_drain(member, drain_state(tools::ProxyDrainPhase::Accepting, 0));
        let drained = planned_drain(member, drain_state(tools::ProxyDrainPhase::Drained, 0));
        assert_ne!(
            plan_digest(&requested, &idle).unwrap(),
            plan_digest(&requested, &drained).unwrap()
        );
        assert_ne!(plan_digest(&requested, &idle).unwrap(), plan);
    }
}

#[test]
fn only_an_execution_that_names_a_plan_is_held_to_it() {
    let args = topic_args("orders");
    let plan = digest(&args, planned(&args, absent()));
    let current = || MutationToolResponse::Topic(planned(&args, absent()));
    let never_read = || -> MutationToolResponse { panic!("no plan was expected") };

    // An execution without a plan runs on what it reads, and so does a dry run with one.
    let mut unbound = args.clone();
    unbound.dry_run = false;
    assert_eq!(
        PlanBinding::of(&MutationToolRequest::Topic(unbound))
            .unwrap()
            .admit(never_read),
        Ok(())
    );
    let mut reporting = args.clone();
    reporting.expected_plan_digest = Some("0".repeat(64));
    assert_eq!(
        PlanBinding::of(&MutationToolRequest::Topic(reporting))
            .unwrap()
            .admit(never_read),
        Ok(())
    );
    assert_eq!(PlanBinding::unbound().admit(never_read), Ok(()));

    // An execution that names the plan read now may write. Any other plan is a conflict.
    let mut bound = args.clone();
    bound.dry_run = false;
    bound.expected_plan_digest = Some(plan);
    let binding = PlanBinding::of(&MutationToolRequest::Topic(bound.clone())).unwrap();
    assert_eq!(binding.admit(current), Ok(()));
    let mut created = absent();
    created.insert(
        "broker-a".to_owned(),
        tools::VisibleState::Present {
            version: 1,
            value: args.replacement.clone(),
        },
    );
    assert_eq!(
        binding.admit(|| MutationToolResponse::Topic(planned(&args, created))),
        Err(ControlError::precondition_conflict())
    );
    bound.expected_plan_digest = Some("0".repeat(64));
    assert_eq!(
        PlanBinding::of(&MutationToolRequest::Topic(bound))
            .unwrap()
            .admit(current),
        Err(ControlError::precondition_conflict())
    );
}
