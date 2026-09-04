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

use std::collections::{BTreeMap, BTreeSet};

use rocketmq_admin_core::core::supervised_mutation as admin;

use super::{dry_status, map_request_mode, map_request_mode_from_admin, status_from_failures};
use crate::error::ControlError;
use crate::tool_runtime::admin_session::SupervisedMutationBackend;
use crate::tool_runtime::{map_failure, map_persistence, map_verification};
use crate::tools;

pub(crate) async fn run_request_mode<B: SupervisedMutationBackend>(
    backend: &mut B,
    args: tools::SetConsumerRequestModeArgs,
) -> Result<tools::RequestModeMutationToolResponse, ControlError> {
    let replacement = tools::RequestModeValue {
        mode: args.mode,
        pop_share_queue_num: args.pop_share_queue_num,
    };
    let request = admin::RequestModePreflightRequest {
        cluster: args.cluster.clone(),
        topic: args.topic.clone(),
        consumer_group: args.consumer_group.clone(),
        replacement: map_request_mode(replacement),
    };
    let plan = backend.preflight_request_mode(&request).await?;
    let targets = B::request_mode_targets(&plan);
    let failures = B::request_mode_failures(&plan).to_vec();
    if args.dry_run {
        return Ok(request_mode_dry_run(&args, replacement, targets, failures));
    }
    let before = targets;
    let outcome = backend.execute_request_mode(&plan, args.timeout_millis).await?;
    Ok(request_mode_executed(&args, replacement, before, outcome))
}

fn request_mode_dry_run(
    args: &tools::SetConsumerRequestModeArgs,
    requested: tools::RequestModeValue,
    targets: Vec<(String, Option<admin::RequestModeValue>)>,
    failures: Vec<admin::MutationTargetFailure>,
) -> tools::RequestModeMutationToolResponse {
    let before: BTreeMap<String, Option<tools::RequestModeValue>> = targets
        .iter()
        .map(|(broker, value)| (broker.clone(), value.map(map_request_mode_from_admin)))
        .collect();
    let mut result_targets = targets
        .into_iter()
        .map(|(broker_name, current)| {
            let current = current.map(map_request_mode_from_admin);
            tools::RequestModeMutationTarget {
                broker_name,
                before: current,
                requested,
                after: None,
                applied: false,
                changed: current != Some(requested),
                persistence: tools::PersistenceState::NotRequired,
                verification: tools::VerificationState::NotPerformed,
                failure: None,
                retryable: false,
            }
        })
        .collect::<Vec<_>>();
    result_targets.extend(failures.iter().map(|failure| tools::RequestModeMutationTarget {
        broker_name: failure.broker_name.clone(),
        before: None,
        requested,
        after: None,
        applied: false,
        changed: false,
        persistence: tools::PersistenceState::NotRequired,
        verification: tools::VerificationState::NotPerformed,
        failure: Some(map_failure(failure.code)),
        retryable: failure.retryable,
    }));
    result_targets.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    request_mode_response(
        args,
        requested,
        dry_status(before.len(), failures.len()),
        before,
        None,
        result_targets,
    )
}

fn request_mode_executed(
    args: &tools::SetConsumerRequestModeArgs,
    requested: tools::RequestModeValue,
    before_targets: Vec<(String, Option<admin::RequestModeValue>)>,
    outcome: admin::RequestModeMutationOutcome,
) -> tools::RequestModeMutationToolResponse {
    let before = before_targets
        .into_iter()
        .map(|(broker, current)| (broker, current.map(map_request_mode_from_admin)))
        .collect::<BTreeMap<_, _>>();
    let mut targets = outcome
        .targets
        .into_iter()
        .map(|target| tools::RequestModeMutationTarget {
            broker_name: target.broker_name,
            before: target.expected.map(map_request_mode_from_admin),
            requested,
            after: target.current.map(map_request_mode_from_admin),
            applied: target.applied,
            changed: target.changed,
            persistence: map_persistence(target.persistence),
            verification: map_verification(target.verification),
            failure: target.failure.map(map_failure),
            retryable: target.retryable,
        })
        .collect::<Vec<_>>();
    targets.extend(outcome.failures.iter().map(|failure| tools::RequestModeMutationTarget {
        broker_name: failure.broker_name.clone(),
        before: None,
        requested,
        after: None,
        applied: false,
        changed: false,
        persistence: tools::PersistenceState::NotRequired,
        verification: tools::VerificationState::NotPerformed,
        failure: Some(map_failure(failure.code)),
        retryable: failure.retryable,
    }));
    targets.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    let after = targets.iter().any(|target| target.after.is_some()).then(|| {
        targets
            .iter()
            .map(|target| (target.broker_name.clone(), target.after))
            .collect()
    });
    let status = status_from_failures(targets.iter().map(|target| target.failure));
    request_mode_response(args, requested, status, before, after, targets)
}

fn request_mode_response(
    args: &tools::SetConsumerRequestModeArgs,
    requested: tools::RequestModeValue,
    status: tools::MutationStatus,
    before: BTreeMap<String, Option<tools::RequestModeValue>>,
    after: Option<BTreeMap<String, Option<tools::RequestModeValue>>>,
    targets: Vec<tools::RequestModeMutationTarget>,
) -> tools::RequestModeMutationToolResponse {
    let brokers = targets
        .iter()
        .map(|target| target.broker_name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    tools::RequestModeMutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::ConsumerRequestModeOperation::ConsumerRequestMode,
        cluster: args.cluster.clone(),
        mode: if args.dry_run {
            tools::MutationMode::DryRun
        } else {
            tools::MutationMode::Execute
        },
        status,
        error_code: tools::response_error_code(status, targets.iter().map(|target| target.failure)),
        target: tools::RequestModeResource {
            topic: args.topic.clone(),
            consumer_group: args.consumer_group.clone(),
            brokers,
        },
        before,
        requested: tools::RequestModeRequested {
            mode: requested.mode,
            pop_share_queue_num: requested.pop_share_queue_num,
            timeout_millis: args.timeout_millis,
        },
        after,
        targets,
        warnings: Vec::new(),
    }
}
