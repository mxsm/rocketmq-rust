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

use rocketmq_admin_core::core::supervised_mutation as admin;

use super::{broker_patch_changes, dry_status, map_broker_patch, map_broker_state, status_from_failures};
use crate::error::ControlError;
use crate::tool_runtime::admin_session::SupervisedMutationBackend;
use crate::tool_runtime::{map_failure, map_persistence, map_verification};
use crate::tools;

pub(crate) async fn run_broker<B: SupervisedMutationBackend>(
    backend: &mut B,
    args: tools::PatchBrokerConfigArgs,
) -> Result<tools::BrokerConfigMutationToolResponse, ControlError> {
    let patch = args.properties.typed()?;
    let admin_patch = map_broker_patch(patch);
    let plan = backend.preflight_broker(&args.cluster, &args.broker_name).await?;
    let targets = B::broker_targets(&plan);
    let failures = B::broker_failures(&plan).to_vec();
    if args.dry_run {
        return Ok(broker_dry_run(&args, patch, targets, failures));
    }
    let before = targets;
    let outcome = backend.execute_broker(&plan, admin_patch).await?;
    Ok(broker_executed(&args, patch, before, outcome))
}

fn broker_dry_run(
    args: &tools::PatchBrokerConfigArgs,
    patch: tools::BrokerConfigPatch,
    targets: Vec<admin::BrokerMutationConfigTarget>,
    failures: Vec<admin::MutationTargetFailure>,
) -> tools::BrokerConfigMutationToolResponse {
    let before: BTreeMap<String, tools::BrokerConfigState> = targets
        .iter()
        .map(|target| (target.broker_name.clone(), map_broker_state(target.state)))
        .collect();
    let mut result_targets = targets
        .into_iter()
        .map(|target| tools::BrokerConfigMutationTarget {
            broker_name: target.broker_name,
            before: Some(map_broker_state(target.state)),
            requested: patch,
            after: None,
            applied: false,
            changed: broker_patch_changes(map_broker_state(target.state), patch),
            persistence: tools::PersistenceState::NotRequired,
            verification: tools::VerificationState::NotPerformed,
            failure: None,
            retryable: false,
        })
        .collect::<Vec<_>>();
    result_targets.extend(failures.iter().map(|failure| tools::BrokerConfigMutationTarget {
        broker_name: failure.broker_name.clone(),
        before: None,
        requested: patch,
        after: None,
        applied: false,
        changed: false,
        persistence: tools::PersistenceState::NotRequired,
        verification: tools::VerificationState::NotPerformed,
        failure: Some(map_failure(failure.code)),
        retryable: failure.retryable,
    }));
    result_targets.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    broker_response(
        args,
        patch,
        dry_status(before.len(), failures.len()),
        before,
        None,
        result_targets,
    )
}

fn broker_executed(
    args: &tools::PatchBrokerConfigArgs,
    patch: tools::BrokerConfigPatch,
    before_targets: Vec<admin::BrokerMutationConfigTarget>,
    outcome: admin::BrokerMutationConfigOutcome,
) -> tools::BrokerConfigMutationToolResponse {
    let before = before_targets
        .into_iter()
        .map(|target| (target.broker_name, map_broker_state(target.state)))
        .collect::<BTreeMap<_, _>>();
    let mut targets = outcome
        .targets
        .into_iter()
        .map(|target| tools::BrokerConfigMutationTarget {
            broker_name: target.broker_name,
            before: Some(map_broker_state(target.before)),
            requested: patch,
            after: target.after.map(map_broker_state),
            applied: target.applied,
            changed: target.changed,
            persistence: map_persistence(target.persistence),
            verification: map_verification(target.verification),
            failure: target.failure.map(map_failure),
            retryable: target.retryable,
        })
        .collect::<Vec<_>>();
    targets.extend(
        outcome
            .failures
            .iter()
            .map(|failure| tools::BrokerConfigMutationTarget {
                broker_name: failure.broker_name.clone(),
                before: None,
                requested: patch,
                after: None,
                applied: false,
                changed: false,
                persistence: tools::PersistenceState::NotRequired,
                verification: tools::VerificationState::NotPerformed,
                failure: Some(map_failure(failure.code)),
                retryable: failure.retryable,
            }),
    );
    targets.sort_by(|left, right| left.broker_name.cmp(&right.broker_name));
    let after = targets
        .iter()
        .filter_map(|target| target.after.map(|state| (target.broker_name.clone(), state)))
        .collect::<BTreeMap<_, _>>();
    let after = (!after.is_empty()).then_some(after);
    let status = status_from_failures(targets.iter().map(|target| target.failure));
    broker_response(args, patch, status, before, after, targets)
}

fn broker_response(
    args: &tools::PatchBrokerConfigArgs,
    patch: tools::BrokerConfigPatch,
    status: tools::MutationStatus,
    before: BTreeMap<String, tools::BrokerConfigState>,
    after: Option<BTreeMap<String, tools::BrokerConfigState>>,
    targets: Vec<tools::BrokerConfigMutationTarget>,
) -> tools::BrokerConfigMutationToolResponse {
    tools::BrokerConfigMutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::BrokerConfigPatchOperation::BrokerConfigPatch,
        cluster: args.cluster.clone(),
        mode: if args.dry_run {
            tools::MutationMode::DryRun
        } else {
            tools::MutationMode::Execute
        },
        status,
        target: tools::BrokerConfigResource {
            broker_name: args.broker_name.clone(),
        },
        before,
        requested: patch,
        after,
        targets,
        warnings: Vec::new(),
    }
}
