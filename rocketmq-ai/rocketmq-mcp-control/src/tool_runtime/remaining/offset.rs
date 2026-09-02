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

use super::dry_status;
use super::status_from_failures;
use crate::error::ControlError;
use crate::tool_runtime::admin_session::SupervisedMutationBackend;
use crate::tool_runtime::map_failure;
use crate::tools;

pub(crate) async fn run_offset<B: SupervisedMutationBackend>(
    backend: &mut B,
    args: tools::ResetConsumerOffsetArgs,
) -> Result<tools::OffsetMutationToolResponse, ControlError> {
    let timestamp_millis = args.validate(true, false)?;
    let request = admin::OffsetResetPreviewRequest {
        cluster: args.cluster.clone(),
        topic: args.topic.clone(),
        consumer_group: args.consumer_group.clone(),
        timestamp: timestamp_millis,
        force: args.force,
    };
    let plan = backend.preview_offset(&request).await?;
    let rows = B::offset_rows(&plan);
    let failures = B::offset_failures(&plan).to_vec();
    if args.dry_run {
        return Ok(offset_dry_run(&args, timestamp_millis, rows, failures));
    }
    let before = rows.clone();
    let outcome = backend.execute_offset(&plan).await?;
    Ok(offset_executed(&args, timestamp_millis, before, outcome))
}

fn offset_dry_run(
    args: &tools::ResetConsumerOffsetArgs,
    timestamp_millis: i64,
    rows: Vec<admin::OffsetResetPreviewRow>,
    failures: Vec<admin::MutationTargetFailure>,
) -> tools::OffsetMutationToolResponse {
    let before = rows.iter().map(offset_before).collect::<Vec<_>>();
    let mut targets = rows
        .iter()
        .map(|row| tools::OffsetMutationTarget {
            broker_name: row.broker_name.clone(),
            queue_id: Some(row.queue_id),
            before: Some(row.current_offset),
            planned: Some(row.planned_offset),
            delta: Some(row.delta),
            after: None,
            applied: false,
            changed: row.changed,
            failure: None,
            retryable: false,
        })
        .collect::<Vec<_>>();
    targets.extend(failures.iter().map(offset_failure_target));
    sort_offset_targets(&mut targets);
    let status = dry_status(rows.len(), failures.len());
    offset_response(args, timestamp_millis, status, before, None, targets)
}

fn offset_executed(
    args: &tools::ResetConsumerOffsetArgs,
    timestamp_millis: i64,
    before_rows: Vec<admin::OffsetResetPreviewRow>,
    outcome: admin::OffsetResetOutcome,
) -> tools::OffsetMutationToolResponse {
    let before = before_rows.iter().map(offset_before).collect::<Vec<_>>();
    let deltas = before_rows
        .iter()
        .map(|row| ((row.broker_name.clone(), row.queue_id), row.delta))
        .collect::<BTreeMap<_, _>>();
    let mut targets = outcome
        .targets
        .into_iter()
        .map(|target| tools::OffsetMutationTarget {
            broker_name: target.broker_name.clone(),
            queue_id: Some(target.queue_id),
            before: Some(target.expected_offset),
            planned: Some(target.planned_offset),
            delta: deltas.get(&(target.broker_name.clone(), target.queue_id)).copied(),
            after: target.observed_offset,
            applied: target.applied,
            changed: target.changed,
            failure: target.failure.map(map_failure),
            retryable: target.retryable,
        })
        .collect::<Vec<_>>();
    targets.extend(outcome.failures.iter().map(offset_failure_target));
    sort_offset_targets(&mut targets);
    let after = targets
        .iter()
        .filter_map(|target| {
            Some(tools::OffsetQueueState {
                broker_name: target.broker_name.clone(),
                queue_id: target.queue_id?,
                offset: target.after?,
            })
        })
        .collect::<Vec<_>>();
    let after = (!after.is_empty()).then_some(after);
    let status = status_from_failures(targets.iter().map(|target| target.failure));
    offset_response(args, timestamp_millis, status, before, after, targets)
}

fn offset_response(
    args: &tools::ResetConsumerOffsetArgs,
    timestamp_millis: i64,
    status: tools::MutationStatus,
    before: Vec<tools::OffsetQueueState>,
    after: Option<Vec<tools::OffsetQueueState>>,
    targets: Vec<tools::OffsetMutationTarget>,
) -> tools::OffsetMutationToolResponse {
    let brokers = targets
        .iter()
        .map(|target| target.broker_name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    tools::OffsetMutationToolResponse {
        schema_version: tools::MutationResultSchemaVersion::V1,
        operation: tools::ConsumerOffsetResetOperation::ConsumerOffsetReset,
        cluster: args.cluster.clone(),
        mode: if args.dry_run {
            tools::MutationMode::DryRun
        } else {
            tools::MutationMode::Execute
        },
        status,
        target: tools::OffsetResetResource {
            topic: args.topic.clone(),
            consumer_group: args.consumer_group.clone(),
            brokers,
        },
        before,
        requested: tools::OffsetRequested {
            timestamp: args.timestamp.clone(),
            timestamp_millis,
            force: args.force,
        },
        after,
        targets,
        warnings: Vec::new(),
    }
}

fn sort_offset_targets(targets: &mut [tools::OffsetMutationTarget]) {
    targets.sort_by(|left, right| (&left.broker_name, left.queue_id).cmp(&(&right.broker_name, right.queue_id)));
}

fn offset_before(row: &admin::OffsetResetPreviewRow) -> tools::OffsetQueueState {
    tools::OffsetQueueState {
        broker_name: row.broker_name.clone(),
        queue_id: row.queue_id,
        offset: row.current_offset,
    }
}

fn offset_failure_target(failure: &admin::MutationTargetFailure) -> tools::OffsetMutationTarget {
    tools::OffsetMutationTarget {
        broker_name: failure.broker_name.clone(),
        queue_id: failure.queue_id,
        before: None,
        planned: None,
        delta: None,
        after: None,
        applied: false,
        changed: false,
        failure: Some(map_failure(failure.code)),
        retryable: failure.retryable,
    }
}
