// Copyright 2023 The RocketMQ Rust Authors
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

use rocketmq_runtime::{
    OperationContext, OperationDrainStatus, OperationWaitPolicy, RuntimeContext, ShutdownDeadline, TaskKind,
};
use std::time::Duration;

#[tokio::test]
async fn graceful_completion_closes_admission_and_confirms_owner_settlement() {
    let context = RuntimeContext::try_from_current("operation-policy").unwrap();
    let service = context.service_context("service");
    let operation = OperationContext::without_deadline(TaskKind::Worker);
    service
        .task_group()
        .spawn_operation(&operation, "ready", async {})
        .unwrap();
    let deadline = ShutdownDeadline::after(Duration::from_secs(5));
    let outcome: OperationDrainStatus = operation
        .wait_with_policy(service.task_group(), OperationWaitPolicy::new(deadline, deadline))
        .await
        .unwrap();
    assert_eq!(outcome, OperationDrainStatus::Completed);
    assert_eq!(operation.active_task_count(), 0);
    assert!(service
        .task_group()
        .spawn_operation(&operation, "closed", async {})
        .is_err());
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}

#[tokio::test]
async fn abort_confirmation_is_distinct_from_graceful_completion() {
    let context = RuntimeContext::try_from_current("operation-confirm").unwrap();
    let service = context.service_context("service");
    let operation = OperationContext::without_deadline(TaskKind::Worker);
    service
        .task_group()
        .spawn_operation(&operation, "pending", std::future::pending())
        .unwrap();
    let policy = OperationWaitPolicy::new(
        ShutdownDeadline::after(Duration::ZERO),
        ShutdownDeadline::after(Duration::from_secs(5)),
    );
    assert_eq!(
        operation.wait_with_policy(service.task_group(), policy).await.unwrap(),
        OperationDrainStatus::AbortConfirmed
    );
    assert_eq!(operation.active_task_count(), 0);
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}

#[tokio::test]
async fn earlier_confirmation_deadline_tightens_grace_and_reports_unconfirmed_work() {
    let context = RuntimeContext::try_from_current("operation-unconfirmed").unwrap();
    let service = context.service_context("service");
    let operation = OperationContext::without_deadline(TaskKind::Worker);
    service
        .task_group()
        .spawn_operation(&operation, "pending", std::future::pending())
        .unwrap();
    // On this current-thread executor no other task can destroy the future
    // while the zero-budget wait requests its abort without yielding.
    let policy = OperationWaitPolicy::new(
        ShutdownDeadline::after(Duration::from_secs(60)),
        ShutdownDeadline::after(Duration::ZERO),
    );
    assert_eq!(
        operation.wait_with_policy(service.task_group(), policy).await.unwrap(),
        OperationDrainStatus::Unconfirmed { remaining_tasks: 1 }
    );
    assert_eq!(operation.active_task_count(), 1);
    assert!(operation
        .wait(service.task_group(), Duration::from_secs(5))
        .await
        .unwrap());
    assert!(context.shutdown_tasks(Duration::from_secs(5)).await.is_healthy());
}
