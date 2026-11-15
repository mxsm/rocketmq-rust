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

use super::*;
use rocketmq_runtime::{OperationContext, RuntimeContext, ShutdownDeadline, TaskKind};

#[tokio::test(start_paused = true)]
async fn late_cancel_does_not_reset_original_mutation_deadline() {
    let runtime = RuntimeContext::from_current("d20-mutation-deadline");
    let owner = runtime.service_context("operation").task_group().clone();
    let lifecycle = ExecutionLifecycle {
        owner: owner.clone(),
        shutdown_timeout: Duration::from_secs(20),
        mutation_timeout: Duration::from_secs(10),
    };
    let (entered, started) = tokio::sync::oneshot::channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    owner
        .spawn_draining_operation(
            &OperationContext::without_deadline(TaskKind::Worker),
            "original-mutation",
            async move {
                let result: Result<(), ExecutorRequestFailure> = drain_mutation(&lifecycle, async move {
                    entered.send(()).unwrap();
                    std::future::pending().await
                })
                .await;
                finished.send(result.is_err()).unwrap();
            },
        )
        .unwrap();
    let origin = tokio::time::Instant::now();
    started.await.unwrap();
    tokio::time::advance(Duration::from_secs(9)).await;
    drop(owner.shutdown_until(ShutdownDeadline::after(Duration::from_secs(20))));
    owner.cancel();
    assert!(done.await.unwrap());
    assert_eq!(origin.elapsed(), Duration::from_secs(10));
    assert!(owner.shutdown(Duration::from_secs(1)).await.is_healthy());
}

#[tokio::test(start_paused = true)]
async fn shorter_owner_budget_reserves_settlement_without_claiming_remote_cancel() {
    let runtime = RuntimeContext::from_current("d20-mutation-settlement");
    let owner = runtime.service_context("operation").task_group().clone();
    let lifecycle = ExecutionLifecycle {
        owner: owner.clone(),
        shutdown_timeout: Duration::from_secs(3),
        mutation_timeout: Duration::from_secs(30),
    };
    let (entered, started) = tokio::sync::oneshot::channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    owner
        .spawn_draining_operation(
            &OperationContext::without_deadline(TaskKind::Worker),
            "bounded-local-wait",
            async move {
                let result: Result<(), ExecutorRequestFailure> = drain_mutation(&lifecycle, async move {
                    entered.send(()).unwrap();
                    std::future::pending().await
                })
                .await;
                // Model an entered durable append: it drains rather than being raced
                // against observation cancellation. Real PG coverage checks this path.
                tokio::time::sleep(Duration::from_millis(10)).await;
                finished.send(result.is_err()).unwrap();
            },
        )
        .unwrap();
    started.await.unwrap();
    let origin = tokio::time::Instant::now();
    drop(owner.shutdown_until(ShutdownDeadline::after(Duration::from_secs(3))));
    owner.cancel();
    assert!(done.await.unwrap());
    assert!(origin.elapsed() < Duration::from_secs(3));
    assert!(owner.shutdown(Duration::from_secs(1)).await.is_healthy());
}
