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

use crate::{ChangeExecutor, ExecutorRequestFailure};
use rocketmq_runtime::task_group::TaskGroup;
use std::{future::Future, time::Duration};

#[derive(Clone)]
pub(super) struct ExecutionLifecycle {
    owner: TaskGroup,
    shutdown_timeout: Duration,
    mutation_timeout: Duration,
}

impl ChangeExecutor {
    pub(super) fn record_shutdown_storage_failure<T>(&self, result: &Result<T, ExecutorRequestFailure>) {
        if self.observation_cancelled()
            && matches!(result,Err(ExecutorRequestFailure::Operational(error)) if error.is_persistence_failure())
        {
            self.metrics
                .shutdown_storage_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                durable_settlement_confirmed = false,
                "Executor storage settlement failed during shutdown"
            );
        }
    }

    pub(crate) fn settlement_healthy(&self) -> bool {
        self.metrics
            .shutdown_storage_failures
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
    }
    /// Owns admitted operations independently of the HTTP response waiter.
    /// Cancellation stops reads/new dispatch, but an entered journal append is
    /// drained under the same owner deadline. No remote cancellation is implied.
    #[must_use]
    pub fn with_lifecycle(mut self, owner: TaskGroup, shutdown_timeout: Duration, mutation_timeout: Duration) -> Self {
        self.verifier = self.verifier.take().map(|verifier| verifier.with_owner(owner.clone()));
        self.lifecycle = Some(ExecutionLifecycle {
            owner,
            shutdown_timeout,
            mutation_timeout,
        });
        self
    }

    pub(crate) fn operation_owner(&self) -> Option<&TaskGroup> {
        self.lifecycle.as_ref().map(|lifecycle| &lifecycle.owner)
    }

    pub(crate) fn observation_cancelled(&self) -> bool {
        self.operation_owner()
            .is_some_and(|owner| owner.cancellation_token().is_cancelled())
    }

    pub(crate) async fn read_owned<T>(
        &self,
        read: impl Future<Output = Result<T, ExecutorRequestFailure>>,
    ) -> Result<T, ExecutorRequestFailure> {
        match self.operation_owner() {
            Some(owner) => tokio::select! { biased;
                ()=owner.cancellation_token().cancelled_owned()=>Err(ExecutorRequestFailure::ObservationUnknown),
                result=read=>result,
            },
            None => read.await,
        }
    }

    /// Local stop waiting is Unknown. The original intent/lease/fence remain
    /// authoritative for later reconciliation; neither branch retries a write.
    pub(crate) async fn drain_mutation<T>(
        &self,
        mutation: impl Future<Output = Result<T, ExecutorRequestFailure>>,
    ) -> Result<T, ExecutorRequestFailure> {
        match &self.lifecycle {
            Some(lifecycle) => drain_mutation(lifecycle, mutation).await,
            None => mutation.await,
        }
    }
}

async fn drain_mutation<T>(
    lifecycle: &ExecutionLifecycle,
    mutation: impl Future<Output = Result<T, ExecutorRequestFailure>>,
) -> Result<T, ExecutorRequestFailure> {
    if lifecycle.owner.cancellation_token().is_cancelled() {
        return Err(ExecutorRequestFailure::ObservationUnknown);
    }
    let original_deadline = tokio::time::Instant::now()
        .checked_add(lifecycle.mutation_timeout)
        .ok_or(ExecutorRequestFailure::ObservationUnknown)?;
    tokio::pin!(mutation);
    tokio::select! { biased;
        result=tokio::time::timeout_at(original_deadline,&mut mutation)=>return result.unwrap_or(Err(ExecutorRequestFailure::ObservationUnknown)),
        ()=lifecycle.owner.cancellation_token().cancelled_owned()=>{},
    };
    // Reserve time for the durable Unknown result/state append. If storage
    // outlives this reserve the owner reports unhealthy and keeps the intent;
    // a timeout is never converted into a successful compensation/result.
    let remaining = lifecycle
        .owner
        .shutdown_deadline()
        .map_or(lifecycle.shutdown_timeout, |deadline| deadline.remaining());
    let reserve = (lifecycle.shutdown_timeout / 3).min(Duration::from_secs(2));
    let wait = remaining.saturating_sub(reserve).min(lifecycle.mutation_timeout);
    let deadline = (tokio::time::Instant::now() + wait).min(original_deadline);
    tokio::time::timeout_at(deadline, mutation)
        .await
        .unwrap_or(Err(ExecutorRequestFailure::ObservationUnknown))
}

#[cfg(test)]
#[path = "execution_lifecycle/tests.rs"]
mod tests;
