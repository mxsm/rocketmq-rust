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

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashSet;
use futures::future::join_all;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use tokio_util::sync::CancellationToken;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;

/// Cancellation, deadline, and task-class metadata for one bounded operation.
///
/// An operation is not a lifecycle owner and does not create a child
/// [`TaskGroup`]. Tasks spawned for the operation remain registered with a
/// fixed component owner while this context supplies operation-local
/// cancellation and bounded join semantics.
#[derive(Debug, Clone)]
pub struct OperationContext {
    inner: Arc<OperationContextInner>,
    task_kind: TaskKind,
}

#[derive(Debug)]
struct OperationContextInner {
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    accepting: AtomicBool,
    spawn_gate: Mutex<()>,
    owner_id: Mutex<Option<TaskGroupId>>,
    active_tasks: Arc<DashSet<TaskId>>,
}

impl OperationContext {
    /// Creates an operation with an absolute deadline.
    pub fn new(deadline: Instant, task_kind: TaskKind) -> Self {
        Self::from_parts(Some(deadline), task_kind)
    }

    /// Creates an operation that remains active until explicitly cancelled.
    pub fn without_deadline(task_kind: TaskKind) -> Self {
        Self::from_parts(None, task_kind)
    }

    fn from_parts(deadline: Option<Instant>, task_kind: TaskKind) -> Self {
        Self {
            inner: Arc::new(OperationContextInner {
                cancellation: CancellationToken::new(),
                deadline,
                accepting: AtomicBool::new(true),
                spawn_gate: Mutex::new(()),
                owner_id: Mutex::new(None),
                active_tasks: Arc::new(DashSet::new()),
            }),
            task_kind,
        }
    }

    /// Returns the operation-local cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    /// Returns the absolute deadline, when one was configured.
    pub fn deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    /// Returns the task classification applied to operation tasks.
    pub fn task_kind(&self) -> TaskKind {
        self.task_kind
    }

    /// Returns a view of this operation with a different task classification.
    ///
    /// Cancellation, deadline, owner binding, and active-task tracking remain
    /// shared with the original context.
    pub fn with_task_kind(&self, task_kind: TaskKind) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            task_kind,
        }
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancellation.is_cancelled()
    }

    /// Requests cancellation for every task in this operation.
    pub fn cancel(&self) {
        let _spawn_guard = self.inner.spawn_gate.lock();
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.cancellation.cancel();
    }

    /// Closes task admission without cancelling tasks that were already
    /// accepted.
    pub fn close_admission(&self) {
        let _spawn_guard = self.inner.spawn_gate.lock();
        self.inner.accepting.store(false, Ordering::Release);
    }

    /// Returns the number of operation tasks that have not completed.
    pub fn active_task_count(&self) -> usize {
        self.inner.active_tasks.len()
    }

    /// Cancels this operation and waits for all tasks registered with `owner`.
    ///
    /// Returns `true` when every task completed before the shared timeout.
    /// Tasks still running at the deadline are aborted and awaited without
    /// extending the timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when `owner` differs from the component owner used to
    /// spawn the operation.
    pub async fn cancel_and_wait(&self, owner: &TaskGroup, timeout: Duration) -> RuntimeResult<bool> {
        self.cancel();
        self.wait(owner, timeout).await
    }

    /// Waits for all currently registered operation tasks without requesting
    /// cancellation. Tasks still running at the shared deadline are aborted.
    ///
    /// # Errors
    ///
    /// Returns an error when `owner` differs from the component owner used to
    /// spawn the operation.
    pub async fn wait(&self, owner: &TaskGroup, timeout: Duration) -> RuntimeResult<bool> {
        self.ensure_owner(owner.id())?;
        let deadline = Instant::now() + timeout;
        let task_ids = self
            .inner
            .active_tasks
            .iter()
            .map(|task_id| *task_id)
            .collect::<Vec<_>>();

        let completed = join_all(task_ids.iter().copied().map(|task_id| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            owner.wait_task(task_id, remaining)
        }))
        .await;
        let timed_out = task_ids
            .into_iter()
            .zip(completed)
            .filter_map(|(task_id, completed)| (!completed && owner.contains_task(task_id)).then_some(task_id))
            .collect::<Vec<_>>();

        join_all(timed_out.iter().copied().map(|task_id| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            owner.abort_task_and_wait(task_id, remaining)
        }))
        .await;

        Ok(timed_out.is_empty() && self.active_task_count() == 0)
    }

    pub(crate) fn prepare_spawn(&self, owner_id: TaskGroupId) -> RuntimeResult<OperationTaskRegistration> {
        self.bind_owner(owner_id)?;
        if !self.inner.accepting.load(Ordering::Acquire)
            || self.is_cancelled()
            || self.inner.deadline.is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::SpawnOperation,
            ));
        }
        Ok(OperationTaskRegistration::new(Arc::clone(&self.inner.active_tasks)))
    }

    pub(crate) fn spawn_guard(&self) -> MutexGuard<'_, ()> {
        self.inner.spawn_gate.lock()
    }

    pub(crate) async fn run<F>(&self, future: F)
    where
        F: Future<Output = ()>,
    {
        match self.inner.deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = self.inner.cancellation.cancelled() => {}
                    _ = tokio::time::sleep_until(deadline.into()) => {}
                    _ = future => {}
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = self.inner.cancellation.cancelled() => {}
                    _ = future => {}
                }
            }
        }
    }

    fn bind_owner(&self, owner_id: TaskGroupId) -> RuntimeResult<()> {
        let mut bound_owner = self.inner.owner_id.lock();
        match *bound_owner {
            Some(bound_owner) if bound_owner != owner_id => {
                Err(RuntimeError::internal_failure(crate::RuntimeOperation::OperationOwner))
            }
            Some(_) => Ok(()),
            None => {
                *bound_owner = Some(owner_id);
                Ok(())
            }
        }
    }

    fn ensure_owner(&self, owner_id: TaskGroupId) -> RuntimeResult<()> {
        match *self.inner.owner_id.lock() {
            Some(bound_owner) if bound_owner != owner_id => {
                Err(RuntimeError::internal_failure(crate::RuntimeOperation::OperationOwner))
            }
            _ => Ok(()),
        }
    }
}

pub(crate) struct OperationTaskRegistration {
    state: Arc<OperationTaskRegistrationState>,
}

struct OperationTaskRegistrationState {
    task_id: AtomicU64,
    completed: AtomicBool,
    active_tasks: Arc<DashSet<TaskId>>,
}

impl OperationTaskRegistration {
    fn new(active_tasks: Arc<DashSet<TaskId>>) -> Self {
        Self {
            state: Arc::new(OperationTaskRegistrationState {
                task_id: AtomicU64::new(0),
                completed: AtomicBool::new(false),
                active_tasks,
            }),
        }
    }

    pub(crate) fn guard(&self) -> OperationTaskGuard {
        OperationTaskGuard {
            state: Arc::clone(&self.state),
        }
    }

    pub(crate) fn register(self, task_id: TaskId) {
        self.state.active_tasks.insert(task_id);
        self.state.task_id.store(task_id.as_u64(), Ordering::Release);
        if self.state.completed.load(Ordering::Acquire) {
            self.state.active_tasks.remove(&task_id);
        }
    }
}

pub(crate) struct OperationTaskGuard {
    state: Arc<OperationTaskRegistrationState>,
}

impl Drop for OperationTaskGuard {
    fn drop(&mut self) {
        self.state.completed.store(true, Ordering::Release);
        let task_id = self.state.task_id.load(Ordering::Acquire);
        if task_id != 0 {
            self.state.active_tasks.remove(&TaskId::from_raw(task_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeContext;

    #[tokio::test]
    async fn completed_operations_leave_no_registry_history() {
        const TASKS: usize = 1_024;

        let runtime = RuntimeContext::from_current("operation-churn-test");
        let owner = runtime.service_context("operations");
        let baseline_components = owner.task_group().component_count();
        let operation = OperationContext::without_deadline(TaskKind::Worker);

        for _ in 0..TASKS {
            owner
                .task_group()
                .spawn_operation(&operation, "short-operation", async {})
                .expect("operation task should spawn");
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while operation.active_task_count() != 0 || owner.task_group().task_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("operation tasks should complete");

        assert_eq!(owner.task_group().component_count(), baseline_components);
        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn cancellation_joins_only_operation_tasks() {
        let runtime = RuntimeContext::from_current("operation-cancellation-test");
        let owner = runtime.service_context("operations");
        let first = OperationContext::without_deadline(TaskKind::Worker);
        let second = OperationContext::without_deadline(TaskKind::Worker);

        owner
            .task_group()
            .spawn_operation(&first, "first-operation", std::future::pending())
            .expect("first operation should spawn");
        owner
            .task_group()
            .spawn_operation(&second, "second-operation", std::future::pending())
            .expect("second operation should spawn");

        assert!(first
            .cancel_and_wait(owner.task_group(), Duration::from_secs(1))
            .await
            .expect("first operation should use its bound owner"));
        assert_eq!(first.active_task_count(), 0);
        assert_eq!(second.active_task_count(), 1);
        assert_eq!(
            owner.task_group().lifecycle_state(),
            crate::TaskGroupLifecycleState::Open
        );

        assert!(second
            .cancel_and_wait(owner.task_group(), Duration::from_secs(1))
            .await
            .expect("second operation should use its bound owner"));
        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn owner_shutdown_cancels_operation_tasks() {
        let runtime = RuntimeContext::from_current("operation-owner-shutdown-test");
        let owner = runtime.service_context("operations");
        let operation = OperationContext::without_deadline(TaskKind::Worker);

        owner
            .task_group()
            .spawn_operation(&operation, "owned-operation", std::future::pending())
            .expect("operation task should spawn");

        let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;

        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(operation.active_task_count(), 0);
    }
}
