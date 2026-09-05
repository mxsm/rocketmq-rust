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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::Semaphore;

use super::admission::GlobalBlockingBudget;
use super::diagnostics::BlockingTaskMeta;
use super::BlockingExecutorSnapshot;
use super::BlockingKind;
use super::BlockingLane;
use super::BlockingPoolPolicy;
use super::BlockingTaskId;
use super::BlockingTaskState;
use crate::error::RuntimeContractViolation;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::task_group::TaskGroup;

/// Runs short blocking work through a bounded lane and one root-owned global
/// admission budget.
///
/// Cloning this value shares queue state and capacity; it never creates a new
/// owner. Cancellation while queued removes the task immediately. Cancellation
/// or timeout after execution begins leaves the admission permit inside the
/// actual blocking closure, so capacity is released only when that closure
/// exits.
#[derive(Debug, Clone)]
pub struct BlockingExecutor {
    policy: Arc<BlockingPoolPolicy>,
    lane: BlockingLane,
    budget: GlobalBlockingBudget,
    queue_permits: Arc<Semaphore>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    next_task_id: Arc<AtomicU64>,
    rejected: Arc<AtomicU64>,
}

struct QueuedBlockingTaskGuard {
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    task_id: BlockingTaskId,
    armed: bool,
}

impl QueuedBlockingTaskGuard {
    fn new(tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>, task_id: BlockingTaskId) -> Self {
        Self {
            tasks,
            task_id,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for QueuedBlockingTaskGuard {
    fn drop(&mut self) {
        if self.armed {
            self.tasks.remove(&self.task_id);
        }
    }
}

struct RunningBlockingTaskGuard<R>
where
    R: Send + 'static,
{
    join_handle: Option<tokio::task::JoinHandle<R>>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    task_id: BlockingTaskId,
}

impl<R> RunningBlockingTaskGuard<R>
where
    R: Send + 'static,
{
    fn new(
        join_handle: tokio::task::JoinHandle<R>,
        tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
        task_id: BlockingTaskId,
    ) -> Self {
        Self {
            join_handle: Some(join_handle),
            tasks,
            task_id,
        }
    }

    fn join_handle(&mut self) -> Option<&mut tokio::task::JoinHandle<R>> {
        self.join_handle.as_mut()
    }

    fn disarm(&mut self) {
        self.join_handle.take();
    }

    fn mark_timed_out(&mut self) {
        if let Some(mut meta) = self.tasks.get_mut(&self.task_id) {
            meta.state = BlockingTaskState::TimedOutStillRunning;
        }
        self.join_handle.take();
    }
}

impl<R> Drop for RunningBlockingTaskGuard<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if self.join_handle.is_some() {
            self.mark_timed_out();
        }
    }
}

struct BlockingCompletionGuard {
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    task_id: BlockingTaskId,
}

impl Drop for BlockingCompletionGuard {
    fn drop(&mut self) {
        self.tasks.remove(&self.task_id);
    }
}

impl BlockingExecutor {
    /// Creates an isolated compatibility executor.
    ///
    /// Runtime composition roots use one shared budget through
    /// `new_managed`; this constructor preserves the existing public test and
    /// adapter surface by assigning the executor its own exact capacity.
    pub fn new(policy: BlockingPoolPolicy, _owner_group: TaskGroup) -> Result<Self, RuntimeContractViolation> {
        policy.validate()?;
        let capacity = policy.max_concurrency;
        Ok(Self::new_with_budget(
            policy,
            BlockingLane::StorageIo,
            GlobalBlockingBudget::isolated(capacity),
        ))
    }

    pub(crate) fn new_managed(
        policy: BlockingPoolPolicy,
        lane: BlockingLane,
        budget: GlobalBlockingBudget,
    ) -> Result<Self, RuntimeContractViolation> {
        policy.validate()?;
        Ok(Self::new_with_budget(policy, lane, budget))
    }

    fn new_with_budget(policy: BlockingPoolPolicy, lane: BlockingLane, budget: GlobalBlockingBudget) -> Self {
        Self {
            queue_permits: Arc::new(Semaphore::new(policy.max_queue_depth)),
            policy: Arc::new(policy),
            lane,
            budget,
            tasks: Arc::new(DashMap::new()),
            next_task_id: Arc::new(AtomicU64::new(1)),
            rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the policy.
    pub fn policy(&self) -> &BlockingPoolPolicy {
        &self.policy
    }

    /// Spawns io.
    pub async fn spawn_io<F, R>(&self, name: impl Into<Arc<str>>, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn(name, BlockingKind::ShortIo, operation).await
    }

    /// Runs short blocking I/O without admitting or waiting for work beyond `deadline`.
    pub async fn spawn_io_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_until(name, BlockingKind::ShortIo, deadline, operation).await
    }

    /// Spawns the supplied task.
    pub async fn spawn<F, R>(&self, name: impl Into<Arc<str>>, kind: BlockingKind, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_inner(name.into(), kind, None, operation).await
    }

    /// Runs blocking work while bounding queue admission and execution by one absolute deadline.
    pub async fn spawn_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        kind: BlockingKind,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_inner(name.into(), kind, Some(deadline), operation).await
    }

    async fn spawn_inner<F, R>(
        &self,
        name: Arc<str>,
        kind: BlockingKind,
        deadline: Option<ShutdownDeadline>,
        operation: F,
    ) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if kind == BlockingKind::LongRunning {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RuntimeError::unsupported(crate::RuntimeOperation::BlockingExecutorKind));
        }
        if deadline.is_some_and(ShutdownDeadline::is_expired) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RuntimeError::timed_out(crate::RuntimeOperation::BlockingQueueAdmission));
        }
        let submitted_at = Instant::now();
        let operation_deadline = deadline.map_or_else(
            || {
                submitted_at
                    .checked_add(self.policy.queue_timeout.saturating_add(self.policy.task_timeout))
                    .unwrap_or(submitted_at)
            },
            ShutdownDeadline::instant,
        );

        let queue_permit = self.queue_permits.clone().try_acquire_owned().map_err(|_error| {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            RuntimeError::capacity(crate::RuntimeOperation::BlockingQueueAdmission)
        })?;
        let task_id = BlockingTaskId(self.next_task_id.fetch_add(1, Ordering::Relaxed));
        self.tasks.insert(
            task_id,
            BlockingTaskMeta {
                id: task_id,
                name: name.clone(),
                kind,
                state: BlockingTaskState::Queued,
                queued_at: Instant::now(),
                started_at: None,
            },
        );
        let queued_task_guard = QueuedBlockingTaskGuard::new(self.tasks.clone(), task_id);

        let queue_deadline = phase_deadline(submitted_at, self.policy.queue_timeout, operation_deadline);
        let permit = self.budget.acquire(self.lane, queue_deadline).await.map_err(|()| {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            RuntimeError::timed_out(crate::RuntimeOperation::BlockingQueueAdmission)
        })?;
        drop(queue_permit);

        let task_deadline = phase_deadline(Instant::now(), self.policy.task_timeout, operation_deadline);
        if task_deadline <= Instant::now() {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RuntimeError::timed_out(crate::RuntimeOperation::BlockingTaskDeadline));
        }

        let started_at = Instant::now();
        if let Some(mut meta) = self.tasks.get_mut(&task_id) {
            meta.state = BlockingTaskState::Running;
            meta.started_at = Some(started_at);
        }
        queued_task_guard.disarm();

        let tasks = self.tasks.clone();
        let completion_tasks = tasks.clone();
        let join_handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _completion = BlockingCompletionGuard {
                tasks: completion_tasks,
                task_id,
            };
            operation()
        });
        let mut running_task_guard = RunningBlockingTaskGuard::new(join_handle, tasks, task_id);
        let Some(join_handle) = running_task_guard.join_handle() else {
            return Err(RuntimeError::internal_failure(crate::RuntimeOperation::RunBlockingTask));
        };

        match tokio::time::timeout_at(tokio::time::Instant::from_std(task_deadline), join_handle).await {
            Ok(Ok(value)) => {
                running_task_guard.disarm();
                let elapsed = started_at.elapsed();
                if elapsed > self.policy.warn_after {
                    tracing::warn!(
                        task_id = task_id.as_u64(),
                        task_name = %name,
                        elapsed_ms = elapsed.as_millis(),
                        "blocking task exceeded warn_after"
                    );
                }
                Ok(value)
            }
            Ok(Err(error)) => {
                running_task_guard.disarm();
                Err(RuntimeError::join(crate::RuntimeOperation::RunBlockingTask, error))
            }
            Err(_elapsed) => {
                running_task_guard.mark_timed_out();
                Err(RuntimeError::timed_out(crate::RuntimeOperation::BlockingTask))
            }
        }
    }

    /// Returns the snapshot.
    pub fn snapshot(&self) -> BlockingExecutorSnapshot {
        let tasks = self
            .tasks
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect::<Vec<_>>();
        let queued = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::Queued)
            .count();
        let running = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::Running)
            .count();
        let timed_out_still_running = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::TimedOutStillRunning)
            .count();
        let oldest_queue_wait = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::Queued)
            .map(|task| task.elapsed)
            .max()
            .unwrap_or(Duration::ZERO);
        let admission = self.budget.snapshot(self.lane);

        BlockingExecutorSnapshot {
            name: self.policy.name.clone(),
            lane: self.lane,
            max_concurrency: self.policy.max_concurrency,
            max_queue_depth: self.policy.max_queue_depth,
            global_capacity: admission.global_capacity,
            global_running: admission.global_running,
            global_available: admission.global_available,
            lane_reserved: admission.lane_reserved,
            lane_running: admission.lane_running,
            lane_borrowed: admission.lane_borrowed,
            queued,
            running,
            timed_out_still_running,
            blocking_still_running: running + timed_out_still_running,
            rejected: self.rejected.load(Ordering::Relaxed),
            oldest_queue_wait,
            tasks,
        }
    }

    /// Returns the blocking still running.
    pub fn blocking_still_running(&self) -> usize {
        self.tasks
            .iter()
            .filter(|entry| {
                matches!(
                    entry.value().state,
                    BlockingTaskState::Running | BlockingTaskState::TimedOutStillRunning
                )
            })
            .count()
    }
}

fn phase_deadline(started_at: Instant, policy_timeout: Duration, operation_deadline: Instant) -> Instant {
    started_at
        .checked_add(policy_timeout)
        .unwrap_or(operation_deadline)
        .min(operation_deadline)
}
