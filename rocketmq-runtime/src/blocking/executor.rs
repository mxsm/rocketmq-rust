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
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use super::admission::GlobalBlockingBudget;
use super::admission::GlobalBlockingPermit;
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
use crate::handle::RuntimeHandle;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupLifecycleState;

/// Runs short blocking work through a bounded lane and one root-owned global
/// admission budget.
///
/// Cloning this value shares queue state and capacity; it never creates a new
/// owner. Execution always uses the injected owner's Tokio runtime, including
/// when the submission future is polled by a different runtime.
/// Cancellation while queued removes the task immediately. Cancellation
/// or timeout after execution begins leaves the admission permit inside the
/// actual blocking closure, so capacity is released only when that closure
/// exits.
#[derive(Debug, Clone)]
pub struct BlockingExecutor {
    runtime: RuntimeHandle,
    policy: Arc<BlockingPoolPolicy>,
    lane: BlockingLane,
    budget: GlobalBlockingBudget,
    queue_permits: Arc<Semaphore>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    next_task_id: Arc<AtomicU64>,
    rejected: Arc<AtomicU64>,
    admission: BlockingAdmission,
}

#[derive(Debug, Clone)]
enum BlockingAdmission {
    Unscoped,
    Scope(TaskGroup),
    Drain(Arc<DrainLeaseState>),
}

#[derive(Debug)]
struct DrainLeaseState {
    scope: TaskGroup,
    deadline: ShutdownDeadline,
    remaining: AtomicUsize,
}

impl DrainLeaseState {
    fn effective_deadline(&self) -> ShutdownDeadline {
        self.scope
            .shutdown_deadline()
            .map_or(self.deadline, |scope_deadline| self.deadline.earliest(scope_deadline))
    }
}

/// A bounded authority for I/O required by an operation already accepted by a
/// service scope.
///
/// A lease is created while its scope is open. It may then be used during that
/// scope's shutdown, but only until its original deadline or an earlier
/// shutdown deadline installed on that scope, and only for its reserved number
/// of submissions. It is deliberately not cloneable: moving it preserves one
/// shared, non-expandable allowance.
#[derive(Debug)]
pub struct BlockingDrainLease {
    executor: BlockingExecutor,
}

impl BlockingDrainLease {
    /// Runs one short I/O operation under this lease.
    ///
    /// The existing lane capacity and deadline policy still apply. A running
    /// closure retains its execution permit until it exits. This does not
    /// reopen the scope's ordinary admission.
    pub fn spawn_io<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<R>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor.spawn_io(name, operation)
    }

    /// Runs one short I/O operation without extending this lease's deadline.
    pub fn spawn_io_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<R>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.executor.spawn_io_until(name, deadline, operation)
    }

    /// Returns the number of submissions that this lease can still admit.
    #[must_use]
    pub fn remaining_operations(&self) -> usize {
        let BlockingAdmission::Drain(state) = &self.executor.admission else {
            return 0;
        };
        state.remaining.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct DrainReservation {
    state: Option<Arc<DrainLeaseState>>,
}

impl DrainReservation {
    const fn none() -> Self {
        Self { state: None }
    }

    fn committed(mut self) {
        self.state.take();
    }
}

impl Drop for DrainReservation {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            state.remaining.fetch_add(1, Ordering::Release);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum AdmissionFailure {
    ScopeClosed,
    LeaseExpired,
    LeaseExhausted,
    QueueCapacityExhausted,
    QueueDeadlineExpired,
}

impl AdmissionFailure {
    fn into_error(self) -> RuntimeError {
        match self {
            Self::ScopeClosed => RuntimeError::closed(crate::RuntimeOperation::BlockingQueueAdmission),
            Self::LeaseExpired | Self::QueueDeadlineExpired => {
                RuntimeError::timed_out(crate::RuntimeOperation::BlockingQueueAdmission)
            }
            Self::LeaseExhausted | Self::QueueCapacityExhausted => {
                RuntimeError::capacity(crate::RuntimeOperation::BlockingQueueAdmission)
            }
        }
    }
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

/// An admitted operation whose actual completion outlives any individual wait.
///
/// The owner may retain this ticket after `wait_until` expires and call `wait`
/// to observe the real result. Dropping it abandons observation, not execution.
pub(crate) struct BlockingTask<R>
where
    R: Send + 'static,
{
    join_handle: Option<tokio::task::JoinHandle<RuntimeResult<R>>>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    task_id: BlockingTaskId,
    wait_deadline: Instant,
}

impl<R> BlockingTask<R>
where
    R: Send + 'static,
{
    fn new(
        join_handle: tokio::task::JoinHandle<RuntimeResult<R>>,
        tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
        task_id: BlockingTaskId,
        wait_deadline: Instant,
    ) -> Self {
        Self {
            join_handle: Some(join_handle),
            tasks,
            task_id,
            wait_deadline,
        }
    }

    /// Waits for real execution completion without changing ownership on drop.
    pub(crate) async fn wait(&mut self) -> RuntimeResult<R> {
        let Some(join_handle) = self.join_handle.as_mut() else {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::RunBlockingTask,
            ));
        };
        let result = join_handle.await;
        self.join_handle.take();
        result.map_err(|error| RuntimeError::join(crate::RuntimeOperation::RunBlockingTask, error))?
    }

    async fn wait_until(&mut self, deadline: Instant) -> RuntimeResult<R> {
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), self.wait()).await {
            Ok(result) => result,
            Err(_elapsed) => {
                self.mark_timed_out();
                Err(RuntimeError::timed_out(crate::RuntimeOperation::BlockingTask))
            }
        }
    }

    fn mark_timed_out(&mut self) {
        if let Some(mut meta) = self.tasks.get_mut(&self.task_id) {
            meta.state = BlockingTaskState::TimedOutStillRunning;
        }
    }
}

impl<R> Drop for BlockingTask<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        if self.join_handle.is_some() {
            self.mark_timed_out();
        }
    }
}

// Field order also governs cancellation before Tokio invokes the closure:
// destroy user captures, return execution capacity, then remove diagnostics.
struct BlockingWork<F> {
    operation: F,
    permit: GlobalBlockingPermit,
    completion: BlockingCompletionGuard,
    execution_deadline: Option<ShutdownDeadline>,
}

impl<F> BlockingWork<F> {
    fn run<R>(self) -> RuntimeResult<R>
    where
        F: FnOnce() -> R,
    {
        // Reverse local destruction order preserves the same ordering on panic.
        let completion = self.completion;
        let permit = self.permit;
        let result = if self.execution_deadline.is_some_and(ShutdownDeadline::is_expired) {
            Err(RuntimeError::timed_out(crate::RuntimeOperation::BlockingTaskDeadline))
        } else {
            Ok((self.operation)())
        };
        drop(permit);
        drop(completion);
        result
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
    pub fn new(policy: BlockingPoolPolicy, owner_group: TaskGroup) -> Result<Self, RuntimeContractViolation> {
        policy.validate()?;
        let capacity = policy.max_concurrency;
        Ok(Self::new_with_budget(
            policy,
            BlockingLane::StorageIo,
            GlobalBlockingBudget::isolated(capacity),
            owner_group.runtime().clone(),
        )
        .scoped_to(owner_group))
    }

    pub(crate) fn new_managed(
        policy: BlockingPoolPolicy,
        lane: BlockingLane,
        budget: GlobalBlockingBudget,
        runtime: RuntimeHandle,
    ) -> Result<Self, RuntimeContractViolation> {
        policy.validate()?;
        Ok(Self::new_with_budget(policy, lane, budget, runtime))
    }

    fn new_with_budget(
        policy: BlockingPoolPolicy,
        lane: BlockingLane,
        budget: GlobalBlockingBudget,
        runtime: RuntimeHandle,
    ) -> Self {
        Self {
            runtime,
            queue_permits: Arc::new(Semaphore::new(policy.max_queue_depth)),
            policy: Arc::new(policy),
            lane,
            budget,
            // Admission bounds the table to the lane's running and queued work.
            tasks: Arc::new(DashMap::with_shard_amount(8)),
            next_task_id: Arc::new(AtomicU64::new(1)),
            rejected: Arc::new(AtomicU64::new(0)),
            admission: BlockingAdmission::Unscoped,
        }
    }

    pub(crate) fn scoped_to(&self, scope: TaskGroup) -> Self {
        let mut scoped = self.clone();
        scoped.admission = BlockingAdmission::Scope(scope);
        scoped
    }

    /// Reserves a bounded blocking-I/O allowance for work already accepted by
    /// this executor's service scope, or for that owner's finalization slot.
    ///
    /// The scope must still be open when the lease is created. The lease never
    /// extends `deadline` or a shutdown deadline later installed on its scope,
    /// and its non-zero submission allowance is shared if the lease is moved
    /// through application-owned shutdown code.
    ///
    /// # Errors
    ///
    /// Returns an unavailable failure when this executor is unscoped or its
    /// scope is no longer open, and a timeout failure when `deadline` expired.
    pub fn try_drain_lease(
        &self,
        deadline: ShutdownDeadline,
        max_operations: NonZeroUsize,
    ) -> RuntimeResult<BlockingDrainLease> {
        let BlockingAdmission::Scope(scope) = &self.admission else {
            return Err(AdmissionFailure::ScopeClosed.into_error());
        };
        if !scope_is_open(scope) {
            return Err(AdmissionFailure::ScopeClosed.into_error());
        }
        let deadline = scope
            .shutdown_deadline()
            .map_or(deadline, |scope_deadline| deadline.earliest(scope_deadline));
        if deadline.is_expired() {
            return Err(AdmissionFailure::LeaseExpired.into_error());
        }
        let mut leased = self.clone();
        leased.admission = BlockingAdmission::Drain(Arc::new(DrainLeaseState {
            scope: scope.clone(),
            deadline,
            remaining: AtomicUsize::new(max_operations.get()),
        }));
        Ok(BlockingDrainLease { executor: leased })
    }

    /// Returns the policy.
    pub fn policy(&self) -> &BlockingPoolPolicy {
        &self.policy
    }

    /// Runs short blocking I/O with the preparation and polling behavior of [`Self::spawn`].
    pub fn spawn_io<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<R>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn(name, BlockingKind::ShortIo, operation)
    }

    /// Admits short I/O and returns its execution-owned completion ticket.
    #[cfg(test)]
    pub(crate) fn submit_io<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<BlockingTask<R>>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.submit_inner(name.into(), BlockingKind::ShortIo, None, Box::new(operation))
    }

    /// Admits short I/O under an optional caller deadline and returns its
    /// execution-owned completion ticket.
    ///
    /// The deadline is combined with the lane phase budgets by `phase_deadline`,
    /// so it can only tighten them. An expired deadline refuses the submission
    /// instead of starting the closure.
    pub(crate) fn submit_io_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        deadline: Option<ShutdownDeadline>,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<BlockingTask<R>>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.submit_inner(name.into(), BlockingKind::ShortIo, deadline, Box::new(operation))
    }

    /// Runs short blocking I/O without admitting or waiting for work beyond `deadline`.
    pub fn spawn_io_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<R>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_until(name, BlockingKind::ShortIo, deadline, operation)
    }

    /// Spawns the supplied task.
    ///
    /// Converts the name and boxes the closure immediately to keep large
    /// captures out of the returned future. Admission and submission begin
    /// only when that future is polled.
    pub fn spawn<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        kind: BlockingKind,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<R>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        // A synchronous boundary keeps F out of every admission/wait future.
        // One closure allocation also avoids a size-dependent public future
        // layout: an enum containing an inline F would still be as large as F.
        self.spawn_inner(name.into(), kind, None, Box::new(operation))
    }

    /// Runs blocking work with one absolute deadline for admission and waiting.
    ///
    /// Expiry stops waiting; an already running closure retains its capacity
    /// until it exits and may still produce side effects.
    pub fn spawn_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        kind: BlockingKind,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> impl Future<Output = RuntimeResult<R>> + Send + '_
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_inner(name.into(), kind, Some(deadline), Box::new(operation))
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
        if std::mem::size_of::<R>() > crate::stack::MAX_INLINE_SIZE {
            // Keep a large result out of nested JoinHandle/timeout/Result poll
            // frames; unwrap it only once at the public return boundary.
            let task = self
                .submit_inner(name, kind, deadline, move || Box::new(operation()))
                .await?;
            self.wait_for_caller(task).await.map(|value| *value)
        } else {
            let task = self.submit_inner(name, kind, deadline, operation).await?;
            self.wait_for_caller(task).await
        }
    }

    async fn wait_for_caller<R: Send + 'static>(&self, mut task: BlockingTask<R>) -> RuntimeResult<R> {
        let task_id = task.task_id;
        let started_at = Instant::now();
        let deadline = task.wait_deadline;
        let result = task.wait_until(deadline).await;
        if result.is_ok() {
            let elapsed = started_at.elapsed();
            if elapsed > self.policy.warn_after {
                tracing::warn!(
                    task_id = task_id.as_u64(),
                    elapsed_ms = elapsed.as_millis(),
                    "blocking task exceeded warn_after"
                );
            }
        }
        result
    }

    async fn submit_inner<F, R>(
        &self,
        name: Arc<str>,
        kind: BlockingKind,
        deadline: Option<ShutdownDeadline>,
        operation: F,
    ) -> RuntimeResult<BlockingTask<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if kind == BlockingKind::LongRunning {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RuntimeError::unsupported(crate::RuntimeOperation::BlockingExecutorKind));
        }
        let caller_deadline = deadline;
        let admission_deadline = self.admission.effective_deadline(caller_deadline);
        if admission_deadline.is_some_and(ShutdownDeadline::is_expired) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(RuntimeError::timed_out(crate::RuntimeOperation::BlockingQueueAdmission));
        }
        let drain_reservation = self.admission.reserve().map_err(|failure| {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            failure.into_error()
        })?;
        let submitted_at = Instant::now();
        let operation_deadline = admission_deadline.map_or_else(
            || {
                submitted_at
                    .checked_add(self.policy.queue_timeout.saturating_add(self.policy.task_timeout))
                    .unwrap_or(submitted_at)
            },
            ShutdownDeadline::instant,
        );
        let queue_deadline = phase_deadline(submitted_at, self.policy.queue_timeout, operation_deadline);

        let queue_permit = self.acquire_queue_permit(queue_deadline).await.map_err(|failure| {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            failure.into_error()
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

        let permit = self.admit(queue_deadline).await.map_err(|failure| {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            failure.into_error()
        })?;
        drop(queue_permit);

        let execution_deadline = self.admission.effective_deadline(caller_deadline);
        let current_operation_deadline = execution_deadline.map_or(operation_deadline, ShutdownDeadline::instant);
        let task_deadline = phase_deadline(Instant::now(), self.policy.task_timeout, current_operation_deadline);
        if task_deadline <= Instant::now()
            || execution_deadline.is_some_and(ShutdownDeadline::is_expired)
            || !self.admission.still_valid()
        {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(
                if task_deadline <= Instant::now() || execution_deadline.is_some_and(ShutdownDeadline::is_expired) {
                    RuntimeError::timed_out(crate::RuntimeOperation::BlockingTaskDeadline)
                } else {
                    AdmissionFailure::ScopeClosed.into_error()
                },
            );
        }

        let started_at = Instant::now();
        if let Some(mut meta) = self.tasks.get_mut(&task_id) {
            meta.state = BlockingTaskState::Running;
            meta.started_at = Some(started_at);
        }
        queued_task_guard.disarm();

        let work = BlockingWork {
            operation,
            permit,
            completion: BlockingCompletionGuard {
                tasks: self.tasks.clone(),
                task_id,
            },
            execution_deadline,
        };
        let join_handle = self.runtime.tokio_handle().spawn_blocking(move || work.run());
        drain_reservation.committed();
        Ok(BlockingTask::new(
            join_handle,
            self.tasks.clone(),
            task_id,
            task_deadline,
        ))
    }

    async fn acquire_queue_permit(&self, deadline: Instant) -> Result<OwnedSemaphorePermit, AdmissionFailure> {
        if !matches!(&self.admission, BlockingAdmission::Drain(_)) {
            return self
                .queue_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_error| AdmissionFailure::QueueCapacityExhausted);
        }

        let deadline = self.admission.effective_instant(deadline);
        match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.queue_permits.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_closed)) => Err(AdmissionFailure::QueueDeadlineExpired),
            Err(_elapsed) => Err(AdmissionFailure::QueueDeadlineExpired),
        }
    }

    async fn admit(&self, deadline: Instant) -> Result<GlobalBlockingPermit, AdmissionFailure> {
        match &self.admission {
            BlockingAdmission::Scope(scope) => {
                if !scope_is_open(scope) {
                    return Err(AdmissionFailure::ScopeClosed);
                }
                let cancellation = scope.cancellation_token();
                let permit = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(AdmissionFailure::ScopeClosed),
                    permit = self.budget.acquire(self.lane, deadline) => {
                        permit.map_err(|()| AdmissionFailure::QueueDeadlineExpired)?
                    }
                };
                if !scope_is_open(scope) {
                    drop(permit);
                    return Err(AdmissionFailure::ScopeClosed);
                }
                Ok(permit)
            }
            BlockingAdmission::Drain(state) => {
                if state.effective_deadline().is_expired() {
                    return Err(AdmissionFailure::LeaseExpired);
                }
                let deadline = self.admission.effective_instant(deadline);
                self.budget
                    .acquire(self.lane, deadline)
                    .await
                    .map_err(|()| AdmissionFailure::QueueDeadlineExpired)
            }
            BlockingAdmission::Unscoped => self
                .budget
                .acquire(self.lane, deadline)
                .await
                .map_err(|()| AdmissionFailure::QueueDeadlineExpired),
        }
    }

    /// Samples aggregate state without allocating task names or a detail list.
    pub(crate) fn aggregate(&self) -> super::BlockingExecutorAggregate {
        let mut aggregate =
            super::BlockingExecutorAggregate::new(self.lane, self.policy.max_concurrency, self.policy.max_queue_depth);
        let now = Instant::now();
        for entry in self.tasks.iter() {
            let task = entry.value();
            aggregate.record_kind(
                task.kind,
                now.saturating_duration_since(task.started_at.unwrap_or(task.queued_at)),
            );
            match task.state {
                BlockingTaskState::Queued => aggregate.queued += 1,
                BlockingTaskState::Running => aggregate.running += 1,
                BlockingTaskState::TimedOutStillRunning => aggregate.timed_out_still_running += 1,
                BlockingTaskState::Completed | BlockingTaskState::JoinFailed => {}
            }
        }
        aggregate.blocking_still_running = aggregate.running + aggregate.timed_out_still_running;
        aggregate
    }

    /// Returns the snapshot, including task names and individual elapsed times.
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

impl BlockingAdmission {
    fn effective_deadline(&self, deadline: Option<ShutdownDeadline>) -> Option<ShutdownDeadline> {
        match self {
            Self::Unscoped => deadline,
            Self::Scope(scope) => bound_deadline(deadline, scope.shutdown_deadline()),
            Self::Drain(state) => bound_deadline(deadline, Some(state.effective_deadline())),
        }
    }

    fn reserve(&self) -> Result<DrainReservation, AdmissionFailure> {
        match self {
            Self::Unscoped => Ok(DrainReservation::none()),
            Self::Scope(scope) => {
                if scope_is_open(scope) {
                    Ok(DrainReservation::none())
                } else {
                    Err(AdmissionFailure::ScopeClosed)
                }
            }
            Self::Drain(state) => {
                if state.effective_deadline().is_expired() {
                    return Err(AdmissionFailure::LeaseExpired);
                }
                let reserved = state
                    .remaining
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        remaining.checked_sub(1)
                    });
                if reserved.is_err() {
                    return Err(AdmissionFailure::LeaseExhausted);
                }
                Ok(DrainReservation {
                    state: Some(Arc::clone(state)),
                })
            }
        }
    }

    fn still_valid(&self) -> bool {
        match self {
            Self::Unscoped => true,
            Self::Scope(scope) => {
                scope_is_open(scope) && !scope.shutdown_deadline().is_some_and(ShutdownDeadline::is_expired)
            }
            Self::Drain(state) => !state.effective_deadline().is_expired(),
        }
    }

    fn effective_instant(&self, deadline: Instant) -> Instant {
        self.effective_deadline(Some(ShutdownDeadline::at(deadline)))
            .map_or(deadline, ShutdownDeadline::instant)
    }
}

fn bound_deadline(
    deadline: Option<ShutdownDeadline>,
    scope_deadline: Option<ShutdownDeadline>,
) -> Option<ShutdownDeadline> {
    match (deadline, scope_deadline) {
        (Some(deadline), Some(scope_deadline)) => Some(deadline.earliest(scope_deadline)),
        (Some(deadline), None) => Some(deadline),
        (None, Some(scope_deadline)) => Some(scope_deadline),
        (None, None) => None,
    }
}

fn scope_is_open(scope: &TaskGroup) -> bool {
    scope.lifecycle_state() == TaskGroupLifecycleState::Open && !scope.cancellation_token().is_cancelled()
}

fn phase_deadline(started_at: Instant, policy_timeout: Duration, operation_deadline: Instant) -> Instant {
    started_at
        .checked_add(policy_timeout)
        .unwrap_or(operation_deadline)
        .min(operation_deadline)
}

#[cfg(test)]
mod tests;
