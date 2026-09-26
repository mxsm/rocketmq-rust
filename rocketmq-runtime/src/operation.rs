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
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use futures::future::join_all;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::shutdown_deadline::ABORT_CONFIRMATION_TIMEOUT;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskKind;

/// Why an accepted operation task finished, after its future was destroyed.
///
/// Completion means normal return, not business success. Explicit aborts are
/// distinct from a cancellation branch selected by the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationOutcome {
    /// The future returned normally.
    Completed = 1,
    /// The operation-local cancellation branch won.
    Cancelled = 2,
    /// The operation's deadline branch won.
    DeadlineExceeded = 3,
    /// The component owner's cancellation branch won.
    OwnerCancelled = 4,
    /// Polling or destroying the future panicked.
    Panicked = 5,
    /// The future was discarded before selecting another terminal outcome.
    Aborted = 6,
}

impl OperationOutcome {
    /// All outcomes in counter snapshot order.
    pub const ALL: [Self; 6] = [
        Self::Completed,
        Self::Cancelled,
        Self::DeadlineExceeded,
        Self::OwnerCancelled,
        Self::Panicked,
        Self::Aborted,
    ];

    /// Stable, low-cardinality diagnostic label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::OwnerCancelled => "owner_cancelled",
            Self::Panicked => "panicked",
            Self::Aborted => "aborted",
        }
    }
}

/// Observes task outcomes after future destruction.
///
/// Callbacks must be short, nonblocking and panic-free, including during panic
/// unwinding. They must not flush exporters or retain request data.
pub trait OperationOutcomeObserver: std::fmt::Debug + Send + Sync {
    /// Records one terminal task outcome using bounded labels.
    fn on_outcome(&self, kind: TaskKind, outcome: OperationOutcome);
}

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
    /// Process-unique id; the owner's task registry tags operation tasks with it.
    id: u64,
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    accepting: AtomicBool,
    /// Serializes admission with `close_admission` and `cancel`.
    spawn_gate: Mutex<()>,
    /// Id of the component owner, or zero before the first submission.
    owner_id: AtomicU64,
    /// Accepted tasks whose futures have not yet been destroyed.
    active: AtomicUsize,
    /// Woken when `active` drops to zero.
    idle: Notify,
    outcomes: [AtomicU64; 6],
    observer: OnceLock<Arc<dyn OperationOutcomeObserver>>,
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
        static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
        Self {
            inner: Arc::new(OperationContextInner {
                id: NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed),
                cancellation: CancellationToken::new(),
                deadline,
                accepting: AtomicBool::new(true),
                spawn_gate: Mutex::new(()),
                owner_id: AtomicU64::new(0),
                active: AtomicUsize::new(0),
                idle: Notify::new(),
                outcomes: std::array::from_fn(|_| AtomicU64::new(0)),
                observer: OnceLock::new(),
            }),
            task_kind,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.inner.id
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
        self.inner.active.load(Ordering::Acquire)
    }

    /// Reads counters without retaining individual task history.
    ///
    /// Counters share this operation's lifetime and are individually atomic;
    /// completions can occur between fields in this snapshot.
    pub fn outcomes(&self) -> [(OperationOutcome, u64); 6] {
        OperationOutcome::ALL.map(|outcome| {
            (
                outcome,
                self.inner.outcomes[outcome as usize - 1].load(Ordering::Acquire),
            )
        })
    }

    /// Binds an observer before the first submission attempt.
    ///
    /// # Errors
    ///
    /// Returns an error after owner binding or an earlier observer installation.
    pub fn set_outcome_observer(&self, observer: Arc<dyn OperationOutcomeObserver>) -> RuntimeResult<()> {
        let _gate = self.inner.spawn_gate.lock();
        if self.inner.owner_id.load(Ordering::Acquire) != 0 || self.inner.observer.set(observer).is_err() {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::SpawnOperation,
            ));
        }
        Ok(())
    }

    /// Cancels this operation and waits for all tasks registered with `owner`.
    ///
    /// Returns `true` when every task completed before the shared timeout.
    /// Tasks still running at the deadline are aborted and awaited for a
    /// bounded confirmation window beyond it, so their running futures are
    /// dropped before this returns.
    ///
    /// # Errors
    ///
    /// Returns an error when `owner` differs from the component owner used to
    /// spawn the operation.
    pub async fn cancel_and_wait(&self, owner: &TaskGroup, timeout: Duration) -> RuntimeResult<bool> {
        self.cancel();
        self.wait(owner, timeout).await
    }

    /// Waits until no operation task is active and `owner` has settled every
    /// one, without requesting cancellation. Tasks still running at the
    /// deadline are aborted and awaited for a bounded confirmation window.
    ///
    /// Tasks accepted during the wait are waited for too; close admission
    /// first when new tasks may still be submitted. Returns `true` when every
    /// task finished before the deadline; `owner` then no longer lists any of
    /// them.
    ///
    /// # Errors
    ///
    /// Returns an error when `owner` differs from the component owner used to
    /// spawn the operation.
    pub async fn wait(&self, owner: &TaskGroup, timeout: Duration) -> RuntimeResult<bool> {
        self.ensure_owner(owner.id())?;
        let deadline = Instant::now() + timeout;
        loop {
            // Registered before the check, so a wakeup in between is not lost.
            let idle = self.inner.idle.notified();
            if self.inner.active.load(Ordering::Acquire) == 0 {
                // A task stops counting as active when its future is
                // destroyed; its owner settles it just afterwards. Wait for
                // that too, so a caller never finds a finished task still
                // registered with the owner.
                let unsettled = owner.operation_task_ids(self.inner.id);
                if unsettled.is_empty() {
                    return Ok(true);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                let settled = join_all(unsettled.into_iter().map(|task_id| owner.wait_task(task_id, remaining))).await;
                if settled.into_iter().all(|settled| settled) {
                    continue;
                }
                break;
            }
            if tokio::time::timeout_at(deadline.into(), idle).await.is_err() {
                break;
            }
        }

        let remaining = owner.operation_task_ids(self.inner.id);
        join_all(
            remaining
                .into_iter()
                .map(|task_id| owner.abort_task_and_wait(task_id, ABORT_CONFIRMATION_TIMEOUT)),
        )
        .await;
        Ok(false)
    }

    /// Admits one task: binds the owner and counts the task as active.
    ///
    /// The caller holds [`Self::spawn_guard`], so admission cannot interleave
    /// with `close_admission` or `cancel`.
    pub(crate) fn prepare_spawn(&self, owner_id: TaskGroupId) -> RuntimeResult<OperationTaskRegistration> {
        self.bind_owner(owner_id)?;
        if !self.inner.accepting.load(Ordering::Acquire)
            || self.is_cancelled()
            || self.inner.deadline.is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(RuntimeError::closed(crate::RuntimeOperation::SpawnOperation));
        }
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        Ok(OperationTaskRegistration::new(Arc::clone(&self.inner), self.task_kind))
    }

    pub(crate) fn spawn_guard(&self) -> MutexGuard<'_, ()> {
        self.inner.spawn_gate.lock()
    }

    async fn run<F>(&self, future: F) -> OperationOutcome
    where
        F: Future<Output = ()>,
    {
        match self.inner.deadline {
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = self.inner.cancellation.cancelled() => OperationOutcome::Cancelled,
                    _ = tokio::time::sleep_until(deadline.into()) => OperationOutcome::DeadlineExceeded,
                    _ = future => OperationOutcome::Completed,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = self.inner.cancellation.cancelled() => OperationOutcome::Cancelled,
                    _ = future => OperationOutcome::Completed,
                }
            }
        }
    }

    fn bind_owner(&self, owner_id: TaskGroupId) -> RuntimeResult<()> {
        match self
            .inner
            .owner_id
            .compare_exchange(0, owner_id.as_u64(), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Ok(()),
            Err(bound_owner) if bound_owner == owner_id.as_u64() => Ok(()),
            Err(_) => Err(RuntimeError::internal_failure(crate::RuntimeOperation::OperationOwner)),
        }
    }

    fn ensure_owner(&self, owner_id: TaskGroupId) -> RuntimeResult<()> {
        match self.inner.owner_id.load(Ordering::Acquire) {
            0 => Ok(()),
            bound_owner if bound_owner == owner_id.as_u64() => Ok(()),
            _ => Err(RuntimeError::internal_failure(crate::RuntimeOperation::OperationOwner)),
        }
    }
}

pub(crate) struct OperationTaskRegistration {
    state: Arc<OperationTaskRegistrationState>,
}

/// Set once the owner accepted the task.
const TASK_REGISTERED: u8 = 1;
/// Set once the task's future was destroyed.
const TASK_COMPLETED: u8 = 2;

struct OperationTaskRegistrationState {
    /// `TASK_REGISTERED` and `TASK_COMPLETED` bits. Both sides set their bit
    /// with one read-modify-write, so exactly one sees the other bit and
    /// records the outcome.
    progress: AtomicU8,
    outcome: AtomicU8,
    operation: Arc<OperationContextInner>,
    task_kind: TaskKind,
}

impl OperationTaskRegistration {
    fn new(operation: Arc<OperationContextInner>, task_kind: TaskKind) -> Self {
        Self {
            state: Arc::new(OperationTaskRegistrationState {
                progress: AtomicU8::new(0),
                outcome: AtomicU8::new(0),
                operation,
                task_kind,
            }),
        }
    }

    pub(crate) fn guard(&self) -> OperationTaskGuard {
        OperationTaskGuard {
            state: Arc::clone(&self.state),
            outcome: OperationOutcome::Aborted,
        }
    }

    /// Marks the task as accepted by its owner. A task the owner rejected
    /// never records an outcome.
    pub(crate) fn finish_registration(self) {
        let previous = self.state.progress.fetch_or(TASK_REGISTERED, Ordering::AcqRel);
        if previous & TASK_COMPLETED != 0 {
            self.state.record(self.state.outcome.load(Ordering::Acquire));
        }
    }
}

pub(crate) struct OperationTaskGuard {
    state: Arc<OperationTaskRegistrationState>,
    outcome: OperationOutcome,
}

impl Drop for OperationTaskGuard {
    fn drop(&mut self) {
        let outcome = if std::thread::panicking() {
            OperationOutcome::Panicked
        } else {
            self.outcome
        };
        self.state.outcome.store(outcome as u8, Ordering::Release);
        let previous = self.state.progress.fetch_or(TASK_COMPLETED, Ordering::AcqRel);
        if previous & TASK_REGISTERED != 0 {
            self.state.record(outcome as u8);
        }
        // Counters are published before a waiter can observe the operation idle.
        let operation = &self.state.operation;
        if operation.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            operation.idle.notify_waiters();
        }
    }
}

impl OperationTaskRegistrationState {
    fn record(&self, value: u8) {
        let Some(outcome) = OperationOutcome::ALL
            .into_iter()
            .find(|outcome| *outcome as u8 == value)
        else {
            return;
        };
        self.operation.outcomes[value as usize - 1].fetch_add(1, Ordering::Release);
        if let Some(observer) = self.operation.observer.get() {
            observer.on_outcome(self.task_kind, outcome);
        }
    }
}

// Also destroys the user future first when an accepted task is never polled.
pub(crate) struct OperationExecution<F> {
    future: F,
    finalizer: OperationTaskGuard,
    operation: OperationContext,
    owner_cancellation: Option<CancellationToken>,
}

impl<F: Future<Output = ()>> OperationExecution<F> {
    pub(crate) fn new(
        future: F,
        finalizer: OperationTaskGuard,
        operation: OperationContext,
        owner_cancellation: Option<CancellationToken>,
    ) -> Self {
        Self {
            future,
            finalizer,
            operation,
            owner_cancellation,
        }
    }

    pub(crate) async fn run(self) {
        let mut finalizer = self.finalizer;
        let outcome = match self.owner_cancellation {
            Some(owner) => tokio::select! {
                biased;
                _ = owner.cancelled() => OperationOutcome::OwnerCancelled,
                outcome = self.operation.run(self.future) => outcome,
            },
            None => self.operation.run(self.future).await,
        };
        finalizer.outcome = outcome;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeContext;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_returns_only_after_the_owner_settles_the_tasks() {
        let context = RuntimeContext::from_current("operation-settlement");
        let owner = context.service_context("owner").task_group().clone();
        let operation = OperationContext::without_deadline(TaskKind::Worker);
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
        let task_id = owner
            .spawn_operation(&operation, "settles-late", async move {
                let _ = finish_rx.await;
            })
            .unwrap();

        // Hold the owner's settlement gate on another thread, so the task's
        // future can be destroyed while the owner cannot yet settle it.
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let gate_owner = owner.clone();
        let holder = std::thread::spawn(move || {
            let _settlement = gate_owner.lock_settlement_for_test();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();
        finish_tx.send(()).unwrap();
        while operation.active_task_count() != 0 {
            tokio::task::yield_now().await;
        }

        let mut wait = Box::pin(operation.wait(&owner, Duration::from_secs(5)));
        assert!(
            futures::poll!(wait.as_mut()).is_pending(),
            "a task the owner has not settled still belongs to the operation"
        );
        assert!(owner.contains_task(task_id));

        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert!(wait.await.unwrap());
        assert!(!owner.contains_task(task_id));
        assert_eq!(owner.task_count(), 0);
    }

    struct OutcomeFuture {
        dropped: Arc<AtomicBool>,
        ready: bool,
        panic_poll: bool,
        panic_drop: bool,
    }

    impl Future for OutcomeFuture {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
            assert!(!self.panic_poll, "operation poll panic");
            if self.ready {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }
    }

    impl Drop for OutcomeFuture {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
            assert!(!self.panic_drop, "operation destructor panic");
        }
    }

    #[derive(Debug)]
    struct RecordingOutcomeObserver {
        dropped: Arc<AtomicBool>,
        events: Mutex<Vec<OperationOutcome>>,
    }

    impl OperationOutcomeObserver for RecordingOutcomeObserver {
        fn on_outcome(&self, _kind: TaskKind, outcome: OperationOutcome) {
            assert!(
                self.dropped.load(Ordering::Acquire),
                "outcome preceded future destruction"
            );
            self.events.lock().push(outcome);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn all_outcomes_are_recorded_once_after_future_destruction() {
        for expected in OperationOutcome::ALL {
            let runtime = RuntimeContext::from_current("operation-outcome");
            let owner = runtime.service_context("operation-owner");
            let operation = if expected == OperationOutcome::DeadlineExceeded {
                OperationContext::new(Instant::now() + Duration::from_secs(60), TaskKind::Worker)
            } else {
                OperationContext::without_deadline(TaskKind::Worker)
            };
            let dropped = Arc::new(AtomicBool::new(false));
            let observer = Arc::new(RecordingOutcomeObserver {
                dropped: dropped.clone(),
                events: Mutex::new(Vec::new()),
            });
            operation.set_outcome_observer(observer.clone()).unwrap();
            let id = owner
                .task_group()
                .spawn_operation(
                    &operation,
                    "selected-outcome",
                    OutcomeFuture {
                        dropped,
                        ready: expected == OperationOutcome::Completed,
                        panic_poll: expected == OperationOutcome::Panicked,
                        panic_drop: false,
                    },
                )
                .unwrap();
            match expected {
                OperationOutcome::Cancelled => operation.cancel(),
                OperationOutcome::OwnerCancelled => owner.task_group().cancel(),
                OperationOutcome::DeadlineExceeded => tokio::time::advance(Duration::from_secs(61)).await,
                OperationOutcome::Aborted => {
                    assert!(owner.task_group().abort_task(id));
                }
                OperationOutcome::Completed | OperationOutcome::Panicked => {}
            }
            while owner.task_group().task_count() != 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(*observer.events.lock(), [expected]);
            assert_eq!(operation.active_task_count(), 0);
            for (outcome, count) in operation.outcomes() {
                assert_eq!(count, u64::from(outcome == expected), "{expected:?}: {outcome:?}");
            }
            let report = runtime.shutdown_tasks(Duration::from_secs(1)).await;
            assert_eq!(report.is_healthy(), expected != OperationOutcome::Panicked);
        }
    }

    #[tokio::test]
    async fn destructor_panic_is_an_operation_failure_and_rejected_work_has_no_outcome() {
        let runtime = RuntimeContext::from_current("operation-destructor");
        let owner = runtime.service_context("operation-owner");
        let operation = OperationContext::without_deadline(TaskKind::Worker);
        let dropped = Arc::new(AtomicBool::new(false));
        let observer = Arc::new(RecordingOutcomeObserver {
            dropped: dropped.clone(),
            events: Mutex::new(Vec::new()),
        });
        operation.set_outcome_observer(observer.clone()).unwrap();
        owner
            .task_group()
            .spawn_operation(
                &operation,
                "destructor-panic",
                OutcomeFuture {
                    dropped,
                    ready: true,
                    panic_poll: false,
                    panic_drop: true,
                },
            )
            .unwrap();
        while owner.task_group().task_count() != 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(*observer.events.lock(), [OperationOutcome::Panicked]);
        runtime.shutdown_tasks(Duration::from_secs(1)).await;

        let rejected = OperationContext::without_deadline(TaskKind::Worker);
        assert!(owner
            .task_group()
            .spawn_operation(&rejected, "closed", async {})
            .is_err());
        assert_eq!(rejected.active_task_count(), 0);
        assert!(rejected.outcomes().into_iter().all(|(_, count)| count == 0));
    }

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
        assert_eq!(operation.outcomes()[0], (OperationOutcome::Completed, TASKS as u64));
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
    async fn wait_confirms_cancellation_of_an_overrunning_operation() {
        struct DropMarker(Arc<AtomicBool>);

        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let runtime = RuntimeContext::from_current("operation-abort-confirmation-test");
        let owner = runtime.service_context("operations");
        let operation = OperationContext::without_deadline(TaskKind::Worker);
        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));

        owner
            .task_group()
            .spawn_operation(&operation, "overrunning-operation", {
                let started = Arc::clone(&started);
                let dropped = Arc::clone(&dropped);
                async move {
                    let _marker = DropMarker(dropped);
                    started.store(true, Ordering::Release);
                    std::future::pending::<()>().await
                }
            })
            .expect("operation task should spawn");

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("operation task should start");

        assert!(
            !operation
                .wait(owner.task_group(), Duration::from_millis(10))
                .await
                .expect("operation should use its bound owner"),
            "an overrunning operation must not report a clean drain"
        );
        assert!(
            dropped.load(Ordering::Acquire),
            "wait should confirm cancellation before returning"
        );
        assert_eq!(operation.active_task_count(), 0);

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
