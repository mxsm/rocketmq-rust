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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::mapref::entry::Entry;
use futures::future::join_all;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::critical::CriticalFailureState;
use crate::critical::CriticalRegistration;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::operation::OperationContext;
use crate::service_context::RootGroupPermit;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::shutdown_report::ShutdownAnnotation;
use crate::shutdown_report::ShutdownReport;
use crate::shutdown_report::TaskSnapshot;

mod completion;
mod diagnostics;
mod registry;
mod shutdown;
mod submission;

use registry::ActiveTaskRegistry;
use shutdown::ShutdownCoordinator;

const STATE_OPEN: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;
const STATE_SHUTDOWN_COMPLETED: u8 = 3;
const STATE_POISONED: u8 = 4;

// Apply the large-future boundary before adding lifecycle and tracker wrappers. Waiting for
// Tokio's spawn boundary leaves their by-value stack frames live during task submission.
const MAX_INLINE_TASK_FUTURE_SIZE: usize = crate::stack::MAX_INLINE_SIZE;

/// Identifies a task within the group that owns it.
///
/// The id records its owning group, so a group never mistakes another
/// group's task for one of its own finished tasks. It serializes as the
/// per-group sequence number; reports carry the group id separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TaskId {
    sequence: u64,
    #[serde(skip)]
    group: TaskGroupId,
}

impl TaskId {
    pub(crate) fn new(group: TaskGroupId, sequence: u64) -> Self {
        Self { sequence, group }
    }

    /// Returns the sequence number of this task within its group.
    pub fn as_u64(self) -> u64 {
        self.sequence
    }

    /// Returns the id of the group that owns this task.
    pub fn group_id(self) -> TaskGroupId {
        self.group
    }
}

/// Identifies a task group.
///
/// Ids are unique within the process: groups of different runtime owners or
/// contexts never share an id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct TaskGroupId(u64);

impl TaskGroupId {
    /// Borrows this value as u64.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    fn next() -> Self {
        static NEXT_TASK_GROUP_ID: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_TASK_GROUP_ID.fetch_add(1, Ordering::Relaxed))
    }
}

/// Name of a task, shown in shutdown reports and diagnostics.
///
/// A `&'static str` is stored without allocating; owned and shared strings
/// are kept as they are.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskName(TaskNameRepr);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TaskNameRepr {
    Static(&'static str),
    Shared(Arc<str>),
}

impl TaskName {
    /// Returns the name.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            TaskNameRepr::Static(name) => name,
            TaskNameRepr::Shared(name) => name,
        }
    }
}

impl std::fmt::Display for TaskName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&'static str> for TaskName {
    fn from(name: &'static str) -> Self {
        Self(TaskNameRepr::Static(name))
    }
}

impl From<String> for TaskName {
    fn from(name: String) -> Self {
        Self(TaskNameRepr::Shared(Arc::from(name)))
    }
}

impl From<&String> for TaskName {
    fn from(name: &String) -> Self {
        Self(TaskNameRepr::Shared(Arc::from(name.as_str())))
    }
}

impl From<Arc<str>> for TaskName {
    fn from(name: Arc<str>) -> Self {
        Self(TaskNameRepr::Shared(name))
    }
}

impl From<&Arc<str>> for TaskName {
    fn from(name: &Arc<str>) -> Self {
        Self(TaskNameRepr::Shared(Arc::clone(name)))
    }
}

impl From<Box<str>> for TaskName {
    fn from(name: Box<str>) -> Self {
        Self(TaskNameRepr::Shared(Arc::from(name)))
    }
}

impl From<std::borrow::Cow<'static, str>> for TaskName {
    fn from(name: std::borrow::Cow<'static, str>) -> Self {
        match name {
            std::borrow::Cow::Borrowed(name) => Self::from(name),
            std::borrow::Cow::Owned(name) => Self::from(name),
        }
    }
}

/// Counts of failures that a task group tree absorbs without an error.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TaskGroupEventCounts {
    /// Groups poisoned by a panicking task.
    pub poisoned_groups: u64,
    /// Component requests answered with an already closed group, because the
    /// owner no longer admitted children.
    pub closed_component_requests: u64,
}

/// Event counters shared by every group of one tree.
#[derive(Debug, Default)]
struct TaskGroupTreeEvents {
    poisoned_groups: AtomicU64,
    closed_component_requests: AtomicU64,
}

/// Identifies the task kind state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskKind {
    /// Represents the service case.
    Service,
    /// Represents the worker case.
    Worker,
    /// Represents the scheduled driver case.
    ScheduledDriver,
    /// Represents the scheduled run case.
    ScheduledRun,
    /// Represents the blocking reaper case.
    BlockingReaper,
    /// Represents the shutdown case.
    Shutdown,
    /// Represents the other case.
    Other,
}

impl TaskKind {
    pub(crate) const ALL: [Self; 7] = [
        Self::Service,
        Self::Worker,
        Self::ScheduledDriver,
        Self::ScheduledRun,
        Self::BlockingReaper,
        Self::Shutdown,
        Self::Other,
    ];
    pub(crate) const COUNT: usize = Self::ALL.len();

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Service => 0,
            Self::Worker => 1,
            Self::ScheduledDriver => 2,
            Self::ScheduledRun => 3,
            Self::BlockingReaper => 4,
            Self::Shutdown => 5,
            Self::Other => 6,
        }
    }
}

/// Identifies the task state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskState {
    /// Represents the queued case.
    Queued,
    /// Represents the running case.
    Running,
    /// Represents the completed case.
    Completed,
    /// Represents the cancelled case.
    Cancelled,
    /// Represents the aborted case.
    Aborted,
    /// Represents the panicked case.
    Panicked,
    /// Represents the leaked case.
    Leaked,
}

/// Identifies the task result state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskResult {
    /// Represents the completed case.
    Completed,
    /// Represents the cancelled case.
    Cancelled,
    /// Represents the aborted case.
    Aborted,
    /// Represents the panicked case.
    Panicked,
}

/// Lifecycle state of a task group.
///
/// A shutdown moves a group through `Closing`, `Closed` and
/// `ShutdownCompleted` in that order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskGroupLifecycleState {
    /// Accepts tasks and child groups.
    Open,
    /// Admission is closed; cancellation has not yet reached every task.
    Closing,
    /// Admission is closed and cancellation was broadcast; owned work is draining.
    Closed,
    /// Shutdown finished and its report is published.
    ShutdownCompleted,
    /// A task panicked; the group rejects new work until it is shut down.
    Poisoned,
}

impl TaskGroupLifecycleState {
    /// Returns the failure reported to a submission rejected in this state.
    ///
    /// Callers use it only after observing a state other than `Open`.
    pub(crate) fn admission_error(self, operation: crate::RuntimeOperation) -> RuntimeError {
        match self {
            Self::Poisoned => RuntimeError::poisoned(operation),
            Self::Open | Self::Closing | Self::Closed | Self::ShutdownCompleted => RuntimeError::closed(operation),
        }
    }
}

/// Represents a task-group owner.
///
/// Cloning a task group keeps the same owner and cancellation token. Use
/// [`Self::try_child`] when work needs an independently cancellable owner whose
/// lifetime remains bounded by this parent.
///
/// Dropping a handle does not request cancellation or wait for cleanup.
/// Active descendants retain their ancestor ownership path. Call
/// [`Self::shutdown_until`] to close admission, cancel and await the subtree.
/// A task panic poisons its group against new submissions; it remains owned
/// and can still be shut down and reported.
#[derive(Debug, Clone)]
pub struct TaskGroup {
    inner: Arc<TaskGroupInner>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskKindDiagnostics {
    pub(crate) kind: TaskKind,
    pub(crate) active: usize,
    pub(crate) long_running: usize,
    pub(crate) max_elapsed: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskGroupDiagnostics {
    pub(crate) local_task_count: usize,
    pub(crate) group_count: usize,
    pub(crate) task_count: usize,
    pub(crate) task_kinds: Vec<TaskKindDiagnostics>,
}

/// Which population one bounded task detail was observed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskDetailScope {
    /// A task of the group the scan started from.
    Local,
    /// A task of one of its descendant groups.
    Subtree,
}

/// One bounded task detail.
///
/// Deliberately carries no task identifier, task name, or group name: a detail
/// list is served through an authenticated endpoint, and those values are
/// caller-provided labels that diagnostics must not disclose.
#[derive(Debug, Clone)]
pub(crate) struct TaskDetail {
    pub(crate) kind: TaskKind,
    pub(crate) scope: TaskDetailScope,
    pub(crate) elapsed: Duration,
}

/// The result of one bounded detail scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct TaskDetailScan {
    /// How many tasks were examined before the scan budget was reached.
    pub(crate) scanned: usize,
    /// The details that fit inside the output budget.
    pub(crate) details: Vec<TaskDetail>,
    /// Whether either budget stopped the scan short of every task.
    pub(crate) truncated: bool,
}

#[derive(Debug)]
struct TaskGroupInner {
    id: TaskGroupId,
    parent_id: Option<TaskGroupId>,
    name: Arc<str>,
    runtime: RuntimeHandle,
    cancellation_token: CancellationToken,
    tracker: TaskTracker,
    registry: Arc<ActiveTaskRegistry>,
    // A live descendant retains the entire ownership path. The parent's
    // registry points back weakly, so dropping an idle subtree releases it.
    parent: Option<Arc<TaskGroupInner>>,
    events: Arc<TaskGroupTreeEvents>,
    // Component requests this group answered with a closed group.
    closed_component_requests: AtomicUsize,
    next_task_id: AtomicU64,
    completed: AtomicUsize,
    cancelled: AtomicUsize,
    aborted: AtomicUsize,
    panicked: AtomicUsize,
    lifecycle: AtomicU8,
    spawn_gate: Mutex<()>,
    settlement_gate: Mutex<()>,
    shutdown: ShutdownCoordinator,
}

#[derive(Debug, Clone)]
struct TaskMeta {
    id: TaskId,
    name: TaskName,
    group_id: TaskGroupId,
    group_name: Arc<str>,
    kind: TaskKind,
    state: TaskState,
    started_at: Instant,
    abort_handle: Option<AbortHandle>,
    abort_requested: bool,
    completion: Arc<TaskCompletion>,
    /// Id of the operation that submitted the task, if any.
    operation: Option<u64>,
}

#[derive(Debug)]
struct TaskCompletion {
    done: AtomicU8,
    notify: Notify,
}

impl TaskCompletion {
    fn new() -> Self {
        Self {
            done: AtomicU8::new(0),
            notify: Notify::new(),
        }
    }

    fn mark_done(&self) {
        if self.done.swap(1, Ordering::AcqRel) == 0 {
            self.notify.notify_waiters();
        }
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire) != 0
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.is_done() {
                return;
            }
            notified.await;
        }
    }
}

impl TaskGroup {
    /// Creates the root group of a runtime scope tree.
    ///
    /// Only [`RootServiceContext::new`](crate::RootServiceContext) can create
    /// the permit, so every other group is a descendant of a root context.
    pub(crate) fn root(name: Arc<str>, runtime: RuntimeHandle, _permit: RootGroupPermit) -> Self {
        Self {
            inner: Arc::new(TaskGroupInner::new(
                TaskGroupId::next(),
                None,
                name,
                runtime,
                CancellationToken::new(),
                None,
                Arc::default(),
            )),
        }
    }

    /// Returns the id.
    pub fn id(&self) -> TaskGroupId {
        self.inner.id
    }

    pub(crate) fn runtime(&self) -> &RuntimeHandle {
        &self.inner.runtime
    }

    /// Returns the parent id.
    pub fn parent_id(&self) -> Option<TaskGroupId> {
        self.inner.parent_id
    }

    /// Returns the name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation_token.clone()
    }

    /// Returns the earliest absolute deadline installed by a shutdown owner.
    pub fn shutdown_deadline(&self) -> Option<ShutdownDeadline> {
        self.inner.shutdown.deadline()
    }

    /// Returns the lifecycle state.
    pub fn lifecycle_state(&self) -> TaskGroupLifecycleState {
        self.inner.lifecycle_state()
    }

    /// Returns the active count registered directly in this group.
    ///
    /// Descendants are excluded; subtree diagnostics aggregate them separately.
    pub fn task_count(&self) -> usize {
        self.inner.registry.tasks.len()
    }

    /// Returns the number of active component groups directly owned by this group.
    pub fn component_count(&self) -> usize {
        self.inner.registry.component_count()
    }

    /// Returns whether `task_id` was issued by this group, whether or not the
    /// task is still registered.
    pub fn owns_task(&self, task_id: TaskId) -> bool {
        task_id.group_id() == self.inner.id
    }

    /// Returns whether `task_id` is a registered task of this group.
    ///
    /// Returns `false` for an id issued by another group.
    pub fn contains_task(&self, task_id: TaskId) -> bool {
        self.inner.registry.tasks.contains_key(&task_id)
    }

    /// Holds the settlement gate, so a finished task stays registered until
    /// the guard is dropped.
    #[cfg(test)]
    pub(crate) fn lock_settlement_for_test(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.inner.settlement_gate.lock()
    }

    /// Returns the registered tasks of the operation with id `operation`.
    pub(crate) fn operation_task_ids(&self, operation: u64) -> Vec<TaskId> {
        self.inner
            .registry
            .tasks
            .iter()
            .filter(|entry| entry.operation == Some(operation))
            .map(|entry| entry.id)
            .collect()
    }

    /// Returns the counts of absorbed failures over this group's whole tree.
    pub fn event_counts(&self) -> TaskGroupEventCounts {
        let events = &self.inner.events;
        TaskGroupEventCounts {
            poisoned_groups: events.poisoned_groups.load(Ordering::Relaxed),
            closed_component_requests: events.closed_component_requests.load(Ordering::Relaxed),
        }
    }

    /// Creates a component, or a closed group once this owner stops admitting children.
    ///
    /// Callers keep a usable handle either way; the first closed answer per
    /// owner is logged and every one is counted in [`Self::event_counts`].
    pub(crate) fn component(&self, name: impl Into<Arc<str>>) -> Self {
        let name = name.into();
        self.try_child(name.clone()).unwrap_or_else(|_error| {
            self.inner
                .events
                .closed_component_requests
                .fetch_add(1, Ordering::Relaxed);
            if self.inner.closed_component_requests.fetch_add(1, Ordering::Relaxed) == 0 {
                tracing::warn!(
                    group = %self.inner.path(),
                    component = %name,
                    state = ?self.lifecycle_state(),
                    "component requested after its owner stopped admitting children; returning a closed group"
                );
            }
            self.closed_component(name)
        })
    }

    /// Creates an independently cancellable child owned by this task group.
    ///
    /// Parent cancellation propagates to the returned child. Cancelling the
    /// child does not cancel this parent or any sibling child. In contrast,
    /// [`Clone::clone`] keeps the same owner and cancellation token.
    ///
    /// # Errors
    ///
    /// Returns a [`crate::RuntimeErrorKind::Closed`] failure after this owner
    /// starts shutting down, or [`crate::RuntimeErrorKind::Poisoned`] after a
    /// task panic poisoned it.
    pub fn try_child(&self, name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        let name = name.into();
        let _spawn_guard = self.inner.spawn_gate.lock();
        let state = self.inner.lifecycle_state();
        if state != TaskGroupLifecycleState::Open {
            return Err(state.admission_error(crate::RuntimeOperation::CreateTaskGroupChild));
        }

        let child = self.open_component(name);
        self.inner
            .registry
            .register_component(child.id(), Arc::downgrade(&child.inner));
        Ok(child)
    }

    fn open_component(&self, name: Arc<str>) -> Self {
        Self {
            inner: Arc::new(TaskGroupInner::new(
                TaskGroupId::next(),
                Some(self.inner.id),
                name,
                self.inner.runtime.clone(),
                self.inner.cancellation_token.child_token(),
                Some(self.inner.clone()),
                Arc::clone(&self.inner.events),
            )),
        }
    }

    fn closed_component(&self, name: Arc<str>) -> Self {
        let child = self.open_component(name);
        child.inner.tracker.close();
        child.inner.cancellation_token.cancel();
        child.inner.lifecycle.store(STATE_SHUTDOWN_COMPLETED, Ordering::Release);
        child
    }

    /// Spawns the supplied task.
    pub fn spawn<F>(&self, name: impl Into<TaskName>, kind: TaskKind, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner(name.into(), kind, future)
    }

    /// Spawns a task whose panic is recorded as a critical failure.
    ///
    /// Registration is explicit and opt-in: a task spawned the ordinary way keeps
    /// today's behavior. A normal completion is not a failure here, because a
    /// worker or job may legitimately return; use [`Self::spawn_critical_service`]
    /// when returning before owner cancellation must be treated as one.
    ///
    /// # Errors
    ///
    /// Returns an error when this task group is shutting down or closed.
    pub fn spawn_critical<F>(
        &self,
        name: impl Into<TaskName>,
        kind: TaskKind,
        failures: CriticalFailureState,
        future: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let registration = CriticalRegistration {
            failures,
            task_kind: kind,
            expects_until_cancelled: false,
        };
        let (task_id, join_handle) =
            self.spawn_inner_with_handle(name.into(), kind, false, Some(registration), None, future)?;
        drop(join_handle);
        Ok(task_id)
    }

    /// Spawns a critical service that must run until its owner is cancelled.
    ///
    /// Both a panic and a return before owner cancellation are recorded as
    /// critical failures. Owner cancellation is an expected exit and records
    /// nothing, so an ordinary shutdown never triggers failure handling.
    ///
    /// # Errors
    ///
    /// Returns an error when this task group is shutting down or closed.
    pub fn spawn_critical_service<F>(
        &self,
        name: impl Into<TaskName>,
        failures: CriticalFailureState,
        future: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let registration = CriticalRegistration {
            failures,
            task_kind: TaskKind::Service,
            expects_until_cancelled: true,
        };
        let (task_id, join_handle) =
            self.spawn_inner_with_handle(name.into(), TaskKind::Service, false, Some(registration), None, future)?;
        drop(join_handle);
        Ok(task_id)
    }

    /// Spawns a service that owns its shutdown protocol.
    ///
    /// The service remains tracked during owner shutdown, but its future must
    /// observe an appropriate cancellation signal and perform any required
    /// ordered cleanup itself. Use [`Self::spawn_cancellable_service`] when it
    /// is safe to drop the service future as soon as its owner is cancelled.
    pub fn spawn_service<F>(&self, name: impl Into<TaskName>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn(name, TaskKind::Service, future)
    }

    /// Spawns a service that exits when either its future completes or its owner is cancelled.
    ///
    /// Owner cancellation drops `future`; services that require an ordered
    /// cleanup sequence must use [`Self::spawn_service`] and observe a
    /// cancellation token explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when this task group is shutting down or closed.
    pub fn spawn_cancellable_service<F>(&self, name: impl Into<TaskName>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if std::mem::size_of::<F>() > MAX_INLINE_TASK_FUTURE_SIZE {
            self.spawn_cancellable_service_inner(name.into(), Box::pin(future))
        } else {
            self.spawn_cancellable_service_inner(name.into(), future)
        }
    }

    fn spawn_cancellable_service_inner<F>(&self, name: TaskName, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let owner_cancellation = self.cancellation_token();
        self.spawn_service(name, async move {
            tokio::select! {
                biased;
                _ = owner_cancellation.cancelled() => {}
                _ = future => {}
            }
        })
    }

    /// Spawns work for a bounded operation under this fixed component owner.
    ///
    /// The operation context supplies task classification, cancellation, and
    /// an optional deadline without creating a child task group.
    pub fn spawn_operation<F>(
        &self,
        context: &OperationContext,
        name: impl Into<TaskName>,
        future: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_operation_with_cancellation(context, name.into(), future, Some(self.cancellation_token()))
    }

    /// Spawns accepted operation work that must drain during owner shutdown.
    ///
    /// Unlike [`Self::spawn_operation`], owner cancellation does not drop the
    /// supplied future. The operation context still enforces its own
    /// cancellation and deadline, and [`OperationContext::wait`] aborts work
    /// that outlives the caller's drain deadline. This is intended for work
    /// that has already been accepted and whose ordered shutdown protocol is
    /// owned by a tracked service.
    pub fn spawn_draining_operation<F>(
        &self,
        context: &OperationContext,
        name: impl Into<TaskName>,
        future: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_operation_with_cancellation(context, name.into(), future, None)
    }

    fn spawn_operation_with_cancellation<F>(
        &self,
        context: &OperationContext,
        name: TaskName,
        future: F,
        owner_cancellation: Option<CancellationToken>,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if std::mem::size_of::<F>() > MAX_INLINE_TASK_FUTURE_SIZE {
            self.spawn_operation_inner(context, name, Box::pin(future), owner_cancellation)
        } else {
            self.spawn_operation_inner(context, name, future, owner_cancellation)
        }
    }

    fn spawn_operation_inner<F>(
        &self,
        context: &OperationContext,
        name: TaskName,
        future: F,
        owner_cancellation: Option<CancellationToken>,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let registration = {
            let _operation_spawn_guard = context.spawn_guard();
            context.prepare_spawn(self.id())?
        };
        let guard = registration.guard();
        let execution = crate::operation::OperationExecution::new(future, guard, context.clone(), owner_cancellation);
        let task_id = self.spawn_operation_task(name, context.task_kind(), context.id(), execution.run())?;
        registration.finish_registration();
        Ok(task_id)
    }

    /// Spawns with handle.
    pub fn spawn_with_handle<F>(
        &self,
        name: impl Into<TaskName>,
        kind: TaskKind,
        future: F,
    ) -> RuntimeResult<(TaskId, tokio::task::JoinHandle<()>)>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner_with_handle(name.into(), kind, true, None, None, future)
    }

    /// Spawns service with handle.
    pub fn spawn_service_with_handle<F>(
        &self,
        name: impl Into<TaskName>,
        future: F,
    ) -> RuntimeResult<(TaskId, tokio::task::JoinHandle<()>)>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_with_handle(name, TaskKind::Service, future)
    }

    /// Signals cancellation to this group and its descendants without waiting.
    ///
    /// The signal does not close submission admission or confirm task
    /// destruction. Services must observe it and perform their own cleanup.
    /// Use [`Self::shutdown_until`] for the complete shutdown boundary.
    pub fn cancel(&self) {
        self.inner.cancellation_token.cancel();
    }

    /// Requests cancellation of an active task without waiting for destruction.
    ///
    /// Returns whether the task was still registered. Its record and resources
    /// remain owned until the executor destroys the future. An id issued by
    /// another group is never registered here, so it returns `false`.
    pub fn abort_task(&self, task_id: TaskId) -> bool {
        self.abort_task_inner(task_id).is_some()
    }

    /// Requests cancellation and waits until the user future is destroyed.
    ///
    /// Returns `false` if the task was already absent, was issued by another
    /// group, or the wait expired. A timeout does not remove the task's record
    /// or confirm cancellation.
    pub async fn abort_task_and_wait(&self, task_id: TaskId, timeout: Duration) -> bool {
        let Some(completion) = self.abort_task_inner(task_id) else {
            return false;
        };

        if completion.is_done() {
            return true;
        }

        if timeout.is_zero() {
            return false;
        }

        tokio::time::timeout(timeout, completion.wait()).await.is_ok()
    }

    /// Asynchronously waits for a local task's future to be destroyed.
    ///
    /// Returns `true` when this group's task is already absent, including after
    /// earlier completion, and `false` when a registered task outlives the
    /// timeout. Unlike [`Self::abort_task_and_wait`], absence is treated as
    /// success. An id issued by another group returns `false` immediately:
    /// this group cannot tell whether that task finished; see
    /// [`Self::owns_task`]. This does not request cancellation or prove
    /// business-level success.
    pub async fn wait_task(&self, task_id: TaskId, timeout: Duration) -> bool {
        if !self.owns_task(task_id) {
            return false;
        }
        let Some(completion) = self
            .inner
            .registry
            .tasks
            .get(&task_id)
            .map(|meta| meta.completion.clone())
        else {
            return true;
        };

        if completion.is_done() {
            return true;
        }

        if timeout.is_zero() {
            return false;
        }

        tokio::time::timeout(timeout, completion.wait()).await.is_ok()
    }

    /// Closes and asynchronously drains this subtree within a relative budget.
    ///
    /// The budget is converted once to an absolute deadline. See
    /// [`Self::shutdown_until`] for repeated calls and completion semantics.
    pub fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, ShutdownReport> {
        self.shutdown_until(ShutdownDeadline::after(timeout))
    }

    /// Closes admission, signals cancellation and asynchronously awaits owned work.
    ///
    /// All children share the absolute deadline. A later call can tighten but
    /// cannot extend it. Tasks that do not finish in time are asked to abort;
    /// the report distinguishes confirmed destruction from remaining work.
    /// Once published, the report is retained and returned by subsequent calls.
    pub fn shutdown_until(&self, deadline: ShutdownDeadline) -> BoxFuture<'_, ShutdownReport> {
        self.tighten_shutdown_deadline(deadline);
        async move {
            let report = self
                .inner
                .shutdown
                .report
                .get_or_init(|| async { self.shutdown_inner().await })
                .await;
            self.inner.shutdown.report_ready.notify_waiters();
            report.clone()
        }
        .boxed()
    }

    /// Requests immediate shutdown without waiting for future destruction.
    ///
    /// The retained report may contain unconfirmed remaining work even when
    /// cancellation completes shortly afterward. A later graceful call returns
    /// that same report; it does not turn this call into an awaited drain.
    pub fn shutdown_now(&self) -> ShutdownReport {
        if let Some(report) = self.inner.shutdown.report.get() {
            return report.clone();
        }

        let report = self.shutdown_now_inner();
        let _ = self.inner.shutdown.report.set(report.clone());
        self.inner.shutdown.report_ready.notify_waiters();
        report
    }

    fn tighten_shutdown_deadline(&self, deadline: ShutdownDeadline) {
        let mut pending = vec![(self.clone(), deadline)];
        while let Some((group, deadline)) = pending.pop() {
            let installed = group.inner.shutdown.tighten(deadline);
            pending.extend(
                group
                    .inner
                    .registry
                    .components_snapshot()
                    .into_iter()
                    .map(|child| (child, installed)),
            );
        }
    }

    async fn shutdown_inner(&self) -> ShutdownReport {
        let started_at = Instant::now();
        let deadline = self
            .inner
            .shutdown
            .deadline()
            .unwrap_or_else(|| ShutdownDeadline::after(Duration::ZERO));
        // Retain the entire accepted subtree before propagating cancellation.
        // A fast-finishing leaf must not erase its ancestors and outcomes while
        // shutdown is still walking the tree.
        let descendants = self.close_admission_tree();
        // Reapply after sealing admission, including children created between
        // the initial deadline traversal and the admission traversal.
        self.tighten_shutdown_deadline(deadline);
        self.inner.cancellation_token.cancel();
        self.mark_closed(&descendants);
        // Poll every scope at one level. A scope observes child reports via
        // notifications rather than recursively polling their shutdown futures.
        let children = join_all(descendants.iter().map(|group| async move {
            group.inner.shutdown.report.get_or_init(|| group.shutdown_one()).await;
            group.inner.shutdown.report_ready.notify_waiters();
        }));
        let (_, mut report) = tokio::join!(children, self.shutdown_one());
        report.elapsed = started_at.elapsed();
        report
    }

    async fn shutdown_one(&self) -> ShutdownReport {
        let started_at = Instant::now();
        let children = self.inner.registry.components_snapshot();

        let child_reports = async {
            join_all(
                children
                    .into_iter()
                    .map(|child| async move { child.inner.shutdown.wait_report().await.clone() }),
            )
            .await
        };
        // On a deadline, returns the abort count seen just before this
        // shutdown aborted the remaining tasks.
        let tracked_shutdown = async {
            if self.inner.shutdown.run_until(self.inner.tracker.wait()).await.is_err() {
                let aborted_before_timeout = self.inner.aborted.load(Ordering::Acquire);
                self.abort_tracked_tasks();
                let _ = self.inner.shutdown.run_until(self.inner.tracker.wait()).await;
                Some(aborted_before_timeout)
            } else {
                None
            }
        };

        let (child_reports, aborted_before_timeout) = tokio::join!(child_reports, tracked_shutdown);

        let mut report = ShutdownReport::new(self.inner.name.to_string(), started_at.elapsed());
        report.children = child_reports;
        self.record_task_outcomes(&mut report);

        if let Some(aborted_before_timeout) = aborted_before_timeout {
            // Aborts requested earlier through `abort_task` are not timeouts.
            let aborted_by_timeout = report.aborted.saturating_sub(aborted_before_timeout);
            if aborted_by_timeout > 0 {
                report.annotations.push(ShutdownAnnotation::new(format!(
                    "confirmed cancellation of {aborted_by_timeout} aborted tasks"
                )));
            }
            report.timed_out = aborted_by_timeout + report.leaked;
        }

        self.inner.lifecycle.store(STATE_SHUTDOWN_COMPLETED, Ordering::Release);
        report
    }

    fn close_admission_tree(&self) -> Vec<TaskGroup> {
        let mut retained = Vec::new();
        let mut pending = vec![self.clone()];
        while let Some(group) = pending.pop() {
            {
                let _spawn_guard = group.inner.spawn_gate.lock();
                // Admission is sealed under the spawn gate. `Closed` follows
                // once cancellation has reached the whole tree.
                let _ = group
                    .inner
                    .lifecycle
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                        matches!(state, STATE_OPEN | STATE_POISONED).then_some(STATE_CLOSING)
                    });
                group.inner.tracker.close();
                pending.extend(group.inner.registry.components_snapshot());
            }
            if group.id() != self.id() {
                retained.push(group);
            }
        }
        // Descendants precede parents for immediate report assembly.
        retained.reverse();
        retained
    }

    /// Moves the groups closed by this shutdown from `Closing` to `Closed`.
    ///
    /// Called after cancellation was broadcast, so a group observed as
    /// `Closed` has a cancelled token.
    fn mark_closed(&self, descendants: &[TaskGroup]) {
        for group in descendants.iter().chain(std::iter::once(self)) {
            let _ = group.inner.lifecycle.compare_exchange(
                STATE_CLOSING,
                STATE_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    fn shutdown_now_inner(&self) -> ShutdownReport {
        let started_at = Instant::now();
        let descendants = self.close_admission_tree();
        self.inner.cancellation_token.cancel();
        self.mark_closed(&descendants);
        let mut reports = std::collections::HashMap::new();
        for group in &descendants {
            let children = group
                .inner
                .registry
                .components_snapshot()
                .into_iter()
                .filter_map(|child| reports.remove(&child.id()))
                .collect();
            let report = if let Some(report) = group.inner.shutdown.report.get() {
                report.clone()
            } else {
                let report = group.shutdown_now_one(children);
                let _ = group.inner.shutdown.report.set(report.clone());
                group.inner.shutdown.report_ready.notify_waiters();
                report
            };
            reports.insert(group.id(), report);
        }
        let children = self
            .inner
            .registry
            .components_snapshot()
            .into_iter()
            .filter_map(|child| reports.remove(&child.id()))
            .collect();
        let mut report = self.shutdown_now_one(children);
        report.elapsed = started_at.elapsed();
        report
    }

    fn shutdown_now_one(&self, child_reports: Vec<ShutdownReport>) -> ShutdownReport {
        let started_at = Instant::now();

        let aborted_before = self.inner.aborted.load(Ordering::Acquire);
        self.abort_tracked_tasks();

        let mut report = ShutdownReport::new(self.inner.name.to_string(), started_at.elapsed());
        report.children = child_reports;
        self.record_task_outcomes(&mut report);

        let aborted_now = report.aborted.saturating_sub(aborted_before);
        if aborted_now > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "confirmed cancellation of {aborted_now} aborted tasks"
            )));
        }

        self.inner.lifecycle.store(STATE_SHUTDOWN_COMPLETED, Ordering::Release);
        report
    }

    fn abort_tracked_tasks(&self) {
        for mut entry in self.inner.registry.tasks.iter_mut() {
            entry.abort_requested = true;
            if let Some(abort_handle) = &entry.abort_handle {
                abort_handle.abort();
            }
        }
    }

    fn record_task_outcomes(&self, report: &mut ShutdownReport) {
        // Snapshot counters and active records together: a concurrent finalizer
        // must not make a task disappear between these two observations.
        let _settlement = self.inner.settlement_gate.lock();
        report.completed = self.inner.completed.load(Ordering::Relaxed);
        report.cancelled = self.inner.cancelled.load(Ordering::Relaxed);
        report.panicked = self.inner.panicked.load(Ordering::Relaxed);
        report.aborted = self.inner.aborted.load(Ordering::Relaxed);
        let closed_components = self.inner.closed_component_requests.load(Ordering::Relaxed);
        if closed_components > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "{closed_components} component requests after admission closed received closed groups"
            )));
        }
        let mut requested = 0;
        for entry in self.inner.registry.tasks.iter() {
            requested += usize::from(entry.abort_requested);
            report.leaked += 1;
            report.push_remaining_task(entry.snapshot(TaskState::Leaked));
        }
        if requested > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "abort requested for {requested} tasks whose destruction is not yet confirmed"
            )));
        }
    }
}

impl TaskGroupInner {
    fn new(
        id: TaskGroupId,
        parent_id: Option<TaskGroupId>,
        name: Arc<str>,
        runtime: RuntimeHandle,
        cancellation_token: CancellationToken,
        parent: Option<Arc<TaskGroupInner>>,
        events: Arc<TaskGroupTreeEvents>,
    ) -> Self {
        Self {
            id,
            parent_id,
            name,
            runtime,
            cancellation_token,
            tracker: TaskTracker::new(),
            registry: Arc::new(ActiveTaskRegistry::new()),
            parent,
            events,
            closed_component_requests: AtomicUsize::new(0),
            next_task_id: AtomicU64::new(1),
            completed: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
            panicked: AtomicUsize::new(0),
            lifecycle: AtomicU8::new(STATE_OPEN),
            spawn_gate: Mutex::new(()),
            settlement_gate: Mutex::new(()),
            shutdown: ShutdownCoordinator::new(),
        }
    }

    fn lifecycle_state(&self) -> TaskGroupLifecycleState {
        match self.lifecycle.load(Ordering::Acquire) {
            STATE_OPEN => TaskGroupLifecycleState::Open,
            STATE_CLOSING => TaskGroupLifecycleState::Closing,
            STATE_CLOSED => TaskGroupLifecycleState::Closed,
            STATE_SHUTDOWN_COMPLETED => TaskGroupLifecycleState::ShutdownCompleted,
            _ => TaskGroupLifecycleState::Poisoned,
        }
    }

    fn mark_poisoned_if_open(&self) {
        if self
            .lifecycle
            .compare_exchange(STATE_OPEN, STATE_POISONED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.events.poisoned_groups.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                group = %self.path(),
                "task group poisoned by a panicking task; it no longer admits work"
            );
        }
    }

    /// Returns the names from the root to this group, separated by `/`.
    fn path(&self) -> String {
        let mut names = vec![self.name.as_ref()];
        let mut parent = self.parent.as_deref();
        while let Some(group) = parent {
            names.push(group.name.as_ref());
            parent = group.parent.as_deref();
        }
        names.reverse();
        names.join("/")
    }

    fn finish_task(&self, task_id: TaskId, result: TaskResult) {
        let _settlement = self.settlement_gate.lock();
        let Entry::Occupied(entry) = self.registry.tasks.entry(task_id) else {
            return;
        };

        match result {
            TaskResult::Completed => {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
            TaskResult::Cancelled => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            TaskResult::Panicked => {
                self.panicked.fetch_add(1, Ordering::Relaxed);
                self.mark_poisoned_if_open();
            }
            TaskResult::Aborted => {
                self.aborted.fetch_add(1, Ordering::Relaxed);
            }
        }
        // A missing record is also treated as finished by wait_task. Publish
        // the counters before removing it, after user resources were dropped.
        entry.remove();
    }
}

impl Drop for TaskGroupInner {
    fn drop(&mut self) {
        let mut parent = self.parent.take();
        let mut child_id = self.id;
        while let Some(ancestor) = parent {
            ancestor.registry.unregister_component(child_id);
            let Some(mut ancestor) = Arc::into_inner(ancestor) else {
                break;
            };
            child_id = ancestor.id;
            parent = ancestor.parent.take();
            // Its parent is now detached, so dropping this owned node does
            // not recursively destroy the rest of a long ownership chain.
        }
    }
}

impl TaskMeta {
    fn snapshot(&self, override_state: TaskState) -> TaskSnapshot {
        TaskSnapshot {
            id: self.id,
            name: self.name.to_string(),
            group_id: self.group_id,
            group_name: self.group_name.to_string(),
            kind: self.kind,
            state: override_state,
            elapsed: self.started_at.elapsed(),
        }
    }
}
