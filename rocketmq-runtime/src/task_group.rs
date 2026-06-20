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
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::future::FutureExt;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::shutdown_report::ShutdownAnnotation;
use crate::shutdown_report::ShutdownReport;
use crate::shutdown_report::TaskSnapshot;

const STATE_OPEN: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct TaskId(u64);

impl TaskId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct TaskGroupId(u64);

impl TaskGroupId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskKind {
    Service,
    Worker,
    ScheduledDriver,
    ScheduledRun,
    BlockingReaper,
    Shutdown,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskState {
    Queued,
    Running,
    Completed,
    Cancelled,
    Aborted,
    Panicked,
    Leaked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskResult {
    Completed,
    Cancelled,
    Aborted,
    Panicked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetachedTaskPolicy {
    TrackOnly,
    AbortOnShutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskGroupLifecycleState {
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone)]
pub struct TaskGroup {
    inner: Arc<TaskGroupInner>,
}

#[derive(Debug)]
struct TaskGroupInner {
    id: TaskGroupId,
    parent_id: Option<TaskGroupId>,
    name: Arc<str>,
    runtime: RuntimeHandle,
    cancellation_token: CancellationToken,
    tracker: TaskTracker,
    tasks: DashMap<TaskId, TaskMeta>,
    children: Mutex<Vec<TaskGroup>>,
    next_task_id: AtomicU64,
    next_child_id: AtomicU64,
    completed: AtomicUsize,
    cancelled: AtomicUsize,
    panicked: AtomicUsize,
    lifecycle: AtomicU8,
    spawn_gate: Mutex<()>,
    shutdown_report: tokio::sync::OnceCell<ShutdownReport>,
}

#[derive(Debug, Clone)]
struct TaskMeta {
    id: TaskId,
    name: Arc<str>,
    group_id: TaskGroupId,
    group_name: Arc<str>,
    kind: TaskKind,
    state: TaskState,
    started_at: Instant,
    detached: bool,
    abort_handle: Option<AbortHandle>,
}

impl TaskGroup {
    pub fn root(name: impl Into<Arc<str>>, runtime: RuntimeHandle) -> Self {
        Self {
            inner: Arc::new(TaskGroupInner::new(
                TaskGroupId(1),
                None,
                name.into(),
                runtime,
                CancellationToken::new(),
            )),
        }
    }

    pub fn id(&self) -> TaskGroupId {
        self.inner.id
    }

    pub fn parent_id(&self) -> Option<TaskGroupId> {
        self.inner.parent_id
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.inner.runtime
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation_token.clone()
    }

    pub fn lifecycle_state(&self) -> TaskGroupLifecycleState {
        self.inner.lifecycle_state()
    }

    pub fn task_count(&self) -> usize {
        self.inner.tasks.len()
    }

    pub fn child(&self, name: impl Into<Arc<str>>) -> Self {
        let child_id = TaskGroupId(self.inner.next_child_id.fetch_add(1, Ordering::Relaxed));
        let child = Self {
            inner: Arc::new(TaskGroupInner::new(
                child_id,
                Some(self.inner.id),
                name.into(),
                self.inner.runtime.clone(),
                self.inner.cancellation_token.child_token(),
            )),
        };
        self.inner.children.lock().push(child.clone());
        child
    }

    pub fn spawn<F>(&self, name: impl Into<Arc<str>>, kind: TaskKind, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner(name.into(), kind, false, future)
    }

    pub fn spawn_service<F>(&self, name: impl Into<Arc<str>>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn(name, TaskKind::Service, future)
    }

    pub fn spawn_detached<F>(&self, name: impl Into<Arc<str>>, kind: TaskKind, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_inner(name.into(), kind, true, future)
    }

    pub fn cancel(&self) {
        self.inner.cancellation_token.cancel();
    }

    pub fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, ShutdownReport> {
        async move {
            self.inner
                .shutdown_report
                .get_or_init(|| async { self.shutdown_inner(timeout).await })
                .await
                .clone()
        }
        .boxed()
    }

    pub fn shutdown_now(&self) -> ShutdownReport {
        if let Some(report) = self.inner.shutdown_report.get() {
            return report.clone();
        }

        let report = self.shutdown_now_inner();
        let _ = self.inner.shutdown_report.set(report.clone());
        report
    }

    fn spawn_inner<F>(&self, name: Arc<str>, kind: TaskKind, detached: bool, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let _spawn_guard = self.inner.spawn_gate.lock();
        if self.inner.lifecycle_state() != TaskGroupLifecycleState::Open {
            return Err(RuntimeError::TaskGroupClosing {
                group_id: self.inner.id,
                group_name: self.inner.name.clone(),
            });
        }

        let task_id = TaskId(self.inner.next_task_id.fetch_add(1, Ordering::Relaxed));
        self.inner.tasks.insert(
            task_id,
            TaskMeta {
                id: task_id,
                name: name.clone(),
                group_id: self.inner.id,
                group_name: self.inner.name.clone(),
                kind,
                state: TaskState::Queued,
                started_at: Instant::now(),
                detached,
                abort_handle: None,
            },
        );

        let inner = self.inner.clone();
        let token = inner.cancellation_token.clone();
        let wrapped = async move {
            let result = AssertUnwindSafe(future).catch_unwind().await;
            let task_result = match result {
                Ok(()) if token.is_cancelled() => TaskResult::Cancelled,
                Ok(()) => TaskResult::Completed,
                Err(error) => {
                    tracing::error!(task_id = task_id.as_u64(), ?error, "task panicked");
                    TaskResult::Panicked
                }
            };
            inner.finish_task(task_id, task_result);
        };

        let join_handle = if detached {
            self.inner.runtime.spawn(wrapped)
        } else {
            self.inner.tracker.spawn_on(wrapped, self.inner.runtime.inner())
        };
        let abort_handle = join_handle.abort_handle();
        drop(join_handle);

        if let Some(mut meta) = self.inner.tasks.get_mut(&task_id) {
            meta.abort_handle = Some(abort_handle);
            meta.state = TaskState::Running;
        }

        Ok(task_id)
    }

    async fn shutdown_inner(&self, timeout: Duration) -> ShutdownReport {
        let started_at = Instant::now();
        let children = self.inner.children.lock().clone();

        {
            let _spawn_guard = self.inner.spawn_gate.lock();
            self.inner.lifecycle.store(STATE_CLOSING, Ordering::Release);
            self.inner.tracker.close();
            self.inner.cancellation_token.cancel();
        }

        let mut child_reports = Vec::with_capacity(children.len());
        for child in children {
            child_reports.push(child.shutdown(timeout).await);
        }

        let mut timed_out = false;
        if tokio::time::timeout(timeout, self.inner.tracker.wait()).await.is_err() {
            timed_out = true;
            self.abort_tracked_tasks();
            let drain_timeout = timeout.min(Duration::from_secs(1));
            let _ = tokio::time::timeout(drain_timeout, self.inner.tracker.wait()).await;
        }

        let mut report = ShutdownReport::new(self.inner.name.to_string(), started_at.elapsed());
        report.completed = self.inner.completed.load(Ordering::Relaxed);
        report.cancelled = self.inner.cancelled.load(Ordering::Relaxed);
        report.panicked = self.inner.panicked.load(Ordering::Relaxed);
        report.children = child_reports;

        let aborted = self.remove_aborted_tasks();
        report.aborted = aborted;
        if aborted > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "aborted {aborted} tracked tasks after shutdown timeout"
            )));
        }

        let remaining = self.remaining_snapshots(TaskState::Leaked);
        report.detached_still_running = remaining.iter().filter(|task| task.detached).count();
        report.leaked = remaining.iter().filter(|task| !task.detached).count();
        report.remaining_tasks = remaining;
        if timed_out {
            report.timed_out = aborted + report.leaked;
        }

        if report.detached_still_running > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "{} detached tasks are still running",
                report.detached_still_running
            )));
        }

        self.inner.lifecycle.store(STATE_CLOSED, Ordering::Release);
        report
    }

    fn shutdown_now_inner(&self) -> ShutdownReport {
        let started_at = Instant::now();
        let children = self.inner.children.lock().clone();

        {
            let _spawn_guard = self.inner.spawn_gate.lock();
            self.inner.lifecycle.store(STATE_CLOSING, Ordering::Release);
            self.inner.tracker.close();
            self.inner.cancellation_token.cancel();
        }

        let mut child_reports = Vec::with_capacity(children.len());
        for child in children {
            child_reports.push(child.shutdown_now());
        }

        self.abort_tracked_tasks();

        let mut report = ShutdownReport::new(self.inner.name.to_string(), started_at.elapsed());
        report.completed = self.inner.completed.load(Ordering::Relaxed);
        report.cancelled = self.inner.cancelled.load(Ordering::Relaxed);
        report.panicked = self.inner.panicked.load(Ordering::Relaxed);
        report.children = child_reports;

        let aborted = self.remove_aborted_tasks();
        report.aborted = aborted;
        if aborted > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "aborted {aborted} tracked tasks during immediate shutdown"
            )));
        }

        let remaining = self.remaining_snapshots(TaskState::Leaked);
        report.detached_still_running = remaining.iter().filter(|task| task.detached).count();
        report.leaked = remaining.iter().filter(|task| !task.detached).count();
        report.remaining_tasks = remaining;

        if report.detached_still_running > 0 {
            report.annotations.push(ShutdownAnnotation::new(format!(
                "{} detached tasks are still running",
                report.detached_still_running
            )));
        }

        self.inner.lifecycle.store(STATE_CLOSED, Ordering::Release);
        report
    }

    fn abort_tracked_tasks(&self) {
        for mut entry in self.inner.tasks.iter_mut() {
            if entry.detached {
                continue;
            }
            entry.state = TaskState::Aborted;
            if let Some(abort_handle) = &entry.abort_handle {
                abort_handle.abort();
            }
        }
    }

    fn remove_aborted_tasks(&self) -> usize {
        let aborted_ids = self
            .inner
            .tasks
            .iter()
            .filter_map(|entry| (!entry.detached && entry.state == TaskState::Aborted).then_some(*entry.key()))
            .collect::<Vec<_>>();

        for task_id in &aborted_ids {
            self.inner.tasks.remove(task_id);
        }

        aborted_ids.len()
    }

    fn remaining_snapshots(&self, state: TaskState) -> Vec<TaskSnapshot> {
        self.inner
            .tasks
            .iter()
            .map(|entry| entry.value().snapshot(state))
            .collect()
    }
}

impl TaskGroupInner {
    fn new(
        id: TaskGroupId,
        parent_id: Option<TaskGroupId>,
        name: Arc<str>,
        runtime: RuntimeHandle,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            id,
            parent_id,
            name,
            runtime,
            cancellation_token,
            tracker: TaskTracker::new(),
            tasks: DashMap::new(),
            children: Mutex::new(Vec::new()),
            next_task_id: AtomicU64::new(1),
            next_child_id: AtomicU64::new(id.as_u64() * 1_000_000 + 1),
            completed: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            panicked: AtomicUsize::new(0),
            lifecycle: AtomicU8::new(STATE_OPEN),
            spawn_gate: Mutex::new(()),
            shutdown_report: tokio::sync::OnceCell::new(),
        }
    }

    fn lifecycle_state(&self) -> TaskGroupLifecycleState {
        match self.lifecycle.load(Ordering::Acquire) {
            STATE_OPEN => TaskGroupLifecycleState::Open,
            STATE_CLOSING => TaskGroupLifecycleState::Closing,
            _ => TaskGroupLifecycleState::Closed,
        }
    }

    fn finish_task(&self, task_id: TaskId, result: TaskResult) {
        match result {
            TaskResult::Completed => {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
            TaskResult::Cancelled => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            TaskResult::Panicked => {
                self.panicked.fetch_add(1, Ordering::Relaxed);
            }
            TaskResult::Aborted => {}
        }

        self.tasks.remove(&task_id);
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
            detached: self.detached,
        }
    }
}
