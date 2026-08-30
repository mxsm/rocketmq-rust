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

use std::fmt;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;

use rocketmq_runtime::OperationContext;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionError;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScopeHandle;
use crate::admission::PartialFramePermit;
use crate::dispatch::deferred_resume::DeferredResumeSubmitError;
use crate::dispatch::deferred_resume::ResumeJobCell;
use crate::request_ordering::RequestOrdering;
use crate::request_ordering::RequestSequencer;

/// Error returned before a request can enter its session-owned execution task.
#[derive(Debug)]
pub(crate) enum SessionDispatchError {
    Admission {
        error: AdmissionError,
        retained_partial: Option<Box<PartialFramePermit>>,
    },
    Closing(RuntimeError),
}

impl fmt::Display for SessionDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission { error, .. } => error.fmt(formatter),
            Self::Closing(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SessionDispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission { error, .. } => Some(error),
            Self::Closing(error) => Some(error),
        }
    }
}

impl From<AdmissionError> for SessionDispatchError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission {
            error,
            retained_partial: None,
        }
    }
}

impl From<RuntimeError> for SessionDispatchError {
    fn from(error: RuntimeError) -> Self {
        Self::Closing(error)
    }
}

/// Owns bounded request tasks for one transport session.
///
/// A request first reserves queued and in-flight capacity. Its task then waits
/// for any declared ordering predecessor, converts queued capacity into a
/// processor permit, and runs under the session's bounded operation context.
pub(crate) struct SessionExecutor {
    inner: Arc<SessionExecutorInner>,
}

struct SessionExecutorInner {
    admission: AdmissionScopeHandle,
    request_group: TaskGroup,
    operation: OperationContext,
    sequencer: RequestSequencer,
    accepting: AtomicBool,
    task_counts: Arc<SessionTaskCounts>,
    #[cfg(test)]
    close_resume_operation_before_spawn: AtomicBool,
}

impl SessionExecutor {
    pub(crate) fn try_new(session_group: &TaskGroup, admission: AdmissionScopeHandle) -> RuntimeResult<Self> {
        Ok(Self {
            inner: Arc::new(SessionExecutorInner {
                admission,
                request_group: session_group.clone(),
                operation: OperationContext::without_deadline(TaskKind::Worker),
                sequencer: RequestSequencer::default(),
                accepting: AtomicBool::new(true),
                task_counts: Arc::new(SessionTaskCounts::default()),
                #[cfg(test)]
                close_resume_operation_before_spawn: AtomicBool::new(false),
            }),
        })
    }

    pub(crate) fn try_execute<F, Fut, R, Rejected>(
        &self,
        retained_bytes: usize,
        class: AdmissionClass,
        partial_frame: Option<PartialFramePermit>,
        ordering: RequestOrdering,
        execute: F,
        reject: R,
    ) -> Result<TaskId, SessionDispatchError>
    where
        F: FnOnce(OperationContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        R: FnOnce(OperationContext, AdmissionError) -> Rejected + Send + 'static,
        Rejected: Future<Output = ()> + Send + 'static,
    {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(SessionDispatchError::Closing(RuntimeError::TaskGroupClosing {
                group_id: self.inner.request_group.id(),
                group_name: self.inner.request_group.name().into(),
            }));
        }
        let queued = match self
            .inner
            .admission
            .try_acquire(AdmissionResource::Queued, retained_bytes, class)
        {
            Ok(queued) => queued,
            Err(error) => {
                return Err(SessionDispatchError::Admission {
                    error,
                    retained_partial: partial_frame.map(Box::new),
                });
            }
        };
        let inflight = match partial_frame {
            Some(partial_frame) => {
                match partial_frame.try_rebind(&self.inner.admission, AdmissionResource::Inflight, class) {
                    Ok(inflight) => inflight,
                    Err((retained_partial, error)) => {
                        return Err(SessionDispatchError::Admission {
                            error,
                            retained_partial: Some(retained_partial),
                        });
                    }
                }
            }
            None => self
                .inner
                .admission
                .try_acquire(AdmissionResource::Inflight, retained_bytes, class)?,
        };
        let admission = self.inner.admission.clone();
        let sequencer = self.inner.sequencer.clone();
        let request_operation = self.inner.operation.clone();
        let request_operation_for_task = request_operation.clone();
        let spawn_group = self.inner.request_group.clone();
        let task_count = SessionTaskCountGuard::inline(Arc::clone(&self.inner.task_counts));
        spawn_group
            .spawn_draining_operation(&request_operation, "rocketmq.transport.session.request", async move {
                let _task_count = task_count;
                let ordering_guard = sequencer.acquire(ordering).await;
                let processor = match admission.try_acquire(AdmissionResource::Processor, retained_bytes, class) {
                    Ok(processor) => processor,
                    Err(error) => {
                        drop(ordering_guard);
                        drop(queued);
                        reject(request_operation_for_task, error).await;
                        return;
                    }
                };
                drop(queued);
                let _inflight = inflight;
                let _processor = processor;
                let _ordering_guard = ordering_guard;
                execute(request_operation_for_task).await;
            })
            .map_err(SessionDispatchError::Closing)
    }

    fn stop_admission(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.operation.close_admission();
    }

    pub(crate) fn begin_close(&self) {
        self.stop_admission();
    }

    pub(crate) fn operation_context(&self) -> &OperationContext {
        &self.inner.operation
    }

    pub(crate) fn deferred_resume_executor(&self) -> DeferredResumeExecutor {
        DeferredResumeExecutor {
            inner: Arc::downgrade(&self.inner),
        }
    }

    #[cfg(test)]
    pub(crate) fn close_resume_operation_before_spawn_for_test(&self) {
        self.inner
            .close_resume_operation_before_spawn
            .store(true, Ordering::Release);
    }

    pub(crate) async fn drain_report_until(&self, deadline: ShutdownDeadline) -> SessionExecutorDrainReport {
        let started_at = Instant::now();
        self.stop_admission();
        let active_inline_tasks = self.inner.task_counts.inline.load(Ordering::Acquire);
        let active_resume_tasks = self.inner.task_counts.resume.load(Ordering::Acquire);
        let active_before = active_inline_tasks.saturating_add(active_resume_tasks);
        let joined = self
            .inner
            .operation
            .wait(&self.inner.request_group, deadline.remaining())
            .await
            .unwrap_or(false);
        let mut report = ShutdownReport::new("rocketmq.transport.session.requests", started_at.elapsed());
        if joined {
            report.completed = active_before;
        } else {
            report.aborted = active_before;
            report.timed_out = usize::from(active_before > 0);
        }
        SessionExecutorDrainReport {
            shutdown: report,
            active_inline_tasks,
            active_resume_tasks,
            remaining_inline_tasks: self.inner.task_counts.inline.load(Ordering::Acquire),
            remaining_resume_tasks: self.inner.task_counts.resume.load(Ordering::Acquire),
        }
    }

    pub(crate) async fn drain_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        self.drain_report_until(deadline).await.shutdown
    }
}

#[derive(Debug)]
pub(crate) struct SessionExecutorDrainReport {
    pub(crate) shutdown: ShutdownReport,
    pub(crate) active_inline_tasks: usize,
    pub(crate) active_resume_tasks: usize,
    pub(crate) remaining_inline_tasks: usize,
    pub(crate) remaining_resume_tasks: usize,
}

impl SessionExecutorDrainReport {
    pub(crate) fn is_healthy(&self) -> bool {
        self.shutdown.is_healthy() && self.remaining_inline_tasks == 0 && self.remaining_resume_tasks == 0
    }
}

#[derive(Default)]
struct SessionTaskCounts {
    inline: AtomicUsize,
    resume: AtomicUsize,
}

enum SessionTaskCountKind {
    Inline,
    Resume,
}

struct SessionTaskCountGuard {
    counts: Arc<SessionTaskCounts>,
    kind: SessionTaskCountKind,
}

impl SessionTaskCountGuard {
    fn inline(counts: Arc<SessionTaskCounts>) -> Self {
        counts.inline.fetch_add(1, Ordering::AcqRel);
        Self {
            counts,
            kind: SessionTaskCountKind::Inline,
        }
    }

    fn resume(counts: Arc<SessionTaskCounts>) -> Self {
        counts.resume.fetch_add(1, Ordering::AcqRel);
        Self {
            counts,
            kind: SessionTaskCountKind::Resume,
        }
    }
}

impl Drop for SessionTaskCountGuard {
    fn drop(&mut self) {
        let counter = match self.kind {
            SessionTaskCountKind::Inline => &self.counts.inline,
            SessionTaskCountKind::Resume => &self.counts.resume,
        };
        let previous = counter.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "session task count guard owns one active count");
    }
}

#[derive(Clone)]
pub(crate) struct DeferredResumeExecutor {
    inner: Weak<SessionExecutorInner>,
}

impl DeferredResumeExecutor {
    #[cfg(test)]
    pub(crate) fn retired() -> Self {
        Self { inner: Weak::new() }
    }

    pub(crate) fn try_execute_resume(&self, cell: Arc<ResumeJobCell>) -> Result<TaskId, DeferredResumeSubmitError> {
        let Some(inner) = self.inner.upgrade() else {
            return Err(DeferredResumeSubmitError::Closing {
                source: RuntimeError::LifecycleOperation {
                    operation: "deferred_resume.upgrade_session_executor",
                    message: "session executor retired".to_owned(),
                },
                cell,
            });
        };
        if !inner.accepting.load(Ordering::Acquire) {
            return Err(DeferredResumeSubmitError::Closing {
                source: RuntimeError::TaskGroupClosing {
                    group_id: inner.request_group.id(),
                    group_name: inner.request_group.name().into(),
                },
                cell,
            });
        }
        let retained_bytes = cell.retained_bytes();
        let class = cell.class();
        let ordering = cell.ordering();
        let queued = inner
            .admission
            .try_acquire(AdmissionResource::Queued, retained_bytes, class)
            .map_err(|error| DeferredResumeSubmitError::Admission {
                error,
                cell: Arc::clone(&cell),
            })?;
        let inflight = inner
            .admission
            .try_acquire(AdmissionResource::Inflight, retained_bytes, class)
            .map_err(|error| DeferredResumeSubmitError::Admission {
                error,
                cell: Arc::clone(&cell),
            })?;
        let admission = inner.admission.clone();
        let sequencer = inner.sequencer.clone();
        let operation = inner.operation.clone();
        let operation_for_task = operation.clone();
        let spawn_group = inner.request_group.clone();
        let task_cell = Arc::clone(&cell);
        let task_count = SessionTaskCountGuard::resume(Arc::clone(&inner.task_counts));
        #[cfg(test)]
        if inner.close_resume_operation_before_spawn.swap(false, Ordering::AcqRel) {
            operation.close_admission();
        }
        spawn_group
            .spawn_draining_operation(&operation, "rocketmq.transport.session.deferred-resume", async move {
                let _task_count = task_count;
                #[cfg(test)]
                task_cell.wait_first_poll_gate().await;
                let Some(job) = task_cell.take() else {
                    return;
                };
                #[cfg(test)]
                job.notify_before_ordering();
                if let Some(stop) = job.current_before_resume() {
                    drop(queued);
                    drop(inflight);
                    job.finish_stopped(stop);
                    return;
                }
                let ordering_result = {
                    let ordering_wait = sequencer.acquire(ordering);
                    let stop_wait = job.wait_before_resume();
                    tokio::pin!(ordering_wait);
                    tokio::pin!(stop_wait);
                    tokio::select! {
                        biased;
                        stop = &mut stop_wait => Err(stop),
                        ordering_guard = &mut ordering_wait => Ok(ordering_guard),
                    }
                };
                let ordering_guard = match ordering_result {
                    Ok(ordering_guard) => ordering_guard,
                    Err(stop) => {
                        drop(queued);
                        drop(inflight);
                        job.finish_stopped(stop);
                        return;
                    }
                };
                if let Some(stop) = job.current_before_resume() {
                    drop(ordering_guard);
                    drop(queued);
                    drop(inflight);
                    job.finish_stopped(stop);
                    return;
                }
                let processor = match admission.try_acquire(AdmissionResource::Processor, retained_bytes, class) {
                    Ok(processor) => processor,
                    Err(error) => {
                        drop(ordering_guard);
                        drop(queued);
                        job.reject(error).await;
                        return;
                    }
                };
                drop(queued);
                let _inflight = inflight;
                let _processor = processor;
                let _ordering_guard = ordering_guard;
                job.execute(operation_for_task).await;
            })
            .map_err(|source| DeferredResumeSubmitError::Closing { source, cell })
    }
}
