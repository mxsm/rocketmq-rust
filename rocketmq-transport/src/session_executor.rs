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
use std::sync::atomic::Ordering;
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
    admission: AdmissionScopeHandle,
    request_group: TaskGroup,
    operation: OperationContext,
    sequencer: RequestSequencer,
    accepting: AtomicBool,
}

impl SessionExecutor {
    pub(crate) fn try_new(session_group: &TaskGroup, admission: AdmissionScopeHandle) -> RuntimeResult<Self> {
        Ok(Self {
            admission,
            request_group: session_group.clone(),
            operation: OperationContext::without_deadline(TaskKind::Worker),
            sequencer: RequestSequencer::default(),
            accepting: AtomicBool::new(true),
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
        if !self.accepting.load(Ordering::Acquire) {
            return Err(SessionDispatchError::Closing(RuntimeError::TaskGroupClosing {
                group_id: self.request_group.id(),
                group_name: self.request_group.name().into(),
            }));
        }
        let queued = match self
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
                match partial_frame.try_rebind(&self.admission, AdmissionResource::Inflight, class) {
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
                .admission
                .try_acquire(AdmissionResource::Inflight, retained_bytes, class)?,
        };
        let admission = self.admission.clone();
        let sequencer = self.sequencer.clone();
        let request_operation = self.operation.clone();
        let request_operation_for_task = request_operation.clone();
        let spawn_group = self.request_group.clone();
        spawn_group
            .spawn_draining_operation(&request_operation, "rocketmq.transport.session.request", async move {
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
        self.accepting.store(false, Ordering::Release);
        self.operation.close_admission();
    }

    pub(crate) fn operation_context(&self) -> &OperationContext {
        &self.operation
    }

    pub(crate) async fn drain_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        let started_at = Instant::now();
        self.stop_admission();
        let active_before = self.operation.active_task_count();
        let joined = self
            .operation
            .wait(&self.request_group, deadline.remaining())
            .await
            .unwrap_or(false);
        let mut report = ShutdownReport::new("rocketmq.transport.session.requests", started_at.elapsed());
        if joined {
            report.completed = active_before;
        } else {
            report.aborted = active_before;
            report.timed_out = usize::from(active_before > 0);
        }
        report
    }
}
