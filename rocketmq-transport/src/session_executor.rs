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
use std::sync::Arc;

use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::RuntimeResult;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupChildLease;
use rocketmq_runtime::TaskId;
use rocketmq_runtime::TaskKind;

use crate::admission::AdmissionClass;
use crate::admission::AdmissionController;
use crate::admission::AdmissionError;
use crate::admission::AdmissionResource;
use crate::admission::AdmissionScope;
use crate::request_ordering::RequestOrdering;
use crate::request_ordering::RequestSequencer;

/// Error returned before a request can enter its session-owned execution task.
#[derive(Debug)]
pub(crate) enum SessionDispatchError {
    Admission(AdmissionError),
    Closing(RuntimeError),
}

impl fmt::Display for SessionDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(formatter),
            Self::Closing(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SessionDispatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Closing(error) => Some(error),
        }
    }
}

impl From<AdmissionError> for SessionDispatchError {
    fn from(error: AdmissionError) -> Self {
        Self::Admission(error)
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
/// processor permit, and runs under a dynamically registered child task group.
pub(crate) struct SessionExecutor {
    admission: Arc<AdmissionController>,
    scope: AdmissionScope,
    request_group: TaskGroup,
    _request_group_lease: TaskGroupChildLease,
    sequencer: RequestSequencer,
    accepting: AtomicBool,
}

impl SessionExecutor {
    pub(crate) fn try_new(
        session_group: &TaskGroup,
        admission: Arc<AdmissionController>,
        scope: AdmissionScope,
    ) -> RuntimeResult<Self> {
        let request_group_lease = session_group.try_child_lease("rocketmq.transport.session.request-executor")?;
        Ok(Self {
            admission,
            scope,
            request_group: request_group_lease.group().clone(),
            _request_group_lease: request_group_lease,
            sequencer: RequestSequencer::default(),
            accepting: AtomicBool::new(true),
        })
    }

    pub(crate) fn try_execute<F, Fut, R, Rejected>(
        &self,
        retained_bytes: usize,
        class: AdmissionClass,
        ordering: RequestOrdering,
        execute: F,
        reject: R,
    ) -> Result<TaskId, SessionDispatchError>
    where
        F: FnOnce(TaskGroup) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        R: FnOnce(TaskGroup, AdmissionError) -> Rejected + Send + 'static,
        Rejected: Future<Output = ()> + Send + 'static,
    {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(SessionDispatchError::Closing(RuntimeError::TaskGroupClosing {
                group_id: self.request_group.id(),
                group_name: self.request_group.name().into(),
            }));
        }
        let request_lease = self
            .request_group
            .try_child_lease("rocketmq.transport.session.request")?;
        let queued = self
            .admission
            .try_acquire(AdmissionResource::Queued, self.scope, retained_bytes, class)?;
        let inflight = self
            .admission
            .try_acquire(AdmissionResource::Inflight, self.scope, retained_bytes, class)?;
        let admission = self.admission.clone();
        let scope = self.scope;
        let sequencer = self.sequencer.clone();
        let request_group = request_lease.group().clone();
        let spawn_group = request_group.clone();
        spawn_group
            .spawn("rocketmq.transport.session.request", TaskKind::Worker, async move {
                let _request_lease = request_lease;
                let ordering_guard = sequencer.acquire(ordering).await;
                let processor = match admission.try_acquire(AdmissionResource::Processor, scope, retained_bytes, class)
                {
                    Ok(processor) => processor,
                    Err(error) => {
                        drop(ordering_guard);
                        drop(queued);
                        reject(request_group, error).await;
                        return;
                    }
                };
                drop(queued);
                let _inflight = inflight;
                let _processor = processor;
                let _ordering_guard = ordering_guard;
                execute(request_group).await;
            })
            .map_err(SessionDispatchError::Closing)
    }

    fn stop_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    pub(crate) fn task_group(&self) -> &TaskGroup {
        &self.request_group
    }

    pub(crate) async fn drain_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        self.stop_admission();
        self.request_group.shutdown_until(deadline).await
    }
}
