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
use std::sync::Arc;

use futures::FutureExt;

use crate::critical::CriticalFailureKind;
use crate::critical::CriticalRegistration;

use super::TaskCompletion;
use super::TaskGroupInner;
use super::TaskId;
use super::TaskResult;

// Before the first poll, struct field order destroys the user future before
// its finalizer. Once polled, run() keeps the same order: its combinator holds
// the future ahead of the closure that owns the finalizer. This keeps the
// existing inline/large-future allocation boundary intact.
pub(super) struct TaskExecution<F> {
    future: F,
    finalizer: TaskFinalizer,
    propagate_panic: bool,
}

impl<F: Future<Output = ()>> TaskExecution<F> {
    pub(super) fn new(
        future: F,
        inner: Arc<TaskGroupInner>,
        task_id: TaskId,
        completion: Arc<TaskCompletion>,
        propagate_panic: bool,
        critical: Option<CriticalRegistration>,
    ) -> Self {
        Self {
            future,
            finalizer: TaskFinalizer {
                inner,
                task_id,
                completion,
                result: TaskResult::Aborted,
                critical,
            },
            propagate_panic,
        }
    }

    /// Runs the user future and records its result in the finalizer.
    ///
    /// This is a combinator rather than an `async fn`: a coroutine keeps its argument as well
    /// as the locals moved out of it, which made the wrapper of a 464-byte request future
    /// 1,584 bytes, and Tokio copies a task's future several times. `Map` drops the finished
    /// future before it calls the closure, and drops the future before the closure when the
    /// task is dropped unfinished, so the finalizer always runs after the user future is gone.
    pub(super) fn run(self) -> impl Future<Output = ()> {
        let Self {
            future,
            finalizer,
            propagate_panic,
        } = self;
        AssertUnwindSafe(future).catch_unwind().map(move |result| {
            // Its `Drop` publishes the result when the closure returns or unwinds.
            let mut finalizer = finalizer;
            // Record the observed result once, independently of subsequent aborts.
            match result {
                Ok(()) => {
                    if finalizer.inner.cancellation_token.is_cancelled() {
                        finalizer.result = TaskResult::Cancelled;
                    } else {
                        finalizer.result = TaskResult::Completed;
                    }
                }
                Err(error) => {
                    finalizer.result = TaskResult::Panicked;
                    tracing::error!(task_id = finalizer.task_id.as_u64(), "task panicked");
                    if propagate_panic {
                        std::panic::resume_unwind(error);
                    }
                }
            }
        })
    }
}

struct TaskFinalizer {
    inner: Arc<TaskGroupInner>,
    task_id: TaskId,
    completion: Arc<TaskCompletion>,
    result: TaskResult,
    critical: Option<CriticalRegistration>,
}

impl Drop for TaskFinalizer {
    fn drop(&mut self) {
        // Also classify a panic in the user future's destructor, outside the
        // catch_unwind that surrounds polling. No user callback runs here.
        let result = if std::thread::panicking() {
            TaskResult::Panicked
        } else {
            self.result
        };
        if let Some(critical) = &self.critical {
            let failure = match result {
                TaskResult::Panicked => Some(CriticalFailureKind::Panicked),
                TaskResult::Completed if critical.expects_until_cancelled => {
                    Some(CriticalFailureKind::ExitedUnexpectedly)
                }
                TaskResult::Completed | TaskResult::Cancelled | TaskResult::Aborted => None,
            };
            if let Some(kind) = failure {
                critical.failures.record(kind, critical.task_kind);
            }
        }
        self.inner.finish_task(self.task_id, result);
        self.completion.mark_done();
    }
}
