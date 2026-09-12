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

use super::TaskCompletion;
use super::TaskGroupInner;
use super::TaskId;
use super::TaskResult;

// Before the first poll, struct field order destroys the user future before
// its finalizer. Once polled, run() establishes the same order for its locals.
// This keeps the existing inline/large-future allocation boundary intact.
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
    ) -> Self {
        Self {
            future,
            finalizer: TaskFinalizer {
                inner,
                task_id,
                completion,
                result: TaskResult::Aborted,
            },
            propagate_panic,
        }
    }

    pub(super) async fn run(self) {
        let mut finalizer = self.finalizer;
        let result = AssertUnwindSafe(self.future).catch_unwind().await;
        // The awaited future has been destroyed at this statement boundary.
        // Record the observed result once, independently of subsequent aborts.
        match result {
            Ok(()) => {
                finalizer.result = if finalizer.inner.cancellation_token.is_cancelled() {
                    TaskResult::Cancelled
                } else {
                    TaskResult::Completed
                };
            }
            Err(error) => {
                finalizer.result = TaskResult::Panicked;
                tracing::error!(task_id = finalizer.task_id.as_u64(), "task panicked");
                if self.propagate_panic {
                    std::panic::resume_unwind(error);
                }
            }
        }
    }
}

struct TaskFinalizer {
    inner: Arc<TaskGroupInner>,
    task_id: TaskId,
    completion: Arc<TaskCompletion>,
    result: TaskResult,
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
        self.inner.finish_task(self.task_id, result);
        self.completion.mark_done();
    }
}
