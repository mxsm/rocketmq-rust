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

//! Task registration and submission under the owner's admission gate.

use super::completion::TaskExecution;
use super::{
    DetachedTaskPolicy, TaskCompletion, TaskGroup, TaskGroupLifecycleState, TaskId, TaskKind, TaskMeta, TaskState,
    MAX_INLINE_TASK_FUTURE_SIZE,
};
use crate::critical::CriticalRegistration;
use crate::RuntimeError;
use crate::RuntimeResult;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

impl TaskGroup {
    pub(super) fn spawn_inner<F>(
        &self,
        name: Arc<str>,
        kind: TaskKind,
        detached_policy: Option<DetachedTaskPolicy>,
        future: F,
    ) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (task_id, join_handle) = self.spawn_inner_with_handle(name, kind, detached_policy, false, None, future)?;
        drop(join_handle);
        Ok(task_id)
    }

    pub(super) fn spawn_inner_with_handle<F>(
        &self,
        name: Arc<str>,
        kind: TaskKind,
        detached_policy: Option<DetachedTaskPolicy>,
        propagate_panic: bool,
        critical: Option<CriticalRegistration>,
        future: F,
    ) -> RuntimeResult<(TaskId, tokio::task::JoinHandle<()>)>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if std::mem::size_of::<F>() > MAX_INLINE_TASK_FUTURE_SIZE {
            self.spawn_registered(name, kind, detached_policy, propagate_panic, critical, Box::pin(future))
        } else {
            self.spawn_registered(name, kind, detached_policy, propagate_panic, critical, future)
        }
    }

    pub(super) fn spawn_registered<F>(
        &self,
        name: Arc<str>,
        kind: TaskKind,
        detached_policy: Option<DetachedTaskPolicy>,
        propagate_panic: bool,
        critical: Option<CriticalRegistration>,
        future: F,
    ) -> RuntimeResult<(TaskId, tokio::task::JoinHandle<()>)>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let _spawn_guard = self.inner.spawn_gate.lock();
        if self.inner.lifecycle_state() != TaskGroupLifecycleState::Open {
            return Err(RuntimeError::context_unavailable(
                crate::RuntimeOperation::SpawnTaskGroupTask,
            ));
        }

        let task_id = TaskId(self.inner.next_task_id.fetch_add(1, Ordering::Relaxed));
        let completion = Arc::new(TaskCompletion::new());
        self.inner.registry.tasks.insert(
            task_id,
            TaskMeta {
                id: task_id,
                name: name.clone(),
                group_id: self.inner.id,
                group_name: self.inner.name.clone(),
                kind,
                state: TaskState::Queued,
                started_at: Instant::now(),
                detached: detached_policy.is_some(),
                detached_policy,
                abort_handle: None,
                abort_requested: false,
                completion: completion.clone(),
            },
        );

        let wrapped = TaskExecution::new(
            future,
            self.inner.clone(),
            task_id,
            completion,
            propagate_panic,
            critical,
        )
        .run();

        let join_handle = if detached_policy.is_some() {
            self.inner.runtime.spawn_owned(wrapped)
        } else {
            self.inner.tracker.spawn_on(wrapped, self.inner.runtime.tokio_handle())
        };
        let abort_handle = join_handle.abort_handle();

        let abort_requested = if let Some(mut meta) = self.inner.registry.tasks.get_mut(&task_id) {
            meta.abort_handle = Some(abort_handle);
            meta.state = TaskState::Running;
            meta.abort_requested
        } else {
            false
        };
        // A diagnostic reader can discover the registered ID before this
        // handle is installed. Honor any cancellation requested in that gap.
        if abort_requested {
            join_handle.abort();
        }

        Ok((task_id, join_handle))
    }

    pub(super) fn abort_task_inner(&self, task_id: TaskId) -> Option<Arc<TaskCompletion>> {
        let (abort_handle, completion) = {
            let mut meta = self.inner.registry.tasks.get_mut(&task_id)?;
            meta.abort_requested = true;
            (meta.abort_handle.clone(), meta.completion.clone())
        };
        if let Some(abort_handle) = abort_handle {
            abort_handle.abort();
        }
        Some(completion)
    }
}
