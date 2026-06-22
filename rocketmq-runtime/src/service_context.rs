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
use std::sync::Arc;

use crate::blocking::BlockingExecutor;
use crate::diagnostics::RuntimeDiagnostics;
use crate::diagnostics::RuntimeDiagnosticsSnapshot;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::scheduled::ScheduledTaskGroup;
use crate::task_group::TaskGroup;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;

#[derive(Debug, Clone)]
pub struct ServiceContext {
    name: Arc<str>,
    runtime: RuntimeHandle,
    task_group: TaskGroup,
    blocking: BlockingExecutor,
    diagnostics: RuntimeDiagnostics,
}

impl ServiceContext {
    pub fn new(
        name: Arc<str>,
        runtime: RuntimeHandle,
        task_group: TaskGroup,
        blocking: BlockingExecutor,
        diagnostics: RuntimeDiagnostics,
    ) -> Self {
        Self {
            name,
            runtime,
            task_group,
            blocking,
            diagnostics,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    pub fn blocking(&self) -> &BlockingExecutor {
        &self.blocking
    }

    pub fn diagnostics(&self) -> &RuntimeDiagnostics {
        &self.diagnostics
    }

    pub fn diagnostics_snapshot(&self) -> RuntimeDiagnosticsSnapshot {
        self.diagnostics.snapshot(&self.task_group, &self.blocking)
    }

    pub fn child(&self, name: impl Into<Arc<str>>) -> Self {
        let name = name.into();
        Self {
            name: name.clone(),
            runtime: self.runtime.clone(),
            task_group: self.task_group.child(name),
            blocking: self.blocking.clone(),
            diagnostics: self.diagnostics.clone(),
        }
    }

    pub fn scheduled_tasks(&self, name: impl Into<Arc<str>>) -> ScheduledTaskGroup {
        ScheduledTaskGroup::new(self.task_group.child(name))
    }

    pub fn spawn<F>(&self, name: impl Into<Arc<str>>, kind: TaskKind, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn(name, kind, future)
    }

    pub fn spawn_service<F>(&self, name: impl Into<Arc<str>>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn_service(name, future)
    }
}
