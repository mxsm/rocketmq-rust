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

use std::sync::Arc;
use std::time::Duration;

use crate::blocking::BlockingExecutor;
use crate::blocking::BlockingPoolPolicy;
use crate::diagnostics::RuntimeDiagnostics;
use crate::diagnostics::RuntimeDiagnosticsSnapshot;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::service_context::ServiceContext;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskGroup;

#[derive(Debug, Clone)]
pub struct RuntimeContext {
    runtime: RuntimeHandle,
    root_group: TaskGroup,
    blocking: BlockingExecutor,
    diagnostics: RuntimeDiagnostics,
}

impl RuntimeContext {
    pub fn new(runtime: RuntimeHandle, name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        Self::new_with_blocking_policy(runtime, name, BlockingPoolPolicy::default())
    }

    pub fn new_with_blocking_policy(
        runtime: RuntimeHandle,
        name: impl Into<Arc<str>>,
        blocking_policy: BlockingPoolPolicy,
    ) -> RuntimeResult<Self> {
        let root_group = TaskGroup::root(name, runtime.clone());
        let blocking = BlockingExecutor::new(blocking_policy, root_group.child("runtime.blocking-reaper"))?;
        let diagnostics = RuntimeDiagnostics::new(runtime.clone());
        Ok(Self {
            runtime,
            root_group,
            blocking,
            diagnostics,
        })
    }

    pub fn try_from_current(name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_error| RuntimeError::NoCurrentRuntime)?;
        Self::new(RuntimeHandle::new(handle), name)
    }

    pub fn from_current(name: impl Into<Arc<str>>) -> Self {
        Self::try_from_current(name).expect("current Tokio runtime must be available")
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn root_group(&self) -> &TaskGroup {
        &self.root_group
    }

    pub fn blocking(&self) -> &BlockingExecutor {
        &self.blocking
    }

    pub fn diagnostics(&self) -> &RuntimeDiagnostics {
        &self.diagnostics
    }

    pub fn diagnostics_snapshot(&self) -> RuntimeDiagnosticsSnapshot {
        self.diagnostics.snapshot(&self.root_group, &self.blocking)
    }

    pub fn service_context(&self, name: impl Into<Arc<str>>) -> ServiceContext {
        let name = name.into();
        ServiceContext::new(
            name.clone(),
            self.runtime.clone(),
            self.root_group.child(name),
            self.blocking.clone(),
            self.diagnostics.clone(),
        )
    }

    pub async fn shutdown_tasks(&self, timeout: Duration) -> ShutdownReport {
        let mut report = self.root_group.shutdown(timeout).await;
        report.merge_blocking(self.blocking.snapshot());
        report
    }

    pub fn shutdown_tasks_now(&self) -> ShutdownReport {
        let mut report = self.root_group.shutdown_now();
        report.merge_blocking(self.blocking.snapshot());
        report
    }
}
