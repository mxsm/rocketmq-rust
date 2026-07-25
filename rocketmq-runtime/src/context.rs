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
use crate::blocking::BlockingLane;
use crate::blocking::BlockingLanePolicies;
use crate::blocking::BlockingPoolPolicy;
use crate::diagnostics::RuntimeDiagnostics;
use crate::diagnostics::RuntimeDiagnosticsSnapshot;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::service_context::ChildServiceContext;
use crate::service_context::RootServiceContext;
use crate::service_context::ScopeId;
use crate::shutdown_deadline::ShutdownDeadline;
use crate::shutdown_report::ShutdownReport;
use crate::task_group::TaskGroup;

/// Test and migration harness for borrowing an already-running Tokio runtime.
///
/// Production composition roots must use [`crate::RuntimeOwner`]. Production
/// libraries must receive [`ChildServiceContext`] instead of constructing this
/// harness or discovering the current Tokio runtime.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RuntimeContext {
    root: Arc<RootServiceContext>,
}

impl RuntimeContext {
    #[doc(hidden)]
    pub fn new(runtime: RuntimeHandle, name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        Self::new_with_blocking_lanes(runtime, name, BlockingLanePolicies::default())
    }

    #[doc(hidden)]
    pub fn new_with_blocking_policy(
        runtime: RuntimeHandle,
        name: impl Into<Arc<str>>,
        blocking_policy: BlockingPoolPolicy,
    ) -> RuntimeResult<Self> {
        Self::new_with_blocking_lanes(runtime, name, BlockingLanePolicies::uniform(blocking_policy))
    }

    pub(crate) fn new_with_blocking_lanes(
        runtime: RuntimeHandle,
        name: impl Into<Arc<str>>,
        blocking_policies: BlockingLanePolicies,
    ) -> RuntimeResult<Self> {
        let name = name.into();
        let root_group = TaskGroup::root(name.clone(), runtime.clone());
        let diagnostics = RuntimeDiagnostics::new(runtime.clone());
        let root = RootServiceContext::new(name, runtime, root_group, blocking_policies, diagnostics)?;
        Ok(Self { root: Arc::new(root) })
    }

    #[doc(hidden)]
    pub fn try_from_current(name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_error| RuntimeError::NoCurrentRuntime)?;
        Self::new(RuntimeHandle::new(handle), name)
    }

    #[doc(hidden)]
    pub fn from_current(name: impl Into<Arc<str>>) -> Self {
        Self::try_from_current(name).expect("current Tokio runtime must be available for test harness")
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        self.root.runtime()
    }

    pub fn root_group(&self) -> &TaskGroup {
        self.root.task_group()
    }

    pub fn blocking(&self, lane: BlockingLane) -> &BlockingExecutor {
        self.root.blocking(lane)
    }

    pub fn diagnostics(&self) -> &RuntimeDiagnostics {
        self.root.diagnostics()
    }

    pub fn diagnostics_snapshot(&self) -> RuntimeDiagnosticsSnapshot {
        self.root.diagnostics_snapshot()
    }

    pub fn service_context(&self, scope: impl Into<ScopeId>) -> ChildServiceContext {
        self.root.child(scope)
    }

    pub async fn shutdown_tasks(&self, timeout: Duration) -> ShutdownReport {
        self.shutdown_tasks_until(ShutdownDeadline::after(timeout)).await
    }

    pub async fn shutdown_tasks_until(&self, deadline: ShutdownDeadline) -> ShutdownReport {
        let mut report = self.root.task_group().shutdown_until(deadline).await;
        for snapshot in self.root.blocking_snapshots() {
            report.merge_blocking(snapshot);
        }
        report
    }

    pub fn shutdown_tasks_now(&self) -> ShutdownReport {
        let mut report = self.root.task_group().shutdown_now();
        for snapshot in self.root.blocking_snapshots() {
            report.merge_blocking(snapshot);
        }
        report
    }
}
