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
use crate::blocking::BlockingLane;
use crate::blocking::BlockingLanePolicies;
use crate::blocking::GlobalBlockingBudget;
use crate::diagnostics::RuntimeDiagnostics;
use crate::diagnostics::RuntimeDiagnosticsSnapshot;
use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::handle::RuntimeHandle;
use crate::scheduled::ScheduledTaskGroup;
use crate::task_group::TaskGroup;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;
use crate::task_spawner::TaskSpawner;

/// A validated, named position in the service ownership tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScopeId(Arc<str>);

impl ScopeId {
    /// Creates a scope identifier.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] when `name` is empty or only
    /// contains whitespace.
    pub fn new(name: impl Into<Arc<str>>) -> RuntimeResult<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(RuntimeError::InvalidConfig(
                "service context scope name must not be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Creates a statically named scope.
    ///
    /// # Panics
    ///
    /// Panics when `name` is empty or only contains whitespace. Static scope
    /// names are programmer-owned lifecycle invariants.
    pub fn from_static(name: &'static str) -> Self {
        Self::new(Arc::<str>::from(name)).expect("static service context scope name must be valid")
    }

    /// Borrows this value as str.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn into_inner(self) -> Arc<str> {
        self.0
    }
}

impl From<&'static str> for ScopeId {
    fn from(value: &'static str) -> Self {
        Self::from_static(value)
    }
}

impl From<String> for ScopeId {
    fn from(value: String) -> Self {
        Self::new(Arc::<str>::from(value)).expect("service context scope name must be valid")
    }
}

impl From<Arc<str>> for ScopeId {
    fn from(value: Arc<str>) -> Self {
        Self::new(value).expect("service context scope name must be valid")
    }
}

#[derive(Debug, Clone)]
struct BlockingLanes {
    storage_io: BlockingExecutor,
    metadata_io: BlockingExecutor,
    cpu_crypto: BlockingExecutor,
}

impl BlockingLanes {
    fn new(policies: BlockingLanePolicies, global_capacity: usize) -> RuntimeResult<Self> {
        let budget = GlobalBlockingBudget::managed(global_capacity, &policies)?;
        Ok(Self {
            storage_io: BlockingExecutor::new_managed(policies.storage_io, BlockingLane::StorageIo, budget.clone())?,
            metadata_io: BlockingExecutor::new_managed(policies.metadata_io, BlockingLane::MetadataIo, budget.clone())?,
            cpu_crypto: BlockingExecutor::new_managed(policies.cpu_crypto, BlockingLane::CpuCrypto, budget)?,
        })
    }

    fn get(&self, lane: BlockingLane) -> &BlockingExecutor {
        match lane {
            BlockingLane::StorageIo => &self.storage_io,
            BlockingLane::MetadataIo => &self.metadata_io,
            BlockingLane::CpuCrypto => &self.cpu_crypto,
        }
    }

    fn snapshots(&self) -> Vec<crate::blocking::BlockingExecutorSnapshot> {
        vec![
            self.storage_io.snapshot(),
            self.metadata_io.snapshot(),
            self.cpu_crypto.snapshot(),
        ]
    }
}

/// The unique, non-cloneable lifecycle root owned by [`crate::RuntimeOwner`].
///
/// The type has no public constructor and cannot be promoted from a child
/// context. Composition roots only use it to derive the first named child.
#[derive(Debug)]
pub struct RootServiceContext {
    name: Arc<str>,
    runtime: RuntimeHandle,
    task_group: TaskGroup,
    blocking_lanes: BlockingLanes,
    diagnostics: RuntimeDiagnostics,
    _sealed: RootContextSeal,
}

#[derive(Debug)]
struct RootContextSeal;

impl RootServiceContext {
    pub(crate) fn new(
        name: Arc<str>,
        runtime: RuntimeHandle,
        task_group: TaskGroup,
        blocking_policies: BlockingLanePolicies,
        global_blocking_capacity: usize,
        diagnostics: RuntimeDiagnostics,
    ) -> RuntimeResult<Self> {
        let blocking_lanes = BlockingLanes::new(blocking_policies, global_blocking_capacity)?;
        Ok(Self {
            name,
            runtime,
            task_group,
            blocking_lanes,
            diagnostics,
            _sealed: RootContextSeal,
        })
    }

    /// Returns the name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the child.
    pub fn child(&self, scope: impl Into<ScopeId>) -> ChildServiceContext {
        ChildServiceContext::new(
            scope.into(),
            &self.task_group,
            self.blocking_lanes.clone(),
            self.diagnostics.clone(),
        )
    }

    /// Returns the diagnostics snapshot.
    pub fn diagnostics_snapshot(&self) -> RuntimeDiagnosticsSnapshot {
        self.diagnostics
            .snapshot(&self.task_group, self.blocking_lanes.snapshots())
    }

    pub fn diagnostics_view_v1(
        &self,
        component: crate::diagnostics::RuntimeComponent,
    ) -> crate::diagnostics::RuntimeDiagnosticsViewV1 {
        self.diagnostics
            .view_v1(component, &self.task_group, self.blocking_lanes.snapshots())
    }

    pub(crate) fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    pub(crate) fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub(crate) fn blocking(&self, lane: BlockingLane) -> &BlockingExecutor {
        self.blocking_lanes.get(lane)
    }

    pub(crate) fn diagnostics(&self) -> &RuntimeDiagnostics {
        &self.diagnostics
    }

    pub(crate) fn blocking_snapshots(&self) -> Vec<crate::blocking::BlockingExecutorSnapshot> {
        self.blocking_lanes.snapshots()
    }
}

/// A sealed, cloneable descendant of a [`RootServiceContext`].
///
/// Libraries receive this type or a capability derived from it. They cannot
/// construct it from a Tokio handle, create a root task group, or promote it
/// back to a root.
#[derive(Debug, Clone)]
pub struct ChildServiceContext {
    name: Arc<str>,
    task_group: TaskGroup,
    blocking_lanes: BlockingLanes,
    diagnostics: RuntimeDiagnostics,
    _sealed: Arc<ChildContextSeal>,
}

#[derive(Debug)]
struct ChildContextSeal;

impl ChildServiceContext {
    fn new(
        scope: ScopeId,
        parent_group: &TaskGroup,
        blocking_lanes: BlockingLanes,
        diagnostics: RuntimeDiagnostics,
    ) -> Self {
        let name = scope.into_inner();
        Self {
            name: name.clone(),
            task_group: parent_group.child(name),
            blocking_lanes,
            diagnostics,
            _sealed: Arc::new(ChildContextSeal),
        }
    }

    /// Returns the name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the task spawner.
    pub fn task_spawner(&self) -> TaskSpawner {
        TaskSpawner::new(self.task_group.clone())
    }

    /// Returns the task group.
    pub fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    /// Returns the blocking.
    pub fn blocking(&self, lane: BlockingLane) -> &BlockingExecutor {
        self.blocking_lanes.get(lane)
    }

    /// Returns the storage io.
    pub fn storage_io(&self) -> &BlockingExecutor {
        self.blocking(BlockingLane::StorageIo)
    }

    /// Returns the metadata io.
    pub fn metadata_io(&self) -> &BlockingExecutor {
        self.blocking(BlockingLane::MetadataIo)
    }

    /// Returns the cpu crypto.
    pub fn cpu_crypto(&self) -> &BlockingExecutor {
        self.blocking(BlockingLane::CpuCrypto)
    }

    /// Returns the diagnostics.
    pub fn diagnostics(&self) -> &RuntimeDiagnostics {
        &self.diagnostics
    }

    /// Returns the diagnostics snapshot.
    pub fn diagnostics_snapshot(&self) -> RuntimeDiagnosticsSnapshot {
        self.diagnostics
            .snapshot(&self.task_group, self.blocking_lanes.snapshots())
    }

    pub fn diagnostics_view_v1(
        &self,
        component: crate::diagnostics::RuntimeComponent,
    ) -> crate::diagnostics::RuntimeDiagnosticsViewV1 {
        self.diagnostics
            .view_v1(component, &self.task_group, self.blocking_lanes.snapshots())
    }

    /// Returns the child.
    pub fn child(&self, scope: impl Into<ScopeId>) -> Self {
        Self::new(
            scope.into(),
            &self.task_group,
            self.blocking_lanes.clone(),
            self.diagnostics.clone(),
        )
    }

    /// Returns the scheduled tasks.
    pub fn scheduled_tasks(&self, scope: impl Into<ScopeId>) -> ScheduledTaskGroup {
        let scope = scope.into().into_inner();
        ScheduledTaskGroup::new(self.task_group.child(scope))
    }

    /// Spawns the supplied task.
    pub fn spawn<F>(&self, name: impl Into<Arc<str>>, kind: TaskKind, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn(name, kind, future)
    }

    /// Spawns service.
    pub fn spawn_service<F>(&self, name: impl Into<Arc<str>>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn_service(name, future)
    }
}
