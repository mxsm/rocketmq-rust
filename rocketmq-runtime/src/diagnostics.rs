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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde::Serialize;

use crate::blocking::BlockingExecutor;
use crate::blocking::BlockingExecutorSnapshot;
use crate::handle::RuntimeHandle;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskGroupLifecycleState;

static NEXT_RUNTIME_DIAGNOSTICS_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct RuntimeDiagnostics {
    runtime: RuntimeHandle,
    runtime_id: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeDiagnosticsSnapshot {
    pub runtime_id: String,
    pub root_name: String,
    pub group_id: TaskGroupId,
    pub parent_group_id: Option<TaskGroupId>,
    pub lifecycle_state: TaskGroupLifecycleState,
    pub task_count: usize,
    pub child_count: usize,
    pub blocking: BlockingExecutorSnapshot,
}

impl RuntimeDiagnostics {
    pub fn new(runtime: RuntimeHandle) -> Self {
        let runtime_id = NEXT_RUNTIME_DIAGNOSTICS_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            runtime,
            runtime_id: Arc::from(format!("rocketmq-runtime-{runtime_id}")),
        }
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn runtime_id(&self) -> &str {
        &self.runtime_id
    }

    pub fn snapshot(&self, root: &TaskGroup, blocking: &BlockingExecutor) -> RuntimeDiagnosticsSnapshot {
        RuntimeDiagnosticsSnapshot {
            runtime_id: self.runtime_id.to_string(),
            root_name: root.name().to_string(),
            group_id: root.id(),
            parent_group_id: root.parent_id(),
            lifecycle_state: root.lifecycle_state(),
            task_count: root.task_count(),
            child_count: root.child_count(),
            blocking: blocking.snapshot(),
        }
    }
}
