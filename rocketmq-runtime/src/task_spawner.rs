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

use tokio_util::sync::CancellationToken;

use crate::error::RuntimeResult;
use crate::task_group::TaskGroup;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;

/// Narrow capability for spawning work owned by an existing [`TaskGroup`].
///
/// Cloning this value shares lifecycle authority; it cannot create a runtime
/// root, expose a raw Tokio handle, detach work, or outlive owner shutdown.
#[derive(Debug, Clone)]
pub struct TaskSpawner {
    task_group: TaskGroup,
}

impl TaskSpawner {
    pub(crate) fn new(task_group: TaskGroup) -> Self {
        Self { task_group }
    }

    /// Returns the identity of the owned task group.
    pub fn group_id(&self) -> TaskGroupId {
        self.task_group.id()
    }

    /// Returns the diagnostic name of the owned task group.
    pub fn group_name(&self) -> &str {
        self.task_group.name()
    }

    /// Returns a child token cancelled by owner shutdown.
    ///
    /// Cancelling the returned token does not cancel the owning task group.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.task_group.cancellation_token().child_token()
    }

    /// Registers `future` as parent-owned work.
    ///
    /// The future must be `Send` and tasks have no ordering guarantee relative
    /// to other submissions. Owner shutdown cancels and awaits the registered
    /// task according to the parent task group's deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent group is shutting down or closed.
    pub fn spawn<F>(&self, name: impl Into<Arc<str>>, kind: TaskKind, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn(name, kind, future)
    }

    /// Registers a long-lived service future with a custom shutdown protocol.
    ///
    /// The future must observe a cancellation token and perform its own
    /// ordered cleanup. Use [`Self::spawn_cancellable_service`] when owner
    /// cancellation may drop the future immediately.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent group is shutting down or closed.
    pub fn spawn_service<F>(&self, name: impl Into<Arc<str>>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn_service(name, future)
    }

    /// Registers a service that exits when either its future completes or its owner is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when the parent group is shutting down or closed.
    pub fn spawn_cancellable_service<F>(&self, name: impl Into<Arc<str>>, future: F) -> RuntimeResult<TaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.task_group.spawn_cancellable_service(name, future)
    }
}
