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

use crate::blocking::BlockingKind;
use crate::blocking::BlockingTaskId;
use crate::task_group::TaskGroupId;

/// Alias for the runtime result type.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, thiserror::Error)]
/// Identifies the runtime error state.
pub enum RuntimeError {
    #[error("invalid runtime config: {0}")]
    /// Represents the invalid config case.
    InvalidConfig(String),

    #[error("failed to build tokio runtime: {0}")]
    /// Represents the build runtime case.
    BuildRuntime(#[from] std::io::Error),

    #[error("runtime I/O failed: {0}")]
    /// Represents the io case.
    Io(std::io::Error),

    #[error("runtime configuration loading failed: {0}")]
    /// Represents the configuration case.
    Configuration(String),

    #[error("no current Tokio runtime is available")]
    /// Represents the no current runtime case.
    NoCurrentRuntime,

    #[error("operation {0} cannot run inside a Tokio runtime")]
    /// Represents the inside tokio runtime case.
    InsideTokioRuntime(&'static str),

    #[error("task group {group_name} ({group_id:?}) is closing or closed")]
    /// Represents the task group closing case.
    TaskGroupClosing {
        /// The struct field value.
        group_id: TaskGroupId,
        /// The struct field value.
        group_name: Arc<str>,
    },

    #[error("blocking queue timeout for {name}")]
    /// Represents the blocking queue timeout case.
    BlockingQueueTimeout {
        /// The name value.
        name: Arc<str>,
    },

    #[error("blocking queue is full for {name}; maximum queued tasks: {max_queue_depth}")]
    /// Represents the blocking queue full case.
    BlockingQueueFull {
        /// The name value.
        name: Arc<str>,
        /// The max queue depth value.
        max_queue_depth: usize,
    },

    #[error("blocking kind {kind:?} is not supported by BlockingExecutor for {name}; use a dedicated thread")]
    /// Represents the unsupported blocking kind case.
    UnsupportedBlockingKind {
        /// The name value.
        name: Arc<str>,
        /// The kind value.
        kind: BlockingKind,
    },

    #[error("blocking task {name} ({task_id:?}) timed out and is still running")]
    /// Represents the blocking task timeout still running case.
    BlockingTaskTimeoutStillRunning {
        /// The name value.
        name: Arc<str>,
        /// The task identifier.
        task_id: BlockingTaskId,
    },

    #[error("blocking task join failed for {name}: {error}")]
    /// Represents the blocking join case.
    BlockingJoin {
        /// The struct field value.
        name: Arc<str>,
        /// The struct field value.
        error: tokio::task::JoinError,
    },

    #[error("scheduled task {name} already exists")]
    /// Represents the scheduled task exists case.
    ScheduledTaskExists {
        /// The name value.
        name: Arc<str>,
    },

    #[error("runtime lifecycle operation {operation} failed: {message}")]
    /// Represents the lifecycle operation case.
    LifecycleOperation {
        /// The operation value.
        operation: &'static str,
        /// The message value.
        message: String,
    },
}
