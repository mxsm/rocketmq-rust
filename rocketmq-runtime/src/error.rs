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

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("invalid runtime config: {0}")]
    InvalidConfig(String),

    #[error("failed to build tokio runtime: {0}")]
    BuildRuntime(#[from] std::io::Error),

    #[error("no current Tokio runtime is available")]
    NoCurrentRuntime,

    #[error("operation {0} cannot run inside a Tokio runtime")]
    InsideTokioRuntime(&'static str),

    #[error("task group {group_name} ({group_id:?}) is closing or closed")]
    TaskGroupClosing {
        group_id: TaskGroupId,
        group_name: Arc<str>,
    },

    #[error("blocking queue timeout for {name}")]
    BlockingQueueTimeout { name: Arc<str> },

    #[error("blocking kind {kind:?} is not supported by BlockingExecutor for {name}; use a dedicated thread")]
    UnsupportedBlockingKind { name: Arc<str>, kind: BlockingKind },

    #[error("blocking task {name} ({task_id:?}) timed out and is still running")]
    BlockingTaskTimeoutStillRunning { name: Arc<str>, task_id: BlockingTaskId },

    #[error("blocking task join failed for {name}: {error}")]
    BlockingJoin {
        name: Arc<str>,
        error: tokio::task::JoinError,
    },

    #[error("scheduled task {name} already exists")]
    ScheduledTaskExists { name: Arc<str> },

    #[error("runtime lifecycle operation {operation} failed: {message}")]
    LifecycleOperation { operation: &'static str, message: String },
}
