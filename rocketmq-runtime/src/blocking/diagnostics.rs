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
use std::time::Instant;

use serde::Serialize;

use super::BlockingKind;
use super::BlockingLane;
use super::BlockingTaskId;
use super::BlockingTaskState;

#[derive(Debug, Clone)]
pub(crate) struct BlockingTaskMeta {
    pub id: BlockingTaskId,
    pub name: Arc<str>,
    pub kind: BlockingKind,
    pub state: BlockingTaskState,
    pub queued_at: Instant,
    pub started_at: Option<Instant>,
}

impl BlockingTaskMeta {
    pub(crate) fn snapshot(&self) -> BlockingTaskSnapshot {
        let elapsed = self.started_at.unwrap_or(self.queued_at).elapsed();
        BlockingTaskSnapshot {
            id: self.id,
            name: self.name.to_string(),
            kind: self.kind,
            state: self.state,
            elapsed,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
/// Represents blocking executor snapshot.
pub struct BlockingExecutorSnapshot {
    /// The name value.
    pub name: String,
    /// The lane value.
    pub lane: BlockingLane,
    /// The max concurrency value.
    pub max_concurrency: usize,
    /// The max queue depth value.
    pub max_queue_depth: usize,
    /// The global capacity value.
    pub global_capacity: usize,
    /// The global running value.
    pub global_running: usize,
    /// The global available value.
    pub global_available: usize,
    /// The lane reserved value.
    pub lane_reserved: usize,
    /// The lane running value.
    pub lane_running: usize,
    /// The lane borrowed value.
    pub lane_borrowed: usize,
    /// The queued value.
    pub queued: usize,
    /// The running value.
    pub running: usize,
    /// The timed out still running value.
    pub timed_out_still_running: usize,
    /// The blocking still running value.
    pub blocking_still_running: usize,
    /// The rejected value.
    pub rejected: u64,
    #[serde(with = "duration_millis")]
    /// The oldest queue wait value.
    pub oldest_queue_wait: Duration,
    /// The tasks value.
    pub tasks: Vec<BlockingTaskSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
/// Represents blocking task snapshot.
pub struct BlockingTaskSnapshot {
    /// The id identifier.
    pub id: BlockingTaskId,
    /// The name value.
    pub name: String,
    /// The kind value.
    pub kind: BlockingKind,
    /// The state value.
    pub state: BlockingTaskState,
    #[serde(with = "duration_millis")]
    /// The elapsed value.
    pub elapsed: Duration,
}

mod duration_millis {
    use std::time::Duration;

    use serde::Serializer;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_millis() as u64)
    }
}
