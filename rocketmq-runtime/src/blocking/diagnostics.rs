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
pub struct BlockingExecutorSnapshot {
    pub name: String,
    pub lane: BlockingLane,
    pub max_concurrency: usize,
    pub max_queue_depth: usize,
    pub global_capacity: usize,
    pub global_running: usize,
    pub global_available: usize,
    pub lane_reserved: usize,
    pub lane_running: usize,
    pub lane_borrowed: usize,
    pub queued: usize,
    pub running: usize,
    pub timed_out_still_running: usize,
    pub blocking_still_running: usize,
    pub rejected: u64,
    #[serde(with = "duration_millis")]
    pub oldest_queue_wait: Duration,
    pub tasks: Vec<BlockingTaskSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockingTaskSnapshot {
    pub id: BlockingTaskId,
    pub name: String,
    pub kind: BlockingKind,
    pub state: BlockingTaskState,
    #[serde(with = "duration_millis")]
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
