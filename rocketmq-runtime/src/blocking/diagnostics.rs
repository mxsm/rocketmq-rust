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
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;

use super::BlockingKind;
use super::BlockingLane;
use super::BlockingTaskId;
use super::BlockingTaskState;

/// The task table of one blocking executor.
pub(crate) type BlockingTaskTable = DashMap<BlockingTaskId, BlockingTaskMeta>;

/// Task tables of the isolated executors bound to one task-group tree.
///
/// An isolated executor has its own budget and task table outside the managed
/// lanes. Registering the table keeps its work visible to the shutdown report
/// of the owner that holds the tree. Every running closure holds its table, so
/// a closure that outlives all of its executor handles is still reported.
#[derive(Debug, Default)]
pub(crate) struct IsolatedBlockingTables {
    tables: Mutex<Vec<Weak<BlockingTaskTable>>>,
}

impl IsolatedBlockingTables {
    pub(crate) fn register(&self, table: &Arc<BlockingTaskTable>) {
        let mut tables = self.tables.lock();
        // A released table can no longer hold work. Pruning on registration
        // bounds the list by the live tables plus those released since then.
        tables.retain(|table| table.strong_count() > 0);
        tables.push(Arc::downgrade(table));
    }

    /// Returns the number of closures still running and a snapshot of every
    /// queued or running task across the live tables.
    pub(crate) fn report(&self) -> (usize, Vec<BlockingTaskSnapshot>) {
        let tables = self.tables.lock().iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
        let mut still_running = 0;
        let mut tasks = Vec::new();
        for table in tables {
            for entry in table.iter() {
                let task = entry.value();
                if matches!(
                    task.state,
                    BlockingTaskState::Running | BlockingTaskState::TimedOutStillRunning
                ) {
                    still_running += 1;
                }
                tasks.push(task.snapshot());
            }
        }
        (still_running, tasks)
    }
}

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

/// Internal, name-free aggregates collected without materializing task details.
pub(crate) struct BlockingExecutorAggregate {
    pub lane: BlockingLane,
    pub max_concurrency: usize,
    pub max_queue_depth: usize,
    pub queued: usize,
    pub running: usize,
    pub timed_out_still_running: usize,
    pub blocking_still_running: usize,
    pub task_kinds: [(BlockingKind, usize, Duration); 3],
}

impl BlockingExecutorAggregate {
    pub(crate) fn new(lane: BlockingLane, max_concurrency: usize, max_queue_depth: usize) -> Self {
        Self {
            lane,
            max_concurrency,
            max_queue_depth,
            queued: 0,
            running: 0,
            timed_out_still_running: 0,
            blocking_still_running: 0,
            task_kinds: [
                (BlockingKind::ShortIo, 0, Duration::ZERO),
                (BlockingKind::CpuBound, 0, Duration::ZERO),
                (BlockingKind::LongRunning, 0, Duration::ZERO),
            ],
        }
    }

    pub(crate) fn record_kind(&mut self, kind: BlockingKind, elapsed: Duration) {
        let index = match kind {
            BlockingKind::ShortIo => 0,
            BlockingKind::CpuBound => 1,
            BlockingKind::LongRunning => 2,
        };
        self.task_kinds[index].1 = self.task_kinds[index].1.saturating_add(1);
        self.task_kinds[index].2 = self.task_kinds[index].2.max(elapsed);
    }
}

impl From<BlockingExecutorSnapshot> for BlockingExecutorAggregate {
    fn from(snapshot: BlockingExecutorSnapshot) -> Self {
        let mut aggregate = Self::new(snapshot.lane, snapshot.max_concurrency, snapshot.max_queue_depth);
        aggregate.queued = snapshot.queued;
        aggregate.running = snapshot.running;
        aggregate.timed_out_still_running = snapshot.timed_out_still_running;
        aggregate.blocking_still_running = snapshot.blocking_still_running;
        for task in snapshot.tasks {
            aggregate.record_kind(task.kind, task.elapsed);
        }
        aggregate
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
