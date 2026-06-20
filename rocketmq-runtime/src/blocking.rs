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
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::error::RuntimeError;
use crate::error::RuntimeResult;
use crate::task_group::TaskGroup;
use crate::task_group::TaskKind;

#[derive(Debug, Clone)]
pub struct BlockingPoolPolicy {
    pub name: String,
    pub max_concurrency: usize,
    pub queue_timeout: Duration,
    pub task_timeout: Duration,
    pub warn_after: Duration,
}

impl BlockingPoolPolicy {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.max_concurrency == 0 {
            return Err(RuntimeError::InvalidConfig(
                "blocking max_concurrency must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for BlockingPoolPolicy {
    fn default() -> Self {
        Self {
            name: "rocketmq-blocking".to_string(),
            max_concurrency: 64,
            queue_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(30),
            warn_after: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockingKind {
    ShortIo,
    CpuBound,
    LongRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct BlockingTaskId(u64);

impl BlockingTaskId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum BlockingTaskState {
    Queued,
    Running,
    Completed,
    JoinFailed,
    TimedOutStillRunning,
}

#[derive(Debug, Clone)]
pub struct BlockingExecutor {
    policy: Arc<BlockingPoolPolicy>,
    permits: Arc<Semaphore>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    reaper_group: TaskGroup,
    next_task_id: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct BlockingTaskMeta {
    id: BlockingTaskId,
    name: Arc<str>,
    kind: BlockingKind,
    state: BlockingTaskState,
    queued_at: Instant,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockingExecutorSnapshot {
    pub name: String,
    pub max_concurrency: usize,
    pub queued: usize,
    pub running: usize,
    pub timed_out_still_running: usize,
    pub blocking_still_running: usize,
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

impl BlockingExecutor {
    pub fn new(policy: BlockingPoolPolicy, reaper_group: TaskGroup) -> RuntimeResult<Self> {
        policy.validate()?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(policy.max_concurrency)),
            policy: Arc::new(policy),
            tasks: Arc::new(DashMap::new()),
            reaper_group,
            next_task_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub fn policy(&self) -> &BlockingPoolPolicy {
        &self.policy
    }

    pub async fn spawn_io<F, R>(&self, name: impl Into<Arc<str>>, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn(name, BlockingKind::ShortIo, operation).await
    }

    pub async fn spawn<F, R>(&self, name: impl Into<Arc<str>>, kind: BlockingKind, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let task_id = BlockingTaskId(self.next_task_id.fetch_add(1, Ordering::Relaxed));
        let name = name.into();
        self.tasks.insert(
            task_id,
            BlockingTaskMeta {
                id: task_id,
                name: name.clone(),
                kind,
                state: BlockingTaskState::Queued,
                queued_at: Instant::now(),
                started_at: None,
            },
        );

        let permit = match tokio::time::timeout(self.policy.queue_timeout, self.permits.clone().acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_closed)) => {
                self.tasks.remove(&task_id);
                return Err(RuntimeError::BlockingQueueTimeout { name });
            }
            Err(_elapsed) => {
                self.tasks.remove(&task_id);
                return Err(RuntimeError::BlockingQueueTimeout { name });
            }
        };

        if let Some(mut meta) = self.tasks.get_mut(&task_id) {
            meta.state = BlockingTaskState::Running;
            meta.started_at = Some(Instant::now());
        }

        let tasks = self.tasks.clone();
        let mut join_handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        });

        match tokio::time::timeout(self.policy.task_timeout, &mut join_handle).await {
            Ok(Ok(value)) => {
                let elapsed = tasks
                    .get(&task_id)
                    .and_then(|meta| meta.started_at.map(|started_at| started_at.elapsed()));
                self.finish_task(task_id, BlockingTaskState::Completed);
                if let Some(elapsed) = elapsed {
                    if elapsed > self.policy.warn_after {
                        tracing::warn!(
                            task_id = task_id.as_u64(),
                            task_name = %name,
                            elapsed_ms = elapsed.as_millis(),
                            "blocking task exceeded warn_after"
                        );
                    }
                }
                Ok(value)
            }
            Ok(Err(error)) => {
                self.finish_task(task_id, BlockingTaskState::JoinFailed);
                Err(RuntimeError::BlockingJoin { name, error })
            }
            Err(_elapsed) => {
                if let Some(mut meta) = self.tasks.get_mut(&task_id) {
                    meta.state = BlockingTaskState::TimedOutStillRunning;
                }
                let tasks = self.tasks.clone();
                let reaper_name = format!("blocking-reaper:{name}");
                let _ = self
                    .reaper_group
                    .spawn_detached(reaper_name, TaskKind::BlockingReaper, async move {
                        let _ = join_handle.await;
                        tasks.remove(&task_id);
                    });
                Err(RuntimeError::BlockingTaskTimeoutStillRunning { name, task_id })
            }
        }
    }

    pub fn snapshot(&self) -> BlockingExecutorSnapshot {
        let tasks = self
            .tasks
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect::<Vec<_>>();
        let queued = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::Queued)
            .count();
        let running = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::Running)
            .count();
        let timed_out_still_running = tasks
            .iter()
            .filter(|task| task.state == BlockingTaskState::TimedOutStillRunning)
            .count();

        BlockingExecutorSnapshot {
            name: self.policy.name.clone(),
            max_concurrency: self.policy.max_concurrency,
            queued,
            running,
            timed_out_still_running,
            blocking_still_running: running + timed_out_still_running,
            tasks,
        }
    }

    fn finish_task(&self, task_id: BlockingTaskId, state: BlockingTaskState) {
        if let Some(mut meta) = self.tasks.get_mut(&task_id) {
            meta.state = state;
        }
        self.tasks.remove(&task_id);
    }
}

impl BlockingTaskMeta {
    fn snapshot(&self) -> BlockingTaskSnapshot {
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
