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
use crate::shutdown_deadline::ShutdownDeadline;
use crate::task_group::TaskGroup;
use crate::task_group::TaskKind;

#[derive(Debug, Clone)]
pub struct BlockingPoolPolicy {
    pub name: String,
    pub max_concurrency: usize,
    pub max_queue_depth: usize,
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
        if self.max_queue_depth == 0 {
            return Err(RuntimeError::InvalidConfig(
                "blocking max_queue_depth must be greater than zero".to_string(),
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
            max_queue_depth: 256,
            queue_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(30),
            warn_after: Duration::from_secs(1),
        }
    }
}

/// Capacity-isolated blocking work owned by a service root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockingLane {
    StorageIo,
    MetadataIo,
    CpuCrypto,
}

#[derive(Debug, Clone)]
pub struct BlockingLanePolicies {
    pub storage_io: BlockingPoolPolicy,
    pub metadata_io: BlockingPoolPolicy,
    pub cpu_crypto: BlockingPoolPolicy,
}

impl BlockingLanePolicies {
    pub fn validate(&self) -> RuntimeResult<()> {
        self.storage_io.validate()?;
        self.metadata_io.validate()?;
        self.cpu_crypto.validate()
    }

    pub fn uniform(policy: BlockingPoolPolicy) -> Self {
        let mut storage_io = policy.clone();
        storage_io.name = format!("{}.storage-io", policy.name);
        let mut metadata_io = policy.clone();
        metadata_io.name = format!("{}.metadata-io", policy.name);
        let mut cpu_crypto = policy;
        cpu_crypto.name = format!("{}.cpu-crypto", cpu_crypto.name);
        Self {
            storage_io,
            metadata_io,
            cpu_crypto,
        }
    }
}

impl Default for BlockingLanePolicies {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4);
        Self {
            storage_io: BlockingPoolPolicy {
                name: "rocketmq-blocking.storage-io".to_string(),
                max_concurrency: parallelism.saturating_mul(4).max(4),
                max_queue_depth: parallelism.saturating_mul(16).max(16),
                ..BlockingPoolPolicy::default()
            },
            metadata_io: BlockingPoolPolicy {
                name: "rocketmq-blocking.metadata-io".to_string(),
                max_concurrency: parallelism.saturating_mul(2).max(2),
                max_queue_depth: parallelism.saturating_mul(8).max(8),
                ..BlockingPoolPolicy::default()
            },
            cpu_crypto: BlockingPoolPolicy {
                name: "rocketmq-blocking.cpu-crypto".to_string(),
                max_concurrency: parallelism.max(2),
                max_queue_depth: parallelism.saturating_mul(4).max(8),
                ..BlockingPoolPolicy::default()
            },
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
    queue_permits: Arc<Semaphore>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    reaper_group: TaskGroup,
    next_task_id: Arc<AtomicU64>,
}

struct QueuedBlockingTaskGuard {
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    task_id: BlockingTaskId,
    armed: bool,
}

impl QueuedBlockingTaskGuard {
    fn new(tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>, task_id: BlockingTaskId) -> Self {
        Self {
            tasks,
            task_id,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for QueuedBlockingTaskGuard {
    fn drop(&mut self) {
        if self.armed {
            self.tasks.remove(&self.task_id);
        }
    }
}

struct RunningBlockingTaskGuard<R>
where
    R: Send + 'static,
{
    join_handle: Option<tokio::task::JoinHandle<R>>,
    tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
    reaper_group: TaskGroup,
    task_id: BlockingTaskId,
    name: Arc<str>,
}

impl<R> RunningBlockingTaskGuard<R>
where
    R: Send + 'static,
{
    fn new(
        join_handle: tokio::task::JoinHandle<R>,
        tasks: Arc<DashMap<BlockingTaskId, BlockingTaskMeta>>,
        reaper_group: TaskGroup,
        task_id: BlockingTaskId,
        name: Arc<str>,
    ) -> Self {
        Self {
            join_handle: Some(join_handle),
            tasks,
            reaper_group,
            task_id,
            name,
        }
    }

    fn join_handle(&mut self) -> Option<&mut tokio::task::JoinHandle<R>> {
        self.join_handle.as_mut()
    }

    fn disarm(&mut self) {
        self.join_handle.take();
    }

    fn schedule_reaper(&mut self) {
        let Some(join_handle) = self.join_handle.take() else {
            return;
        };
        if let Some(mut meta) = self.tasks.get_mut(&self.task_id) {
            meta.state = BlockingTaskState::TimedOutStillRunning;
        }
        let tasks = self.tasks.clone();
        let task_id = self.task_id;
        let reaper_name = format!("blocking-reaper:{}", self.name);
        let _ = self
            .reaper_group
            .spawn_detached(reaper_name, TaskKind::BlockingReaper, async move {
                let _ = join_handle.await;
                tasks.remove(&task_id);
            });
    }
}

impl<R> Drop for RunningBlockingTaskGuard<R>
where
    R: Send + 'static,
{
    fn drop(&mut self) {
        self.schedule_reaper();
    }
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
    pub max_queue_depth: usize,
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
            queue_permits: Arc::new(Semaphore::new(policy.max_queue_depth)),
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

    /// Runs short blocking I/O without admitting or waiting for work beyond `deadline`.
    pub async fn spawn_io_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_until(name, BlockingKind::ShortIo, deadline, operation).await
    }

    pub async fn spawn<F, R>(&self, name: impl Into<Arc<str>>, kind: BlockingKind, operation: F) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_inner(name.into(), kind, None, operation).await
    }

    /// Runs blocking work while bounding queue admission and execution by one absolute deadline.
    pub async fn spawn_until<F, R>(
        &self,
        name: impl Into<Arc<str>>,
        kind: BlockingKind,
        deadline: ShutdownDeadline,
        operation: F,
    ) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.spawn_inner(name.into(), kind, Some(deadline), operation).await
    }

    async fn spawn_inner<F, R>(
        &self,
        name: Arc<str>,
        kind: BlockingKind,
        deadline: Option<ShutdownDeadline>,
        operation: F,
    ) -> RuntimeResult<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if kind == BlockingKind::LongRunning {
            return Err(RuntimeError::UnsupportedBlockingKind { name, kind });
        }
        if deadline.is_some_and(ShutdownDeadline::is_expired) {
            return Err(RuntimeError::BlockingQueueTimeout { name });
        }

        let queue_permit =
            self.queue_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_error| RuntimeError::BlockingQueueFull {
                    name: name.clone(),
                    max_queue_depth: self.policy.max_queue_depth,
                })?;
        let task_id = BlockingTaskId(self.next_task_id.fetch_add(1, Ordering::Relaxed));
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
        let queued_task_guard = QueuedBlockingTaskGuard::new(self.tasks.clone(), task_id);

        let queue_deadline = phase_deadline(self.policy.queue_timeout, deadline);
        let permit = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(queue_deadline),
            self.permits.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_closed)) => {
                return Err(RuntimeError::BlockingQueueTimeout { name });
            }
            Err(_elapsed) => {
                return Err(RuntimeError::BlockingQueueTimeout { name });
            }
        };
        drop(queue_permit);

        let task_deadline = phase_deadline(self.policy.task_timeout, deadline);
        if task_deadline <= Instant::now() {
            return Err(RuntimeError::BlockingQueueTimeout { name });
        }

        if let Some(mut meta) = self.tasks.get_mut(&task_id) {
            meta.state = BlockingTaskState::Running;
            meta.started_at = Some(Instant::now());
        }
        queued_task_guard.disarm();

        let tasks = self.tasks.clone();
        let join_handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        });
        let mut running_task_guard = RunningBlockingTaskGuard::new(
            join_handle,
            tasks.clone(),
            self.reaper_group.clone(),
            task_id,
            name.clone(),
        );
        let Some(join_handle) = running_task_guard.join_handle() else {
            self.finish_task(task_id, BlockingTaskState::JoinFailed);
            return Err(RuntimeError::LifecycleOperation {
                operation: "run_blocking_task",
                message: format!("blocking task {name} lost its owned join handle"),
            });
        };

        match tokio::time::timeout_at(tokio::time::Instant::from_std(task_deadline), join_handle).await {
            Ok(Ok(value)) => {
                running_task_guard.disarm();
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
                running_task_guard.disarm();
                self.finish_task(task_id, BlockingTaskState::JoinFailed);
                Err(RuntimeError::BlockingJoin { name, error })
            }
            Err(_elapsed) => {
                running_task_guard.schedule_reaper();
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
            max_queue_depth: self.policy.max_queue_depth,
            queued,
            running,
            timed_out_still_running,
            blocking_still_running: running + timed_out_still_running,
            tasks,
        }
    }

    pub fn blocking_still_running(&self) -> usize {
        self.tasks
            .iter()
            .filter(|entry| {
                matches!(
                    entry.value().state,
                    BlockingTaskState::Running | BlockingTaskState::TimedOutStillRunning
                )
            })
            .count()
    }

    fn finish_task(&self, task_id: BlockingTaskId, state: BlockingTaskState) {
        if let Some(mut meta) = self.tasks.get_mut(&task_id) {
            meta.state = state;
        }
        self.tasks.remove(&task_id);
    }
}

fn phase_deadline(policy_timeout: Duration, deadline: Option<ShutdownDeadline>) -> Instant {
    let policy_deadline = Instant::now() + policy_timeout;
    deadline.map_or(policy_deadline, |deadline| policy_deadline.min(deadline.instant()))
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
