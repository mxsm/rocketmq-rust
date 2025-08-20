/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
pub mod executor;
pub mod scheduler;
pub mod task;
pub mod trigger;

use std::error::Error;
use std::fmt;

pub use executor::ExecutorPool;
pub use task::Task;
pub use task::TaskContext;
pub use task::TaskResult;
pub use task::TaskStatus;

/// Scheduler error type
#[derive(Debug)]
pub enum SchedulerError {
    TaskNotFound(String),
    TaskAlreadyExists(String),
    ExecutorError(String),
    TriggerError(String),
    SystemError(String),
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchedulerError::TaskNotFound(id) => write!(f, "Task not found: {id}"),
            SchedulerError::TaskAlreadyExists(id) => write!(f, "Task already exists: {id}"),
            SchedulerError::ExecutorError(msg) => write!(f, "Executor error: {msg}"),
            SchedulerError::TriggerError(msg) => write!(f, "Trigger error: {msg}"),
            SchedulerError::SystemError(msg) => write!(f, "System error: {msg}"),
        }
    }
}

impl Error for SchedulerError {}

pub type SchedulerResult<T> = Result<T, SchedulerError>;

pub mod simple_scheduler {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use anyhow::Result;
    use tokio::sync::Mutex;
    use tokio::sync::RwLock;
    use tokio::sync::Semaphore;
    use tokio::task::JoinHandle;
    use tokio::time::Duration;
    use tokio::time::Instant;
    use tokio::time::{self};
    use tokio_util::sync::CancellationToken;
    use tracing::error;
    use tracing::info;

    #[derive(Debug, Clone, Copy)]
    pub enum ScheduleMode {
        /// Align the beats, and they might pile up.
        FixedRate,
        /// Sleep only after the task is completed, and there will be no accumulation.
        FixedDelay,
        /// Align the beats, but skip if the last task is not yet completed.
        FixedRateNoOverlap,
    }

    type TaskId = u64;

    pub struct TaskInfo {
        cancel_token: CancellationToken,
        handle: JoinHandle<()>,
    }

    #[derive(Clone)]
    pub struct ScheduledTaskManager {
        tasks: Arc<RwLock<HashMap<TaskId, TaskInfo>>>,
        counter: Arc<AtomicU64>,
    }

    impl Default for ScheduledTaskManager {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ScheduledTaskManager {
        pub fn new() -> Self {
            Self {
                tasks: Arc::new(RwLock::new(HashMap::new())),
                counter: Arc::new(AtomicU64::new(0)),
            }
        }

        fn next_id(&self) -> TaskId {
            self.counter.fetch_add(1, Ordering::Relaxed)
        }

        /// Adds a scheduled task to the task manager.
        ///
        /// # Arguments
        /// * `mode` - The scheduling mode for the task. Determines how the task is executed:
        ///   - `FixedRate`: Aligns the beats, allowing tasks to pile up if they take too long.
        ///   - `FixedDelay`: Executes tasks serially, with a delay after each task completes.
        ///   - `FixedRateNoOverlap`: Aligns the beats but skips execution if the previous task is
        ///     still running.
        /// * `initial_delay` - The delay before the first execution of the task.
        /// * `period` - The interval between task executions.
        /// * `task_fn` - A function that defines the task to be executed. It takes a
        ///   `CancellationToken` as an argument and returns a `Future` that resolves to a
        ///   `Result<()>`.
        ///
        /// # Returns
        /// A `TaskId` representing the unique identifier of the scheduled task.
        ///
        /// # Notes
        /// - The task function is executed asynchronously.
        /// - The `CancellationToken` can be used to gracefully cancel the task.
        /// - The task is added to the internal task manager and can be managed (e.g., canceled or
        ///   aborted) later.
        pub async fn add_scheduled_task<F, Fut>(
            &self,
            mode: ScheduleMode,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> TaskId
        where
            F: FnMut(CancellationToken) -> Fut + Send + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            let id = self.next_id();
            let token = CancellationToken::new();
            let token_child = token.clone();

            let task_fn = Arc::new(Mutex::new(task_fn));

            let handle = tokio::spawn({
                let task_fn = task_fn.clone();
                async move {
                    match mode {
                        ScheduleMode::FixedRate => {
                            let start = Instant::now() + initial_delay;
                            let mut ticker = time::interval_at(start, period);

                            loop {
                                tokio::select! {
                                    _ = token_child.cancelled() => {
                                        info!("Task {} cancelled gracefully", id);
                                        break;
                                    }
                                    _ = ticker.tick() => {
                                        // Allow concurrent execution: One subtask per tick
                                        let task_fn = task_fn.clone();
                                        let child = token_child.clone();
                                        tokio::spawn(async move {
                                            // 1) Lock out &mut F, call once to get a future
                                            let fut = {
                                                let mut f = task_fn.lock().await;
                                                (f)(child)
                                            };
                                            // The lock has been released. Awaiting here ensures the lock doesn't cross await boundaries.
                                            if let Err(e) = fut.await {
                                                error!("FixedRate task {} failed: {:?}", id, e);
                                            }
                                        });
                                    }
                                }
                            }
                        }

                        ScheduleMode::FixedDelay => {
                            time::sleep(initial_delay).await;
                            loop {
                                tokio::select! {
                                    _ = token_child.cancelled() => {
                                        info!("Task {} cancelled gracefully", id);
                                        break;
                                    }
                                    _ = async {
                                        // Serial execution: complete one task and then sleep
                                        let fut = {
                                            let mut f = task_fn.lock().await;
                                            (f)(token_child.clone())
                                        };
                                        if let Err(e) = fut.await {
                                            error!("FixedDelay task {} failed: {:?}", id, e);
                                        }
                                        time::sleep(period).await;
                                    } => {}
                                }
                            }
                        }

                        ScheduleMode::FixedRateNoOverlap => {
                            let start = Instant::now() + initial_delay;
                            let mut ticker = time::interval_at(start, period);

                            // Permission=1, controls non-overlapping execution
                            let gate = Arc::new(Semaphore::new(1));

                            loop {
                                tokio::select! {
                                    _ = token_child.cancelled() => {
                                        info!("Task {} cancelled gracefully", id);
                                        break;
                                    }
                                    _ = ticker.tick() => {
                                        // Try to get permission. If you can't get it, skip the current tick.
                                        if let Ok(permit) = gate.clone().try_acquire_owned() {
                                            let task_fn = task_fn.clone();
                                            let child = token_child.clone();
                                            tokio::spawn(async move {
                                                // Release the lock immediately after generating the future
                                                let fut = {
                                                    let mut f = task_fn.lock().await;
                                                    (f)(child)
                                                };
                                                if let Err(e) = fut.await {
                                                    error!("FixedRateNoOverlap task {} failed: {:?}", id, e);
                                                }
                                                drop(permit); // Release the permit after completion
                                            });
                                        } else {
                                            info!("Task {} skipped due to overlap", id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            });

            self.tasks.write().await.insert(
                id,
                TaskInfo {
                    cancel_token: token,
                    handle,
                },
            );

            id
        }

        /// Graceful cancellation
        pub async fn cancel_task(&self, id: TaskId) {
            if let Some(info) = self.tasks.write().await.remove(&id) {
                info.cancel_token.cancel();
                tokio::spawn(async move {
                    let _ = info.handle.await;
                });
            }
        }

        /// Roughly abort
        pub async fn abort_task(&self, id: TaskId) {
            if let Some(info) = self.tasks.write().await.remove(&id) {
                info.handle.abort();
            }
        }

        /// Batch cancel
        pub async fn cancel_all(&self) {
            let mut tasks = self.tasks.write().await;
            for (_, info) in tasks.drain() {
                info.cancel_token.cancel();
                tokio::spawn(async move {
                    let _ = info.handle.await;
                });
            }
        }

        /// Batch abort
        pub async fn abort_all(&self) {
            let mut tasks = self.tasks.write().await;
            for (_, info) in tasks.drain() {
                info.handle.abort();
            }
        }

        pub async fn task_count(&self) -> usize {
            self.tasks.read().await.len()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::schedule::simple_scheduler::*;

    #[tokio::test]
    async fn adds_task_and_increments_task_count() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager
            .add_scheduled_task(
                ScheduleMode::FixedRate,
                Duration::from_secs(1),
                Duration::from_secs(2),
                |token| async move {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    Ok(())
                },
            )
            .await;

        assert_eq!(manager.task_count().await, 1);
        manager.cancel_task(task_id).await;
    }

    #[tokio::test]
    async fn cancels_task_and_decrements_task_count() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager
            .add_scheduled_task(
                ScheduleMode::FixedRate,
                Duration::from_secs(1),
                Duration::from_secs(2),
                |token| async move {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    Ok(())
                },
            )
            .await;

        manager.cancel_task(task_id).await;
        assert_eq!(manager.task_count().await, 0);
    }

    #[tokio::test]
    async fn aborts_task_and_decrements_task_count() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager
            .add_scheduled_task(
                ScheduleMode::FixedRate,
                Duration::from_secs(1),
                Duration::from_secs(2),
                |token| async move {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    Ok(())
                },
            )
            .await;

        manager.abort_task(task_id).await;
        assert_eq!(manager.task_count().await, 0);
    }

    #[tokio::test]
    async fn cancels_all_tasks() {
        let manager = ScheduledTaskManager::new();
        for _ in 0..3 {
            manager
                .add_scheduled_task(
                    ScheduleMode::FixedRate,
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                    |token| async move {
                        if token.is_cancelled() {
                            return Ok(());
                        }
                        Ok(())
                    },
                )
                .await;
        }

        assert_eq!(manager.task_count().await, 3);
        manager.cancel_all().await;
        assert_eq!(manager.task_count().await, 0);
    }

    #[tokio::test]
    async fn aborts_all_tasks() {
        let manager = ScheduledTaskManager::new();
        for _ in 0..3 {
            manager
                .add_scheduled_task(
                    ScheduleMode::FixedRate,
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                    |token| async move {
                        if token.is_cancelled() {
                            return Ok(());
                        }
                        Ok(())
                    },
                )
                .await;
        }

        assert_eq!(manager.task_count().await, 3);
        manager.abort_all().await;
        assert_eq!(manager.task_count().await, 0);
    }

    #[tokio::test]
    async fn skips_task_execution_in_fixed_rate_no_overlap_mode() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager
            .add_scheduled_task(
                ScheduleMode::FixedRateNoOverlap,
                Duration::from_secs(0),
                Duration::from_millis(100),
                |token| async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    Ok(())
                },
            )
            .await;

        tokio::time::sleep(Duration::from_secs(1)).await;
        manager.cancel_task(task_id).await;
        assert_eq!(manager.task_count().await, 0);
    }
}
