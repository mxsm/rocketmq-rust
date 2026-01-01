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
    use parking_lot::RwLock;
    use tokio::sync::Semaphore;
    use tokio::task::JoinHandle;
    use tokio::time::Duration;
    use tokio::time::Instant;
    use tokio::time::{self};
    use tokio_util::sync::CancellationToken;
    use tracing::error;
    use tracing::info;

    use crate::ArcMut;

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

        /// Adds a fixed-rate scheduled task to the task manager.
        ///
        /// # Arguments
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
        /// - Tasks are executed at fixed intervals, even if previous executions overlap.
        pub fn add_fixed_rate_task<F, Fut>(&self, initial_delay: Duration, period: Duration, task_fn: F) -> TaskId
        where
            F: FnMut(CancellationToken) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            self.add_scheduled_task(ScheduleMode::FixedRate, initial_delay, period, task_fn)
        }

        /// Adds a fixed-delay scheduled task to the task manager.
        ///
        /// # Arguments
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
        /// - Tasks are executed serially, with a delay after each task completes.
        pub fn add_fixed_delay_task<F, Fut>(&self, initial_delay: Duration, period: Duration, task_fn: F) -> TaskId
        where
            F: FnMut(CancellationToken) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            self.add_scheduled_task(ScheduleMode::FixedDelay, initial_delay, period, task_fn)
        }

        /// Adds a fixed-rate-no-overlap scheduled task to the task manager.
        ///
        /// # Arguments
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
        /// - Tasks are executed at fixed intervals, but overlapping executions are skipped.
        pub fn add_fixed_rate_no_overlap_task<F, Fut>(
            &self,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> TaskId
        where
            F: FnMut(CancellationToken) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            self.add_scheduled_task(ScheduleMode::FixedRateNoOverlap, initial_delay, period, task_fn)
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
        pub fn add_scheduled_task<F, Fut>(
            &self,
            mode: ScheduleMode,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> TaskId
        where
            F: FnMut(CancellationToken) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            let id = self.next_id();
            let token = CancellationToken::new();
            let token_child = token.clone();

            let task_fn = ArcMut::new(task_fn);

            let handle = tokio::spawn({
                let mut task_fn = task_fn;
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
                                        let mut task_fn = task_fn.clone();
                                        let child = token_child.clone();
                                        tokio::spawn(async move {
                                            // 1) Lock out &mut F, call once to get a future
                                            let fut = {
                                                (task_fn)(child)
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
                                            (task_fn)(token_child.clone())
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
                                        // Try to acquire permission. If unable to acquire, skip the current tick.
                                        if let Ok(permit) = gate.clone().try_acquire_owned() {
                                            let mut task_fn = task_fn.clone();
                                            let child = token_child.clone();
                                            tokio::spawn(async move {
                                                // Release the lock immediately after generating the future
                                                let fut = {
                                                    (task_fn)(child)
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

            self.tasks.write().insert(
                id,
                TaskInfo {
                    cancel_token: token,
                    handle,
                },
            );

            id
        }

        /// Graceful cancellation
        pub fn cancel_task(&self, id: TaskId) {
            if let Some(info) = self.tasks.write().remove(&id) {
                info.cancel_token.cancel();
                tokio::spawn(async move {
                    let _ = info.handle.await;
                });
            }
        }

        /// Roughly abort
        pub fn abort_task(&self, id: TaskId) {
            if let Some(info) = self.tasks.write().remove(&id) {
                info.handle.abort();
            }
        }

        /// Batch cancel
        pub fn cancel_all(&self) {
            let mut tasks = self.tasks.write();
            for (_, info) in tasks.drain() {
                info.cancel_token.cancel();
                tokio::spawn(async move {
                    let _ = info.handle.await;
                });
            }
        }

        /// Batch abort
        pub fn abort_all(&self) {
            let mut tasks = self.tasks.write();
            for (_, info) in tasks.drain() {
                info.handle.abort();
            }
        }

        pub fn task_count(&self) -> usize {
            self.tasks.read().len()
        }
    }

    impl ScheduledTaskManager {
        /// Adds a fixed-rate scheduled task to the task manager asynchronously.
        ///
        /// # Arguments
        /// * `initial_delay` - The delay before the first execution of the task.
        /// * `period` - The interval between task executions.
        /// * `task_fn` - A function that defines the task to be executed. It takes a
        ///   `CancellationToken` as an argument and returns a `Result<()>`.
        ///
        /// # Returns
        /// A `TaskId` representing the unique identifier of the scheduled task.
        ///
        /// # Notes
        /// - Tasks are executed at fixed intervals, even if previous executions overlap.
        /// - The task function is executed asynchronously.
        pub fn add_fixed_rate_task_async<F>(&self, initial_delay: Duration, period: Duration, task_fn: F) -> TaskId
        where
            F: AsyncFnMut(CancellationToken) -> Result<()> + Send + Sync + 'static,
            for<'a> <F as AsyncFnMut<(CancellationToken,)>>::CallRefFuture<'a>: Send,
        {
            self.add_scheduled_task_async(ScheduleMode::FixedRate, initial_delay, period, task_fn)
        }

        /// Adds a fixed-delay scheduled task to the task manager asynchronously.
        ///
        /// # Arguments
        /// * `initial_delay` - The delay before the first execution of the task.
        /// * `period` - The interval between task executions.
        /// * `task_fn` - A function that defines the task to be executed. It takes a
        ///   `CancellationToken` as an argument and returns a `Result<()>`.
        ///
        /// # Returns
        /// A `TaskId` representing the unique identifier of the scheduled task.
        ///
        /// # Notes
        /// - Tasks are executed serially, with a delay after each task completes.
        /// - The task function is executed asynchronously.
        pub fn add_fixed_delay_task_async<F>(&self, initial_delay: Duration, period: Duration, task_fn: F) -> TaskId
        where
            F: AsyncFnMut(CancellationToken) -> Result<()> + Send + Sync + 'static,
            for<'a> <F as AsyncFnMut<(CancellationToken,)>>::CallRefFuture<'a>: Send,
        {
            self.add_scheduled_task_async(ScheduleMode::FixedDelay, initial_delay, period, task_fn)
        }

        /// Adds a fixed-rate-no-overlap scheduled task to the task manager asynchronously.
        ///
        /// # Arguments
        /// * `initial_delay` - The delay before the first execution of the task.
        /// * `period` - The interval between task executions.
        /// * `task_fn` - A function that defines the task to be executed. It takes a
        ///   `CancellationToken` as an argument and returns a `Result<()>`.
        ///
        /// # Returns
        /// A `TaskId` representing the unique identifier of the scheduled task.
        ///
        /// # Notes
        /// - Tasks are executed at fixed intervals, but overlapping executions are skipped.
        /// - The task function is executed asynchronously.
        pub fn add_fixed_rate_no_overlap_task_async<F>(
            &self,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> TaskId
        where
            F: AsyncFnMut(CancellationToken) -> Result<()> + Send + Sync + 'static,
            for<'a> <F as AsyncFnMut<(CancellationToken,)>>::CallRefFuture<'a>: Send,
        {
            self.add_scheduled_task_async(ScheduleMode::FixedRateNoOverlap, initial_delay, period, task_fn)
        }

        /// Adds a scheduled task to the task manager asynchronously.
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
        pub fn add_scheduled_task_async<F>(
            &self,
            mode: ScheduleMode,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> TaskId
        where
            F: AsyncFnMut(CancellationToken) -> Result<()> + Send + Sync + 'static,
            for<'a> <F as AsyncFnMut<(CancellationToken,)>>::CallRefFuture<'a>: Send,
        {
            let id = self.next_id();
            let token = CancellationToken::new();
            let token_child = token.clone();

            let task_fn = ArcMut::new(task_fn);

            let handle = tokio::spawn({
                let mut task_fn = task_fn;
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
                                        let mut task_fn = task_fn.clone();
                                        let child = token_child.clone();
                                        tokio::spawn(async move {
                                            // 1) Lock out &mut F, call once to get a future
                                            let fut = {
                                                task_fn(child)
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
                                            (task_fn)(token_child.clone())
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
                                        // Try to acquire permission. If unable to acquire, skip the current tick.
                                        if let Ok(permit) = gate.clone().try_acquire_owned() {
                                            let mut task_fn = task_fn.clone();
                                            let child = token_child.clone();
                                            tokio::spawn(async move {
                                                // Release the lock immediately after generating the future
                                                let fut = {
                                                    (task_fn)(child)
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

            self.tasks.write().insert(
                id,
                TaskInfo {
                    cancel_token: token,
                    handle,
                },
            );

            id
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time;

    use crate::schedule::simple_scheduler::*;

    #[tokio::test]
    async fn adds_task_and_increments_task_count() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager.add_scheduled_task(
            ScheduleMode::FixedRate,
            Duration::from_secs(1),
            Duration::from_secs(2),
            |token| async move {
                if token.is_cancelled() {
                    return Ok(());
                }
                Ok(())
            },
        );

        assert_eq!(manager.task_count(), 1);
        manager.cancel_task(task_id);
    }

    #[tokio::test]
    async fn cancels_task_and_decrements_task_count() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager.add_scheduled_task(
            ScheduleMode::FixedRate,
            Duration::from_secs(1),
            Duration::from_secs(2),
            |token| async move {
                if token.is_cancelled() {
                    return Ok(());
                }
                Ok(())
            },
        );

        manager.cancel_task(task_id);
        assert_eq!(manager.task_count(), 0);
    }

    #[tokio::test]
    async fn aborts_task_and_decrements_task_count() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager.add_scheduled_task(
            ScheduleMode::FixedRate,
            Duration::from_secs(1),
            Duration::from_secs(2),
            |token| async move {
                if token.is_cancelled() {
                    return Ok(());
                }
                Ok(())
            },
        );

        manager.abort_task(task_id);
        assert_eq!(manager.task_count(), 0);
    }

    #[tokio::test]
    async fn cancels_all_tasks() {
        let manager = ScheduledTaskManager::new();
        for _ in 0..3 {
            manager.add_scheduled_task(
                ScheduleMode::FixedRate,
                Duration::from_secs(1),
                Duration::from_secs(2),
                |token| async move {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    Ok(())
                },
            );
        }

        assert_eq!(manager.task_count(), 3);
        manager.cancel_all();
        assert_eq!(manager.task_count(), 0);
    }

    #[tokio::test]
    async fn aborts_all_tasks() {
        let manager = ScheduledTaskManager::new();
        for _ in 0..3 {
            manager.add_scheduled_task(
                ScheduleMode::FixedRate,
                Duration::from_secs(1),
                Duration::from_secs(2),
                |token| async move {
                    if token.is_cancelled() {
                        return Ok(());
                    }
                    Ok(())
                },
            );
        }

        assert_eq!(manager.task_count(), 3);
        manager.abort_all();
        assert_eq!(manager.task_count(), 0);
    }

    #[tokio::test]
    async fn skips_task_execution_in_fixed_rate_no_overlap_mode() {
        let manager = ScheduledTaskManager::new();
        let task_id = manager.add_scheduled_task(
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
        );

        time::sleep(Duration::from_millis(400)).await;
        manager.cancel_task(task_id);
        assert_eq!(manager.task_count(), 0);
    }

    fn new_manager() -> ScheduledTaskManager {
        ScheduledTaskManager::new()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fixed_rate_task() {
        let mgr = new_manager();
        let counter = Arc::new(AtomicUsize::new(0));

        let c = counter.clone();
        let task_id = mgr.add_fixed_rate_task_async(
            Duration::from_millis(50),
            Duration::from_millis(100),
            async move |_ctx| {
                c.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        );

        time::sleep(Duration::from_millis(500)).await;

        mgr.cancel_task(task_id);
        time::sleep(Duration::from_millis(50)).await;

        let executed = counter.load(Ordering::Relaxed);
        assert!(executed >= 3, "FixedRate executed too few times: {}", executed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fixed_delay_task() {
        let mgr = new_manager();
        let counter = Arc::new(AtomicUsize::new(0));

        let c = counter.clone();
        let task_id = mgr.add_fixed_delay_task_async(
            Duration::from_millis(10),
            Duration::from_millis(50),
            async move |_ctx| {
                c.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        );

        time::sleep(Duration::from_millis(300)).await;

        mgr.cancel_task(task_id);
        time::sleep(Duration::from_millis(50)).await;

        let executed = counter.load(Ordering::Relaxed);
        assert!((3..=6).contains(&executed), "FixedDelay count unexpected: {}", executed);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fixed_rate_no_overlap_task() {
        let mgr = new_manager();
        let counter = Arc::new(AtomicUsize::new(0));

        let c = counter.clone();
        let task_id = mgr.add_fixed_rate_no_overlap_task_async(
            Duration::from_millis(10),
            Duration::from_millis(50),
            async move |_ctx| {
                time::sleep(Duration::from_millis(80)).await;
                c.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        );

        time::sleep(Duration::from_millis(400)).await;

        mgr.cancel_task(task_id);
        time::sleep(Duration::from_millis(50)).await;

        let executed = counter.load(Ordering::Relaxed);
        assert!(
            (2..=5).contains(&executed),
            "FixedRateNoOverlap count unexpected: {}",
            executed
        );
    }
}
