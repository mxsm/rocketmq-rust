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
    use std::collections::HashSet;
    use std::future::Future;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use anyhow::anyhow;
    use anyhow::Result;
    use parking_lot::RwLock;
    use rocketmq_runtime::RuntimeHandle;
    use rocketmq_runtime::ShutdownReport;
    use rocketmq_runtime::TaskGroup;
    use rocketmq_runtime::TaskId as RuntimeTaskId;
    use rocketmq_runtime::TaskKind;
    use tokio::sync::oneshot;
    use tokio::sync::Semaphore;
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
        runtime_task_id: RuntimeTaskId,
        task_group: TaskGroup,
        done: oneshot::Receiver<()>,
    }

    fn spawn_scheduled_task<F>(
        task_group: &TaskGroup,
        operation: &'static str,
        task_name: String,
        future: F,
    ) -> Result<RuntimeTaskId>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        task_group
            .spawn_service(task_name, future)
            .map_err(|error| anyhow!("{operation} failed to spawn scheduled task driver: {error}"))
    }

    #[derive(Debug, Clone)]
    pub struct ScheduledShutdownReport {
        pub task_count: usize,
        pub completed: usize,
        pub aborted: usize,
        pub panicked: usize,
        pub timed_out: usize,
        pub elapsed: Duration,
    }

    impl ScheduledShutdownReport {
        fn new(task_count: usize) -> Self {
            Self {
                task_count,
                completed: 0,
                aborted: 0,
                panicked: 0,
                timed_out: 0,
                elapsed: Duration::ZERO,
            }
        }

        pub fn is_healthy(&self) -> bool {
            self.panicked == 0 && self.timed_out == 0
        }

        fn record_completed_driver(&mut self) {
            self.completed += 1;
        }

        fn record_aborted_driver(&mut self) {
            self.aborted += 1;
        }
    }

    #[derive(Clone)]
    pub struct ScheduledTaskManager {
        tasks: Arc<RwLock<HashMap<TaskId, TaskInfo>>>,
        counter: Arc<AtomicU64>,
        task_group: Arc<RwLock<Option<TaskGroup>>>,
        last_task_group_shutdown_report: Arc<RwLock<Option<ShutdownReport>>>,
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
                task_group: Arc::new(RwLock::new(None)),
                last_task_group_shutdown_report: Arc::new(RwLock::new(None)),
            }
        }

        fn next_id(&self) -> TaskId {
            self.counter.fetch_add(1, Ordering::Relaxed)
        }

        fn task_group(&self, operation: &'static str) -> Result<TaskGroup> {
            let mut task_group = self.task_group.write();
            if let Some(task_group) = task_group.as_ref() {
                return Ok(task_group.clone());
            }

            let handle = tokio::runtime::Handle::try_current()
                .map_err(|error| anyhow!("{operation} requires a Tokio runtime: {error}"))?;
            let new_group = TaskGroup::root("rocketmq.simple-scheduled-task-manager", RuntimeHandle::new(handle));
            *task_group = Some(new_group.clone());
            Ok(new_group)
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
        pub fn add_fixed_rate_task<F, Fut>(
            &self,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> Result<TaskId>
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
        pub fn add_fixed_delay_task<F, Fut>(
            &self,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> Result<TaskId>
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
        ) -> Result<TaskId>
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
        ) -> Result<TaskId>
        where
            F: FnMut(CancellationToken) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<()>> + Send + 'static,
        {
            let id = self.next_id();
            let token = CancellationToken::new();
            let token_child = token.clone();
            let task_group = self.task_group("ScheduledTaskManager::add_scheduled_task")?;

            let task_fn = ArcMut::new(task_fn);

            let (done_tx, done) = oneshot::channel();
            let driver_group = task_group.clone();
            let run_group = task_group.clone();
            let runtime_task_id = spawn_scheduled_task(
                &driver_group,
                "ScheduledTaskManager::add_scheduled_task",
                format!("scheduled-task-manager.driver.{id}"),
                {
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
                                            if let Err(error) = run_group.spawn(
                                                format!("scheduled-task-manager.run.{id}"),
                                                TaskKind::ScheduledRun,
                                                async move {
                                                // 1) Lock out &mut F, call once to get a future
                                                let fut = {
                                                    (task_fn)(child)
                                                };
                                                // The lock has been released. Awaiting here ensures the lock doesn't cross await boundaries.
                                                if let Err(e) = fut.await {
                                                    error!("FixedRate task {} failed: {:?}", id, e);
                                                }
                                            }) {
                                                error!("FixedRate task {} failed to spawn run: {:?}", id, error);
                                            }
                                        }
                                    }
                                }
                            }

                            ScheduleMode::FixedDelay => {
                                if !initial_delay.is_zero() {
                                    tokio::select! {
                                        _ = token_child.cancelled() => {
                                            info!("Task {} cancelled gracefully", id);
                                            return;
                                        }
                                        _ = time::sleep(initial_delay) => {}
                                    }
                                }

                                loop {
                                    if token_child.is_cancelled() {
                                        info!("Task {} cancelled gracefully", id);
                                        break;
                                    }

                                    let fut = { (task_fn)(token_child.clone()) };
                                    if let Err(e) = fut.await {
                                        error!("FixedDelay task {} failed: {:?}", id, e);
                                    }

                                    tokio::select! {
                                        _ = token_child.cancelled() => {
                                            info!("Task {} cancelled gracefully", id);
                                            break;
                                        }
                                        _ = time::sleep(period) => {}
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
                                                if let Err(error) = run_group.spawn(
                                                    format!("scheduled-task-manager.no-overlap-run.{id}"),
                                                    TaskKind::ScheduledRun,
                                                    async move {
                                                    // Release the lock immediately after generating the future
                                                    let fut = {
                                                        (task_fn)(child)
                                                    };
                                                    if let Err(e) = fut.await {
                                                        error!("FixedRateNoOverlap task {} failed: {:?}", id, e);
                                                    }
                                                    drop(permit); // Release the permit after completion
                                                }) {
                                                    error!(
                                                        "FixedRateNoOverlap task {} failed to spawn run: {:?}",
                                                        id, error
                                                    );
                                                }
                                            } else {
                                                info!("Task {} skipped due to overlap", id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        drop(done_tx);
                    }
                },
            )?;

            self.tasks.write().insert(
                id,
                TaskInfo {
                    cancel_token: token,
                    runtime_task_id,
                    task_group,
                    done,
                },
            );

            Ok(id)
        }

        /// Cancel and stop a scheduled task driver.
        pub fn cancel_task(&self, id: TaskId) {
            if let Some(info) = self.tasks.write().remove(&id) {
                info.cancel_token.cancel();
                info.task_group.abort_task(info.runtime_task_id);
            }
        }

        /// Roughly abort
        pub fn abort_task(&self, id: TaskId) {
            if let Some(info) = self.tasks.write().remove(&id) {
                info.task_group.abort_task(info.runtime_task_id);
            }
        }

        /// Batch cancel
        pub fn cancel_all(&self) {
            let mut tasks = self.tasks.write();
            for (_, info) in tasks.drain() {
                info.cancel_token.cancel();
                info.task_group.abort_task(info.runtime_task_id);
            }
        }

        /// Batch abort
        pub fn abort_all(&self) {
            let mut tasks = self.tasks.write();
            for (_, info) in tasks.drain() {
                info.task_group.abort_task(info.runtime_task_id);
            }
        }

        pub async fn shutdown_all(&self, timeout: Duration) -> ScheduledShutdownReport {
            let tasks = {
                let mut tasks = self.tasks.write();
                tasks.drain().collect::<Vec<_>>()
            };
            let report = Self::shutdown_entries(tasks, timeout).await;
            self.shutdown_task_group(timeout).await;
            report
        }

        pub async fn shutdown_tasks<I>(&self, task_ids: I, timeout: Duration) -> ScheduledShutdownReport
        where
            I: IntoIterator<Item = TaskId>,
        {
            let task_ids = task_ids.into_iter().collect::<HashSet<_>>();
            let tasks = {
                let mut tasks = self.tasks.write();
                task_ids
                    .into_iter()
                    .filter_map(|id| tasks.remove(&id).map(|info| (id, info)))
                    .collect::<Vec<_>>()
            };
            Self::shutdown_entries(tasks, timeout).await
        }

        async fn shutdown_entries(tasks: Vec<(TaskId, TaskInfo)>, timeout: Duration) -> ScheduledShutdownReport {
            let mut report = ScheduledShutdownReport::new(tasks.len());
            let started = Instant::now();

            for (_, info) in &tasks {
                info.cancel_token.cancel();
            }

            for (_, mut info) in tasks {
                if Self::driver_done(&mut info.done) {
                    report.record_completed_driver();
                    continue;
                }

                let remaining = timeout.checked_sub(started.elapsed()).unwrap_or(Duration::ZERO);
                if !remaining.is_zero() && time::timeout(remaining, &mut info.done).await.is_ok() {
                    report.record_completed_driver();
                    continue;
                }

                if Self::driver_done(&mut info.done) {
                    report.record_completed_driver();
                    continue;
                }

                report.timed_out += 1;
                let abort_wait = timeout.min(Duration::from_secs(1));
                if info
                    .task_group
                    .abort_task_and_wait(info.runtime_task_id, abort_wait)
                    .await
                {
                    report.record_aborted_driver();
                }
            }

            report.elapsed = started.elapsed();
            report
        }

        fn driver_done(done: &mut oneshot::Receiver<()>) -> bool {
            !matches!(done.try_recv(), Err(oneshot::error::TryRecvError::Empty))
        }

        pub fn task_count(&self) -> usize {
            self.tasks.read().len()
        }

        pub fn driver_task_count(&self) -> usize {
            self.task_group
                .read()
                .as_ref()
                .map(TaskGroup::task_count)
                .unwrap_or_default()
        }

        pub fn last_task_group_shutdown_report(&self) -> Option<ShutdownReport> {
            self.last_task_group_shutdown_report.read().clone()
        }

        async fn shutdown_task_group(&self, timeout: Duration) {
            let task_group = self.task_group.write().take();
            if let Some(task_group) = task_group {
                let report = task_group.shutdown(timeout).await;
                report.log_if_unhealthy();
                *self.last_task_group_shutdown_report.write() = Some(report);
            }
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
        pub fn add_fixed_rate_task_async<F>(
            &self,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> Result<TaskId>
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
        pub fn add_fixed_delay_task_async<F>(
            &self,
            initial_delay: Duration,
            period: Duration,
            task_fn: F,
        ) -> Result<TaskId>
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
        ) -> Result<TaskId>
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
        ) -> Result<TaskId>
        where
            F: AsyncFnMut(CancellationToken) -> Result<()> + Send + Sync + 'static,
            for<'a> <F as AsyncFnMut<(CancellationToken,)>>::CallRefFuture<'a>: Send,
        {
            let id = self.next_id();
            let token = CancellationToken::new();
            let token_child = token.clone();
            let task_group = self.task_group("ScheduledTaskManager::add_scheduled_task_async")?;

            let task_fn = ArcMut::new(task_fn);

            let (done_tx, done) = oneshot::channel();
            let driver_group = task_group.clone();
            let run_group = task_group.clone();
            let runtime_task_id = spawn_scheduled_task(
                &driver_group,
                "ScheduledTaskManager::add_scheduled_task_async",
                format!("scheduled-task-manager.async-driver.{id}"),
                {
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
                                            if let Err(error) = run_group.spawn(
                                                format!("scheduled-task-manager.async-run.{id}"),
                                                TaskKind::ScheduledRun,
                                                async move {
                                                // 1) Lock out &mut F, call once to get a future
                                                let fut = {
                                                    task_fn(child)
                                                };
                                                // The lock has been released. Awaiting here ensures the lock doesn't cross await boundaries.
                                                if let Err(e) = fut.await {
                                                    error!("FixedRate task {} failed: {:?}", id, e);
                                                }
                                            }) {
                                                error!("FixedRate task {} failed to spawn async run: {:?}", id, error);
                                            }
                                        }
                                    }
                                }
                            }

                            ScheduleMode::FixedDelay => {
                                if !initial_delay.is_zero() {
                                    tokio::select! {
                                        _ = token_child.cancelled() => {
                                            info!("Task {} cancelled gracefully", id);
                                            return;
                                        }
                                        _ = time::sleep(initial_delay) => {}
                                    }
                                }

                                loop {
                                    if token_child.is_cancelled() {
                                        info!("Task {} cancelled gracefully", id);
                                        break;
                                    }

                                    let fut = { (task_fn)(token_child.clone()) };
                                    if let Err(e) = fut.await {
                                        error!("FixedDelay task {} failed: {:?}", id, e);
                                    }

                                    tokio::select! {
                                        _ = token_child.cancelled() => {
                                            info!("Task {} cancelled gracefully", id);
                                            break;
                                        }
                                        _ = time::sleep(period) => {}
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
                                                if let Err(error) = run_group.spawn(
                                                    format!("scheduled-task-manager.async-no-overlap-run.{id}"),
                                                    TaskKind::ScheduledRun,
                                                    async move {
                                                    // Release the lock immediately after generating the future
                                                    let fut = {
                                                        (task_fn)(child)
                                                    };
                                                    if let Err(e) = fut.await {
                                                        error!("FixedRateNoOverlap task {} failed: {:?}", id, e);
                                                    }
                                                    drop(permit); // Release the permit after completion
                                                }) {
                                                    error!(
                                                        "FixedRateNoOverlap task {} failed to spawn async run: {:?}",
                                                        id, error
                                                    );
                                                }
                                            } else {
                                                info!("Task {} skipped due to overlap", id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        drop(done_tx);
                    }
                },
            )?;

            self.tasks.write().insert(
                id,
                TaskInfo {
                    cancel_token: token,
                    runtime_task_id,
                    task_group,
                    done,
                },
            );

            Ok(id)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time;

    use crate::schedule::simple_scheduler::*;

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn add_scheduled_task_without_tokio_runtime_returns_error() {
        let manager = ScheduledTaskManager::new();
        let error = manager
            .add_scheduled_task(
                ScheduleMode::FixedDelay,
                Duration::ZERO,
                Duration::from_secs(1),
                |_token| async move { Ok(()) },
            )
            .expect_err("adding a scheduled task without a Tokio runtime should fail");

        assert!(
            error.to_string().contains("requires a Tokio runtime"),
            "unexpected error: {error}"
        );
        assert_eq!(manager.task_count(), 0);
    }

    #[test]
    fn add_scheduled_task_async_without_tokio_runtime_returns_error() {
        let manager = ScheduledTaskManager::new();
        let error = manager
            .add_scheduled_task_async(
                ScheduleMode::FixedDelay,
                Duration::ZERO,
                Duration::from_secs(1),
                async move |_token| Ok(()),
            )
            .expect_err("adding an async scheduled task without a Tokio runtime should fail");

        assert!(
            error.to_string().contains("requires a Tokio runtime"),
            "unexpected error: {error}"
        );
        assert_eq!(manager.task_count(), 0);
    }

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
            .expect("fixed-rate scheduled task should start");

        assert_eq!(manager.task_count(), 1);
        assert_eq!(manager.driver_task_count(), 1);
        manager.cancel_task(task_id);
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
            .expect("fixed-rate scheduled task should start");

        manager.cancel_task(task_id);
        assert_eq!(manager.task_count(), 0);
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
            .expect("fixed-rate scheduled task should start");

        manager.abort_task(task_id);
        assert_eq!(manager.task_count(), 0);
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
                .expect("fixed-rate scheduled task should start");
        }

        assert_eq!(manager.task_count(), 3);
        assert_eq!(manager.driver_task_count(), 3);
        manager.cancel_all();
        assert_eq!(manager.task_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_all_waits_for_idle_drivers() {
        let manager = ScheduledTaskManager::new();
        for _ in 0..3 {
            manager
                .add_scheduled_task(
                    ScheduleMode::FixedDelay,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                    |_token| async move { Ok(()) },
                )
                .expect("fixed-delay scheduled task should start");
        }

        let report = manager.shutdown_all(Duration::from_secs(1)).await;

        assert_eq!(manager.task_count(), 0);
        assert_eq!(manager.driver_task_count(), 0);
        assert_eq!(report.task_count, 3);
        assert_eq!(report.completed, 3);
        assert_eq!(report.aborted, 0);
        assert_eq!(report.timed_out, 0);
        assert!(report.is_healthy());
        let task_group_report = manager
            .last_task_group_shutdown_report()
            .expect("task group shutdown report should be recorded");
        assert!(task_group_report.is_healthy(), "{}", task_group_report.to_json());
        assert_eq!(task_group_report.leaked, 0);
    }

    #[tokio::test]
    async fn shutdown_tasks_only_stops_selected_drivers() {
        let manager = ScheduledTaskManager::new();
        let selected = manager
            .add_scheduled_task(
                ScheduleMode::FixedDelay,
                Duration::from_secs(60),
                Duration::from_secs(60),
                |_token| async move { Ok(()) },
            )
            .expect("selected scheduled task should start");
        manager
            .add_scheduled_task(
                ScheduleMode::FixedDelay,
                Duration::from_secs(60),
                Duration::from_secs(60),
                |_token| async move { Ok(()) },
            )
            .expect("unselected scheduled task should start");

        let report = manager.shutdown_tasks([selected], Duration::from_secs(1)).await;

        assert_eq!(report.task_count, 1);
        assert_eq!(report.completed, 1);
        assert_eq!(report.timed_out, 0);
        assert!(report.is_healthy());
        assert_eq!(manager.task_count(), 1);
        assert_eq!(manager.driver_task_count(), 1);
        assert!(manager.last_task_group_shutdown_report().is_none());

        let report = manager.shutdown_all(Duration::from_secs(1)).await;
        assert_eq!(report.task_count, 1);
        assert!(report.is_healthy());
        assert_eq!(manager.task_count(), 0);
        let task_group_report = manager
            .last_task_group_shutdown_report()
            .expect("task group shutdown report should be recorded");
        assert!(task_group_report.is_healthy(), "{}", task_group_report.to_json());
        assert_eq!(task_group_report.leaked, 0);
    }

    #[tokio::test]
    async fn shutdown_all_aborts_driver_after_timeout() {
        let manager = ScheduledTaskManager::new();
        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        manager
            .add_scheduled_task(ScheduleMode::FixedDelay, Duration::ZERO, Duration::from_secs(60), {
                let started = Arc::clone(&started);
                let dropped = Arc::clone(&dropped);
                move |_token| {
                    let started = Arc::clone(&started);
                    let dropped = Arc::clone(&dropped);
                    async move {
                        let _marker = DropMarker(dropped);
                        started.store(true, Ordering::Release);
                        future::pending::<anyhow::Result<()>>().await
                    }
                }
            })
            .expect("fixed-delay scheduled task should start");
        time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduled run should start");

        let report = manager.shutdown_all(Duration::from_millis(10)).await;

        assert_eq!(manager.task_count(), 0);
        assert_eq!(report.task_count, 1);
        assert_eq!(report.timed_out, 1);
        assert_eq!(report.aborted, 1);
        assert!(!report.is_healthy());
        assert!(
            dropped.load(Ordering::Acquire),
            "shutdown_all should wait until the aborted driver drops its running future"
        );
    }

    #[tokio::test]
    async fn shutdown_all_only_times_out_unfinished_drivers() {
        let manager = ScheduledTaskManager::new();
        let started = Arc::new(AtomicBool::new(false));
        manager
            .add_scheduled_task(ScheduleMode::FixedDelay, Duration::ZERO, Duration::from_secs(60), {
                let started = Arc::clone(&started);
                move |_token| {
                    let started = Arc::clone(&started);
                    async move {
                        started.store(true, Ordering::Release);
                        future::pending::<anyhow::Result<()>>().await
                    }
                }
            })
            .expect("fixed-delay scheduled task should start");
        for _ in 0..3 {
            manager
                .add_scheduled_task(
                    ScheduleMode::FixedDelay,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                    |_token| async move { Ok(()) },
                )
                .expect("idle scheduled task should start");
        }
        time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduled run should start");

        let report = manager.shutdown_all(Duration::from_millis(100)).await;

        assert_eq!(manager.task_count(), 0);
        assert_eq!(report.task_count, 4);
        assert_eq!(report.completed, 3);
        assert_eq!(report.aborted, 1);
        assert_eq!(report.timed_out, 1);
        assert!(!report.is_healthy());
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
                .expect("fixed-rate scheduled task should start");
        }

        assert_eq!(manager.task_count(), 3);
        manager.abort_all();
        assert_eq!(manager.task_count(), 0);
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
            .expect("fixed-rate-no-overlap scheduled task should start");

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
        let task_id = mgr
            .add_fixed_rate_task_async(
                Duration::from_millis(50),
                Duration::from_millis(100),
                async move |_ctx| {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .expect("fixed-rate async task should start");

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
        let task_id = mgr
            .add_fixed_delay_task_async(
                Duration::from_millis(10),
                Duration::from_millis(50),
                async move |_ctx| {
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .expect("fixed-delay async task should start");

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
        let task_id = mgr
            .add_fixed_rate_no_overlap_task_async(
                Duration::from_millis(10),
                Duration::from_millis(50),
                async move |_ctx| {
                    time::sleep(Duration::from_millis(80)).await;
                    c.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                },
            )
            .expect("fixed-rate-no-overlap async task should start");

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
