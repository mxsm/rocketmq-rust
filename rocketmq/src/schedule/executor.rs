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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use rocketmq_runtime::RuntimeHandle;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskId;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::error;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use crate::schedule::task::TaskExecution;
use crate::schedule::SchedulerError;
use crate::schedule::Task;
use crate::schedule::TaskContext;
use crate::schedule::TaskResult;
use crate::schedule::TaskStatus;

/// Task executor pool configuration
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub max_concurrent_tasks: usize,
    pub default_timeout: Duration,
    pub enable_metrics: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 10,
            default_timeout: Duration::from_secs(300), // 5 minutes
            enable_metrics: true,
        }
    }
}

/// Task execution metrics
#[derive(Debug, Clone, Default)]
pub struct ExecutionMetrics {
    pub total_executions: u64,
    pub successful_executions: u64,
    pub failed_executions: u64,
    pub cancelled_executions: u64,
    pub average_execution_time: Duration,
    pub max_execution_time: Duration,
}

/// Task executor responsible for running tasks
pub struct TaskExecutor {
    config: ExecutorConfig,
    semaphore: Arc<Semaphore>,
    running_tasks: Arc<RwLock<HashMap<String, RunningTask>>>,
    metrics: Arc<RwLock<ExecutionMetrics>>,
    executions: Arc<RwLock<HashMap<String, TaskExecution>>>,
    task_group: Arc<RwLock<Option<TaskGroup>>>,
    last_task_group_shutdown_report: Arc<RwLock<Option<ShutdownReport>>>,
}

struct RunningTask {
    task_id: TaskId,
    task_group: TaskGroup,
}

fn new_executor_task_group(operation: &'static str) -> Result<TaskGroup, SchedulerError> {
    let handle = tokio::runtime::Handle::try_current()
        .map_err(|error| SchedulerError::SystemError(format!("{operation} requires a Tokio runtime: {error}")))?;
    Ok(TaskGroup::root("rocketmq.task-executor", RuntimeHandle::new(handle)))
}

fn spawn_executor_task<F>(
    task_group: &TaskGroup,
    operation: &'static str,
    task_name: String,
    future: F,
) -> Result<TaskId, SchedulerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    task_group
        .spawn_service(task_name, future)
        .map_err(|error| SchedulerError::SystemError(format!("{operation} failed to spawn task execution: {error}")))
}

impl TaskExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        Self::new_with_optional_task_group(config, None)
    }

    pub fn new_with_task_group(config: ExecutorConfig, parent_task_group: TaskGroup) -> Self {
        Self::new_with_optional_task_group(config, Some(parent_task_group.child("rocketmq.task-executor")))
    }

    fn new_with_optional_task_group(config: ExecutorConfig, task_group: Option<TaskGroup>) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));

        Self {
            config,
            semaphore,
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(ExecutionMetrics::default())),
            executions: Arc::new(RwLock::new(HashMap::new())),
            task_group: Arc::new(RwLock::new(task_group)),
            last_task_group_shutdown_report: Arc::new(RwLock::new(None)),
        }
    }

    /// Execute a task asynchronously
    pub async fn execute_task(&self, task: Arc<Task>, scheduled_time: SystemTime) -> Result<String, SchedulerError> {
        let execution_id = Uuid::new_v4().to_string();
        let mut execution = TaskExecution::new(task.id.clone(), scheduled_time);
        execution.execution_id = execution_id.clone();

        // Store execution record
        {
            let mut executions = self.executions.write().await;
            executions.insert(execution_id.clone(), execution.clone());
        }

        // Clone necessary data for the task
        let executor = self.clone_for_task();
        let task_clone = task.clone();
        let execution_id_clone = execution_id.clone();
        let task_group = self.task_group("TaskExecutor::execute_task").await?;

        // Spawn the task execution
        let task_id = spawn_executor_task(
            &task_group,
            "TaskExecutor::execute_task",
            format!("task-executor.execution.{execution_id}"),
            async move {
                executor
                    .run_task_internal(task_clone, execution_id_clone, scheduled_time)
                    .await;
            },
        )?;

        self.store_running_task(execution_id.as_str(), RunningTask { task_id, task_group })
            .await;

        info!("Task scheduled for execution: {} ({})", task.name, execution_id);
        Ok(execution_id)
    }

    /// Cancel a running task
    pub async fn cancel_task(&self, execution_id: &str) -> Result<(), SchedulerError> {
        let handle = {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.remove(execution_id)
        };

        if let Some(running_task) = handle {
            if !running_task
                .task_group
                .abort_task_and_wait(running_task.task_id, self.config.default_timeout)
                .await
            {
                warn!("Cancelled task {} did not finish before timeout", execution_id);
            }

            self.mark_execution_cancelled(execution_id).await;

            info!("Task cancelled: {}", execution_id);
            Ok(())
        } else {
            Err(SchedulerError::TaskNotFound(execution_id.to_string()))
        }
    }

    /// Abort all running task executions and wait for their futures to be dropped.
    pub async fn shutdown_all(&self, join_timeout: Duration) -> usize {
        let handles = {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.drain().collect::<Vec<_>>()
        };

        let mut cancelled = 0;
        for (execution_id, running_task) in handles {
            if !running_task
                .task_group
                .abort_task_and_wait(running_task.task_id, join_timeout)
                .await
            {
                warn!("Shutdown task {} timed out after abort", execution_id);
            }
            self.mark_execution_cancelled(execution_id.as_str()).await;
            cancelled += 1;
        }

        self.shutdown_task_group(join_timeout).await;
        cancelled
    }

    /// Get task execution status
    pub async fn get_execution(&self, execution_id: &str) -> Option<TaskExecution> {
        let executions = self.executions.read().await;
        executions.get(execution_id).cloned()
    }

    /// Get all executions for a task
    pub async fn get_task_executions(&self, task_id: &str) -> Vec<TaskExecution> {
        let executions = self.executions.read().await;
        executions.values().filter(|e| e.task_id == task_id).cloned().collect()
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> ExecutionMetrics {
        let metrics = self.metrics.read().await;
        metrics.clone()
    }

    /// Get number of running tasks
    pub async fn running_task_count(&self) -> usize {
        let running_tasks = self.running_tasks.read().await;
        running_tasks.len()
    }

    pub async fn task_group_task_count(&self) -> usize {
        self.task_group
            .read()
            .await
            .as_ref()
            .map(TaskGroup::task_count)
            .unwrap_or_default()
    }

    pub async fn last_task_group_shutdown_report(&self) -> Option<ShutdownReport> {
        self.last_task_group_shutdown_report.read().await.clone()
    }

    /// Clean up completed executions older than the specified duration
    pub async fn cleanup_old_executions(&self, older_than: Duration) {
        let cutoff_time = SystemTime::now() - older_than;
        let mut executions = self.executions.write().await;

        executions.retain(|_, execution| match execution.status {
            TaskStatus::Pending | TaskStatus::Running => true,
            _ => execution.end_time.is_none_or(|end| end > cutoff_time),
        });
    }

    // Internal methods

    fn clone_for_task(&self) -> TaskExecutorInternal {
        TaskExecutorInternal {
            config: self.config.clone(),
            semaphore: self.semaphore.clone(),
            running_tasks: self.running_tasks.clone(),
            metrics: self.metrics.clone(),
            executions: self.executions.clone(),
        }
    }

    async fn run_task_internal(&self, task: Arc<Task>, execution_id: String, scheduled_time: SystemTime) {
        let internal = self.clone_for_task();
        internal.run_task_internal(task, execution_id, scheduled_time).await;
    }

    async fn task_group(&self, operation: &'static str) -> Result<TaskGroup, SchedulerError> {
        let mut task_group = self.task_group.write().await;
        if let Some(task_group) = task_group.as_ref() {
            return Ok(task_group.clone());
        }

        let new_group = new_executor_task_group(operation)?;
        *task_group = Some(new_group.clone());
        Ok(new_group)
    }

    async fn store_running_task(&self, execution_id: &str, running_task: RunningTask) {
        let mut running_tasks = self.running_tasks.write().await;
        running_tasks.insert(execution_id.to_string(), running_task);
        if running_tasks
            .get(execution_id)
            .is_some_and(|running_task| !running_task.task_group.contains_task(running_task.task_id))
        {
            running_tasks.remove(execution_id);
        }
    }

    async fn mark_execution_cancelled(&self, execution_id: &str) {
        if let Some(mut execution) = self.get_execution(execution_id).await {
            execution.cancel();
            let mut executions = self.executions.write().await;
            executions.insert(execution_id.to_string(), execution);
        }
    }

    async fn shutdown_task_group(&self, timeout: Duration) {
        let task_group = self.task_group.write().await.take();
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(timeout).await;
            report.log_if_unhealthy();
            *self.last_task_group_shutdown_report.write().await = Some(report);
        }
    }

    pub async fn execute_task_with_delay(
        &self,
        task: Arc<Task>,
        scheduled_time: SystemTime,
        execution_delay: Option<Duration>,
    ) -> Result<String, SchedulerError> {
        let execution_id = Uuid::new_v4().to_string();
        let mut execution = TaskExecution::new(task.id.clone(), scheduled_time);
        execution.execution_id = execution_id.clone();

        // Calculate actual execution time considering delay
        let actual_execution_time = if let Some(delay) = execution_delay.or(task.execution_delay) {
            scheduled_time + delay
        } else {
            scheduled_time
        };

        // Store execution record
        {
            let mut executions = self.executions.write().await;
            executions.insert(execution_id.clone(), execution.clone());
        }

        // Clone necessary data for the task
        let executor = self.clone_for_task();
        let task_clone = task.clone();
        let execution_id_clone = execution_id.clone();
        let task_group = self.task_group("TaskExecutor::execute_task_with_delay").await?;

        // Spawn the delayed task execution
        let task_id = spawn_executor_task(
            &task_group,
            "TaskExecutor::execute_task_with_delay",
            format!("task-executor.delayed-execution.{execution_id}"),
            async move {
                // Wait until actual execution time
                let now = SystemTime::now();
                if actual_execution_time > now {
                    if let Ok(delay_duration) = actual_execution_time.duration_since(now) {
                        tokio::time::sleep(delay_duration).await;
                    }
                }

                executor
                    .run_task_internal(task_clone, execution_id_clone, actual_execution_time)
                    .await;
            },
        )?;

        self.store_running_task(execution_id.as_str(), RunningTask { task_id, task_group })
            .await;

        info!(
            "Task scheduled for delayed execution: {} ({}) - delay: {:?}",
            task.name,
            execution_id,
            execution_delay.or(task.execution_delay)
        );
        Ok(execution_id)
    }
}

// Internal executor for task execution
#[derive(Clone)]
struct TaskExecutorInternal {
    config: ExecutorConfig,
    semaphore: Arc<Semaphore>,
    running_tasks: Arc<RwLock<HashMap<String, RunningTask>>>,
    metrics: Arc<RwLock<ExecutionMetrics>>,
    executions: Arc<RwLock<HashMap<String, TaskExecution>>>,
}

impl TaskExecutorInternal {
    async fn run_task_internal(&self, task: Arc<Task>, execution_id: String, scheduled_time: SystemTime) {
        // Acquire semaphore permit
        let _permit = match self.semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                error!("Failed to acquire semaphore permit for task: {}", task.id);
                return;
            }
        };

        // Update execution record - start
        let mut context = TaskContext::new(task.id.clone(), scheduled_time);
        context.execution_id = execution_id.clone();
        context.mark_started();

        {
            let mut executions = self.executions.write().await;
            if let Some(execution) = executions.get_mut(&execution_id) {
                execution.start();
            }
        }

        info!("Starting task execution: {} ({})", task.name, execution_id);

        // Execute the task with timeout
        let task_timeout = task.timeout.unwrap_or(self.config.default_timeout);
        let result = match timeout(task_timeout, task.execute(context)).await {
            Ok(result) => result,
            Err(_) => {
                warn!("Task execution timed out: {} ({})", task.name, execution_id);
                TaskResult::Failed("Task execution timed out".to_string())
            }
        };

        // Update execution record - complete
        {
            let mut executions = self.executions.write().await;
            if let Some(execution) = executions.get_mut(&execution_id) {
                execution.complete(result.clone());
            }
        }

        // Update metrics
        if self.config.enable_metrics {
            self.update_metrics(&result).await;
        }

        // Remove from running tasks
        {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.remove(&execution_id);
        }

        match result {
            TaskResult::Success(msg) => {
                info!(
                    "Task completed successfully: {} ({}) - {:?}",
                    task.name, execution_id, msg
                );
            }
            TaskResult::Failed(err) => {
                error!("Task failed: {} ({}) - {}", task.name, execution_id, err);
            }
            TaskResult::Skipped(reason) => {
                info!("Task skipped: {} ({}) - {}", task.name, execution_id, reason);
            }
        }
    }

    async fn update_metrics(&self, result: &TaskResult) {
        let mut metrics = self.metrics.write().await;
        metrics.total_executions += 1;

        match result {
            TaskResult::Success(_) => metrics.successful_executions += 1,
            TaskResult::Failed(_) => metrics.failed_executions += 1,
            TaskResult::Skipped(_) => metrics.successful_executions += 1,
        }
    }
}

/// Executor pool for managing multiple executors
pub struct ExecutorPool {
    executors: Vec<Arc<TaskExecutor>>,
    current_index: Arc<RwLock<usize>>,
}

impl ExecutorPool {
    pub fn new(pool_size: usize, config: ExecutorConfig) -> Self {
        let executors = (0..pool_size)
            .map(|_| Arc::new(TaskExecutor::new(config.clone())))
            .collect();

        Self {
            executors,
            current_index: Arc::new(RwLock::new(0)),
        }
    }

    pub fn new_with_task_group(pool_size: usize, config: ExecutorConfig, parent_task_group: TaskGroup) -> Self {
        let executors = (0..pool_size)
            .map(|index| {
                Arc::new(TaskExecutor::new_with_task_group(
                    config.clone(),
                    parent_task_group.child(format!("rocketmq.task-executor.{index}")),
                ))
            })
            .collect();

        Self {
            executors,
            current_index: Arc::new(RwLock::new(0)),
        }
    }

    /// Get the next executor using round-robin
    pub async fn get_executor(&self) -> Arc<TaskExecutor> {
        let mut index = self.current_index.write().await;
        let executor = self.executors[*index].clone();
        *index = (*index + 1) % self.executors.len();
        executor
    }

    /// Get total number of running tasks across all executors
    pub async fn total_running_tasks(&self) -> usize {
        let mut total = 0;
        for executor in &self.executors {
            total += executor.running_task_count().await;
        }
        total
    }

    /// Abort all running tasks across the pool.
    pub async fn shutdown_all(&self, join_timeout: Duration) -> usize {
        let mut total = 0;
        for executor in &self.executors {
            total += executor.shutdown_all(join_timeout).await;
        }
        total
    }

    /// Get combined metrics from all executors
    pub async fn combined_metrics(&self) -> ExecutionMetrics {
        let mut combined = ExecutionMetrics::default();

        for executor in &self.executors {
            let metrics = executor.get_metrics().await;
            combined.total_executions += metrics.total_executions;
            combined.successful_executions += metrics.successful_executions;
            combined.failed_executions += metrics.failed_executions;
            combined.cancelled_executions += metrics.cancelled_executions;

            if metrics.max_execution_time > combined.max_execution_time {
                combined.max_execution_time = metrics.max_execution_time;
            }
        }

        combined
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use rocketmq_runtime::RuntimeContext;

    use super::*;

    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn executor_task_group_without_tokio_runtime_returns_error() {
        let error = match new_executor_task_group("test-executor-spawn") {
            Ok(_) => panic!("new_executor_task_group should fail without an ambient Tokio runtime"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            SchedulerError::SystemError(message) if message.contains("requires a Tokio runtime")
        ));
    }

    #[tokio::test]
    async fn store_running_task_drops_already_finished_task() {
        let executor = TaskExecutor::new(ExecutorConfig::default());
        let task_group = executor
            .task_group("test-store-running-task-handle")
            .await
            .expect("task group should be available");
        let task_id = spawn_executor_task(
            &task_group,
            "test-store-running-task-handle",
            "finished-task".to_string(),
            async {},
        )
        .expect("task should spawn");

        while task_group.contains_task(task_id) {
            tokio::task::yield_now().await;
        }

        executor
            .store_running_task("finished-task", RunningTask { task_id, task_group })
            .await;

        assert_eq!(executor.running_task_count().await, 0);
    }

    #[tokio::test]
    async fn new_with_task_group_parents_executor_tasks() {
        let context = RuntimeContext::from_current("task-executor-parent-test");
        let service = context.service_context("executor-service");
        let executor = TaskExecutor::new_with_task_group(ExecutorConfig::default(), service.task_group().clone());
        let task = Arc::new(Task::new("parented-task", "parented task", |_context| async {
            TaskResult::Success(None)
        }));

        executor
            .execute_task(task, SystemTime::now())
            .await
            .expect("parented executor task should start");
        executor.shutdown_all(Duration::from_secs(1)).await;

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq.task-executor"),
            "{}",
            report.to_json()
        );
    }

    #[tokio::test]
    async fn cancel_task_waits_until_future_is_dropped() {
        let executor = TaskExecutor::new(ExecutorConfig::default());
        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let task = Arc::new(Task::new("pending-task", "pending-task", {
            let started = Arc::clone(&started);
            let dropped = Arc::clone(&dropped);
            move |_context| {
                let started = Arc::clone(&started);
                let dropped = Arc::clone(&dropped);
                async move {
                    let _marker = DropMarker(dropped);
                    started.store(true, Ordering::Release);
                    future::pending::<TaskResult>().await
                }
            }
        }));

        let execution_id = executor
            .execute_task(task, SystemTime::now())
            .await
            .expect("task should be scheduled");
        assert_eq!(executor.task_group_task_count().await, 1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("task should start");

        executor
            .cancel_task(execution_id.as_str())
            .await
            .expect("task should cancel");

        assert!(
            dropped.load(Ordering::Acquire),
            "cancel_task should wait until the aborted task future is dropped"
        );
        assert_eq!(executor.running_task_count().await, 0);
        assert_eq!(executor.task_group_task_count().await, 0);
        let execution = executor
            .get_execution(execution_id.as_str())
            .await
            .expect("execution record should exist");
        assert_eq!(execution.status, TaskStatus::Cancelled);
    }

    #[tokio::test]
    async fn shutdown_all_aborts_and_waits_for_running_tasks() {
        let executor = TaskExecutor::new(ExecutorConfig::default());
        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let task = Arc::new(Task::new("pending-task", "pending-task", {
            let started = Arc::clone(&started);
            let dropped = Arc::clone(&dropped);
            move |_context| {
                let started = Arc::clone(&started);
                let dropped = Arc::clone(&dropped);
                async move {
                    let _marker = DropMarker(dropped);
                    started.store(true, Ordering::Release);
                    future::pending::<TaskResult>().await
                }
            }
        }));

        let execution_id = executor
            .execute_task(task, SystemTime::now())
            .await
            .expect("task should be scheduled");
        assert_eq!(executor.task_group_task_count().await, 1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("task should start");

        assert_eq!(executor.shutdown_all(Duration::from_secs(1)).await, 1);

        assert!(
            dropped.load(Ordering::Acquire),
            "shutdown_all should wait until the aborted task future is dropped"
        );
        assert_eq!(executor.running_task_count().await, 0);
        assert_eq!(executor.task_group_task_count().await, 0);
        let report = executor
            .last_task_group_shutdown_report()
            .await
            .expect("task group shutdown report should be recorded");
        assert!(report.is_healthy(), "{}", report.to_json());
        assert_eq!(report.aborted, 1);
        let execution = executor
            .get_execution(execution_id.as_str())
            .await
            .expect("execution record should exist");
        assert_eq!(execution.status, TaskStatus::Cancelled);
    }
}
