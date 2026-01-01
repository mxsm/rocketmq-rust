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
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

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
    running_tasks: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
    metrics: Arc<RwLock<ExecutionMetrics>>,
    executions: Arc<RwLock<HashMap<String, TaskExecution>>>,
}

impl TaskExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks));

        Self {
            config,
            semaphore,
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(ExecutionMetrics::default())),
            executions: Arc::new(RwLock::new(HashMap::new())),
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

        // Spawn the task execution
        let handle = tokio::spawn(async move {
            executor
                .run_task_internal(task_clone, execution_id_clone, scheduled_time)
                .await;
        });

        // Store the handle
        {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.insert(execution_id.clone(), handle);
        }

        info!("Task scheduled for execution: {} ({})", task.name, execution_id);
        Ok(execution_id)
    }

    /// Cancel a running task
    pub async fn cancel_task(&self, execution_id: &str) -> Result<(), SchedulerError> {
        let handle = {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.remove(execution_id)
        };

        if let Some(handle) = handle {
            handle.abort();

            // Update execution record
            if let Some(mut execution) = self.get_execution(execution_id).await {
                execution.cancel();
                let mut executions = self.executions.write().await;
                executions.insert(execution_id.to_string(), execution);
            }

            info!("Task cancelled: {}", execution_id);
            Ok(())
        } else {
            Err(SchedulerError::TaskNotFound(execution_id.to_string()))
        }
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

        // Spawn the delayed task execution
        let handle = tokio::spawn(async move {
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
        });

        // Store the handle
        {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.insert(execution_id.clone(), handle);
        }

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
    running_tasks: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
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
