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
use tokio::time::interval;
use tracing::error;
use tracing::info;
use uuid::Uuid;

use crate::schedule::executor::ExecutorConfig;
use crate::schedule::task::TaskExecution;
use crate::schedule::trigger::DelayedIntervalTrigger;
use crate::schedule::trigger::Trigger;
use crate::schedule::ExecutorPool;
use crate::schedule::SchedulerError;
use crate::schedule::SchedulerResult;
use crate::schedule::Task;
use crate::DelayTrigger;

/// Scheduler configuration
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub executor_config: ExecutorConfig,
    pub executor_pool_size: usize,
    pub check_interval: Duration,
    pub max_scheduler_threads: usize,
    pub enable_persistence: bool,
    pub persistence_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            executor_config: ExecutorConfig::default(),
            executor_pool_size: 3,
            check_interval: Duration::from_secs(1),
            max_scheduler_threads: 2,
            enable_persistence: false,
            persistence_interval: Duration::from_secs(60),
        }
    }
}

/// Scheduled job containing task and trigger
#[derive(Clone)]
pub struct ScheduledJob {
    pub id: String,
    pub task: Arc<Task>,
    pub trigger: Arc<dyn Trigger>,
    pub next_execution: Option<SystemTime>,
    pub enabled: bool,
    pub created_at: SystemTime,
    pub last_execution: Option<SystemTime>,
}

impl ScheduledJob {
    pub fn new(task: Arc<Task>, trigger: Arc<dyn Trigger>) -> Self {
        let next_execution = trigger.next_execution_time(SystemTime::now());

        Self {
            id: Uuid::new_v4().to_string(),
            task,
            trigger,
            next_execution,
            enabled: true,
            created_at: SystemTime::now(),
            last_execution: None,
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn update_next_execution(&mut self) {
        let after = self.last_execution.unwrap_or_else(SystemTime::now);
        self.next_execution = self.trigger.next_execution_time(after);
    }

    pub fn should_execute(&self, now: SystemTime) -> bool {
        self.enabled && self.next_execution.is_some_and(|next| next <= now)
    }
}

/// Main task scheduler
pub struct TaskScheduler {
    config: SchedulerConfig,
    executor_pool: Arc<ExecutorPool>,
    jobs: Arc<RwLock<HashMap<String, ScheduledJob>>>,
    running: Arc<RwLock<bool>>,
    scheduler_handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Default for TaskScheduler {
    fn default() -> Self {
        Self::new(SchedulerConfig::default())
    }
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new(config: SchedulerConfig) -> Self {
        let executor_pool = Arc::new(ExecutorPool::new(
            config.executor_pool_size,
            config.executor_config.clone(),
        ));

        Self {
            config,
            executor_pool,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
            scheduler_handles: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start the scheduler
    pub async fn start(&self) -> SchedulerResult<()> {
        let mut running = self.running.write().await;
        if *running {
            return Err(SchedulerError::SystemError("Scheduler is already running".to_string()));
        }
        *running = true;

        info!("Starting task scheduler");

        // Start scheduler threads
        let mut handles = self.scheduler_handles.write().await;

        for i in 0..self.config.max_scheduler_threads {
            let scheduler = self.clone_for_thread();
            let handle = tokio::spawn(async move {
                scheduler.scheduler_loop(i).await;
            });
            handles.push(handle);
        }

        // Start cleanup thread
        if self.config.enable_persistence {
            let scheduler = self.clone_for_thread();
            let handle = tokio::spawn(async move {
                scheduler.cleanup_loop().await;
            });
            handles.push(handle);
        }

        info!(
            "Task scheduler started with {} threads",
            self.config.max_scheduler_threads
        );
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) -> SchedulerResult<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        *running = false;

        info!("Stopping task scheduler");

        // Cancel all scheduler threads
        let mut handles = self.scheduler_handles.write().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        info!("Task scheduler stopped");
        Ok(())
    }

    /// Schedule a new job
    pub async fn schedule_job(&self, task: Arc<Task>, trigger: Arc<dyn Trigger>) -> SchedulerResult<String> {
        let job = ScheduledJob::new(task.clone(), trigger);
        let job_id = job.id.clone();

        let mut jobs = self.jobs.write().await;
        if jobs.contains_key(&task.id) {
            return Err(SchedulerError::TaskAlreadyExists(task.id.clone()));
        }

        jobs.insert(job_id.clone(), job);
        info!("Job scheduled: {} ({})", task.name, job_id);

        Ok(job_id)
    }

    /// Schedule a job with a specific ID
    pub async fn schedule_job_with_id(
        &self,
        job_id: impl Into<String>,
        task: Arc<Task>,
        trigger: Arc<dyn Trigger>,
    ) -> SchedulerResult<String> {
        let job_id = job_id.into();
        let job = ScheduledJob::new(task.clone(), trigger).with_id(job_id.clone());

        let mut jobs = self.jobs.write().await;
        if jobs.contains_key(&job_id) {
            return Err(SchedulerError::TaskAlreadyExists(job_id));
        }

        jobs.insert(job_id.clone(), job);
        info!("Job scheduled with ID: {} ({})", task.name, job_id);

        Ok(job_id)
    }

    /// Unschedule a job
    pub async fn unschedule_job(&self, job_id: &str) -> SchedulerResult<()> {
        let mut jobs = self.jobs.write().await;
        if jobs.remove(job_id).is_some() {
            info!("Job unscheduled: {}", job_id);
            Ok(())
        } else {
            Err(SchedulerError::TaskNotFound(job_id.to_string()))
        }
    }

    /// Enable or disable a job
    pub async fn set_job_enabled(&self, job_id: &str, enabled: bool) -> SchedulerResult<()> {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            job.enabled = enabled;
            info!(
                "Job {} {}: {}",
                if enabled { "enabled" } else { "disabled" },
                job_id,
                job.task.name
            );
            Ok(())
        } else {
            Err(SchedulerError::TaskNotFound(job_id.to_string()))
        }
    }

    /// Get job information
    pub async fn get_job(&self, job_id: &str) -> Option<ScheduledJob> {
        let jobs = self.jobs.read().await;
        jobs.get(job_id).cloned()
    }

    /// Get all jobs
    pub async fn get_all_jobs(&self) -> Vec<ScheduledJob> {
        let jobs = self.jobs.read().await;
        jobs.values().cloned().collect()
    }

    /// Get jobs by task group
    pub async fn get_jobs_by_group(&self, group: &str) -> Vec<ScheduledJob> {
        let jobs = self.jobs.read().await;
        jobs.values()
            .filter(|job| job.task.group.as_ref().is_some_and(|g| g == group))
            .cloned()
            .collect()
    }

    /// Execute a job immediately (ad-hoc execution)
    pub async fn execute_job_now(&self, job_id: &str) -> SchedulerResult<String> {
        let job = {
            let jobs = self.jobs.read().await;
            jobs.get(job_id).cloned()
        };

        if let Some(job) = job {
            let executor = self.executor_pool.get_executor().await;
            let execution_id = executor.execute_task(job.task, SystemTime::now()).await?;
            info!("Job executed immediately: {} ({})", job_id, execution_id);
            Ok(execution_id)
        } else {
            Err(SchedulerError::TaskNotFound(job_id.to_string()))
        }
    }

    /// Schedule a delayed job (execute once after delay)
    pub async fn schedule_delayed_job(&self, task: Arc<Task>, delay: Duration) -> SchedulerResult<String> {
        let trigger = Arc::new(DelayTrigger::new(delay));
        self.schedule_job(task, trigger).await
    }

    /// Schedule a delayed job with specific ID
    pub async fn schedule_delayed_job_with_id(
        &self,
        job_id: impl Into<String>,
        task: Arc<Task>,
        delay: Duration,
    ) -> SchedulerResult<String> {
        let trigger = Arc::new(DelayTrigger::new(delay));
        self.schedule_job_with_id(job_id, task, trigger).await
    }

    /// Schedule an interval job with initial delay
    pub async fn schedule_interval_job_with_delay(
        &self,
        task: Arc<Task>,
        interval: Duration,
        initial_delay: Duration,
    ) -> SchedulerResult<String> {
        let trigger = Arc::new(DelayedIntervalTrigger::new(interval, initial_delay));
        self.schedule_job(task, trigger).await
    }

    /// Schedule an interval job with initial delay and specific ID
    pub async fn schedule_interval_job_with_delay_and_id(
        &self,
        job_id: impl Into<String>,
        task: Arc<Task>,
        interval: Duration,
        initial_delay: Duration,
    ) -> SchedulerResult<String> {
        let trigger = Arc::new(DelayedIntervalTrigger::new(interval, initial_delay));
        self.schedule_job_with_id(job_id, task, trigger).await
    }

    /// Execute a job immediately with optional execution delay
    pub async fn execute_job_now_with_delay(
        &self,
        job_id: &str,
        execution_delay: Option<Duration>,
    ) -> SchedulerResult<String> {
        let job = {
            let jobs = self.jobs.read().await;
            jobs.get(job_id).cloned()
        };

        if let Some(job) = job {
            let executor = self.executor_pool.get_executor().await;
            let execution_id = executor
                .execute_task_with_delay(job.task, SystemTime::now(), execution_delay)
                .await?;
            info!("Job executed immediately with delay: {} ({})", job_id, execution_id);
            Ok(execution_id)
        } else {
            Err(SchedulerError::TaskNotFound(job_id.to_string()))
        }
    }

    /// Get task execution
    pub async fn get_execution(&self, execution_id: &str) -> Option<TaskExecution> {
        // Try to find the execution in any executor
        for _ in 0..self.config.executor_pool_size {
            let executor = self.executor_pool.get_executor().await;
            if let Some(execution) = executor.get_execution(execution_id).await {
                return Some(execution);
            }
        }
        None
    }

    /// Cancel a running task execution
    pub async fn cancel_execution(&self, execution_id: &str) -> SchedulerResult<()> {
        // Try to cancel in any executor
        for _ in 0..self.config.executor_pool_size {
            let executor = self.executor_pool.get_executor().await;
            if executor.cancel_task(execution_id).await.is_ok() {
                return Ok(());
            }
        }
        Err(SchedulerError::TaskNotFound(execution_id.to_string()))
    }

    /// Get scheduler status
    pub async fn get_status(&self) -> SchedulerStatus {
        let jobs = self.jobs.read().await;
        let running = *self.running.read().await;
        let total_jobs = jobs.len();
        let enabled_jobs = jobs.values().filter(|job| job.enabled).count();
        let running_tasks = self.executor_pool.total_running_tasks().await;

        SchedulerStatus {
            running,
            total_jobs,
            enabled_jobs,
            running_tasks,
        }
    }

    // Internal methods

    fn clone_for_thread(&self) -> TaskSchedulerInternal {
        TaskSchedulerInternal {
            config: self.config.clone(),
            executor_pool: self.executor_pool.clone(),
            jobs: self.jobs.clone(),
            running: self.running.clone(),
        }
    }

    async fn scheduler_loop(&self, thread_id: usize) {
        let internal = self.clone_for_thread();
        internal.scheduler_loop(thread_id).await;
    }

    async fn cleanup_loop(&self) {
        let internal = self.clone_for_thread();
        internal.cleanup_loop().await;
    }
}

// Internal scheduler for running the scheduling loop
#[derive(Clone)]
struct TaskSchedulerInternal {
    config: SchedulerConfig,
    executor_pool: Arc<ExecutorPool>,
    jobs: Arc<RwLock<HashMap<String, ScheduledJob>>>,
    running: Arc<RwLock<bool>>,
}

impl TaskSchedulerInternal {
    async fn scheduler_loop(&self, thread_id: usize) {
        info!("Scheduler thread {} started", thread_id);

        let mut interval = interval(self.config.check_interval);

        while *self.running.read().await {
            interval.tick().await;

            let now = SystemTime::now();
            let jobs_to_execute = self.get_jobs_to_execute(now).await;

            for job_id in jobs_to_execute {
                if !*self.running.read().await {
                    break;
                }

                self.execute_job(&job_id, now).await;
            }
        }

        info!("Scheduler thread {} stopped", thread_id);
    }

    async fn cleanup_loop(&self) {
        info!("Cleanup thread started");

        let mut interval = interval(self.config.persistence_interval);

        while *self.running.read().await {
            interval.tick().await;

            // Clean up old executions (keep last 24 hours)
            let cleanup_duration = Duration::from_secs(24 * 3600);
            for _ in 0..self.config.executor_pool_size {
                let executor = self.executor_pool.get_executor().await;
                executor.cleanup_old_executions(cleanup_duration).await;
            }
        }

        info!("Cleanup thread stopped");
    }

    async fn get_jobs_to_execute(&self, now: SystemTime) -> Vec<String> {
        let jobs = self.jobs.read().await;
        jobs.values()
            .filter(|job| job.should_execute(now))
            .map(|job| job.id.clone())
            .collect()
    }

    async fn execute_job(&self, job_id: &str, now: SystemTime) {
        let job = {
            let jobs = self.jobs.read().await;
            jobs.get(job_id).cloned()
        };

        if let Some(mut job) = job {
            // Get executor and execute task
            let executor = self.executor_pool.get_executor().await;

            match executor.execute_task(job.task.clone(), now).await {
                Ok(execution_id) => {
                    // Update job's last execution and next execution time
                    job.last_execution = Some(now);
                    job.update_next_execution();

                    // Update the job in storage
                    let mut jobs = self.jobs.write().await;
                    jobs.insert(job_id.to_string(), job);

                    info!("Job executed: {} ({})", job_id, execution_id);
                }
                Err(e) => {
                    error!("Failed to execute job {}: {}", job_id, e);
                }
            }
        }
    }
}

/// Scheduler status information
#[derive(Debug, Clone)]
pub struct SchedulerStatus {
    pub running: bool,
    pub total_jobs: usize,
    pub enabled_jobs: usize,
    pub running_tasks: usize,
}
