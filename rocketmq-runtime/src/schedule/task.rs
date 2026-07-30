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
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use uuid::Uuid;

/// Task execution result.
#[derive(Debug, Clone)]
pub enum TaskResult {
    /// Represents the success case.
    Success(Option<String>),
    /// Represents the failed case.
    Failed(String),
    /// Represents the skipped case.
    Skipped(String),
}

/// Task execution status
#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    /// Represents the pending case.
    Pending,
    /// Represents the running case.
    Running,
    /// Represents the completed case.
    Completed,
    /// Represents the failed case.
    Failed,
    /// Represents the cancelled case.
    Cancelled,
}

/// Task execution context
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// The task identifier.
    pub task_id: String,
    /// The execution identifier.
    pub execution_id: String,
    /// The scheduled time value.
    pub scheduled_time: SystemTime,
    /// The actual start time value.
    pub actual_start_time: Option<SystemTime>,
    /// The metadata value.
    pub metadata: HashMap<String, String>,
}

impl TaskContext {
    /// Creates a new `TaskContext`.
    pub fn new(task_id: String, scheduled_time: SystemTime) -> Self {
        Self {
            task_id,
            execution_id: Uuid::new_v4().to_string(),
            scheduled_time,
            actual_start_time: None,
            metadata: HashMap::new(),
        }
    }

    /// Sets metadata and returns the updated value.
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Executes mark started.
    pub fn mark_started(&mut self) {
        self.actual_start_time = Some(SystemTime::now());
    }

    /// Returns the execution delay.
    pub fn execution_delay(&self) -> Option<Duration> {
        self.actual_start_time
            .and_then(|start| start.duration_since(self.scheduled_time).ok())
    }
}

/// Task execution function type
pub type TaskFn = dyn Fn(TaskContext) -> Pin<Box<dyn Future<Output = TaskResult> + Send>> + Send + Sync;

/// Schedulable task
#[derive(Clone)]
pub struct Task {
    /// The id identifier.
    pub id: String,
    /// The name value.
    pub name: String,
    /// The description value.
    pub description: Option<String>,
    /// The group value.
    pub group: Option<String>,
    /// The priority value.
    pub priority: i32,
    /// The max retry value.
    pub max_retry: u32,
    /// The timeout value.
    pub timeout: Option<Duration>,
    /// Whether enabled.
    pub enabled: bool,
    /// The initial delay value.
    pub initial_delay: Option<Duration>,
    /// The execution delay value.
    pub execution_delay: Option<Duration>,
    executor: Arc<TaskFn>,
    metadata: HashMap<String, String>,
}

impl Task {
    /// Creates a new `Task`.
    pub fn new<F, Fut>(id: impl Into<String>, name: impl Into<String>, executor: F) -> Self
    where
        F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        let executor = Arc::new(move |ctx| {
            let fut = executor(ctx);
            Box::pin(fut) as Pin<Box<dyn Future<Output = TaskResult> + Send>>
        });

        Self {
            id: id.into(),
            name: name.into(),
            description: None,
            group: None,
            priority: 0,
            max_retry: 0,
            timeout: None,
            enabled: true,
            initial_delay: None,
            execution_delay: None,
            executor,
            metadata: HashMap::new(),
        }
    }

    /// Set initial delay before first execution
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = Some(delay);
        self
    }

    /// Set delay before each execution
    pub fn with_execution_delay(mut self, delay: Duration) -> Self {
        self.execution_delay = Some(delay);
        self
    }

    /// Sets description and returns the updated value.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets group and returns the updated value.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Sets priority and returns the updated value.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Sets max retry and returns the updated value.
    pub fn with_max_retry(mut self, max_retry: u32) -> Self {
        self.max_retry = max_retry;
        self
    }

    /// Sets timeout and returns the updated value.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets metadata and returns the updated value.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Returns the enabled.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Returns the execute.
    pub async fn execute(&self, context: TaskContext) -> TaskResult {
        if !self.enabled {
            return TaskResult::Skipped("Task is disabled".to_string());
        }

        (self.executor)(context).await
    }

    /// Returns metadata.
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("description", &self.description)
            .field("group", &self.group)
            .field("priority", &self.priority)
            .field("max_retry", &self.max_retry)
            .field("timeout", &self.timeout)
            .field("enabled", &self.enabled)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Task execution record
#[derive(Debug, Clone)]
pub struct TaskExecution {
    /// The execution identifier.
    pub execution_id: String,
    /// The task identifier.
    pub task_id: String,
    /// The scheduled time value.
    pub scheduled_time: SystemTime,
    /// The start time value.
    pub start_time: Option<SystemTime>,
    /// The end time value.
    pub end_time: Option<SystemTime>,
    /// The status value.
    pub status: TaskStatus,
    /// The result value.
    pub result: Option<TaskResult>,
    /// The number of retry entries.
    pub retry_count: u32,
    /// The error message value.
    pub error_message: Option<String>,
}

impl TaskExecution {
    /// Creates a new `TaskExecution`.
    pub fn new(task_id: String, scheduled_time: SystemTime) -> Self {
        Self {
            execution_id: Uuid::new_v4().to_string(),
            task_id,
            scheduled_time,
            start_time: None,
            end_time: None,
            status: TaskStatus::Pending,
            result: None,
            retry_count: 0,
            error_message: None,
        }
    }

    /// Starts the owned service.
    pub fn start(&mut self) {
        self.start_time = Some(SystemTime::now());
        self.status = TaskStatus::Running;
    }

    /// Completes the operation with the supplied result.
    pub fn complete(&mut self, result: TaskResult) {
        self.end_time = Some(SystemTime::now());
        self.status = match &result {
            TaskResult::Success(_) => TaskStatus::Completed,
            TaskResult::Failed(msg) => {
                self.error_message = Some(msg.clone());
                TaskStatus::Failed
            }
            TaskResult::Skipped(_) => TaskStatus::Completed,
        };
        self.result = Some(result);
    }

    /// Executes cancel.
    pub fn cancel(&mut self) {
        self.end_time = Some(SystemTime::now());
        self.status = TaskStatus::Cancelled;
    }

    /// Returns the duration.
    pub fn duration(&self) -> Option<Duration> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => end.duration_since(start).ok(),
            _ => None,
        }
    }
}
