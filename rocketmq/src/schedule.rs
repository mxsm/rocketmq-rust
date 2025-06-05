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
