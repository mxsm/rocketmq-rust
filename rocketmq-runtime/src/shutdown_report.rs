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

use std::time::Duration;

use serde::Serialize;

use crate::blocking::BlockingTaskSnapshot;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;
use crate::task_group::TaskState;

#[derive(Debug, Clone, Serialize)]
pub struct ShutdownReport {
    pub name: String,
    #[serde(with = "duration_millis")]
    pub elapsed: Duration,
    pub completed: usize,
    pub cancelled: usize,
    pub aborted: usize,
    pub panicked: usize,
    pub timed_out: usize,
    pub leaked: usize,
    pub blocking_still_running: usize,
    pub detached_still_running: usize,
    pub children: Vec<ShutdownReport>,
    pub remaining_tasks: Vec<TaskSnapshot>,
    pub blocking_tasks: Vec<BlockingTaskSnapshot>,
    pub annotations: Vec<ShutdownAnnotation>,
}

impl ShutdownReport {
    pub fn new(name: impl Into<String>, elapsed: Duration) -> Self {
        Self {
            name: name.into(),
            elapsed,
            completed: 0,
            cancelled: 0,
            aborted: 0,
            panicked: 0,
            timed_out: 0,
            leaked: 0,
            blocking_still_running: 0,
            detached_still_running: 0,
            children: Vec::new(),
            remaining_tasks: Vec::new(),
            blocking_tasks: Vec::new(),
            annotations: Vec::new(),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.leaked == 0
            && self.panicked == 0
            && self.timed_out == 0
            && self.blocking_still_running == 0
            && self.children.iter().all(Self::is_healthy)
    }

    pub fn assert_no_task_leak(&self) -> Result<(), String> {
        if self.is_healthy() {
            Ok(())
        } else {
            Err(format!("shutdown report is unhealthy: {}", self.to_json()))
        }
    }

    pub fn log_if_unhealthy(&self) {
        if !self.is_healthy() {
            tracing::warn!(report = %self.to_json(), "runtime shutdown report is unhealthy");
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|error| format!("{{\"serialization_error\":\"{}\"}}", error))
    }

    pub fn merge_blocking(&mut self, snapshot: crate::blocking::BlockingExecutorSnapshot) {
        self.blocking_still_running += snapshot.blocking_still_running;
        self.blocking_tasks.extend(snapshot.tasks);
        if self.blocking_still_running > 0 {
            self.annotations.push(ShutdownAnnotation::new(
                "spawn_blocking tasks may continue after timeout; see blocking_still_running",
            ));
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub name: String,
    pub group_id: TaskGroupId,
    pub group_name: String,
    pub kind: TaskKind,
    pub state: TaskState,
    #[serde(with = "duration_millis")]
    pub elapsed: Duration,
    pub detached: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShutdownAnnotation {
    pub message: String,
}

impl ShutdownAnnotation {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
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
