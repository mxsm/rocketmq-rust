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
use crate::task_group::DetachedTaskPolicy;
use crate::task_group::TaskGroupId;
use crate::task_group::TaskId;
use crate::task_group::TaskKind;
use crate::task_group::TaskState;

#[derive(Debug, Clone, Serialize)]
/// Represents shutdown report.
pub struct ShutdownReport {
    /// The name value.
    pub name: String,
    #[serde(with = "duration_millis")]
    /// The elapsed value.
    pub elapsed: Duration,
    /// The completed value.
    pub completed: usize,
    /// The cancelled value.
    pub cancelled: usize,
    /// The aborted value.
    pub aborted: usize,
    /// The panicked value.
    pub panicked: usize,
    /// The timed out value.
    pub timed_out: usize,
    /// The leaked value.
    pub leaked: usize,
    /// The blocking still running value.
    pub blocking_still_running: usize,
    /// The detached still running value.
    pub detached_still_running: usize,
    /// The children value.
    pub children: Vec<ShutdownReport>,
    /// The remaining tasks value.
    pub remaining_tasks: Vec<TaskSnapshot>,
    /// The blocking tasks value.
    pub blocking_tasks: Vec<BlockingTaskSnapshot>,
    /// The annotations value.
    pub annotations: Vec<ShutdownAnnotation>,
}

impl ShutdownReport {
    /// Creates a new `ShutdownReport`.
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

    /// Returns whether healthy.
    pub fn is_healthy(&self) -> bool {
        self.leaked == 0
            && self.panicked == 0
            && self.timed_out == 0
            && self.blocking_still_running == 0
            && self.detached_still_running == 0
            && self.children.iter().all(Self::is_healthy)
    }

    /// Returns the assert no task leak.
    pub fn assert_no_task_leak(&self) -> Result<(), String> {
        if self.is_healthy() {
            Ok(())
        } else {
            Err(format!("shutdown report is unhealthy: {}", self.to_json()))
        }
    }

    /// Executes log if unhealthy.
    pub fn log_if_unhealthy(&self) {
        if !self.is_healthy() {
            tracing::warn!(report = %self.to_json(), "runtime shutdown report is unhealthy");
        }
    }

    /// Converts this value to json.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|error| format!("{{\"serialization_error\":\"{}\"}}", error))
    }

    /// Executes merge blocking.
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
/// Represents task snapshot.
pub struct TaskSnapshot {
    /// The id identifier.
    pub id: TaskId,
    /// The name value.
    pub name: String,
    /// The group identifier.
    pub group_id: TaskGroupId,
    /// The group name value.
    pub group_name: String,
    /// The kind value.
    pub kind: TaskKind,
    /// The state value.
    pub state: TaskState,
    #[serde(with = "duration_millis")]
    /// The elapsed value.
    pub elapsed: Duration,
    /// Whether detached.
    pub detached: bool,
    /// The detached policy value.
    pub detached_policy: Option<DetachedTaskPolicy>,
}

#[derive(Debug, Clone, Serialize)]
/// Represents shutdown annotation.
pub struct ShutdownAnnotation {
    /// The message value.
    pub message: String,
}

impl ShutdownAnnotation {
    /// Creates a new `ShutdownAnnotation`.
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
