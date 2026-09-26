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

/// Represents shutdown report.
#[derive(Debug, Serialize)]
pub struct ShutdownReport {
    /// The name value.
    pub name: String,
    /// The elapsed value.
    #[serde(with = "duration_millis")]
    pub elapsed: Duration,
    /// The completed value.
    pub completed: usize,
    /// The cancelled value.
    pub cancelled: usize,
    /// The aborted value.
    pub aborted: usize,
    /// Lifecycle components that completed with an explicit failure.
    pub failed: usize,
    /// The panicked value.
    pub panicked: usize,
    /// Tasks this shutdown aborted after its deadline expired, plus tasks
    /// still registered when it finished.
    pub timed_out: usize,
    /// Tasks still registered when the shutdown finished.
    pub leaked: usize,
    /// The blocking still running value.
    pub blocking_still_running: usize,
    /// The children value.
    pub children: Vec<ShutdownReport>,
    /// Tasks still registered when the shutdown finished, at most
    /// [`Self::REMAINING_TASKS_LIMIT`] of them.
    pub remaining_tasks: Vec<TaskSnapshot>,
    /// Remaining tasks left out of `remaining_tasks` because of its limit.
    /// They are still counted in `leaked`.
    pub remaining_tasks_omitted: usize,
    /// The blocking tasks value.
    pub blocking_tasks: Vec<BlockingTaskSnapshot>,
    /// The annotations value.
    pub annotations: Vec<ShutdownAnnotation>,
}

impl Clone for ShutdownReport {
    fn clone(&self) -> Self {
        let mut copy = self.clone_local();
        let mut pending = vec![(self, &mut copy)];
        while let Some((source, target)) = pending.pop() {
            target.children = source.children.iter().map(Self::clone_local).collect();
            pending.extend(source.children.iter().zip(target.children.iter_mut()));
        }
        copy
    }
}

impl ShutdownReport {
    fn clone_local(&self) -> Self {
        Self {
            name: self.name.clone(),
            elapsed: self.elapsed,
            completed: self.completed,
            cancelled: self.cancelled,
            aborted: self.aborted,
            failed: self.failed,
            panicked: self.panicked,
            timed_out: self.timed_out,
            leaked: self.leaked,
            blocking_still_running: self.blocking_still_running,
            children: Vec::new(),
            remaining_tasks: self.remaining_tasks.clone(),
            remaining_tasks_omitted: self.remaining_tasks_omitted,
            blocking_tasks: self.blocking_tasks.clone(),
            annotations: self.annotations.clone(),
        }
    }

    /// Creates a new `ShutdownReport`.
    pub fn new(name: impl Into<String>, elapsed: Duration) -> Self {
        Self {
            name: name.into(),
            elapsed,
            completed: 0,
            cancelled: 0,
            aborted: 0,
            failed: 0,
            panicked: 0,
            timed_out: 0,
            leaked: 0,
            blocking_still_running: 0,
            children: Vec::new(),
            remaining_tasks: Vec::new(),
            remaining_tasks_omitted: 0,
            blocking_tasks: Vec::new(),
            annotations: Vec::new(),
        }
    }

    /// Maximum number of task snapshots kept in `remaining_tasks`.
    ///
    /// The limit bounds the size of a report for a group that leaks many
    /// tasks; the counters stay exact.
    pub const REMAINING_TASKS_LIMIT: usize = 64;

    pub(crate) fn push_remaining_task(&mut self, task: TaskSnapshot) {
        if self.remaining_tasks.len() < Self::REMAINING_TASKS_LIMIT {
            self.remaining_tasks.push(task);
        } else {
            self.remaining_tasks_omitted += 1;
        }
    }

    /// Returns whether healthy.
    pub fn is_healthy(&self) -> bool {
        let mut pending = vec![self];
        while let Some(report) = pending.pop() {
            if report.leaked != 0
                || report.failed != 0
                || report.panicked != 0
                || report.timed_out != 0
                || report.blocking_still_running != 0
            {
                return false;
            }
            pending.extend(&report.children);
        }
        true
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

/// Represents task snapshot.
#[derive(Debug, Clone, Serialize)]
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
    /// The elapsed value.
    #[serde(with = "duration_millis")]
    pub elapsed: Duration,
}

/// Represents shutdown annotation.
#[derive(Debug, Clone, Serialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_component_failure_makes_the_report_unhealthy() {
        let mut report = ShutdownReport::new("failed-component", Duration::ZERO);
        report.failed = 1;

        assert!(!report.is_healthy());
        assert!(report.to_json().contains("\"failed\": 1"));
    }

    #[tokio::test]
    async fn remaining_tasks_are_capped_and_the_omission_is_counted() {
        let context = crate::RuntimeContext::from_current("remaining-task-limit");
        let group_id = context.root_group().id();
        let mut report = ShutdownReport::new("many-leaks", Duration::ZERO);
        for id in 0..10_000 {
            report.leaked += 1;
            report.push_remaining_task(TaskSnapshot {
                id: TaskId::new(group_id, id),
                name: "leaked".to_owned(),
                group_id,
                group_name: "many-leaks".to_owned(),
                kind: TaskKind::Worker,
                state: TaskState::Leaked,
                elapsed: Duration::ZERO,
            });
        }

        assert_eq!(report.remaining_tasks.len(), ShutdownReport::REMAINING_TASKS_LIMIT);
        assert_eq!(
            report.remaining_tasks_omitted,
            10_000 - ShutdownReport::REMAINING_TASKS_LIMIT
        );
        assert_eq!(report.leaked, 10_000);
        let json = report.to_json();
        assert!(json.contains("\"remaining_tasks_omitted\": 9936"), "{json}");
        assert!(json.len() < 64 * 1024, "report JSON is {} bytes", json.len());
    }
}
