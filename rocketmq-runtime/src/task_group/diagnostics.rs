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

//! Aggregate task scans and bounded detail collection.

use super::{
    TaskDetail, TaskDetailScan, TaskDetailScope, TaskGroup, TaskGroupDiagnostics, TaskKind, TaskKindDiagnostics,
};
use std::time::Duration;

impl TaskGroup {
    pub(crate) fn diagnostics(&self, long_running_threshold: Duration) -> TaskGroupDiagnostics {
        let mut aggregate = TaskGroupDiagnosticsAccumulator::default();
        self.accumulate_diagnostics(long_running_threshold, &mut aggregate);
        aggregate.finish()
    }

    /// Returns diagnostics for this group's own tasks without descending into
    /// child groups.
    pub(crate) fn local_diagnostics(&self, long_running_threshold: Duration) -> TaskGroupDiagnostics {
        let mut aggregate = TaskGroupDiagnosticsAccumulator {
            group_count: 1,
            ..TaskGroupDiagnosticsAccumulator::default()
        };
        for task in self.inner.registry.tasks.iter() {
            let elapsed = task.started_at.elapsed();
            aggregate.record_task(task.kind, elapsed, elapsed >= long_running_threshold);
        }
        aggregate.finish()
    }

    /// Scans this group and its descendants for a bounded detail list.
    ///
    /// The scan budget bounds the work and the output budget bounds the payload.
    /// When either is reached the result reports how many tasks were examined, so
    /// a partial list is never presented as the complete tree.
    pub(crate) fn bounded_task_details(&self, scan_budget: usize, output_budget: usize) -> TaskDetailScan {
        let mut scan = TaskDetailScan::default();
        self.collect_task_details(scan_budget, output_budget, TaskDetailScope::Local, &mut scan);
        scan
    }

    fn collect_task_details(
        &self,
        scan_budget: usize,
        output_budget: usize,
        scope: TaskDetailScope,
        scan: &mut TaskDetailScan,
    ) {
        for task in self.inner.registry.tasks.iter() {
            if scan.scanned >= scan_budget {
                scan.truncated = true;
                return;
            }
            scan.scanned = scan.scanned.saturating_add(1);
            if scan.details.len() < output_budget {
                scan.details.push(TaskDetail {
                    kind: task.kind,
                    scope,
                    elapsed: task.started_at.elapsed(),
                });
            } else {
                scan.truncated = true;
            }
        }
        for child in self.inner.registry.components_snapshot() {
            if scan.scanned >= scan_budget {
                scan.truncated = true;
                return;
            }
            child.collect_task_details(scan_budget, output_budget, TaskDetailScope::Subtree, scan);
        }
    }

    fn accumulate_diagnostics(
        &self,
        long_running_threshold: Duration,
        aggregate: &mut TaskGroupDiagnosticsAccumulator,
    ) {
        aggregate.group_count = aggregate.group_count.saturating_add(1);
        for task in self.inner.registry.tasks.iter() {
            let elapsed = task.started_at.elapsed();
            aggregate.record_task(task.kind, elapsed, elapsed >= long_running_threshold);
        }

        for child in self.inner.registry.components_snapshot() {
            child.accumulate_diagnostics(long_running_threshold, aggregate);
        }
    }
}

#[derive(Debug, Default)]
struct TaskGroupDiagnosticsAccumulator {
    group_count: usize,
    task_count: usize,
    active_by_kind: [usize; TaskKind::COUNT],
    long_running_by_kind: [usize; TaskKind::COUNT],
    max_elapsed_by_kind: [Duration; TaskKind::COUNT],
}

impl TaskGroupDiagnosticsAccumulator {
    fn record_task(&mut self, kind: TaskKind, elapsed: Duration, long_running: bool) {
        let index = kind.index();
        self.task_count = self.task_count.saturating_add(1);
        self.active_by_kind[index] = self.active_by_kind[index].saturating_add(1);
        if long_running {
            self.long_running_by_kind[index] = self.long_running_by_kind[index].saturating_add(1);
        }
        self.max_elapsed_by_kind[index] = self.max_elapsed_by_kind[index].max(elapsed);
    }

    fn finish(self) -> TaskGroupDiagnostics {
        let task_kinds = TaskKind::ALL
            .into_iter()
            .filter_map(|kind| {
                let index = kind.index();
                (self.active_by_kind[index] > 0).then_some(TaskKindDiagnostics {
                    kind,
                    active: self.active_by_kind[index],
                    long_running: self.long_running_by_kind[index],
                    max_elapsed: self.max_elapsed_by_kind[index],
                })
            })
            .collect();

        TaskGroupDiagnostics {
            group_count: self.group_count,
            task_count: self.task_count,
            task_kinds,
        }
    }
}
