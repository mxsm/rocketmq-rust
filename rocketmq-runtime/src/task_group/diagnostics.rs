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

use super::{TaskDetailScan, TaskDetailScope, TaskGroup, TaskGroupDiagnostics, TaskKind, TaskKindDiagnostics};
use std::time::Duration;

impl TaskGroup {
    /// Samples sanitized task details without computing subtree aggregates.
    ///
    /// At most `scan_budget` tasks and `scan_budget` descendant registrations
    /// are examined; at most `output_budget` details are returned. Enumeration
    /// and temporary group storage obey the same budget, including empty or
    /// expired groups. Concurrent changes are not a globally atomic snapshot.
    pub fn diagnostics_task_details(&self, scan_budget: usize, output_budget: usize) -> crate::RuntimeTaskDetails {
        self.bounded_task_details(scan_budget, output_budget).into()
    }

    /// Aggregates every task of this group and its descendants.
    ///
    /// The work is proportional to the active tasks and groups of the subtree.
    /// An explicit stack keeps a deep component tree off the call stack.
    pub(crate) fn diagnostics(&self, long_running_threshold: Duration) -> TaskGroupDiagnostics {
        let mut aggregate = TaskGroupDiagnosticsAccumulator::default();
        let local_task_count = self.accumulate_local_diagnostics(long_running_threshold, &mut aggregate);
        let mut pending = self.inner.registry.components_snapshot();
        while let Some(group) = pending.pop() {
            group.accumulate_local_diagnostics(long_running_threshold, &mut aggregate);
            pending.extend(group.inner.registry.components_snapshot());
        }
        aggregate.finish(local_task_count)
    }

    /// Scans this group and its descendants for a bounded detail list.
    ///
    /// The scan budget bounds the work: at most `scan_budget` tasks are
    /// examined and at most `scan_budget` descendant groups are visited, so a
    /// tree of empty groups cannot make a bounded scan unbounded. The output
    /// budget bounds the payload. When any budget is reached the result is
    /// truncated and reports how many tasks were examined, so a partial list is
    /// never presented as the complete tree.
    pub(crate) fn bounded_task_details(&self, scan_budget: usize, output_budget: usize) -> TaskDetailScan {
        let mut scan = TaskDetailScan::default();
        if !self.collect_local_task_details(scan_budget, output_budget, TaskDetailScope::Local, &mut scan) {
            return scan;
        }
        let mut remaining_groups = scan_budget;
        let (mut pending, truncated) = self.inner.registry.bounded_components_snapshot(&mut remaining_groups);
        scan.truncated |= truncated;
        while let Some(group) = pending.pop() {
            if scan.scanned >= scan_budget {
                scan.truncated = true;
                break;
            }
            if !group.collect_local_task_details(scan_budget, output_budget, TaskDetailScope::Subtree, &mut scan) {
                break;
            }
            let (children, truncated) = group.inner.registry.bounded_components_snapshot(&mut remaining_groups);
            scan.truncated |= truncated;
            pending.extend(children);
        }
        scan.group_entries_scanned = scan_budget - remaining_groups;
        scan
    }

    /// Adds this group's own tasks to `scan`; returns `false` once the scan
    /// budget stops the scan.
    fn collect_local_task_details(
        &self,
        scan_budget: usize,
        output_budget: usize,
        scope: TaskDetailScope,
        scan: &mut TaskDetailScan,
    ) -> bool {
        self.inner
            .registry
            .collect_task_details(scan_budget, output_budget, scope, scan)
    }

    fn accumulate_local_diagnostics(
        &self,
        long_running_threshold: Duration,
        aggregate: &mut TaskGroupDiagnosticsAccumulator,
    ) -> usize {
        aggregate.group_count = aggregate.group_count.saturating_add(1);
        let mut local_task_count = 0;
        for task in self.inner.registry.tasks.iter() {
            local_task_count += 1;
            let elapsed = task.started_at.elapsed();
            aggregate.record_task(task.kind, elapsed, elapsed >= long_running_threshold);
        }
        local_task_count
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

    fn finish(self, local_task_count: usize) -> TaskGroupDiagnostics {
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
            local_task_count,
            group_count: self.group_count,
            task_count: self.task_count,
            task_kinds,
        }
    }
}
