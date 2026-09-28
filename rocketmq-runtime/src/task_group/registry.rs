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

use std::collections::BTreeMap;
use std::sync::Weak;
use std::time::Instant;

use dashmap::DashMap;
use parking_lot::Mutex;

use super::TaskGroup;
use super::TaskGroupId;
use super::TaskGroupInner;
use super::TaskId;
use super::TaskMeta;
use super::{TaskDetail, TaskDetailScan, TaskDetailScope, TaskKind};

/// Shards of a group's task table.
///
/// Dashmap defaults to four shards per CPU, which costs several KiB for every
/// idle connection group. Eight keep concurrent submissions to one shared
/// group spread out.
const TASK_TABLE_SHARDS: usize = 8;

#[derive(Debug)]
struct ChildRegistration {
    inner: Weak<TaskGroupInner>,
}

#[derive(Debug)]
pub(super) struct ActiveTaskRegistry {
    pub(super) tasks: DashMap<TaskId, TaskMeta>,
    // Ordered entries allow bounded enumeration without scanning a sparse hash
    // table's historical capacity.
    children: Mutex<BTreeMap<u64, ChildRegistration>>,
    // Sanitized entries keep bounded detail scans independent of DashMap's
    // historical bucket capacity. Never take a task-table lock from this lock.
    details: [Mutex<BTreeMap<u64, (TaskKind, Instant)>>; TASK_TABLE_SHARDS],
}

impl ActiveTaskRegistry {
    pub(super) fn new() -> Self {
        Self {
            tasks: DashMap::with_shard_amount(TASK_TABLE_SHARDS),
            children: Mutex::new(BTreeMap::new()),
            details: std::array::from_fn(|_| Mutex::new(BTreeMap::new())),
        }
    }

    pub(super) fn insert_task(&self, task: TaskMeta) {
        let id = task.id;
        let detail = (task.kind, task.started_at);
        self.tasks.insert(id, task);
        self.detail_shard(id).lock().insert(id.as_u64(), detail);
    }

    pub(super) fn remove_task_detail(&self, id: TaskId) {
        self.detail_shard(id).lock().remove(&id.as_u64());
    }

    fn detail_shard(&self, id: TaskId) -> &Mutex<BTreeMap<u64, (TaskKind, Instant)>> {
        &self.details[id.as_u64() as usize % TASK_TABLE_SHARDS]
    }

    pub(super) fn collect_task_details(
        &self,
        scan_budget: usize,
        output_budget: usize,
        scope: TaskDetailScope,
        scan: &mut TaskDetailScan,
    ) -> bool {
        // Eight fixed shards spread concurrent detail insertion and removal;
        // checking an empty shard does not scan its historical capacity.
        for shard in &self.details {
            let details = shard.lock();
            let examined = details.len().min(scan_budget.saturating_sub(scan.scanned));
            for &(kind, started_at) in details.values().take(examined) {
                if scan.details.len() < output_budget {
                    scan.details.push(TaskDetail {
                        kind,
                        scope,
                        elapsed: started_at.elapsed(),
                    });
                } else {
                    scan.truncated = true;
                }
            }
            scan.scanned += examined;
            if examined < details.len() {
                scan.truncated = true;
                return false;
            }
        }
        true
    }

    pub(super) fn register_component(&self, id: TaskGroupId, child: Weak<TaskGroupInner>) {
        let previous = self
            .children
            .lock()
            .insert(id.as_u64(), ChildRegistration { inner: child });
        debug_assert!(previous.is_none(), "task-group ids must be unique");
    }

    pub(super) fn unregister_component(&self, id: TaskGroupId) {
        self.children.lock().remove(&id.as_u64());
    }

    pub(super) fn component_count(&self) -> usize {
        self.children.lock().len()
    }

    pub(super) fn components_snapshot(&self) -> Vec<TaskGroup> {
        let mut children = self.children.lock();
        let mut snapshot = Vec::with_capacity(children.len());
        children.retain(|_, entry| {
            let Some(inner) = entry.inner.upgrade() else {
                return false;
            };
            snapshot.push(TaskGroup { inner });
            true
        });
        snapshot
    }

    /// Charges enumeration, including stale weak entries, before allocating.
    pub(super) fn bounded_components_snapshot(&self, remaining: &mut usize) -> (Vec<TaskGroup>, bool) {
        let children = self.children.lock();
        let examined = children.len().min(*remaining);
        let mut snapshot = Vec::with_capacity(examined);
        for entry in children.values().take(examined) {
            if let Some(inner) = entry.inner.upgrade() {
                snapshot.push(TaskGroup { inner });
            }
        }
        *remaining -= examined;
        (snapshot, examined < children.len())
    }
}
