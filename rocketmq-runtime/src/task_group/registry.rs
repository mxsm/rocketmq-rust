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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Weak;

use dashmap::DashMap;
use parking_lot::Mutex;

use super::TaskGroup;
use super::TaskGroupId;
use super::TaskGroupInner;
use super::TaskId;
use super::TaskMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChildRegistrationKind {
    Component,
    Operation,
}

#[derive(Debug)]
struct ChildRegistration {
    inner: Weak<TaskGroupInner>,
    kind: ChildRegistrationKind,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ActiveChildStats {
    pub(super) active_operations: usize,
    pub(super) operations_created: usize,
    pub(super) operations_released: usize,
    pub(super) registry_slots: usize,
}

#[derive(Debug)]
pub(super) struct ActiveTaskRegistry {
    pub(super) tasks: DashMap<TaskId, TaskMeta>,
    children: Mutex<HashMap<TaskGroupId, ChildRegistration>>,
    operations_created: AtomicUsize,
    operations_released: AtomicUsize,
}

impl ActiveTaskRegistry {
    pub(super) fn new() -> Self {
        Self {
            tasks: DashMap::new(),
            children: Mutex::new(HashMap::new()),
            operations_created: AtomicUsize::new(0),
            operations_released: AtomicUsize::new(0),
        }
    }

    pub(super) fn register_child(&self, id: TaskGroupId, child: Weak<TaskGroupInner>, kind: ChildRegistrationKind) {
        let previous = self
            .children
            .lock()
            .insert(id, ChildRegistration { inner: child, kind });
        debug_assert!(previous.is_none(), "task-group ids must be unique");
        if kind == ChildRegistrationKind::Operation {
            self.operations_created.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn unregister_child(&self, id: TaskGroupId) {
        let removed = self.children.lock().remove(&id);
        if removed.is_some_and(|entry| entry.kind == ChildRegistrationKind::Operation) {
            self.operations_released.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn child_count(&self) -> usize {
        self.children.lock().len()
    }

    pub(super) fn child_stats(&self) -> ActiveChildStats {
        let children = self.children.lock();
        ActiveChildStats {
            active_operations: children
                .values()
                .filter(|entry| entry.kind == ChildRegistrationKind::Operation)
                .count(),
            operations_created: self.operations_created.load(Ordering::Relaxed),
            operations_released: self.operations_released.load(Ordering::Relaxed),
            registry_slots: children.len(),
        }
    }

    pub(super) fn children_snapshot(&self) -> Vec<TaskGroup> {
        self.children
            .lock()
            .values()
            .filter_map(|entry| entry.inner.upgrade())
            .map(|inner| TaskGroup { inner })
            .collect()
    }
}
