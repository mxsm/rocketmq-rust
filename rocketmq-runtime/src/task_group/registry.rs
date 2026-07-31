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
use std::sync::Weak;

use dashmap::DashMap;
use parking_lot::Mutex;

use super::TaskGroup;
use super::TaskGroupId;
use super::TaskGroupInner;
use super::TaskId;
use super::TaskMeta;

#[derive(Debug)]
struct ChildRegistration {
    inner: Weak<TaskGroupInner>,
}

#[derive(Debug)]
pub(super) struct ActiveTaskRegistry {
    pub(super) tasks: DashMap<TaskId, TaskMeta>,
    children: Mutex<HashMap<TaskGroupId, ChildRegistration>>,
}

impl ActiveTaskRegistry {
    pub(super) fn new() -> Self {
        Self {
            tasks: DashMap::new(),
            children: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn register_component(&self, id: TaskGroupId, child: Weak<TaskGroupInner>) {
        let previous = self.children.lock().insert(id, ChildRegistration { inner: child });
        debug_assert!(previous.is_none(), "task-group ids must be unique");
    }

    pub(super) fn unregister_component(&self, id: TaskGroupId) {
        self.children.lock().remove(&id);
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
}
