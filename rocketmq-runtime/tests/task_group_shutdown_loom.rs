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

//! Loom model for the TaskGroup registration, cancellation, and join invariants.

use loom::sync::Arc;
use loom::sync::Mutex;
use loom::thread;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Lifecycle {
    Open,
    Closing,
    Closed,
}

#[derive(Debug)]
struct TaskGroupModel {
    lifecycle: Lifecycle,
    active_tasks: usize,
    accepted_tasks: usize,
    completed_tasks: usize,
    active_child_leases: usize,
    accepted_child_leases: usize,
    released_child_leases: usize,
}

impl TaskGroupModel {
    fn new() -> Self {
        Self {
            lifecycle: Lifecycle::Open,
            active_tasks: 0,
            accepted_tasks: 0,
            completed_tasks: 0,
            active_child_leases: 0,
            accepted_child_leases: 0,
            released_child_leases: 0,
        }
    }

    fn try_register(&mut self) -> bool {
        if self.lifecycle != Lifecycle::Open {
            return false;
        }
        self.active_tasks += 1;
        self.accepted_tasks += 1;
        true
    }

    fn complete(&mut self) {
        assert!(self.active_tasks > 0, "a task may complete only after registration");
        self.active_tasks -= 1;
        self.completed_tasks += 1;
    }

    fn try_acquire_child_lease(&mut self) -> bool {
        if self.lifecycle != Lifecycle::Open {
            return false;
        }
        self.active_child_leases += 1;
        self.accepted_child_leases += 1;
        true
    }

    fn release_child_lease(&mut self) {
        assert!(
            self.active_child_leases > 0,
            "a child lease may be released only after acquisition"
        );
        self.active_child_leases -= 1;
        self.released_child_leases += 1;
    }

    fn begin_shutdown(&mut self) {
        if self.lifecycle == Lifecycle::Open {
            self.lifecycle = Lifecycle::Closing;
        }
    }

    fn finish_shutdown(&mut self) {
        assert_eq!(self.lifecycle, Lifecycle::Closing);
        assert_eq!(self.active_tasks, 0, "shutdown must join every accepted task");
        assert_eq!(
            self.active_child_leases, 0,
            "shutdown must join every accepted dynamic child"
        );
        self.lifecycle = Lifecycle::Closed;
    }
}

#[test]
fn spawn_racing_with_shutdown_is_either_joined_or_rejected() {
    loom::model(|| {
        let group = Arc::new(Mutex::new(TaskGroupModel::new()));
        let mut task_threads = Vec::with_capacity(2);

        for _ in 0..2 {
            let group = Arc::clone(&group);
            task_threads.push(thread::spawn(move || {
                let accepted = group.lock().expect("register lock").try_register();
                if accepted {
                    thread::yield_now();
                    group.lock().expect("completion lock").complete();
                }
                accepted
            }));
        }

        let shutdown_group = Arc::clone(&group);
        let shutdown = thread::spawn(move || {
            shutdown_group.lock().expect("shutdown lock").begin_shutdown();
        });

        shutdown.join().expect("shutdown transition");
        let accepted = task_threads
            .into_iter()
            .map(|task| task.join().expect("task registration"))
            .filter(|accepted| *accepted)
            .count();

        let mut group = group.lock().expect("final lock");
        group.finish_shutdown();
        assert_eq!(group.lifecycle, Lifecycle::Closed);
        assert_eq!(group.accepted_tasks, accepted);
        assert_eq!(group.completed_tasks, accepted);
    });
}

#[test]
fn ha_reconnect_child_lease_racing_with_shutdown_is_tracked_or_rejected() {
    loom::model(|| {
        let group = Arc::new(Mutex::new(TaskGroupModel::new()));
        let mut reconnects = Vec::with_capacity(2);

        for _ in 0..2 {
            let group = Arc::clone(&group);
            reconnects.push(thread::spawn(move || {
                let accepted = group.lock().expect("child lease lock").try_acquire_child_lease();
                if accepted {
                    thread::yield_now();
                    group.lock().expect("child lease release").release_child_lease();
                }
                accepted
            }));
        }

        let shutdown_group = Arc::clone(&group);
        let shutdown = thread::spawn(move || {
            shutdown_group.lock().expect("shutdown lock").begin_shutdown();
        });

        shutdown.join().expect("shutdown transition");
        let accepted = reconnects
            .into_iter()
            .map(|reconnect| reconnect.join().expect("reconnect lease"))
            .filter(|accepted| *accepted)
            .count();

        let mut group = group.lock().expect("final lock");
        group.finish_shutdown();
        assert_eq!(group.lifecycle, Lifecycle::Closed);
        assert_eq!(group.accepted_child_leases, accepted);
        assert_eq!(group.released_child_leases, accepted);
    });
}
