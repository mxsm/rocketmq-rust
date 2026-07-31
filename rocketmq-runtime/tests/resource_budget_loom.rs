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

//! Loom model for ResourceBudgetTree permit acquisition and Drop recovery.

use loom::sync::Arc;
use loom::sync::Condvar;
use loom::sync::Mutex;
use loom::thread;

#[derive(Debug, Default)]
struct BudgetState {
    current_count: usize,
    current_bytes: usize,
    acquired_count: usize,
    released_count: usize,
    waiters: usize,
    closed: bool,
}

#[derive(Clone)]
struct Budget {
    state: Arc<Mutex<BudgetState>>,
    capacity: Arc<Condvar>,
    max_count: usize,
    max_bytes: usize,
}

impl Budget {
    fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(BudgetState::default())),
            capacity: Arc::new(Condvar::new()),
            max_count,
            max_bytes,
        }
    }

    fn try_acquire(&self, bytes: usize) -> Option<Permit> {
        let mut state = self.state.lock().expect("budget state");
        if state.closed
            || state.current_count == self.max_count
            || state
                .current_bytes
                .checked_add(bytes)
                .is_none_or(|total| total > self.max_bytes)
        {
            return None;
        }
        state.current_count += 1;
        state.current_bytes += bytes;
        state.acquired_count += 1;
        assert_state_invariants(&state, self.max_count, self.max_bytes);
        Some(Permit {
            budget: self.clone(),
            bytes,
        })
    }

    fn acquire_or_wait(&self, bytes: usize) -> Option<Permit> {
        let mut state = self.state.lock().expect("budget state");
        loop {
            if state.closed {
                return None;
            }
            if state.current_count < self.max_count
                && state
                    .current_bytes
                    .checked_add(bytes)
                    .is_some_and(|total| total <= self.max_bytes)
            {
                state.current_count += 1;
                state.current_bytes += bytes;
                state.acquired_count += 1;
                assert_state_invariants(&state, self.max_count, self.max_bytes);
                return Some(Permit {
                    budget: self.clone(),
                    bytes,
                });
            }
            state.waiters += 1;
            state = self.capacity.wait(state).expect("capacity wait");
            state.waiters -= 1;
        }
    }

    fn close(&self) {
        let mut state = self.state.lock().expect("budget state");
        state.closed = true;
        drop(state);
        self.capacity.notify_all();
    }
}

struct Permit {
    budget: Budget,
    bytes: usize,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().expect("permit release");
        assert!(state.current_count > 0);
        assert!(state.current_bytes >= self.bytes);
        state.current_count -= 1;
        state.current_bytes -= self.bytes;
        state.released_count += 1;
        assert_state_invariants(&state, self.budget.max_count, self.budget.max_bytes);
        drop(state);
        self.budget.capacity.notify_all();
    }
}

fn assert_state_invariants(state: &BudgetState, max_count: usize, max_bytes: usize) {
    assert!(state.current_count <= max_count);
    assert!(state.current_bytes <= max_bytes);
    assert_eq!(state.acquired_count, state.released_count + state.current_count);
}

#[test]
fn task_completion_and_cancellation_release_every_permit() {
    loom::model(|| {
        let budget = Budget::new(1, 8);
        let mut owners = Vec::with_capacity(2);

        for _ in 0..2 {
            let budget = budget.clone();
            owners.push(thread::spawn(move || {
                if let Some(permit) = budget.try_acquire(8) {
                    thread::yield_now();
                    drop(permit);
                }
            }));
        }

        for owner in owners {
            owner.join().expect("permit owner");
        }

        let retry = budget.try_acquire(8).expect("capacity must recover after owner exit");
        drop(retry);

        let state = budget.state.lock().expect("final budget state");
        assert_eq!(state.current_count, 0);
        assert_eq!(state.current_bytes, 0);
        assert_eq!(state.acquired_count, state.released_count);
    });
}

#[test]
fn queue_waiter_owner_cancellation_and_close_preserve_permit_conservation() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_permutations = Some(10_000);
    model.check(|| {
        let budget = Budget::new(1, 8);
        let owner = budget.try_acquire(8).expect("owner permit");

        let waiter_budget = budget.clone();
        let waiter = thread::spawn(move || {
            if let Some(permit) = waiter_budget.acquire_or_wait(8) {
                thread::yield_now();
                drop(permit);
            }
        });

        let closer_budget = budget.clone();
        let closer = thread::spawn(move || {
            closer_budget.close();
        });

        drop(owner);
        waiter.join().expect("waiter thread");
        closer.join().expect("closer thread");

        assert!(budget.try_acquire(1).is_none(), "closed budget accepted new work");
        let state = budget.state.lock().expect("final budget state");
        assert!(state.closed);
        assert_eq!(state.waiters, 0);
        assert_eq!(state.current_count, 0);
        assert_eq!(state.current_bytes, 0);
        assert_eq!(state.acquired_count, state.released_count);
    });
}
