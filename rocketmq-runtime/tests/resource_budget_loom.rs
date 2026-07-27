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
use loom::sync::Mutex;
use loom::thread;

#[derive(Debug, Default)]
struct BudgetState {
    current_count: usize,
    current_bytes: usize,
    acquired_count: usize,
    released_count: usize,
}

#[derive(Clone)]
struct Budget {
    state: Arc<Mutex<BudgetState>>,
    max_count: usize,
    max_bytes: usize,
}

impl Budget {
    fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(BudgetState::default())),
            max_count,
            max_bytes,
        }
    }

    fn try_acquire(&self, bytes: usize) -> Option<Permit> {
        let mut state = self.state.lock().expect("budget state");
        if state.current_count == self.max_count || state.current_bytes + bytes > self.max_bytes {
            return None;
        }
        state.current_count += 1;
        state.current_bytes += bytes;
        state.acquired_count += 1;
        Some(Permit {
            budget: self.clone(),
            bytes,
        })
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
    }
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
