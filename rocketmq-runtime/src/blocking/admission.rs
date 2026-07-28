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

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use tokio::sync::Notify;

use super::BlockingLane;
use super::BlockingLanePolicies;
use crate::error::RuntimeResult;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LaneAdmissionSnapshot {
    pub global_capacity: usize,
    pub global_running: usize,
    pub global_available: usize,
    pub lane_reserved: usize,
    pub lane_running: usize,
    pub lane_borrowed: usize,
}

#[derive(Debug)]
struct AdmissionState {
    running: [usize; 3],
    waiting: [usize; 3],
}

#[derive(Debug)]
struct GlobalBlockingBudgetInner {
    capacity: usize,
    lane_ceilings: [usize; 3],
    lane_reservations: [usize; 3],
    state: Mutex<AdmissionState>,
    changed: Notify,
}

/// The single admission owner shared by all blocking lanes in one runtime.
///
/// A lane may borrow idle capacity. Once another lane has a waiter, its
/// reservation is protected from new borrowers until that waiter is admitted.
#[derive(Debug, Clone)]
pub(crate) struct GlobalBlockingBudget {
    inner: Arc<GlobalBlockingBudgetInner>,
}

impl GlobalBlockingBudget {
    pub(crate) fn managed(capacity: usize, policies: &BlockingLanePolicies) -> RuntimeResult<Self> {
        policies.validate_for_global_capacity(capacity)?;
        Ok(Self::new(
            capacity,
            [
                policies.max_concurrency(BlockingLane::StorageIo),
                policies.max_concurrency(BlockingLane::MetadataIo),
                policies.max_concurrency(BlockingLane::CpuCrypto),
            ],
            [1; 3],
        ))
    }

    pub(crate) fn isolated(capacity: usize) -> Self {
        Self::new(capacity, [capacity, 0, 0], [capacity, 0, 0])
    }

    fn new(capacity: usize, lane_ceilings: [usize; 3], lane_reservations: [usize; 3]) -> Self {
        Self {
            inner: Arc::new(GlobalBlockingBudgetInner {
                capacity,
                lane_ceilings,
                lane_reservations,
                state: Mutex::new(AdmissionState {
                    running: [0; 3],
                    waiting: [0; 3],
                }),
                changed: Notify::new(),
            }),
        }
    }

    pub(crate) async fn acquire(&self, lane: BlockingLane, deadline: Instant) -> Result<GlobalBlockingPermit, ()> {
        let waiter = BlockingWaiter::new(self.clone(), lane);
        loop {
            let notified = self.inner.changed.notified();
            if self.try_acquire(lane) {
                waiter.complete();
                return Ok(GlobalBlockingPermit {
                    budget: self.clone(),
                    lane,
                });
            }
            if deadline <= Instant::now() {
                return Err(());
            }
            if tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), notified)
                .await
                .is_err()
            {
                return Err(());
            }
        }
    }

    fn try_acquire(&self, lane: BlockingLane) -> bool {
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let lane_index = lane.index();
        let total_running = state.running.iter().sum::<usize>();
        if total_running >= self.inner.capacity || state.running[lane_index] >= self.inner.lane_ceilings[lane_index] {
            return false;
        }

        let protected_for_waiters = BlockingLane::ALL
            .iter()
            .copied()
            .filter(|other| *other != lane)
            .map(BlockingLane::index)
            .filter(|index| state.waiting[*index] > 0)
            .map(|index| self.inner.lane_reservations[index].saturating_sub(state.running[index]))
            .sum::<usize>();
        let available = self.inner.capacity - total_running;
        if available <= protected_for_waiters {
            return false;
        }

        state.running[lane_index] += 1;
        true
    }

    fn release(&self, lane: BlockingLane) {
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let running = &mut state.running[lane.index()];
        debug_assert!(*running > 0, "blocking admission permit released more than once");
        *running = running.saturating_sub(1);
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn add_waiter(&self, lane: BlockingLane) {
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.waiting[lane.index()] += 1;
        drop(state);
        self.inner.changed.notify_waiters();
    }

    fn remove_waiter(&self, lane: BlockingLane) {
        let mut state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let waiting = &mut state.waiting[lane.index()];
        debug_assert!(*waiting > 0, "blocking admission waiter removed more than once");
        *waiting = waiting.saturating_sub(1);
        drop(state);
        self.inner.changed.notify_waiters();
    }

    pub(crate) fn snapshot(&self, lane: BlockingLane) -> LaneAdmissionSnapshot {
        let state = self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let global_running = state.running.iter().sum::<usize>();
        let lane_index = lane.index();
        let lane_running = state.running[lane_index];
        let lane_reserved = self.inner.lane_reservations[lane_index];
        LaneAdmissionSnapshot {
            global_capacity: self.inner.capacity,
            global_running,
            global_available: self.inner.capacity.saturating_sub(global_running),
            lane_reserved,
            lane_running,
            lane_borrowed: lane_running.saturating_sub(lane_reserved),
        }
    }
}

#[derive(Debug)]
struct BlockingWaiter {
    budget: GlobalBlockingBudget,
    lane: BlockingLane,
    active: bool,
}

impl BlockingWaiter {
    fn new(budget: GlobalBlockingBudget, lane: BlockingLane) -> Self {
        budget.add_waiter(lane);
        Self {
            budget,
            lane,
            active: true,
        }
    }

    fn complete(mut self) {
        self.budget.remove_waiter(self.lane);
        self.active = false;
    }
}

impl Drop for BlockingWaiter {
    fn drop(&mut self) {
        if self.active {
            self.budget.remove_waiter(self.lane);
        }
    }
}

#[derive(Debug)]
pub(crate) struct GlobalBlockingPermit {
    budget: GlobalBlockingBudget,
    lane: BlockingLane,
}

impl Drop for GlobalBlockingPermit {
    fn drop(&mut self) {
        self.budget.release(self.lane);
    }
}
