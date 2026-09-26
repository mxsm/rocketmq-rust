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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Instant;

use tokio::sync::Notify;

use super::BlockingLane;
use super::BlockingLanePolicies;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LaneAdmissionSnapshot {
    pub global_capacity: usize,
    pub global_running: usize,
    pub global_available: usize,
    pub lane_reserved: usize,
    pub lane_running: usize,
    pub lane_borrowed: usize,
}

/// One queued request for a slot.
#[derive(Debug)]
struct WaiterSlot {
    /// Arrival order across all lanes.
    arrival: u64,
    /// Written under the admission lock when the slot is granted.
    granted: std::sync::atomic::AtomicBool,
    notify: Notify,
}

impl WaiterSlot {
    fn is_granted(&self) -> bool {
        self.granted.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[derive(Debug)]
struct AdmissionState {
    running: [usize; 3],
    waiters: [VecDeque<Arc<WaiterSlot>>; 3],
    next_arrival: u64,
}

#[derive(Debug)]
struct GlobalBlockingBudgetInner {
    capacity: usize,
    lane_ceilings: [usize; 3],
    lane_reservations: [usize; 3],
    state: Mutex<AdmissionState>,
}

/// The single admission owner shared by all blocking lanes in one runtime.
///
/// Each lane admits its waiters in arrival order, and a request never passes
/// a waiter of its own lane. A lane may borrow idle capacity. Once another
/// lane has a waiter, its reservation is protected from new borrowers until
/// that waiter is admitted.
///
/// A released slot is handed directly to the waiter that can use it, which
/// wakes at most that one waiter.
#[derive(Debug, Clone)]
pub(crate) struct GlobalBlockingBudget {
    inner: Arc<GlobalBlockingBudgetInner>,
}

impl GlobalBlockingBudget {
    pub(crate) fn managed(capacity: usize, policies: &BlockingLanePolicies) -> Self {
        debug_assert!(policies.validate_for_global_capacity(capacity).is_ok());
        Self::new(
            capacity,
            [
                policies.max_concurrency(BlockingLane::StorageIo),
                policies.max_concurrency(BlockingLane::MetadataIo),
                policies.max_concurrency(BlockingLane::CpuCrypto),
            ],
            [1; 3],
        )
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
                    waiters: Default::default(),
                    next_arrival: 0,
                }),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, AdmissionState> {
        self.inner.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) async fn acquire(&self, lane: BlockingLane, deadline: Instant) -> Result<GlobalBlockingPermit, ()> {
        let slot = {
            let mut state = self.lock();
            let lane_index = lane.index();
            if state.waiters[lane_index].is_empty() && self.can_admit(&state, lane_index) {
                state.running[lane_index] += 1;
                return Ok(self.permit(lane));
            }
            if deadline <= Instant::now() {
                return Err(());
            }
            let slot = Arc::new(WaiterSlot {
                arrival: state.next_arrival,
                granted: std::sync::atomic::AtomicBool::new(false),
                notify: Notify::new(),
            });
            state.next_arrival += 1;
            state.waiters[lane_index].push_back(Arc::clone(&slot));
            slot
        };

        let waiter = QueuedWaiter {
            budget: self,
            lane,
            slot,
            settled: false,
        };
        // A grant made before this wait starts stores the notification permit.
        let _ = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), waiter.slot.notify.notified()).await;
        if waiter.settle() {
            Ok(self.permit(lane))
        } else {
            Err(())
        }
    }

    fn permit(&self, lane: BlockingLane) -> GlobalBlockingPermit {
        GlobalBlockingPermit {
            budget: self.clone(),
            lane,
        }
    }

    /// Returns whether one more slot of `lane` fits the global capacity, the
    /// lane ceiling, and the reservations of the other lanes' waiters.
    fn can_admit(&self, state: &AdmissionState, lane_index: usize) -> bool {
        let total_running = state.running.iter().sum::<usize>();
        if total_running >= self.inner.capacity || state.running[lane_index] >= self.inner.lane_ceilings[lane_index] {
            return false;
        }
        let protected_for_waiters = (0..state.waiters.len())
            .filter(|index| *index != lane_index && !state.waiters[*index].is_empty())
            .map(|index| self.inner.lane_reservations[index].saturating_sub(state.running[index]))
            .sum::<usize>();
        self.inner.capacity - total_running > protected_for_waiters
    }

    /// Grants free slots to queued waiters.
    ///
    /// A lane still below its reservation goes first; otherwise the earliest
    /// arrival among the lanes that fit wins. Each grant wakes one waiter.
    fn dispatch(&self, state: &mut AdmissionState) {
        loop {
            let next = (0..state.waiters.len())
                .filter(|index| self.can_admit(state, *index))
                .filter_map(|index| {
                    let head = state.waiters[index].front()?;
                    let above_reservation = state.running[index] >= self.inner.lane_reservations[index];
                    Some(((above_reservation, head.arrival), index))
                })
                .min_by_key(|(order, _)| *order)
                .map(|(_, index)| index);
            let Some(lane_index) = next else {
                return;
            };
            let Some(slot) = state.waiters[lane_index].pop_front() else {
                return;
            };
            state.running[lane_index] += 1;
            slot.granted.store(true, std::sync::atomic::Ordering::Release);
            slot.notify.notify_one();
        }
    }

    fn release(&self, lane: BlockingLane) {
        let mut state = self.lock();
        let running = &mut state.running[lane.index()];
        debug_assert!(*running > 0, "blocking admission permit released more than once");
        *running = running.saturating_sub(1);
        self.dispatch(&mut state);
    }

    pub(crate) fn snapshot(&self, lane: BlockingLane) -> LaneAdmissionSnapshot {
        let state = self.lock();
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

/// A queued request that leaves the queue exactly once.
struct QueuedWaiter<'a> {
    budget: &'a GlobalBlockingBudget,
    lane: BlockingLane,
    slot: Arc<WaiterSlot>,
    settled: bool,
}

impl QueuedWaiter<'_> {
    /// Returns whether the slot was granted; otherwise leaves the queue.
    fn settle(mut self) -> bool {
        self.settled = true;
        let mut state = self.budget.lock();
        if self.slot.is_granted() {
            return true;
        }
        self.leave_queue(&mut state);
        false
    }

    fn leave_queue(&self, state: &mut AdmissionState) {
        state.waiters[self.lane.index()].retain(|queued| !Arc::ptr_eq(queued, &self.slot));
        // A lane without waiters no longer protects its reservation.
        self.budget.dispatch(state);
    }
}

impl Drop for QueuedWaiter<'_> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // The acquiring future was dropped while queued.
        let mut state = self.budget.lock();
        if self.slot.is_granted() {
            let running = &mut state.running[self.lane.index()];
            *running = running.saturating_sub(1);
            self.budget.dispatch(&mut state);
        } else {
            self.leave_queue(&mut state);
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn budget(capacity: usize) -> GlobalBlockingBudget {
        GlobalBlockingBudget::new(capacity, [capacity; 3], [1; 3])
    }

    #[tokio::test]
    async fn waiters_of_one_lane_are_admitted_in_arrival_order() {
        let budget = budget(1);
        let held = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut waiters = Vec::new();
        for index in 0..4 {
            let waiter_budget = budget.clone();
            let order_tx = order_tx.clone();
            waiters.push(tokio::spawn(async move {
                let permit = waiter_budget
                    .acquire(BlockingLane::StorageIo, deadline())
                    .await
                    .unwrap();
                order_tx.send(index).unwrap();
                tokio::task::yield_now().await;
                drop(permit);
            }));
            // Queue the waiters one after another.
            while budget.lock().waiters[BlockingLane::StorageIo.index()].len() < index + 1 {
                tokio::task::yield_now().await;
            }
        }
        drop(order_tx);
        drop(held);
        for waiter in waiters {
            waiter.await.unwrap();
        }
        let mut order = Vec::new();
        while let Some(index) = order_rx.recv().await {
            order.push(index);
        }
        assert_eq!(order, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn a_new_request_does_not_pass_a_queued_waiter() {
        let budget = budget(1);
        let held = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let queued = {
            let budget = budget.clone();
            tokio::spawn(async move {
                let permit = budget.acquire(BlockingLane::StorageIo, deadline()).await;
                let admitted = permit.is_ok();
                let _ = release_rx.await;
                drop(permit);
                admitted
            })
        };
        while budget.lock().waiters[BlockingLane::StorageIo.index()].is_empty() {
            tokio::task::yield_now().await;
        }
        drop(held);
        // The slot went to the queued waiter, so a newcomer finds none free.
        let newcomer = budget
            .acquire(BlockingLane::StorageIo, Instant::now() + Duration::from_millis(20))
            .await;
        assert!(newcomer.is_err());
        release_tx.send(()).unwrap();
        assert!(queued.await.unwrap());
    }

    #[tokio::test]
    async fn one_release_grants_and_wakes_one_waiter() {
        let budget = budget(2);
        let first = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let _second = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let mut waiters = Vec::new();
        for _ in 0..3 {
            let budget = budget.clone();
            waiters.push(tokio::spawn(async move {
                budget.acquire(BlockingLane::StorageIo, deadline()).await.map(drop)
            }));
        }
        while budget.lock().waiters[BlockingLane::StorageIo.index()].len() < 3 {
            tokio::task::yield_now().await;
        }

        drop(first);
        {
            let state = budget.lock();
            assert_eq!(state.waiters[BlockingLane::StorageIo.index()].len(), 2);
            assert_eq!(state.running[BlockingLane::StorageIo.index()], 2);
        }
        for waiter in waiters.drain(..1) {
            waiter.await.unwrap().unwrap();
        }
        for waiter in waiters {
            waiter.abort();
        }
    }

    #[tokio::test]
    async fn a_lane_below_its_reservation_is_served_before_a_borrower() {
        let budget = budget(2);
        let storage = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let storage_extra = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let borrower = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.acquire(BlockingLane::StorageIo, deadline()).await.map(drop) })
        };
        while budget.lock().waiters[BlockingLane::StorageIo.index()].is_empty() {
            tokio::task::yield_now().await;
        }
        let metadata = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.acquire(BlockingLane::MetadataIo, deadline()).await.map(drop) })
        };
        while budget.lock().waiters[BlockingLane::MetadataIo.index()].is_empty() {
            tokio::task::yield_now().await;
        }

        drop(storage_extra);
        {
            let state = budget.lock();
            assert_eq!(state.running[BlockingLane::MetadataIo.index()], 1);
            assert_eq!(state.waiters[BlockingLane::StorageIo.index()].len(), 1);
        }
        metadata.await.unwrap().unwrap();
        drop(storage);
        borrower.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_timed_out_waiter_leaves_the_queue() {
        let budget = budget(1);
        let held = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let result = budget
            .acquire(BlockingLane::StorageIo, Instant::now() + Duration::from_millis(10))
            .await;
        assert!(result.is_err());
        assert!(budget.lock().waiters[BlockingLane::StorageIo.index()].is_empty());
        drop(held);
        assert_eq!(budget.snapshot(BlockingLane::StorageIo).global_running, 0);
    }

    #[tokio::test]
    async fn a_dropped_waiter_returns_a_slot_granted_to_it() {
        let budget = budget(1);
        let held = budget.acquire(BlockingLane::StorageIo, deadline()).await.unwrap();
        let mut waiter = Box::pin(budget.acquire(BlockingLane::StorageIo, deadline()));
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        drop(held);
        // The slot is granted but the waiter never observes it.
        assert_eq!(budget.snapshot(BlockingLane::StorageIo).global_running, 1);
        drop(waiter);
        assert_eq!(budget.snapshot(BlockingLane::StorageIo).global_running, 0);
        assert!(budget.acquire(BlockingLane::StorageIo, deadline()).await.is_ok());
    }
}
