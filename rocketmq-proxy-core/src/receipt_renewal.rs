// Copyright 2026 The RocketMQ Rust Authors
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

//! Monotonic, generation-aware scheduling for receipt-handle renewal.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

/// Operational counters for the receipt-renewal deadline scheduler.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiptRenewalMetricsSnapshot {
    /// Number of new or replacement deadlines installed.
    pub scheduled: u64,
    /// Number of due renewal attempts claimed by workers.
    pub claimed: u64,
    /// Number of transient attempts explicitly rescheduled.
    pub retries: u64,
    /// Number of successful renewal completions.
    pub successes: u64,
    /// Number of invalid receipt handles removed after a renewal response.
    pub invalid_receipts: u64,
    /// Number of receipts that reached their invisible deadline before claim.
    pub expired_before_renewal: u64,
    /// Number of stale heap entries discarded after replacement or removal.
    pub stale_entries: u64,
    /// Number of bounded heap compactions.
    pub compactions: u64,
    /// Largest observed delay between a due deadline and its claim.
    pub max_due_lag_micros: u64,
    /// Number of live receipt deadlines.
    pub live: usize,
    /// Number of heap entries, including entries awaiting lazy stale removal.
    pub heap_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReceiptRenewalToken {
    generation: u64,
    attempt: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ScheduledReceiptRenewal<K> {
    pub(crate) key: K,
    pub(crate) token: ReceiptRenewalToken,
    pub(crate) due_at: Instant,
    pub(crate) expires_at: Instant,
}

pub(crate) struct ReceiptRenewalBatch<K> {
    pub(crate) claims: Vec<ScheduledReceiptRenewal<K>>,
    pub(crate) expired: Vec<K>,
}

#[derive(Debug, Clone, Copy)]
struct LiveRenewal {
    generation: u64,
    attempt: u64,
    deadline: Instant,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct RenewalEntry<K> {
    deadline: Instant,
    generation: u64,
    key: K,
}

impl<K> PartialEq for RenewalEntry<K> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.generation == other.generation
    }
}

impl<K> Eq for RenewalEntry<K> {}

impl<K> PartialOrd for RenewalEntry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for RenewalEntry<K> {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.generation.cmp(&self.generation))
    }
}

struct ReceiptRenewalState<K> {
    next_generation: u64,
    heap: BinaryHeap<RenewalEntry<K>>,
    live: HashMap<K, LiveRenewal>,
}

impl<K> Default for ReceiptRenewalState<K> {
    fn default() -> Self {
        Self {
            next_generation: 1,
            heap: BinaryHeap::new(),
            live: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct ReceiptRenewalMetrics {
    scheduled: AtomicU64,
    claimed: AtomicU64,
    retries: AtomicU64,
    successes: AtomicU64,
    invalid_receipts: AtomicU64,
    expired_before_renewal: AtomicU64,
    stale_entries: AtomicU64,
    compactions: AtomicU64,
    max_due_lag_micros: AtomicU64,
}

struct ReceiptRenewalScheduleInner<K> {
    state: Mutex<ReceiptRenewalState<K>>,
    notify: Notify,
    metrics: ReceiptRenewalMetrics,
}

pub(crate) struct ReceiptRenewalSchedule<K> {
    inner: Arc<ReceiptRenewalScheduleInner<K>>,
}

impl<K> Clone for ReceiptRenewalSchedule<K> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K> Default for ReceiptRenewalSchedule<K> {
    fn default() -> Self {
        Self {
            inner: Arc::new(ReceiptRenewalScheduleInner {
                state: Mutex::new(ReceiptRenewalState::default()),
                notify: Notify::new(),
                metrics: ReceiptRenewalMetrics::default(),
            }),
        }
    }
}

impl<K> ReceiptRenewalSchedule<K>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn schedule(&self, key: K, deadline: Instant, expires_at: Instant) {
        let mut state = self.lock_state();
        let generation = state.next_generation;
        state.next_generation = state.next_generation.wrapping_add(1).max(1);
        state.live.insert(
            key.clone(),
            LiveRenewal {
                generation,
                attempt: 0,
                deadline,
                expires_at,
            },
        );
        state.heap.push(RenewalEntry {
            deadline,
            generation,
            key,
        });
        self.inner.metrics.scheduled.fetch_add(1, Ordering::Relaxed);
        self.compact_if_needed(&mut state);
        drop(state);
        self.inner.notify.notify_one();
    }

    pub(crate) fn remove(&self, key: &K) -> bool {
        let mut state = self.lock_state();
        let removed = state.live.remove(key).is_some();
        if removed {
            self.compact_if_needed(&mut state);
        }
        drop(state);
        if removed {
            self.inner.notify.notify_one();
        }
        removed
    }

    pub(crate) fn claim_due(&self, now: Instant, limit: usize, lease: Duration) -> ReceiptRenewalBatch<K> {
        let mut state = self.lock_state();
        let mut claims = Vec::with_capacity(limit);
        let mut expired = Vec::new();

        while claims.len() < limit {
            let Some(entry) = state.heap.peek() else {
                break;
            };
            if entry.deadline > now {
                break;
            }
            let Some(entry) = state.heap.pop() else {
                break;
            };
            let Some(live) = state.live.get(&entry.key).copied() else {
                self.record_stale_entry();
                continue;
            };
            if live.generation != entry.generation || live.deadline != entry.deadline {
                self.record_stale_entry();
                continue;
            }
            if live.expires_at <= now {
                state.live.remove(&entry.key);
                expired.push(entry.key);
                self.inner
                    .metrics
                    .expired_before_renewal
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let attempt = live.attempt.saturating_add(1);
            let lease_deadline = now.checked_add(lease).unwrap_or(live.expires_at).min(live.expires_at);
            let updated = LiveRenewal {
                attempt,
                deadline: lease_deadline,
                ..live
            };
            state.live.insert(entry.key.clone(), updated);
            state.heap.push(RenewalEntry {
                deadline: lease_deadline,
                generation: live.generation,
                key: entry.key.clone(),
            });
            claims.push(ScheduledReceiptRenewal {
                key: entry.key,
                token: ReceiptRenewalToken {
                    generation: live.generation,
                    attempt,
                },
                due_at: entry.deadline,
                expires_at: live.expires_at,
            });
            self.inner.metrics.claimed.fetch_add(1, Ordering::Relaxed);
            self.record_due_lag(now.saturating_duration_since(entry.deadline));
        }
        self.compact_if_needed(&mut state);
        ReceiptRenewalBatch { claims, expired }
    }

    pub(crate) fn reschedule_claim(
        &self,
        key: &K,
        token: ReceiptRenewalToken,
        deadline: Instant,
        expires_at: Instant,
        retry: bool,
    ) -> bool {
        let mut state = self.lock_state();
        let Some(live) = state.live.get(key).copied() else {
            return false;
        };
        if live.generation != token.generation || live.attempt != token.attempt {
            return false;
        }
        let deadline = deadline.min(expires_at);
        let generation = state.next_generation;
        state.next_generation = state.next_generation.wrapping_add(1).max(1);
        state.live.insert(
            key.clone(),
            LiveRenewal {
                generation,
                attempt: 0,
                deadline,
                expires_at,
            },
        );
        state.heap.push(RenewalEntry {
            deadline,
            generation,
            key: key.clone(),
        });
        if retry {
            self.inner.metrics.retries.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.metrics.successes.fetch_add(1, Ordering::Relaxed);
        }
        self.compact_if_needed(&mut state);
        drop(state);
        self.inner.notify.notify_one();
        true
    }

    pub(crate) fn remove_claim(&self, key: &K, token: ReceiptRenewalToken, invalid: bool) -> bool {
        let mut state = self.lock_state();
        let matches = state
            .live
            .get(key)
            .is_some_and(|live| live.generation == token.generation && live.attempt == token.attempt);
        if !matches {
            return false;
        }
        state.live.remove(key);
        if invalid {
            self.inner.metrics.invalid_receipts.fetch_add(1, Ordering::Relaxed);
        }
        self.compact_if_needed(&mut state);
        drop(state);
        self.inner.notify.notify_one();
        true
    }

    pub(crate) fn is_current(&self, key: &K, token: ReceiptRenewalToken) -> bool {
        self.lock_state()
            .live
            .get(key)
            .is_some_and(|live| live.generation == token.generation && live.attempt == token.attempt)
    }

    pub(crate) async fn wait_until_due(&self) {
        loop {
            let notified = self.inner.notify.notified();
            let deadline = {
                let mut state = self.lock_state();
                self.discard_stale_head(&mut state);
                state.heap.peek().map(|entry| entry.deadline)
            };
            match deadline {
                Some(deadline) if deadline <= Instant::now() => return,
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, notified).await.is_err() {
                        return;
                    }
                }
                None => notified.await,
            }
        }
    }

    pub(crate) fn metrics_snapshot(&self) -> ReceiptRenewalMetricsSnapshot {
        let state = self.lock_state();
        ReceiptRenewalMetricsSnapshot {
            scheduled: self.inner.metrics.scheduled.load(Ordering::Relaxed),
            claimed: self.inner.metrics.claimed.load(Ordering::Relaxed),
            retries: self.inner.metrics.retries.load(Ordering::Relaxed),
            successes: self.inner.metrics.successes.load(Ordering::Relaxed),
            invalid_receipts: self.inner.metrics.invalid_receipts.load(Ordering::Relaxed),
            expired_before_renewal: self.inner.metrics.expired_before_renewal.load(Ordering::Relaxed),
            stale_entries: self.inner.metrics.stale_entries.load(Ordering::Relaxed),
            compactions: self.inner.metrics.compactions.load(Ordering::Relaxed),
            max_due_lag_micros: self.inner.metrics.max_due_lag_micros.load(Ordering::Relaxed),
            live: state.live.len(),
            heap_entries: state.heap.len(),
        }
    }

    fn discard_stale_head(&self, state: &mut ReceiptRenewalState<K>) {
        while let Some(entry) = state.heap.peek() {
            let current = state
                .live
                .get(&entry.key)
                .is_some_and(|live| live.generation == entry.generation && live.deadline == entry.deadline);
            if current {
                break;
            }
            state.heap.pop();
            self.record_stale_entry();
        }
    }

    fn compact_if_needed(&self, state: &mut ReceiptRenewalState<K>) {
        let stale = state.heap.len().saturating_sub(state.live.len());
        if stale == 0 || (state.live.is_empty() && state.heap.is_empty()) {
            return;
        }
        if !state.live.is_empty() && stale <= state.live.len().saturating_mul(2) {
            return;
        }
        state.heap = state
            .live
            .iter()
            .map(|(key, live)| RenewalEntry {
                deadline: live.deadline,
                generation: live.generation,
                key: key.clone(),
            })
            .collect();
        self.inner.metrics.compactions.fetch_add(1, Ordering::Relaxed);
    }

    fn lock_state(&self) -> MutexGuard<'_, ReceiptRenewalState<K>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn record_stale_entry(&self) {
        self.inner.metrics.stale_entries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_due_lag(&self, lag: Duration) {
        let micros = lag.as_micros().min(u128::from(u64::MAX)) as u64;
        self.inner
            .metrics
            .max_due_lag_micros
            .fetch_max(micros, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn earlier_deadline_wakes_waiter() {
        let schedule = ReceiptRenewalSchedule::<String>::default();
        schedule.schedule(
            "later".to_owned(),
            Instant::now() + Duration::from_secs(30),
            Instant::now() + Duration::from_secs(60),
        );
        let waiter = {
            let schedule = schedule.clone();
            tokio::spawn(async move { schedule.wait_until_due().await })
        };

        tokio::task::yield_now().await;
        schedule.schedule(
            "earlier".to_owned(),
            Instant::now() + Duration::from_secs(5),
            Instant::now() + Duration::from_secs(15),
        );
        tokio::time::advance(Duration::from_secs(5)).await;

        waiter.await.expect("deadline waiter");
        let batch = schedule.claim_due(Instant::now(), 1, Duration::from_secs(1));
        assert_eq!(batch.claims.len(), 1);
        assert_eq!(batch.claims[0].key, "earlier");
    }

    #[tokio::test(start_paused = true)]
    async fn replacement_and_removal_make_old_entries_stale() {
        let schedule = ReceiptRenewalSchedule::<String>::default();
        let key = "message".to_owned();
        schedule.schedule(
            key.clone(),
            Instant::now() + Duration::from_secs(5),
            Instant::now() + Duration::from_secs(15),
        );
        schedule.schedule(
            key.clone(),
            Instant::now() + Duration::from_secs(10),
            Instant::now() + Duration::from_secs(20),
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(schedule
            .claim_due(Instant::now(), 1, Duration::from_secs(1))
            .claims
            .is_empty());

        assert!(schedule.remove(&key));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(schedule
            .claim_due(Instant::now(), 1, Duration::from_secs(1))
            .claims
            .is_empty());
        assert_eq!(schedule.metrics_snapshot().live, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn claim_installs_lease_and_stale_completion_cannot_reschedule() {
        let schedule = ReceiptRenewalSchedule::<String>::default();
        let key = "message".to_owned();
        schedule.schedule(
            key.clone(),
            Instant::now() + Duration::from_secs(5),
            Instant::now() + Duration::from_secs(15),
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        let first = schedule
            .claim_due(Instant::now(), 1, Duration::from_secs(1))
            .claims
            .remove(0);
        assert!(schedule
            .claim_due(Instant::now(), 1, Duration::from_secs(1))
            .claims
            .is_empty());

        tokio::time::advance(Duration::from_secs(1)).await;
        let second = schedule
            .claim_due(Instant::now(), 1, Duration::from_secs(1))
            .claims
            .remove(0);
        assert!(!schedule.reschedule_claim(
            &key,
            first.token,
            Instant::now() + Duration::from_secs(5),
            first.expires_at,
            false,
        ));
        assert!(schedule.reschedule_claim(
            &key,
            second.token,
            Instant::now() + Duration::from_secs(1),
            second.expires_at,
            true,
        ));
    }
}
