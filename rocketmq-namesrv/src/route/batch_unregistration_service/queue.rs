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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_observability::metrics::namesrv::UnregistrationAgeSource;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;

use super::BrokerUnregistrationRequest;
use super::PendingUnregistrationKey;

pub(super) enum Submission {
    Queued,
    Coalesced,
    Fallback(QueueUnavailable, BrokerUnregistrationRequest),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QueueUnavailable {
    Full,
    Closed,
}

impl QueueUnavailable {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Closed => "closed",
        }
    }
}

pub(super) struct UnregistrationQueue {
    tx: mpsc::Sender<BrokerUnregistrationRequest>,
    pending: Arc<PendingRequests>,
}

struct PendingRequests {
    keys: DashMap<PendingUnregistrationKey, ()>,
    admission_closed: AtomicBool,
    metrics_state: Option<Mutex<PendingMetrics>>,
    metrics: NameServerMetrics,
}

impl UnregistrationAgeSource for PendingRequests {
    fn oldest_age(&self) -> Duration {
        self.metrics_state
            .as_ref()
            .map_or(Duration::ZERO, |state| state.lock().oldest_age())
    }
}

#[derive(Default)]
struct PendingMetrics {
    ages: BTreeMap<Instant, usize>,
    queued: usize,
}

impl PendingMetrics {
    fn oldest_age(&self) -> Duration {
        self.ages
            .first_key_value()
            .map_or(Duration::ZERO, |(at, _)| at.elapsed())
    }

    fn release(&mut self, at: Instant) {
        if let std::collections::btree_map::Entry::Occupied(mut entry) = self.ages.entry(at) {
            *entry.get_mut() -= 1;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }

    fn record(&self, pending: &PendingRequests) {
        // Serialize gauge publication with the associated state transitions, so
        // a late producer cannot overwrite the consumer's final zero.
        pending.metrics.record_unregistration_queue_depth(self.queued);
    }
}

impl UnregistrationQueue {
    pub(super) fn new(capacity: usize, metrics: NameServerMetrics) -> (Self, UnregistrationReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        let pending = Arc::new(PendingRequests {
            keys: DashMap::new(),
            admission_closed: AtomicBool::new(false),
            // Disabled telemetry must not serialize independent broker keys or
            // maintain an age index on the submission path.
            metrics_state: metrics.is_enabled().then(|| Mutex::new(PendingMetrics::default())),
            metrics,
        });
        pending.metrics.register_unregistration_age_source(&pending);
        (
            Self {
                tx,
                pending: Arc::clone(&pending),
            },
            UnregistrationReceiver { rx, pending },
        )
    }

    pub(super) fn submit(&self, mut request: BrokerUnregistrationRequest) -> Submission {
        let key = request.pending_key();
        let candidate = match self.pending.keys.try_entry(key) {
            Some(entry) => entry,
            None => {
                // A contended shard often contains an already accepted key.
                // Let duplicates share read access instead of queuing writers.
                // Uncontended admission still hashes and looks up only once.
                let key = request.pending_key();
                if self.pending.keys.contains_key(&key) {
                    self.record_coalesced();
                    return Submission::Coalesced;
                }
                // Recheck under the write lock before reserving capacity.
                self.pending.keys.entry(key)
            }
        };
        let entry = match candidate {
            Entry::Occupied(_) => {
                self.record_coalesced();
                return Submission::Coalesced;
            }
            Entry::Vacant(entry) => entry,
        };
        if self.pending.admission_closed.load(Ordering::Acquire) {
            return Submission::Fallback(QueueUnavailable::Closed, request);
        }
        // Reserve under the key's shard: a failed admission cannot coalesce a
        // concurrent producer, or allocate age tracking for rejected work.
        let permit = match self.tx.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(())) => return Submission::Fallback(QueueUnavailable::Full, request),
            Err(TrySendError::Closed(())) => return Submission::Fallback(QueueUnavailable::Closed, request),
        };
        let state = self.pending.metrics_state.as_ref().map(|state| {
            // Timestamp admitted work only, and read the clock before taking
            // the shared metrics lock so producers can do this concurrently.
            let now = Instant::now();
            request.enqueued_at = Some(now);
            let mut state = state.lock();
            *state.ages.entry(now).or_default() += 1;
            state.queued += 1;
            state
        });
        // Keep the shard until both publication and registration commit. The
        // consumer's cleanup must not pass an unpublished pending insertion.
        permit.send(request);
        drop(entry.insert(()));
        if let Some(state) = state.as_ref() {
            self.pending.metrics.record_unregistration_queue("queued", state.queued);
        }
        Submission::Queued
    }

    pub(super) fn close(&self) {
        self.pending.admission_closed.store(true, Ordering::Release);
    }

    pub(super) fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn record_coalesced(&self) {
        let state = self.pending.metrics_state.as_ref().map(|state| state.lock());
        if let Some(state) = state.as_ref() {
            self.pending
                .metrics
                .record_unregistration_queue("coalesced", state.queued);
        }
    }

    pub(super) fn record_fallback(&self, reason: QueueUnavailable) {
        let state = self.pending.metrics_state.as_ref().map(|state| state.lock());
        if let Some(state) = state.as_ref() {
            self.pending
                .metrics
                .record_unregistration_fallback(reason.as_str(), state.queued);
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }

    #[cfg(test)]
    pub(super) fn pending_len(&self) -> usize {
        self.pending.keys.len()
    }

    #[cfg(test)]
    pub(super) fn oldest_age(&self) -> Duration {
        self.pending.oldest_age()
    }
}

// The worker owns cleanup for buffered requests, so queue entries carry no
// shared-reference lease. A separate batch guard owns requests after dequeue.
pub(super) struct UnregistrationReceiver {
    rx: mpsc::Receiver<BrokerUnregistrationRequest>,
    pending: Arc<PendingRequests>,
}

impl UnregistrationReceiver {
    pub(super) fn close(&mut self) {
        self.pending.admission_closed.store(true, Ordering::Release);
        self.rx.close();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.rx.is_closed()
    }

    pub(super) fn is_drained(&self) -> bool {
        self.rx.is_empty() && self.rx.capacity() == self.rx.max_capacity()
    }

    pub(super) async fn recv_batch(&mut self, capacity: usize) -> Option<UnregistrationBatch> {
        let request = self.rx.recv().await?;
        let mut batch = UnregistrationBatch {
            requests: Vec::with_capacity(capacity),
            registration: BatchRegistration {
                keys: Vec::with_capacity(capacity),
                ages: if self.pending.metrics_state.is_some() {
                    Vec::with_capacity(capacity)
                } else {
                    Vec::new()
                },
                pending: Arc::clone(&self.pending),
            },
        };
        // Register cleanup before returning from the receive future. Cancelling
        // collection, even before its first poll, cannot orphan the first key.
        batch.push(request);
        Some(batch)
    }

    pub(super) async fn recv_into(&mut self, batch: &mut UnregistrationBatch) -> bool {
        match self.rx.recv().await {
            Some(request) => {
                batch.push(request);
                true
            }
            None => false,
        }
    }
}

impl Drop for UnregistrationReceiver {
    fn drop(&mut self) {
        self.close();
        // A sender holds its key's shard through channel publication and key
        // insertion. Clearing after close fences every such sender, including
        // one that reserved channel capacity before close. No new key can be
        // accepted after its shard has been cleared.
        self.pending.keys.clear();
        if let Some(state) = self.pending.metrics_state.as_ref() {
            let mut state = state.lock();
            *state = PendingMetrics::default();
            self.pending
                .metrics
                .record_unregistration_queue_state(0, Duration::ZERO);
        }
    }
}

pub(super) struct UnregistrationBatch {
    requests: Vec<BrokerUnregistrationRequest>,
    registration: BatchRegistration,
}

impl UnregistrationBatch {
    fn push(&mut self, request: BrokerUnregistrationRequest) {
        self.registration.keys.push(request.pending_key());
        if let Some(at) = request.enqueued_at {
            self.registration.ages.push(at);
        }
        self.requests.push(request);
        if let Some(state) = self.registration.pending.metrics_state.as_ref() {
            let mut state = state.lock();
            state.queued -= 1;
            state.record(&self.registration.pending);
        }
    }

    pub(super) fn len(&self) -> usize {
        self.requests.len()
    }

    #[cfg(test)]
    pub(super) fn requests(&self) -> &[BrokerUnregistrationRequest] {
        &self.requests
    }

    pub(super) fn into_parts(self) -> (Vec<BrokerUnregistrationRequest>, BatchRegistration) {
        (self.requests, self.registration)
    }
}

pub(super) struct BatchRegistration {
    keys: Vec<PendingUnregistrationKey>,
    ages: Vec<Instant>,
    pending: Arc<PendingRequests>,
}

impl Drop for BatchRegistration {
    fn drop(&mut self) {
        for key in &self.keys {
            self.pending.keys.remove(key);
        }
        if let Some(state) = self.pending.metrics_state.as_ref() {
            let mut state = state.lock();
            for at in &self.ages {
                state.release(*at);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> BrokerUnregistrationRequest {
        BrokerUnregistrationRequest::explicit(
            rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader {
                cluster_name: "cluster".into(),
                broker_name: "broker".into(),
                broker_addr: "127.0.0.1:10911".into(),
                broker_id: 0,
            },
        )
    }

    #[tokio::test]
    async fn close_drains_a_request_whose_capacity_was_reserved_before_publication() {
        let (queue, mut rx) = UnregistrationQueue::new(1, NameServerMetrics::noop());
        let request = request();
        let entry = queue.pending.keys.entry(request.pending_key());
        let permit = queue.tx.try_reserve().unwrap();
        rx.close();
        assert!(!rx.is_drained());
        permit.send(request);
        entry.or_insert(());
        let batch = rx.recv_batch(1).await.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(queue.pending_len(), 1);
        drop(batch);
        assert_eq!(queue.pending_len(), 0);
        assert!(rx.recv_batch(1).await.is_none());
    }

    #[test]
    fn receiver_drop_clears_a_registration_published_after_channel_close() {
        let (queue, mut rx) = UnregistrationQueue::new(1, NameServerMetrics::noop());
        let request = request();
        let entry = queue.pending.keys.entry(request.pending_key());
        let permit = queue.tx.try_reserve().unwrap();
        rx.close();
        let gate = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                gate.wait();
                drop(rx);
            });
            gate.wait();
            permit.send(request);
            drop(entry.or_insert(()));
        });
        assert_eq!(queue.pending_len(), 0);
        assert_eq!(queue.len(), 0);
    }

    #[cfg(feature = "otel-metrics")]
    #[tokio::test(start_paused = true)]
    async fn age_source_tracks_idle_batches_and_does_not_own_the_queue() {
        let mut config = rocketmq_observability::ObservabilityConfig {
            enabled: true,
            ..Default::default()
        };
        config.metrics.enabled = true;
        config.metrics.exporter = rocketmq_observability::MetricsExporter::Log;
        config.metrics.export_interval_millis = 3_600_000;
        let telemetry = rocketmq_observability::init_observability(&config).unwrap();
        let metrics = NameServerMetrics::from_handle(&telemetry.handle());
        let (queue, mut rx) = UnregistrationQueue::new(8, metrics);
        let source = Arc::downgrade(&queue.pending);
        assert!(matches!(queue.submit(request()), Submission::Queued));
        let batch = rx.recv_batch(8).await.unwrap();
        tokio::time::advance(Duration::from_millis(10)).await;
        let mut second = request();
        second.header.broker_id = 1;
        assert!(matches!(queue.submit(second), Submission::Queued));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(source.upgrade().unwrap().oldest_age(), Duration::from_millis(1010));
        drop(batch);
        assert_eq!(source.upgrade().unwrap().oldest_age(), Duration::from_secs(1));
        let inflight = rx.recv_batch(8).await.unwrap();
        drop(rx);
        assert_eq!(source.upgrade().unwrap().oldest_age(), Duration::ZERO);
        assert_eq!(queue.pending_len(), 0);
        drop(queue);
        drop(inflight);
        assert!(
            source.upgrade().is_none(),
            "the SDK callback must not retain queue state"
        );
    }
}
