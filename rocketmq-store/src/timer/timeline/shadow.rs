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
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::ShadowObservationKind;

/// Stable reason classes for bounded shadow comparison samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ShadowDiffReason {
    MissingTimeline,
    Identity,
    Deadline,
    PayloadChecksum,
    CancelState,
    DueObservation,
}

/// One compact shadow difference. Message bodies are intentionally excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ShadowDiffSample {
    pub(crate) reason: ShadowDiffReason,
    pub(crate) source_cq_offset: i64,
    pub(crate) source_physical_offset: i64,
    pub(crate) timer_id: TimerId,
    pub(crate) generation: TimerGeneration,
    pub(crate) due_time_ms: i64,
}

/// Expected facts derived independently from the durable Timer CQ source.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ShadowExpectedRecord {
    pub(crate) source_cq_offset: i64,
    pub(crate) source_physical_offset: i64,
    pub(crate) source_size: u32,
    pub(crate) timer_id: TimerId,
    pub(crate) generation: TimerGeneration,
    pub(crate) due_time_ms: i64,
    pub(crate) cancelled: bool,
}

/// Bounded, restart-safe-source shadow reconciler.
///
/// Counters are live telemetry; durable observations remain in RocksDB and are
/// authoritative after restart. Samples are intentionally disposable and capped.
pub(crate) struct ShadowReconciler {
    timeline: Arc<RocksDbTimelineIndex>,
    payloads: Arc<TimerPayloadStore>,
    sample_limit: usize,
    samples: Mutex<BTreeMap<ShadowDiffReason, VecDeque<ShadowDiffSample>>>,
    compared: AtomicU64,
    differences: AtomicU64,
    due_observed: AtomicU64,
}

impl ShadowReconciler {
    pub(crate) fn new(
        timeline: Arc<RocksDbTimelineIndex>,
        payloads: Arc<TimerPayloadStore>,
        sample_limit: usize,
    ) -> Self {
        Self {
            timeline,
            payloads,
            sample_limit: sample_limit.max(1),
            samples: Mutex::new(BTreeMap::new()),
            compared: AtomicU64::new(0),
            differences: AtomicU64::new(0),
            due_observed: AtomicU64::new(0),
        }
    }

    pub(crate) fn reconcile_materialized(&self, expected: ShadowExpectedRecord) {
        self.compared.fetch_add(1, Ordering::Relaxed);
        let existing = match self.timeline.get_shadow(
            expected.source_cq_offset,
            expected.source_physical_offset,
            expected.generation,
        ) {
            Ok(Some(existing)) => existing,
            _ => {
                self.record(expected, ShadowDiffReason::MissingTimeline);
                return;
            }
        };
        if existing.source_cq_offset.get() != expected.source_cq_offset
            || existing.source_physical_offset != expected.source_physical_offset
            || existing.source_size != expected.source_size
            || !existing.shadow_only
        {
            self.record(expected, ShadowDiffReason::Identity);
        }
        match self.payloads.read(existing.payload) {
            Ok(payload) => {
                if payload.timer_id != expected.timer_id || payload.generation != expected.generation {
                    self.record(expected, ShadowDiffReason::Identity);
                }
                if payload.due_time_ms != expected.due_time_ms {
                    self.record(expected, ShadowDiffReason::Deadline);
                }
                if payload
                    .encode()
                    .and_then(|bytes| bytes.get(bytes.len().saturating_sub(4)..).map(<[u8; 4]>::try_from))
                    .and_then(Result::ok)
                    .map(u32::from_be_bytes)
                    != Some(existing.payload.checksum())
                {
                    self.record(expected, ShadowDiffReason::PayloadChecksum);
                }
            }
            Err(_) => self.record(expected, ShadowDiffReason::PayloadChecksum),
        }
        let cancel_observed = self
            .timeline
            .shadow_observation(
                expected.source_cq_offset,
                expected.source_physical_offset,
                expected.generation,
                ShadowObservationKind::Cancelled,
            )
            .ok()
            .flatten()
            .is_some();
        if cancel_observed != expected.cancelled {
            self.record(expected, ShadowDiffReason::CancelState);
        }
    }

    pub(crate) fn reconcile_due(&self, expected: ShadowExpectedRecord) {
        let observed = self
            .timeline
            .shadow_observation(
                expected.source_cq_offset,
                expected.source_physical_offset,
                expected.generation,
                ShadowObservationKind::Due,
            )
            .ok()
            .flatten()
            .is_some();
        if observed {
            self.due_observed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.record(expected, ShadowDiffReason::DueObservation);
        }
    }

    pub(crate) fn snapshot(&self) -> ShadowReconciliationSnapshot {
        let samples = self.samples.lock();
        ShadowReconciliationSnapshot {
            compared: self.compared.load(Ordering::Relaxed),
            differences: self.differences.load(Ordering::Relaxed),
            due_observed: self.due_observed.load(Ordering::Relaxed),
            retained_samples: samples.values().map(VecDeque::len).sum(),
        }
    }

    fn record(&self, expected: ShadowExpectedRecord, reason: ShadowDiffReason) {
        self.differences.fetch_add(1, Ordering::Relaxed);
        let sample = ShadowDiffSample {
            reason,
            source_cq_offset: expected.source_cq_offset,
            source_physical_offset: expected.source_physical_offset,
            timer_id: expected.timer_id,
            generation: expected.generation,
            due_time_ms: expected.due_time_ms,
        };
        let mut samples = self.samples.lock();
        let reason_samples = samples.entry(reason).or_default();
        if reason_samples.len() == self.sample_limit {
            reason_samples.pop_front();
        }
        reason_samples.push_back(sample);
    }
}

/// Allocation-free shadow comparison counters exposed through store runtime info.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ShadowReconciliationSnapshot {
    pub(crate) compared: u64,
    pub(crate) differences: u64,
    pub(crate) due_observed: u64,
    pub(crate) retained_samples: usize,
}

#[cfg(test)]
mod tests {
    use rocketmq_store_local::timer::payload_store::TimerPayloadStoreConfig;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn difference_samples_are_bounded_per_reason() {
        let dir = tempdir().expect("tempdir");
        let timeline = Arc::new(
            RocksDbTimelineIndex::open(dir.path())
                .expect("timeline")
                .expect("valid Timeline configuration"),
        );
        let payloads = Arc::new(
            TimerPayloadStore::new(TimerPayloadStoreConfig::for_store_root(dir.path()))
                .expect("payload store")
                .expect("valid payload configuration"),
        );
        payloads.load().expect("payload load");
        let reconciler = ShadowReconciler::new(timeline, payloads, 2);
        for offset in 0..10 {
            reconciler.reconcile_materialized(ShadowExpectedRecord {
                source_cq_offset: offset,
                source_physical_offset: offset * 100,
                source_size: 10,
                timer_id: TimerId::new(offset as u128),
                generation: TimerGeneration::new(1),
                due_time_ms: 8_000,
                cancelled: false,
            });
        }
        let snapshot = reconciler.snapshot();
        assert_eq!(snapshot.compared, 10);
        assert_eq!(snapshot.differences, 10);
        assert_eq!(snapshot.retained_samples, 2);
    }
}
