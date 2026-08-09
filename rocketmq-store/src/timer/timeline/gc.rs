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
use std::collections::BTreeSet;
use std::sync::Arc;

use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_local::timer::partition_manifest::TimerPayloadPartitionKey;
use rocketmq_store_local::timer::partition_manifest::TimerPayloadPartitionState;
use rocketmq_store_local::timer::payload_store::TimerPayloadStore;
use rocketmq_store_rocksdb::batch::RocksDbWriteBatch;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::state_index::RocksDbTimelineStateIndex;
use rocketmq_store_rocksdb::timer::state_index::TimelineState;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use thiserror::Error;

use super::usage_summary_keys;
use super::TimelineReadyOutbox;
use super::TimelineReceiptStore;

/// Terminal-only GC coordinated by state, receipt, replication, snapshot, and grace fences.
pub(crate) struct TimelineGcService {
    timeline: Arc<RocksDbTimelineIndex>,
    state: RocksDbTimelineStateIndex,
    payload: Arc<TimerPayloadStore>,
    retention_grace_ms: u64,
}

impl TimelineGcService {
    pub(crate) fn new(
        timeline: Arc<RocksDbTimelineIndex>,
        payload: Arc<TimerPayloadStore>,
        retention_grace_ms: u64,
    ) -> Self {
        Self {
            state: timeline.state_index(),
            timeline,
            payload,
            retention_grace_ms,
        }
    }

    /// Deletes one bounded terminal page and then reclaims now-empty whole partitions.
    pub(crate) fn run_once(
        &self,
        now_ms: i64,
        completion_physical_cursor: i64,
        replicated_physical_cursor: i64,
        max_records: usize,
    ) -> Result<TimelineGcResult, TimelineGcError> {
        if now_ms < 0 || completion_physical_cursor < 0 || replicated_physical_cursor < 0 || max_records == 0 {
            return Err(TimelineGcError::InvalidFence);
        }
        let grace_ms = i64::try_from(self.retention_grace_ms).unwrap_or(i64::MAX);
        let due_fence = now_ms.saturating_sub(grace_ms);
        let requested_fence = TimelineKeyV1 {
            due_time_ms: due_fence,
            lane: 0,
            timer_id: TimerId::new(0),
            generation: TimerGeneration::new(0),
        };
        let candidates = self.timeline.gc_candidates(requested_fence, max_records)?;
        let receipts = TimelineReceiptStore::new(Arc::clone(&self.timeline));
        let mut batch = RocksDbWriteBatch::with_capacity(candidates.len().saturating_mul(4));
        let mut usage_deltas = BTreeMap::<Vec<u8>, (u64, u64)>::new();
        let mut partitions = BTreeSet::new();
        let mut deleted = 0usize;
        for candidate in candidates {
            let Some(state) = self.state.get(candidate.key.timer_id, candidate.key.generation)? else {
                continue;
            };
            if !terminal_grace_elapsed(&state, due_fence) {
                continue;
            }
            let source_end = candidate
                .record
                .source_physical_offset
                .saturating_add(i64::from(candidate.record.source_size));
            if source_end > replicated_physical_cursor {
                continue;
            }
            if state.state == TimelineState::Delivered {
                let Some(receipt) = receipts.get(state.route.delivery_token())? else {
                    continue;
                };
                let final_end = receipt
                    .final_physical_offset
                    .saturating_add(i64::from(receipt.final_record_size));
                if final_end > completion_physical_cursor || final_end > replicated_physical_cursor {
                    continue;
                }
                TimelineReceiptStore::delete(&mut batch, state.route.delivery_token())?;
            }
            let payload = self.payload.read(candidate.record.payload)?;
            let encoded_bytes = u64::try_from(payload.encoded_len()?).unwrap_or(u64::MAX);
            let keys = usage_summary_keys(&payload.real_topic, candidate.key.due_time_ms);
            for key in [keys.global, keys.topic, keys.tenant, keys.bucket] {
                let delta = usage_deltas.entry(key).or_default();
                delta.0 = delta.0.saturating_add(1);
                delta.1 = delta.1.saturating_add(encoded_bytes);
            }
            partitions.insert(TimerPayloadPartitionKey {
                due_day_utc: candidate.record.payload.due_day_utc(),
                lane: candidate.record.payload.lane(),
            });
            RocksDbTimelineIndex::append_delete_entry(&mut batch, candidate.key);
            RocksDbTimelineStateIndex::append_delete(&mut batch, candidate.key.timer_id, candidate.key.generation);
            TimelineReadyOutbox::delete_ready(&mut batch, candidate.key);
            TimelineReadyOutbox::delete_late_ready(&mut batch, candidate.key);
            deleted = deleted.saturating_add(1);
        }
        for (key, (count_delta, bytes_delta)) in usage_deltas {
            let (count, bytes) = self.timeline.bucket_summary(&key)?.unwrap_or_default();
            if count < count_delta || bytes < bytes_delta {
                return Err(TimelineGcError::UsageUnderflow);
            }
            RocksDbTimelineIndex::append_bucket_summary(&mut batch, key, count - count_delta, bytes - bytes_delta);
        }
        if deleted > 0 {
            self.timeline.write_batch(&batch)?;
        }

        let mut partitions_deleted = 0usize;
        for partition in partitions {
            let day_start = i64::from(partition.due_day_utc).saturating_mul(86_400_000);
            let still_referenced = !self
                .timeline
                .range_scan(day_start, day_start.saturating_add(86_400_000), None, 1, 4 * 1024)?
                .entries
                .is_empty();
            if still_referenced {
                continue;
            }
            let Some(manifest) = self.payload.partition_manifest(partition) else {
                continue;
            };
            if manifest.snapshot_pin_generation != 0 {
                continue;
            }
            match manifest.state {
                TimerPayloadPartitionState::Open => {
                    self.payload.seal_partition(partition)?;
                    self.payload.mark_gc_eligible(partition)?;
                }
                TimerPayloadPartitionState::Sealed => self.payload.mark_gc_eligible(partition)?,
                TimerPayloadPartitionState::GcEligible => {}
                TimerPayloadPartitionState::Deleted => continue,
            }
            if self.payload.gc_partition(partition, true, true, true)? {
                partitions_deleted = partitions_deleted.saturating_add(1);
            }
        }
        Ok(TimelineGcResult {
            records_deleted: deleted,
            partitions_deleted,
        })
    }
}

fn terminal_grace_elapsed(
    state: &rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1,
    terminal_fence_ms: i64,
) -> bool {
    matches!(state.state, TimelineState::Delivered | TimelineState::Cancelled)
        && state.terminal_at_ms > 0
        && state.terminal_at_ms <= terminal_fence_ms
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TimelineGcResult {
    pub(crate) records_deleted: usize,
    pub(crate) partitions_deleted: usize,
}

#[derive(Debug, Error)]
pub(crate) enum TimelineGcError {
    #[error("invalid Extended GC fence or budget")]
    InvalidFence,
    #[error("Extended usage summary would underflow")]
    UsageUnderflow,
    #[error(transparent)]
    Timeline(#[from] rocketmq_error::RocketMQError),
    #[error(transparent)]
    Payload(#[from] rocketmq_store_local::timer::payload_store::TimerPayloadStoreError),
    #[error(transparent)]
    PayloadRecord(#[from] rocketmq_store_local::timer::payload_record::TimerPayloadRecordError),
}

#[cfg(test)]
mod tests {
    use rocketmq_store_api::PersistedTimerRoute;
    use rocketmq_store_api::TimerEngineEpoch;
    use rocketmq_store_api::TimerEngineId;
    use rocketmq_store_api::TimerGeneration;
    use rocketmq_store_api::EXTENDED_TIMELINE_FORMAT_VERSION;
    use rocketmq_store_rocksdb::timer::state_index::TimelineStateRecordV1;

    use super::*;

    fn terminal_record(terminal_at_ms: i64) -> TimelineStateRecordV1 {
        TimelineStateRecordV1 {
            state: TimelineState::Delivered,
            state_version: 1,
            route: PersistedTimerRoute::try_new(
                TimerEngineId::ExtendedTimeline,
                EXTENDED_TIMELINE_FORMAT_VERSION,
                1,
                TimerGeneration::new(0),
                "gc-token",
            )
            .expect("route"),
            admission_epoch: TimerEngineEpoch::new(1),
            owner_epoch: TimerEngineEpoch::new(1),
            claim_seq: 1,
            due_time_ms: 10,
            lane: 0,
            terminal_at_ms,
            shadow_only: false,
        }
    }

    #[test]
    fn gc_grace_starts_at_the_terminal_transition_not_the_due_time() {
        assert!(!terminal_grace_elapsed(&terminal_record(0), i64::MAX));
        assert!(!terminal_grace_elapsed(&terminal_record(1_000), 999));
        assert!(terminal_grace_elapsed(&terminal_record(1_000), 1_000));

        let mut pending = terminal_record(1);
        pending.state = TimelineState::Pending;
        assert!(!terminal_grace_elapsed(&pending, i64::MAX));
    }
}
