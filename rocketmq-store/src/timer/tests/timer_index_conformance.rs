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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use rocketmq_store_api::PersistedTimerRoute;
use rocketmq_store_api::TimerEngineEpoch;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_api::TimerTimelineCursor;

use crate::timer::engine::WorkBudget;
use crate::timer::error::TimerEngineError;
use crate::timer::index::TimerIndex;
use crate::timer::index::TimerIndexCheckpoint;
use crate::timer::index::TimerIndexCursor;
use crate::timer::index::TimerIndexPage;
use crate::timer::index::TimerRecordState;
use crate::timer::index::TimerSnapshotPin;
use crate::timer::request::DueTimerRecord;
use crate::timer::request::TimerSourceRecord;

#[derive(Default)]
struct MemoryTimerIndex {
    records: Mutex<BTreeMap<(i64, u16, TimerId, u64), DueTimerRecord>>,
    states: Mutex<BTreeMap<(TimerId, u64), TimerRecordState>>,
    checkpoint: Mutex<Option<TimerIndexCheckpoint>>,
    pins: Mutex<BTreeMap<u64, TimerSnapshotPin>>,
    generation: AtomicU64,
}

impl TimerIndex for MemoryTimerIndex {
    async fn put_batch(&self, records: Vec<TimerSourceRecord>, budget: WorkBudget) -> Result<usize, TimerEngineError> {
        let mut inserted = 0usize;
        let mut bytes = 0usize;
        for source in records {
            let next_bytes = bytes.saturating_add(source.estimated_bytes);
            if !budget.allows(inserted.saturating_add(1), next_bytes) {
                break;
            }
            bytes = next_bytes;
            let cursor = TimerTimelineCursor::new(source.due_time_ms, source.source_offset.get() as u64);
            let index_cursor = (source.due_time_ms, 0, source.id, source.route.generation().get());
            self.states
                .lock()
                .insert((source.id, source.route.generation().get()), TimerRecordState::Pending);
            self.records.lock().insert(
                index_cursor,
                DueTimerRecord {
                    source,
                    cursor,
                    shard: 0,
                },
            );
            inserted += 1;
        }
        Ok(inserted)
    }

    async fn scan_due(
        &self,
        from: Option<TimerIndexCursor>,
        due_exclusive_ms: i64,
        budget: WorkBudget,
    ) -> Result<TimerIndexPage, TimerEngineError> {
        let mut page = TimerIndexPage::default();
        let mut bytes = 0usize;
        let from_key = from
            .as_ref()
            .map(|from| (from.due_time_ms, from.lane, from.timer_id, from.generation));
        for (cursor, record) in self.records.lock().iter() {
            if from_key.is_some_and(|from| *cursor <= from) || cursor.0 >= due_exclusive_ms {
                continue;
            }
            if self.states.lock().get(&(cursor.2, cursor.3)).is_some_and(|state| {
                matches!(
                    state,
                    TimerRecordState::Delivered | TimerRecordState::Cancelled | TimerRecordState::Quarantined
                )
            }) {
                continue;
            }
            let next_bytes = bytes.saturating_add(record.source.estimated_bytes);
            if !budget.allows(page.records.len().saturating_add(1), next_bytes) {
                page.continuation = page.records.last().map(|record| {
                    TimerIndexCursor::ordered_key(
                        record.source.due_time_ms,
                        0,
                        record.source.id,
                        record.source.route.generation().get(),
                    )
                });
                break;
            }
            bytes = next_bytes;
            page.records.push(record.clone());
        }
        Ok(page)
    }

    async fn set_state(
        &self,
        timer_id: TimerId,
        generation: u64,
        state: TimerRecordState,
    ) -> Result<(), TimerEngineError> {
        self.states.lock().insert((timer_id, generation), state);
        Ok(())
    }

    async fn checkpoint(&self, checkpoint: TimerIndexCheckpoint) -> Result<(), TimerEngineError> {
        *self.checkpoint.lock() = Some(checkpoint);
        Ok(())
    }

    async fn load_checkpoint(&self) -> Result<Option<TimerIndexCheckpoint>, TimerEngineError> {
        Ok(*self.checkpoint.lock())
    }

    async fn pin_snapshot(&self, gc_fence: TimerTimelineCursor) -> Result<TimerSnapshotPin, TimerEngineError> {
        let pin = TimerSnapshotPin {
            generation: self.generation.fetch_add(1, Ordering::Relaxed) + 1,
            gc_fence,
        };
        self.pins.lock().insert(pin.generation, pin);
        Ok(pin)
    }

    async fn release_snapshot(&self, pin: TimerSnapshotPin) -> Result<(), TimerEngineError> {
        self.pins
            .lock()
            .remove(&pin.generation)
            .filter(|persisted| *persisted == pin)
            .map(|_| ())
            .ok_or_else(|| TimerEngineError::Storage(std::io::Error::other("unknown snapshot pin")))
    }

    async fn gc(&self, fence: TimerTimelineCursor, budget: WorkBudget) -> Result<usize, TimerEngineError> {
        let effective_fence = self
            .pins
            .lock()
            .values()
            .map(|pin| pin.gc_fence)
            .min()
            .map_or(fence, |pin| {
                if pin.due_time_ms() < fence.due_time_ms() {
                    pin
                } else {
                    fence
                }
            });
        let states = self.states.lock();
        let keys: Vec<_> = self
            .records
            .lock()
            .keys()
            .cloned()
            .filter(|cursor| {
                cursor.0 < effective_fence.due_time_ms()
                    && states.get(&(cursor.2, cursor.3)).is_some_and(|state| {
                        matches!(
                            state,
                            TimerRecordState::Delivered | TimerRecordState::Cancelled | TimerRecordState::Quarantined
                        )
                    })
            })
            .take(budget.max_messages)
            .collect();
        let removed = keys.len();
        let mut records = self.records.lock();
        for key in keys {
            records.remove(&key);
        }
        Ok(removed)
    }
}

fn record(id: u128, due_time_ms: i64, offset: i64, bytes: usize) -> TimerSourceRecord {
    TimerSourceRecord {
        id: TimerId::new(id),
        source_offset: TimerSourceCqOffset::new(offset),
        due_time_ms,
        payload: TimerPayloadLocator::try_new(offset * 100, bytes as u32).expect("payload"),
        route: PersistedTimerRoute::try_new(
            TimerEngineId::JavaCompat,
            1,
            7,
            TimerGeneration::new(0),
            format!("token-{id}"),
        )
        .expect("route"),
        estimated_bytes: bytes,
    }
}

fn budget(messages: usize, bytes: usize) -> WorkBudget {
    WorkBudget::try_new(messages, bytes, Instant::now() + Duration::from_secs(1)).expect("budget")
}

#[tokio::test]
async fn timer_index_conformance_pages_by_count_bytes_and_continuation() {
    let index = MemoryTimerIndex::default();
    assert_eq!(
        index
            .put_batch(vec![record(1, 10, 1, 40), record(2, 20, 2, 40)], budget(2, 80))
            .await
            .expect("put"),
        2
    );
    let first = index.scan_due(None, 100, budget(1, 80)).await.expect("scan");
    assert_eq!(first.records.len(), 1);
    let second = index
        .scan_due(first.continuation, 100, budget(2, 80))
        .await
        .expect("continue");
    assert_eq!(second.records.len(), 1);
}

#[tokio::test]
async fn timer_index_conformance_never_exceeds_the_byte_budget_for_one_large_record() {
    let index = MemoryTimerIndex::default();
    assert_eq!(
        index
            .put_batch(vec![record(1, 10, 1, 81)], budget(2, 80))
            .await
            .expect("put"),
        0
    );
}

#[tokio::test]
async fn timer_index_conformance_exposes_state_checkpoint_snapshot_and_gc_fences() {
    let index = MemoryTimerIndex::default();
    index
        .put_batch(vec![record(1, 10, 1, 16)], budget(1, 16))
        .await
        .expect("put");
    index
        .set_state(TimerId::new(1), 0, TimerRecordState::Cancelled)
        .await
        .expect("cancel");
    index
        .checkpoint(TimerIndexCheckpoint {
            cursor: TimerTimelineCursor::new(10, 1),
            epoch: TimerEngineEpoch::new(2),
        })
        .await
        .expect("checkpoint");
    assert_eq!(
        index
            .load_checkpoint()
            .await
            .expect("load checkpoint")
            .expect("present")
            .epoch
            .get(),
        2
    );
    let pin = index.pin_snapshot(TimerTimelineCursor::new(11, 0)).await.expect("pin");
    assert_eq!(index.gc(pin.gc_fence, budget(8, 128)).await.expect("gc"), 1);
    index.release_snapshot(pin).await.expect("release");
}

#[cfg(feature = "extended_timeline")]
#[tokio::test]
async fn rocksdb_pages_checkpoints_and_gc() {
    let directory = tempfile::tempdir().expect("timeline root");
    let native = std::sync::Arc::new(
        rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex::open(directory.path())
            .expect("open Timeline")
            .expect("valid Timeline configuration"),
    );
    let scope = crate::runtime::test_scope("rocksdb-timer-index-conformance");
    let index = crate::timer::timeline::RocksDbTimerIndex::new(native, scope.storage_io());

    assert_eq!(
        index
            .put_batch(vec![record(1, 10, 1, 40), record(2, 20, 2, 40)], budget(2, 80))
            .await
            .expect("put"),
        2
    );
    let first = index.scan_due(None, 100, budget(1, 80)).await.expect("scan");
    assert_eq!(first.records.len(), 1);
    let second = index
        .scan_due(first.continuation, 100, budget(2, 80))
        .await
        .expect("continue");
    assert_eq!(second.records.len(), 1);
    index
        .checkpoint(TimerIndexCheckpoint {
            cursor: TimerTimelineCursor::new(20, 2),
            epoch: TimerEngineEpoch::new(3),
        })
        .await
        .expect("checkpoint");
    assert_eq!(
        index
            .load_checkpoint()
            .await
            .expect("load checkpoint")
            .expect("checkpoint")
            .epoch
            .get(),
        3
    );
    index
        .set_state(TimerId::new(1), 0, TimerRecordState::Cancelled)
        .await
        .expect("cancel");
    let visible = index
        .scan_due(None, 100, budget(8, 256))
        .await
        .expect("scan after cancel");
    assert_eq!(
        visible
            .records
            .iter()
            .map(|record| record.source.id)
            .collect::<Vec<_>>(),
        vec![TimerId::new(2)]
    );
    let pin = index.pin_snapshot(TimerTimelineCursor::new(11, 0)).await.expect("pin");
    assert_eq!(index.gc(pin.gc_fence, budget(8, 128)).await.expect("gc"), 1);
    index.release_snapshot(pin).await.expect("release");
}

#[cfg(feature = "extended_timeline")]
#[tokio::test]
async fn segmented_pages_checkpoints_cancel_visibility_and_gc() {
    let directory = tempfile::tempdir().expect("timeline root");
    let native = std::sync::Arc::new(
        rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline::open(
            directory.path(),
            rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineConfig::default(),
        )
        .expect("open native Timeline")
        .expect("valid native Timeline configuration"),
    );
    let overlay = std::sync::Arc::new(
        rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex::open(directory.path())
            .expect("open overlay")
            .expect("valid overlay configuration"),
    );
    let scope = crate::runtime::test_scope("segmented-timer-index-conformance");
    let index =
        crate::timer::timeline::segmented_index::SegmentedTimelineIndex::new(native, overlay, scope.storage_io());

    run_backend_trace(&index).await;
}

#[cfg(feature = "extended_timeline")]
#[tokio::test]
async fn rocksdb_fixed_random_trace_matches_reference_set() {
    let directory = tempfile::tempdir().expect("timeline root");
    let native = std::sync::Arc::new(
        rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex::open(directory.path())
            .expect("open Timeline")
            .expect("valid Timeline configuration"),
    );
    let scope = crate::runtime::test_scope("rocksdb-timer-index-trace");
    let index = crate::timer::timeline::RocksDbTimerIndex::new(native, scope.storage_io());
    run_backend_trace(&index).await;
}

#[cfg(feature = "extended_timeline")]
async fn run_backend_trace<I: TimerIndex>(index: &I) {
    let mut seed = 0x4d59_5df4_d0f3_3173u64;
    let mut records = Vec::new();
    for id in 1..=257u64 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let due = 10_000 + i64::try_from(seed % 31).expect("small due bucket");
        records.push(record(u128::from(id), due, id as i64, 32));
    }
    for chunk in records.chunks(37) {
        assert_eq!(
            index
                .put_batch(chunk.to_vec(), budget(chunk.len(), chunk.len() * 32))
                .await
                .expect("put trace"),
            chunk.len()
        );
    }
    // Exact replay is idempotent and must not create a second visible generation.
    assert_eq!(
        index
            .put_batch(records[..17].to_vec(), budget(17, 17 * 32))
            .await
            .expect("replay"),
        17
    );

    let cancelled = records
        .iter()
        .filter(|record| record.id.get() % 7 == 0)
        .map(|record| record.id)
        .collect::<std::collections::BTreeSet<_>>();
    for timer_id in &cancelled {
        index
            .set_state(*timer_id, 0, TimerRecordState::Cancelled)
            .await
            .expect("cancel trace");
    }
    let mut continuation = None;
    let mut visible = Vec::new();
    loop {
        let page = index
            .scan_due(continuation, 20_000, budget(19, 19 * 32))
            .await
            .expect("trace scan");
        visible.extend(page.records.iter().map(|record| record.source.id));
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    let expected = records
        .iter()
        .filter(|record| !cancelled.contains(&record.id))
        .map(|record| record.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        visible.iter().copied().collect::<std::collections::BTreeSet<_>>(),
        expected
    );
    assert_eq!(
        visible.len(),
        expected.len(),
        "idempotent replay must not duplicate keys"
    );

    let checkpoint = TimerIndexCheckpoint {
        cursor: TimerTimelineCursor::new(20_000, 257),
        epoch: TimerEngineEpoch::new(9),
    };
    index.checkpoint(checkpoint).await.expect("checkpoint");
    assert_eq!(
        index.load_checkpoint().await.expect("load checkpoint"),
        Some(checkpoint)
    );
    let pin = index
        .pin_snapshot(TimerTimelineCursor::new(20_001, 0))
        .await
        .expect("pin");
    assert_eq!(
        index
            .gc(TimerTimelineCursor::new(30_000, 0), budget(512, 512 * 32))
            .await
            .expect("gc"),
        cancelled.len()
    );
    index.release_snapshot(pin).await.expect("release");
}
