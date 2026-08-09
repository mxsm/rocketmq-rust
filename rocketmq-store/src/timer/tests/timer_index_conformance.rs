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
use crate::timer::index::TimerIndexPage;
use crate::timer::index::TimerRecordState;
use crate::timer::index::TimerSnapshotPin;
use crate::timer::request::DueTimerRecord;
use crate::timer::request::TimerSourceRecord;

#[derive(Default)]
struct MemoryTimerIndex {
    records: Mutex<BTreeMap<TimerTimelineCursor, DueTimerRecord>>,
    states: Mutex<BTreeMap<TimerId, TimerRecordState>>,
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
            self.states.lock().insert(source.id, TimerRecordState::Pending);
            self.records.lock().insert(
                cursor,
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
        from: Option<TimerTimelineCursor>,
        due_exclusive_ms: i64,
        budget: WorkBudget,
    ) -> Result<TimerIndexPage, TimerEngineError> {
        let mut page = TimerIndexPage::default();
        let mut bytes = 0usize;
        for (cursor, record) in self.records.lock().iter() {
            if from.is_some_and(|from| *cursor <= from) || cursor.due_time_ms() >= due_exclusive_ms {
                continue;
            }
            let next_bytes = bytes.saturating_add(record.source.estimated_bytes);
            if !budget.allows(page.records.len().saturating_add(1), next_bytes) {
                page.continuation = page.records.last().map(|record| record.cursor);
                break;
            }
            bytes = next_bytes;
            page.records.push(record.clone());
        }
        Ok(page)
    }

    async fn set_state(&self, timer_id: TimerId, state: TimerRecordState) -> Result<(), TimerEngineError> {
        self.states.lock().insert(timer_id, state);
        Ok(())
    }

    async fn checkpoint(&self, _checkpoint: TimerIndexCheckpoint) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn pin_snapshot(&self, gc_fence: TimerTimelineCursor) -> Result<TimerSnapshotPin, TimerEngineError> {
        Ok(TimerSnapshotPin {
            generation: self.generation.fetch_add(1, Ordering::Relaxed) + 1,
            gc_fence,
        })
    }

    async fn release_snapshot(&self, _pin: TimerSnapshotPin) -> Result<(), TimerEngineError> {
        Ok(())
    }

    async fn gc(&self, fence: TimerTimelineCursor, budget: WorkBudget) -> Result<usize, TimerEngineError> {
        let keys: Vec<_> = self
            .records
            .lock()
            .keys()
            .copied()
            .filter(|cursor| *cursor < fence)
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
        .set_state(TimerId::new(1), TimerRecordState::Cancelled)
        .await
        .expect("cancel");
    index
        .checkpoint(TimerIndexCheckpoint {
            cursor: TimerTimelineCursor::new(10, 1),
            epoch: TimerEngineEpoch::new(2),
        })
        .await
        .expect("checkpoint");
    let pin = index.pin_snapshot(TimerTimelineCursor::new(11, 0)).await.expect("pin");
    assert_eq!(index.gc(pin.gc_fence, budget(8, 128)).await.expect("gc"), 1);
    index.release_snapshot(pin).await.expect("release");
}
