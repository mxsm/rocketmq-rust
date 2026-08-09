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

#![cfg(feature = "extended_timeline")]

use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;

fn entry(sequence: u64) -> TimelineIndexEntry {
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: 1_800_000_000_000 + (sequence as i64 % 400) * 86_400_000,
            lane: (sequence % 16) as u16,
            timer_id: TimerId::new(sequence as u128 + 1),
            generation: TimerGeneration::new(0),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(0, (sequence % 16) as u16, 0, sequence * 64, 64, 1)
                .expect("soak locator"),
            source_cq_offset: TimerSourceCqOffset::new(sequence as i64),
            source_physical_offset: (sequence * 256) as i64,
            source_size: 256,
            state_version: 0,
            owner_engine: TimerEngineId::ExtendedTimeline,
            shadow_only: false,
        },
    }
}

#[test]
#[ignore = "manual bounded-format soak; run before increasing the production admission horizon"]
fn one_hundred_thousand_records_span_the_full_four_hundred_day_horizon() {
    const RECORDS: u64 = 100_000;
    let root = tempfile::tempdir().expect("soak Timeline root");
    let timeline = RocksDbTimelineIndex::open(root.path()).expect("open soak Timeline");
    for start in (0..RECORDS).step_by(1_000) {
        let end = (start + 1_000).min(RECORDS);
        let batch = (start..end).map(entry).collect::<Vec<_>>();
        timeline.put_batch(&batch, None).expect("append soak batch");
    }

    let mut continuation = None;
    let mut observed = 0u64;
    loop {
        let page = timeline
            .range_scan(
                1_800_000_000_000,
                1_800_000_000_000 + 401 * 86_400_000,
                continuation,
                1_024,
                256 * 1024,
            )
            .expect("scan soak page");
        observed += page.entries.len() as u64;
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(observed, RECORDS);
}
