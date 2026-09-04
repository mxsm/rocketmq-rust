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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;
use std::hint::black_box;

fn entry(sequence: u64) -> TimelineIndexEntry {
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms: 1_800_000_000_000 + (sequence as i64 % 400) * 86_400_000,
            lane: (sequence % 16) as u16,
            timer_id: TimerId::new(sequence as u128 + 1),
            generation: TimerGeneration::new(0),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(0, (sequence % 16) as u16, 0, sequence * 128, 128, 1)
                .expect("benchmark locator"),
            source_cq_offset: TimerSourceCqOffset::new(sequence as i64),
            source_physical_offset: (sequence * 512) as i64,
            source_size: 512,
            state_version: 0,
            owner_engine: TimerEngineId::ExtendedTimeline,
            shadow_only: false,
        },
    }
}

fn timeline_range_scan(c: &mut Criterion) {
    let root = tempfile::tempdir().expect("benchmark Timeline root");
    let timeline = RocksDbTimelineIndex::open(root.path())
        .expect("open benchmark Timeline")
        .expect("valid benchmark Timeline configuration");
    let records = (0..16_384).map(entry).collect::<Vec<_>>();
    timeline.put_batch(&records, None).expect("seed benchmark Timeline");

    c.bench_function("timer_extended_due_page_1024", |b| {
        b.iter(|| {
            let page = timeline
                .range_scan(
                    1_800_000_000_000,
                    1_800_000_000_000 + 401 * 86_400_000,
                    None,
                    1_024,
                    256 * 1024,
                )
                .expect("bounded due page");
            black_box(page)
        })
    });
}

criterion_group!(benches, timeline_range_scan);
criterion_main!(benches);
