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

use std::hint::black_box;
use std::path::Path;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerGeneration;
use rocketmq_store_api::TimerId;
use rocketmq_store_api::TimerPayloadStoreLocator;
use rocketmq_store_api::TimerSourceCqOffset;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimeline;
use rocketmq_store_local::timer::segmented_timeline::SegmentedTimelineConfig;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentKey;
use rocketmq_store_local::timer::timeline_segment::TimelineSegmentRecord;
use rocketmq_store_rocksdb::timer::codec::TimelineKeyV1;
use rocketmq_store_rocksdb::timer::codec::TimelineRecordV1;
use rocketmq_store_rocksdb::timer::timeline_index::RocksDbTimelineIndex;
use rocketmq_store_rocksdb::timer::timeline_index::TimelineIndexEntry;

const YEAR_MS: i64 = 365 * 86_400_000;
const BASE_MS: i64 = 1_800_000_000_000;
const BATCH_RECORDS: usize = 4_096;

#[derive(Clone, Copy, Debug)]
enum Workload {
    UniformYear,
    NinetyPercent24Hours,
    ZipfLike,
    SameSecondHotspot,
}

impl Workload {
    fn from_environment() -> Self {
        match std::env::var("ROCKETMQ_TIMER_BENCH_WORKLOAD").as_deref() {
            Ok("near") => Self::NinetyPercent24Hours,
            Ok("zipf") => Self::ZipfLike,
            Ok("hotspot") => Self::SameSecondHotspot,
            Ok("uniform") => Self::UniformYear,
            _ => Self::SameSecondHotspot,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::UniformYear => "uniform-year",
            Self::NinetyPercent24Hours => "ninety-percent-24h",
            Self::ZipfLike => "zipf-like",
            Self::SameSecondHotspot => "same-second-hotspot",
        }
    }

    fn due_time(self, sequence: u64, total: u64) -> i64 {
        let position = i64::try_from(sequence).unwrap_or(i64::MAX);
        let total = i64::try_from(total.max(1)).unwrap_or(i64::MAX);
        match self {
            Self::UniformYear => BASE_MS.saturating_add(position.saturating_mul(YEAR_MS).div_euclid(total)),
            Self::NinetyPercent24Hours if !sequence.is_multiple_of(10) => {
                BASE_MS.saturating_add(position.saturating_mul(86_400_000).div_euclid(total))
            }
            Self::NinetyPercent24Hours => {
                BASE_MS.saturating_add(position.saturating_mul(YEAR_MS).div_euclid(total).max(86_400_001))
            }
            Self::ZipfLike => {
                let bucket = position.saturating_mul(position).rem_euclid(YEAR_MS);
                BASE_MS.saturating_add(bucket)
            }
            Self::SameSecondHotspot => BASE_MS.saturating_add(1_000),
        }
    }
}

fn benchmark(c: &mut Criterion) {
    let records = std::env::var("ROCKETMQ_TIMER_BENCH_RECORDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10_000)
        .max(1);
    let workload = Workload::from_environment();
    eprintln!(
        "timer-index raw environment: os={}, arch={}, parallelism={}, records={}, workload={}, key_bytes={}, value_bytes={}, native_record_bytes={}, production_owner=experimental-until-100M-evidence",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
        records,
        workload.name(),
        TimelineKeyV1::encoded_size(),
        TimelineRecordV1::encoded_size(),
        TimelineSegmentRecord::encoded_size(),
    );

    let mut ingest = c.benchmark_group("timer-timeline-index/ingest");
    ingest.throughput(Throughput::Elements(records));
    ingest.sample_size(10);
    ingest.measurement_time(Duration::from_secs(5));
    ingest.bench_with_input(
        BenchmarkId::new("rocksdb-sync-wal", workload.name()),
        &records,
        |b, &count| {
            b.iter_batched(
                || tempfile::tempdir().expect("RocksDB ingest root"),
                |root| {
                    let index = RocksDbTimelineIndex::open(root.path()).expect("open RocksDB Timeline");
                    populate_rocks(&index, count, workload);
                    black_box(index.store().latest_sequence_number().expect("sequence"));
                },
                BatchSize::PerIteration,
            );
        },
    );
    ingest.bench_with_input(
        BenchmarkId::new("segmented-fsync", workload.name()),
        &records,
        |b, &count| {
            b.iter_batched(
                || tempfile::tempdir().expect("segmented ingest root"),
                |root| {
                    let index = SegmentedTimeline::open(root.path(), SegmentedTimelineConfig::default())
                        .expect("open segmented Timeline");
                    populate_segmented(&index, count, workload);
                    black_box(index.manifest().durable_end);
                },
                BatchSize::PerIteration,
            );
        },
    );
    ingest.finish();

    benchmark_scan(c, records, workload);
    report_physical_bytes(records, workload);
}

fn benchmark_scan(c: &mut Criterion, records: u64, workload: Workload) {
    let rocks_root = tempfile::tempdir().expect("RocksDB scan root");
    let rocks = RocksDbTimelineIndex::open(rocks_root.path()).expect("open RocksDB scan Timeline");
    populate_rocks(&rocks, records, workload);
    let native_root = tempfile::tempdir().expect("native scan root");
    let native = SegmentedTimeline::open(native_root.path(), SegmentedTimelineConfig::default())
        .expect("open native scan Timeline");
    populate_segmented(&native, records, workload);

    let mut scan = c.benchmark_group("timer-timeline-index/full-range-scan");
    scan.throughput(Throughput::Elements(records));
    scan.sample_size(10);
    scan.measurement_time(Duration::from_secs(5));
    scan.bench_function(BenchmarkId::new("rocksdb", workload.name()), |b| {
        b.iter(|| black_box(scan_rocks(&rocks)))
    });
    scan.bench_function(BenchmarkId::new("segmented", workload.name()), |b| {
        b.iter(|| black_box(scan_segmented(&native)))
    });
    scan.finish();
}

fn populate_rocks(index: &RocksDbTimelineIndex, count: u64, workload: Workload) {
    for start in (0..count).step_by(BATCH_RECORDS) {
        let end = start.saturating_add(BATCH_RECORDS as u64).min(count);
        let entries = (start..end)
            .map(|sequence| entry(sequence, count, workload))
            .collect::<Vec<_>>();
        index.put_batch(&entries, None).expect("RocksDB ingest");
    }
}

fn populate_segmented(index: &SegmentedTimeline, count: u64, workload: Workload) {
    for start in (0..count).step_by(BATCH_RECORDS) {
        let end = start.saturating_add(BATCH_RECORDS as u64).min(count);
        let entries = (start..end)
            .map(|sequence| native_record(entry(sequence, count, workload)))
            .collect::<Vec<_>>();
        index.append_batch(&entries).expect("segmented ingest");
    }
}

fn scan_rocks(index: &RocksDbTimelineIndex) -> usize {
    let mut continuation = None;
    let mut records = 0usize;
    loop {
        let page = index
            .range_scan(i64::MIN, i64::MAX, continuation, BATCH_RECORDS, BATCH_RECORDS * 128)
            .expect("RocksDB scan");
        records = records.saturating_add(page.entries.len());
        continuation = page.continuation;
        if continuation.is_none() {
            return records;
        }
    }
}

fn scan_segmented(index: &SegmentedTimeline) -> usize {
    let mut continuation = None;
    let mut records = 0usize;
    loop {
        let page = index
            .scan_due(
                None,
                i64::MAX,
                BATCH_RECORDS,
                BATCH_RECORDS * TimelineSegmentRecord::encoded_size(),
                continuation,
            )
            .expect("segmented scan");
        records = records.saturating_add(page.records.len());
        continuation = page.continuation;
        if continuation.is_none() {
            return records;
        }
    }
}

fn entry(sequence: u64, total: u64, workload: Workload) -> TimelineIndexEntry {
    let due_time_ms = workload.due_time(sequence, total);
    let lane = u16::try_from(sequence % 16).expect("lane");
    let day = i32::try_from(due_time_ms.div_euclid(86_400_000)).expect("day");
    TimelineIndexEntry {
        key: TimelineKeyV1 {
            due_time_ms,
            lane,
            timer_id: TimerId::new(u128::from(sequence).saturating_add(1)),
            generation: TimerGeneration::new(1),
        },
        record: TimelineRecordV1 {
            payload: TimerPayloadStoreLocator::try_new(day, lane, sequence / 1_000_000, sequence * 128, 128, 7)
                .expect("payload"),
            source_cq_offset: TimerSourceCqOffset::new(i64::try_from(sequence).expect("source offset")),
            source_physical_offset: i64::try_from(sequence.saturating_mul(1_024)).expect("physical offset"),
            source_size: 128,
            state_version: 0,
            owner_engine: TimerEngineId::ExtendedTimeline,
            shadow_only: false,
        },
    }
}

fn native_record(entry: TimelineIndexEntry) -> TimelineSegmentRecord {
    TimelineSegmentRecord {
        key: TimelineSegmentKey {
            due_time_ms: entry.key.due_time_ms,
            lane: entry.key.lane,
            timer_id: entry.key.timer_id,
            generation: entry.key.generation,
        },
        payload: entry.record.payload,
        source_cq_offset: entry.record.source_cq_offset,
        source_physical_offset: entry.record.source_physical_offset,
        source_size: entry.record.source_size,
        state_version: entry.record.state_version,
        owner_engine: entry.record.owner_engine,
        shadow_only: entry.record.shadow_only,
    }
}

fn report_physical_bytes(records: u64, workload: Workload) {
    let rocks_root = tempfile::tempdir().expect("RocksDB size root");
    let rocks = RocksDbTimelineIndex::open(rocks_root.path()).expect("open RocksDB size Timeline");
    populate_rocks(&rocks, records, workload);
    rocks.close();
    let rocks_bytes = directory_bytes(rocks_root.path());

    let native_root = tempfile::tempdir().expect("native size root");
    let native = SegmentedTimeline::open(native_root.path(), SegmentedTimelineConfig::default())
        .expect("open native size Timeline");
    populate_segmented(&native, records, workload);
    let native_bytes = directory_bytes(native_root.path());
    eprintln!("timer-index raw size: records={records}, rocksdb_bytes={rocks_bytes}, segmented_bytes={native_bytes}");
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            entry.metadata().map_or(0, |metadata| {
                if metadata.is_dir() {
                    directory_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
        })
        .sum()
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
