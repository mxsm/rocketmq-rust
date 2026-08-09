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

use std::hint::black_box;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_store_local::timer::metrics::TimerStorageMetrics;
use rocketmq_store_local::timer::slot::Slot;
use rocketmq_store_local::timer::slot_drain_file::SlotDrainFileBuilder;
use rocketmq_store_local::timer::slot_drain_file::SlotDrainLocator;
use rocketmq_store_local::timer::timer_wheel::TimerWheel;
use tempfile::tempdir;

fn timer_storage_benchmark(criterion: &mut Criterion) {
    let directory = tempdir().expect("temporary benchmark directory");
    let metrics = Arc::new(TimerStorageMetrics::default());
    let wheel = TimerWheel::with_page_size_and_metrics(
        directory.path().join("timerwheel"),
        604_800,
        1_000,
        4_096,
        Arc::clone(&metrics),
    );
    wheel.load().expect("load benchmark wheel");
    let mut tick = 0i64;
    criterion.bench_function("timer_wheel_single_dirty_page_flush", |bencher| {
        bencher.iter(|| {
            tick += 1_000;
            wheel.put_slot(tick, tick, tick, 1, 1).expect("update one timer slot");
            let generation = wheel.flush_generation().expect("flush one dirty page");
            wheel.commit_generation(generation).expect("commit wheel generation");
            black_box(metrics.snapshot().physical_write_bytes)
        });
    });

    let slots = vec![Slot::new_with_num_magic(1_000, 0, 40, 1, 1); 1_209_600];
    let mut legacy_file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(directory.path().join("legacy-timerwheel"))
        .expect("create legacy timer wheel");
    criterion.bench_function("timer_wheel_legacy_full_rewrite", |bencher| {
        bencher.iter(|| {
            let mut bytes = Vec::with_capacity(slots.len() * Slot::SIZE as usize);
            for slot in &slots {
                bytes.extend_from_slice(&slot.time_ms.to_be_bytes());
                bytes.extend_from_slice(&slot.first_pos.to_be_bytes());
                bytes.extend_from_slice(&slot.last_pos.to_be_bytes());
                bytes.extend_from_slice(&slot.num.to_be_bytes());
                bytes.extend_from_slice(&slot.magic.to_be_bytes());
            }
            legacy_file.seek(SeekFrom::Start(0)).expect("rewind legacy timer wheel");
            legacy_file.set_len(0).expect("truncate legacy timer wheel");
            legacy_file.write_all(&bytes).expect("rewrite legacy timer wheel");
            legacy_file.sync_data().expect("flush legacy timer wheel");
            black_box(bytes.len())
        });
    });

    let drain_path = directory.path().join("hot-slot");
    let mut builder = SlotDrainFileBuilder::create(&drain_path, 1_000, 1).expect("create hot-slot spill");
    for index in (0..100_000usize).rev() {
        builder
            .push_reverse(SlotDrainLocator {
                timer_log_position: index as i64 * 40,
                commit_log_offset: index as i64 * 100,
                size: 64,
                magic: 1,
                queue_offset: index as i64,
                generation: 1,
            })
            .expect("append hot-slot locator");
    }
    let drain = builder.finish().expect("finish hot-slot spill");
    criterion.bench_function("timer_hot_slot_100k_linear_scan", |bencher| {
        bencher.iter(|| {
            let mut cursor = 0usize;
            let mut records_read = 0usize;
            while cursor < drain.record_count() {
                let batch = drain.read_batch(cursor, 192).expect("read hot-slot batch");
                cursor += batch.len();
                records_read += batch.len();
            }
            assert_eq!(records_read, drain.record_count());
            black_box(records_read)
        });
    });
}

criterion_group!(benches, timer_storage_benchmark);
criterion_main!(benches);
