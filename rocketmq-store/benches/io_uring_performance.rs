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

//! Flush benchmark entry point for the experimental `io_uring` storage path.
//!
//! Run with:
//! `cargo bench -p rocketmq-store --bench io_uring_performance --features io_uring`

use std::hint::black_box;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store::bench_support::io_uring_flush_benchmark_cases;
use rocketmq_store::bench_support::IoUringFlushBenchmarkPath;
use rocketmq_store::bench_support::IoUringFlushBenchmarkWorkload;
use rocketmq_store::bench_support::IO_URING_FLUSH_BENCHMARK_GROUP;
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
use rocketmq_store::log_file::mapped_file::io_uring_impl::probe_io_uring_runtime_capability;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tempfile::TempDir;

fn bench_flush_semantics(c: &mut Criterion) {
    let capability = probe_io_uring_runtime_capability();
    let mut group = c.benchmark_group(IO_URING_FLUSH_BENCHMARK_GROUP);
    group.sample_size(10);

    for case in io_uring_flush_benchmark_cases() {
        group.throughput(Throughput::Bytes(case.workload.total_bytes() as u64));
        match case.path {
            IoUringFlushBenchmarkPath::DefaultMappedFileBaseline => {
                group.bench_with_input(
                    BenchmarkId::new(case.path.as_str(), case.name),
                    &case.workload,
                    |b, &workload| {
                        b.iter_batched(
                            || prepare_default_flush_workload(workload),
                            |(_temp_dir, file, data)| {
                                let flushed_position = run_default_flush_workload(&file, &data, workload);
                                black_box(flushed_position);
                            },
                            BatchSize::SmallInput,
                        );
                    },
                );
            }
            IoUringFlushBenchmarkPath::IoUringExperimental => {
                if capability.basic_path_available() {
                    eprintln!(
                        "skipping {}: concrete io_uring flush backend is not wired yet",
                        case.name
                    );
                } else {
                    eprintln!(
                        "skipping {}: io_uring runtime capability fallback={:?}",
                        case.name, capability.fallback_reason
                    );
                }
            }
        }
    }

    group.finish();
}

fn prepare_default_flush_workload(workload: IoUringFlushBenchmarkWorkload) -> (TempDir, DefaultMappedFile, Vec<u8>) {
    let temp_dir = TempDir::new().expect("create io_uring flush benchmark directory");
    let file_path = temp_dir.path().join("00000000000000000000");
    let file = DefaultMappedFile::new(
        CheetahString::from_string(file_path.to_string_lossy().into_owned()),
        workload.file_size as u64,
    );
    let data = vec![0xAA; workload.message_size];

    (temp_dir, file, data)
}

fn run_default_flush_workload(file: &DefaultMappedFile, data: &[u8], workload: IoUringFlushBenchmarkWorkload) -> i32 {
    for _ in 0..workload.message_count {
        assert!(file.append_message_offset_length(data, 0, data.len()));
    }
    file.flush(workload.flush_least_pages)
}

criterion_group!(benches, bench_flush_semantics);
criterion_main!(benches);
