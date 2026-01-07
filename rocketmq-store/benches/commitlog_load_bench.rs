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

//! Benchmark: CommitLog load performance (optimized vs. sequential)
//!
//! This benchmark measures the performance difference between:
//! 1. Sequential loading (original implementation)
//! 2. Optimized parallel loading (new implementation)
//!
//! ## Running Benchmarks
//! ```bash
//! # Run all benchmarks
//! cargo bench --bench commitlog_load_bench
//!
//! # Run specific benchmark
//! cargo bench --bench commitlog_load_bench -- "load/sequential"
//!
//! # Generate HTML reports
//! cargo bench --bench commitlog_load_bench -- --output-format html
//! ```
//!
//! ## Scenarios
//! - **Small dataset**: 5 files × 128 MB (640 MB total)
//! - **Medium dataset**: 20 files × 128 MB (2.5 GB total)
//! - **Large dataset**: 50 files × 128 MB (6.4 GB total)
//! - **Realistic**: 100 files × 1 GB (100 GB total) - run manually

use std::fs;
use std::hint::black_box;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store::consume_queue::mapped_file_queue::MappedFileQueue;
use rocketmq_store::log_file::mapped_file::MappedFile;
use tempfile::TempDir;

/// Create a temporary directory with N commit log files of given size
fn create_test_commitlog_files(dir: &TempDir, num_files: usize, file_size: u64) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::with_capacity(num_files);

    for i in 0..num_files {
        let offset = i as u64 * file_size;
        let file_path = dir.path().join(format!("{:020}", offset));

        // Create file with pseudo-random data (to prevent OS compression)
        let mut data = vec![0u8; file_size as usize];
        for (idx, byte) in data.iter_mut().enumerate() {
            *byte = ((idx + i * 1024) % 256) as u8;
        }

        fs::write(&file_path, data).expect("Failed to write test file");
        paths.push(file_path);
    }

    paths
}

/// Benchmark sequential loading (original MappedFileQueue::load)
fn bench_sequential_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("commitlog_load/sequential");

    // Test configurations: (num_files, file_size_mb, description)
    let configs = vec![
        (5, 128, "small_5x128MB"),
        (20, 128, "medium_20x128MB"),
        (50, 128, "large_50x128MB"),
    ];

    for (num_files, size_mb, desc) in configs {
        let file_size = size_mb * 1024 * 1024;
        let total_size = num_files * file_size;

        group.throughput(Throughput::Bytes(total_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(desc), &num_files, |b, &n| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    create_test_commitlog_files(&temp_dir, n, file_size as u64);
                    let queue =
                        MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size as u64, None);
                    (temp_dir, queue)
                },
                |(temp_dir, mut queue)| {
                    let result = black_box(queue.load());
                    assert!(result, "Sequential load failed");
                    drop(temp_dir); // Cleanup
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// Benchmark optimized parallel loading (new CommitLogLoader)
fn bench_optimized_load(c: &mut Criterion) {
    // Note: CommitLogLoader is module-private, so we benchmark via the public API
    // using environment variable to control behavior

    let mut group = c.benchmark_group("commitlog_load/optimized_parallel");

    let configs = vec![
        (5, 128, "small_5x128MB"),
        (20, 128, "medium_20x128MB"),
        (50, 128, "large_50x128MB"),
    ];

    for (num_files, size_mb, desc) in configs {
        let file_size = size_mb * 1024 * 1024;
        let total_size = num_files * file_size;

        group.throughput(Throughput::Bytes(total_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(desc), &num_files, |b, &n| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    create_test_commitlog_files(&temp_dir, n, file_size as u64);
                    let queue =
                        MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size as u64, None);
                    (temp_dir, queue)
                },
                |(temp_dir, mut queue)| {
                    // Force optimized path by unsetting ROCKETMQ_SAFE_LOAD
                    std::env::remove_var("ROCKETMQ_SAFE_LOAD");
                    let result = black_box(queue.load());
                    assert!(result, "Optimized load failed");
                    drop(temp_dir); // Cleanup
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// Benchmark comparison: sequential vs. optimized side-by-side
fn bench_load_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("commitlog_load/comparison");

    let num_files = 20;
    let file_size = 128 * 1024 * 1024u64;
    let total_size = num_files * file_size as usize;

    group.throughput(Throughput::Bytes(total_size as u64));

    // Sequential
    group.bench_function("sequential_20x128MB", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                create_test_commitlog_files(&temp_dir, num_files, file_size);
                let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
                (temp_dir, queue)
            },
            |(temp_dir, mut queue)| {
                std::env::set_var("ROCKETMQ_SAFE_LOAD", "1");
                black_box(queue.load());
                drop(temp_dir);
            },
            criterion::BatchSize::LargeInput,
        );
    });

    // Optimized
    group.bench_function("optimized_20x128MB", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                create_test_commitlog_files(&temp_dir, num_files, file_size);
                let queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
                (temp_dir, queue)
            },
            |(temp_dir, mut queue)| {
                std::env::remove_var("ROCKETMQ_SAFE_LOAD");
                let result = black_box(queue.load());
                assert!(result);
                drop(temp_dir);
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// Benchmark memory access patterns after load
fn bench_post_load_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("commitlog_load/memory_access");

    let num_files = 10;
    let file_size = 128 * 1024 * 1024u64;

    // Setup: Load files once
    let temp_dir = TempDir::new().unwrap();
    create_test_commitlog_files(&temp_dir, num_files, file_size);

    let mut queue = MappedFileQueue::new(temp_dir.path().to_string_lossy().to_string(), file_size, None);
    queue.load();
    let files = queue.get_mapped_files();
    let files_guard = files.load();

    // Benchmark: Sequential reads from first file
    group.bench_function("sequential_read_4KB_blocks", |b| {
        b.iter(|| {
            let file = &files_guard[0];
            let mut sum = 0u64;
            let block_size = 4096;
            let num_blocks = (file_size / block_size as u64) as usize;

            for i in 0..num_blocks.min(1000) {
                // Read first 1000 blocks
                if let Some(bytes) = file.get_bytes(i * block_size, block_size) {
                    sum += bytes.len() as u64;
                }
            }
            black_box(sum)
        });
    });

    drop(files_guard);
    group.finish();
    drop(temp_dir);
}

criterion_group!(
    benches,
    bench_sequential_load,
    bench_optimized_load,
    bench_load_comparison,
    bench_post_load_access
);
criterion_main!(benches);
