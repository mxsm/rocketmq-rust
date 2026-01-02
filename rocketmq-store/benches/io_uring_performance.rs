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

/*
//! Performance benchmarks for IoUringMappedFile vs DefaultMappedFile
//!
//! Run with: cargo bench --bench io_uring_performance --features io_uring
//!
//! Expected improvements with io_uring:
//! - Write throughput: +20-30%
//! - Flush latency: -30-40%
//! - CPU usage: -10-15%

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use std::sync::Arc;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use cheetah_string::CheetahString;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use criterion::criterion_group;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use criterion::criterion_main;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use criterion::BenchmarkId;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use criterion::Criterion;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use criterion::Throughput;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::DefaultMappedFile;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use rocketmq_store::log_file::mapped_file::io_uring_impl::IoUringMappedFile;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use rocketmq_store::log_file::mapped_file::MappedFile;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use tokio::runtime::Runtime;

/// Benchmark: io_uring vs default write performance
#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn bench_write_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_comparison");
    let rt = Runtime::new().unwrap();

    let sizes = [256, 1024, 4096, 16384]; // Different message sizes

    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        // Benchmark io_uring
        group.bench_with_input(BenchmarkId::new("io_uring", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let file = IoUringMappedFile::new(
                        CheetahString::from(format!("/tmp/bench_io_uring_{}", size)),
                        10 * 1024 * 1024, // 10MB
                    )
                    .await
                    .unwrap();

                    let data = vec![0x42; size];
                    for i in 0..100 {
                        let offset = (i * size) % (10 * 1024 * 1024 - size);
                        std::hint::black_box(file.write_async(&data, offset).await.unwrap());
                    }

                    let _ = std::fs::remove_file(format!("/tmp/bench_io_uring_{}", size));
                })
            });
        });

        // Benchmark default
        group.bench_with_input(BenchmarkId::new("default", size), &size, |b, &size| {
            b.iter(|| {
                let file = DefaultMappedFile::new(
                    CheetahString::from(format!("/tmp/bench_default_{}", size)),
                    10 * 1024 * 1024, // 10MB
                    None,
                )
                .unwrap();

                let data = vec![0x42; size];
                for i in 0..100 {
                    let offset = (i * size) % (10 * 1024 * 1024 - size);
                    std::hint::black_box(file.append_message_offset_length(&data, 0, size));
                }

                let _ = std::fs::remove_file(format!("/tmp/bench_default_{}", size));
            });
        });
    }

    group.finish();
}

/// Benchmark: Flush performance comparison
#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn bench_flush_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_comparison");
    let rt = Runtime::new().unwrap();

    // Benchmark io_uring async flush
    group.bench_function("io_uring_flush", |b| {
        b.iter(|| {
            rt.block_on(async {
                let file = IoUringMappedFile::new(
                    CheetahString::from("/tmp/bench_flush_io_uring"),
                    1024 * 1024, // 1MB
                )
                .await
                .unwrap();

                // Write some data
                let data = vec![0xAA; 4096];
                for i in 0..10 {
                    file.write_async(&data, i * data.len()).await.unwrap();
                }
                file.set_wrote_position((10 * data.len()) as i32);

                // Flush
                std::hint::black_box(file.flush_async().await.unwrap());

                let _ = std::fs::remove_file("/tmp/bench_flush_io_uring");
            })
        });
    });

    // Benchmark default sync flush
    group.bench_function("default_flush", |b| {
        b.iter(|| {
            let file = DefaultMappedFile::new(
                CheetahString::from("/tmp/bench_flush_default"),
                1024 * 1024, // 1MB
                None,
            )
            .unwrap();

            // Write some data
            let data = vec![0xAA; 4096];
            for i in 0..10 {
                file.append_message_offset_length(&data, 0, data.len());
            }

            // Flush
            std::hint::black_box(file.flush(0));

            let _ = std::fs::remove_file("/tmp/bench_flush_default");
        });
    });

    group.finish();
}

/// Benchmark: Concurrent write performance
#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");
    let rt = Runtime::new().unwrap();

    let thread_counts = [1, 2, 4, 8];

    for num_threads in thread_counts {
        group.bench_with_input(
            BenchmarkId::new("io_uring", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter(|| {
                    rt.block_on(async move {
                        let file = Arc::new(
                            IoUringMappedFile::new(
                                CheetahString::from(format!(
                                    "/tmp/bench_concurrent_io_uring_{}",
                                    num_threads
                                )),
                                50 * 1024 * 1024, // 50MB
                            )
                            .await
                            .unwrap(),
                        );

                        let mut handles = vec![];
                        for task_id in 0..num_threads {
                            let file_clone = Arc::clone(&file);
                            let handle = tokio::spawn(async move {
                                let data = vec![task_id as u8; 1024];
                                for i in 0..100 {
                                    let offset = (task_id * 100 + i) * data.len();
                                    file_clone.write_async(&data, offset).await.unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }

                        let _ = std::fs::remove_file(format!(
                            "/tmp/bench_concurrent_io_uring_{}",
                            num_threads
                        ));
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Large sequential write throughput
#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn bench_sequential_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_throughput");
    let rt = Runtime::new().unwrap();

    group.throughput(Throughput::Bytes(10 * 1024 * 1024)); // 10MB

    group.bench_function("io_uring_sequential", |b| {
        b.iter(|| {
            rt.block_on(async {
                let file = IoUringMappedFile::new(
                    CheetahString::from("/tmp/bench_seq_io_uring"),
                    20 * 1024 * 1024, // 20MB
                )
                .await
                .unwrap();

                let data = vec![0x55; 1024 * 1024]; // 1MB chunks
                for i in 0..10 {
                    std::hint::black_box(file.write_async(&data, i * data.len()).await.unwrap());
                }

                let _ = std::fs::remove_file("/tmp/bench_seq_io_uring");
            })
        });
    });

    group.bench_function("default_sequential", |b| {
        b.iter(|| {
            let file = DefaultMappedFile::new(
                CheetahString::from("/tmp/bench_seq_default"),
                20 * 1024 * 1024, // 20MB
                None,
            )
            .unwrap();

            let data = vec![0x55; 1024 * 1024]; // 1MB chunks
            for i in 0..10 {
                std::hint::black_box(file.append_message_offset_length(&data, 0, data.len()));
            }

            let _ = std::fs::remove_file("/tmp/bench_seq_default");
        });
    });

    group.finish();
}

/// Benchmark: Read performance
#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn bench_read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_performance");
    let rt = Runtime::new().unwrap();

    // Setup: Create files with data
    rt.block_on(async {
        // io_uring file
        let io_uring_file = IoUringMappedFile::new(
            CheetahString::from("/tmp/bench_read_io_uring"),
            10 * 1024 * 1024, // 10MB
        )
        .await
        .unwrap();
        let data = vec![0x77; 4096];
        for i in 0..(10 * 1024 * 1024 / 4096) {
            io_uring_file.write_async(&data, i * 4096).await.unwrap();
        }

        // default file
        let default_file = DefaultMappedFile::new(
            CheetahString::from("/tmp/bench_read_default"),
            10 * 1024 * 1024, // 10MB
            None,
        )
        .unwrap();
        for i in 0..(10 * 1024 * 1024 / 4096) {
            default_file.append_message_offset_length(&data, 0, data.len());
        }
    });

    group.throughput(Throughput::Bytes(4096 * 100)); // 100 reads of 4KB

    group.bench_function("io_uring_read", |b| {
        b.iter(|| {
            let file = rt.block_on(async {
                IoUringMappedFile::new(
                    CheetahString::from("/tmp/bench_read_io_uring"),
                    10 * 1024 * 1024,
                )
                .await
                .unwrap()
            });

            for i in 0..100 {
                let offset = (i * 4096) % (10 * 1024 * 1024 - 4096);
                std::hint::black_box(file.get_bytes(offset, 4096));
            }
        });
    });

    group.bench_function("default_read", |b| {
        b.iter(|| {
            let file = DefaultMappedFile::new(
                CheetahString::from("/tmp/bench_read_default"),
                10 * 1024 * 1024,
                None,
            )
            .unwrap();

            for i in 0..100 {
                let offset = (i * 4096) % (10 * 1024 * 1024 - 4096);
                std::hint::black_box(file.get_bytes(offset, 4096));
            }
        });
    });

    // Cleanup
    let _ = std::fs::remove_file("/tmp/bench_read_io_uring");
    let _ = std::fs::remove_file("/tmp/bench_read_default");

    group.finish();
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
criterion_group!(
    benches,
    bench_write_comparison,
    bench_flush_comparison,
    bench_concurrent_writes,
    bench_sequential_throughput,
    bench_read_performance
);

#[cfg(all(target_os = "linux", feature = "io_uring"))]
criterion_main!(benches);
*/
fn main() {
    // This benchmark requires Linux with io_uring feature enabled
    println!("io_uring benchmarks require Linux and --features io_uring");
}
