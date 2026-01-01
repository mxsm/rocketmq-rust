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

//! Performance benchmarks for async segmented lock implementation
//!
//! This benchmark compares:
//! - Global Tokio RwLock (single lock for all operations)
//! - Async Segmented Lock (16-way lock striping)
//!
//! Run with: cargo bench --bench async_segmented_lock_bench

use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_namesrv::route::async_segmented_lock::AsyncSegmentedLock;
use tokio::runtime::Runtime;
use tokio::sync::RwLock as TokioRwLock;

/// Benchmark concurrent read operations with global lock
async fn bench_global_lock_reads(concurrency: usize, operations: usize) {
    let lock = Arc::new(TokioRwLock::new(0u64));
    let mut handles = vec![];

    for _ in 0..concurrency {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            for _ in 0..operations {
                let _guard = lock.read().await;
                // Simulate very light work
                tokio::task::yield_now().await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Benchmark concurrent read operations with segmented lock
async fn bench_segmented_lock_reads(concurrency: usize, operations: usize) {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let mut handles = vec![];

    for i in 0..concurrency {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            for j in 0..operations {
                // Use different keys to hit different segments
                let key = format!("key-{}", (i + j) % 100);
                let _guard = lock.read_lock(&key).await;
                // Simulate very light work
                tokio::task::yield_now().await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Benchmark concurrent write operations with global lock
async fn bench_global_lock_writes(concurrency: usize, operations: usize) {
    let lock = Arc::new(TokioRwLock::new(0u64));
    let mut handles = vec![];

    for _ in 0..concurrency {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            for _ in 0..operations {
                let mut _guard = lock.write().await;
                // Simulate very light work
                tokio::task::yield_now().await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Benchmark concurrent write operations with segmented lock
async fn bench_segmented_lock_writes(concurrency: usize, operations: usize) {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let mut handles = vec![];

    for i in 0..concurrency {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            for j in 0..operations {
                // Use different keys to hit different segments
                let key = format!("key-{}", (i + j) % 100);
                let _guard = lock.write_lock(&key).await;
                // Simulate very light work
                tokio::task::yield_now().await;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Benchmark mixed read/write operations with global lock (90% reads, 10% writes)
async fn bench_global_lock_mixed(concurrency: usize, operations: usize) {
    let lock = Arc::new(TokioRwLock::new(0u64));
    let mut handles = vec![];

    for _ in 0..concurrency {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            for i in 0..operations {
                if i % 10 == 0 {
                    let mut _guard = lock.write().await;
                    tokio::task::yield_now().await;
                } else {
                    let _guard = lock.read().await;
                    tokio::task::yield_now().await;
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Benchmark mixed read/write operations with segmented lock (90% reads, 10% writes)
async fn bench_segmented_lock_mixed(concurrency: usize, operations: usize) {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let mut handles = vec![];

    for task_id in 0..concurrency {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            for i in 0..operations {
                let key = format!("key-{}", (task_id + i) % 100);
                if i % 10 == 0 {
                    let _guard = lock.write_lock(&key).await;
                    tokio::task::yield_now().await;
                } else {
                    let _guard = lock.read_lock(&key).await;
                    tokio::task::yield_now().await;
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

fn benchmark_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_concurrent_reads");
    let operations = 100;
    let rt = Runtime::new().unwrap();

    for concurrency in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("global_lock", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        bench_global_lock_reads(concurrency, operations).await;
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("segmented_lock", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        bench_segmented_lock_reads(concurrency, operations).await;
                    })
                });
            },
        );
    }

    group.finish();
}

fn benchmark_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_concurrent_writes");
    let operations = 50;
    let rt = Runtime::new().unwrap();

    for concurrency in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("global_lock", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        bench_global_lock_writes(concurrency, operations).await;
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("segmented_lock", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        bench_segmented_lock_writes(concurrency, operations).await;
                    })
                });
            },
        );
    }

    group.finish();
}

fn benchmark_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_mixed_workload");
    let operations = 100;
    let rt = Runtime::new().unwrap();

    for concurrency in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("global_lock", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        bench_global_lock_mixed(concurrency, operations).await;
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("segmented_lock", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter(|| {
                    rt.block_on(async {
                        bench_segmented_lock_mixed(concurrency, operations).await;
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_concurrent_reads,
    benchmark_concurrent_writes,
    benchmark_mixed_workload
);
criterion_main!(benches);
