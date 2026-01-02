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

//! Performance benchmarks for ScheduleMessageService
//!
//! These benchmarks measure the performance improvements from Phase 4 optimizations:
//! - RwLock vs Mutex for concurrent access
//! - Batch processing efficiency
//! - Memory allocation overhead
//! - Queue capacity pre-allocation

use std::collections::VecDeque;
use std::hint::black_box;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

// Benchmark constants
const SMALL_CAPACITY: usize = 10;
const MEDIUM_CAPACITY: usize = 128;
const LARGE_CAPACITY: usize = 1000;

/// Benchmark VecDeque with and without capacity pre-allocation
fn bench_vecdeque_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("vecdeque_allocation");

    for size in [SMALL_CAPACITY, MEDIUM_CAPACITY, LARGE_CAPACITY].iter() {
        group.bench_with_input(BenchmarkId::new("without_capacity", size), size, |b, &size| {
            b.iter(|| {
                let mut queue: VecDeque<i64> = VecDeque::new();
                for i in 0..size {
                    queue.push_back(black_box(i as i64));
                }
                queue
            });
        });

        group.bench_with_input(BenchmarkId::new("with_capacity", size), size, |b, &size| {
            b.iter(|| {
                let mut queue: VecDeque<i64> = VecDeque::with_capacity(size);
                for i in 0..size {
                    queue.push_back(black_box(i as i64));
                }
                queue
            });
        });
    }

    group.finish();
}

/// Benchmark Mutex vs RwLock for read-heavy workloads
fn bench_mutex_vs_rwlock_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("lock_read_performance");

    // Setup: Create shared data structures
    let mutex_data = Arc::new(Mutex::new(vec![1, 2, 3, 4, 5]));
    let rwlock_data = Arc::new(RwLock::new(vec![1, 2, 3, 4, 5]));

    group.bench_function("mutex_sequential_read", |b| {
        b.iter(|| {
            rt.block_on(async {
                let data = mutex_data.lock().await;
                black_box(data.len())
            })
        });
    });

    group.bench_function("rwlock_sequential_read", |b| {
        b.iter(|| {
            rt.block_on(async {
                let data = rwlock_data.read().await;
                black_box(data.len())
            })
        });
    });

    group.finish();
}

/// Benchmark Mutex vs RwLock for write-heavy workloads
fn bench_mutex_vs_rwlock_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("lock_write_performance");

    let mutex_data = Arc::new(Mutex::new(Vec::new()));
    let rwlock_data = Arc::new(RwLock::new(Vec::new()));

    group.bench_function("mutex_sequential_write", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut data = mutex_data.lock().await;
                data.push(black_box(42));
            })
        });
    });

    group.bench_function("rwlock_sequential_write", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut data = rwlock_data.write().await;
                data.push(black_box(42));
            })
        });
    });

    group.finish();
}

/// Benchmark batch processing vs single item processing
fn bench_batch_vs_single_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_processing");

    let items: Vec<i64> = (0..1000).collect();

    group.bench_function("single_item_processing", |b| {
        b.iter(|| {
            let mut result = 0i64;
            for &item in &items {
                result += process_single_item(black_box(item));
            }
            result
        });
    });

    for batch_size in [10, 32, 64, 128].iter() {
        group.bench_with_input(
            BenchmarkId::new("batch_processing", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let mut result = 0i64;
                    for chunk in items.chunks(batch_size) {
                        result += process_batch(black_box(chunk));
                    }
                    result
                });
            },
        );
    }

    group.finish();
}

/// Benchmark AtomicI64 operations (used for offset_table)
fn bench_atomic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_operations");

    let atomic_value = Arc::new(AtomicI64::new(0));

    group.bench_function("atomic_load_relaxed", |b| {
        b.iter(|| black_box(atomic_value.load(Ordering::Relaxed)));
    });

    group.bench_function("atomic_store_relaxed", |b| {
        b.iter(|| atomic_value.store(black_box(42), Ordering::Relaxed));
    });

    group.bench_function("atomic_fetch_add", |b| {
        b.iter(|| atomic_value.fetch_add(black_box(1), Ordering::Relaxed));
    });

    group.finish();
}

/// Simulate processing a single item
#[inline]
fn process_single_item(item: i64) -> i64 {
    item * 2 + 1
}

/// Simulate processing a batch of items
#[inline]
fn process_batch(items: &[i64]) -> i64 {
    items.iter().map(|&i| i * 2 + 1).sum()
}

criterion_group!(
    benches,
    bench_vecdeque_allocation,
    bench_mutex_vs_rwlock_read,
    bench_mutex_vs_rwlock_write,
    bench_batch_vs_single_processing,
    bench_atomic_operations,
);

criterion_main!(benches);
