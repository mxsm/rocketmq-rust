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

//! ProduceAccumulator Performance Benchmark
//!
//!
//! Run with: cargo bench --bench produce_accumulator_benchmark

use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use dashmap::DashMap;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

// Simulated key structure (matching AggregateKey)
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct TestKey {
    topic: String,
    queue_id: i32,
}

impl TestKey {
    fn new(topic: &str, queue_id: i32) -> Self {
        Self {
            topic: topic.to_string(),
            queue_id,
        }
    }
}

// Simulated batch value
#[derive(Clone)]
struct TestBatch {
    messages: Vec<String>,
    size: usize,
}

impl TestBatch {
    fn new() -> Self {
        Self {
            messages: Vec::new(),
            size: 0,
        }
    }

    fn add_message(&mut self, msg: String) {
        self.size += msg.len();
        self.messages.push(msg);
    }
}

/// Benchmark: HashMap with Mutex (Before optimization)
async fn bench_hashmap_mutex_concurrent_insert(num_keys: usize, operations_per_key: usize) -> Duration {
    let map: Arc<Mutex<HashMap<TestKey, Arc<Mutex<TestBatch>>>>> = Arc::new(Mutex::new(HashMap::new()));

    let start = std::time::Instant::now();

    let mut tasks = Vec::new();

    // Spawn concurrent tasks
    for key_id in 0..num_keys {
        let map = map.clone();
        let task = tokio::spawn(async move {
            let key = TestKey::new("TestTopic", key_id as i32);

            for i in 0..operations_per_key {
                // Get or create batch (requires locking entire HashMap)
                let batch = {
                    let mut map_guard = map.lock().await;
                    map_guard
                        .entry(key.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(TestBatch::new())))
                        .clone()
                };

                // Add message to batch
                let mut batch_guard = batch.lock().await;
                batch_guard.add_message(format!("Message-{}-{}", key_id, i));
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks
    for task in tasks {
        let _ = task.await;
    }

    start.elapsed()
}

/// Benchmark: DashMap
async fn bench_dashmap_concurrent_insert(num_keys: usize, operations_per_key: usize) -> Duration {
    let map: Arc<DashMap<TestKey, Arc<Mutex<TestBatch>>>> = Arc::new(DashMap::new());

    let start = std::time::Instant::now();

    let mut tasks = Vec::new();

    // Spawn concurrent tasks
    for key_id in 0..num_keys {
        let map = map.clone();
        let task = tokio::spawn(async move {
            let key = TestKey::new("TestTopic", key_id as i32);

            for i in 0..operations_per_key {
                // Get or create batch (no global lock, only shard lock)
                let batch = map
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(TestBatch::new())))
                    .clone();

                // Add message to batch
                let mut batch_guard = batch.lock().await;
                batch_guard.add_message(format!("Message-{}-{}", key_id, i));
            }
        });
        tasks.push(task);
    }

    // Wait for all tasks
    for task in tasks {
        let _ = task.await;
    }

    start.elapsed()
}

fn reserve_with_lock(counter: &AtomicU64, lock: &StdMutex<()>, body_size: u64, limit: u64) -> bool {
    let _guard = lock.lock().expect("benchmark reservation lock should not be poisoned");
    let current = counter.load(Ordering::Acquire);
    let Some(next) = current.checked_add(body_size) else {
        return false;
    };
    if next > limit {
        return false;
    }
    counter.store(next, Ordering::Release);
    true
}

fn reserve_with_try_update(counter: &AtomicU64, body_size: u64, limit: u64) -> bool {
    counter
        .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            let next = current.checked_add(body_size)?;
            (next <= limit).then_some(next)
        })
        .is_ok()
}

async fn bench_capacity_reservation(
    num_keys: usize,
    operations_per_key: usize,
    workers_per_key: usize,
    use_try_update: bool,
) -> Duration {
    let counter = Arc::new(AtomicU64::new(0));
    let lock = Arc::new(StdMutex::new(()));
    let total_operations = num_keys * operations_per_key;
    let limit = total_operations as u64 + 1;
    let operations_per_worker = operations_per_key / workers_per_key.max(1);
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();

    for _key_id in 0..num_keys {
        for _ in 0..workers_per_key {
            let counter = counter.clone();
            let lock = lock.clone();
            let task = tokio::spawn(async move {
                let mut accepted = 0usize;
                for _ in 0..operations_per_worker {
                    let reserved = if use_try_update {
                        reserve_with_try_update(&counter, 1, limit)
                    } else {
                        reserve_with_lock(&counter, &lock, 1, limit)
                    };
                    accepted += usize::from(reserved);
                }
                black_box(accepted);
            });
            tasks.push(task);
        }
    }

    for task in tasks {
        let _ = task.await;
    }

    start.elapsed()
}

#[derive(Clone, Eq, PartialEq)]
struct BenchDeadline {
    deadline_tick: usize,
    sequence: usize,
}

impl Ord for BenchDeadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .deadline_tick
            .cmp(&self.deadline_tick)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for BenchDeadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn bench_guard_full_scan_low_activity(num_keys: usize, ticks: usize) -> usize {
    let mut deadlines = vec![ticks + 1; num_keys];
    deadlines[0] = ticks / 2;
    let mut active = vec![true; num_keys];
    let mut processed = 0usize;

    for tick in 0..ticks {
        for index in 0..num_keys {
            if active[index] && deadlines[index] <= tick {
                active[index] = false;
                processed += 1;
            }
        }
    }

    black_box(processed)
}

fn bench_guard_deadline_heap_low_activity(num_keys: usize, ticks: usize) -> usize {
    let mut heap = BinaryHeap::new();
    for sequence in 0..num_keys {
        let deadline_tick = if sequence == 0 { ticks / 2 } else { ticks + 1 };
        heap.push(BenchDeadline {
            deadline_tick,
            sequence,
        });
    }

    let mut processed = 0usize;
    for tick in 0..ticks {
        while heap.peek().is_some_and(|deadline| deadline.deadline_tick <= tick) {
            heap.pop();
            processed += 1;
        }
    }

    black_box(processed)
}

/// Benchmark: Concurrent reads and writes with HashMap
async fn bench_hashmap_mutex_mixed_ops(num_keys: usize, read_ops: usize, write_ops: usize) -> Duration {
    let map: Arc<Mutex<HashMap<TestKey, Arc<Mutex<TestBatch>>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Pre-populate
    {
        let mut map_guard = map.lock().await;
        for i in 0..num_keys {
            let key = TestKey::new("TestTopic", i as i32);
            map_guard.insert(key, Arc::new(Mutex::new(TestBatch::new())));
        }
    }

    let start = std::time::Instant::now();

    let mut tasks = Vec::new();

    // Spawn reader tasks
    for _ in 0..read_ops {
        let map = map.clone();
        let task = tokio::spawn(async move {
            for i in 0..num_keys {
                let key = TestKey::new("TestTopic", i as i32);
                let map_guard = map.lock().await;
                if let Some(batch) = map_guard.get(&key) {
                    let batch_guard = batch.lock().await;
                    black_box(batch_guard.size);
                }
            }
        });
        tasks.push(task);
    }

    // Spawn writer tasks
    for _ in 0..write_ops {
        let map = map.clone();
        let task = tokio::spawn(async move {
            for i in 0..num_keys {
                let key = TestKey::new("TestTopic", i as i32);
                let batch = {
                    let map_guard = map.lock().await;
                    map_guard.get(&key).cloned()
                };
                if let Some(batch) = batch {
                    let mut batch_guard = batch.lock().await;
                    batch_guard.add_message("test".to_string());
                }
            }
        });
        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    start.elapsed()
}

/// Benchmark: Concurrent reads and writes with DashMap
async fn bench_dashmap_mixed_ops(num_keys: usize, read_ops: usize, write_ops: usize) -> Duration {
    let map: Arc<DashMap<TestKey, Arc<Mutex<TestBatch>>>> = Arc::new(DashMap::new());

    // Pre-populate
    for i in 0..num_keys {
        let key = TestKey::new("TestTopic", i as i32);
        map.insert(key, Arc::new(Mutex::new(TestBatch::new())));
    }

    let start = std::time::Instant::now();

    let mut tasks = Vec::new();

    // Spawn reader tasks
    for _ in 0..read_ops {
        let map = map.clone();
        let task = tokio::spawn(async move {
            for i in 0..num_keys {
                let key = TestKey::new("TestTopic", i as i32);
                if let Some(batch) = map.get(&key) {
                    let batch_guard = batch.lock().await;
                    black_box(batch_guard.size);
                }
            }
        });
        tasks.push(task);
    }

    // Spawn writer tasks
    for _ in 0..write_ops {
        let map = map.clone();
        let task = tokio::spawn(async move {
            for i in 0..num_keys {
                let key = TestKey::new("TestTopic", i as i32);
                if let Some(batch) = map.get(&key) {
                    let mut batch_guard = batch.lock().await;
                    batch_guard.add_message("test".to_string());
                }
            }
        });
        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    start.elapsed()
}

/// Benchmark Group: Concurrent Insert Performance
fn bench_concurrent_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_insert");

    // Test different key counts (simulating different topic/queue combinations)
    for num_keys in [1, 4, 16, 64].iter() {
        let operations_per_key = 100;
        let total_ops = num_keys * operations_per_key;

        group.throughput(Throughput::Elements(total_ops as u64));

        // Benchmark HashMap with Mutex
        group.bench_with_input(BenchmarkId::new("HashMap_Mutex", num_keys), num_keys, |b, &num_keys| {
            b.to_async(&rt)
                .iter(|| async move { bench_hashmap_mutex_concurrent_insert(num_keys, operations_per_key).await });
        });

        // Benchmark DashMap
        group.bench_with_input(BenchmarkId::new("DashMap", num_keys), num_keys, |b, &num_keys| {
            b.to_async(&rt)
                .iter(|| async move { bench_dashmap_concurrent_insert(num_keys, operations_per_key).await });
        });
    }

    group.finish();
}

fn bench_capacity_reservation_control(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("capacity_reservation");

    for num_keys in [1usize, 4, 16, 64] {
        let operations_per_key = 256;
        let workers_per_key = if num_keys == 1 { 64 } else { 4 };
        group.throughput(Throughput::Elements((num_keys * operations_per_key) as u64));

        for (label, use_try_update) in [("Mutex_Lock", false), ("Atomic_TryUpdate", true)] {
            group.bench_with_input(BenchmarkId::new(label, num_keys), &num_keys, |b, &num_keys| {
                b.to_async(&rt).iter(|| async move {
                    bench_capacity_reservation(num_keys, operations_per_key, workers_per_key, use_try_update).await
                });
            });
        }
    }

    group.throughput(Throughput::Elements(40 * 50));
    for (label, use_try_update) in [("Mutex_Lock_40x50", false), ("Atomic_TryUpdate_40x50", true)] {
        group.bench_function(label, |b| {
            b.to_async(&rt)
                .iter(|| async move { bench_capacity_reservation(40, 50, 2, use_try_update).await });
        });
    }

    group.finish();
}

fn bench_guard_deadline_scheduler(c: &mut Criterion) {
    let mut group = c.benchmark_group("guard_deadline_scheduler");
    let num_keys = 64;
    let ticks = 1024;

    group.throughput(Throughput::Elements((num_keys * ticks) as u64));
    group.bench_function("FullScan_64keys_low_activity", |b| {
        b.iter(|| bench_guard_full_scan_low_activity(num_keys, ticks));
    });
    group.bench_function("DeadlineHeap_64keys_low_activity", |b| {
        b.iter(|| bench_guard_deadline_heap_low_activity(num_keys, ticks));
    });

    group.finish();
}

/// Benchmark Group: Mixed Read/Write Operations
fn bench_mixed_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("mixed_operations");

    let num_keys = 16;
    let read_ops = 8;
    let write_ops = 8;

    // Benchmark HashMap with Mutex
    group.bench_function("HashMap_Mutex", |b| {
        b.to_async(&rt)
            .iter(|| async { bench_hashmap_mutex_mixed_ops(num_keys, read_ops, write_ops).await });
    });

    // Benchmark DashMap
    group.bench_function("DashMap", |b| {
        b.to_async(&rt)
            .iter(|| async { bench_dashmap_mixed_ops(num_keys, read_ops, write_ops).await });
    });

    group.finish();
}

/// Benchmark Group: Multi-Topic Scenario
fn bench_multi_topic_scenario(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("multi_topic_scenario");

    // Simulate real-world scenario: 10 topics, 4 queues each = 40 keys
    let num_topics = 10;
    let queues_per_topic = 4;
    let num_keys = num_topics * queues_per_topic;
    let messages_per_key = 50;

    group.throughput(Throughput::Elements((num_keys * messages_per_key) as u64));

    // Benchmark HashMap with Mutex
    group.bench_function("HashMap_Mutex", |b| {
        b.to_async(&rt)
            .iter(|| async { bench_hashmap_mutex_concurrent_insert(num_keys, messages_per_key).await });
    });

    // Benchmark DashMap
    group.bench_function("DashMap", |b| {
        b.to_async(&rt)
            .iter(|| async { bench_dashmap_concurrent_insert(num_keys, messages_per_key).await });
    });

    group.finish();
}

/// Benchmark Group: High Contention Scenario (Single Key)
fn bench_high_contention(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("high_contention");

    // Worst case: All threads competing for the same key
    let num_keys = 1;
    let operations_per_key = 1000;

    group.throughput(Throughput::Elements(operations_per_key as u64));

    // Benchmark HashMap with Mutex
    group.bench_function("HashMap_Mutex_1key", |b| {
        b.to_async(&rt)
            .iter(|| async { bench_hashmap_mutex_concurrent_insert(num_keys, operations_per_key).await });
    });

    // Benchmark DashMap
    group.bench_function("DashMap_1key", |b| {
        b.to_async(&rt)
            .iter(|| async { bench_dashmap_concurrent_insert(num_keys, operations_per_key).await });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_insert,
    bench_capacity_reservation_control,
    bench_guard_deadline_scheduler,
    bench_mixed_operations,
    bench_multi_topic_scenario,
    bench_high_contention
);

criterion_main!(benches);
