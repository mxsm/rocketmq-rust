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
//! This benchmark tests the performance improvements from:
//! - P0/P1: tokio::Mutex + Notify (vs parking_lot::Mutex + polling)
//! - P2: DashMap (vs Arc<Mutex<HashMap>>)
//!
//! Run with: cargo bench --bench produce_accumulator_benchmark

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use criterion::black_box;
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

/// Benchmark: DashMap (After P2 optimization)
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
    bench_mixed_operations,
    bench_multi_topic_scenario,
    bench_high_contention
);

criterion_main!(benches);
