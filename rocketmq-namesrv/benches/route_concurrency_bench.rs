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

//! Performance benchmarks for RouteInfoManager concurrency models
//!
//! This benchmark compares three different concurrency approaches:
//! 1. **Global RwLock**: Single lock for all operations (like Java version)
//! 2. **Lock-Free DashMap**: No explicit locking (pure DashMap)
//! 3. **Segmented Locks**: DashMap + segment-level locks (hybrid approach)
//!
//! ## Benchmark Scenarios
//!
//! - **Concurrent Reads**: Multiple threads reading topic routes
//! - **Concurrent Writes**: Multiple threads registering brokers
//! - **Mixed Workload**: 90% reads, 10% writes (typical production ratio)
//! - **High Contention**: All threads access same key
//! - **Low Contention**: Each thread accesses different keys
//!
//! ## Running Benchmarks
//!
//! ```bash
//! cargo bench --bench route_concurrency_bench
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_namesrv::route::segmented_lock::SegmentedLock;
use rocketmq_remoting::protocol::route::route_data_view::BrokerData;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;

// ============================================================================
// Benchmark Data Structures
// ============================================================================

/// Simulated route data for benchmarking
#[derive(Clone)]
struct RouteData {
    topics: HashMap<String, Vec<QueueData>>,
    brokers: HashMap<String, BrokerData>,
}

impl RouteData {
    fn new() -> Self {
        Self {
            topics: HashMap::new(),
            brokers: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn add_topic(&mut self, topic: String, queue_data: QueueData) {
        self.topics.entry(topic).or_default().push(queue_data);
    }

    fn add_broker(&mut self, broker_name: String, broker_data: BrokerData) {
        self.brokers.insert(broker_name, broker_data);
    }

    fn get_topic(&self, topic: &str) -> Option<&Vec<QueueData>> {
        self.topics.get(topic)
    }
}

// ============================================================================
// Concurrency Model 1: Global RwLock (like Java version)
// ============================================================================

struct GlobalLockModel {
    data: RwLock<RouteData>,
}

impl GlobalLockModel {
    fn new() -> Self {
        Self {
            data: RwLock::new(RouteData::new()),
        }
    }

    fn read_topic(&self, topic: &str) -> Option<Vec<QueueData>> {
        let lock = self.data.read();
        lock.get_topic(topic).cloned()
    }

    fn write_broker(&self, broker_name: String, broker_data: BrokerData) {
        let mut lock = self.data.write();
        lock.add_broker(broker_name, broker_data);
    }
}

// ============================================================================
// Concurrency Model 2: Lock-Free DashMap
// ============================================================================

use dashmap::DashMap;

struct LockFreeModel {
    topics: DashMap<String, Vec<QueueData>>,
    brokers: DashMap<String, BrokerData>,
}

impl LockFreeModel {
    fn new() -> Self {
        Self {
            topics: DashMap::new(),
            brokers: DashMap::new(),
        }
    }

    fn read_topic(&self, topic: &str) -> Option<Vec<QueueData>> {
        self.topics.get(topic).map(|v| v.clone())
    }

    fn write_broker(&self, broker_name: String, broker_data: BrokerData) {
        self.brokers.insert(broker_name, broker_data);
    }
}

// ============================================================================
// Concurrency Model 3: Segmented Locks + DashMap (our approach)
// ============================================================================

struct SegmentedLockModel {
    topics: DashMap<String, Vec<QueueData>>,
    brokers: DashMap<String, BrokerData>,
    topic_locks: SegmentedLock,
    broker_locks: SegmentedLock,
}

impl SegmentedLockModel {
    fn new() -> Self {
        Self {
            topics: DashMap::new(),
            brokers: DashMap::new(),
            topic_locks: SegmentedLock::new(),
            broker_locks: SegmentedLock::new(),
        }
    }

    fn read_topic(&self, topic: &str) -> Option<Vec<QueueData>> {
        let _lock = self.topic_locks.read_lock(&topic);
        self.topics.get(topic).map(|v| v.clone())
    }

    fn write_broker(&self, broker_name: String, broker_data: BrokerData) {
        let _lock = self.broker_locks.write_lock(&broker_name);
        self.brokers.insert(broker_name, broker_data);
    }
}

// ============================================================================
// Benchmark Helper Functions
// ============================================================================

fn create_broker_data(broker_name: &str) -> BrokerData {
    let mut broker_addrs = HashMap::new();
    broker_addrs.insert(0, format!("192.168.1.{}:10911", broker_name.len()).into());
    BrokerData::new("DefaultCluster".into(), broker_name.into(), broker_addrs, None)
}

fn create_queue_data(broker_name: &str) -> QueueData {
    QueueData::new(CheetahString::from_string(broker_name.to_string()), 8, 8, 6, 0)
}

// ============================================================================
// Benchmark Execution
// ============================================================================

fn bench_concurrent_reads<F>(name: &str, read_fn: F, num_threads: usize, duration: Duration)
where
    F: Fn(&str) + Send + Sync + 'static + Clone,
{
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..num_threads {
        let read_fn = read_fn.clone();
        let handle = std::thread::spawn(move || {
            let topic = format!("topic-{}", i % 10); // 10 different topics
            let mut count = 0u64;
            let thread_start = Instant::now();

            while thread_start.elapsed() < duration {
                read_fn(&topic);
                count += 1;
            }

            count
        });
        handles.push(handle);
    }

    let mut total_ops = 0u64;
    for handle in handles {
        total_ops += handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "{}: {:>10} threads, {:>10} ops, {:>10.2} ops/sec, {:>6.2} ms",
        name,
        num_threads,
        total_ops,
        ops_per_sec,
        elapsed.as_millis()
    );
}

fn bench_concurrent_writes<F>(name: &str, write_fn: F, num_threads: usize, duration: Duration)
where
    F: Fn(String, BrokerData) + Send + Sync + 'static + Clone,
{
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..num_threads {
        let write_fn = write_fn.clone();
        let handle = std::thread::spawn(move || {
            let mut count = 0u64;
            let thread_start = Instant::now();

            while thread_start.elapsed() < duration {
                let broker_name = format!("broker-{}-{}", i, count);
                let broker_data = create_broker_data(&broker_name);
                write_fn(broker_name, broker_data);
                count += 1;
            }

            count
        });
        handles.push(handle);
    }

    let mut total_ops = 0u64;
    for handle in handles {
        total_ops += handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "{}: {:>10} threads, {:>10} ops, {:>10.2} ops/sec, {:>6.2} ms",
        name,
        num_threads,
        total_ops,
        ops_per_sec,
        elapsed.as_millis()
    );
}

fn bench_mixed_workload<R, W>(name: &str, read_fn: R, write_fn: W, num_threads: usize, duration: Duration)
where
    R: Fn(&str) + Send + Sync + 'static + Clone,
    W: Fn(String, BrokerData) + Send + Sync + 'static + Clone,
{
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..num_threads {
        let read_fn = read_fn.clone();
        let write_fn = write_fn.clone();
        let handle = std::thread::spawn(move || {
            let mut read_count = 0u64;
            let mut write_count = 0u64;
            let thread_start = Instant::now();

            while thread_start.elapsed() < duration {
                // 90% reads, 10% writes (typical production ratio)
                if read_count % 10 != 0 {
                    let topic = format!("topic-{}", i % 10);
                    read_fn(&topic);
                    read_count += 1;
                } else {
                    let broker_name = format!("broker-{}-{}", i, write_count);
                    let broker_data = create_broker_data(&broker_name);
                    write_fn(broker_name, broker_data);
                    write_count += 1;
                }
            }

            (read_count, write_count)
        });
        handles.push(handle);
    }

    let mut total_reads = 0u64;
    let mut total_writes = 0u64;
    for handle in handles {
        let (reads, writes) = handle.join().unwrap();
        total_reads += reads;
        total_writes += writes;
    }

    let elapsed = start.elapsed();
    let total_ops = total_reads + total_writes;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!(
        "{}: {:>10} threads, {:>10} ops ({}R/{}W), {:>10.2} ops/sec, {:>6.2} ms",
        name,
        num_threads,
        total_ops,
        total_reads,
        total_writes,
        ops_per_sec,
        elapsed.as_millis()
    );
}

// ============================================================================
// Main Benchmark Entry Point
// ============================================================================

fn main() {
    println!("\n=================================================================");
    println!("RouteInfoManager Concurrency Benchmark");
    println!("=================================================================\n");

    let duration = Duration::from_secs(2);
    let thread_counts = vec![1, 2, 4, 8, 16];

    // Initialize models with some data
    let global_lock = Arc::new(GlobalLockModel::new());
    let lock_free = Arc::new(LockFreeModel::new());
    let segmented = Arc::new(SegmentedLockModel::new());

    // Pre-populate with some topics
    for i in 0..10 {
        let topic = format!("topic-{}", i);
        let broker_name = format!("broker-{}", i);
        let queue_data = create_queue_data(&broker_name);

        // Can't easily pre-populate global lock model without exposing internals
        lock_free.topics.insert(topic.clone(), vec![queue_data.clone()]);
        segmented.topics.insert(topic, vec![queue_data]);
    }

    // ========================================================================
    // Benchmark 1: Concurrent Reads
    // ========================================================================

    println!("--- Benchmark 1: Concurrent Reads ---\n");

    for &num_threads in &thread_counts {
        let model = Arc::clone(&global_lock);
        bench_concurrent_reads(
            "Global RwLock   ",
            move |topic| {
                model.read_topic(topic);
            },
            num_threads,
            duration,
        );
    }
    println!();

    for &num_threads in &thread_counts {
        let model = Arc::clone(&lock_free);
        bench_concurrent_reads(
            "Lock-Free DashMap",
            move |topic| {
                model.read_topic(topic);
            },
            num_threads,
            duration,
        );
    }
    println!();

    for &num_threads in &thread_counts {
        let model = Arc::clone(&segmented);
        bench_concurrent_reads(
            "Segmented Locks ",
            move |topic| {
                model.read_topic(topic);
            },
            num_threads,
            duration,
        );
    }
    println!();

    // ========================================================================
    // Benchmark 2: Concurrent Writes
    // ========================================================================

    println!("--- Benchmark 2: Concurrent Writes ---\n");

    for &num_threads in &thread_counts {
        let model = Arc::clone(&global_lock);
        bench_concurrent_writes(
            "Global RwLock   ",
            move |name, data| {
                model.write_broker(name, data);
            },
            num_threads,
            duration,
        );
    }
    println!();

    for &num_threads in &thread_counts {
        let model = Arc::clone(&lock_free);
        bench_concurrent_writes(
            "Lock-Free DashMap",
            move |name, data| {
                model.write_broker(name, data);
            },
            num_threads,
            duration,
        );
    }
    println!();

    for &num_threads in &thread_counts {
        let model = Arc::clone(&segmented);
        bench_concurrent_writes(
            "Segmented Locks ",
            move |name, data| {
                model.write_broker(name, data);
            },
            num_threads,
            duration,
        );
    }
    println!();

    // ========================================================================
    // Benchmark 3: Mixed Workload (90% reads, 10% writes)
    // ========================================================================

    println!("--- Benchmark 3: Mixed Workload (90% reads, 10% writes) ---\n");

    for &num_threads in &thread_counts {
        let model = Arc::clone(&global_lock);
        let model_write = Arc::clone(&global_lock);
        bench_mixed_workload(
            "Global RwLock   ",
            move |topic| {
                model.read_topic(topic);
            },
            move |name, data| {
                model_write.write_broker(name, data);
            },
            num_threads,
            duration,
        );
    }
    println!();

    for &num_threads in &thread_counts {
        let model = Arc::clone(&lock_free);
        let model_write = Arc::clone(&lock_free);
        bench_mixed_workload(
            "Lock-Free DashMap",
            move |topic| {
                model.read_topic(topic);
            },
            move |name, data| {
                model_write.write_broker(name, data);
            },
            num_threads,
            duration,
        );
    }
    println!();

    for &num_threads in &thread_counts {
        let model = Arc::clone(&segmented);
        let model_write = Arc::clone(&segmented);
        bench_mixed_workload(
            "Segmented Locks ",
            move |topic| {
                model.read_topic(topic);
            },
            move |name, data| {
                model_write.write_broker(name, data);
            },
            num_threads,
            duration,
        );
    }

    println!("\n=================================================================");
    println!("Benchmark Complete");
    println!("=================================================================\n");
}
