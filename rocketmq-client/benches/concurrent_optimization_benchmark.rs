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

//! Concurrent Optimization Benchmark
//!
//! This benchmark validates the performance improvements from concurrent optimizations:
//! - Removed thread::spawn + block_on anti-pattern in ServiceDetector
//! - Optimized async execution pathways
//! - Reduced thread context switching overhead
//! - DashMap-based lock-free concurrent access
//!
//! ## Performance Baselines
//! - Concurrent config reads: 2.835ms for 50k ops (56.7ns/op)
//! - Message creation: ~174ns per message
//! - Producer initialization: ~9.3ms
//!
//! ## Target Improvements
//! - 15-25% performance improvement in concurrent scenarios
//! - Reduced CPU usage due to eliminating unnecessary thread creation
//!
//! Run with: cargo bench --bench concurrent_optimization_benchmark

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use cheetah_string::CheetahString;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_common::common::message::message_single::Message;

/// Benchmark 1: Concurrent Producer Access (DashMap verification)
///
/// This tests the concurrent access pattern after DashMap migration in Phase 1.
/// Expected: No regression, should maintain ~56.7ns/op latency
fn bench_concurrent_producer_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_producer_access");

    for thread_count in [1, 2, 4, 8, 16, 32].iter() {
        group.throughput(Throughput::Elements((*thread_count * 1000) as u64));

        group.bench_with_input(
            BenchmarkId::new("config_reads", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    // Create producer
                    let producer = DefaultMQProducer::new();

                    let producer_arc = Arc::new(producer);

                    // Spawn threads to concurrently access producer config
                    let handles: Vec<_> = (0..thread_count)
                        .map(|_| {
                            let producer_clone = Arc::clone(&producer_arc);
                            thread::spawn(move || {
                                for _ in 0..1000 {
                                    // Access producer group name (reads from internal structures)
                                    black_box(producer_clone.producer_config().producer_group());
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark 2: Message Creation and Validation
///
/// Tests message creation overhead, which is a baseline for send operations.
fn bench_message_creation(c: &mut Criterion) {
    c.bench_function("message_creation", |b| {
        b.iter(|| {
            let msg = Message::new(CheetahString::from_static_str("TestTopic"), "Hello RocketMQ".as_bytes());
            black_box(msg);
        });
    });
}

/// Benchmark 3: Producer Initialization Overhead
///
/// Measures the cost of creating a new producer instance.
fn bench_producer_initialization(c: &mut Criterion) {
    c.bench_function("producer_initialization", |b| {
        b.iter(|| {
            let producer = DefaultMQProducer::new();
            black_box(producer);
        });
    });
}

/// Benchmark 4: Concurrent Message Preparation
///
/// Simulates preparing multiple messages concurrently (pre-send phase).
fn bench_concurrent_message_preparation(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_message_preparation");

    for msg_count in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*msg_count as u64));

        group.bench_with_input(
            BenchmarkId::new("prepare_messages", msg_count),
            msg_count,
            |b, &msg_count| {
                b.iter(|| {
                    let messages: Vec<_> = (0..msg_count)
                        .map(|i| {
                            Message::new(
                                CheetahString::from_slice(format!("Topic_{}", i % 10).as_str()),
                                format!("Message body {}", i).as_bytes(),
                            )
                        })
                        .collect();
                    black_box(messages);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark 5: Performance Baseline Comparison
///
/// This establishes performance baselines for concurrent operations.
/// Baseline: 2.835ms for 50k ops (56.7ns/op)
/// Target: ≤ 2.4ms (15% improvement)
fn bench_performance_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("performance_baseline");

    // Use same parameters as original baseline test
    let thread_count = 50;
    let ops_per_thread = 1000;
    let total_ops = thread_count * ops_per_thread;

    group.throughput(Throughput::Elements(total_ops as u64));

    group.bench_function("concurrent_config_reads_50k", |b| {
        b.iter(|| {
            let producer = DefaultMQProducer::new();

            let producer_arc = Arc::new(producer);

            let start = Instant::now();

            let handles: Vec<_> = (0..thread_count)
                .map(|_| {
                    let producer_clone = Arc::clone(&producer_arc);
                    thread::spawn(move || {
                        for _ in 0..ops_per_thread {
                            black_box(producer_clone.producer_config().producer_group());
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }

            let elapsed = start.elapsed();
            black_box(elapsed);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_producer_access,
    bench_message_creation,
    bench_producer_initialization,
    bench_concurrent_message_preparation,
    bench_performance_baseline
);

criterion_main!(benches);
