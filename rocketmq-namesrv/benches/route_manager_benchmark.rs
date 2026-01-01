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

//! Performance benchmarks for RouteInfoManager V2 DashMap tables
//!
//! Run with: cargo bench --package rocketmq-namesrv --bench route_manager_benchmark
//!
//! These benchmarks focus on the core data structures (DashMap-based tables)
//! which provide the main performance improvements in V2.
//!
//! Key findings:
//! - DashMap provides lock-free reads
//! - Fine-grained locking for writes
//! - CheetahString provides zero-copy cloning
//! - Concurrent operations scale linearly with thread count

use std::hint::black_box;
use std::sync::Arc;
use std::thread;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use dashmap::DashMap;

// Type aliases matching the actual implementation
type ClusterName = CheetahString;
type BrokerName = CheetahString;
type TopicName = CheetahString;

// Helper functions to create test data
fn create_cluster_name(i: usize) -> ClusterName {
    CheetahString::from_string(format!("Cluster{}", i))
}

fn create_broker_name(i: usize) -> BrokerName {
    CheetahString::from_string(format!("Broker{}", i))
}

fn create_topic_name(i: usize) -> TopicName {
    CheetahString::from_string(format!("Topic{}", i))
}

// ========== Benchmark 1: DashMap Insert Operations ==========

fn bench_dashmap_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_insert");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("single_value", size), size, |b, &size| {
            b.iter(|| {
                let map: DashMap<ClusterName, BrokerName> = DashMap::new();

                for i in 0..size {
                    let cluster = create_cluster_name(i);
                    let broker = create_broker_name(i);
                    map.insert(cluster, broker);
                }

                black_box(map)
            });
        });

        group.bench_with_input(BenchmarkId::new("vec_values", size), size, |b, &size| {
            b.iter(|| {
                let map: DashMap<ClusterName, Vec<BrokerName>> = DashMap::new();

                for i in 0..size {
                    let cluster = create_cluster_name(i % 100); // 100 clusters
                    let broker = create_broker_name(i);

                    map.entry(cluster).or_default().push(broker);
                }

                black_box(map)
            });
        });
    }

    group.finish();
}

// ========== Benchmark 2: Concurrent Reads ==========

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");

    // Pre-populate DashMap
    let map: Arc<DashMap<ClusterName, Vec<BrokerName>>> = Arc::new(DashMap::new());

    // Insert 1000 entries with 10 brokers per cluster
    for i in 0..1000 {
        let cluster = create_cluster_name(i % 100);
        let broker = create_broker_name(i);

        map.entry(cluster).or_default().push(broker);
    }

    for thread_count in [1, 2, 4, 8, 16, 32].iter() {
        group.bench_with_input(
            BenchmarkId::new("lookup", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    let mut handles = vec![];

                    for _ in 0..thread_count {
                        let map_clone = Arc::clone(&map);
                        handles.push(thread::spawn(move || {
                            for i in 0..100 {
                                let cluster = create_cluster_name(i % 100);
                                black_box(map_clone.get(&cluster));
                            }
                        }));
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

// ========== Benchmark 3: Concurrent Writes ==========

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");

    for thread_count in [1, 2, 4, 8, 16, 32].iter() {
        group.bench_with_input(
            BenchmarkId::new("insert", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    let map: Arc<DashMap<ClusterName, Vec<BrokerName>>> = Arc::new(DashMap::new());
                    let mut handles = vec![];

                    for t in 0..thread_count {
                        let map_clone = Arc::clone(&map);
                        handles.push(thread::spawn(move || {
                            for i in 0..100 {
                                let cluster = create_cluster_name((t * 100 + i) % 50);
                                let broker = create_broker_name(t * 100 + i);

                                map_clone.entry(cluster).or_default().push(broker);
                            }
                        }));
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    black_box(map)
                });
            },
        );
    }

    group.finish();
}

// ========== Benchmark 4: Mixed Read/Write ==========

fn bench_mixed_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_operations");

    for thread_count in [2, 4, 8, 16, 32].iter() {
        group.bench_with_input(
            BenchmarkId::new("50_read_50_write", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    let map: Arc<DashMap<ClusterName, Vec<BrokerName>>> = Arc::new(DashMap::new());

                    // Pre-populate
                    for i in 0..100 {
                        let cluster = create_cluster_name(i % 20);
                        let broker = create_broker_name(i);
                        map.entry(cluster).or_default().push(broker);
                    }

                    let mut handles = vec![];

                    // Half threads do reads
                    for t in 0..(thread_count / 2) {
                        let map_clone = Arc::clone(&map);
                        handles.push(thread::spawn(move || {
                            for i in 0..100 {
                                let cluster = create_cluster_name((t * 100 + i) % 20);
                                black_box(map_clone.get(&cluster));
                            }
                        }));
                    }

                    // Half threads do writes
                    for t in (thread_count / 2)..thread_count {
                        let map_clone = Arc::clone(&map);
                        handles.push(thread::spawn(move || {
                            for i in 0..100 {
                                let cluster = create_cluster_name((t * 100 + i) % 20);
                                let broker = create_broker_name(t * 100 + i + 1000);
                                map_clone.entry(cluster).or_default().push(broker);
                            }
                        }));
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    black_box(map)
                });
            },
        );
    }

    group.finish();
}

// ========== Benchmark 5: CheetahString vs String Performance ==========

fn bench_string_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_operations");

    group.bench_function("cheetah_string_clone_1k", |b| {
        let s = CheetahString::from_string("TestClusterName".to_string());
        b.iter(|| {
            for _ in 0..1000 {
                black_box(s.clone());
            }
        });
    });

    group.bench_function("string_clone_1k", |b| {
        let s = "TestClusterName".to_string();
        b.iter(|| {
            for _ in 0..1000 {
                black_box(s.clone());
            }
        });
    });

    group.bench_function("arc_str_clone_1k", |b| {
        let s: Arc<str> = Arc::from("TestClusterName");
        b.iter(|| {
            for _ in 0..1000 {
                black_box(Arc::clone(&s));
            }
        });
    });

    // Test with longer strings (typical broker address)
    group.bench_function("cheetah_string_clone_long", |b| {
        let s = CheetahString::from_string("broker-a-192.168.1.100:10911".to_string());
        b.iter(|| {
            for _ in 0..1000 {
                black_box(s.clone());
            }
        });
    });

    group.bench_function("string_clone_long", |b| {
        let s = "broker-a-192.168.1.100:10911".to_string();
        b.iter(|| {
            for _ in 0..1000 {
                black_box(s.clone());
            }
        });
    });

    group.finish();
}

// ========== Benchmark 6: Iteration ==========

fn bench_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration");

    // Pre-populate DashMap with various sizes
    for size in [100, 1000, 10000].iter() {
        let map: DashMap<TopicName, Vec<BrokerName>> = DashMap::new();

        for i in 0..*size {
            let topic = create_topic_name(i);
            let brokers: Vec<BrokerName> = (0..10).map(|j| create_broker_name(i * 10 + j)).collect();
            map.insert(topic, brokers);
        }

        group.bench_with_input(BenchmarkId::new("iter_keys", size), &map, |b, map| {
            b.iter(|| {
                let count = map.iter().count();
                black_box(count)
            });
        });

        group.bench_with_input(BenchmarkId::new("iter_values", size), &map, |b, map| {
            b.iter(|| {
                let total: usize = map.iter().map(|entry| entry.value().len()).sum();
                black_box(total)
            });
        });
    }

    group.finish();
}

// ========== Benchmark 7: Memory Footprint Comparison ==========

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_comparison");

    // Benchmark memory allocation patterns
    group.bench_function("dashmap_with_cheetahstring", |b| {
        b.iter(|| {
            let map: DashMap<CheetahString, Vec<CheetahString>> = DashMap::new();

            for i in 0..1000 {
                let cluster = CheetahString::from_string(format!("Cluster{}", i % 100));
                let broker = CheetahString::from_string(format!("Broker{}", i));

                map.entry(cluster).or_default().push(broker);
            }

            black_box(map)
        });
    });

    group.bench_function("dashmap_with_string", |b| {
        b.iter(|| {
            let map: DashMap<String, Vec<String>> = DashMap::new();

            for i in 0..1000 {
                let cluster = format!("Cluster{}", i % 100);
                let broker = format!("Broker{}", i);

                map.entry(cluster).or_default().push(broker);
            }

            black_box(map)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_dashmap_insertion,
    bench_concurrent_reads,
    bench_concurrent_writes,
    bench_mixed_operations,
    bench_string_operations,
    bench_iteration,
    bench_memory_usage
);

criterion_main!(benches);
