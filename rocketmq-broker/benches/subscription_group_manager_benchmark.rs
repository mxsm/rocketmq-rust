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

//! Performance benchmarks for SubscriptionGroupManager
//!
//! This benchmark suite validates the DashMap optimization claims:
//! - 2-5x performance improvement in concurrent read scenarios
//! - 2-3x improvement in concurrent write scenarios
//! - Reduced lock contention under high load

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use dashmap::DashMap;
use parking_lot::Mutex;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

// Simulate the old Mutex-based approach for comparison
struct MutexBasedManager {
    table: Arc<Mutex<HashMap<CheetahString, SubscriptionGroupConfig>>>,
}

impl MutexBasedManager {
    fn new() -> Self {
        Self {
            table: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn insert(&self, key: CheetahString, value: SubscriptionGroupConfig) {
        self.table.lock().insert(key, value);
    }

    fn get(&self, key: &str) -> Option<SubscriptionGroupConfig> {
        self.table.lock().get(key).cloned()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.table.lock().contains_key(key)
    }

    fn len(&self) -> usize {
        self.table.lock().len()
    }
}

// New DashMap-based approach
struct DashMapBasedManager {
    table: Arc<DashMap<CheetahString, Arc<SubscriptionGroupConfig>>>,
}

impl DashMapBasedManager {
    fn new() -> Self {
        Self {
            table: Arc::new(DashMap::new()),
        }
    }

    fn insert(&self, key: CheetahString, value: SubscriptionGroupConfig) {
        self.table.insert(key, Arc::new(value));
    }

    fn get(&self, key: &str) -> Option<SubscriptionGroupConfig> {
        self.table.get(key).map(|entry| (**entry.value()).clone())
    }

    fn contains_key(&self, key: &str) -> bool {
        self.table.contains_key(key)
    }

    fn len(&self) -> usize {
        self.table.len()
    }
}

fn setup_mutex_manager(size: usize) -> MutexBasedManager {
    let manager = MutexBasedManager::new();
    for i in 0..size {
        let group_name = CheetahString::from_string(format!("GROUP_{}", i));
        let config = SubscriptionGroupConfig::new(group_name.clone());
        manager.insert(group_name, config);
    }
    manager
}

fn setup_dashmap_manager(size: usize) -> DashMapBasedManager {
    let manager = DashMapBasedManager::new();
    for i in 0..size {
        let group_name = CheetahString::from_string(format!("GROUP_{}", i));
        let config = SubscriptionGroupConfig::new(group_name.clone());
        manager.insert(group_name, config);
    }
    manager
}

fn benchmark_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // Mutex-based benchmark
        group.bench_with_input(BenchmarkId::new("mutex", size), size, |b, &size| {
            let manager = Arc::new(setup_mutex_manager(size));
            b.iter(|| {
                let handles: Vec<_> = (0..10)
                    .map(|_| {
                        let mgr = manager.clone();
                        thread::spawn(move || {
                            for i in 0..100 {
                                let key = format!("GROUP_{}", i % size);
                                mgr.get(&key);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        // DashMap-based benchmark
        group.bench_with_input(BenchmarkId::new("dashmap", size), size, |b, &size| {
            let manager = Arc::new(setup_dashmap_manager(size));
            b.iter(|| {
                let handles: Vec<_> = (0..10)
                    .map(|_| {
                        let mgr = manager.clone();
                        thread::spawn(move || {
                            for i in 0..100 {
                                let key = format!("GROUP_{}", i % size);
                                mgr.get(&key);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

fn benchmark_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");

    for size in [100, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // Mutex-based benchmark
        group.bench_with_input(BenchmarkId::new("mutex", size), size, |b, &size| {
            b.iter(|| {
                let manager = Arc::new(MutexBasedManager::new());
                let handles: Vec<_> = (0..10)
                    .map(|thread_id| {
                        let mgr = manager.clone();
                        thread::spawn(move || {
                            for i in 0..size / 10 {
                                let key = CheetahString::from_string(format!("GROUP_{}_{}", thread_id, i));
                                let config = SubscriptionGroupConfig::new(key.clone());
                                mgr.insert(key, config);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });

        // DashMap-based benchmark
        group.bench_with_input(BenchmarkId::new("dashmap", size), size, |b, &size| {
            b.iter(|| {
                let manager = Arc::new(DashMapBasedManager::new());
                let handles: Vec<_> = (0..10)
                    .map(|thread_id| {
                        let mgr = manager.clone();
                        thread::spawn(move || {
                            for i in 0..size / 10 {
                                let key = CheetahString::from_string(format!("GROUP_{}_{}", thread_id, i));
                                let config = SubscriptionGroupConfig::new(key.clone());
                                mgr.insert(key, config);
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

fn benchmark_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    let size = 1000;
    group.throughput(Throughput::Elements(size as u64));

    // Mutex-based benchmark
    group.bench_function("mutex", |b| {
        let manager = Arc::new(setup_mutex_manager(size));
        b.iter(|| {
            let handles: Vec<_> = (0..10)
                .map(|thread_id| {
                    let mgr = manager.clone();
                    thread::spawn(move || {
                        for i in 0..100 {
                            if i % 3 == 0 {
                                // Write operation
                                let key = CheetahString::from_string(format!("NEW_GROUP_{}_{}", thread_id, i));
                                let config = SubscriptionGroupConfig::new(key.clone());
                                mgr.insert(key, config);
                            } else {
                                // Read operation
                                let key = format!("GROUP_{}", i % size);
                                mgr.get(&key);
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    // DashMap-based benchmark
    group.bench_function("dashmap", |b| {
        let manager = Arc::new(setup_dashmap_manager(size));
        b.iter(|| {
            let handles: Vec<_> = (0..10)
                .map(|thread_id| {
                    let mgr = manager.clone();
                    thread::spawn(move || {
                        for i in 0..100 {
                            if i % 3 == 0 {
                                // Write operation
                                let key = CheetahString::from_string(format!("NEW_GROUP_{}_{}", thread_id, i));
                                let config = SubscriptionGroupConfig::new(key.clone());
                                mgr.insert(key, config);
                            } else {
                                // Read operation
                                let key = format!("GROUP_{}", i % size);
                                mgr.get(&key);
                            }
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        });
    });

    group.finish();
}

fn benchmark_contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_key");

    let size = 1000;

    // Mutex-based benchmark
    group.bench_function("mutex", |b| {
        let manager = setup_mutex_manager(size);
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("GROUP_{}", i % size);
                manager.contains_key(&key);
            }
        });
    });

    // DashMap-based benchmark
    group.bench_function("dashmap", |b| {
        let manager = setup_dashmap_manager(size);
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("GROUP_{}", i % size);
                manager.contains_key(&key);
            }
        });
    });

    group.finish();
}

fn benchmark_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        // Mutex-based benchmark
        group.bench_with_input(BenchmarkId::new("mutex", size), size, |b, &size| {
            let manager = setup_mutex_manager(size);
            b.iter(|| {
                let len = manager.len();
                assert_eq!(len, size);
            });
        });

        // DashMap-based benchmark
        group.bench_with_input(BenchmarkId::new("dashmap", size), size, |b, &size| {
            let manager = setup_dashmap_manager(size);
            b.iter(|| {
                let len = manager.len();
                assert_eq!(len, size);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_concurrent_reads,
    benchmark_concurrent_writes,
    benchmark_mixed_workload,
    benchmark_contains_key,
    benchmark_iteration,
);

criterion_main!(benches);
