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

//! Benchmark to measure RwLock overhead for consumer listener list access
//!
//! Compares:
//! - Direct Vec access (baseline)
//! - Arc<RwLock<Vec>> read access (P5 optimization)

use std::hint::black_box;
use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use parking_lot::RwLock;

struct DummyListener;

impl DummyListener {
    fn handle(&self, _value: u64) {
        // Simulate minimal work
        black_box(_value);
    }
}

/// Baseline: Direct Vec access
fn vec_direct_access(listeners: &[DummyListener], value: u64) {
    for listener in listeners {
        listener.handle(value);
    }
}

/// P5 implementation: Arc<RwLock<Vec>> with read lock
fn rwlock_read_access(listeners: &Arc<RwLock<Vec<DummyListener>>>, value: u64) {
    let guard = listeners.read();
    for listener in guard.iter() {
        listener.handle(value);
    }
}

fn benchmark_listener_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("listener_lock_overhead");

    // Setup: 1 listener (typical case)
    let vec_listeners = vec![DummyListener];
    let rwlock_listeners = Arc::new(RwLock::new(vec![DummyListener]));

    group.bench_function("vec_direct_1_listener", |b| {
        b.iter(|| vec_direct_access(black_box(&vec_listeners), black_box(42)))
    });

    group.bench_function("rwlock_read_1_listener", |b| {
        b.iter(|| rwlock_read_access(black_box(&rwlock_listeners), black_box(42)))
    });

    // Setup: 2 listeners (common case)
    let vec_listeners_2 = vec![DummyListener, DummyListener];
    let rwlock_listeners_2 = Arc::new(RwLock::new(vec![DummyListener, DummyListener]));

    group.bench_function("vec_direct_2_listeners", |b| {
        b.iter(|| vec_direct_access(black_box(&vec_listeners_2), black_box(42)))
    });

    group.bench_function("rwlock_read_2_listeners", |b| {
        b.iter(|| rwlock_read_access(black_box(&rwlock_listeners_2), black_box(42)))
    });

    group.finish();
}

/// Benchmark concurrent read access under RwLock
fn benchmark_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_listener_access");

    let rwlock_listeners = Arc::new(RwLock::new(vec![DummyListener]));

    // Simulate concurrent access by multiple threads
    group.bench_function("single_threaded_reads", |b| {
        b.iter(|| {
            for _ in 0..10 {
                rwlock_read_access(black_box(&rwlock_listeners), black_box(42));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_listener_access, benchmark_concurrent_reads);
criterion_main!(benches);
