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

use std::cell::RefCell;
use std::hint::black_box;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rand::Rng;
use rocketmq_client_rust::common::thread_local_index::ThreadLocalIndex;

// ============================================================================
// Original implementation (before optimization)
// ============================================================================

thread_local! {
    static ORIGINAL_INDEX: RefCell<Option<i32>> = const {RefCell::new(None)};
}

const POSITIVE_MASK: i32 = 0x7FFFFFFF;
const MAX: i32 = i32::MAX;

#[derive(Default, Clone)]
pub struct OriginalThreadLocalIndex;

impl OriginalThreadLocalIndex {
    pub fn increment_and_get(&self) -> i32 {
        ORIGINAL_INDEX.with(|index| {
            let mut index = index.borrow_mut();
            let new_value = match *index {
                Some(val) => val.wrapping_add(1) & POSITIVE_MASK,
                None => rand::rng().random_range(0..=MAX) & POSITIVE_MASK,
            };
            *index = Some(new_value);
            new_value
        })
    }

    pub fn reset(&self) {
        let new_value = rand::rng().random_range(0..=MAX).abs();
        ORIGINAL_INDEX.with(|index| {
            *index.borrow_mut() = Some(new_value);
        });
    }
}

// ============================================================================
// Optimized implementation with ThreadRng (intermediate optimization)
// ============================================================================

thread_local! {
    static THREAD_RNG_INDEX: RefCell<Option<i32>> = const {RefCell::new(None)};
    static THREAD_RNG: RefCell<rand::rngs::ThreadRng> = RefCell::new(rand::rng());
}

#[derive(Default, Clone)]
pub struct ThreadRngThreadLocalIndex;

impl ThreadRngThreadLocalIndex {
    pub fn increment_and_get(&self) -> i32 {
        THREAD_RNG_INDEX.with(|index| {
            let mut index = index.borrow_mut();
            let new_value = match *index {
                Some(val) => val.wrapping_add(1),
                None => THREAD_RNG.with(|rng| rng.borrow_mut().random::<i32>()),
            };
            *index = Some(new_value);
            new_value & POSITIVE_MASK
        })
    }

    pub fn reset(&self) {
        THREAD_RNG_INDEX.with(|index| {
            let new_value = THREAD_RNG.with(|rng| rng.borrow_mut().random_range(0..MAX));
            *index.borrow_mut() = Some(new_value);
        });
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_increment_first_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("increment_first_call");

    group.bench_function("original", |b| {
        b.iter(|| {
            // Reset to None before each iteration to simulate first call
            ORIGINAL_INDEX.with(|index| *index.borrow_mut() = None);
            black_box(OriginalThreadLocalIndex.increment_and_get())
        })
    });

    group.bench_function("thread_rng", |b| {
        b.iter(|| {
            THREAD_RNG_INDEX.with(|index| *index.borrow_mut() = None);
            black_box(ThreadRngThreadLocalIndex.increment_and_get())
        })
    });

    group.bench_function("small_rng", |b| {
        b.iter(|| {
            // Reset the optimized version
            let idx = ThreadLocalIndex;
            idx.reset();
            black_box(idx.increment_and_get())
        })
    });

    group.finish();
}

fn bench_increment_subsequent_calls(c: &mut Criterion) {
    let mut group = c.benchmark_group("increment_subsequent_calls");

    // Initialize all versions first
    let _ = OriginalThreadLocalIndex.increment_and_get();
    let _ = ThreadRngThreadLocalIndex.increment_and_get();
    let _ = ThreadLocalIndex.increment_and_get();

    group.bench_function("original", |b| {
        b.iter(|| black_box(OriginalThreadLocalIndex.increment_and_get()))
    });

    group.bench_function("thread_rng", |b| {
        b.iter(|| black_box(ThreadRngThreadLocalIndex.increment_and_get()))
    });

    group.bench_function("small_rng", |b| {
        b.iter(|| black_box(ThreadLocalIndex.increment_and_get()))
    });

    group.finish();
}

fn bench_reset(c: &mut Criterion) {
    let mut group = c.benchmark_group("reset");

    group.bench_function("original", |b| {
        b.iter(|| {
            OriginalThreadLocalIndex.reset();
            black_box(())
        })
    });

    group.bench_function("thread_rng", |b| {
        b.iter(|| {
            ThreadRngThreadLocalIndex.reset();
            black_box(())
        })
    });

    group.bench_function("small_rng", |b| {
        b.iter(|| {
            ThreadLocalIndex.reset();
            black_box(())
        })
    });

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    group.bench_function("original", |b| {
        b.iter(|| {
            let idx = OriginalThreadLocalIndex;
            for _ in 0..100 {
                black_box(idx.increment_and_get());
            }
            idx.reset();
        })
    });

    group.bench_function("thread_rng", |b| {
        b.iter(|| {
            let idx = ThreadRngThreadLocalIndex;
            for _ in 0..100 {
                black_box(idx.increment_and_get());
            }
            idx.reset();
        })
    });

    group.bench_function("small_rng", |b| {
        b.iter(|| {
            let idx = ThreadLocalIndex;
            for _ in 0..100 {
                black_box(idx.increment_and_get());
            }
            idx.reset();
        })
    });

    group.finish();
}

fn bench_contention_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("contention_simulation");

    group.bench_function("original", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for _ in 0..4 {
                    s.spawn(|| {
                        let idx = OriginalThreadLocalIndex;
                        for _ in 0..1000 {
                            black_box(idx.increment_and_get());
                        }
                    });
                }
            });
        })
    });

    group.bench_function("thread_rng", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for _ in 0..4 {
                    s.spawn(|| {
                        let idx = ThreadRngThreadLocalIndex;
                        for _ in 0..1000 {
                            black_box(idx.increment_and_get());
                        }
                    });
                }
            });
        })
    });

    group.bench_function("small_rng", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for _ in 0..4 {
                    s.spawn(|| {
                        let idx = ThreadLocalIndex;
                        for _ in 0..1000 {
                            black_box(idx.increment_and_get());
                        }
                    });
                }
            });
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_increment_first_call,
    bench_increment_subsequent_calls,
    bench_reset,
    bench_mixed_workload,
    bench_contention_simulation
);
criterion_main!(benches);
