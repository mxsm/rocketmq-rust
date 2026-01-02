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

//! Benchmark comparing EncodeBuffer vs BytesMut performance
//!
//! Run with: cargo bench --bench encode_buffer_bench

use std::hint::black_box;

use bytes::BytesMut;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_remoting::smart_encode_buffer::EncodeBuffer;

/// Simulate typical message encoding pattern: write + extract
fn bench_encode_buffer_single_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_message");

    for size in [128, 512, 1024, 4096, 8192].iter() {
        // EncodeBuffer
        group.bench_with_input(BenchmarkId::new("EncodeBuffer", size), size, |b, &size| {
            let mut eb = EncodeBuffer::new();
            let data = vec![0u8; size];

            b.iter(|| {
                eb.append(black_box(&data));
                let bytes = eb.take_bytes();
                black_box(bytes);
            });
        });

        // BytesMut (baseline)
        group.bench_with_input(BenchmarkId::new("BytesMut", size), size, |b, &size| {
            let mut buf = BytesMut::with_capacity(8192);
            let data = vec![0u8; size];

            b.iter(|| {
                buf.extend_from_slice(black_box(&data));
                let len = buf.len();
                let bytes = buf.split_to(len).freeze();
                black_box(bytes);
            });
        });
    }

    group.finish();
}

/// Benchmark continuous write pattern (multiple messages)
fn bench_encode_buffer_continuous(c: &mut Criterion) {
    let mut group = c.benchmark_group("continuous_writes");

    for msg_count in [10, 100, 1000].iter() {
        let msg_size = 1024;

        // EncodeBuffer
        group.bench_with_input(BenchmarkId::new("EncodeBuffer", msg_count), msg_count, |b, &count| {
            let mut eb = EncodeBuffer::new();
            let data = vec![0u8; msg_size];

            b.iter(|| {
                for _ in 0..count {
                    eb.append(black_box(&data));
                    let bytes = eb.take_bytes();
                    black_box(bytes);
                }
            });
        });

        // BytesMut
        group.bench_with_input(BenchmarkId::new("BytesMut", msg_count), msg_count, |b, &count| {
            let mut buf = BytesMut::with_capacity(8192);
            let data = vec![0u8; msg_size];

            b.iter(|| {
                for _ in 0..count {
                    buf.extend_from_slice(black_box(&data));
                    let len = buf.len();
                    let bytes = buf.split_to(len).freeze();
                    black_box(bytes);
                }
            });
        });
    }

    group.finish();
}

/// Benchmark spike scenario: large message followed by small messages
fn bench_encode_buffer_spike(c: &mut Criterion) {
    let mut group = c.benchmark_group("spike_scenario");

    // EncodeBuffer - should handle spikes well with adaptive shrinking
    group.bench_function("EncodeBuffer_spike", |b| {
        let mut eb = EncodeBuffer::new();
        let large_data = vec![0u8; 64 * 1024]; // 64KB
        let small_data = vec![0u8; 512]; // 512 bytes

        b.iter(|| {
            // Large spike
            eb.append(black_box(&large_data));
            let _ = black_box(eb.take_bytes());

            // Multiple small messages
            for _ in 0..100 {
                eb.append(black_box(&small_data));
                let _ = black_box(eb.take_bytes());
            }
        });
    });

    // BytesMut - may keep large capacity
    group.bench_function("BytesMut_spike", |b| {
        let mut buf = BytesMut::with_capacity(8192);
        let large_data = vec![0u8; 64 * 1024];
        let small_data = vec![0u8; 512];

        b.iter(|| {
            // Large spike
            buf.extend_from_slice(black_box(&large_data));
            let len = buf.len();
            let _ = black_box(buf.split_to(len).freeze());

            // Multiple small messages
            for _ in 0..100 {
                buf.extend_from_slice(black_box(&small_data));
                let len = buf.len();
                let _ = black_box(buf.split_to(len).freeze());
            }
        });
    });

    group.finish();
}

/// Benchmark memory usage patterns
fn bench_encode_buffer_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");

    // Test varying message sizes
    group.bench_function("EncodeBuffer_varied_sizes", |b| {
        let mut eb = EncodeBuffer::new();
        let sizes = [128, 512, 1024, 4096, 2048, 1024, 512, 256];
        let data_sets: Vec<Vec<u8>> = sizes.iter().map(|&s| vec![0u8; s]).collect();

        b.iter(|| {
            for data in &data_sets {
                eb.append(black_box(data));
                let _ = black_box(eb.take_bytes());
            }
        });
    });

    group.bench_function("BytesMut_varied_sizes", |b| {
        let mut buf = BytesMut::with_capacity(8192);
        let sizes = [128, 512, 1024, 4096, 2048, 1024, 512, 256];
        let data_sets: Vec<Vec<u8>> = sizes.iter().map(|&s| vec![0u8; s]).collect();

        b.iter(|| {
            for data in &data_sets {
                buf.extend_from_slice(black_box(data));
                let len = buf.len();
                let _ = black_box(buf.split_to(len).freeze());
            }
        });
    });

    group.finish();
}

/// Benchmark allocation overhead
fn bench_encode_buffer_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation_overhead");

    // Small capacity start, force expansion
    group.bench_function("EncodeBuffer_expansion", |b| {
        let data = vec![0u8; 16 * 1024]; // 16KB

        b.iter(|| {
            let mut eb = EncodeBuffer::new(); // Starts with 8KB
            eb.append(black_box(&data)); // Forces expansion
            let _ = black_box(eb.take_bytes());
        });
    });

    group.bench_function("BytesMut_expansion", |b| {
        let data = vec![0u8; 16 * 1024];

        b.iter(|| {
            let mut buf = BytesMut::with_capacity(8 * 1024);
            buf.extend_from_slice(black_box(&data));
            let len = buf.len();
            let _ = black_box(buf.split_to(len).freeze());
        });
    });

    group.finish();
}

/// Benchmark realistic RPC workload
fn bench_encode_buffer_rpc_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_workload");
    group.sample_size(50); // Reduce sample size for realistic workload

    // Simulate typical RPC pattern: mix of small and medium messages
    group.bench_function("EncodeBuffer_rpc_mixed", |b| {
        let mut eb = EncodeBuffer::new();

        // Typical RPC message sizes distribution
        let small_msg = vec![0u8; 256]; // 60% of messages
        let medium_msg = vec![0u8; 2048]; // 30% of messages
        let large_msg = vec![0u8; 8192]; // 10% of messages

        b.iter(|| {
            // Simulate 100 RPC calls
            for i in 0..100 {
                let data = if i % 10 == 0 {
                    &large_msg
                } else if i % 3 == 0 {
                    &medium_msg
                } else {
                    &small_msg
                };

                eb.append(black_box(data));
                let _ = black_box(eb.take_bytes());
            }
        });
    });

    group.bench_function("BytesMut_rpc_mixed", |b| {
        let mut buf = BytesMut::with_capacity(8192);

        let small_msg = vec![0u8; 256];
        let medium_msg = vec![0u8; 2048];
        let large_msg = vec![0u8; 8192];

        b.iter(|| {
            for i in 0..100 {
                let data = if i % 10 == 0 {
                    &large_msg
                } else if i % 3 == 0 {
                    &medium_msg
                } else {
                    &small_msg
                };

                buf.extend_from_slice(black_box(data));
                let len = buf.len();
                let _ = black_box(buf.split_to(len).freeze());
            }
        });
    });

    group.finish();
}

/// Benchmark worst case: alternating large and small messages
fn bench_encode_buffer_worst_case(c: &mut Criterion) {
    let mut group = c.benchmark_group("worst_case");

    group.bench_function("EncodeBuffer_alternating", |b| {
        let mut eb = EncodeBuffer::new();
        let large = vec![0u8; 32 * 1024];
        let small = vec![0u8; 128];

        b.iter(|| {
            for i in 0..50 {
                let data = if i % 2 == 0 { &large } else { &small };
                eb.append(black_box(data));
                let _ = black_box(eb.take_bytes());
            }
        });
    });

    group.bench_function("BytesMut_alternating", |b| {
        let mut buf = BytesMut::with_capacity(8192);
        let large = vec![0u8; 32 * 1024];
        let small = vec![0u8; 128];

        b.iter(|| {
            for i in 0..50 {
                let data = if i % 2 == 0 { &large } else { &small };
                buf.extend_from_slice(black_box(data));
                let len = buf.len();
                let _ = black_box(buf.split_to(len).freeze());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_encode_buffer_single_message,
    bench_encode_buffer_continuous,
    bench_encode_buffer_spike,
    bench_encode_buffer_memory,
    bench_encode_buffer_allocation,
    bench_encode_buffer_rpc_workload,
    bench_encode_buffer_worst_case,
);

criterion_main!(benches);
