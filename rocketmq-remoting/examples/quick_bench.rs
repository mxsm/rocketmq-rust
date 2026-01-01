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

//! Quick benchmark runner to validate EncodeBuffer performance
//!
//! Run with: cargo run --release --example quick_bench

use std::time::Instant;

use bytes::BytesMut;
use rocketmq_remoting::smart_encode_buffer::EncodeBuffer;

fn bench_encode_buffer(iterations: usize, size: usize) -> u128 {
    let mut eb = EncodeBuffer::new();
    let data = vec![0u8; size];

    let start = Instant::now();
    for _ in 0..iterations {
        eb.append(&data);
        let _ = eb.take_bytes();
    }
    start.elapsed().as_nanos()
}

fn bench_bytesmut(iterations: usize, size: usize) -> u128 {
    let mut buf = BytesMut::with_capacity(8192);
    let data = vec![0u8; size];

    let start = Instant::now();
    for _ in 0..iterations {
        buf.extend_from_slice(&data);
        let len = buf.len();
        let _ = buf.split_to(len).freeze();
    }
    start.elapsed().as_nanos()
}

fn main() {
    println!("EncodeBuffer vs BytesMut Performance Benchmark\n");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let test_cases = [
        ("Small (256B)", 10000, 256),
        ("Medium (1KB)", 10000, 1024),
        ("Large (4KB)", 5000, 4096),
        ("XLarge (16KB)", 2000, 16384),
    ];

    for (name, iterations, size) in test_cases.iter() {
        println!("Test: {} x {} iterations", name, iterations);
        println!("─────────────────────────────────────────────────────");

        // Warmup
        let _ = bench_encode_buffer(100, *size);
        let _ = bench_bytesmut(100, *size);

        // Actual benchmark
        let eb_time = bench_encode_buffer(*iterations, *size);
        let bm_time = bench_bytesmut(*iterations, *size);

        let eb_avg = eb_time / *iterations as u128;
        let bm_avg = bm_time / *iterations as u128;

        println!(
            "  EncodeBuffer: {:>8} ns/op  (total: {:>10} ms)",
            eb_avg,
            eb_time / 1_000_000
        );
        println!(
            "  BytesMut:     {:>8} ns/op  (total: {:>10} ms)",
            bm_avg,
            bm_time / 1_000_000
        );

        let diff = if eb_avg > bm_avg {
            let percent = ((eb_avg - bm_avg) as f64 / bm_avg as f64) * 100.0;
            format!("BytesMut is {:.1}% faster", percent)
        } else {
            let percent = ((bm_avg - eb_avg) as f64 / eb_avg as f64) * 100.0;
            format!("EncodeBuffer is {:.1}% faster", percent)
        };

        println!("  Result: {}", diff);
        println!();
    }

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Memory spike test
    println!("Spike Scenario Test (1x 64KB + 100x 512B)");
    println!("─────────────────────────────────────────────────────");

    let spike_test = || {
        let mut eb = EncodeBuffer::new();
        let mut bm = BytesMut::with_capacity(8192);

        let large = vec![0u8; 64 * 1024];
        let small = vec![0u8; 512];

        // EncodeBuffer
        let start = Instant::now();
        eb.append(&large);
        let _ = eb.take_bytes();
        for _ in 0..100 {
            eb.append(&small);
            let _ = eb.take_bytes();
        }
        let eb_time = start.elapsed().as_nanos();
        let eb_cap_after = eb.capacity();

        // BytesMut
        let start = Instant::now();
        bm.extend_from_slice(&large);
        let len = bm.len();
        let _ = bm.split_to(len).freeze();
        for _ in 0..100 {
            bm.extend_from_slice(&small);
            let len = bm.len();
            let _ = bm.split_to(len).freeze();
        }
        let bm_time = start.elapsed().as_nanos();
        let bm_cap_after = bm.capacity();

        (eb_time, eb_cap_after, bm_time, bm_cap_after)
    };

    let (eb_time, eb_cap, bm_time, bm_cap) = spike_test();

    println!("  EncodeBuffer: {} ns, final capacity: {} KB", eb_time, eb_cap / 1024);
    println!("  BytesMut:     {} ns, final capacity: {} KB", bm_time, bm_cap / 1024);
    println!(
        "  Memory saved: {} KB ({:.1}%)",
        (bm_cap - eb_cap) / 1024,
        ((bm_cap - eb_cap) as f64 / bm_cap as f64) * 100.0
    );
    println!();

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("\nBenchmark completed!");
    println!("\nFor detailed profiling, run:");
    println!("   cargo bench --bench encode_buffer_bench --package rocketmq-remoting");
}
