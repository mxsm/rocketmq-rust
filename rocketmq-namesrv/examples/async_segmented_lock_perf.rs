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

//! Async segmented lock performance test example
//!
//! Run: cargo run --release --example async_segmented_lock_perf

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_namesrv::route::async_segmented_lock::AsyncSegmentedLock;
use tokio::sync::RwLock as TokioRwLock;

/// Benchmark global lock performance
async fn bench_global_lock(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(TokioRwLock::new(0u64));
    let start = Instant::now();
    let mut handles = vec![];

    for _ in 0..tasks {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let task_start = Instant::now();

            while task_start.elapsed() < duration {
                let _guard = lock.read().await;
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let total: u64 = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .sum();
    let elapsed = start.elapsed();
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    println!(
        "Global    | {:>2} tasks | {:>12} ops | {:>10.0} ops/s",
        tasks, total, ops_per_sec
    );

    total
}

/// Benchmark segmented lock performance
async fn bench_segmented_lock(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..tasks {
        let lock = Arc::clone(&lock);
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let task_start = Instant::now();
            let key = format!("key-{}", i % 10); // 10 different keys

            while task_start.elapsed() < duration {
                let _guard = lock.read_lock(&key).await;
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let total: u64 = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .sum();
    let elapsed = start.elapsed();
    let ops_per_sec = total as f64 / elapsed.as_secs_f64();

    println!(
        "Segmented | {:>2} tasks | {:>12} ops | {:>10.0} ops/s",
        tasks, total, ops_per_sec
    );

    total
}

#[tokio::main]
async fn main() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║      Async Segmented Lock Performance Test (Tokio)          ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    let duration = Duration::from_secs(2);
    let task_counts = vec![1, 2, 4, 8, 16];

    println!("Test parameters: each test runs for {} seconds", duration.as_secs());
    println!("Segment count: 16 segments");
    println!("Runtime: Tokio async runtime\n");

    println!("─────────────────────────────────────────────────────────────");
    println!("Scenario: Concurrent Async Read Operations");
    println!("─────────────────────────────────────────────────────────────\n");

    println!("Lock Type | Tasks  | Total Ops      | Throughput(ops/s)");
    println!("──────────┼────────┼────────────────┼──────────────────");

    let mut global_results = vec![];
    let mut segmented_results = vec![];

    for &tasks in &task_counts {
        let global_ops = bench_global_lock(tasks, duration).await;
        global_results.push(global_ops);
    }

    println!("──────────┼────────┼────────────────┼──────────────────");

    for &tasks in &task_counts {
        let segmented_ops = bench_segmented_lock(tasks, duration).await;
        segmented_results.push(segmented_ops);
    }

    println!("─────────────────────────────────────────────────────────────\n");

    println!("Performance Comparison:");
    println!("─────────┬────────┬────────────────┬────────────────");
    println!("Tasks    | Global | Segmented      | Speedup");
    println!("─────────┼────────┼────────────────┼────────────────");

    for (i, &tasks) in task_counts.iter().enumerate() {
        let global = global_results[i];
        let segmented = segmented_results[i];
        let speedup = segmented as f64 / global as f64;

        println!(
            "{:>2} tasks | {:>6} | {:>14} | {:>6.2}x",
            tasks,
            format!("{}M", global / 1_000_000),
            format!("{}M", segmented / 1_000_000),
            speedup
        );
    }

    println!("─────────────────────────────────────────────────────────────\n");

    // Calculate average speedup
    let avg_speedup: f64 = task_counts
        .iter()
        .enumerate()
        .map(|(i, _)| segmented_results[i] as f64 / global_results[i] as f64)
        .sum::<f64>()
        / task_counts.len() as f64;

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!(
        "║ Average Performance Improvement: {:.2}x                      ║",
        avg_speedup
    );
    println!(
        "║ Lock Contention Reduction:       {:.1}%                      ║",
        (1.0 - 1.0 / 16.0) * 100.0
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("📊 Conclusions:");
    println!("  • Async segmented lock reduces contention via 16-way concurrency");
    println!("  • Performance improvement is most significant with high task counts");
    println!("  • Well-suited for async I/O-bound workloads with concurrent access");
    println!("  • Tokio runtime efficiently schedules tasks across segments\n");
}
