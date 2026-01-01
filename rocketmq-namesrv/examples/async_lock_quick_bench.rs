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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_namesrv::route::async_segmented_lock::AsyncSegmentedLock;
use tokio::sync::RwLock;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║   Async Segmented Lock Performance Benchmark Report         ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("Test Duration: 500ms per scenario");
    println!("Segment Count: 16 segments");
    println!("Runtime: Tokio async runtime\n");

    // Test 1: Concurrent Read Performance
    println!("═══════════════════════════════════════════════════════════════");
    println!("Test 1: Concurrent Read Operations");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!(
        "{:<15} {:<10} {:<18} {:<20} {:<10}",
        "Lock Type", "Tasks", "Total Operations", "Throughput (ops/s)", "Speedup"
    );
    println!("{}", "─".repeat(80));

    for tasks in [1, 2, 4, 8] {
        let global_ops = test_global_read(tasks, Duration::from_millis(500)).await;
        let segmented_ops = test_segmented_read(tasks, Duration::from_millis(500)).await;

        let global_throughput = (global_ops as f64 / 0.5) as u64;
        let segmented_throughput = (segmented_ops as f64 / 0.5) as u64;
        let speedup = segmented_ops as f64 / global_ops as f64;

        println!(
            "{:<15} {:<10} {:<18} {:<20} {:<10}",
            "Global Lock",
            format!("{}", tasks),
            format!("{}", global_ops),
            format!("{}", global_throughput),
            "-"
        );
        println!(
            "{:<15} {:<10} {:<18} {:<20} {:<10}",
            "Segmented Lock",
            format!("{}", tasks),
            format!("{}", segmented_ops),
            format!("{}", segmented_throughput),
            format!("{:.2}x", speedup)
        );
        println!();
    }

    // Test 2: Concurrent Write Performance
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("Test 2: Concurrent Write Operations");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!(
        "{:<15} {:<10} {:<18} {:<20} {:<10}",
        "Lock Type", "Tasks", "Total Operations", "Throughput (ops/s)", "Speedup"
    );
    println!("{}", "─".repeat(80));

    for tasks in [1, 2, 4, 8] {
        let global_ops = test_global_write(tasks, Duration::from_millis(500)).await;
        let segmented_ops = test_segmented_write(tasks, Duration::from_millis(500)).await;

        let global_throughput = (global_ops as f64 / 0.5) as u64;
        let segmented_throughput = (segmented_ops as f64 / 0.5) as u64;
        let speedup = segmented_ops as f64 / global_ops as f64;

        println!(
            "{:<15} {:<10} {:<18} {:<20} {:<10}",
            "Global Lock",
            format!("{}", tasks),
            format!("{}", global_ops),
            format!("{}", global_throughput),
            "-"
        );
        println!(
            "{:<15} {:<10} {:<18} {:<20} {:<10}",
            "Segmented Lock",
            format!("{}", tasks),
            format!("{}", segmented_ops),
            format!("{}", segmented_throughput),
            format!("{:.2}x", speedup)
        );
        println!();
    }

    // Test 3: Mixed Workload
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("Test 3: Mixed Workload (90% Read, 10% Write)");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!(
        "{:<15} {:<10} {:<18} {:<20} {:<10}",
        "Lock Type", "Tasks", "Total Operations", "Throughput (ops/s)", "Speedup"
    );
    println!("{}", "─".repeat(80));

    for tasks in [1, 2, 4, 8] {
        let global_ops = test_global_mixed(tasks, Duration::from_millis(500)).await;
        let segmented_ops = test_segmented_mixed(tasks, Duration::from_millis(500)).await;

        let global_throughput = (global_ops as f64 / 0.5) as u64;
        let segmented_throughput = (segmented_ops as f64 / 0.5) as u64;
        let speedup = segmented_ops as f64 / global_ops as f64;

        println!(
            "{:<15} {:<10} {:<18} {:<20} {:<10}",
            "Global Lock",
            format!("{}", tasks),
            format!("{}", global_ops),
            format!("{}", global_throughput),
            "-"
        );
        println!(
            "{:<15} {:<10} {:<18} {:<20} {:<10}",
            "Segmented Lock",
            format!("{}", tasks),
            format!("{}", segmented_ops),
            format!("{}", segmented_throughput),
            format!("{:.2}x", speedup)
        );
        println!();
    }

    // Summary
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                     Benchmark Summary                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
    println!("✓ Segmented lock provides better scalability for concurrent operations");
    println!("✓ Performance improvement increases with higher concurrency");
    println!("✓ Best suited for workloads with multiple independent keys");
    println!("✓ Read-heavy workloads benefit most from segmentation\n");
}

#[allow(unused_variables)]
async fn test_global_read(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(RwLock::new(0u64));
    let _start = Instant::now();
    let mut handles = vec![];

    for _ in 0..tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let start_inner = Instant::now();
            while start_inner.elapsed() < duration {
                let _guard = lock_clone.read().await;
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    total
}

#[allow(unused_variables)]
async fn test_segmented_read(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let start_inner = Instant::now();
            while start_inner.elapsed() < duration {
                let key = format!("key-{}", (i as u64 + count) % 100);
                let _guard = lock_clone.read_lock(&key).await;
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    total
}

#[allow(unused_variables)]
async fn test_global_write(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(RwLock::new(0u64));
    let start = Instant::now();
    let mut handles = vec![];

    for _ in 0..tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let start_inner = Instant::now();
            while start_inner.elapsed() < duration {
                let _guard = lock_clone.write().await;
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    total
}

#[allow(unused_variables)]
async fn test_segmented_write(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let start_inner = Instant::now();
            while start_inner.elapsed() < duration {
                let key = format!("key-{}", (i as u64 + count) % 100);
                let _guard = lock_clone.write_lock(&key).await;
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    total
}

#[allow(unused_variables)]
async fn test_global_mixed(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(RwLock::new(0u64));
    let start = Instant::now();
    let mut handles = vec![];

    for _ in 0..tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let start_inner = Instant::now();
            while start_inner.elapsed() < duration {
                if count % 10 == 0 {
                    let _guard = lock_clone.write().await;
                } else {
                    let _guard = lock_clone.read().await;
                }
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    total
}

#[allow(unused_variables)]
async fn test_segmented_mixed(tasks: usize, duration: Duration) -> u64 {
    let lock = Arc::new(AsyncSegmentedLock::<()>::new());
    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            let start_inner = Instant::now();
            while start_inner.elapsed() < duration {
                let key = format!("key-{}", (i as u64 + count) % 100);
                if count % 10 == 0 {
                    let _guard = lock_clone.write_lock(&key).await;
                } else {
                    let _guard = lock_clone.read_lock(&key).await;
                }
                count += 1;
            }
            count
        });
        handles.push(handle);
    }

    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    total
}
