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

//! Connection pool usage example
//!
//! This example demonstrates the ConnectionPool API directly,
//! showing metrics tracking and lifecycle management.

use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_remoting::clients::connection_pool::ConnectionPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RocketMQ Connection Pool Example ===\n");

    // === Example 1: Create a connection pool ===
    println!("Example 1: Create a connection pool");
    let pool: ConnectionPool<()> = ConnectionPool::new(
        1000,                     // max_connections
        Duration::from_secs(300), // max_idle_duration (5 min)
    );

    println!("✓ Connection pool created");
    println!("  - Max connections: 1000");
    println!("  - Idle timeout: 300s\n");

    // === Example 2: Record metrics ===
    println!("Example 2: Record connection metrics");
    let addr1 = CheetahString::from("127.0.0.1:10911");
    let addr2 = CheetahString::from("127.0.0.1:10912");

    // Simulate successful requests
    pool.record_success(&addr1, 10); // 10ms latency
    pool.record_success(&addr1, 15);
    pool.record_success(&addr1, 20);

    // Simulate error
    pool.record_error(&addr2);

    println!("✓ Recorded metrics for {} connections", 2);

    // Get metrics for a connection
    if let Some(metrics) = pool.get_metrics(&addr1) {
        println!("\n📊 Metrics for {}:", addr1);
        println!("  Request count: {}", metrics.request_count());
        println!("  Average latency: {:.2}ms", metrics.avg_latency());
        println!("  Error rate: {:.2}%", metrics.error_rate() * 100.0);
    }

    if let Some(metrics) = pool.get_metrics(&addr2) {
        println!("\n📊 Metrics for {}:", addr2);
        println!("  Consecutive errors: {}", metrics.consecutive_errors());
    }

    // === Example 3: Pool statistics ===
    println!("\nExample 3: Pool-level statistics");
    let stats = pool.stats();

    println!("📊 Pool Statistics:");
    println!("  Total connections: {}", stats.total);
    println!("  Healthy connections: {}", stats.healthy);
    println!("  Active connections: {}", stats.active());
    println!("  Idle connections: {}", stats.idle);
    println!("  Max connections: {}", stats.max_connections);
    println!("  Utilization: {:.1}%", stats.utilization() * 100.0);
    println!("  Total requests: {}", stats.total_requests);
    println!("  Total errors: {}", stats.total_errors);
    if stats.total_requests > 0 {
        println!("  Error rate: {:.2}%", stats.error_rate() * 100.0);
    }

    // === Example 4: Cleanup task ===
    println!("\nExample 4: Background cleanup");

    // Start cleanup task
    let cleanup_task = pool.start_cleanup_task(Duration::from_secs(30));

    println!("✓ Cleanup task started");
    println!("  - Runs every 30 seconds");
    println!("  - Evicts idle connections (>300s)");
    println!("  - Removes unhealthy connections");

    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop cleanup task
    cleanup_task.abort();
    println!("✓ Cleanup task stopped");

    println!("\n=== All examples completed ===");
    Ok(())
}
