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

use rocketmq_namesrv::route::async_segmented_lock::AsyncSegmentedLock;
use tokio::time::Duration;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    println!("=== Async Segmented Lock Simple Test ===\n");

    // Test 1: Basic read lock
    println!("Test 1: Basic read lock");
    let lock = Arc::new(AsyncSegmentedLock::new());
    let guard = lock.read_lock("test-key").await;
    println!("✓ Read lock acquired successfully");
    drop(guard);
    println!("✓ Read lock released\n");

    // Test 2: Basic write lock
    println!("Test 2: Basic write lock");
    let guard = lock.write_lock("test-key").await;
    println!("✓ Write lock acquired successfully");
    drop(guard);
    println!("✓ Write lock released\n");

    // Test 3: Concurrent read locks
    println!("Test 3: Concurrent read locks");
    let start = Instant::now();
    let mut handles = vec![];
    for i in 0..10 {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            let _guard = lock_clone.read_lock("shared-key").await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            i
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let elapsed = start.elapsed();
    println!("✓ 10 concurrent read locks completed in {:?}", elapsed);
    println!("  (Should be ~100ms since reads don't block each other)\n");

    // Test 4: Multiple keys lock
    println!("Test 4: Multiple keys lock");
    let keys = vec!["key1", "key2", "key3"];
    let guards = lock.read_lock_multiple(&keys).await;
    println!("✓ Acquired {} read locks simultaneously", guards.len());
    drop(guards);
    println!("✓ All locks released\n");

    // Test 5: Global lock
    println!("Test 5: Global lock");
    let global_guards = lock.global_read_lock().await;
    println!("✓ Global read lock acquired ({} segments)", global_guards.len());
    drop(global_guards);
    println!("✓ Global lock released\n");

    // Test 6: Performance comparison
    println!("Test 6: Performance comparison");
    test_performance(lock).await;

    println!("\n=== All tests passed! ===");
}

async fn test_performance(lock: Arc<AsyncSegmentedLock>) {
    let num_tasks = 8;
    let operations_per_task = 1000;

    // Test with segmented lock
    let start = Instant::now();
    let mut handles = vec![];
    for i in 0..num_tasks {
        let lock_clone = lock.clone();
        let handle = tokio::spawn(async move {
            for j in 0..operations_per_task {
                let key = format!("key-{}", (i * operations_per_task + j) % 100);
                let _guard = lock_clone.read_lock(&key).await;
                // Simulate work
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let elapsed = start.elapsed();
    let total_ops = num_tasks * operations_per_task;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!("✓ Completed {} operations in {:?}", total_ops, elapsed);
    println!("  ({:.0} ops/sec)", ops_per_sec);
}
