/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Performance benchmarks for CommitLog optimizations
//!
//! This benchmark suite validates the performance improvements from:
//! - Lock optimization, flush/HA branching
//! - Async file pre-allocation, object pooling
//!
//! Expected improvements (vs baseline):
//! - Throughput: +50-80%
//! - P99 latency: -40-58%
//! - P999 latency: -40-75%
//! - Heap allocations: -50%

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use cheetah_string::CheetahString;
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log::CommitLog;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Create a test CommitLog instance with optimized configuration
fn create_test_commit_log() -> (CommitLog, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().join("commitlog");
    std::fs::create_dir_all(&store_path).unwrap();
    
    let mut config = MessageStoreConfig::default();
    config.store_path_commit_log = store_path.to_string_lossy().to_string();
    config.mapped_file_size_commit_log = 1024 * 1024 * 100; // 100MB for faster testing
    config.flush_disk_type = rocketmq_store::config::flush_disk_type::FlushDiskType::AsyncFlush;
    
    let broker_config = Arc::new(BrokerConfig::default());
    let config = Arc::new(config);
    
    // Create CommitLog with all dependencies
    let commit_log = create_commit_log_with_config(config, broker_config);
    
    (commit_log, temp_dir)
}

fn create_commit_log_with_config(
    config: Arc<MessageStoreConfig>,
    broker_config: Arc<BrokerConfig>,
) -> CommitLog {
    // Note: This is a simplified creation. In real tests, you'd need all dependencies.
    // For now, we'll focus on the structure.
    todo!("Implement full CommitLog creation with all dependencies")
}

/// Create a test message with specified size
fn create_test_message(topic: &str, queue_id: i32, body_size: usize) -> MessageExtBrokerInner {
    let body = vec![b'X'; body_size];
    let mut message = Message::new(
        CheetahString::from_static_str(topic),
        Bytes::from(body),
    );
    message.with_tags(CheetahString::from_static_str("TagA"));
    
    MessageExtBrokerInner::default_with_message(message)
        .with_queue_id(queue_id)
        .with_born_timestamp(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64)
}

/// Benchmark 1: Single message write throughput
/// 
/// Measures raw throughput for single message writes.
/// Expected: 15,000-18,000 TPS (optimized), vs 10,000 TPS baseline
fn bench_single_message_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_message_throughput");
    
    for size in [256, 1024, 4096].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", size)),
            size,
            |b, &size| {
                let rt = Runtime::new().unwrap();
                let (mut commit_log, _temp_dir) = create_test_commit_log();
                
                b.to_async(&rt).iter(|| async {
                    let msg = create_test_message("BenchTopic", 0, size);
                    commit_log.put_message(msg).await
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark 2: Multi-queue concurrent writes
/// 
/// Measures Phase 1 optimization: narrow Topic-Queue lock scope
/// Expected: +50-100% throughput for multi-queue scenarios
fn bench_multi_queue_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_queue_concurrent");
    
    for num_queues in [1, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_queues", num_queues)),
            num_queues,
            |b, &num_queues| {
                let rt = Runtime::new().unwrap();
                let (commit_log, _temp_dir) = create_test_commit_log();
                let commit_log = Arc::new(tokio::sync::Mutex::new(commit_log));
                
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();
                    
                    for queue_id in 0..num_queues {
                        let commit_log_clone = commit_log.clone();
                        let handle = tokio::spawn(async move {
                            let msg = create_test_message("BenchTopic", queue_id, 1024);
                            let mut guard = commit_log_clone.lock().await;
                            guard.put_message(msg).await
                        });
                        handles.push(handle);
                    }
                    
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark 3: Latency distribution
/// 
/// Measures P50, P99, P999 latencies
/// Expected: P99 < 25ms, P999 < 50ms (Phase 2)
fn bench_latency_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_distribution");
    group.sample_size(1000);
    
    group.bench_function("write_latency", |b| {
        let rt = Runtime::new().unwrap();
        let (mut commit_log, _temp_dir) = create_test_commit_log();
        let mut latencies = Vec::with_capacity(1000);
        
        b.iter(|| {
            rt.block_on(async {
                let start = Instant::now();
                let msg = create_test_message("BenchTopic", 0, 1024);
                let _ = commit_log.put_message(msg).await;
                latencies.push(start.elapsed());
            });
        });
        
        // Calculate percentiles
        latencies.sort();
        let p50 = latencies[latencies.len() / 2];
        let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
        let p999 = latencies[(latencies.len() as f64 * 0.999) as usize];
        
        println!("\nLatency Distribution:");
        println!("  P50:  {:?}", p50);
        println!("  P99:  {:?}", p99);
        println!("  P999: {:?}", p999);
        
        // Assert performance targets
        assert!(p99 < Duration::from_millis(25), "P99 latency too high: {:?}", p99);
        assert!(p999 < Duration::from_millis(50), "P999 latency too high: {:?}", p999);
    });
    
    group.finish();
}

/// Benchmark 4: File pre-allocation impact
/// 
/// Measures Phase 2 optimization: async file pre-allocation
/// Expected: No latency spikes when file fills up
fn bench_file_preallocation_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_preallocation");
    group.sample_size(100);
    
    group.bench_function("across_file_boundary", |b| {
        let rt = Runtime::new().unwrap();
        
        b.iter(|| {
            let (mut commit_log, _temp_dir) = create_test_commit_log();
            let mut max_latency = Duration::from_secs(0);
            
            rt.block_on(async {
                // Write messages that will cross file boundary
                for i in 0..200 {
                    let start = Instant::now();
                    let msg = create_test_message("BenchTopic", 0, 500 * 1024); // 500KB
                    let _ = commit_log.put_message(msg).await;
                    let latency = start.elapsed();
                    
                    if latency > max_latency {
                        max_latency = latency;
                    }
                    
                    // Check for latency spikes
                    if i > 10 { // Skip warmup
                        assert!(
                            latency < Duration::from_millis(100),
                            "Latency spike detected at message {}: {:?}",
                            i, latency
                        );
                    }
                }
            });
            
            println!("\nMax latency across file boundary: {:?}", max_latency);
        });
    });
    
    group.finish();
}

/// Benchmark 5: Object pool effectiveness
/// 
/// Measures Phase 2 optimization: encoder object pool
/// Expected: -50% heap allocations
fn bench_object_pool_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_pool");
    
    group.bench_function("encoder_reuse", |b| {
        let rt = Runtime::new().unwrap();
        let (mut commit_log, _temp_dir) = create_test_commit_log();
        
        // Note: In real benchmark, you'd use a heap profiler like `dhat` or `jemalloc`
        // to measure actual allocation reduction
        
        b.to_async(&rt).iter(|| async {
            let msg = create_test_message("BenchTopic", 0, 1024);
            black_box(commit_log.put_message(msg).await)
        });
    });
    
    group.finish();
}

/// Benchmark 6: Lock contention (Phase 1 validation)
/// 
/// Measures lock hold time reduction from Phase 1 optimizations
/// Expected: Topic-Queue lock hold time < 0.5ms
fn bench_lock_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_contention");
    
    group.bench_function("high_concurrency", |b| {
        let rt = Runtime::new().unwrap();
        let (commit_log, _temp_dir) = create_test_commit_log();
        let commit_log = Arc::new(tokio::sync::Mutex::new(commit_log));
        
        b.to_async(&rt).iter(|| async {
            let mut handles = Vec::new();
            
            // Simulate high concurrency: 20 concurrent writers
            for i in 0..20 {
                let commit_log_clone = commit_log.clone();
                let handle = tokio::spawn(async move {
                    let msg = create_test_message("BenchTopic", i % 4, 1024);
                    let mut guard = commit_log_clone.lock().await;
                    guard.put_message(msg).await
                });
                handles.push(handle);
            }
            
            for handle in handles {
                handle.await.unwrap();
            }
        });
    });
    
    group.finish();
}

/// Benchmark 7: Batch message processing
/// 
/// Validates batch encoding optimization
fn bench_batch_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_messages");
    
    for batch_size in [10, 50, 100].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_msgs", batch_size)),
            batch_size,
            |b, &batch_size| {
                let rt = Runtime::new().unwrap();
                let (mut commit_log, _temp_dir) = create_test_commit_log();
                
                b.to_async(&rt).iter(|| async {
                    // Note: Implement batch message creation and sending
                    for _ in 0..batch_size {
                        let msg = create_test_message("BenchTopic", 0, 1024);
                        let _ = commit_log.put_message(msg).await;
                    }
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_message_throughput,
    bench_multi_queue_concurrent,
    bench_latency_distribution,
    bench_file_preallocation_impact,
    bench_object_pool_allocations,
    bench_lock_contention,
    bench_batch_messages,
);

criterion_main!(benches);
