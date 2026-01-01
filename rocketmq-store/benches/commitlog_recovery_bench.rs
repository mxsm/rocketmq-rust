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

//! Performance benchmarks for CommitLog recovery operations
//!
//! Benchmarks the overhead of recovery structures

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log_recovery::RecoveryContext;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics;

/// Benchmark recovery statistics operations
fn bench_recovery_statistics(c: &mut Criterion) {
    c.bench_function("recovery_statistics_clone", |b| {
        let stats = RecoveryStatistics {
            files_processed: 100,
            messages_recovered: 10000,
            bytes_processed: 10 * 1024 * 1024,
            invalid_messages: 5,
            recovery_time_ms: 1000,
        };

        b.iter(|| {
            let cloned = black_box(stats.clone());
            black_box(cloned);
        });
    });

    c.bench_function("recovery_statistics_update", |b| {
        let mut stats = RecoveryStatistics::default();

        b.iter(|| {
            stats.messages_recovered += 1;
            stats.bytes_processed += 256;
            black_box(&stats);
        });
    });
}

/// Benchmark recovery context creation
fn bench_recovery_context(c: &mut Criterion) {
    c.bench_function("recovery_context_creation", |b| {
        let config = Arc::new(MessageStoreConfig::default());
        let delay_table = BTreeMap::new();

        b.iter(|| {
            let ctx = RecoveryContext::new(
                black_box(true),
                black_box(false),
                config.clone(),
                black_box(16),
                delay_table.clone(),
            );
            black_box(ctx);
        });
    });
}

/// Benchmark simulated message processing overhead
fn bench_message_processing_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_processing");

    for msg_count in [100, 500, 1000, 5000].iter() {
        group.bench_function(format!("{}_messages", msg_count), |b| {
            let mut stats = RecoveryStatistics::default();

            b.iter(|| {
                for _ in 0..*msg_count {
                    stats.messages_recovered += 1;
                    stats.bytes_processed += 256;
                }
                black_box(&stats);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_recovery_statistics,
    bench_recovery_context,
    bench_message_processing_overhead
);
criterion_main!(benches);
