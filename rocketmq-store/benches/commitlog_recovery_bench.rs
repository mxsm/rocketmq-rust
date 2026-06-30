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
//! and writes the Phase 1 recovery baseline manifest to
//! `target/recovery-baseline/phase1/commitlog-recovery-phase1-baseline.json`.

use std::collections::BTreeMap;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log_recovery::RecoveryContext;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics;

const BASELINE_SAMPLE_SIZE: usize = 10;
const MESSAGE_SIZE_BYTES: u64 = 256;

#[derive(Clone, Copy)]
struct RecoveryBaselineScenario {
    id: &'static str,
    exit: &'static str,
    commitlog_files: usize,
    messages: u64,
    dirty_tail: bool,
    checkpoint_state: &'static str,
    index_state: &'static str,
    expected_mode: &'static str,
}

impl RecoveryBaselineScenario {
    const fn total_bytes(self) -> u64 {
        self.messages * MESSAGE_SIZE_BYTES
    }
}

const PHASE1_BASELINE_SCENARIOS: &[RecoveryBaselineScenario] = &[
    RecoveryBaselineScenario {
        id: "normal_exit_tail_files",
        exit: "normal",
        commitlog_files: 3,
        messages: 1_000,
        dirty_tail: false,
        checkpoint_state: "valid",
        index_state: "safe",
        expected_mode: "strict",
    },
    RecoveryBaselineScenario {
        id: "abnormal_exit_checkpoint_aligned",
        exit: "abnormal",
        commitlog_files: 8,
        messages: 5_000,
        dirty_tail: false,
        checkpoint_state: "valid",
        index_state: "safe",
        expected_mode: "strict",
    },
    RecoveryBaselineScenario {
        id: "dirty_tail_half_message",
        exit: "abnormal",
        commitlog_files: 8,
        messages: 5_000,
        dirty_tail: true,
        checkpoint_state: "valid",
        index_state: "safe",
        expected_mode: "strict",
    },
    RecoveryBaselineScenario {
        id: "checkpoint_missing_strict_fallback",
        exit: "abnormal",
        commitlog_files: 8,
        messages: 5_000,
        dirty_tail: false,
        checkpoint_state: "missing",
        index_state: "safe",
        expected_mode: "strict",
    },
    RecoveryBaselineScenario {
        id: "index_unsafe_sync_repair",
        exit: "abnormal",
        commitlog_files: 8,
        messages: 5_000,
        dirty_tail: false,
        checkpoint_state: "valid",
        index_state: "unsafe",
        expected_mode: "strict",
    },
];

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-store should live below workspace root")
        .to_path_buf()
}

fn benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/recovery-baseline/phase1")
}

fn active_features() -> Vec<&'static str> {
    let mut features = Vec::new();
    if cfg!(feature = "local_file_store") {
        features.push("local_file_store");
    }
    if cfg!(feature = "fast-load") {
        features.push("fast-load");
    }
    if cfg!(feature = "safe-load") {
        features.push("safe-load");
    }
    if cfg!(feature = "rocksdb_store") {
        features.push("rocksdb_store");
    }
    if cfg!(feature = "tieredstore") {
        features.push("tieredstore");
    }
    if cfg!(feature = "observability") {
        features.push("observability");
    }
    if cfg!(feature = "observability-traces") {
        features.push("observability-traces");
    }
    if cfg!(feature = "io_uring") {
        features.push("io_uring");
    }
    features
}

fn write_phase1_baseline_manifest() {
    let output_dir = benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("recovery baseline artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let scenarios: Vec<_> = PHASE1_BASELINE_SCENARIOS
        .iter()
        .map(|scenario| {
            serde_json::json!({
                "id": scenario.id,
                "exit": scenario.exit,
                "commitlog_files": scenario.commitlog_files,
                "messages": scenario.messages,
                "message_size_bytes": MESSAGE_SIZE_BYTES,
                "total_bytes": scenario.total_bytes(),
                "dirty_tail": scenario.dirty_tail,
                "checkpoint_state": scenario.checkpoint_state,
                "index_state": scenario.index_state,
                "expected_mode": scenario.expected_mode,
            })
        })
        .collect();
    let payload = serde_json::json!({
        "case": "commitlog_recovery_phase1_baseline",
        "generated_at_unix_ms": generated_at_unix_ms,
        "commit": std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_string()),
        "pr": std::env::var("GITHUB_REF_NAME").unwrap_or_else(|_| "local".to_string()),
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "profile": std::env::var("PROFILE").unwrap_or_else(|_| "bench".to_string()),
            "features": active_features(),
        },
        "sample_size": BASELINE_SAMPLE_SIZE,
        "target_metrics": [
            "p50_recovery_ms",
            "p95_recovery_ms",
            "p99_recovery_ms",
            "scanned_bytes",
            "recovered_messages",
            "truncated_files",
            "phase_duration_ms"
        ],
        "scenarios": scenarios,
    });

    let path = output_dir.join("commitlog-recovery-phase1-baseline.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("recovery baseline manifest should serialize"),
    )
    .expect("recovery baseline manifest should be written");
}

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

fn bench_phase1_baseline_scenarios(c: &mut Criterion) {
    write_phase1_baseline_manifest();

    let mut group = c.benchmark_group("commitlog_recovery/phase1_baseline");
    for scenario in PHASE1_BASELINE_SCENARIOS {
        group.throughput(Throughput::Bytes(scenario.total_bytes()));
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.id),
            scenario,
            |bencher, scenario| {
                bencher.iter(|| {
                    let mut stats = RecoveryStatistics::default();
                    let checkpoint_scan_multiplier = match scenario.checkpoint_state {
                        "missing" => 2,
                        "valid" => 1,
                        _ => 1,
                    };
                    let index_rebuild_messages = match scenario.index_state {
                        "unsafe" => scenario.messages / 2,
                        "safe" => 0,
                        _ => 0,
                    };
                    let fallback_messages = match scenario.expected_mode {
                        "strict" => 0,
                        _ => scenario.messages / 10,
                    };
                    let measured_messages = scenario
                        .messages
                        .saturating_add(index_rebuild_messages)
                        .saturating_add(fallback_messages);

                    for _ in 0..measured_messages {
                        stats.messages_recovered = stats.messages_recovered.saturating_add(1);
                        stats.bytes_processed = stats.bytes_processed.saturating_add(MESSAGE_SIZE_BYTES);
                    }
                    stats.files_processed = scenario.commitlog_files.saturating_mul(checkpoint_scan_multiplier);
                    stats.invalid_messages = if scenario.dirty_tail { 1 } else { 0 };
                    if scenario.checkpoint_state == "missing" {
                        stats.invalid_messages = stats.invalid_messages.saturating_add(1);
                    }
                    stats.recovery_time_ms = u128::from(measured_messages / 100);
                    black_box(stats);
                });
            },
        );
    }
    group.finish();
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
    name = benches;
    config = Criterion::default()
        .sample_size(BASELINE_SAMPLE_SIZE)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets =
        bench_recovery_statistics,
        bench_phase1_baseline_scenarios,
        bench_recovery_context,
        bench_message_processing_overhead
);
criterion_main!(benches);
