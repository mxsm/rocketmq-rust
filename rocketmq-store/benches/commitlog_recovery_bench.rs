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
//! and writes recovery manifests under `target/recovery-baseline/`.

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
use rocketmq_store::config::message_store_config::bounded_local_file_consume_queue_recovery_parallelism;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store::log_file::commit_log_recovery::plan_abnormal_recovery_window_from_ranges;
use rocketmq_store::log_file::commit_log_recovery::AbnormalRecoveryFileRange;
use rocketmq_store::log_file::commit_log_recovery::RecoveryContext;
use rocketmq_store::log_file::commit_log_recovery::RecoveryStatistics;

const BASELINE_SAMPLE_SIZE: usize = 10;
const MESSAGE_SIZE_BYTES: u64 = 256;
const PHASE2_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const PHASE3_CQ_FILE_SIZE_BYTES: u64 = 30 * 1024 * 1024;
const PHASE3_CQ_RECORD_SIZE_BYTES: u64 = 20;
const PHASE3_NON_CQ_STARTUP_OVERHEAD_MS: u128 = 250;

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

#[derive(Clone, Copy)]
struct RecoveryWindowComparisonScenario {
    id: &'static str,
    commitlog_files: usize,
    checkpoint_index: Option<usize>,
    max_recovery_commit_log_files: usize,
    dispatch_progress_file_index: usize,
    confirm_offset_file_index: usize,
}

impl RecoveryWindowComparisonScenario {
    const fn commit_log_max_offset(self) -> i64 {
        (self.commitlog_files as i64) * (PHASE2_FILE_SIZE_BYTES as i64)
    }

    const fn strict_scanned_files(self) -> usize {
        self.commitlog_files
    }

    const fn strict_scanned_bytes(self) -> u64 {
        (self.commitlog_files as u64) * PHASE2_FILE_SIZE_BYTES
    }

    fn file_ranges(self) -> Vec<AbnormalRecoveryFileRange> {
        (0..self.commitlog_files)
            .map(|index| {
                AbnormalRecoveryFileRange::new((index as i64) * (PHASE2_FILE_SIZE_BYTES as i64), PHASE2_FILE_SIZE_BYTES)
            })
            .collect()
    }

    fn offset_for_file(self, index: usize) -> i64 {
        let bounded_index = index.min(self.commitlog_files.saturating_sub(1));
        (bounded_index as i64) * (PHASE2_FILE_SIZE_BYTES as i64)
    }
}

const PHASE2_WINDOW_SCENARIOS: &[RecoveryWindowComparisonScenario] = &[
    RecoveryWindowComparisonScenario {
        id: "checkpoint_aligned_bounded_window",
        commitlog_files: 64,
        checkpoint_index: Some(60),
        max_recovery_commit_log_files: 3,
        dispatch_progress_file_index: 60,
        confirm_offset_file_index: 61,
    },
    RecoveryWindowComparisonScenario {
        id: "dispatch_progress_lagging_expansion",
        commitlog_files: 64,
        checkpoint_index: Some(60),
        max_recovery_commit_log_files: 3,
        dispatch_progress_file_index: 20,
        confirm_offset_file_index: 61,
    },
    RecoveryWindowComparisonScenario {
        id: "confirm_offset_lagging_expansion",
        commitlog_files: 64,
        checkpoint_index: Some(60),
        max_recovery_commit_log_files: 3,
        dispatch_progress_file_index: 62,
        confirm_offset_file_index: 10,
    },
    RecoveryWindowComparisonScenario {
        id: "checkpoint_missing_strict_fallback",
        commitlog_files: 64,
        checkpoint_index: None,
        max_recovery_commit_log_files: 3,
        dispatch_progress_file_index: 60,
        confirm_offset_file_index: 61,
    },
];

#[derive(Clone, Copy)]
struct Phase3CqRecoveryScenario {
    id: &'static str,
    topic_count: usize,
    queue_count: usize,
    mapped_files_per_queue: usize,
    records_per_file: usize,
    empty_queue_count: usize,
    configured_parallelism: usize,
    available_parallelism_reference: usize,
}

impl Phase3CqRecoveryScenario {
    fn active_queue_count(self) -> usize {
        self.queue_count.saturating_sub(self.empty_queue_count)
    }

    fn effective_parallelism(self) -> usize {
        bounded_local_file_consume_queue_recovery_parallelism(
            self.configured_parallelism,
            self.available_parallelism_reference,
        )
        .max(1)
        .min(self.queue_count.max(1))
    }

    fn records_per_queue(self) -> usize {
        self.mapped_files_per_queue.saturating_mul(self.records_per_file)
    }

    fn serial_work_units(self) -> usize {
        self.active_queue_count().saturating_mul(self.records_per_queue())
    }

    fn concurrent_recovery_batches(self) -> usize {
        let parallelism = self.effective_parallelism();
        self.active_queue_count().saturating_add(parallelism - 1) / parallelism
    }

    fn concurrent_work_units(self) -> usize {
        self.concurrent_recovery_batches()
            .saturating_mul(self.records_per_queue())
    }

    fn total_cq_bytes(self) -> u64 {
        (self.active_queue_count() as u64)
            .saturating_mul(self.mapped_files_per_queue as u64)
            .saturating_mul(PHASE3_CQ_FILE_SIZE_BYTES)
    }

    fn serial_cq_recovery_ms(self) -> u128 {
        (self.serial_work_units() as u128).saturating_div(10_000).max(1)
    }

    fn concurrent_cq_recovery_ms(self) -> u128 {
        (self.concurrent_work_units() as u128).saturating_div(10_000).max(1)
    }

    fn startup_reduction_ratio(self) -> f64 {
        let serial = self.serial_cq_recovery_ms() as f64;
        if serial == 0.0 {
            0.0
        } else {
            1.0 - (self.concurrent_cq_recovery_ms() as f64 / serial)
        }
    }

    fn serial_total_startup_ms(self) -> u128 {
        PHASE3_NON_CQ_STARTUP_OVERHEAD_MS.saturating_add(self.serial_cq_recovery_ms())
    }

    fn concurrent_total_startup_ms(self) -> u128 {
        PHASE3_NON_CQ_STARTUP_OVERHEAD_MS.saturating_add(self.concurrent_cq_recovery_ms())
    }

    fn total_startup_reduction_ratio(self) -> f64 {
        let serial = self.serial_total_startup_ms() as f64;
        if serial == 0.0 {
            0.0
        } else {
            1.0 - (self.concurrent_total_startup_ms() as f64 / serial)
        }
    }

    fn concurrency_utilization(self) -> f64 {
        let active = self.active_queue_count();
        if active == 0 {
            return 0.0;
        }
        let parallelism = self.effective_parallelism();
        let batches = self.concurrent_recovery_batches().max(1);
        active as f64 / (parallelism.saturating_mul(batches)) as f64
    }

    fn estimated_cpu_utilization(self, detected_parallelism: usize) -> f64 {
        let detected_parallelism = detected_parallelism.max(1);
        let effective_parallelism = self.effective_parallelism().min(detected_parallelism);
        (effective_parallelism as f64 / detected_parallelism as f64) * self.concurrency_utilization()
    }
}

const PHASE3_CQ_RECOVERY_SCENARIOS: &[Phase3CqRecoveryScenario] = &[
    Phase3CqRecoveryScenario {
        id: "many_queues_exceed_parallelism",
        topic_count: 32,
        queue_count: 512,
        mapped_files_per_queue: 3,
        records_per_file: 128,
        empty_queue_count: 0,
        configured_parallelism: 8,
        available_parallelism_reference: 16,
    },
    Phase3CqRecoveryScenario {
        id: "few_large_queues",
        topic_count: 2,
        queue_count: 8,
        mapped_files_per_queue: 64,
        records_per_file: 256,
        empty_queue_count: 0,
        configured_parallelism: 8,
        available_parallelism_reference: 16,
    },
    Phase3CqRecoveryScenario {
        id: "many_empty_queues",
        topic_count: 64,
        queue_count: 1024,
        mapped_files_per_queue: 1,
        records_per_file: 32,
        empty_queue_count: 896,
        configured_parallelism: 8,
        available_parallelism_reference: 16,
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

fn phase2_benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/recovery-baseline/phase2")
}

fn phase3_benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/recovery-baseline/phase3")
}

fn phase5_benchmark_artifact_dir() -> PathBuf {
    workspace_root().join("target/recovery-baseline/phase5")
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

fn phase2_window_for_scenario(
    scenario: RecoveryWindowComparisonScenario,
) -> rocketmq_store::log_file::commit_log_recovery::AbnormalRecoveryWindow {
    let file_ranges = scenario.file_ranges();
    plan_abnormal_recovery_window_from_ranges(
        &file_ranges,
        scenario.checkpoint_index,
        scenario.max_recovery_commit_log_files,
        scenario.offset_for_file(scenario.dispatch_progress_file_index),
        scenario.offset_for_file(scenario.confirm_offset_file_index),
        0,
        scenario.commit_log_max_offset(),
    )
}

fn write_phase2_window_comparison_manifest() {
    let output_dir = phase2_benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("phase2 recovery benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let scenarios: Vec<_> = PHASE2_WINDOW_SCENARIOS
        .iter()
        .map(|scenario| {
            let window = phase2_window_for_scenario(*scenario);
            let strict_scanned_bytes = scenario.strict_scanned_bytes();
            let bytes_reduction_ratio = if strict_scanned_bytes == 0 {
                0.0
            } else {
                1.0 - (window.scanned_bytes as f64 / strict_scanned_bytes as f64)
            };

            serde_json::json!({
                "id": scenario.id,
                "commitlog_files": scenario.commitlog_files,
                "file_size_bytes": PHASE2_FILE_SIZE_BYTES,
                "checkpoint_index": scenario.checkpoint_index,
                "max_recovery_commit_log_files": scenario.max_recovery_commit_log_files,
                "dispatch_progress_file_index": scenario.dispatch_progress_file_index,
                "confirm_offset_file_index": scenario.confirm_offset_file_index,
                "strict_scanned_files": scenario.strict_scanned_files(),
                "strict_scanned_bytes": strict_scanned_bytes,
                "window_start_index": window.start_index,
                "window_checkpoint_index": window.checkpoint_index,
                "window_dispatch_progress_index": window.dispatch_progress_index,
                "window_confirm_offset_index": window.confirm_offset_index,
                "window_scanned_files": window.scanned_file_count,
                "window_scanned_bytes": window.scanned_bytes,
                "window_expanded_files": window.expanded_files,
                "window_end_offset": window.end_offset,
                "fallback_reason": window.fallback_reason,
                "bytes_reduction_ratio": bytes_reduction_ratio,
            })
        })
        .collect();
    let payload = serde_json::json!({
        "case": "commitlog_recovery_phase2_window_comparison",
        "generated_at_unix_ms": generated_at_unix_ms,
        "commit": std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_string()),
        "pr": std::env::var("GITHUB_REF_NAME").unwrap_or_else(|_| "local".to_string()),
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "profile": std::env::var("PROFILE").unwrap_or_else(|_| "bench".to_string()),
            "features": active_features(),
        },
        "baseline": "target/recovery-baseline/phase1/commitlog-recovery-phase1-baseline.json",
        "target_metrics": [
            "strict_scanned_bytes",
            "window_scanned_bytes",
            "bytes_reduction_ratio",
            "window_scanned_files",
            "fallback_reason"
        ],
        "scenarios": scenarios,
    });

    let path = output_dir.join("commitlog-recovery-phase2-window-comparison.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("phase2 recovery benchmark manifest should serialize"),
    )
    .expect("phase2 recovery benchmark manifest should be written");
}

fn write_phase3_cq_concurrency_manifest() {
    let output_dir = phase3_benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("phase3 recovery benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let detected_parallelism = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1);
    let scenarios: Vec<_> = PHASE3_CQ_RECOVERY_SCENARIOS
        .iter()
        .map(|scenario| {
            let active_queues = scenario.active_queue_count();
            let serial_ms = scenario.serial_cq_recovery_ms();
            let concurrent_ms = scenario.concurrent_cq_recovery_ms();
            let total_cq_bytes = scenario.total_cq_bytes();
            let serial_throughput_bytes_per_ms = if serial_ms == 0 {
                0.0
            } else {
                total_cq_bytes as f64 / serial_ms as f64
            };
            let concurrent_throughput_bytes_per_ms = if concurrent_ms == 0 {
                0.0
            } else {
                total_cq_bytes as f64 / concurrent_ms as f64
            };
            let throughput_increase_ratio = if serial_throughput_bytes_per_ms == 0.0 {
                0.0
            } else {
                (concurrent_throughput_bytes_per_ms / serial_throughput_bytes_per_ms) - 1.0
            };

            serde_json::json!({
                "id": scenario.id,
                "topic_count": scenario.topic_count,
                "queue_count": scenario.queue_count,
                "active_queue_count": active_queues,
                "empty_queue_count": scenario.empty_queue_count,
                "mapped_files_per_queue": scenario.mapped_files_per_queue,
                "cq_file_size_bytes": PHASE3_CQ_FILE_SIZE_BYTES,
                "records_per_file": scenario.records_per_file,
                "cq_record_size_bytes": PHASE3_CQ_RECORD_SIZE_BYTES,
                "configured_parallelism": scenario.configured_parallelism,
                "available_parallelism_reference": scenario.available_parallelism_reference,
                "detected_parallelism": detected_parallelism,
                "effective_parallelism": scenario.effective_parallelism(),
                "serial_work_units": scenario.serial_work_units(),
                "bounded_concurrent_work_units": scenario.concurrent_work_units(),
                "bounded_concurrent_batches": scenario.concurrent_recovery_batches(),
                "estimated_serial_cq_recovery_ms": serial_ms,
                "estimated_concurrent_cq_recovery_ms": concurrent_ms,
                "estimated_cq_stage_reduction_ratio": scenario.startup_reduction_ratio(),
                "estimated_phase2_total_startup_ms_reference": scenario.serial_total_startup_ms(),
                "estimated_phase3_total_startup_ms": scenario.concurrent_total_startup_ms(),
                "estimated_total_startup_reduction_ratio": scenario.total_startup_reduction_ratio(),
                "estimated_serial_throughput_bytes_per_ms": serial_throughput_bytes_per_ms,
                "estimated_concurrent_throughput_bytes_per_ms": concurrent_throughput_bytes_per_ms,
                "estimated_disk_throughput_increase_ratio": throughput_increase_ratio,
                "estimated_cpu_core_utilization_ratio": scenario.estimated_cpu_utilization(detected_parallelism),
                "concurrency_utilization": scenario.concurrency_utilization(),
            })
        })
        .collect();

    let payload = serde_json::json!({
        "case": "commitlog_recovery_phase3_cq_concurrency",
        "generated_at_unix_ms": generated_at_unix_ms,
        "commit": std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_string()),
        "pr": std::env::var("GITHUB_REF_NAME").unwrap_or_else(|_| "local".to_string()),
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "profile": std::env::var("PROFILE").unwrap_or_else(|_| "bench".to_string()),
            "features": active_features(),
            "detected_parallelism": detected_parallelism,
        },
        "baseline": {
            "phase1": "target/recovery-baseline/phase1/commitlog-recovery-phase1-baseline.json",
            "phase2": "target/recovery-baseline/phase2/commitlog-recovery-phase2-window-comparison.json",
            "phase2_total_startup_model": "phase2_total_startup_ms_reference = non_cq_startup_overhead_ms + serial_cq_recovery_ms",
            "non_cq_startup_overhead_ms": PHASE3_NON_CQ_STARTUP_OVERHEAD_MS,
        },
        "default_behavior": {
            "local_file_consume_queue_recovery_concurrently": false,
            "rollout_guidance": "Keep default disabled until many-queue workloads show stable CQ recovery reduction without disk saturation.",
            "recommended_gray_threshold": "Enable only for brokers with queue_count > effective_parallelism and measured disk utilization below saturation.",
        },
        "target_metrics": [
            "estimated_serial_cq_recovery_ms",
            "estimated_concurrent_cq_recovery_ms",
            "estimated_cq_stage_reduction_ratio",
            "estimated_phase2_total_startup_ms_reference",
            "estimated_phase3_total_startup_ms",
            "estimated_total_startup_reduction_ratio",
            "estimated_serial_throughput_bytes_per_ms",
            "estimated_concurrent_throughput_bytes_per_ms",
            "estimated_disk_throughput_increase_ratio",
            "estimated_cpu_core_utilization_ratio",
            "configured_parallelism",
            "effective_parallelism",
            "concurrency_utilization"
        ],
        "scenarios": scenarios,
    });

    let path = output_dir.join("commitlog-recovery-phase3-cq-concurrency.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("phase3 recovery benchmark manifest should serialize"),
    )
    .expect("phase3 recovery benchmark manifest should be written");
}

fn write_phase5_platform_acceptance_manifest() {
    let output_dir = phase5_benchmark_artifact_dir();
    fs::create_dir_all(&output_dir).expect("phase5 platform acceptance artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let report = rocketmq_store::bench_support::phase5_platform_optimization_acceptance_report();
    let payload = serde_json::json!({
        "case": "commitlog_recovery_phase5_platform_acceptance",
        "generated_at_unix_ms": generated_at_unix_ms,
        "commit": std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_string()),
        "pr": std::env::var("GITHUB_REF_NAME").unwrap_or_else(|_| "local".to_string()),
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "profile": std::env::var("PROFILE").unwrap_or_else(|_| "bench".to_string()),
            "features": active_features(),
        },
        "baseline": {
            "phase1": "target/recovery-baseline/phase1/commitlog-recovery-phase1-baseline.json",
            "phase2": "target/recovery-baseline/phase2/commitlog-recovery-phase2-window-comparison.json",
            "phase3": "target/recovery-baseline/phase3/commitlog-recovery-phase3-cq-concurrency.json"
        },
        "report": report,
    });

    let path = output_dir.join("commitlog-recovery-phase5-platform-acceptance.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("phase5 platform acceptance manifest should serialize"),
    )
    .expect("phase5 platform acceptance manifest should be written");
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

fn bench_phase2_window_comparison(c: &mut Criterion) {
    write_phase2_window_comparison_manifest();

    let mut group = c.benchmark_group("commitlog_recovery/phase2_window_comparison");
    for scenario in PHASE2_WINDOW_SCENARIOS {
        let window = phase2_window_for_scenario(*scenario);
        group.throughput(Throughput::Bytes(window.scanned_bytes));
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.id),
            scenario,
            |bencher, scenario| {
                bencher.iter(|| {
                    let window = phase2_window_for_scenario(*scenario);
                    let mut stats = RecoveryStatistics {
                        files_processed: window.scanned_file_count,
                        bytes_processed: window.scanned_bytes,
                        invalid_messages: u64::from(window.fallback_reason.is_some()),
                        ..Default::default()
                    };

                    let work_units = window.scanned_file_count.max(1) * 64;
                    for index in 0..work_units {
                        stats.messages_recovered = stats.messages_recovered.saturating_add(1);
                        black_box(index);
                    }
                    black_box(stats);
                });
            },
        );
    }
    group.finish();
}

fn simulate_phase3_serial_cq_recovery(scenario: Phase3CqRecoveryScenario) -> RecoveryStatistics {
    let mut stats = RecoveryStatistics::default();
    for queue_index in 0..scenario.active_queue_count() {
        for record_index in 0..scenario.records_per_queue() {
            stats.messages_recovered = stats.messages_recovered.saturating_add(1);
            stats.bytes_processed = stats.bytes_processed.saturating_add(PHASE3_CQ_RECORD_SIZE_BYTES);
            black_box((queue_index, record_index));
        }
    }
    stats.files_processed = scenario.active_queue_count() * scenario.mapped_files_per_queue;
    stats.recovery_time_ms = scenario.serial_cq_recovery_ms();
    stats
}

fn simulate_phase3_bounded_cq_recovery(scenario: Phase3CqRecoveryScenario) -> RecoveryStatistics {
    let mut stats = RecoveryStatistics::default();
    let active_queues = scenario.active_queue_count();
    let parallelism = scenario.effective_parallelism();
    let batches = scenario.concurrent_recovery_batches();
    for batch_index in 0..batches {
        let batch_start = batch_index * parallelism;
        let batch_end = batch_start.saturating_add(parallelism).min(active_queues);
        for record_index in 0..scenario.records_per_queue() {
            for queue_index in batch_start..batch_end {
                stats.messages_recovered = stats.messages_recovered.saturating_add(1);
                stats.bytes_processed = stats.bytes_processed.saturating_add(PHASE3_CQ_RECORD_SIZE_BYTES);
                black_box((batch_index, queue_index, record_index));
            }
        }
    }
    stats.files_processed = active_queues * scenario.mapped_files_per_queue;
    stats.recovery_time_ms = scenario.concurrent_cq_recovery_ms();
    stats
}

fn bench_phase3_cq_concurrency_comparison(c: &mut Criterion) {
    write_phase3_cq_concurrency_manifest();

    let mut group = c.benchmark_group("commitlog_recovery/phase3_cq_concurrency");
    for scenario in PHASE3_CQ_RECOVERY_SCENARIOS {
        group.throughput(Throughput::Bytes(scenario.total_cq_bytes()));
        group.bench_with_input(
            BenchmarkId::new("serial", scenario.id),
            scenario,
            |bencher, scenario| {
                bencher.iter(|| {
                    black_box(simulate_phase3_serial_cq_recovery(*scenario));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("bounded_concurrent", scenario.id),
            scenario,
            |bencher, scenario| {
                bencher.iter(|| {
                    black_box(simulate_phase3_bounded_cq_recovery(*scenario));
                });
            },
        );
    }
    group.finish();
}

fn bench_phase5_platform_acceptance(c: &mut Criterion) {
    write_phase5_platform_acceptance_manifest();

    let report = rocketmq_store::bench_support::phase5_platform_optimization_acceptance_report();
    let mut group = c.benchmark_group("commitlog_recovery/phase5_platform_acceptance");
    for scenario in &report.scenarios {
        group.bench_with_input(
            BenchmarkId::from_parameter(scenario.id),
            scenario,
            |bencher, scenario| {
                bencher.iter(|| {
                    black_box((
                        scenario.platform,
                        scenario.storage_medium,
                        scenario.page_cache_state,
                        scenario.default_enabled,
                    ));
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
        bench_phase2_window_comparison,
        bench_phase3_cq_concurrency_comparison,
        bench_phase5_platform_acceptance,
        bench_recovery_context,
        bench_message_processing_overhead
);
criterion_main!(benches);
