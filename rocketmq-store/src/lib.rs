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

//! Storage capabilities are exposed through the crate root. Implementation
//! modules remain private so callers depend on intentional contracts.
//!
//! ```
//! use rocketmq_store::LocalFileMessageStore;
//! use rocketmq_store_local::message_store::local_file_message_store::LocalStoreComposition;
//!
//! let _ = std::mem::size_of::<LocalFileMessageStore>();
//! let _ = std::mem::size_of::<LocalStoreComposition>();
//! ```

#![allow(dead_code)]
#![allow(unused_variables)]

mod base;
mod capability;
mod config;
mod consume_queue;
mod factory;
mod filter;
mod ha;
mod hook;
mod index;
mod inspection;
mod kv;
mod log_file;
pub(crate) mod message_encoder;
mod message_store;
mod platform;
mod pop;
mod public_api;
mod queue;
#[cfg(feature = "rocksdb_store")]
mod rocksdb;
pub(crate) mod runtime;
mod stats;
mod store;
mod store_error;
mod store_path_config_helper;
mod store_ports;
mod telemetry;
#[cfg(feature = "tieredstore")]
mod tieredstore;
mod timer;
mod transfer;
mod utils;

pub use public_api::*;

// Backend implementations and repository-internal test fakes use this sealed
// implementation contract. Application code receives narrow capabilities or
// the concrete `StorePorts` composition root.

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[cfg(test)]
mod test_support_tests {
    use std::time::Duration;

    #[test]
    fn store_runtime_scope_parents_blocking_executor() {
        let owner = rocketmq_runtime::RuntimeOwner::plan(rocketmq_runtime::RuntimeConfig::server_default(
            "store-runtime-scope-test",
        ))
        .expect("runtime configuration is valid")
        .build()
        .expect("store runtime scope test owner should start");
        let service = owner.root_context().component("store-service");
        let scope = super::runtime::StoreRuntimeScope::new(service.clone());

        owner.block_on(async {
            let value = scope
                .spawn_io("store.parented.blocking", || 7usize)
                .await
                .expect("parented store blocking task should complete");
            assert_eq!(value, 7);
            assert_eq!(scope.blocking_snapshot().blocking_still_running, 0);
            let child_group = scope.task_group("rocketmq-store.parented.child");
            assert_eq!(child_group.parent_id(), Some(service.task_group().id()));

            let report = service.task_group().shutdown(Duration::from_secs(1)).await;
            assert!(report.is_healthy(), "{}", report.to_json());
        });

        let report = owner
            .shutdown_runtime_blocking()
            .expect("store runtime scope test owner should stop");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[cfg(feature = "rocksdb_store")]
    #[test]
    fn rocksdb_runtime_scope_parents_blocking_executor() {
        let owner = rocketmq_runtime::RuntimeOwner::plan(rocketmq_runtime::RuntimeConfig::server_default(
            "rocksdb-runtime-scope-test",
        ))
        .expect("runtime configuration is valid")
        .build()
        .expect("RocksDB runtime scope test owner should start");
        let service = owner.root_context().component("rocksdb-service");
        let scope = super::rocksdb::runtime::RocksDbRuntimeScope::new(service.clone());

        owner.block_on(async {
            let value = scope
                .spawn_io(
                    "rocksdb.parented.blocking",
                    rocketmq_store_api::StoreOperation::Read,
                    || 11usize,
                )
                .await
                .expect("parented rocksdb blocking task should complete");
            assert_eq!(value, 11);
            assert_eq!(scope.blocking_snapshot().blocking_still_running, 0);

            let child_group = scope.task_group("rocksdb.parented.child");
            assert_eq!(child_group.parent_id(), Some(service.task_group().id()));

            let report = service.task_group().shutdown(Duration::from_secs(1)).await;
            assert!(report.is_healthy(), "{}", report.to_json());
        });

        let report = owner
            .shutdown_runtime_blocking()
            .expect("RocksDB runtime scope test owner should stop");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn store_blocking_io_probe_reports_no_running_tasks() {
        let owner = rocketmq_runtime::RuntimeOwner::plan(rocketmq_runtime::RuntimeConfig::server_default(
            "store-blocking-probe",
        ))
        .expect("runtime configuration is valid")
        .build()
        .expect("store blocking probe owner should start");
        let probe = owner.block_on(super::test_support::run_store_blocking_io_probe(
            owner.root_context().component("store-service"),
            4,
            Duration::from_millis(1),
        ));

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.snapshot.blocking_still_running, 0, "{probe:?}");
        assert!(probe.snapshot.tasks.is_empty(), "{probe:?}");

        let report = owner
            .shutdown_runtime_blocking()
            .expect("store blocking probe owner should stop");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[test]
    fn store_blocking_io_probe_waits_for_concurrent_store_tasks() {
        let owner = rocketmq_runtime::RuntimeOwner::plan(rocketmq_runtime::RuntimeConfig::server_default(
            "store-blocking-concurrency-probe",
        ))
        .expect("runtime configuration is valid")
        .build()
        .expect("store blocking concurrency probe owner should start");
        let service_context = owner.root_context().component("store-service");
        let probe = owner.block_on(async {
            let runtime_scope = super::runtime::StoreRuntimeScope::new(service_context.clone());
            let (running_tx, running_rx) = tokio::sync::oneshot::channel();
            let in_flight_scope = runtime_scope.clone();
            let (_, in_flight) = service_context
                .task_group()
                .spawn_with_handle(
                    "flush-consume-queue-test",
                    rocketmq_runtime::TaskKind::Worker,
                    async move {
                        super::runtime::spawn_io(&in_flight_scope, "flush-consume-queue", move || {
                            let _ = running_tx.send(());
                            std::thread::sleep(Duration::from_millis(100));
                        })
                        .await
                        .expect("background store blocking task should complete");
                    },
                )
                .expect("background store blocking task should start");
            running_rx.await.expect("background store blocking task should start");

            let probe =
                super::test_support::run_store_blocking_io_probe(service_context.clone(), 1, Duration::from_millis(1))
                    .await;
            in_flight.await.expect("background store blocking task should join");
            probe
        });

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.snapshot.blocking_still_running, 0, "{probe:?}");
        assert!(probe.snapshot.tasks.is_empty(), "{probe:?}");

        let report = owner
            .shutdown_runtime_blocking()
            .expect("store blocking concurrency probe owner should stop");
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_kv_compaction_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::test_support::run_store_kv_compaction_lifecycle_probe(super::runtime::test_service_context(
            "store-compaction-probe",
        ))
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.compacted, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_stats_service_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::test_support::run_store_stats_service_lifecycle_probe(super::runtime::test_service_context(
            "store-stats-probe",
        ))
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.snapshot_count > 0, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_timer_scheduler_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::test_support::run_store_timer_scheduler_lifecycle_probe(
            super::runtime::test_service_context("store-timer-probe"),
        )
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owned_root_wiring_scheduled_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::test_support::run_store_local_file_scheduled_lifecycle_probe(
            super::runtime::test_service_context("store-local-file-probe"),
        )
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[test]
    fn io_uring_flush_benchmark_cases_share_flush_manager_workload() {
        use super::test_support::IoUringFlushBenchmarkPath;

        let cases = super::test_support::io_uring_flush_benchmark_cases();
        assert_eq!(
            super::test_support::IO_URING_FLUSH_BENCHMARK_GROUP,
            "io_uring/flush_semantics"
        );
        assert_eq!(cases.len(), 2);

        let baseline = cases
            .iter()
            .find(|case| case.path == IoUringFlushBenchmarkPath::DefaultMappedFileBaseline)
            .expect("default mapped-file baseline case should exist");
        let experimental = cases
            .iter()
            .find(|case| case.path == IoUringFlushBenchmarkPath::IoUringExperimental)
            .expect("io_uring experimental case should exist");

        assert_eq!(baseline.workload, experimental.workload);
        assert_eq!(baseline.workload.flush_least_pages, 0);
        assert_eq!(
            baseline.workload.total_bytes(),
            baseline.workload.message_size * baseline.workload.message_count
        );
        assert!(baseline.name.contains("default"));
        assert!(experimental.name.contains("io_uring"));
    }

    #[test]
    fn phase5_platform_acceptance_default_gate_requires_stable_benefit() {
        assert!(!super::test_support::phase5_platform_optimization_default_enabled(
            None, false
        ));
        assert!(!super::test_support::phase5_platform_optimization_default_enabled(
            Some(4.99),
            false
        ));
        assert!(!super::test_support::phase5_platform_optimization_default_enabled(
            Some(8.0),
            true
        ));
        assert!(super::test_support::phase5_platform_optimization_default_enabled(
            Some(5.0),
            false
        ));
    }

    #[test]
    fn phase5_platform_acceptance_report_covers_required_scenarios() {
        let report = super::test_support::phase5_platform_optimization_acceptance_report();
        let scenario_ids = report
            .scenarios
            .iter()
            .map(|scenario| scenario.id)
            .collect::<std::collections::BTreeSet<_>>();

        assert!(scenario_ids.contains("linux_cold_boot_nvme"));
        assert!(scenario_ids.contains("linux_hot_page_cache"));
        assert!(scenario_ids.contains("linux_general_ssd_cold_boot"));
        assert!(scenario_ids.contains("windows_local_disk"));
        assert!(scenario_ids.contains("unsupported_platform"));
        assert!(report
            .scenarios
            .iter()
            .any(|scenario| scenario.page_cache_state == "hot_page_cache"));
        assert!(report
            .scenarios
            .iter()
            .any(|scenario| scenario.storage_medium == "nvme"));
        assert!(report
            .scenarios
            .iter()
            .any(|scenario| scenario.storage_medium == "general_ssd"));
    }

    #[test]
    fn phase5_platform_acceptance_report_keeps_unmeasured_defaults_disabled() {
        let report = super::test_support::phase5_platform_optimization_acceptance_report();

        assert!(!report.default_policy.store_io_hint_enable_default);
        assert!(!report.default_policy.store_lazy_mmap_enable_default);
        assert!(report.default_policy.keep_unmeasured_disabled);
        assert_eq!(
            report.default_policy.min_benefit_percent,
            super::test_support::PHASE5_PLATFORM_ACCEPTANCE_MIN_BENEFIT_PERCENT
        );
        assert!(report.scenarios.iter().all(|scenario| !scenario.default_enabled));
        assert!(report
            .recovery_correctness_commands
            .iter()
            .any(|command| command.contains("commitlog_recovery_tests")));
        assert!(!report.current_platform.store_io_hint_enable_default);
        assert!(!report.current_platform.store_lazy_mmap_enable_default);
    }

    #[tokio::test]
    async fn ha_bytes_vectored_benchmark_report_contains_required_comparison_data() {
        let report = super::test_support::run_ha_bytes_vectored_benchmark_report(64 * 1024).await;

        assert!(report.frames_match, "{report:?}");
        assert!(report.ack_offsets_match, "{report:?}");
        assert_eq!(report.batch_count, 1);
        assert_eq!(report.bytes_baseline.engine, "bytes");
        assert_eq!(report.vectored_optimized.engine, "vectored");
        assert_eq!(report.bytes_baseline.body_bytes, 64 * 1024);
        assert_eq!(report.vectored_optimized.body_bytes, 64 * 1024);
        assert_eq!(report.bytes_baseline.frame_bytes, 64 * 1024 + 12);
        assert_eq!(report.vectored_optimized.frame_bytes, 64 * 1024 + 12);
        assert_eq!(report.bytes_baseline.write_syscall_count, 2);
        assert_eq!(report.vectored_optimized.write_syscall_count, 1);
        assert_eq!(report.syscall_reduction_percent, 50);
        assert!(report.bytes_baseline.ack_latency_nanos < 1_000_000_000);
        assert!(report.vectored_optimized.ack_latency_nanos < 1_000_000_000);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn ha_sendfile_large_message_benchmark_report_contains_cpu_comparison_data() {
        let report = super::test_support::run_ha_vectored_sendfile_benchmark_report(1024 * 1024)
            .await
            .expect("sendfile benchmark report");

        assert!(report.frames_match, "{report:?}");
        assert!(report.ack_offsets_match, "{report:?}");
        assert_eq!(report.batch_count, 32);
        assert_eq!(report.vectored_baseline.engine, "vectored");
        assert_eq!(report.sendfile_optimized.engine, "sendfile");
        assert_eq!(report.vectored_baseline.body_bytes, 32 * 1024 * 1024);
        assert_eq!(report.sendfile_optimized.body_bytes, 32 * 1024 * 1024);
        assert_eq!(report.vectored_baseline.sendfile_syscall_count, 0);
        assert_eq!(report.sendfile_optimized.sendfile_syscall_count, 32);
        assert_eq!(report.sendfile_optimized.fallback_bytes, 0);
        assert!(report.vectored_baseline.user_cpu_nanos > 0, "{report:?}");
        assert!(report.sendfile_optimized.user_cpu_nanos > 0, "{report:?}");

        let expected_user_cpu_reduction_percent = report
            .vectored_baseline
            .user_cpu_nanos
            .saturating_sub(report.sendfile_optimized.user_cpu_nanos)
            .checked_mul(100)
            .and_then(|reduction| reduction.checked_div(report.vectored_baseline.user_cpu_nanos))
            .and_then(|percent| usize::try_from(percent).ok())
            .unwrap_or(0);
        assert_eq!(
            report.user_cpu_reduction_percent, expected_user_cpu_reduction_percent,
            "{report:?}"
        );
    }

    #[cfg(feature = "rocksdb_store")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_rocksdb_maintenance_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::test_support::run_store_rocksdb_maintenance_lifecycle_probe(
            super::runtime::test_service_context("rocksdb-maintenance"),
        )
        .await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
