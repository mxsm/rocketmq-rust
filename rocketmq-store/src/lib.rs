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

#![allow(dead_code)]
#![allow(unused_variables)]

pub mod base;
pub mod config;
pub mod consume_queue;
pub mod filter;
pub mod ha;
pub mod hook;
mod index;
mod kv;
pub mod log_file;
pub(crate) mod message_encoder;
pub mod message_store;
pub mod pop;
pub mod queue;
#[cfg(feature = "rocksdb_store")]
pub mod rocksdb;
pub(crate) mod runtime;
pub(crate) mod services;
pub mod stats;
pub mod store;
pub mod store_error;
pub mod store_path_config_helper;
#[cfg(feature = "tieredstore")]
pub mod tieredstore;
pub mod timer;
pub mod utils;

#[doc(hidden)]
pub mod bench_support {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use futures_util::future::join_all;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_runtime::BlockingExecutorSnapshot;
    use rocketmq_runtime::ShutdownReport;
    use rocketmq_rust::ArcMut;
    use serde::Serialize;

    use crate::base::message_store::MessageStore;
    use crate::base::store_stats_service::StoreStatsService;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::kv::compaction_service::CompactionService;
    use crate::kv::compaction_store::CompactionStore;
    use crate::message_store::local_file_message_store::LocalFileMessageStore;
    #[cfg(feature = "rocksdb_store")]
    use crate::rocksdb::config::RocksDbConfig;
    #[cfg(feature = "rocksdb_store")]
    use crate::rocksdb::maintenance::RocksDbMaintenanceService;
    #[cfg(feature = "rocksdb_store")]
    use crate::rocksdb::store::RocksDbStore;
    use crate::timer::timer_message_store::TimerMessageStore;

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreBlockingIoProbe {
        pub task_count: usize,
        pub elapsed_us: u128,
        pub max_active: usize,
        pub queue_wait_min_us: u128,
        pub queue_wait_p50_us: u128,
        pub queue_wait_p95_us: u128,
        pub queue_wait_p99_us: u128,
        pub queue_wait_max_us: u128,
        pub snapshot: BlockingExecutorSnapshot,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreKvCompactionLifecycleProbe {
        pub compacted: bool,
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreStatsServiceLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub snapshot_count: usize,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreTimerSchedulerLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct StoreLocalFileScheduledLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub healthy: bool,
    }

    #[cfg(feature = "rocksdb_store")]
    #[derive(Debug, Clone, Serialize)]
    pub struct StoreRocksDbMaintenanceLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    pub async fn run_store_blocking_io_probe(task_count: usize, work_duration: Duration) -> StoreBlockingIoProbe {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let started_at = Instant::now();
        let runs = (0..task_count)
            .map(|_| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let submitted_at = Instant::now();
                crate::runtime::spawn_io("store blocking benchmark", move || {
                    let queue_wait_us = submitted_at.elapsed().as_micros();
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(current, Ordering::SeqCst);
                    std::thread::sleep(work_duration);
                    active.fetch_sub(1, Ordering::SeqCst);
                    queue_wait_us
                })
            })
            .collect::<Vec<_>>();

        let mut queue_waits = Vec::with_capacity(task_count);
        for result in join_all(runs).await {
            queue_waits.push(result.expect("store blocking benchmark task should complete"));
        }
        let elapsed_us = started_at.elapsed().as_micros();
        let snapshot = crate::runtime::blocking_snapshot().expect("store blocking snapshot should be available");
        assert_eq!(snapshot.blocking_still_running, 0, "{snapshot:?}");
        assert!(snapshot.tasks.is_empty(), "{snapshot:?}");

        let mut shutdown_report = ShutdownReport::new("rocketmq-store.blocking", Duration::ZERO);
        shutdown_report.merge_blocking(snapshot.clone());
        let healthy = shutdown_report.is_healthy() && max_active.load(Ordering::SeqCst) <= snapshot.max_concurrency;

        StoreBlockingIoProbe {
            task_count,
            elapsed_us,
            max_active: max_active.load(Ordering::SeqCst),
            queue_wait_min_us: percentile_us(&queue_waits, 0),
            queue_wait_p50_us: percentile_us(&queue_waits, 50),
            queue_wait_p95_us: percentile_us(&queue_waits, 95),
            queue_wait_p99_us: percentile_us(&queue_waits, 99),
            queue_wait_max_us: percentile_us(&queue_waits, 100),
            snapshot,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_kv_compaction_lifecycle_probe() -> StoreKvCompactionLifecycleProbe {
        let compaction_store = Arc::new(CompactionStore::new());
        let topic = CheetahString::from_static_str("kv-compaction-lifecycle-topic");
        let key = CheetahString::from_static_str("same-key");
        compaction_store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"old-message"));
        compaction_store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::from_static(b"latest-message"));

        let mut service = CompactionService::new(compaction_store.clone(), 1);
        let _ = service.load(true);
        service.start();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let compacted = loop {
            if compaction_store.message_count(&topic, 0) == 1 {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = compacted
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreKvCompactionLifecycleProbe {
            compacted,
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_stats_service_lifecycle_probe() -> StoreStatsServiceLifecycleProbe {
        let service = Arc::new(StoreStatsService::new(None));
        service.start();

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = service.schedule_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let snapshot_count = service.put_snapshot_count();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service.shutdown_gracefully_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = snapshot_count > 0
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreStatsServiceLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            snapshot_count,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_timer_scheduler_lifecycle_probe() -> StoreTimerSchedulerLifecycleProbe {
        let root = tempfile::tempdir().expect("timer scheduler benchmark root should be created");
        let config = Arc::new(MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root.path().to_string_lossy().into_owned()),
            read_uncommitted: true,
            timer_precision_ms: 100,
            ..MessageStoreConfig::default()
        });
        let timer_store = Arc::new(TimerMessageStore::new_with_config(None, config));

        assert!(timer_store.load(), "timer store should load for lifecycle probe");
        timer_store.start();

        let mut snapshots = timer_store.scheduler_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
            snapshots = timer_store.scheduler_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = timer_store.scheduler_task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = timer_store.shutdown_gracefully_with_report().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = timer_store.scheduler_task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreTimerSchedulerLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    pub async fn run_store_local_file_scheduled_lifecycle_probe() -> StoreLocalFileScheduledLifecycleProbe {
        let root = tempfile::tempdir().expect("local file store scheduled benchmark root should be created");
        let config = MessageStoreConfig {
            store_path_root_dir: CheetahString::from_string(root.path().to_string_lossy().into_owned()),
            clean_resource_interval: 1,
            ..MessageStoreConfig::default()
        };
        let mut store = ArcMut::new(LocalFileMessageStore::new(
            Arc::new(config),
            Arc::new(BrokerConfig::default()),
            Arc::new(DashMap::<CheetahString, ArcMut<TopicConfig>>::new()),
            None,
            false,
        ));
        let store_clone = store.clone();
        store.set_message_store_arc(store_clone);
        store.init().await.expect("local file store benchmark should init");
        store.start().await.expect("local file store benchmark should start");

        let mut snapshots = store.scheduled_task_snapshot();
        for _ in 0..100 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            snapshots = store.scheduled_task_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = store.scheduled_task_count();
        let shutdown_started_at = Instant::now();
        store.shutdown().await;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = store.scheduled_task_count();
        let healthy = snapshots.len() == 4
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown >= 4
            && task_count_after_shutdown == 0;

        StoreLocalFileScheduledLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            healthy,
        }
    }

    #[cfg(feature = "rocksdb_store")]
    pub async fn run_store_rocksdb_maintenance_lifecycle_probe() -> StoreRocksDbMaintenanceLifecycleProbe {
        let root = tempfile::tempdir().expect("rocksdb maintenance benchmark root should be created");
        let config = RocksDbConfig {
            enabled: true,
            path: root.path().join("maintenance-db"),
            flush_interval_ms: 1,
            ..RocksDbConfig::default()
        };
        let store =
            Arc::new(RocksDbStore::open(config.clone()).expect("rocksdb maintenance benchmark store should open"));
        let mut service = RocksDbMaintenanceService::new(Arc::clone(&store), config);
        service.start();

        let mut snapshots = service.schedule_snapshot();
        for _ in 0..100 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
            snapshots = service.schedule_snapshot();
        }

        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = service.task_count();
        let shutdown_started_at = Instant::now();
        let shutdown_report = service
            .shutdown_gracefully_with_report()
            .await
            .expect("rocksdb maintenance shutdown should complete");
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = service.task_count();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        StoreRocksDbMaintenanceLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        }
    }

    fn percentile_us(values: &[u128], percentile: usize) -> u128 {
        if values.is_empty() {
            return 0;
        }

        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        let percentile = percentile.min(100);
        let rank = ((sorted.len() * percentile).saturating_add(99) / 100).saturating_sub(1);
        sorted[rank.min(sorted.len() - 1)]
    }
}

#[cfg(test)]
mod bench_support_tests {
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_blocking_io_probe_reports_no_running_tasks() {
        let probe = super::bench_support::run_store_blocking_io_probe(4, Duration::from_millis(1)).await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.snapshot.blocking_still_running, 0, "{probe:?}");
        assert!(probe.snapshot.tasks.is_empty(), "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_kv_compaction_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_kv_compaction_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.compacted, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_stats_service_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_stats_service_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.snapshot_count > 0, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_timer_scheduler_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_timer_scheduler_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_local_file_scheduled_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_local_file_scheduled_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[cfg(feature = "rocksdb_store")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_rocksdb_maintenance_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_store_rocksdb_maintenance_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
