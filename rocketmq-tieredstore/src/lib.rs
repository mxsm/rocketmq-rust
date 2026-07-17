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

pub mod config;
pub mod dispatcher;
pub mod error;
pub mod fetcher;
pub mod file;
pub mod lifecycle;
pub mod metadata;
pub mod provider;
pub(crate) mod runtime;
pub mod service;
pub mod store;

pub use config::TieredStorageLevel;
pub use config::TieredStoreConfig;
pub use dispatcher::DefaultTieredDispatcher;
pub use dispatcher::TieredDispatchHealth;
pub use dispatcher::TieredDispatchReadiness;
pub use dispatcher::TieredDispatchRequest;
pub use dispatcher::TieredDispatcher;
pub use fetcher::DefaultTieredMessageFetcher;
pub use fetcher::TieredMessageFetcher;
pub use file::CommitLogSegment;
pub use file::ConsumeQueueSegment;
pub use file::FileSegment;
pub use file::FileSegmentStatus;
pub use file::FileSegmentType;
pub use file::IndexFileSegment;
pub use file::TieredFileSegment;
pub use file::TieredFlatFile;
pub use file::TieredFlatFileStore;
pub use file::TieredIndexEntry;
pub use lifecycle::TieredLifecycle;
pub use metadata::FileSegmentMetadata;
pub use metadata::JsonMetadataStore;
pub use metadata::TieredMetadataStore;
pub use metadata::TopicMetadata;
pub use metadata::TopicQueueMetadata;
pub use provider::MemoryProvider;
pub use provider::PosixProvider;
pub use provider::ProviderKind;
pub use provider::TieredStoreProvider;
pub use service::CommitLogRecoverService;
pub use service::TieredRecoverResult;
pub use store::TieredStore;

#[cfg(feature = "serde")]
#[doc(hidden)]
pub mod bench_support {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;
    use tokio_util::sync::CancellationToken;

    use crate::dispatcher::TieredDispatcher;
    use crate::file::ConsumeQueueUnit;
    use crate::file::TieredFlatFileStore;
    use crate::file::CONSUME_QUEUE_UNIT_SIZE;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;
    use crate::provider::TieredStoreProvider;
    use crate::service::TieredServiceSet;
    use crate::TieredDispatchRequest;
    use crate::TieredStoreConfig;

    static NEXT_PROBE_ROOT_ID: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone, Debug, Serialize)]
    pub struct TieredDispatcherLifecycleProbe {
        pub request_count: usize,
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub shutdown_elapsed_us: u128,
        pub last_message_read: bool,
        pub shutdown_report: ShutdownReport,
        pub healthy: bool,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct TieredCleanupLifecycleProbe {
        pub task_count_before_shutdown: usize,
        pub task_count_after_shutdown: usize,
        pub shutdown_elapsed_us: u128,
        pub cleanup_completed: bool,
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    pub async fn run_tiered_dispatcher_lifecycle_probe(
        root: PathBuf,
        request_count: usize,
    ) -> Result<TieredDispatcherLifecycleProbe, RocketMQError> {
        let _ = std::fs::remove_dir_all(&root);
        create_probe_root(&root)?;

        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: root.clone(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 1024 * 1024,
            consume_queue_segment_size: 64 * 1024,
            max_pending_tasks: request_count.max(4),
            ..TieredStoreConfig::default()
        });
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            Arc::new(JsonMetadataStore::new(config.clone())),
            MemoryProvider::default(),
        ));
        let dispatcher = crate::DefaultTieredDispatcher::new(config, flat_file_store.clone(), CancellationToken::new());

        dispatcher.start().await?;
        for queue_offset in 0..request_count {
            let body = format!("tiered-dispatch-{queue_offset}").into_bytes();
            dispatcher
                .dispatch(TieredDispatchRequest {
                    topic: "BenchTopic".to_owned(),
                    queue_id: 0,
                    queue_offset: queue_offset as i64,
                    commit_log_offset: queue_offset as i64 * 1024,
                    message_size: body.len() as i32,
                    tags_code: queue_offset as i64,
                    store_timestamp: queue_offset as i64,
                    keys: None,
                    uniq_key: None,
                    offset_id: None,
                    sys_flag: 0,
                    body: Some(Bytes::from(body)),
                })
                .await?;
        }

        let task_count_before_shutdown = dispatcher.task_count().await;
        let shutdown_started_at = Instant::now();
        let shutdown_report = dispatcher.shutdown_with_report().await?;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = dispatcher.task_count().await;
        let last_message_read = if request_count == 0 {
            true
        } else {
            let Some(flat_file) = flat_file_store.get("BenchTopic", 0) else {
                let _ = std::fs::remove_dir_all(root);
                return Ok(TieredDispatcherLifecycleProbe {
                    request_count,
                    task_count_before_shutdown,
                    task_count_after_shutdown,
                    shutdown_elapsed_us,
                    last_message_read: false,
                    healthy: false,
                    shutdown_report,
                });
            };
            flat_file
                .read_message_by_queue_offset(request_count as i64 - 1)
                .await?
                .is_some()
        };

        let finished_tasks = shutdown_report.completed + shutdown_report.cancelled;
        let healthy = shutdown_report.is_healthy()
            && task_count_before_shutdown == 1
            && task_count_after_shutdown == 0
            && finished_tasks == 1
            && last_message_read;

        let _ = std::fs::remove_dir_all(root);
        Ok(TieredDispatcherLifecycleProbe {
            request_count,
            task_count_before_shutdown,
            task_count_after_shutdown,
            shutdown_elapsed_us,
            last_message_read,
            shutdown_report,
            healthy,
        })
    }

    pub async fn run_tiered_cleanup_lifecycle_probe(
        root: PathBuf,
    ) -> Result<TieredCleanupLifecycleProbe, RocketMQError> {
        let _ = std::fs::remove_dir_all(&root);
        create_probe_root(&root)?;

        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: root.clone(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 8,
            consume_queue_segment_size: CONSUME_QUEUE_UNIT_SIZE as u64,
            file_reserved_time: Duration::from_millis(1),
            delete_file_interval: Duration::from_millis(2),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            metadata_store,
            provider.clone(),
        ));
        let flat_file = flat_file_store.get_or_create("CleanupBenchTopic".to_owned(), 0)?;

        for (queue_offset, body, timestamp) in [
            (0, Bytes::from_static(b"old-a"), 10),
            (1, Bytes::from_static(b"old-b"), 11),
            (2, Bytes::from_static(b"new-c"), current_time_millis()),
        ] {
            let commit_log_offset = flat_file.append_commit_log(body.clone(), timestamp).await?;
            flat_file
                .append_consume_queue(
                    queue_offset,
                    ConsumeQueueUnit {
                        commit_log_offset: commit_log_offset as i64,
                        size: body.len() as i32,
                        tags_code: 0,
                    },
                    timestamp,
                )
                .await?;
        }
        flat_file.commit().await?;

        let first_commit_log_path = "CleanupBenchTopic/0/commitlog/00000000000000000000".to_owned();
        let services = TieredServiceSet::<MemoryProvider>::new();
        let shutdown = CancellationToken::new();
        services
            .start_cleanup(config, flat_file_store, shutdown.child_token())
            .await?;

        let mut cleanup_completed = false;
        for _ in 0..100 {
            if provider.segment_size(first_commit_log_path.clone()).await? == 0 {
                cleanup_completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let mut snapshots = services.cleanup_schedule_snapshot().await;
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = services.cleanup_schedule_snapshot().await;
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
        let task_count_before_shutdown = services.task_count().await;
        let shutdown_started_at = Instant::now();
        let shutdown_report = services.shutdown_with_report().await?;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let task_count_after_shutdown = services.task_count().await;
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = cleanup_completed
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && task_count_before_shutdown > 0
            && task_count_after_shutdown == 0
            && shutdown_healthy;

        let _ = std::fs::remove_dir_all(root);
        Ok(TieredCleanupLifecycleProbe {
            task_count_before_shutdown,
            task_count_after_shutdown,
            shutdown_elapsed_us,
            cleanup_completed,
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            shutdown_report,
            healthy,
        })
    }

    pub fn unique_probe_root() -> PathBuf {
        let id = NEXT_PROBE_ROOT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rocketmq-tieredstore-dispatcher-{}-{id}", std::process::id()))
    }

    pub(super) fn create_probe_root(root: &Path) -> Result<(), RocketMQError> {
        std::fs::create_dir_all(root)
            .map_err(|error| RocketMQError::storage_write_failed(root.display().to_string(), error.to_string()))
    }

    fn current_time_millis() -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64,
            Err(_) => 0,
        }
    }
}

#[cfg(all(test, feature = "serde"))]
mod bench_support_tests {
    use rocketmq_error::ErrorKind;

    #[test]
    fn create_probe_root_reports_storage_write_failure_for_file_path() {
        let temp_file = tempfile::NamedTempFile::new().expect("temporary file should be created");
        let path = temp_file.path().display().to_string();
        let error =
            super::bench_support::create_probe_root(temp_file.path()).expect_err("file path should not become a dir");

        assert_eq!(error.kind(), ErrorKind::StorageWriteFailed);
        assert!(error.to_string().contains(&path));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tiered_dispatcher_lifecycle_probe_reports_clean_shutdown() {
        let probe =
            super::bench_support::run_tiered_dispatcher_lifecycle_probe(super::bench_support::unique_probe_root(), 8)
                .await
                .expect("tiered dispatcher lifecycle probe should run");

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert!(
            probe.shutdown_report.is_healthy(),
            "{}",
            probe.shutdown_report.to_json()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tiered_cleanup_lifecycle_probe_reports_clean_shutdown() {
        let probe = super::bench_support::run_tiered_cleanup_lifecycle_probe(super::bench_support::unique_probe_root())
            .await
            .expect("tiered cleanup lifecycle probe should run");

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.cleanup_completed, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
