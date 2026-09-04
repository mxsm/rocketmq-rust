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

use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::TaskGroup;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::config::TieredStoreConfig;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;

pub(super) type CleanupErrorSlot = Arc<Mutex<Option<StoreError>>>;

pub struct CleanupService<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    shutdown: CancellationToken,
}

impl<P> CleanupService<P>
where
    P: TieredStoreProvider,
{
    pub fn new(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            flat_file_store,
            shutdown,
        }
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub(crate) fn schedule(
        &self,
        owner: &crate::runtime::TaskOperationOwner,
        cleanup_error: CleanupErrorSlot,
    ) -> Result<ScheduledTaskGroup, StoreError> {
        let scheduled_tasks = ScheduledTaskGroup::new(owner.task_group().clone());
        let flat_file_store = self.flat_file_store.clone();
        let operation_on_error = owner.operation().clone();
        let shutdown_on_error = self.shutdown.clone();
        let cleanup_error_on_error = cleanup_error.clone();

        scheduled_tasks
            .schedule_fixed_rate_no_overlap_operation(
                owner.operation(),
                ScheduledTaskConfig::fixed_rate_no_overlap(
                    "tieredstore.cleanup.expired-files",
                    self.config.delete_file_interval,
                ),
                move || {
                    let flat_file_store = flat_file_store.clone();
                    let operation_on_error = operation_on_error.clone();
                    let shutdown_on_error = shutdown_on_error.clone();
                    let cleanup_error_on_error = cleanup_error_on_error.clone();
                    async move {
                        if let Err(error) = flat_file_store.cleanup_expired(current_time_millis()).await {
                            let mut cleanup_error = cleanup_error_on_error.lock().await;
                            if cleanup_error.is_none() {
                                *cleanup_error = Some(error);
                            }
                            shutdown_on_error.cancel();
                            operation_on_error.cancel();
                        }
                    }
                },
            )
            .map_err(|error| cleanup_startup_failed("schedule cleanup expired files", error))?;

        let shutdown = self.shutdown.clone();
        let operation_on_shutdown = owner.operation().clone();
        let operation_token = owner.operation().cancellation_token();
        owner
            .task_group()
            .spawn_operation(owner.operation(), "cleanup-shutdown-watcher", async move {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        operation_on_shutdown.cancel();
                    }
                    _ = operation_token.cancelled() => {}
                }
            })
            .map_err(|error| {
                owner.cancel();
                cleanup_startup_failed("spawn cleanup shutdown watcher", error)
            })?;

        Ok(scheduled_tasks)
    }

    pub async fn cleanup_once(&self) -> Result<(), StoreError> {
        self.flat_file_store.cleanup_expired(current_time_millis()).await
    }

    pub async fn run(self, parent_task_group: TaskGroup) -> Result<(), StoreError> {
        let owner = crate::runtime::TaskOperationOwner::new(parent_task_group, rocketmq_runtime::TaskKind::Service);
        let cleanup_error = Arc::new(Mutex::new(None));
        self.schedule(&owner, cleanup_error.clone())?;

        self.shutdown.cancelled().await;
        let report = owner
            .shutdown_report("rocketmq-tieredstore.cleanup.run", Duration::from_secs(5))
            .await?;
        crate::runtime::shutdown_report_result("tieredstore cleanup run", report)?;
        let cleanup_error = cleanup_error.lock().await.take();
        cleanup_worker_result(cleanup_error)
    }
}

pub(super) fn cleanup_worker_result(error: Option<StoreError>) -> Result<(), StoreError> {
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn cleanup_startup_failed(_operation: &'static str, source: rocketmq_runtime::RuntimeError) -> StoreError {
    crate::error::runtime_error(StoreOperation::Start, source)
}

fn current_time_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use rocketmq_store_api::StoreError;
    use tokio::time::sleep;

    use super::*;
    use crate::file::ConsumeQueueUnit;
    use crate::file::CONSUME_QUEUE_UNIT_SIZE;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;

    #[test]
    fn cleanup_worker_result_preserves_original_error_kind() {
        let original = crate::error::request_invalid(StoreOperation::AppendDerived);
        let error = cleanup_worker_result(Some(original)).expect_err("cleanup worker error should be propagated");

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_REQUEST_INVALID);
        assert_eq!(error.operation(), StoreOperation::AppendDerived);
    }

    #[test]
    fn cleanup_startup_failed_uses_service_error_kind() {
        let error = cleanup_startup_failed(
            "schedule test",
            rocketmq_runtime::RuntimeError::InsideTokioRuntime("task group closed"),
        );

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_INTERNAL_FAILURE);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<rocketmq_runtime::RuntimeError>())
            .is_some());
    }

    #[tokio::test]
    async fn cleanup_service_runs_periodically_and_stops_on_shutdown() -> Result<(), StoreError> {
        let temp_dir = tempfile::tempdir().map_err(|source| {
            crate::error::source_error(
                &rocketmq_error::STORAGE_INTERNAL_FAILURE,
                rocketmq_store_api::StoreOperation::Load,
                source,
            )
        })?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            commit_log_segment_size: 8,
            consume_queue_segment_size: CONSUME_QUEUE_UNIT_SIZE as u64,
            file_reserved_time: Duration::from_millis(1),
            delete_file_interval: Duration::from_millis(10),
            ..TieredStoreConfig::default()
        });
        let metadata_store = Arc::new(JsonMetadataStore::new(config.clone()));
        let provider = MemoryProvider::default();
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            metadata_store,
            provider.clone(),
        ));
        let flat_file = flat_file_store.get_or_create("CleanupTopic".to_owned(), 0)?;

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

        let first_commit_log_path = "CleanupTopic/0/commitlog/00000000000000000000".to_owned();
        assert_eq!(
            provider
                .segment_size(StoreOperation::Read, first_commit_log_path.clone())
                .await?,
            5
        );

        let shutdown = CancellationToken::new();
        let service = CleanupService::new(config, flat_file_store, shutdown.clone());
        let context = rocketmq_runtime::RuntimeContext::from_current("rocketmq-tieredstore.cleanup-test");
        let task_group = context.service_context("cleanup-worker").task_group().clone();
        let task_error = Arc::new(tokio::sync::Mutex::new(None));
        let task_error_clone = task_error.clone();
        let service_parent = task_group.clone();
        task_group
            .spawn_service("cleanup-service-test", async move {
                if let Err(error) = service.run(service_parent).await {
                    *task_error_clone.lock().await = Some(error);
                }
            })
            .map_err(|error| cleanup_startup_failed("spawn cleanup service test", error))?;

        for _ in 0..50 {
            if provider
                .segment_size(StoreOperation::Read, first_commit_log_path.clone())
                .await?
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        shutdown.cancel();
        let report = task_group.shutdown(Duration::from_secs(2)).await;
        crate::runtime::shutdown_report_result("cleanup service test", report)?;
        if let Some(error) = task_error.lock().await.take() {
            return Err(error);
        }

        assert_eq!(
            provider
                .segment_size(StoreOperation::Read, first_commit_log_path)
                .await?,
            0
        );
        Ok(())
    }
}
