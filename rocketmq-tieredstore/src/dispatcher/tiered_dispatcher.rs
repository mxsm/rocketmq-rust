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

use rocketmq_error::RocketMQError;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;

use crate::config::TieredStoreConfig;
use crate::dispatcher::TieredDispatchRequest;
use crate::file::ConsumeQueueUnit;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;
use crate::runtime;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;

#[allow(async_fn_in_trait)]
pub trait TieredDispatcher: Send + Sync {
    async fn dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError>;

    async fn start(&self) -> Result<(), RocketMQError>;

    async fn shutdown(&self) -> Result<(), RocketMQError>;
}

pub struct DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    sender: mpsc::Sender<TieredDispatchRequest>,
    receiver: tokio::sync::Mutex<Option<mpsc::Receiver<TieredDispatchRequest>>>,
    permits: Arc<Semaphore>,
    shutdown: CancellationToken,
    task_group: tokio::sync::Mutex<Option<rocketmq_runtime::TaskGroup>>,
    parent_task_group: Option<TaskGroup>,
    task_error: Arc<tokio::sync::Mutex<Option<String>>>,
    metrics: Arc<TieredStoreMetrics>,
}

impl<P> DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    pub fn new(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self::new_with_optional_task_group(config, flat_file_store, shutdown, None)
    }

    pub fn new_with_task_group(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
        parent_task_group: TaskGroup,
    ) -> Self {
        Self::new_with_optional_task_group(config, flat_file_store, shutdown, Some(parent_task_group))
    }

    fn new_with_optional_task_group(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        Self::new_with_optional_metrics(
            config,
            flat_file_store,
            shutdown,
            Arc::new(TieredStoreMetrics::default()),
            parent_task_group,
        )
    }

    pub fn new_with_metrics(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
        metrics: Arc<TieredStoreMetrics>,
    ) -> Self {
        Self::new_with_optional_metrics(config, flat_file_store, shutdown, metrics, None)
    }

    pub fn new_with_metrics_and_task_group(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
        metrics: Arc<TieredStoreMetrics>,
        parent_task_group: TaskGroup,
    ) -> Self {
        Self::new_with_optional_metrics(config, flat_file_store, shutdown, metrics, Some(parent_task_group))
    }

    fn new_with_optional_metrics(
        config: Arc<TieredStoreConfig>,
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        shutdown: CancellationToken,
        metrics: Arc<TieredStoreMetrics>,
        parent_task_group: Option<TaskGroup>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.max_pending_tasks);
        let permits = Arc::new(Semaphore::new((config.max_pending_tasks / 4).max(1)));
        Self {
            config,
            flat_file_store,
            sender,
            receiver: tokio::sync::Mutex::new(Some(receiver)),
            permits,
            shutdown,
            task_group: tokio::sync::Mutex::new(None),
            parent_task_group,
            task_error: Arc::new(tokio::sync::Mutex::new(None)),
            metrics,
        }
    }

    pub fn metrics(&self) -> Arc<TieredStoreMetrics> {
        self.metrics.clone()
    }

    pub async fn task_count(&self) -> usize {
        self.task_group
            .lock()
            .await
            .as_ref()
            .map(rocketmq_runtime::TaskGroup::task_count)
            .unwrap_or(0)
    }

    pub async fn shutdown_with_report(&self) -> Result<ShutdownReport, RocketMQError> {
        self.shutdown.cancel();
        let report = if let Some(task_group) = self.task_group.lock().await.take() {
            let report = task_group.shutdown(std::time::Duration::from_secs(5)).await;
            runtime::shutdown_report_result("tieredstore dispatcher", report.clone())?;
            report
        } else {
            ShutdownReport::new("rocketmq-tieredstore.dispatcher", std::time::Duration::ZERO)
        };
        if let Some(error) = self.task_error.lock().await.take() {
            return Err(RocketMQError::Internal(error));
        }
        Ok(report)
    }

    pub fn try_dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        self.sender
            .try_send(request)
            .map_err(|err| RocketMQError::storage_write_failed("tiered_dispatch_queue", err.to_string()))?;
        rocketmq_observability::metrics::tiered_store::record_dispatch_queued(&self.metrics);
        Ok(())
    }

    async fn run(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        mut receiver: mpsc::Receiver<TieredDispatchRequest>,
        permits: Arc<Semaphore>,
        shutdown: CancellationToken,
        metrics: Arc<TieredStoreMetrics>,
    ) -> Result<(), RocketMQError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    while let Ok(request) = receiver.try_recv() {
                        Self::dispatch_one(flat_file_store.clone(), permits.clone(), metrics.clone(), request).await?;
                    }
                    break;
                }
                maybe_request = receiver.recv() => {
                    let Some(request) = maybe_request else {
                        break;
                    };
                    Self::dispatch_one(flat_file_store.clone(), permits.clone(), metrics.clone(), request).await?;
                }
            }
        }
        Ok(())
    }

    async fn dispatch_one(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        permits: Arc<Semaphore>,
        metrics: Arc<TieredStoreMetrics>,
        request: TieredDispatchRequest,
    ) -> Result<(), RocketMQError> {
        let started = std::time::Instant::now();
        metrics.record_dispatch_dequeued();
        let _permit = permits
            .acquire_owned()
            .await
            .map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let result = Self::dispatch_one_inner(flat_file_store, &request).await;
        match &result {
            Ok(()) => {
                metrics.record_messages_dispatch(&request.topic, request.queue_id, "commitlog", 1);
                metrics.record_dispatch_latency(
                    &request.topic,
                    request.queue_id,
                    "commitlog",
                    started.elapsed().as_millis() as u64,
                );
            }
            Err(_) => metrics.record_commit_failure(),
        }
        result
    }

    async fn dispatch_one_inner(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        request: &TieredDispatchRequest,
    ) -> Result<(), RocketMQError> {
        let file = flat_file_store.get_or_create(request.topic.clone(), request.queue_id)?;
        let consume_queue_min_offset = file.consume_queue_min_offset();
        let consume_queue_commit_offset = file.consume_queue_commit_offset();
        if consume_queue_commit_offset > consume_queue_min_offset {
            if request.queue_offset < consume_queue_commit_offset {
                return Ok(());
            }
            if request.queue_offset > consume_queue_commit_offset {
                return Err(RocketMQError::illegal_argument(format!(
                    "tiered consume queue offset gap, expected {consume_queue_commit_offset}, got {}",
                    request.queue_offset
                )));
            }
        }

        let message = request.body.clone().unwrap_or_default();
        let tiered_offset = file.append_commit_log(message, request.store_timestamp).await?;
        file.append_consume_queue(
            request.queue_offset,
            ConsumeQueueUnit {
                commit_log_offset: tiered_offset as i64,
                size: request.message_size,
                tags_code: request.tags_code,
            },
            request.store_timestamp,
        )
        .await?;
        file.commit().await?;
        flat_file_store.append_index(request, tiered_offset).await
    }
}

impl<P> TieredDispatcher for DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    async fn dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        self.sender
            .send(request)
            .await
            .map_err(|err| RocketMQError::storage_write_failed("tiered_dispatch_queue", err.to_string()))?;
        rocketmq_observability::metrics::tiered_store::record_dispatch_queued(&self.metrics);
        Ok(())
    }

    async fn start(&self) -> Result<(), RocketMQError> {
        let mut receiver_guard = self.receiver.lock().await;
        let Some(receiver) = receiver_guard.take() else {
            return Ok(());
        };
        let task_group = match self.parent_task_group.as_ref() {
            Some(parent_task_group) => {
                runtime::task_group_with_parent("rocketmq-tieredstore.dispatcher", parent_task_group)
            }
            None => runtime::task_group("rocketmq-tieredstore.dispatcher")?,
        };
        let task_error = self.task_error.clone();
        let flat_file_store = self.flat_file_store.clone();
        let permits = self.permits.clone();
        let shutdown = self.shutdown.clone();
        let metrics = self.metrics.clone();
        task_group
            .spawn_service("tiered-dispatcher", async move {
                if let Err(error) = Self::run(flat_file_store, receiver, permits, shutdown, metrics).await {
                    *task_error.lock().await = Some(error.to_string());
                }
            })
            .map_err(|error| RocketMQError::Internal(error.to_string()))?;
        *self.task_group.lock().await = Some(task_group);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RocketMQError> {
        self.shutdown_with_report().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use rocketmq_error::RocketMQError;
    use rocketmq_runtime::RuntimeContext;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::config::TieredStoreConfig;
    use crate::file::TieredFlatFileStore;
    use crate::metadata::JsonMetadataStore;
    use crate::provider::MemoryProvider;

    #[tokio::test]
    async fn dispatch_writes_commit_log_and_consume_queue_unit() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            max_pending_tasks: 4,
            ..TieredStoreConfig::default()
        });
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            Arc::new(JsonMetadataStore::new(config.clone())),
            MemoryProvider::default(),
        ));
        let dispatcher = DefaultTieredDispatcher::new(config, flat_file_store.clone(), CancellationToken::new());

        dispatcher.start().await?;
        dispatcher
            .dispatch(TieredDispatchRequest {
                topic: "TopicA".to_owned(),
                queue_id: 0,
                queue_offset: 3,
                commit_log_offset: 1024,
                message_size: 4,
                tags_code: 7,
                store_timestamp: 100,
                keys: None,
                uniq_key: None,
                offset_id: None,
                sys_flag: 0,
                body: Some(Bytes::from_static(b"body")),
            })
            .await?;
        dispatcher.shutdown().await?;

        let flat_file = flat_file_store
            .get("TopicA", 0)
            .ok_or_else(|| RocketMQError::Internal("missing dispatched flat file".to_owned()))?;
        let cq_unit = flat_file
            .read_consume_queue_unit(3)
            .await?
            .ok_or_else(|| RocketMQError::Internal("missing dispatched consume queue unit".to_owned()))?;

        assert_eq!(cq_unit.commit_log_offset, 0);
        assert_eq!(cq_unit.size, 4);
        assert_eq!(cq_unit.tags_code, 7);
        assert_eq!(
            flat_file.read_message_by_queue_offset(3).await?,
            Some(Bytes::from_static(b"body"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_with_task_group_parents_dispatcher_task() -> Result<(), RocketMQError> {
        let temp_dir = tempfile::tempdir().map_err(|err| RocketMQError::Internal(err.to_string()))?;
        let config = Arc::new(TieredStoreConfig {
            store_path_root_dir: temp_dir.path().to_path_buf(),
            backend_provider: "memory".to_owned(),
            max_pending_tasks: 4,
            ..TieredStoreConfig::default()
        });
        let flat_file_store = Arc::new(TieredFlatFileStore::new(
            config.clone(),
            Arc::new(JsonMetadataStore::new(config.clone())),
            MemoryProvider::default(),
        ));
        let context = RuntimeContext::from_current("tieredstore-dispatcher-parent-test");
        let service = context.service_context("tieredstore-dispatcher");
        let dispatcher = DefaultTieredDispatcher::new_with_task_group(
            config,
            flat_file_store,
            CancellationToken::new(),
            service.task_group().clone(),
        );

        dispatcher.start().await?;
        dispatcher.shutdown().await?;

        let report = service.task_group().shutdown(Duration::from_secs(1)).await;
        assert!(
            report
                .children
                .iter()
                .any(|child| child.name == "rocketmq-tieredstore.dispatcher"),
            "{}",
            report.to_json()
        );
        assert!(report.is_healthy(), "{}", report.to_json());
        Ok(())
    }
}
