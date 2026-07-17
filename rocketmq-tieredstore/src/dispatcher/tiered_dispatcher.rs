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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use parking_lot::RwLock;
use rocketmq_error::RocketMQError;
use rocketmq_error::UnifiedServiceError;
use rocketmq_store_api::DerivedRecordId;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;

use crate::config::TieredStoreConfig;
use crate::dispatcher::progress::FailureRecordOutcome;
use crate::dispatcher::progress::RecordDisposition;
use crate::dispatcher::progress::TieredProgressTracker;
use crate::dispatcher::TieredDispatchRequest;
use crate::file::ConsumeQueueUnit;
use crate::file::TieredFlatFileStore;
use crate::provider::TieredStoreProvider;
use crate::runtime;
use rocketmq_observability::metrics::tiered_store::TieredStoreMetrics;

pub use crate::dispatcher::progress::TieredDispatchHealth;
pub use crate::dispatcher::progress::TieredDispatchReadiness;

type DispatcherTaskErrorSlot = Arc<tokio::sync::Mutex<Option<RocketMQError>>>;
type RetryPayloadResolver = dyn Fn(u64, u32) -> Option<Bytes> + Send + Sync;

struct QueuedDispatch {
    request: TieredDispatchRequest,
    record: Option<DerivedRecordId>,
    _byte_permit: OwnedSemaphorePermit,
}

#[allow(async_fn_in_trait)]
pub trait TieredDispatcher: Send + Sync {
    async fn dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError>;

    /// Dispatches one record with its authoritative CommitLog idempotency key.
    async fn dispatch_derived(
        &self,
        record: DerivedRecordId,
        request: TieredDispatchRequest,
    ) -> Result<(), RocketMQError> {
        let _ = record;
        self.dispatch(request).await
    }

    async fn start(&self) -> Result<(), RocketMQError>;

    async fn shutdown(&self) -> Result<(), RocketMQError>;
}

pub struct DefaultTieredDispatcher<P>
where
    P: TieredStoreProvider,
{
    config: Arc<TieredStoreConfig>,
    flat_file_store: Arc<TieredFlatFileStore<P>>,
    sender: mpsc::Sender<QueuedDispatch>,
    receiver: tokio::sync::Mutex<Option<mpsc::Receiver<QueuedDispatch>>>,
    pending_bytes: Arc<Semaphore>,
    pending_byte_capacity: u32,
    permits: Arc<Semaphore>,
    shutdown: CancellationToken,
    task_group: tokio::sync::Mutex<Option<rocketmq_runtime::TaskGroup>>,
    parent_task_group: Option<TaskGroup>,
    task_error: DispatcherTaskErrorSlot,
    metrics: Arc<TieredStoreMetrics>,
    progress: Arc<TieredProgressTracker>,
    progress_health: Arc<RwLock<TieredDispatchHealth>>,
    retry_payload_resolver: Arc<RwLock<Option<Arc<RetryPayloadResolver>>>>,
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
        let (sender, receiver) = mpsc::channel(config.max_pending_tasks.max(1));
        let permits = Arc::new(Semaphore::new((config.max_pending_tasks / 4).max(1)));
        let pending_byte_capacity = config
            .max_pending_bytes
            .clamp(1, Semaphore::MAX_PERMITS)
            .min(u32::MAX as usize) as u32;
        let pending_bytes = Arc::new(Semaphore::new(pending_byte_capacity as usize));
        let progress_health = Arc::new(RwLock::new(TieredDispatchHealth::initial()));
        let progress = Arc::new(TieredProgressTracker::new(&config, progress_health.clone()));
        Self {
            config,
            flat_file_store,
            sender,
            receiver: tokio::sync::Mutex::new(Some(receiver)),
            pending_bytes,
            pending_byte_capacity,
            permits,
            shutdown,
            task_group: tokio::sync::Mutex::new(None),
            parent_task_group,
            task_error: Arc::new(tokio::sync::Mutex::new(None)),
            metrics,
            progress,
            progress_health,
            retry_payload_resolver: Arc::new(RwLock::new(None)),
        }
    }

    /// Configures the CommitLog reader used to reconstruct payload for durable retry records.
    pub fn set_retry_payload_resolver(&self, resolver: Arc<RetryPayloadResolver>) {
        *self.retry_payload_resolver.write() = Some(resolver);
    }

    /// Loads and validates the durable Tiered cursor/retry snapshot.
    pub async fn load_progress(&self) -> Result<(), RocketMQError> {
        self.progress.load(current_time_millis()).await
    }

    /// Removes the independently versioned Tiered cursor/retry metadata.
    pub async fn destroy_progress(&self) -> Result<(), RocketMQError> {
        self.progress.destroy().await
    }

    /// Returns a bounded readiness, retry, and source-WAL pin snapshot.
    pub fn health(&self) -> TieredDispatchHealth {
        self.progress_health.read().clone()
    }

    /// Returns whether the lifecycle owner has cancelled this dispatcher.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
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
        let task_error = self.task_error.lock().await.take();
        dispatcher_task_result(task_error)?;
        Ok(report)
    }

    pub fn try_dispatch(&self, request: TieredDispatchRequest) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        if self.is_shutdown() {
            return Err(RocketMQError::Service(UnifiedServiceError::Interrupted));
        }
        let byte_permit = self.try_acquire_bytes(&request)?;
        self.sender
            .try_send(QueuedDispatch {
                request,
                record: None,
                _byte_permit: byte_permit,
            })
            .map_err(|err| RocketMQError::storage_write_failed("tiered_dispatch_queue", err.to_string()))?;
        rocketmq_observability::metrics::tiered_store::record_dispatch_queued(&self.metrics);
        Ok(())
    }

    /// Attempts to enqueue one CommitLog-derived record without dropping its identity.
    pub fn try_dispatch_derived(
        &self,
        record: DerivedRecordId,
        request: TieredDispatchRequest,
    ) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        if self.is_shutdown() {
            return Err(RocketMQError::Service(UnifiedServiceError::Interrupted));
        }
        self.validate_record(record, &request)?;
        let byte_permit = self.try_acquire_bytes(&request)?;
        self.sender
            .try_send(QueuedDispatch {
                request,
                record: Some(record),
                _byte_permit: byte_permit,
            })
            .map_err(|err| RocketMQError::storage_write_failed("tiered_dispatch_queue", err.to_string()))?;
        rocketmq_observability::metrics::tiered_store::record_dispatch_queued(&self.metrics);
        Ok(())
    }

    fn validate_record(&self, record: DerivedRecordId, request: &TieredDispatchRequest) -> Result<(), RocketMQError> {
        let request_offset = u64::try_from(request.commit_log_offset)
            .map_err(|_| RocketMQError::illegal_argument("tiered CommitLog offset must not be negative"))?;
        let request_length = u32::try_from(request.message_size)
            .map_err(|_| RocketMQError::illegal_argument("tiered CommitLog length must be positive"))?;
        if record.source_epoch() != self.config.source_epoch
            || record.physical_offset() != request_offset
            || record.length() != request_length
        {
            return Err(RocketMQError::illegal_argument(
                "tiered dispatch record does not match configured source epoch, offset, or length",
            ));
        }
        Ok(())
    }

    fn request_byte_count(request: &TieredDispatchRequest) -> Result<u32, RocketMQError> {
        let bytes = request.body.as_ref().map_or(0, Bytes::len);
        u32::try_from(bytes).map_err(|_| {
            RocketMQError::storage_write_failed("tiered_dispatch_queue", "message exceeds byte permit range")
        })
    }

    fn try_acquire_bytes(&self, request: &TieredDispatchRequest) -> Result<OwnedSemaphorePermit, RocketMQError> {
        let bytes = Self::request_byte_count(request)?;
        if bytes > self.pending_byte_capacity {
            return Err(RocketMQError::storage_write_failed(
                "tiered_dispatch_queue",
                "message exceeds bounded pending byte capacity",
            ));
        }
        self.pending_bytes.clone().try_acquire_many_owned(bytes).map_err(|_| {
            RocketMQError::storage_write_failed("tiered_dispatch_queue", "bounded pending byte capacity reached")
        })
    }

    async fn acquire_bytes(&self, request: &TieredDispatchRequest) -> Result<OwnedSemaphorePermit, RocketMQError> {
        let bytes = Self::request_byte_count(request)?;
        if bytes > self.pending_byte_capacity {
            return Err(RocketMQError::storage_write_failed(
                "tiered_dispatch_queue",
                "message exceeds bounded pending byte capacity",
            ));
        }
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => Err(RocketMQError::Service(UnifiedServiceError::Interrupted)),
            permit = self.pending_bytes.clone().acquire_many_owned(bytes) => {
                permit.map_err(|_| RocketMQError::Service(UnifiedServiceError::Interrupted))
            }
        }
    }

    async fn enqueue(&self, queued: QueuedDispatch) -> Result<(), RocketMQError> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => Err(RocketMQError::Service(UnifiedServiceError::Interrupted)),
            result = self.sender.send(queued) => {
                result.map_err(|error| {
                    RocketMQError::storage_write_failed("tiered_dispatch_queue", error.to_string())
                })
            }
        }
    }

    async fn run(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        mut receiver: mpsc::Receiver<QueuedDispatch>,
        permits: Arc<Semaphore>,
        shutdown: CancellationToken,
        metrics: Arc<TieredStoreMetrics>,
        progress: Arc<TieredProgressTracker>,
        retry_payload_resolver: Arc<RwLock<Option<Arc<RetryPayloadResolver>>>>,
        retry_tick: Arc<Notify>,
    ) -> Result<(), RocketMQError> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    while let Ok(queued) = receiver.try_recv() {
                        Self::process_queued(
                            flat_file_store.clone(),
                            permits.clone(),
                            metrics.clone(),
                            progress.clone(),
                            retry_payload_resolver.clone(),
                            retry_tick.clone(),
                            queued,
                            &shutdown,
                        )
                        .await?;
                    }
                    break;
                }
                maybe_queued = receiver.recv() => {
                    let Some(queued) = maybe_queued else {
                        break;
                    };
                    Self::process_queued(
                        flat_file_store.clone(),
                        permits.clone(),
                        metrics.clone(),
                        progress.clone(),
                        retry_payload_resolver.clone(),
                        retry_tick.clone(),
                        queued,
                        &shutdown,
                    )
                    .await?;
                }
                _ = retry_tick.notified() => {
                    Self::retry_due(
                        flat_file_store.clone(),
                        permits.clone(),
                        metrics.clone(),
                        progress.clone(),
                        retry_payload_resolver.clone(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn process_queued(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        permits: Arc<Semaphore>,
        metrics: Arc<TieredStoreMetrics>,
        progress: Arc<TieredProgressTracker>,
        retry_payload_resolver: Arc<RwLock<Option<Arc<RetryPayloadResolver>>>>,
        retry_tick: Arc<Notify>,
        queued: QueuedDispatch,
        shutdown: &CancellationToken,
    ) -> Result<(), RocketMQError> {
        let Some(record) = queued.record else {
            return Self::dispatch_one(flat_file_store, permits, metrics, queued.request).await;
        };
        let disposition = progress.classify(record).await?;
        match disposition {
            RecordDisposition::AlreadyCommitted => return Ok(()),
            RecordDisposition::Deliver | RecordDisposition::RetryPending => {}
        }

        while disposition == RecordDisposition::Deliver && progress.hard_backpressure() {
            Self::retry_due(
                flat_file_store.clone(),
                permits.clone(),
                metrics.clone(),
                progress.clone(),
                retry_payload_resolver.clone(),
            )
            .await?;
            if !progress.hard_backpressure() {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled() => {
                    return Ok(());
                }
                _ = retry_tick.notified() => {}
            }
        }

        loop {
            let result = Self::dispatch_one(
                flat_file_store.clone(),
                permits.clone(),
                metrics.clone(),
                queued.request.clone(),
            )
            .await;
            match result {
                Ok(()) => {
                    progress.record_success(record, current_time_millis()).await?;
                    return Ok(());
                }
                Err(error) => {
                    let now_millis = current_time_millis();
                    match progress.record_failure(record, &queued.request, now_millis).await? {
                        Ok(FailureRecordOutcome::Recorded | FailureRecordOutcome::AlreadyCommitted) => {
                            tracing::warn!(
                                topic = %queued.request.topic,
                                queue_id = queued.request.queue_id,
                                physical_offset = record.physical_offset(),
                                error = %error,
                                "tiered delivery failure recorded for bounded retry"
                            );
                            return Ok(());
                        }
                        Err(capacity) => {
                            tracing::warn!(
                                ?capacity,
                                physical_offset = record.physical_offset(),
                                "tiered retry ledger bound reached; pausing derived reader"
                            );
                            Self::retry_due(
                                flat_file_store.clone(),
                                permits.clone(),
                                metrics.clone(),
                                progress.clone(),
                                retry_payload_resolver.clone(),
                            )
                            .await?;
                            if !progress.hard_backpressure() {
                                continue;
                            }
                            tokio::select! {
                                _ = shutdown.cancelled() => {
                                    return Ok(());
                                }
                                _ = retry_tick.notified() => {}
                            }
                        }
                    }
                }
            }
        }
    }

    async fn retry_due(
        flat_file_store: Arc<TieredFlatFileStore<P>>,
        permits: Arc<Semaphore>,
        metrics: Arc<TieredStoreMetrics>,
        progress: Arc<TieredProgressTracker>,
        retry_payload_resolver: Arc<RwLock<Option<Arc<RetryPayloadResolver>>>>,
    ) -> Result<(), RocketMQError> {
        let now_millis = current_time_millis();
        let retries = progress.due_retries(now_millis).await;
        if retries.is_empty() {
            return Ok(());
        }
        let resolver = retry_payload_resolver.read().clone();
        for retry in retries {
            let record = retry.record()?;
            let Some(body) = resolver
                .as_ref()
                .and_then(|resolver| resolver(record.physical_offset(), record.length()))
            else {
                progress.record_retry_failure(record, now_millis).await?;
                progress.mark_unready_if_healthy(TieredDispatchReadiness::RetryPayloadUnavailable);
                continue;
            };
            let request = retry.request_with_body(body);
            match Self::dispatch_one(flat_file_store.clone(), permits.clone(), metrics.clone(), request).await {
                Ok(()) => progress.record_retry_success(record, current_time_millis()).await?,
                Err(error) => {
                    tracing::warn!(
                        physical_offset = record.physical_offset(),
                        error = %error,
                        "tiered retry remains pending"
                    );
                    progress.record_retry_failure(record, current_time_millis()).await?;
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
            .map_err(|_| RocketMQError::Service(UnifiedServiceError::Interrupted))?;
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
        // Complete any provider write that failed after staging bytes. This makes an in-process
        // partial write retry resume the same destination range instead of appending a duplicate.
        file.commit().await?;
        let consume_queue_min_offset = file.consume_queue_min_offset();
        let consume_queue_commit_offset = file.consume_queue_commit_offset();
        if consume_queue_commit_offset > consume_queue_min_offset {
            if request.queue_offset < consume_queue_commit_offset {
                let unit = file
                    .read_consume_queue_unit(request.queue_offset)
                    .await?
                    .ok_or_else(|| crate::error::storage_corrupted("tiered duplicate queue unit is missing"))?;
                let tiered_offset = u64::try_from(unit.commit_log_offset).map_err(|_| {
                    crate::error::storage_corrupted("tiered duplicate queue unit contains a negative offset")
                })?;
                return flat_file_store.append_index(request, tiered_offset).await;
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
        let byte_permit = self.acquire_bytes(&request).await?;
        self.enqueue(QueuedDispatch {
            request,
            record: None,
            _byte_permit: byte_permit,
        })
        .await?;
        rocketmq_observability::metrics::tiered_store::record_dispatch_queued(&self.metrics);
        Ok(())
    }

    async fn dispatch_derived(
        &self,
        record: DerivedRecordId,
        request: TieredDispatchRequest,
    ) -> Result<(), RocketMQError> {
        if !self.config.storage_level.enabled() || !request.is_valid() {
            return Ok(());
        }
        self.validate_record(record, &request)?;
        let byte_permit = self.acquire_bytes(&request).await?;
        self.enqueue(QueuedDispatch {
            request,
            record: Some(record),
            _byte_permit: byte_permit,
        })
        .await?;
        rocketmq_observability::metrics::tiered_store::record_dispatch_queued(&self.metrics);
        Ok(())
    }

    async fn start(&self) -> Result<(), RocketMQError> {
        self.load_progress().await?;
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
        let progress = self.progress.clone();
        let retry_payload_resolver = self.retry_payload_resolver.clone();
        let retry_tick = Arc::new(Notify::new());
        let retry_tick_scheduler = retry_tick.clone();
        ScheduledTaskGroup::new(task_group.child("scheduled"))
            .schedule_fixed_rate_no_overlap(
                ScheduledTaskConfig::fixed_rate_no_overlap(
                    "tieredstore.dispatcher.retry-ledger",
                    self.progress.retry_poll_interval(),
                ),
                move || {
                    retry_tick_scheduler.notify_one();
                    std::future::ready(())
                },
            )
            .map_err(|error| dispatcher_startup_failed("schedule retry ledger", error))?;
        task_group
            .spawn_service("tiered-dispatcher", async move {
                if let Err(error) = Self::run(
                    flat_file_store,
                    receiver,
                    permits,
                    shutdown,
                    metrics,
                    progress.clone(),
                    retry_payload_resolver,
                    retry_tick,
                )
                .await
                {
                    progress.mark_unready(TieredDispatchReadiness::DispatcherFailed);
                    *task_error.lock().await = Some(error);
                }
            })
            .map_err(|error| dispatcher_startup_failed("spawn dispatcher worker", error))?;
        *self.task_group.lock().await = Some(task_group);
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RocketMQError> {
        self.shutdown_with_report().await?;
        self.progress.mark_unready(TieredDispatchReadiness::Shutdown);
        Ok(())
    }
}

fn current_time_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

fn dispatcher_task_result(error: Option<RocketMQError>) -> Result<(), RocketMQError> {
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn dispatcher_startup_failed(operation: &'static str, error: impl std::fmt::Display) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "tieredstore dispatcher {operation}: {error}"
    )))
}

#[cfg(test)]
#[path = "tiered_dispatcher_tests.rs"]
mod tests;
