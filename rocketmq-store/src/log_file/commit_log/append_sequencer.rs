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

//! Store-owned CommitLog append worker and per-request result projection.

use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_runtime::resource_budget::BudgetConfigError;
use rocketmq_runtime::resource_budget::QueueSnapshot;
use rocketmq_runtime::RuntimeError;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store_local::commit_log::append::prepared_payload::PreparedPayload;
use rocketmq_store_local::commit_log::append::sequencer::AppendAdmissionErrorKind;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencer;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencerConfig;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencerReceiver;
use rocketmq_store_local::commit_log::append::sequencer::AppendSequencerSender;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendAttempt;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendFailure;
use rocketmq_store_local::commit_log::append_attempt::CommitLogAppendResolution;
use tokio::sync::oneshot;
use tracing::error;
use tracing::warn;

use super::CommitLog;
use super::CommitLogFlushWakeup;
use super::CommitLogRuntimeState;
use super::CommitLogStoreContext;
use super::ConsumeQueueStore;
use super::ConsumeQueueStoreTrait;
use super::DefaultAppendMessageCallback;
use super::DefaultMappedFile;
use super::MappedFile;
use super::MappedFileQueueAppendHandle;
use super::MessageStoreConfig;
use super::PutMessageContext;
use super::PutMessageResult;
use super::PutMessageStatus;
use crate::base::message_result::AppendMessageResult;

const APPEND_WORKER_NAME: &str = "commitlog-append-sequencer";
const APPEND_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Failure to construct the bounded queue or start its lifecycle-owned worker.
#[derive(Debug, thiserror::Error)]
pub(super) enum CommitLogAppendStartError {
    #[error("invalid append queue budget: {0}")]
    InvalidBudget(#[from] BudgetConfigError),
    #[error("failed to start append worker: {0}")]
    WorkerStart(#[from] RuntimeError),
}

/// Result returned to a single-message caller after the worker finishes durable-memory append.
pub(super) struct SequencedMessageAppend {
    pub(super) message: MessageExtBrokerInner,
    pub(super) result: PutMessageResult,
}

/// Result returned to an encoded-message-batch caller.
pub(super) struct SequencedBatchAppend {
    pub(super) batch: MessageExtBatch,
    pub(super) result: PutMessageResult,
}

/// Cloneable admission port shared by normal and internal CommitLog producers.
#[derive(Clone)]
pub(super) struct CommitLogAppendPort {
    sender: AppendSequencerSender<AppendRequest>,
}

impl CommitLogAppendPort {
    pub(super) async fn append_message(
        &self,
        message: MessageExtBrokerInner,
        prepared: PreparedPayload,
        assign_queue_offset: bool,
    ) -> SequencedMessageAppend {
        let (completion, response) = oneshot::channel();
        let request = AppendRequest::Message {
            message,
            prepared,
            assign_queue_offset,
            completion,
        };
        let retained_bytes = request.retained_bytes();
        if let Err(error) = self.sender.try_submit(request, retained_bytes) {
            let kind = error.kind();
            error
                .into_request()
                .reject(PutMessageResult::new_default(Self::admission_status(kind)));
        }
        response.await.unwrap_or_else(|response_error| {
            error!(error = %response_error, "CommitLog append worker dropped a message response");
            SequencedMessageAppend {
                message: MessageExtBrokerInner::default(),
                result: PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable),
            }
        })
    }

    pub(super) async fn append_batch(
        &self,
        batch: MessageExtBatch,
        prepared: PreparedPayload,
        context: PutMessageContext,
    ) -> SequencedBatchAppend {
        let (completion, response) = oneshot::channel();
        let request = AppendRequest::Batch {
            batch,
            prepared,
            context,
            completion,
        };
        let retained_bytes = request.retained_bytes();
        if let Err(error) = self.sender.try_submit(request, retained_bytes) {
            let kind = error.kind();
            error
                .into_request()
                .reject(PutMessageResult::new_default(Self::admission_status(kind)));
        }
        response.await.unwrap_or_else(|response_error| {
            error!(error = %response_error, "CommitLog append worker dropped a batch response");
            SequencedBatchAppend {
                batch: MessageExtBatch::default(),
                result: PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable),
            }
        })
    }

    pub(super) fn close(&self) {
        self.sender.close();
    }

    fn close_and_discard_pending(&self) -> usize {
        self.sender.close_and_discard_pending()
    }

    pub(super) fn snapshot(&self) -> QueueSnapshot {
        self.sender.snapshot()
    }

    fn admission_status(kind: AppendAdmissionErrorKind) -> PutMessageStatus {
        match kind {
            AppendAdmissionErrorKind::Saturated => PutMessageStatus::OsPageCacheBusy,
            AppendAdmissionErrorKind::Closed => PutMessageStatus::ServiceNotAvailable,
        }
    }
}

/// Lifecycle owner for the append worker.
pub(super) struct CommitLogAppendRuntime {
    port: CommitLogAppendPort,
    task_group: TaskGroup,
}

impl CommitLogAppendRuntime {
    pub(super) fn start(
        runtime_scope: crate::runtime::StoreRuntimeScope,
        config: AppendSequencerConfig,
        processor: CommitLogAppendProcessor,
    ) -> Result<Self, CommitLogAppendStartError> {
        let (sender, receiver) = AppendSequencer::bounded(config)?;
        let port = CommitLogAppendPort { sender };
        let task_group = runtime_scope.task_group(APPEND_WORKER_NAME);
        let cancellation = task_group.cancellation_token();
        let worker_port = port.clone();
        task_group.spawn(
            APPEND_WORKER_NAME,
            TaskKind::Worker,
            run_append_worker(receiver, processor, cancellation, worker_port),
        )?;
        Ok(Self { port, task_group })
    }

    pub(super) fn port(&self) -> CommitLogAppendPort {
        self.port.clone()
    }

    pub(super) fn close(&self) {
        self.port.close();
    }

    pub(super) async fn shutdown_gracefully(&self) {
        self.port.close();
        let report = self.task_group.shutdown(APPEND_WORKER_SHUTDOWN_TIMEOUT).await;
        if let Err(shutdown_error) = report.assert_no_task_leak() {
            error!(error = %shutdown_error, "CommitLog append sequencer did not stop cleanly");
        }
    }
}

enum AppendRequest {
    Message {
        message: MessageExtBrokerInner,
        prepared: PreparedPayload,
        assign_queue_offset: bool,
        completion: oneshot::Sender<SequencedMessageAppend>,
    },
    Batch {
        batch: MessageExtBatch,
        prepared: PreparedPayload,
        context: PutMessageContext,
        completion: oneshot::Sender<SequencedBatchAppend>,
    },
}

impl AppendRequest {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Message { message, prepared, .. } => prepared.retained_bytes().saturating_add(message.body_len()),
            Self::Batch { batch, prepared, .. } => prepared
                .retained_bytes()
                .saturating_add(batch.message_ext_broker_inner.body_len()),
        }
    }

    fn reject(self, result: PutMessageResult) {
        match self {
            Self::Message {
                message, completion, ..
            } => {
                let _ = completion.send(SequencedMessageAppend { message, result });
            }
            Self::Batch { batch, completion, .. } => {
                let _ = completion.send(SequencedBatchAppend { batch, result });
            }
        }
    }

    fn bind_sequencer_fields(
        self,
        append_callback: &DefaultAppendMessageCallback,
        duplication_enable: bool,
    ) -> ReadyAppend {
        match self {
            Self::Message {
                mut message,
                prepared,
                assign_queue_offset,
                completion,
            } => {
                if !duplication_enable {
                    message.message_ext_inner.store_timestamp =
                        rocketmq_runtime::common::time_utils::current_millis() as i64;
                }
                let message_count = append_callback.message_num(&message);
                ReadyAppend::Message {
                    message,
                    prepared,
                    assign_queue_offset,
                    message_count,
                    completion,
                }
            }
            Self::Batch {
                mut batch,
                prepared,
                context,
                completion,
            } => {
                batch.message_ext_broker_inner.message_ext_inner.store_timestamp =
                    rocketmq_runtime::common::time_utils::current_millis() as i64;
                let message_count = prepared.message_count();
                ReadyAppend::Batch {
                    batch,
                    prepared,
                    context,
                    message_count,
                    completion,
                }
            }
        }
    }
}

/// An admitted append whose stable encoding and sequencer-owned runtime fields are ready.
enum ReadyAppend {
    Message {
        message: MessageExtBrokerInner,
        prepared: PreparedPayload,
        assign_queue_offset: bool,
        message_count: i16,
        completion: oneshot::Sender<SequencedMessageAppend>,
    },
    Batch {
        batch: MessageExtBatch,
        prepared: PreparedPayload,
        context: PutMessageContext,
        message_count: i16,
        completion: oneshot::Sender<SequencedBatchAppend>,
    },
}

enum CompletedAppend {
    Message {
        message: MessageExtBrokerInner,
        result: PutMessageResult,
        completion: oneshot::Sender<SequencedMessageAppend>,
        unlock_segment: Option<Arc<DefaultMappedFile>>,
    },
    Batch {
        batch: MessageExtBatch,
        result: PutMessageResult,
        completion: oneshot::Sender<SequencedBatchAppend>,
        unlock_segment: Option<Arc<DefaultMappedFile>>,
    },
}

impl CompletedAppend {
    fn committed(&self) -> bool {
        match self {
            Self::Message { result, .. } | Self::Batch { result, .. } => {
                result.put_message_status() == PutMessageStatus::PutOk
            }
        }
    }

    fn finish_post_lock(&mut self, processor: &CommitLogAppendProcessor) {
        match self {
            Self::Message {
                message,
                result,
                unlock_segment,
                ..
            } => {
                if processor.message_store_config.warm_mapped_file_enable {
                    if let Some(mapped_file) = unlock_segment.take() {
                        mapped_file.munlock();
                    }
                }
                if result.put_message_status() == PutMessageStatus::PutOk {
                    if let Some(append_result) = result.append_message_result() {
                        processor.record_put_message_stats(message.topic(), append_result);
                    }
                }
            }
            Self::Batch {
                batch,
                result,
                unlock_segment,
                ..
            } => {
                if processor.message_store_config.warm_mapped_file_enable {
                    if let Some(mapped_file) = unlock_segment.take() {
                        mapped_file.munlock();
                    }
                }
                if result.put_message_status() == PutMessageStatus::PutOk {
                    if let Some(append_result) = result.append_message_result() {
                        processor.record_put_message_stats(batch.message_ext_broker_inner.topic(), append_result);
                    }
                }
            }
        }
    }

    fn complete(self) {
        match self {
            Self::Message {
                message,
                result,
                completion,
                ..
            } => {
                let _ = completion.send(SequencedMessageAppend { message, result });
            }
            Self::Batch {
                batch,
                result,
                completion,
                ..
            } => {
                let _ = completion.send(SequencedBatchAppend { batch, result });
            }
        }
    }
}

/// Dependencies captured by the single CommitLog append worker.
pub(super) struct CommitLogAppendDependencies {
    pub(super) append: MappedFileQueueAppendHandle,
    pub(super) message_store_config: Arc<MessageStoreConfig>,
    pub(super) store_context: CommitLogStoreContext,
    pub(super) runtime_state: Arc<CommitLogRuntimeState>,
    pub(super) append_callback: Arc<DefaultAppendMessageCallback>,
    pub(super) put_message_lock: Arc<tokio::sync::Mutex<()>>,
    pub(super) consume_queue_store: ConsumeQueueStore,
    pub(super) flush: CommitLogFlushWakeup,
    pub(super) store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
}

pub(super) struct CommitLogAppendProcessor {
    append: MappedFileQueueAppendHandle,
    message_store_config: Arc<MessageStoreConfig>,
    store_context: CommitLogStoreContext,
    runtime_state: Arc<CommitLogRuntimeState>,
    append_callback: Arc<DefaultAppendMessageCallback>,
    put_message_lock: Arc<tokio::sync::Mutex<()>>,
    consume_queue_store: ConsumeQueueStore,
    flush: CommitLogFlushWakeup,
    store_metrics: rocketmq_observability::metrics::store::StoreMetricsRecorder,
}

impl CommitLogAppendProcessor {
    pub(super) fn new(dependencies: CommitLogAppendDependencies) -> Self {
        let CommitLogAppendDependencies {
            append,
            message_store_config,
            store_context,
            runtime_state,
            append_callback,
            put_message_lock,
            consume_queue_store,
            flush,
            store_metrics,
        } = dependencies;
        Self {
            append,
            message_store_config,
            store_context,
            runtime_state,
            append_callback,
            put_message_lock,
            consume_queue_store,
            flush,
            store_metrics,
        }
    }

    async fn process_batch(
        &self,
        batch: rocketmq_store_local::commit_log::append::micro_batch::MicroBatch<AppendRequest>,
    ) {
        let request_count = batch.len();
        let retained_bytes = batch.retained_bytes();
        let mut requests = Vec::with_capacity(request_count);
        let mut permits = Vec::with_capacity(request_count);
        for budgeted_request in batch.into_budgeted_items() {
            let (request, permit, _) = budgeted_request.into_parts();
            requests.push(request.bind_sequencer_fields(
                self.append_callback.as_ref(),
                self.message_store_config.duplication_enable,
            ));
            permits.push(permit);
        }

        let wait_started = std::time::Instant::now();
        let guard = self.put_message_lock.lock().await;
        let lock_wait_millis = wait_started.elapsed().as_millis() as u64;
        let lock_started = std::time::Instant::now();
        self.runtime_state
            .set_begin_time_in_lock(rocketmq_runtime::common::time_utils::current_millis());

        let mut completions = Vec::with_capacity(request_count);
        for request in requests {
            completions.push(self.append_ready(request));
        }

        let lock_hold_millis = lock_started.elapsed().as_millis() as u64;
        self.runtime_state
            .record_put_message_lock(lock_wait_millis, lock_hold_millis);
        drop(guard);
        self.runtime_state.clear_begin_time_in_lock();
        self.store_metrics.record_append_latency(lock_hold_millis);
        if lock_hold_millis > 500 {
            warn!(
                lock_hold_millis,
                request_count, retained_bytes, "CommitLog append micro-batch held the writer lock for too long"
            );
        }

        for completion in &mut completions {
            completion.finish_post_lock(self);
        }
        let committed_requests = completions.iter().filter(|completion| completion.committed()).count();
        self.flush.wakeup_after_append_batch(committed_requests);
        for (completion, permit) in completions.into_iter().zip(permits) {
            completion.complete();
            drop(permit);
        }
    }

    /// Performs only logical-offset reservation and mapped CommitLog writes while the global
    /// writer lock is held. Encoding, timestamp binding, stats, segment unlock, flush wake-up, and
    /// result delivery stay outside this critical section.
    fn append_ready(&self, request: ReadyAppend) -> CompletedAppend {
        match request {
            ReadyAppend::Message {
                mut message,
                prepared,
                assign_queue_offset,
                message_count,
                completion,
            } => {
                if assign_queue_offset {
                    self.assign_offset(&mut message);
                }
                let mut queue_offset = message.queue_offset();
                if matches!(
                    MessageSysFlag::get_transaction_value(message.sys_flag()),
                    MessageSysFlag::TRANSACTION_PREPARED_TYPE | MessageSysFlag::TRANSACTION_ROLLBACK_TYPE
                ) {
                    queue_offset = 0;
                }
                let (result, unlock_segment) =
                    self.append_message(&mut message, &prepared, queue_offset, i32::from(message_count));
                if result.put_message_status() == PutMessageStatus::PutOk {
                    self.increase_offset(&message, message_count);
                }
                CompletedAppend::Message {
                    message,
                    result,
                    completion,
                    unlock_segment,
                }
            }
            ReadyAppend::Batch {
                mut batch,
                prepared,
                mut context,
                message_count,
                completion,
            } => {
                self.assign_offset(&mut batch.message_ext_broker_inner);
                let (result, unlock_segment) = self.append_batch(&mut batch, &prepared, &mut context);
                if result.put_message_status() == PutMessageStatus::PutOk {
                    self.increase_offset(&batch.message_ext_broker_inner, message_count);
                }
                CompletedAppend::Batch {
                    batch,
                    result,
                    completion,
                    unlock_segment,
                }
            }
        }
    }

    fn append_message(
        &self,
        message: &mut MessageExtBrokerInner,
        prepared: &PreparedPayload,
        queue_offset: i64,
        message_num: i32,
    ) -> (PutMessageResult, Option<Arc<DefaultMappedFile>>) {
        let initial = self.append.get_last_mapped_file(0, false);
        let outcome = CommitLogAppendAttempt::run(
            initial,
            |mapped_file| mapped_file.is_full(),
            || self.append.get_last_mapped_file(0, true),
            |mapped_file| self.prepare_active_segment(mapped_file),
            |mapped_file| {
                self.append_callback.append_prepared_message(
                    mapped_file.get_file_from_offset() as i64,
                    mapped_file.as_ref(),
                    message,
                    prepared,
                    queue_offset,
                    message_num,
                )
            },
        );
        self.resolve_append(outcome, message.topic(), message.born_host().to_string())
    }

    fn append_batch(
        &self,
        batch: &mut MessageExtBatch,
        prepared: &PreparedPayload,
        context: &mut PutMessageContext,
    ) -> (PutMessageResult, Option<Arc<DefaultMappedFile>>) {
        let initial = self.append.get_last_mapped_file(0, false);
        let outcome = CommitLogAppendAttempt::run(
            initial,
            |mapped_file| mapped_file.is_full(),
            || self.append.get_last_mapped_file(0, true),
            |mapped_file| self.prepare_active_segment(mapped_file),
            |mapped_file| {
                self.append_callback.append_prepared_batch(
                    mapped_file.get_file_from_offset() as i64,
                    mapped_file.as_ref(),
                    batch,
                    prepared,
                    context,
                )
            },
        );
        self.resolve_append(
            outcome,
            batch.message_ext_broker_inner.topic(),
            batch.message_ext_broker_inner.born_host().to_string(),
        )
    }

    fn resolve_append(
        &self,
        outcome: rocketmq_store_local::commit_log::append_attempt::CommitLogAppendOutcome<
            Arc<DefaultMappedFile>,
            rocketmq_error::RocketMQError,
        >,
        topic: &str,
        born_host: String,
    ) -> (PutMessageResult, Option<Arc<DefaultMappedFile>>) {
        match outcome.resolve() {
            CommitLogAppendResolution::Continue {
                status,
                result,
                unlock_segment,
            } => (
                PutMessageResult::new_append_result(CommitLog::put_message_status(status), Some(result)),
                unlock_segment,
            ),
            CommitLogAppendResolution::Return {
                status,
                append_result,
                abandoned_segment,
                failure,
            } => {
                match failure {
                    CommitLogAppendFailure::InitialSegmentUnavailable
                    | CommitLogAppendFailure::RolledSegmentUnavailable => {
                        error!(topic, born_host, "Failed to create CommitLog mapped segment");
                    }
                    CommitLogAppendFailure::InitialActiveLockFailed { error }
                    | CommitLogAppendFailure::RolledActiveLockFailed { error } => {
                        error!(%error, topic, born_host, "Failed to lock active CommitLog mapped segment");
                    }
                    CommitLogAppendFailure::InitialMessageIllegal | CommitLogAppendFailure::InitialUnknown => {}
                }
                drop(abandoned_segment);
                (
                    PutMessageResult::new_append_result(CommitLog::put_message_status(status), append_result),
                    None,
                )
            }
        }
    }

    fn prepare_active_segment(&self, mapped_file: &Arc<DefaultMappedFile>) -> RocketMQResult<()> {
        let target = CommitLog::active_memory_lock_target_for_config(
            self.message_store_config.as_ref(),
            mapped_file.get_wrote_position().max(0) as u64,
            mapped_file.get_file_size(),
        );
        let (active_memory_lock, active_memory_lock_present) = self.runtime_state.active_memory_lock_parts();
        CommitLog::ensure_active_mapped_file_locked_parts(
            active_memory_lock,
            active_memory_lock_present,
            target,
            mapped_file.get_file_from_offset(),
            |manager, target| mapped_file.lock_region(manager, target.category, target.offset, target.len),
            |manager, handle| manager.unlock_region(handle),
        )
    }

    fn assign_offset(&self, message: &mut MessageExtBrokerInner) {
        let transaction = MessageSysFlag::get_transaction_value(message.sys_flag());
        if matches!(
            transaction,
            MessageSysFlag::TRANSACTION_NOT_TYPE | MessageSysFlag::TRANSACTION_COMMIT_TYPE
        ) {
            self.consume_queue_store.assign_queue_offset(message);
        }
    }

    fn increase_offset(&self, message: &MessageExtBrokerInner, message_count: i16) {
        let transaction = MessageSysFlag::get_transaction_value(message.sys_flag());
        if matches!(
            transaction,
            MessageSysFlag::TRANSACTION_NOT_TYPE | MessageSysFlag::TRANSACTION_COMMIT_TYPE
        ) {
            self.consume_queue_store.increase_queue_offset(message, message_count);
        }
    }

    fn record_put_message_stats(&self, topic: &str, append_result: &AppendMessageResult) {
        if append_result.msg_num > 0 {
            self.store_context
                .store_stats_service
                .add_single_put_message_topic_times_total(topic, append_result.msg_num as usize);
        }
        if append_result.wrote_bytes > 0 {
            self.store_context
                .store_stats_service
                .add_single_put_message_topic_size_total(topic, append_result.wrote_bytes as usize);
        }
    }
}

async fn run_append_worker(
    mut receiver: AppendSequencerReceiver<AppendRequest>,
    processor: CommitLogAppendProcessor,
    cancellation: tokio_util::sync::CancellationToken,
    port: CommitLogAppendPort,
) {
    struct WorkerExitGuard(CommitLogAppendPort);

    impl Drop for WorkerExitGuard {
        fn drop(&mut self) {
            let discarded_requests = self.0.close_and_discard_pending();
            if discarded_requests > 0 {
                warn!(
                    discarded_requests,
                    "CommitLog append worker exited before processing every admitted request"
                );
            }
        }
    }

    let _exit_guard = WorkerExitGuard(port);
    while let Some(batch) = receiver.next_batch(&cancellation).await {
        processor.process_batch(batch).await;
    }
}
