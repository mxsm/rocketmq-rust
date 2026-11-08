// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueueEnqueueStatus;
use rocketmq_runtime::QueuePushRejection;
use rocketmq_runtime::QueueSnapshot;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store_api::TimerEngineEpoch;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::config::message_store_config::MessageStoreConfig;
use crate::runtime::StoreRuntimeScope;
use crate::timer::completion::CompletionDisposition;
use crate::timer::completion::OrderedCompletionTracker;
use crate::timer::engine::TimerEngine;
use crate::timer::engine::WorkBudget;
use crate::timer::error::RetryPolicy;

const PUMP_RETAINED_BYTES: usize = std::mem::size_of::<PumpRequest>();
const COMPLETION_RETAINED_BYTES: usize = std::mem::size_of::<CompletionEvent>();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PipelineStage {
    Source,
    Due,
}

#[derive(Clone, Copy, Debug)]
struct PumpRequest {
    sequence: u64,
    epoch: u64,
}

#[derive(Clone, Copy, Debug)]
enum CompletionEvent {
    Request(RequestCompletion),
    Checkpoint(CheckpointCompletion),
}

#[derive(Clone, Copy, Debug)]
struct RequestCompletion {
    stage: PipelineStage,
    sequence: u64,
    epoch: u64,
    messages: usize,
    durable: bool,
}

/// Requests made durable by one engine checkpoint: Source requests through `source_through` and
/// Due requests through `due`.
#[derive(Clone, Copy, Debug)]
struct CheckpointCompletion {
    source_through: u64,
    due: Option<FinishedRequest>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FinishedRequest {
    epoch: u64,
    sequence: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TimerPipelineMetrics {
    active_source_workers: AtomicUsize,
    active_due_workers: AtomicUsize,
    completed_messages: AtomicU64,
    retries: AtomicU64,
    quarantined: AtomicU64,
    stale_completions: AtomicU64,
    rejected_submissions: AtomicU64,
    completion_gaps: AtomicUsize,
    completion_backpressured: AtomicBool,
    source_observed_completion: AtomicU64,
    source_durable_completion: AtomicU64,
    due_observed_completion: AtomicU64,
    due_durable_completion: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct TimerPipelineDiagnostics {
    pub(crate) source_queue: QueueSnapshot,
    pub(crate) due_queue: QueueSnapshot,
    pub(crate) completion_queue: QueueSnapshot,
    pub(crate) active_source_workers: usize,
    pub(crate) active_due_workers: usize,
    pub(crate) completed_messages: u64,
    pub(crate) retries: u64,
    pub(crate) quarantined: u64,
    pub(crate) stale_completions: u64,
    pub(crate) rejected_submissions: u64,
    pub(crate) completion_gaps: usize,
    pub(crate) configured_source_workers: usize,
    pub(crate) configured_due_workers: usize,
    pub(crate) source_observed_completion: u64,
    pub(crate) source_durable_completion: u64,
    pub(crate) due_observed_completion: u64,
    pub(crate) due_durable_completion: u64,
}

impl TimerPipelineDiagnostics {
    pub fn source_queue_messages(&self) -> usize {
        self.source_queue.depth
    }

    pub fn source_queue_bytes(&self) -> usize {
        self.source_queue.retained_bytes
    }

    pub fn due_queue_messages(&self) -> usize {
        self.due_queue.depth
    }

    pub fn due_queue_bytes(&self) -> usize {
        self.due_queue.retained_bytes
    }

    pub const fn configured_source_workers(&self) -> usize {
        self.configured_source_workers
    }

    pub const fn configured_due_workers(&self) -> usize {
        self.configured_due_workers
    }

    pub const fn completion_gaps(&self) -> usize {
        self.completion_gaps
    }

    pub const fn rejected_submissions(&self) -> u64 {
        self.rejected_submissions
    }
}

pub(crate) struct TimerPipeline {
    source_queue: BudgetedQueue<PumpRequest>,
    due_queue: BudgetedQueue<PumpRequest>,
    completion_queue: BudgetedQueue<CompletionEvent>,
    next_source_sequence: Mutex<u64>,
    next_due_sequence: Mutex<u64>,
    source_batch_messages: usize,
    due_batch_messages: usize,
    work_bytes: usize,
    work_timeout: Duration,
    retry_policy: RetryPolicy,
    metrics: Arc<TimerPipelineMetrics>,
    configured_source_workers: usize,
    configured_due_workers: usize,
    remaining_workers: AtomicUsize,
    checkpoint_interval: Duration,
    /// Last Source request whose batches have all returned. Requests of one stage run one at a
    /// time, so every earlier request has returned as well.
    finished_source: AtomicU64,
    /// Last Due request whose batches have all returned.
    finished_due: Mutex<Option<FinishedRequest>>,
    closed: CancellationToken,
}

impl TimerPipeline {
    pub(crate) fn new(runtime_scope: &StoreRuntimeScope, config: &MessageStoreConfig) -> Option<Arc<Self>> {
        let minimum_queue_bytes = PUMP_RETAINED_BYTES.max(COMPLETION_RETAINED_BYTES);
        if config.timer_pipeline_queue_messages == 0
            || config.timer_pipeline_queue_bytes < minimum_queue_bytes
            || config.timer_source_batch_messages == 0
            || config.timer_due_batch_messages == 0
            || config.timer_completion_gap_limit == 0
            || config.timer_put_message_thread_num == 0
            || config.timer_get_message_thread_num == 0
            || config.timer_retry_max_attempts == 0
            || config.timer_retry_initial_backoff_ms == 0
            || config.timer_retry_max_backoff_ms < config.timer_retry_initial_backoff_ms
        {
            return None;
        }
        let queue_limit = BudgetLimit::new(
            config.timer_pipeline_queue_messages,
            config.timer_pipeline_queue_bytes,
            FullPolicy::Reject,
        );
        let parent = runtime_scope.resource_budget();
        let source_budget = parent.child("timer-source-pipeline", queue_limit).ok()?;
        let due_budget = parent.child("timer-due-pipeline", queue_limit).ok()?;
        let completion_budget = parent
            .child(
                "timer-completion-pipeline",
                BudgetLimit::new(
                    config.timer_pipeline_queue_messages,
                    config.timer_pipeline_queue_bytes,
                    FullPolicy::WaitUntilDeadline,
                ),
            )
            .ok()?;

        Some(Arc::new(Self {
            source_queue: BudgetedQueue::new(source_budget),
            due_queue: BudgetedQueue::new(due_budget),
            completion_queue: BudgetedQueue::new(completion_budget),
            next_source_sequence: Mutex::new(0),
            next_due_sequence: Mutex::new(0),
            source_batch_messages: config.timer_source_batch_messages,
            due_batch_messages: config.timer_due_batch_messages,
            work_bytes: config.timer_pipeline_queue_bytes,
            work_timeout: Duration::from_millis(config.timer_precision_ms.max(100).saturating_mul(4)),
            retry_policy: RetryPolicy::new(
                config.timer_retry_max_attempts,
                Duration::from_millis(config.timer_retry_initial_backoff_ms),
                Duration::from_millis(config.timer_retry_max_backoff_ms),
            ),
            metrics: Arc::new(TimerPipelineMetrics::default()),
            configured_source_workers: config.timer_put_message_thread_num,
            configured_due_workers: config.timer_get_message_thread_num,
            remaining_workers: AtomicUsize::new(0),
            checkpoint_interval: Duration::from_millis(config.timer_flush_interval_ms.max(1) as u64),
            finished_source: AtomicU64::new(0),
            finished_due: Mutex::new(None),
            closed: CancellationToken::new(),
        }))
    }

    /// Spawns the stage workers, the checkpoint worker and the completion coordinator, plus a
    /// worker that submits a Source request whenever `source_wake` is notified.
    pub(crate) fn spawn<E>(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        engine: E,
        source_workers: usize,
        due_workers: usize,
        completion_gap_limit: usize,
        source_wake: Option<Arc<Notify>>,
    ) -> bool
    where
        E: TimerEngine + Clone + Send + Sync + 'static,
    {
        let source_workers = source_workers.max(1);
        let due_workers = due_workers.max(1);
        // The checkpoint worker holds a lease too, so completions close only after its last
        // checkpoint event.
        let total_workers = source_workers.saturating_add(due_workers).saturating_add(1);
        self.remaining_workers.store(total_workers, Ordering::Release);
        let mut attempted_workers = 0usize;
        for worker_id in 0..source_workers {
            let pipeline = Arc::clone(self);
            let lease = PipelineWorkerLease {
                pipeline: Arc::clone(&pipeline),
            };
            let engine = engine.clone();
            attempted_workers += 1;
            if task_group
                .spawn(
                    format!("timer-source-worker-{worker_id}"),
                    TaskKind::Worker,
                    async move {
                        let _lease = lease;
                        pipeline.run_source_worker(engine).await;
                    },
                )
                .is_err()
            {
                self.abandon_unspawned(total_workers.saturating_sub(attempted_workers));
                return false;
            }
        }
        for worker_id in 0..due_workers {
            let pipeline = Arc::clone(self);
            let lease = PipelineWorkerLease {
                pipeline: Arc::clone(&pipeline),
            };
            let engine = engine.clone();
            attempted_workers += 1;
            if task_group
                .spawn(format!("timer-due-worker-{worker_id}"), TaskKind::Worker, async move {
                    let _lease = lease;
                    pipeline.run_due_worker(engine).await
                })
                .is_err()
            {
                self.abandon_unspawned(total_workers.saturating_sub(attempted_workers));
                return false;
            }
        }
        let pipeline = Arc::clone(self);
        let lease = PipelineWorkerLease {
            pipeline: Arc::clone(&pipeline),
        };
        attempted_workers += 1;
        if task_group
            .spawn("timer-checkpoint-worker", TaskKind::Worker, async move {
                let _lease = lease;
                pipeline.run_checkpoint_worker(engine).await;
            })
            .is_err()
        {
            self.abandon_unspawned(total_workers.saturating_sub(attempted_workers));
            return false;
        }
        let pipeline = Arc::clone(self);
        if task_group
            .spawn("timer-completion-coordinator", TaskKind::Worker, async move {
                pipeline.run_completion_coordinator(completion_gap_limit).await;
            })
            .is_err()
        {
            return false;
        }
        if let Some(source_wake) = source_wake {
            let pipeline = Arc::clone(self);
            if task_group
                .spawn("timer-source-wake", TaskKind::Worker, async move {
                    pipeline.run_source_wake(source_wake).await;
                })
                .is_err()
            {
                self.close();
                return false;
            }
        }
        true
    }

    /// Submits due work before source work. Both calls are non-blocking, so a saturated stage
    /// cannot pin the scheduler. A stage that still has a queued or running request is skipped:
    /// that request keeps running batches while they come back full, so a tick only restarts a
    /// stage that has gone idle.
    pub(crate) fn submit_tick(&self, epoch: Option<u64>) {
        if self.metrics.completion_backpressured.load(Ordering::Acquire) {
            self.metrics.rejected_submissions.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Some(epoch) = epoch {
            self.submit_if_idle(PipelineStage::Due, epoch);
        }
        self.submit_if_idle(PipelineStage::Source, 0);
    }

    /// Submits a Source request each time Reput reports dispatched Timer topic records, so new
    /// sources are indexed without waiting for the next tick.
    async fn run_source_wake(&self, source_wake: Arc<Notify>) {
        loop {
            tokio::select! {
                () = self.closed.cancelled() => return,
                () = source_wake.notified() => {}
            }
            if self.metrics.completion_backpressured.load(Ordering::Acquire) {
                self.metrics.rejected_submissions.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.submit_if_idle(PipelineStage::Source, 0);
        }
    }

    fn is_idle(&self, stage: PipelineStage) -> bool {
        let (queue, active_workers) = match stage {
            PipelineStage::Source => (&self.source_queue, &self.metrics.active_source_workers),
            PipelineStage::Due => (&self.due_queue, &self.metrics.active_due_workers),
        };
        queue.is_empty() && active_workers.load(Ordering::Acquire) == 0
    }

    pub(crate) fn close(&self) {
        self.closed.cancel();
        self.source_queue.close();
        self.due_queue.close();
        if self.remaining_workers.load(Ordering::Acquire) == 0 {
            self.completion_queue.close();
        }
    }

    fn abandon_unspawned(&self, unspawned: usize) {
        if unspawned > 0 {
            self.remaining_workers.fetch_sub(unspawned, Ordering::AcqRel);
        }
        self.close();
    }

    pub(crate) fn snapshot(&self) -> TimerPipelineDiagnostics {
        TimerPipelineDiagnostics {
            source_queue: self.source_queue.snapshot(),
            due_queue: self.due_queue.snapshot(),
            completion_queue: self.completion_queue.snapshot(),
            active_source_workers: self.metrics.active_source_workers.load(Ordering::Relaxed),
            active_due_workers: self.metrics.active_due_workers.load(Ordering::Relaxed),
            completed_messages: self.metrics.completed_messages.load(Ordering::Relaxed),
            retries: self.metrics.retries.load(Ordering::Relaxed),
            quarantined: self.metrics.quarantined.load(Ordering::Relaxed),
            stale_completions: self.metrics.stale_completions.load(Ordering::Relaxed),
            rejected_submissions: self.metrics.rejected_submissions.load(Ordering::Relaxed),
            completion_gaps: self.metrics.completion_gaps.load(Ordering::Relaxed),
            configured_source_workers: self.configured_source_workers,
            configured_due_workers: self.configured_due_workers,
            source_observed_completion: self.metrics.source_observed_completion.load(Ordering::Relaxed),
            source_durable_completion: self.metrics.source_durable_completion.load(Ordering::Relaxed),
            due_observed_completion: self.metrics.due_observed_completion.load(Ordering::Relaxed),
            due_durable_completion: self.metrics.due_durable_completion.load(Ordering::Relaxed),
        }
    }

    /// Submits a request for `stage` unless one is queued or running. The tick and the source
    /// wake both submit, so the idle check runs under the stage's sequence lock.
    fn submit_if_idle(&self, stage: PipelineStage, epoch: u64) {
        let sequence_lock = match stage {
            PipelineStage::Source => &self.next_source_sequence,
            PipelineStage::Due => &self.next_due_sequence,
        };
        let mut sequence = sequence_lock.lock();
        if !self.is_idle(stage) {
            return;
        }
        let request = PumpRequest {
            sequence: sequence.saturating_add(1),
            epoch,
        };
        let queue = match stage {
            PipelineStage::Source => &self.source_queue,
            PipelineStage::Due => &self.due_queue,
        };
        if !matches!(
            queue.try_push_data(request, PUMP_RETAINED_BYTES),
            QueueEnqueueStatus::Rejected { .. }
        ) {
            *sequence = request.sequence;
        } else {
            self.metrics.rejected_submissions.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn run_source_worker<E>(&self, engine: E)
    where
        E: TimerEngine,
    {
        while let Some(request) = self.source_queue.recv().await {
            let _active = ActiveWorkerGuard::new(&self.metrics.active_source_workers);
            let progress = self.run_until_idle(&engine, PipelineStage::Source, request.epoch).await;
            self.finished_source.fetch_max(request.sequence, Ordering::AcqRel);
            self.publish_completion(PipelineStage::Source, request, progress).await;
        }
    }

    async fn run_due_worker<E>(&self, engine: E)
    where
        E: TimerEngine,
    {
        while let Some(request) = self.due_queue.recv().await {
            let _active = ActiveWorkerGuard::new(&self.metrics.active_due_workers);
            let progress = self.run_until_idle(&engine, PipelineStage::Due, request.epoch).await;
            let finished = FinishedRequest {
                epoch: request.epoch,
                sequence: request.sequence,
            };
            {
                let mut finished_due = self.finished_due.lock();
                if finished_due.is_none_or(|current| finished > current) {
                    *finished_due = Some(finished);
                }
            }
            self.publish_completion(PipelineStage::Due, request, progress).await;
        }
    }

    /// Commits engine progress once per `timerFlushIntervalMs`, like Java's `TimerFlushService`,
    /// and reports every request that had finished before the commit as durable. Stage batches
    /// do not commit on their own.
    async fn run_checkpoint_worker<E>(&self, engine: E)
    where
        E: TimerEngine,
    {
        loop {
            tokio::select! {
                () = self.closed.cancelled() => return,
                () = tokio::time::sleep(self.checkpoint_interval) => {}
            }
            // Read before the commit starts: a request counted here has returned from all of its
            // batches, so the commit covers its changes. Requests that finish later wait for the
            // next checkpoint.
            let covered = CheckpointCompletion {
                source_through: self.finished_source.load(Ordering::Acquire),
                due: *self.finished_due.lock(),
            };
            match engine.checkpoint().await {
                Ok(true) => self.publish_event(CompletionEvent::Checkpoint(covered)).await,
                // A refused or failed commit is retried on the next interval; the stages keep
                // running in the meantime.
                Ok(false) => {}
                Err(_) => {
                    self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Runs batches for one pump request while they come back full, so a backlog drains at the
    /// rate the stage can process it rather than one batch per scheduler tick. The request ends
    /// with the first batch that is not full, when its slice elapses, or when the pipeline
    /// closes. Source and Due batches still interleave on the store's process lock.
    async fn run_until_idle<E>(
        &self,
        engine: &E,
        stage: PipelineStage,
        epoch: u64,
    ) -> crate::timer::request::EngineBatchProgress
    where
        E: TimerEngine,
    {
        let (batch_messages, queue) = match stage {
            PipelineStage::Source => (self.source_batch_messages, &self.source_queue),
            PipelineStage::Due => (self.due_batch_messages, &self.due_queue),
        };
        let slice_deadline = Instant::now() + self.work_timeout;
        let mut total = crate::timer::request::EngineBatchProgress::empty();
        loop {
            let progress = self.run_with_retry(engine, stage, epoch).await;
            total.messages = total.messages.saturating_add(progress.messages);
            total.bytes = total.bytes.saturating_add(progress.bytes);
            total.continuation = progress.continuation;
            // The last batch's commit covers every earlier batch of this request.
            total.durable = progress.durable;
            if progress.messages < batch_messages || queue.is_closed() || Instant::now() >= slice_deadline {
                return total;
            }
        }
    }

    async fn run_with_retry<E>(
        &self,
        engine: &E,
        stage: PipelineStage,
        epoch: u64,
    ) -> crate::timer::request::EngineBatchProgress
    where
        E: TimerEngine,
    {
        for attempt in 1..=self.retry_policy.max_attempts() {
            let max_messages = match stage {
                PipelineStage::Source => self.source_batch_messages,
                PipelineStage::Due => self.due_batch_messages,
            };
            let budget = match WorkBudget::try_new(max_messages, self.work_bytes, Instant::now() + self.work_timeout) {
                Some(budget) => budget,
                None => {
                    return crate::timer::request::EngineBatchProgress {
                        durable: false,
                        ..crate::timer::request::EngineBatchProgress::empty()
                    };
                }
            };
            let result = match stage {
                PipelineStage::Source => engine.enqueue_source(budget).await,
                PipelineStage::Due => engine.roll_due(TimerEngineEpoch::new(epoch), budget).await,
            };
            match result {
                Ok(progress) => return progress,
                Err(_) if attempt < self.retry_policy.max_attempts() => {
                    self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(self.retry_policy.delay(attempt, epoch ^ u64::from(attempt))).await;
                }
                Err(_) => {
                    self.metrics.quarantined.fetch_add(1, Ordering::Relaxed);
                    return crate::timer::request::EngineBatchProgress {
                        durable: false,
                        ..crate::timer::request::EngineBatchProgress::empty()
                    };
                }
            }
        }
        crate::timer::request::EngineBatchProgress {
            durable: false,
            ..crate::timer::request::EngineBatchProgress::empty()
        }
    }

    async fn publish_completion(
        &self,
        stage: PipelineStage,
        request: PumpRequest,
        progress: crate::timer::request::EngineBatchProgress,
    ) {
        self.publish_event(CompletionEvent::Request(RequestCompletion {
            stage,
            sequence: request.sequence,
            epoch: request.epoch,
            messages: progress.messages,
            durable: progress.durable,
        }))
        .await;
    }

    async fn publish_event(&self, mut event: CompletionEvent) {
        loop {
            let deadline = Instant::now() + self.work_timeout;
            match self
                .completion_queue
                .push_until(event, COMPLETION_RETAINED_BYTES, BudgetClass::Control, deadline.into())
                .await
            {
                QueueEnqueueStatus::Enqueued
                | QueueEnqueueStatus::Coalesced { .. }
                | QueueEnqueueStatus::DroppedStale { .. } => return,
                QueueEnqueueStatus::Rejected {
                    item,
                    rejection: QueuePushRejection::DeadlineExceeded,
                } => {
                    event = item;
                    self.metrics.completion_backpressured.store(true, Ordering::Release);
                    self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                }
                QueueEnqueueStatus::Rejected {
                    rejection: QueuePushRejection::Closed,
                    ..
                } => return,
                QueueEnqueueStatus::Rejected { .. } => {
                    // The event is smaller than every validated completion queue. A permanent
                    // budget failure therefore indicates a broken runtime budget tree, not load.
                    self.metrics.quarantined.fetch_add(1, Ordering::Relaxed);
                    self.metrics.completion_backpressured.store(true, Ordering::Release);
                    return;
                }
            }
        }
    }

    async fn run_completion_coordinator(&self, gap_limit: usize) {
        let mut source = OrderedCompletionTracker::new(0, 0, gap_limit);
        let mut due = OrderedCompletionTracker::new(0, 0, gap_limit);
        while let Some(event) = self.completion_queue.recv().await {
            match event {
                CompletionEvent::Request(event) => {
                    let tracker = match event.stage {
                        PipelineStage::Source => &mut source,
                        PipelineStage::Due => {
                            if event.epoch > due.epoch() {
                                due.reset_epoch(event.epoch, event.sequence.saturating_sub(1));
                            }
                            &mut due
                        }
                    };
                    let disposition = if event.durable {
                        tracker.commit_prefix(event.epoch, event.sequence)
                    } else {
                        tracker.observe_pending(event.epoch, event.sequence)
                    };
                    match disposition {
                        CompletionDisposition::StaleEpoch => {
                            self.metrics.stale_completions.fetch_add(1, Ordering::Relaxed);
                        }
                        CompletionDisposition::GapLimitReached => {
                            self.metrics.quarantined.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            self.metrics
                                .completed_messages
                                .fetch_add(event.messages as u64, Ordering::Relaxed);
                        }
                    }
                }
                CompletionEvent::Checkpoint(covered) => {
                    if covered.source_through > 0 {
                        source.commit_prefix(source.epoch(), covered.source_through);
                    }
                    if let Some(finished) = covered.due {
                        if finished.epoch > due.epoch() {
                            due.reset_epoch(finished.epoch, finished.sequence);
                        } else {
                            // A request of an older delivery epoch is stale; the tracker ignores it.
                            due.commit_prefix(finished.epoch, finished.sequence);
                        }
                    }
                }
            }
            self.metrics
                .source_observed_completion
                .store(source.observed_through(), Ordering::Relaxed);
            self.metrics
                .source_durable_completion
                .store(source.durable_through(), Ordering::Relaxed);
            self.metrics
                .due_observed_completion
                .store(due.observed_through(), Ordering::Relaxed);
            self.metrics
                .due_durable_completion
                .store(due.durable_through(), Ordering::Relaxed);
            self.metrics
                .completion_gaps
                .store(source.gap_count().saturating_add(due.gap_count()), Ordering::Relaxed);
            self.metrics
                .completion_backpressured
                .store(!source.can_accept() || !due.can_accept(), Ordering::Release);
        }
    }
}

struct ActiveWorkerGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> ActiveWorkerGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for ActiveWorkerGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

struct PipelineWorkerLease {
    pipeline: Arc<TimerPipeline>,
}

impl Drop for PipelineWorkerLease {
    fn drop(&mut self) {
        if self.pipeline.remaining_workers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.pipeline.completion_queue.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use rocketmq_store_api::TimerEngineEpoch;
    use rocketmq_store_api::TimerEngineId;
    use rocketmq_store_api::TimerId;
    use tokio::sync::Notify;
    use tokio::sync::Semaphore;

    use super::TimerPipeline;
    use crate::config::message_store_config::MessageStoreConfig;
    use crate::timer::engine::TimerEngine;
    use crate::timer::engine::WorkBudget;
    use crate::timer::error::TimerEngineError;
    use crate::timer::request::EngineBatchProgress;

    /// Serves `full_batches` full Source batches and then partial ones. Every Source batch waits
    /// for a `permits` permit and signals `entered` first, so a test can hold a batch open.
    #[derive(Clone)]
    struct ScriptedEngine {
        full_batches: usize,
        source_calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
        permits: Arc<Semaphore>,
        batches_durable: bool,
        checkpoints: Arc<AtomicUsize>,
    }

    impl ScriptedEngine {
        fn new(full_batches: usize, permits: usize) -> Self {
            Self {
                full_batches,
                source_calls: Arc::new(AtomicUsize::new(0)),
                entered: Arc::new(Notify::new()),
                permits: Arc::new(Semaphore::new(permits)),
                batches_durable: true,
                checkpoints: Arc::new(AtomicUsize::new(0)),
            }
        }

        /// Batches report pending progress, as the Java-compatible engine's batches do.
        fn with_pending_batches(mut self) -> Self {
            self.batches_durable = false;
            self
        }
    }

    impl TimerEngine for ScriptedEngine {
        fn engine_id(&self) -> TimerEngineId {
            TimerEngineId::JavaCompat
        }

        async fn load(&self) -> Result<(), TimerEngineError> {
            Ok(())
        }

        async fn enqueue_source(&self, budget: WorkBudget) -> Result<EngineBatchProgress, TimerEngineError> {
            self.entered.notify_one();
            self.permits
                .acquire()
                .await
                .expect("test semaphore stays open")
                .forget();
            let call = self.source_calls.fetch_add(1, Ordering::AcqRel);
            let messages = if call < self.full_batches {
                budget.max_messages
            } else {
                1
            };
            Ok(EngineBatchProgress {
                messages,
                durable: self.batches_durable,
                ..EngineBatchProgress::empty()
            })
        }

        async fn roll_due(
            &self,
            _epoch: TimerEngineEpoch,
            _budget: WorkBudget,
        ) -> Result<EngineBatchProgress, TimerEngineError> {
            Ok(EngineBatchProgress {
                durable: self.batches_durable,
                ..EngineBatchProgress::empty()
            })
        }

        async fn complete(&self, _timer_id: TimerId, _epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
            Ok(())
        }

        async fn cancel(&self, _timer_id: TimerId) -> Result<(), TimerEngineError> {
            Ok(())
        }

        async fn checkpoint(&self) -> Result<bool, TimerEngineError> {
            self.checkpoints.fetch_add(1, Ordering::AcqRel);
            Ok(true)
        }

        async fn on_role_change(&self, _active: bool, _epoch: TimerEngineEpoch) -> Result<(), TimerEngineError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), TimerEngineError> {
            Ok(())
        }
    }

    fn spawn_pipeline(
        name: &'static str,
        engine: &ScriptedEngine,
    ) -> (Arc<TimerPipeline>, rocketmq_runtime::TaskGroup) {
        spawn_pipeline_with_config(name, engine, MessageStoreConfig::default())
    }

    fn spawn_pipeline_with_config(
        name: &'static str,
        engine: &ScriptedEngine,
        config: MessageStoreConfig,
    ) -> (Arc<TimerPipeline>, rocketmq_runtime::TaskGroup) {
        spawn_pipeline_with_wake(name, engine, config, None)
    }

    fn spawn_pipeline_with_wake(
        name: &'static str,
        engine: &ScriptedEngine,
        config: MessageStoreConfig,
        source_wake: Option<Arc<Notify>>,
    ) -> (Arc<TimerPipeline>, rocketmq_runtime::TaskGroup) {
        let scope = crate::runtime::test_scope(name);
        let pipeline = TimerPipeline::new(&scope, &config).expect("default pipeline configuration is valid");
        let task_group = scope.task_group(name);
        assert!(pipeline.spawn(
            &task_group,
            engine.clone(),
            1,
            1,
            config.timer_completion_gap_limit,
            source_wake
        ));
        (pipeline, task_group)
    }

    async fn shutdown_pipeline(pipeline: &TimerPipeline, task_group: &rocketmq_runtime::TaskGroup) {
        pipeline.close();
        let report = task_group.shutdown(Duration::from_secs(5)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }

    #[tokio::test]
    async fn one_tick_keeps_running_source_batches_while_they_come_back_full() {
        let engine = ScriptedEngine::new(5, Semaphore::MAX_PERMITS);
        let (pipeline, task_group) = spawn_pipeline("timer-pipeline-backlog-test", &engine);

        pipeline.submit_tick(None);
        tokio::time::timeout(Duration::from_secs(5), async {
            while engine.source_calls.load(Ordering::Acquire) < 6 || pipeline.snapshot().completed_messages < 5 * 64 + 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a single tick drains the whole backlog");

        // Five full batches and the partial one that ends the request, all from a single tick.
        assert_eq!(engine.source_calls.load(Ordering::Acquire), 6);
        assert_eq!(pipeline.snapshot().completed_messages, 5 * 64 + 1);
        shutdown_pipeline(&pipeline, &task_group).await;
    }

    #[tokio::test]
    async fn tick_skips_a_stage_whose_request_is_still_running() {
        let engine = ScriptedEngine::new(0, 0);
        let (pipeline, task_group) = spawn_pipeline("timer-pipeline-coalesce-test", &engine);

        pipeline.submit_tick(None);
        tokio::time::timeout(Duration::from_secs(5), engine.entered.notified())
            .await
            .expect("the source worker starts its batch");
        pipeline.submit_tick(None);
        pipeline.submit_tick(None);

        assert!(pipeline.source_queue.is_empty());
        assert_eq!(*pipeline.next_source_sequence.lock(), 1);
        assert_eq!(pipeline.snapshot().rejected_submissions(), 0);
        engine.permits.add_permits(1);
        shutdown_pipeline(&pipeline, &task_group).await;
    }

    #[tokio::test]
    async fn a_source_wake_starts_a_source_request_without_a_tick() {
        let engine = ScriptedEngine::new(0, 0);
        let source_wake = Arc::new(Notify::new());
        let (pipeline, task_group) = spawn_pipeline_with_wake(
            "timer-pipeline-wake-test",
            &engine,
            MessageStoreConfig::default(),
            Some(Arc::clone(&source_wake)),
        );

        source_wake.notify_one();
        tokio::time::timeout(Duration::from_secs(5), engine.entered.notified())
            .await
            .expect("a wake starts a Source batch");
        // A wake while that request still runs does not queue another one.
        source_wake.notify_one();
        tokio::task::yield_now().await;
        assert!(pipeline.source_queue.is_empty());
        assert_eq!(*pipeline.next_source_sequence.lock(), 1);
        engine.permits.add_permits(1);
        shutdown_pipeline(&pipeline, &task_group).await;
    }

    #[tokio::test]
    async fn checkpoints_make_pending_completions_of_both_stages_durable() {
        let engine = ScriptedEngine::new(0, Semaphore::MAX_PERMITS).with_pending_batches();
        let config = MessageStoreConfig {
            timer_flush_interval_ms: 10,
            ..MessageStoreConfig::default()
        };
        let (pipeline, task_group) = spawn_pipeline_with_config("timer-pipeline-checkpoint-test", &engine, config);

        pipeline.submit_tick(Some(1));
        // Neither stage reports a durable batch, so only a checkpoint can advance either prefix.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = pipeline.snapshot();
                if snapshot.source_durable_completion == 1 && snapshot.due_durable_completion == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a checkpoint covers the finished Source and Due requests");

        let snapshot = pipeline.snapshot();
        assert_eq!(snapshot.completion_gaps(), 0);
        assert!(engine.checkpoints.load(Ordering::Acquire) > 0);
        shutdown_pipeline(&pipeline, &task_group).await;
    }
}
