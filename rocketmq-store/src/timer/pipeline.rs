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
use rocketmq_runtime::QueuePushErrorKind;
use rocketmq_runtime::QueueSnapshot;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskKind;
use rocketmq_store_api::TimerEngineEpoch;

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
struct CompletionEvent {
    stage: PipelineStage,
    sequence: u64,
    epoch: u64,
    messages: usize,
    durable: bool,
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
        }))
    }

    pub(crate) fn spawn<E>(
        self: &Arc<Self>,
        task_group: &TaskGroup,
        engine: E,
        source_workers: usize,
        due_workers: usize,
        completion_gap_limit: usize,
    ) -> bool
    where
        E: TimerEngine + Clone + Send + Sync + 'static,
    {
        let source_workers = source_workers.max(1);
        let due_workers = due_workers.max(1);
        let total_workers = source_workers.saturating_add(due_workers);
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
        if task_group
            .spawn("timer-completion-coordinator", TaskKind::Worker, async move {
                pipeline.run_completion_coordinator(completion_gap_limit).await;
            })
            .is_err()
        {
            return false;
        }
        true
    }

    /// Submits due work before source work. Both calls are non-blocking, so a saturated stage
    /// cannot pin the scheduler. Source still receives one admission opportunity per tick.
    pub(crate) fn submit_tick(&self, epoch: Option<u64>) {
        if self.metrics.completion_backpressured.load(Ordering::Acquire) {
            self.metrics.rejected_submissions.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Some(epoch) = epoch {
            self.submit(PipelineStage::Due, epoch);
        }
        self.submit(PipelineStage::Source, 0);
    }

    pub(crate) fn close(&self) {
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

    fn submit(&self, stage: PipelineStage, epoch: u64) {
        let sequence_lock = match stage {
            PipelineStage::Source => &self.next_source_sequence,
            PipelineStage::Due => &self.next_due_sequence,
        };
        let mut sequence = sequence_lock.lock();
        let request = PumpRequest {
            sequence: sequence.saturating_add(1),
            epoch,
        };
        let queue = match stage {
            PipelineStage::Source => &self.source_queue,
            PipelineStage::Due => &self.due_queue,
        };
        if queue.try_push_data(request, PUMP_RETAINED_BYTES).is_ok() {
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
            let progress = self.run_with_retry(&engine, PipelineStage::Source, request.epoch).await;
            self.publish_completion(PipelineStage::Source, request, progress).await;
        }
    }

    async fn run_due_worker<E>(&self, engine: E)
    where
        E: TimerEngine,
    {
        while let Some(request) = self.due_queue.recv().await {
            let _active = ActiveWorkerGuard::new(&self.metrics.active_due_workers);
            let progress = self.run_with_retry(&engine, PipelineStage::Due, request.epoch).await;
            self.publish_completion(PipelineStage::Due, request, progress).await;
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
        let mut event = CompletionEvent {
            stage,
            sequence: request.sequence,
            epoch: request.epoch,
            messages: progress.messages,
            durable: progress.durable,
        };
        loop {
            let deadline = Instant::now() + self.work_timeout;
            match self
                .completion_queue
                .push_until(event, COMPLETION_RETAINED_BYTES, BudgetClass::Control, deadline.into())
                .await
            {
                Ok(_) => return,
                Err(error) if matches!(error.kind(), QueuePushErrorKind::DeadlineExceeded) => {
                    event = error.into_item();
                    self.metrics.completion_backpressured.store(true, Ordering::Release);
                    self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) if matches!(error.kind(), QueuePushErrorKind::Closed) => return,
                Err(_) => {
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
            match event.stage {
                PipelineStage::Source => {
                    self.metrics
                        .source_observed_completion
                        .store(source.observed_through(), Ordering::Relaxed);
                    self.metrics
                        .source_durable_completion
                        .store(source.durable_through(), Ordering::Relaxed);
                }
                PipelineStage::Due => {
                    self.metrics
                        .due_observed_completion
                        .store(due.observed_through(), Ordering::Relaxed);
                    self.metrics
                        .due_durable_completion
                        .store(due.durable_through(), Ordering::Relaxed);
                }
            }
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
