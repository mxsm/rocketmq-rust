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

use std::collections::hash_map::DefaultHasher;
use std::collections::BinaryHeap;
use std::future::Future;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::UnifiedServiceError;
use rocketmq_model::common::message::message_enum::MessageRequestMode;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::BudgetedItem;
use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::QueuePushOutcome;
use rocketmq_runtime::RateLimit;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourcePermit;
use rocketmq_runtime::Shutdown;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::runtime::spawn_client_task_with_context;
use crate::runtime::spawn_client_tracked_task_with_context;
use crate::runtime::ClientRuntime;
use crate::runtime::ClientTrackedTaskHandle;
use rocketmq_runtime::ChildServiceContext;

/// Default queue capacity for message requests
const DEFAULT_QUEUE_CAPACITY: usize = 4096;

/// Maximum default shard count for pull request workers.
const DEFAULT_MAX_PULL_SHARDS: usize = 8;

/// Default shutdown timeout in milliseconds
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 1000;

/// Bounded delay before retrying a POP scheduler admission rejected by the immediate queue.
const POP_REQUEUE_DELAY_MS: u64 = 10;

type BoxedMessageRequest = Box<dyn MessageRequest + Send + 'static>;
type MessageRequestQueue = BudgetedQueue<BoxedMessageRequest>;

/// RocketMQ Consumer Pull Message Service
///
/// # Responsibilities
/// - Asynchronously schedules Pull/Pop message requests
/// - Supports both delayed and immediate scheduling
/// - Manages lifecycle of background pull tasks
///
/// # Thread Model
/// - Main loop: Single task on the injected client runtime
/// - Delayed tasks: Tracked tasks on the same injected runtime
///
/// # Shutdown Semantics
/// - `shutdown()` sends stop signal and waits for graceful termination
/// - Main loop processes current request before exiting
/// - Delayed tasks check `is_stopped()` before execution
#[derive(Clone)]
pub struct PullMessageService {
    service_context: Arc<StdRwLock<Option<ChildServiceContext>>>,
    resource_budget: ResourceBudget,
    lifecycle_transition: Arc<Mutex<()>>,

    /// Immediate Pop request queue.
    tx: Arc<StdRwLock<Option<MessageRequestQueue>>>,

    /// Shutdown signal broadcaster
    tx_shutdown: Arc<StdRwLock<Option<tokio::sync::broadcast::Sender<()>>>>,

    /// Service stopped flag (for fast check)
    stopped: Arc<AtomicBool>,

    /// Queue capacity
    queue_capacity: usize,

    /// Pull request worker shard count.
    shard_count: usize,

    /// Pull request dispatcher installed after service startup.
    pull_dispatcher: Arc<StdMutex<Option<PullRequestDispatcher>>>,

    /// Main service loop task handle
    main_task_handle: Arc<tokio::sync::Mutex<Option<ClientTrackedTaskHandle>>>,

    /// Pull worker task handles.
    pull_shard_task_handles: Arc<tokio::sync::Mutex<Vec<ClientTrackedTaskHandle>>>,

    /// Shared pull worker metrics.
    pull_shard_metrics: Arc<Vec<Arc<PullRequestShardMetrics>>>,

    /// Tracks the shared delayed scheduler and immediate generic tasks.
    scheduled_task_tracker: TaskTracker,

    /// Cancels delayed scheduler and tracked generic tasks during shutdown.
    scheduled_task_shutdown: CancellationToken,

    /// Shared delayed scheduler command queue.
    delayed_scheduler_queue: Arc<StdMutex<Option<BudgetedQueue<DelayedScheduleCommand>>>>,

    /// Shared delayed scheduler metrics.
    delayed_scheduler_metrics: Arc<DelayedSchedulerMetrics>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PullMessageServiceLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
    pub delayed_scheduler: PullMessageServiceDelayedSchedulerSnapshot,
    pub shards: PullMessageServiceShardSnapshot,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct PullMessageServiceDelayedSchedulerSnapshot {
    pub queue_depth: usize,
    pub submitted_count: u64,
    pub expired_count: u64,
    pub cancelled_count: u64,
    pub scheduler_task_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PullMessageServiceShardSnapshot {
    pub shard_count: usize,
    pub pending_count: usize,
    pub submitted_count: u64,
    pub processed_count: u64,
    pub rejected_count: u64,
    pub worker_task_count: usize,
    pub shards: Vec<PullMessageServiceShardMetricSnapshot>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct PullMessageServiceShardMetricSnapshot {
    pub index: usize,
    pub pending_count: usize,
    pub submitted_count: u64,
    pub processed_count: u64,
    pub rejected_count: u64,
}

#[derive(Default)]
struct PullRequestShardMetrics {
    pending_count: AtomicUsize,
    submitted_count: AtomicU64,
    processed_count: AtomicU64,
    rejected_count: AtomicU64,
}

impl PullRequestShardMetrics {
    fn record_submitted(&self) {
        self.submitted_count.fetch_add(1, Ordering::Relaxed);
        self.pending_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_processed(&self) {
        self.processed_count.fetch_add(1, Ordering::Relaxed);
        self.decrement_pending();
    }

    fn record_rejected(&self) {
        self.rejected_count.fetch_add(1, Ordering::Relaxed);
        self.decrement_pending();
    }

    fn decrement_pending(&self) {
        let _ = self
            .pending_count
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            });
    }

    fn snapshot(&self, index: usize) -> PullMessageServiceShardMetricSnapshot {
        PullMessageServiceShardMetricSnapshot {
            index,
            pending_count: self.pending_count.load(Ordering::Relaxed),
            submitted_count: self.submitted_count.load(Ordering::Relaxed),
            processed_count: self.processed_count.load(Ordering::Relaxed),
            rejected_count: self.rejected_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
struct PullRequestDispatcher {
    client_id: CheetahString,
    shards: Arc<Vec<PullRequestShard>>,
}

impl PullRequestDispatcher {
    async fn dispatch(&self, request: PullRequest) {
        let shard_index = pull_request_shard_index(self.client_id.as_str(), &request, self.shards.len());
        let shard = &self.shards[shard_index];
        shard.metrics.record_submitted();
        let retained_bytes = request.retained_bytes();
        if let QueuePushOutcome::Rejected { rejection, .. } = shard.queue.try_push_data(request, retained_bytes) {
            shard.metrics.record_rejected();
            warn!(
                shard_index = shard.index,
                "Failed to send sharded pull request: {:?}", rejection
            );
        }
    }

    fn close(&self) {
        for shard in self.shards.iter() {
            shard.queue.close();
        }
    }
}

#[derive(Clone)]
struct PullRequestShard {
    index: usize,
    queue: BudgetedQueue<PullRequest>,
    metrics: Arc<PullRequestShardMetrics>,
}

#[derive(Default)]
struct DelayedSchedulerMetrics {
    queue_depth: AtomicUsize,
    submitted_count: AtomicU64,
    expired_count: AtomicU64,
    cancelled_count: AtomicU64,
    scheduler_running: AtomicBool,
}

impl DelayedSchedulerMetrics {
    fn record_submitted(&self) {
        self.submitted_count.fetch_add(1, Ordering::Relaxed);
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    fn record_expired(&self) {
        self.expired_count.fetch_add(1, Ordering::Relaxed);
        self.decrement_depth(1);
    }

    fn record_cancelled(&self, count: usize) {
        if count == 0 {
            return;
        }
        self.cancelled_count.fetch_add(count as u64, Ordering::Relaxed);
        self.decrement_depth(count);
    }

    fn decrement_depth(&self, count: usize) {
        let _ = self
            .queue_depth
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(count))
            });
    }

    fn snapshot(&self) -> PullMessageServiceDelayedSchedulerSnapshot {
        PullMessageServiceDelayedSchedulerSnapshot {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            submitted_count: self.submitted_count.load(Ordering::Relaxed),
            expired_count: self.expired_count.load(Ordering::Relaxed),
            cancelled_count: self.cancelled_count.load(Ordering::Relaxed),
            scheduler_task_count: usize::from(self.scheduler_running.load(Ordering::Relaxed)),
        }
    }
}

struct DelayedScheduleCommand {
    deadline: TokioInstant,
    payload: DelayedSchedulePayload,
}

impl DelayedScheduleCommand {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.payload.additional_retained_bytes())
    }
}

enum DelayedSchedulePayload {
    Pull {
        request: PullRequest,
        dispatcher: Option<PullRequestDispatcher>,
    },
    Pop {
        request: PopRequest,
        tx: Option<MessageRequestQueue>,
    },
    Task(Box<dyn FnOnce() + Send + 'static>),
}

impl DelayedSchedulePayload {
    fn additional_retained_bytes(&self) -> usize {
        match self {
            Self::Pull { request, .. } => request
                .retained_bytes()
                .saturating_sub(std::mem::size_of::<PullRequest>()),
            Self::Pop { request, .. } => request
                .retained_bytes()
                .saturating_sub(std::mem::size_of::<PopRequest>()),
            Self::Task(task) => std::mem::size_of_val(task.as_ref()),
        }
    }

    async fn execute(self, stopped: &AtomicBool) -> Option<Self> {
        if stopped.load(Ordering::Acquire) {
            return None;
        }

        match self {
            Self::Pull { request, dispatcher } => {
                if let Some(dispatcher) = dispatcher {
                    dispatcher.dispatch(request).await;
                } else {
                    warn!("PullMessageService not started");
                }
                None
            }
            Self::Pop { request, tx } => {
                if let Some(tx) = tx {
                    let retained_bytes = request.retained_bytes();
                    match tx.try_push_data(Box::new(request), retained_bytes) {
                        QueuePushOutcome::Enqueued
                        | QueuePushOutcome::Coalesced { .. }
                        | QueuePushOutcome::DroppedStale { .. } => None,
                        QueuePushOutcome::Rejected { item, rejection } => {
                            warn!(
                                "POP request queue rejected a delayed request; retrying: {:?}",
                                rejection
                            );
                            match item.into_any().downcast::<PopRequest>() {
                                Ok(request) => Some(Self::Pop {
                                    request: *request,
                                    tx: Some(tx),
                                }),
                                Err(_) => {
                                    error!("POP request queue returned an unexpected request type");
                                    None
                                }
                            }
                        }
                    }
                } else {
                    None
                }
            }
            Self::Task(task) => {
                task();
                None
            }
        }
    }
}

struct DelayedQueueEntry {
    deadline: TokioInstant,
    sequence: u64,
    payload: DelayedSchedulePayload,
    _permit: ResourcePermit,
}

impl PartialEq for DelayedQueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.sequence == other.sequence
    }
}

impl Eq for DelayedQueueEntry {}

impl Ord for DelayedQueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for DelayedQueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct DelayedSchedulerRunningGuard {
    metrics: Arc<DelayedSchedulerMetrics>,
}

impl DelayedSchedulerRunningGuard {
    fn new(metrics: Arc<DelayedSchedulerMetrics>) -> Self {
        metrics.scheduler_running.store(true, Ordering::Release);
        Self { metrics }
    }
}

impl Drop for DelayedSchedulerRunningGuard {
    fn drop(&mut self) {
        self.metrics.scheduler_running.store(false, Ordering::Release);
    }
}

async fn run_delayed_scheduler(
    commands: BudgetedQueue<DelayedScheduleCommand>,
    shutdown_token: CancellationToken,
    stopped: Arc<AtomicBool>,
    metrics: Arc<DelayedSchedulerMetrics>,
) {
    let _running = DelayedSchedulerRunningGuard::new(metrics.clone());
    let mut delayed_queue = BinaryHeap::new();
    let mut sequence = 0u64;

    loop {
        if shutdown_token.is_cancelled() {
            cancel_delayed_queue(&mut delayed_queue, &commands, &metrics);
            break;
        }

        match delayed_queue.peek().map(|entry: &DelayedQueueEntry| entry.deadline) {
            Some(deadline) if deadline <= TokioInstant::now() => {
                drain_expired_delayed_requests(&mut delayed_queue, &stopped, &metrics).await;
            }
            Some(deadline) => {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => {
                        drain_expired_delayed_requests(&mut delayed_queue, &stopped, &metrics).await;
                    }
                    command = commands.recv_budgeted() => {
                        if let Some(command) = command {
                            push_delayed_command(&mut delayed_queue, command, &mut sequence);
                        } else {
                            cancel_delayed_queue(&mut delayed_queue, &commands, &metrics);
                            break;
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        cancel_delayed_queue(&mut delayed_queue, &commands, &metrics);
                        break;
                    }
                }
            }
            None => {
                tokio::select! {
                    command = commands.recv_budgeted() => {
                        if let Some(command) = command {
                            push_delayed_command(&mut delayed_queue, command, &mut sequence);
                        } else {
                            cancel_delayed_queue(&mut delayed_queue, &commands, &metrics);
                            break;
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        cancel_delayed_queue(&mut delayed_queue, &commands, &metrics);
                        break;
                    }
                }
            }
        }
    }
}

fn push_delayed_command(
    delayed_queue: &mut BinaryHeap<DelayedQueueEntry>,
    command: BudgetedItem<DelayedScheduleCommand>,
    sequence: &mut u64,
) {
    let (command, permit, _) = command.into_parts();
    delayed_queue.push(DelayedQueueEntry {
        deadline: command.deadline,
        sequence: *sequence,
        payload: command.payload,
        _permit: permit,
    });
    *sequence = sequence.wrapping_add(1);
}

async fn drain_expired_delayed_requests(
    delayed_queue: &mut BinaryHeap<DelayedQueueEntry>,
    stopped: &AtomicBool,
    metrics: &DelayedSchedulerMetrics,
) {
    let now = TokioInstant::now();
    while delayed_queue.peek().is_some_and(|entry| entry.deadline <= now) {
        let entry = delayed_queue.pop().expect("entry should exist after peek");
        let DelayedQueueEntry {
            sequence,
            payload,
            _permit,
            ..
        } = entry;
        if let Some(payload) = payload.execute(stopped).await {
            delayed_queue.push(DelayedQueueEntry {
                deadline: TokioInstant::now() + Duration::from_millis(POP_REQUEUE_DELAY_MS),
                sequence,
                payload,
                _permit,
            });
        } else {
            metrics.record_expired();
        }
    }
}

fn cancel_delayed_queue(
    delayed_queue: &mut BinaryHeap<DelayedQueueEntry>,
    commands: &BudgetedQueue<DelayedScheduleCommand>,
    metrics: &DelayedSchedulerMetrics,
) {
    let mut cancelled = delayed_queue.len();
    delayed_queue.clear();
    while commands.try_pop_budgeted().is_some() {
        cancelled += 1;
    }
    metrics.record_cancelled(cancelled);
}

fn default_pull_shard_count() -> usize {
    num_cpus::get().clamp(1, DEFAULT_MAX_PULL_SHARDS)
}

fn normalize_pull_shard_count(shard_count: usize) -> usize {
    shard_count.max(1)
}

fn pull_request_shard_index(client_id: &str, request: &PullRequest, shard_count: usize) -> usize {
    let shard_count = normalize_pull_shard_count(shard_count);
    let mut hasher = DefaultHasher::new();
    client_id.hash(&mut hasher);
    request.consumer_group.hash(&mut hasher);
    request.message_queue.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

async fn run_pull_request_shard_worker(
    index: usize,
    queue: BudgetedQueue<PullRequest>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    instance: Arc<MQClientInstance>,
    metrics: Arc<PullRequestShardMetrics>,
) {
    info!(shard_index = index, "PullMessageService shard worker started");

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                break;
            }
            request = queue.recv() => {
                match request {
                    Some(request) => {
                        PullMessageService::pull_message(request, instance.as_ref()).await;
                        metrics.record_processed();
                    }
                    None => break,
                }
            }
        }
    }

    info!(shard_index = index, "PullMessageService shard worker end");
}

impl PullMessageService {
    /// Creates a new PullMessageService instance
    pub fn new() -> Self {
        Self::with_capacity_and_shards(DEFAULT_QUEUE_CAPACITY, default_pull_shard_count())
    }

    /// Creates a new PullMessageService instance with custom queue capacity
    pub fn with_capacity(queue_capacity: usize) -> Self {
        Self::with_capacity_and_shards(queue_capacity, default_pull_shard_count())
    }

    /// Creates a new PullMessageService instance with custom shard count.
    pub fn with_shards(shard_count: usize) -> Self {
        Self::with_capacity_and_shards(DEFAULT_QUEUE_CAPACITY, shard_count)
    }

    /// Creates a new PullMessageService instance with custom queue capacity and shard count.
    pub fn with_capacity_and_shards(queue_capacity: usize, shard_count: usize) -> Self {
        let resource_budget = crate::runtime::standalone_client_resource_budget()
            .expect("standalone PullMessageService resource budget must be valid");
        Self::with_capacity_shards_and_resource_budget(queue_capacity, shard_count, resource_budget)
    }

    pub(crate) fn with_shards_and_resource_budget(shard_count: usize, resource_budget: ResourceBudget) -> Self {
        Self::with_capacity_shards_and_resource_budget(DEFAULT_QUEUE_CAPACITY, shard_count, resource_budget)
    }

    fn with_capacity_shards_and_resource_budget(
        queue_capacity: usize,
        shard_count: usize,
        resource_budget: ResourceBudget,
    ) -> Self {
        let shard_count = normalize_pull_shard_count(shard_count);
        let pull_shard_metrics = (0..shard_count)
            .map(|_| Arc::new(PullRequestShardMetrics::default()))
            .collect();

        PullMessageService {
            service_context: Arc::new(StdRwLock::new(None)),
            resource_budget,
            lifecycle_transition: Arc::new(Mutex::new(())),
            tx: Arc::new(StdRwLock::new(None)),
            tx_shutdown: Arc::new(StdRwLock::new(None)),
            stopped: Arc::new(AtomicBool::new(false)),
            queue_capacity,
            shard_count,
            pull_dispatcher: Arc::new(StdMutex::new(None)),
            main_task_handle: Arc::new(tokio::sync::Mutex::new(None)),
            pull_shard_task_handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            pull_shard_metrics: Arc::new(pull_shard_metrics),
            scheduled_task_tracker: TaskTracker::new(),
            scheduled_task_shutdown: CancellationToken::new(),
            delayed_scheduler_queue: Arc::new(StdMutex::new(None)),
            delayed_scheduler_metrics: Arc::new(DelayedSchedulerMetrics::default()),
        }
    }

    /// Checks if the service is stopped
    #[inline]
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Gets service name
    #[inline]
    pub fn get_service_name(&self) -> &'static str {
        "PullMessageService"
    }

    /// Returns configured pull request shard count.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Returns the shard index for a pull request and client id.
    #[doc(hidden)]
    pub fn shard_index_for_pull_request(&self, client_id: &str, pull_request: &PullRequest) -> usize {
        pull_request_shard_index(client_id, pull_request, self.shard_count)
    }

    /// Starts the pull message service
    ///
    /// # Arguments
    /// * `instance` - MQClientInstance for message processing
    ///
    /// # Errors
    /// Returns error if service is already started
    pub async fn start(&self, instance: Arc<MQClientInstance>) -> Result<(), RocketMQError> {
        let _transition = self.lifecycle_transition.lock().await;
        if self
            .tx
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
        {
            warn!("{} already started", self.get_service_name());
            return Ok(());
        }

        let tx = self.build_request_queue("pull-pop-immediate", FullPolicy::Reject)?;
        let main_queue = tx.clone();
        let shard_queues = (0..self.shard_count)
            .map(|index| self.build_request_queue(format!("pull-worker-shard-{index}"), FullPolicy::Reject))
            .collect::<Result<Vec<BudgetedQueue<PullRequest>>, RocketMQError>>()?;
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);
        let pop_instance = instance.clone();
        let service_context = instance.service_context().component("pull-message-service");
        *self
            .service_context
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(service_context.clone());

        let handle = spawn_client_tracked_task_with_context(
            &service_context,
            "rocketmq-client-pull-message-service",
            Box::pin(async move {
                info!("{} service started", "PullMessageService");

                loop {
                    tokio::select! {
                        _ = shutdown.recv() => {
                            info!("{} received shutdown signal", "PullMessageService");
                            break;
                        }
                        request = main_queue.recv() => {
                            match request {
                                Some(request) => {
                                    if let Err(e) = Self::process_request(request, pop_instance.as_ref()).await {
                                        error!("{} failed to process request: {:?}", "PullMessageService", e);
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                }

                info!("{} service end", "PullMessageService");
            }),
        )
        .map_err(|error| pull_message_service_startup_failed("spawn main loop", error))?;

        let mut pull_shards = Vec::with_capacity(self.shard_count);
        let mut pull_handles = Vec::with_capacity(self.shard_count);
        for (index, shard_queue) in shard_queues.into_iter().enumerate() {
            let metrics = self.pull_shard_metrics[index].clone();
            let worker_instance = instance.clone();
            let shutdown_rx = tx_shutdown.subscribe();
            let worker_queue = shard_queue.clone();
            let handle = spawn_client_tracked_task_with_context(
                &service_context,
                "rocketmq-client-pull-message-service-shard",
                Box::pin(run_pull_request_shard_worker(
                    index,
                    worker_queue,
                    shutdown_rx,
                    worker_instance,
                    metrics.clone(),
                )),
            )
            .map_err(|error| pull_message_service_startup_failed("spawn shard worker", error))?;

            pull_shards.push(PullRequestShard {
                index,
                queue: shard_queue,
                metrics,
            });
            pull_handles.push(handle);
        }

        {
            let mut dispatcher = self
                .pull_dispatcher
                .lock()
                .expect("pull dispatcher lock should not be poisoned");
            *dispatcher = Some(PullRequestDispatcher {
                client_id: instance.client_id.clone(),
                shards: Arc::new(pull_shards),
            });
        }

        *self.tx.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(tx);
        *self
            .tx_shutdown
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(tx_shutdown);
        self.stopped.store(false, Ordering::Release);
        *self.main_task_handle.lock().await = Some(handle);
        *self.pull_shard_task_handles.lock().await = pull_handles;

        Ok(())
    }

    async fn main_task_count(&self) -> usize {
        let main_count = self
            .main_task_handle
            .lock()
            .await
            .as_ref()
            .map(ClientTrackedTaskHandle::task_count)
            .unwrap_or_default();
        let shard_count = self
            .pull_shard_task_handles
            .lock()
            .await
            .iter()
            .map(ClientTrackedTaskHandle::task_count)
            .sum::<usize>();
        main_count + shard_count
    }

    pub fn delayed_scheduler_snapshot(&self) -> PullMessageServiceDelayedSchedulerSnapshot {
        self.delayed_scheduler_metrics.snapshot()
    }

    pub async fn shard_snapshot(&self) -> PullMessageServiceShardSnapshot {
        let shards = self
            .pull_shard_metrics
            .iter()
            .enumerate()
            .map(|(index, metrics)| metrics.snapshot(index))
            .collect::<Vec<_>>();
        let worker_task_count = self
            .pull_shard_task_handles
            .lock()
            .await
            .iter()
            .map(ClientTrackedTaskHandle::task_count)
            .sum();

        PullMessageServiceShardSnapshot {
            shard_count: self.shard_count,
            pending_count: shards.iter().map(|snapshot| snapshot.pending_count).sum(),
            submitted_count: shards.iter().map(|snapshot| snapshot.submitted_count).sum(),
            processed_count: shards.iter().map(|snapshot| snapshot.processed_count).sum(),
            rejected_count: shards.iter().map(|snapshot| snapshot.rejected_count).sum(),
            worker_task_count,
            shards,
        }
    }

    fn pull_dispatcher(&self) -> Option<PullRequestDispatcher> {
        self.pull_dispatcher
            .lock()
            .expect("pull dispatcher lock should not be poisoned")
            .clone()
    }

    fn submit_delayed(&self, payload: DelayedSchedulePayload, time_delay: u64) {
        self.delayed_scheduler_metrics.record_submitted();
        let Some(queue) = self.ensure_delayed_scheduler_started() else {
            self.delayed_scheduler_metrics.record_cancelled(1);
            return;
        };

        let command = DelayedScheduleCommand {
            deadline: TokioInstant::now() + Duration::from_millis(time_delay),
            payload,
        };
        let retained_bytes = command.retained_bytes();
        match queue.try_push_data(command, retained_bytes) {
            QueuePushOutcome::DroppedStale { dropped } => {
                self.delayed_scheduler_metrics.record_cancelled(dropped);
            }
            QueuePushOutcome::Enqueued | QueuePushOutcome::Coalesced { .. } => {}
            QueuePushOutcome::Rejected { .. } => self.delayed_scheduler_metrics.record_cancelled(1),
        }
    }

    fn ensure_delayed_scheduler_started(&self) -> Option<BudgetedQueue<DelayedScheduleCommand>> {
        if self.scheduled_task_shutdown.is_cancelled() {
            return None;
        }

        let mut guard = self
            .delayed_scheduler_queue
            .lock()
            .expect("delayed scheduler queue lock should not be poisoned");
        if let Some(queue) = guard.as_ref() {
            return Some(queue.clone());
        }

        let queue = match self.build_delayed_scheduler_queue() {
            Ok(queue) => queue,
            Err(error) => {
                error!("Failed to build PullMessageService delayed scheduler budget: {error}");
                return None;
            }
        };
        let tracked_task = self.scheduled_task_tracker.track_future(run_delayed_scheduler(
            queue.clone(),
            self.scheduled_task_shutdown.clone(),
            self.stopped.clone(),
            self.delayed_scheduler_metrics.clone(),
        ));

        let service_context = self
            .service_context
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()?;
        match spawn_client_task_with_context(
            &service_context,
            "rocketmq-client-pull-delayed-scheduler",
            Box::pin(tracked_task),
        ) {
            Ok(_) => {
                *guard = Some(queue.clone());
                Some(queue)
            }
            Err(error) => {
                error!("Failed to spawn PullMessageService delayed scheduler: {}", error);
                None
            }
        }
    }

    fn build_delayed_scheduler_queue(&self) -> Result<BudgetedQueue<DelayedScheduleCommand>, RocketMQError> {
        self.build_request_queue("pull-delayed-scheduler", FullPolicy::DropStale)
    }

    fn build_request_queue<T>(
        &self,
        name: impl Into<String>,
        policy: FullPolicy,
    ) -> Result<BudgetedQueue<T>, RocketMQError> {
        let queue_count = self.queue_capacity.max(1);
        let queue_bytes = (self.resource_budget.limit().capacity.bytes / 16).max(1);
        let queue_rate = u64::try_from(queue_count).unwrap_or(u64::MAX).max(1);
        let budget = self
            .resource_budget
            .child(
                name,
                BudgetLimit::new(queue_count, queue_bytes, policy)
                    .with_rate(RateLimit::new(queue_rate, queue_rate))
                    .with_max_age(Duration::from_secs(300)),
            )
            .map_err(|error| RocketMQError::ConfigInvalidValue {
                key: "client.pull.requestQueue",
                value: queue_count.to_string(),
                reason: error.to_string(),
            })?;
        Ok(BudgetedQueue::new(budget))
    }

    /// Processes a message request (Pull or Pop)
    ///
    /// # Arguments
    /// * `request` - Message request to process
    /// * `instance` - MQClientInstance for consumer lookup
    ///
    /// # Errors
    /// Returns error if request processing fails
    async fn process_request(
        request: Box<dyn MessageRequest + Send + 'static>,
        instance: &MQClientInstance,
    ) -> Result<(), RocketMQError> {
        match request.get_message_request_mode() {
            MessageRequestMode::Pull => {
                // Safe downcast using Any trait
                if let Ok(pull_request) = request.into_any().downcast::<PullRequest>() {
                    Self::pull_message(*pull_request, instance).await;
                    Ok(())
                } else {
                    Err(pull_message_service_request_type_mismatch("PullRequest"))
                }
            }
            MessageRequestMode::Pop => {
                if let Ok(pop_request) = request.into_any().downcast::<PopRequest>() {
                    Self::pop_message(*pop_request, instance).await;
                    Ok(())
                } else {
                    Err(pull_message_service_request_type_mismatch("PopRequest"))
                }
            }
        }
    }

    /// Handles pull message request
    async fn pull_message(request: PullRequest, instance: &MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pull_message(request).await;
        } else {
            warn!("No matched consumer for the PullRequest {}, drop it", request)
        }
    }

    /// Handles pop message request
    async fn pop_message(request: PopRequest, instance: &MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pop_message(request).await;
        } else {
            warn!("No matched consumer for the PopRequest {}, drop it", request)
        }
    }

    /// Executes pull request with delay
    ///
    /// # Arguments
    /// * `pull_request` - The pull request to execute
    /// * `time_delay` - Delay in milliseconds before execution
    ///
    /// # Behavior
    /// - Returns immediately if service is stopped
    /// - Spawns a tokio task that sleeps then sends the request
    pub fn execute_pull_request_later(&self, pull_request: PullRequest, time_delay: u64) {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute later task", self.get_service_name());
            return;
        }

        self.submit_delayed(
            DelayedSchedulePayload::Pull {
                request: pull_request,
                dispatcher: self.pull_dispatcher(),
            },
            time_delay,
        );
    }

    /// Executes pull request immediately
    ///
    /// # Arguments
    /// * `pull_request` - The pull request to execute
    ///
    /// # Behavior
    /// Logs error but does not return error (aligned with Java implementation)
    pub async fn execute_pull_request_immediately(&self, pull_request: PullRequest) {
        if self.is_stopped() {
            warn!("PullMessageService has shutdown");
            return;
        }

        if let Some(dispatcher) = self.pull_dispatcher() {
            dispatcher.dispatch(pull_request).await;
        } else {
            warn!("PullMessageService not started");
        }
    }

    /// Executes pop request with delay
    pub fn execute_pop_pull_request_later(&self, pop_request: PopRequest, time_delay: u64) {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute later task", self.get_service_name());
            return;
        }

        self.submit_delayed(
            DelayedSchedulePayload::Pop {
                request: pop_request,
                tx: self.tx.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone(),
            },
            time_delay,
        );
    }

    /// Executes pop request immediately
    pub async fn execute_pop_pull_request_immediately(&self, pop_request: PopRequest) {
        if self.is_stopped() {
            warn!("PullMessageService has shutdown");
            return;
        }

        let tx = self.tx.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        if let Some(tx) = tx {
            let retained_bytes = pop_request.retained_bytes();
            if let QueuePushOutcome::Rejected { item, rejection } =
                tx.try_push_data(Box::new(pop_request), retained_bytes)
            {
                warn!(
                    "executePopPullRequestImmediately queue admission failed; retrying with delay: {:?}",
                    rejection
                );
                match item.into_any().downcast::<PopRequest>() {
                    Ok(pop_request) => self.execute_pop_pull_request_later(*pop_request, POP_REQUEUE_DELAY_MS),
                    Err(_) => error!("POP request queue returned an unexpected request type"),
                }
            }
        } else {
            warn!("PullMessageService not started");
        }
    }

    /// Executes a generic task with delay (equivalent to Java's executeTaskLater)
    ///
    /// # Arguments
    /// * `task` - Task function to execute
    /// * `time_delay` - Delay in milliseconds before execution
    pub fn execute_task_later<F>(&self, task: F, time_delay: u64)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute task", self.get_service_name());
            return;
        }

        self.submit_delayed(DelayedSchedulePayload::Task(Box::new(task)), time_delay);
    }

    /// Executes a generic task immediately (equivalent to Java's executeTask)
    pub fn execute_task<F>(&self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_stopped() {
            warn!("{} has shutdown, cannot execute task", self.get_service_name());
            return;
        }

        spawn_scheduled_pull_message_task(
            self.service_context
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref(),
            "rocketmq-client-pull-task",
            &self.scheduled_task_tracker,
            &self.scheduled_task_shutdown,
            async move {
                task();
            },
        );
    }

    /// Gracefully shuts down the service
    ///
    /// # Arguments
    /// * `timeout_ms` - Maximum time to wait for shutdown (milliseconds)
    ///
    /// # Behavior
    /// - Sets stopped flag
    /// - Sends shutdown signal to main loop
    /// - Cancels all scheduled tasks
    /// - Waits for main loop to finish (with timeout)
    pub async fn shutdown(&self, timeout_ms: u64) -> Result<(), RocketMQError> {
        let _transition = self.lifecycle_transition.lock().await;
        if self.is_stopped() {
            warn!("{} already stopped", self.get_service_name());
            return Ok(());
        }

        info!("{} shutting down...", self.get_service_name());

        // 1. Set stopped flag
        self.stopped.store(true, Ordering::Release);
        self.scheduled_task_shutdown.cancel();
        if let Some(queue) = self
            .delayed_scheduler_queue
            .lock()
            .expect("delayed scheduler queue lock should not be poisoned")
            .take()
        {
            queue.close();
        }
        // 2. Send shutdown signal
        let tx_shutdown = self
            .tx_shutdown
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let shutdown_signal_failed = tx_shutdown.is_some_and(|tx_shutdown| tx_shutdown.send(()).is_err());
        if let Some(queue) = self.tx.write().unwrap_or_else(|poisoned| poisoned.into_inner()).take() {
            queue.close();
        }
        if let Some(dispatcher) = self
            .pull_dispatcher
            .lock()
            .expect("pull dispatcher lock should not be poisoned")
            .take()
        {
            dispatcher.close();
        }
        if shutdown_signal_failed {
            return Err(pull_message_service_shutdown_signal_failed());
        }

        // 3. Wait for main loop to exit (with timeout)
        let timeout = Duration::from_millis(timeout_ms);
        let main_task = self.main_task_handle.lock().await.take();
        if let Some(handle) = main_task {
            let report = handle.shutdown(timeout).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "{} main loop shutdown report is unhealthy",
                    self.get_service_name()
                );
            }
        }

        let shard_tasks = std::mem::take(&mut *self.pull_shard_task_handles.lock().await);
        for handle in shard_tasks {
            let report = handle.shutdown(timeout).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "{} shard worker shutdown report is unhealthy",
                    self.get_service_name()
                );
            }
        }

        self.scheduled_task_tracker.close();
        if tokio::time::timeout(timeout, self.scheduled_task_tracker.wait())
            .await
            .is_err()
        {
            warn!(
                "{} scheduled task shutdown timeout after {}ms",
                self.get_service_name(),
                timeout_ms
            );
        }

        info!("{} shutdown completed", self.get_service_name());
        Ok(())
    }

    /// Shuts down with default timeout
    pub async fn shutdown_default(&self) -> Result<(), RocketMQError> {
        self.shutdown(DEFAULT_SHUTDOWN_TIMEOUT_MS).await
    }
}

impl Default for PullMessageService {
    fn default() -> Self {
        Self::new()
    }
}

#[doc(hidden)]
pub async fn run_pull_message_service_lifecycle_probe(
    client_runtime: Arc<ClientRuntime>,
) -> PullMessageServiceLifecycleProbe {
    let service = PullMessageService::new();
    let instance = MQClientInstance::new_arc(
        ClientConfig::default(),
        0,
        "pull-message-service-probe",
        None,
        client_runtime.component("pull-message-service-probe"),
        client_runtime.telemetry_handle().clone(),
        client_runtime.pool().request_future_holder(),
    );

    let start_result = service.start(instance.clone()).await;
    let task_count_before_shutdown = service.main_task_count().await;
    let expected_task_count_before_shutdown = service.shard_count() + 1;

    let shutdown_started = Instant::now();
    let shutdown_result = service.shutdown(1_000).await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = service.main_task_count().await;
    let delayed_scheduler = service.delayed_scheduler_snapshot();
    let shards = service.shard_snapshot().await;

    instance.shutdown().await;

    PullMessageServiceLifecycleProbe {
        healthy: start_result.is_ok()
            && shutdown_result.is_ok()
            && task_count_before_shutdown == expected_task_count_before_shutdown
            && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
        delayed_scheduler,
        shards,
    }
}

fn spawn_scheduled_pull_message_task<F>(
    service_context: Option<&ChildServiceContext>,
    thread_name: &'static str,
    tracker: &TaskTracker,
    shutdown_token: &CancellationToken,
    task: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    if shutdown_token.is_cancelled() {
        return;
    }
    let Some(service_context) = service_context else {
        error!(
            "Failed to spawn {} background task: ClientRuntime context is not bound",
            thread_name
        );
        return;
    };

    let shutdown_token = shutdown_token.clone();
    let tracked_task = tracker.track_future(async move {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {},
            _ = task => {},
        }
    });

    if let Err(error) = spawn_client_task_with_context(service_context, thread_name, Box::pin(tracked_task)) {
        error!("Failed to spawn {} background task: {}", thread_name, error);
    }
}

fn pull_message_service_startup_failed(operation: &'static str, error: impl std::fmt::Display) -> RocketMQError {
    RocketMQError::Service(UnifiedServiceError::StartupFailed(format!(
        "PullMessageService {operation}: {error}"
    )))
}

fn pull_message_service_request_type_mismatch(expected: &'static str) -> RocketMQError {
    RocketMQError::ClientInvalidState {
        expected,
        actual: "message request payload type mismatch".to_string(),
    }
}

fn pull_message_service_shutdown_signal_failed() -> RocketMQError {
    RocketMQError::ClientInvalidState {
        expected: "active shutdown receiver",
        actual: "shutdown receiver unavailable".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_error::ErrorKind;

    fn test_pop_request(queue_id: i32) -> PopRequest {
        PopRequest::new(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("group"),
            rocketmq_model::common::message::message_queue::MessageQueue::from_parts("topic", "broker-a", queue_id),
            crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue::new(),
            0,
        )
    }

    #[tokio::test]
    async fn rejected_delayed_pop_payload_returns_the_original_request_for_retry() {
        let service = PullMessageService::with_capacity_and_shards(1, 1);
        let queue = service
            .build_request_queue::<BoxedMessageRequest>("pop-retry-overload", FullPolicy::Reject)
            .expect("POP retry queue");
        let first = test_pop_request(0);
        assert!(matches!(
            queue.try_push_data(Box::new(first), std::mem::size_of::<PopRequest>()),
            rocketmq_runtime::QueuePushOutcome::Enqueued
        ));

        let retry = DelayedSchedulePayload::Pop {
            request: test_pop_request(1),
            tx: Some(queue.clone()),
        }
        .execute(&AtomicBool::new(false))
        .await;

        let Some(DelayedSchedulePayload::Pop { request, tx }) = retry else {
            panic!("rejected POP request should be returned");
        };
        assert_eq!(request.get_message_queue().queue_id(), 1);
        assert!(tx.is_some());
        assert_eq!(queue.snapshot().depth, 1);
    }

    #[test]
    fn overload_rejects_excess_pull_worker_requests() {
        let service = PullMessageService::with_capacity_and_shards(2, 1);
        let queue = service
            .build_request_queue::<u64>("pull-worker-overload", FullPolicy::Reject)
            .expect("pull worker queue");

        assert!(matches!(
            queue.try_push_data(1, 1),
            rocketmq_runtime::QueuePushOutcome::Enqueued
        ));
        assert!(matches!(
            queue.try_push_data(2, 1),
            rocketmq_runtime::QueuePushOutcome::Enqueued
        ));
        assert!(matches!(
            queue.try_push_data(3, 1),
            rocketmq_runtime::QueuePushOutcome::Rejected { .. }
        ));
        assert!(matches!(
            queue.try_push_data(4, 1),
            rocketmq_runtime::QueuePushOutcome::Rejected { .. }
        ));

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.depth, 2);
        assert_eq!(snapshot.retained_bytes, 2);
        assert_eq!(snapshot.rejected_count, 2);
    }

    #[test]
    fn delayed_command_accounts_for_the_captured_task_allocation() {
        let captured = [7_u8; 1024];
        let command = DelayedScheduleCommand {
            deadline: TokioInstant::now() + Duration::from_secs(1),
            payload: DelayedSchedulePayload::Task(Box::new(move || {
                std::hint::black_box(captured);
            })),
        };

        assert!(command.retained_bytes() >= std::mem::size_of::<DelayedScheduleCommand>() + captured.len());
    }

    fn service_with_context(scope: &'static str) -> PullMessageService {
        let service = PullMessageService::new();
        *service
            .service_context
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(crate::runtime::test_service_context(scope));
        service
    }

    #[test]
    fn pull_message_service_startup_failed_uses_service_error_kind() {
        let error = pull_message_service_startup_failed("spawn test worker", "task group closed");

        assert_eq!(error.kind(), ErrorKind::Service);
        assert!(error.to_string().contains("PullMessageService spawn test worker"));
    }

    #[test]
    fn pull_message_service_request_type_mismatch_uses_client_invalid_state() {
        let error = pull_message_service_request_type_mismatch("PullRequest");

        assert_eq!(error.kind(), ErrorKind::ClientInvalidState);
        assert!(error.to_string().contains("PullRequest"));
    }

    struct MismatchedMessageRequest {
        mode: MessageRequestMode,
    }

    impl MessageRequest for MismatchedMessageRequest {
        fn get_message_request_mode(&self) -> MessageRequestMode {
            self.mode
        }
    }

    async fn process_mismatched_request(mode: MessageRequestMode) -> RocketMQError {
        let runtime = crate::runtime::test_client_runtime("pull-message-mismatch-test");
        let instance = MQClientInstance::new_arc(
            ClientConfig::default(),
            0,
            "pull-message-mismatch-test",
            None,
            runtime.component("instance"),
            runtime.telemetry_handle().clone(),
            runtime.pool().request_future_holder(),
        );

        match PullMessageService::process_request(Box::new(MismatchedMessageRequest { mode }), instance.as_ref()).await
        {
            Ok(_) => panic!("mismatched message request should be rejected"),
            Err(error) => error,
        }
    }

    #[tokio::test]
    async fn process_request_reports_pull_downcast_mismatch_as_client_invalid_state() {
        let error = process_mismatched_request(MessageRequestMode::Pull).await;

        assert_eq!(error.kind(), ErrorKind::ClientInvalidState);
        assert!(error.to_string().contains("PullRequest"));
    }

    #[tokio::test]
    async fn process_request_reports_pop_downcast_mismatch_as_client_invalid_state() {
        let error = process_mismatched_request(MessageRequestMode::Pop).await;

        assert_eq!(error.kind(), ErrorKind::ClientInvalidState);
        assert!(error.to_string().contains("PopRequest"));
    }

    #[test]
    fn execute_task_uses_bound_context_without_current_tokio_runtime() {
        let service = service_with_context("pull-message-immediate-task-test");
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task(move || ran_clone.store(true, Ordering::Release));

        std::thread::sleep(Duration::from_millis(50));
        assert!(ran.load(Ordering::Acquire));
    }

    #[test]
    fn execute_task_later_uses_bound_context_without_current_tokio_runtime() {
        let service = service_with_context("pull-message-delayed-task-test");
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task_later(move || ran_clone.store(true, Ordering::Release), 1);

        std::thread::sleep(Duration::from_millis(50));
        assert!(ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn execute_task_is_tracked_until_completion() {
        let service = service_with_context("pull-message-tracked-task-test");
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task(move || ran_clone.store(true, Ordering::Release));
        service.scheduled_task_tracker.close();

        tokio::time::timeout(Duration::from_secs(1), service.scheduled_task_tracker.wait())
            .await
            .expect("scheduled task tracker should finish completed immediate task");

        assert!(ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_cancels_delayed_tasks_and_waits_for_tracker() {
        let service = PullMessageService::new();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = ran.clone();

        service.execute_task_later(move || ran_clone.store(true, Ordering::Release), 60_000);
        tokio::task::yield_now().await;

        service.shutdown(100).await.expect("shutdown should succeed");

        tokio::time::timeout(Duration::from_secs(1), service.scheduled_task_tracker.wait())
            .await
            .expect("shutdown should release delayed task tracker");

        assert!(!ran.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn shutdown_reports_closed_shutdown_receiver_as_client_invalid_state() {
        let service = PullMessageService::new();
        let (tx_shutdown, rx_shutdown) = tokio::sync::broadcast::channel(1);
        drop(rx_shutdown);
        *service
            .tx_shutdown
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(tx_shutdown);

        let error = match service.shutdown(100).await {
            Ok(_) => panic!("closed shutdown receiver should be rejected"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), ErrorKind::ClientInvalidState);
    }

    #[tokio::test]
    async fn pull_message_service_lifecycle_probe_reports_clean_shutdown() {
        let probe =
            run_pull_message_service_lifecycle_probe(crate::runtime::test_client_runtime("pull-message-probe")).await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, probe.shards.shard_count + 1);
        assert_eq!(probe.task_count_after_shutdown, 0);
    }
}
