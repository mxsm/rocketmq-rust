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

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use tokio::sync::oneshot;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

const TASK_STATE_PENDING: u8 = 0;
const TASK_STATE_RUNNING: u8 = 1;
const TASK_STATE_COMPLETED: u8 = 2;
const TASK_STATE_CANCELED: u8 = 3;

const INITIAL_DELAY: Duration = Duration::from_millis(1_000);
const SCAN_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_SCAN_BATCH_LIMIT: usize = 256;
const JSTACK_LOG_INTERVAL_MILLIS: i64 = 15_000;

const ALL_QUEUE_KINDS: [FastFailureQueueKind; 7] = [
    FastFailureQueueKind::Send,
    FastFailureQueueKind::Pull,
    FastFailureQueueKind::LitePull,
    FastFailureQueueKind::Heartbeat,
    FastFailureQueueKind::Transaction,
    FastFailureQueueKind::Ack,
    FastFailureQueueKind::AdminBroker,
];

type PageCacheBusyChecker = Arc<dyn Fn() -> bool + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FastFailureQueueKind {
    Send,
    Pull,
    LitePull,
    Heartbeat,
    Transaction,
    Ack,
    AdminBroker,
}

impl FastFailureQueueKind {
    fn name(self) -> &'static str {
        match self {
            Self::Send => "send",
            Self::Pull => "pull",
            Self::LitePull => "litePull",
            Self::Heartbeat => "heartbeat",
            Self::Transaction => "transaction",
            Self::Ack => "ack",
            Self::AdminBroker => "adminBroker",
        }
    }
}

pub(crate) struct FastFailureTask {
    id: u64,
    created_timestamp_millis: u64,
    opaque: i32,
    state: AtomicU8,
    response_tx: Mutex<Option<oneshot::Sender<Option<RemotingCommand>>>>,
}

impl FastFailureTask {
    fn new(
        id: u64,
        opaque: i32,
        created_timestamp_millis: u64,
        response_tx: oneshot::Sender<Option<RemotingCommand>>,
    ) -> Self {
        Self {
            id,
            created_timestamp_millis,
            opaque,
            state: AtomicU8::new(TASK_STATE_PENDING),
            response_tx: Mutex::new(Some(response_tx)),
        }
    }

    #[inline]
    pub(crate) fn created_timestamp_millis(&self) -> u64 {
        self.created_timestamp_millis
    }

    #[inline]
    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn mark_running(&self) -> bool {
        self.state
            .compare_exchange(
                TASK_STATE_PENDING,
                TASK_STATE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn complete(&self, response: Option<RemotingCommand>) -> bool {
        if self
            .state
            .compare_exchange(
                TASK_STATE_RUNNING,
                TASK_STATE_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.send_response(response);
            return true;
        }

        false
    }

    fn cancel(&self, response: RemotingCommand) -> bool {
        if self
            .state
            .compare_exchange(
                TASK_STATE_PENDING,
                TASK_STATE_CANCELED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.send_response(Some(response));
            return true;
        }

        false
    }

    fn send_response(&self, response: Option<RemotingCommand>) {
        if let Some(response_tx) = self.response_tx.lock().take() {
            let _ = response_tx.send(response);
        }
    }
}

struct FastFailureQueue {
    kind: FastFailureQueueKind,
    tasks: Mutex<VecDeque<Arc<FastFailureTask>>>,
    pending_count: AtomicUsize,
    running_count: AtomicUsize,
    semaphore: Arc<Semaphore>,
}

impl FastFailureQueue {
    fn new(kind: FastFailureQueueKind, permits: usize) -> Self {
        Self {
            kind,
            tasks: Mutex::new(VecDeque::new()),
            pending_count: AtomicUsize::new(0),
            running_count: AtomicUsize::new(0),
            semaphore: Arc::new(Semaphore::new(permits.max(1))),
        }
    }

    fn enqueue(&self, task: Arc<FastFailureTask>) {
        self.tasks.lock().push_back(task);
        self.pending_count.fetch_add(1, Ordering::AcqRel);
    }

    async fn acquire_permit(&self) -> Option<OwnedSemaphorePermit> {
        self.semaphore.clone().acquire_owned().await.ok()
    }

    fn try_mark_running(&self, task: &FastFailureTask) -> bool {
        if task.mark_running() {
            self.pending_count.fetch_sub(1, Ordering::AcqRel);
            self.running_count.fetch_add(1, Ordering::AcqRel);
            return true;
        }

        false
    }

    fn complete(&self, task: &FastFailureTask, response: Option<RemotingCommand>) {
        if task.complete(response) {
            self.running_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn cancel_pending(&self, task: &FastFailureTask, response: RemotingCommand) -> bool {
        if task.cancel(response) {
            self.pending_count.fetch_sub(1, Ordering::AcqRel);
            return true;
        }

        false
    }

    #[inline]
    fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    fn clean_expired(&self, now_millis: u64, max_wait_millis: u64, batch_limit: usize) -> usize {
        self.clean_pending(now_millis, Some(max_wait_millis), batch_limit, "[TIMEOUT_CLEAN_QUEUE]")
    }

    fn clean_page_cache_busy(&self, now_millis: u64, batch_limit: usize) -> usize {
        self.clean_pending(now_millis, None, batch_limit, "[PCBUSY_CLEAN_QUEUE]")
    }

    fn clean_pending(
        &self,
        now_millis: u64,
        max_wait_millis: Option<u64>,
        batch_limit: usize,
        remark_prefix: &'static str,
    ) -> usize {
        let mut cleaned = 0;

        while cleaned < batch_limit {
            let Some(task) = self.take_cleanable_head(now_millis, max_wait_millis) else {
                break;
            };
            let behind = now_millis.saturating_sub(task.created_timestamp_millis());
            let response = system_busy_response(task.opaque, remark_prefix, behind, self.pending_count());

            if self.cancel_pending(&task, response) {
                cleaned += 1;
            }
        }

        cleaned
    }

    fn take_cleanable_head(&self, now_millis: u64, max_wait_millis: Option<u64>) -> Option<Arc<FastFailureTask>> {
        loop {
            let mut tasks = self.tasks.lock();
            let front = tasks.front().cloned()?;

            match front.state() {
                TASK_STATE_RUNNING | TASK_STATE_COMPLETED | TASK_STATE_CANCELED => {
                    tasks.pop_front();
                }
                TASK_STATE_PENDING => {
                    let expired = max_wait_millis.is_none_or(|max_wait_millis| {
                        now_millis.saturating_sub(front.created_timestamp_millis()) >= max_wait_millis
                    });
                    if !expired {
                        return None;
                    }
                    tasks.pop_front();
                    return Some(front);
                }
                _ => {
                    warn!(
                        "unknown fast failure task state: queue={}, task_id={}, state={}",
                        self.kind.name(),
                        front.id,
                        front.state()
                    );
                    tasks.pop_front();
                }
            }
        }
    }
}

struct FastFailureQueues {
    send: Arc<FastFailureQueue>,
    pull: Arc<FastFailureQueue>,
    lite_pull: Arc<FastFailureQueue>,
    heartbeat: Arc<FastFailureQueue>,
    transaction: Arc<FastFailureQueue>,
    ack: Arc<FastFailureQueue>,
    admin_broker: Arc<FastFailureQueue>,
}

impl FastFailureQueues {
    fn new() -> Self {
        Self {
            send: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::Send,
                permits_for(FastFailureQueueKind::Send),
            )),
            pull: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::Pull,
                permits_for(FastFailureQueueKind::Pull),
            )),
            lite_pull: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::LitePull,
                permits_for(FastFailureQueueKind::LitePull),
            )),
            heartbeat: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::Heartbeat,
                permits_for(FastFailureQueueKind::Heartbeat),
            )),
            transaction: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::Transaction,
                permits_for(FastFailureQueueKind::Transaction),
            )),
            ack: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::Ack,
                permits_for(FastFailureQueueKind::Ack),
            )),
            admin_broker: Arc::new(FastFailureQueue::new(
                FastFailureQueueKind::AdminBroker,
                permits_for(FastFailureQueueKind::AdminBroker),
            )),
        }
    }

    fn get(&self, kind: FastFailureQueueKind) -> &Arc<FastFailureQueue> {
        match kind {
            FastFailureQueueKind::Send => &self.send,
            FastFailureQueueKind::Pull => &self.pull,
            FastFailureQueueKind::LitePull => &self.lite_pull,
            FastFailureQueueKind::Heartbeat => &self.heartbeat,
            FastFailureQueueKind::Transaction => &self.transaction,
            FastFailureQueueKind::Ack => &self.ack,
            FastFailureQueueKind::AdminBroker => &self.admin_broker,
        }
    }
}

#[derive(Default)]
struct FastFailureLifecycle {
    cancel: Option<CancellationToken>,
    handle: Option<JoinHandle<()>>,
}

struct BrokerFastFailureInner {
    broker_config: Arc<BrokerConfig>,
    queues: FastFailureQueues,
    next_task_id: AtomicU64,
    running: AtomicBool,
    lifecycle: Mutex<FastFailureLifecycle>,
    page_cache_busy_checker: Mutex<Option<PageCacheBusyChecker>>,
    jstack_time_millis: AtomicI64,
    scan_batch_limit: usize,
}

#[derive(Clone)]
pub(crate) struct BrokerFastFailure {
    inner: Arc<BrokerFastFailureInner>,
}

impl BrokerFastFailure {
    pub(crate) fn new(broker_config: Arc<BrokerConfig>) -> Self {
        Self {
            inner: Arc::new(BrokerFastFailureInner {
                broker_config,
                queues: FastFailureQueues::new(),
                next_task_id: AtomicU64::new(0),
                running: AtomicBool::new(false),
                lifecycle: Mutex::new(FastFailureLifecycle::default()),
                page_cache_busy_checker: Mutex::new(None),
                jstack_time_millis: AtomicI64::new(current_millis() as i64),
                scan_batch_limit: DEFAULT_SCAN_BATCH_LIMIT,
            }),
        }
    }

    pub(crate) fn set_page_cache_busy_checker<F>(&self, checker: F)
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        *self.inner.page_cache_busy_checker.lock() = Some(Arc::new(checker));
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.broker_config.broker_fast_failure_enable
    }

    pub(crate) fn is_running(&self) -> bool {
        self.inner.running.load(Ordering::Acquire)
    }

    pub(crate) fn start(&self) {
        if self
            .inner
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let this = self.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = cancel_for_task.cancelled() => {
                    this.inner.running.store(false, Ordering::Release);
                    return;
                }
                _ = tokio::time::sleep(INITIAL_DELAY) => {}
            }

            let mut interval = tokio::time::interval(SCAN_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = cancel_for_task.cancelled() => {
                        this.inner.running.store(false, Ordering::Release);
                        return;
                    }
                    _ = interval.tick() => {
                        if this.is_enabled() {
                            this.clean_expired_request();
                        }
                    }
                }
            }
        });

        let mut lifecycle = self.inner.lifecycle.lock();
        lifecycle.cancel = Some(cancel);
        lifecycle.handle = Some(handle);
        info!("BrokerFastFailure started");
    }

    pub(crate) fn shutdown(&self) {
        self.inner.running.store(false, Ordering::Release);
        let mut lifecycle = self.inner.lifecycle.lock();
        if let Some(cancel) = lifecycle.cancel.take() {
            cancel.cancel();
        }
        if let Some(handle) = lifecycle.handle.take() {
            handle.abort();
        }
        info!("BrokerFastFailure shutdown");
    }

    pub(crate) fn enqueue(
        &self,
        kind: FastFailureQueueKind,
        opaque: i32,
    ) -> (Arc<FastFailureTask>, oneshot::Receiver<Option<RemotingCommand>>) {
        self.enqueue_with_timestamp(kind, opaque, current_millis())
    }

    fn enqueue_with_timestamp(
        &self,
        kind: FastFailureQueueKind,
        opaque: i32,
        created_timestamp_millis: u64,
    ) -> (Arc<FastFailureTask>, oneshot::Receiver<Option<RemotingCommand>>) {
        let (response_tx, response_rx) = oneshot::channel();
        let task_id = self.inner.next_task_id.fetch_add(1, Ordering::AcqRel);
        let task = Arc::new(FastFailureTask::new(
            task_id,
            opaque,
            created_timestamp_millis,
            response_tx,
        ));
        self.inner.queues.get(kind).enqueue(task.clone());
        (task, response_rx)
    }

    pub(crate) async fn acquire_permit(&self, kind: FastFailureQueueKind) -> Option<OwnedSemaphorePermit> {
        self.inner.queues.get(kind).acquire_permit().await
    }

    pub(crate) fn try_mark_running(&self, kind: FastFailureQueueKind, task: &FastFailureTask) -> bool {
        self.inner.queues.get(kind).try_mark_running(task)
    }

    pub(crate) fn complete(
        &self,
        kind: FastFailureQueueKind,
        task: &FastFailureTask,
        response: Option<RemotingCommand>,
    ) {
        self.inner.queues.get(kind).complete(task, response);
    }

    fn clean_expired_request(&self) {
        if !self.is_enabled() {
            return;
        }

        let now_millis = current_millis();

        if self.is_os_page_cache_busy() {
            let cleaned = self
                .inner
                .queues
                .send
                .clean_page_cache_busy(now_millis, self.inner.scan_batch_limit);
            if cleaned >= self.inner.scan_batch_limit {
                warn!(
                    "BrokerFastFailure page-cache cleanup reached batch limit: cleaned={}",
                    cleaned
                );
            }
        }

        for kind in ALL_QUEUE_KINDS {
            let cleaned = self.inner.queues.get(kind).clean_expired(
                now_millis,
                self.wait_time_millis(kind),
                self.inner.scan_batch_limit,
            );
            if cleaned > 0 && self.should_log_jstack(now_millis as i64) {
                warn!(
                    "BrokerFastFailure cleaned expired requests: queue={}, cleaned={}",
                    kind.name(),
                    cleaned
                );
            }
        }
    }

    fn wait_time_millis(&self, kind: FastFailureQueueKind) -> u64 {
        match kind {
            FastFailureQueueKind::Send => self.inner.broker_config.wait_time_mills_in_send_queue,
            FastFailureQueueKind::Pull => self.inner.broker_config.wait_time_mills_in_pull_queue,
            FastFailureQueueKind::LitePull => self.inner.broker_config.wait_time_mills_in_lite_pull_queue,
            FastFailureQueueKind::Heartbeat => self.inner.broker_config.wait_time_mills_in_heartbeat_queue,
            FastFailureQueueKind::Transaction => self.inner.broker_config.wait_time_mills_in_transaction_queue,
            FastFailureQueueKind::Ack => self.inner.broker_config.wait_time_mills_in_ack_queue,
            FastFailureQueueKind::AdminBroker => self.inner.broker_config.wait_time_mills_in_admin_broker_queue,
        }
    }

    fn is_os_page_cache_busy(&self) -> bool {
        let checker = self.inner.page_cache_busy_checker.lock().clone();
        checker.is_some_and(|checker| checker())
    }

    fn should_log_jstack(&self, now_millis: i64) -> bool {
        let last = self.inner.jstack_time_millis.load(Ordering::Acquire);
        if now_millis.saturating_sub(last) <= JSTACK_LOG_INTERVAL_MILLIS {
            return false;
        }

        self.inner
            .jstack_time_millis
            .compare_exchange(last, now_millis, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

fn permits_for(kind: FastFailureQueueKind) -> usize {
    let cpus = num_cpus::get().max(1);
    match kind {
        FastFailureQueueKind::Send => cpus.clamp(1, 8),
        FastFailureQueueKind::Pull | FastFailureQueueKind::LitePull => (cpus * 2).clamp(4, 32),
        FastFailureQueueKind::Heartbeat => cpus.clamp(2, 16),
        FastFailureQueueKind::Transaction | FastFailureQueueKind::Ack => (cpus * 2).clamp(4, 16),
        FastFailureQueueKind::AdminBroker => cpus.clamp(2, 8),
    }
}

fn system_busy_response(
    opaque: i32,
    remark_prefix: &'static str,
    period_in_queue_millis: u64,
    queue_size: usize,
) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code_remark(
        ResponseCode::SystemBusy,
        format!(
            "{remark_prefix}broker busy, start flow control for a while, period in queue: {period_in_queue_millis}ms, \
             size of queue: {queue_size}"
        ),
    )
    .set_opaque(opaque)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use tokio::sync::oneshot::error::TryRecvError;

    use super::*;

    fn config_with_fast_failure_waits(wait_millis: u64) -> Arc<BrokerConfig> {
        Arc::new(BrokerConfig {
            wait_time_mills_in_send_queue: wait_millis,
            wait_time_mills_in_pull_queue: wait_millis,
            wait_time_mills_in_lite_pull_queue: wait_millis,
            wait_time_mills_in_heartbeat_queue: wait_millis,
            wait_time_mills_in_transaction_queue: wait_millis,
            wait_time_mills_in_ack_queue: wait_millis,
            wait_time_mills_in_admin_broker_queue: wait_millis,
            ..BrokerConfig::default()
        })
    }

    #[tokio::test]
    async fn head_not_expired_stops_timeout_cleanup() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(1_000));
        let now = current_millis();
        let (_head_task, mut head_rx) = fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Send, 1, now);
        let (_tail_task, mut tail_rx) = fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Send, 2, now - 2_000);

        fast_failure.clean_expired_request();

        assert!(matches!(head_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(tail_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn head_expired_is_canceled_with_timeout_response() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(10));
        let (_task, response_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Send, 7, current_millis() - 20);

        fast_failure.clean_expired_request();

        let response = response_rx.await.expect("timeout response").expect("response command");
        assert_eq!(response.code(), ResponseCode::SystemBusy.to_i32());
        assert_eq!(response.opaque(), 7);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.starts_with("[TIMEOUT_CLEAN_QUEUE]")));
    }

    #[tokio::test]
    async fn running_task_is_not_canceled_by_cleanup() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(10));
        let (task, mut response_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Pull, 9, current_millis() - 20);

        assert!(fast_failure.try_mark_running(FastFailureQueueKind::Pull, &task));
        fast_failure.clean_expired_request();
        assert!(matches!(response_rx.try_recv(), Err(TryRecvError::Empty)));

        let response = system_busy_response(9, "[TEST]", 0, 0);
        fast_failure.complete(FastFailureQueueKind::Pull, &task, Some(response));
        let response = response_rx.await.expect("worker response").expect("response command");
        assert_eq!(response.opaque(), 9);
    }

    #[tokio::test]
    async fn page_cache_busy_cleans_send_queue_without_waiting() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(60_000));
        fast_failure.set_page_cache_busy_checker(|| true);
        let (_task, response_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Send, 11, current_millis());

        fast_failure.clean_expired_request();

        let response = response_rx
            .await
            .expect("page cache response")
            .expect("response command");
        assert_eq!(response.code(), ResponseCode::SystemBusy.to_i32());
        assert_eq!(response.opaque(), 11);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.starts_with("[PCBUSY_CLEAN_QUEUE]")));
    }

    #[tokio::test]
    async fn start_is_idempotent_and_shutdown_stops_running_flag() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(1_000));

        fast_failure.start();
        fast_failure.start();
        assert!(fast_failure.is_running());

        fast_failure.shutdown();
        assert!(!fast_failure.is_running());
    }

    #[tokio::test]
    async fn disabled_config_starts_scanner_but_does_not_clean() {
        let fast_failure = BrokerFastFailure::new(Arc::new(BrokerConfig {
            broker_fast_failure_enable: false,
            ..BrokerConfig::default()
        }));
        let (_task, mut response_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Send, 15, current_millis() - 60_000);

        fast_failure.start();
        assert!(fast_failure.is_running());

        fast_failure.clean_expired_request();

        assert!(matches!(response_rx.try_recv(), Err(TryRecvError::Empty)));
        fast_failure.shutdown();

        assert!(!fast_failure.is_running());
    }

    #[tokio::test]
    async fn page_cache_checker_is_called_when_configured() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(1_000));
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        fast_failure.set_page_cache_busy_checker(move || {
            called_clone.store(true, Ordering::Release);
            false
        });

        fast_failure.clean_expired_request();

        assert!(called.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn completed_head_is_skipped_and_next_expired_task_is_cleaned() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(10));
        let (completed_task, mut completed_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Ack, 21, current_millis() - 30);
        assert!(fast_failure.try_mark_running(FastFailureQueueKind::Ack, &completed_task));
        fast_failure.complete(FastFailureQueueKind::Ack, &completed_task, None);

        let (_expired_task, expired_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Ack, 22, current_millis() - 30);

        fast_failure.clean_expired_request();

        assert!(matches!(completed_rx.try_recv(), Ok(None)));
        let response = expired_rx.await.expect("expired response").expect("response command");
        assert_eq!(response.opaque(), 22);
        assert_eq!(response.code(), ResponseCode::SystemBusy.to_i32());
        assert!(response
            .remark()
            .is_some_and(|remark| remark.starts_with("[TIMEOUT_CLEAN_QUEUE]")));
    }

    #[tokio::test]
    async fn page_cache_busy_only_cleans_send_queue() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(60_000));
        fast_failure.set_page_cache_busy_checker(|| true);
        let (_send_task, send_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Send, 31, current_millis());
        let (_pull_task, mut pull_rx) =
            fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Pull, 32, current_millis());

        fast_failure.clean_expired_request();

        let response = send_rx.await.expect("send response").expect("response command");
        assert_eq!(response.opaque(), 31);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.starts_with("[PCBUSY_CLEAN_QUEUE]")));
        assert!(matches!(pull_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn timeout_cleanup_respects_single_scan_batch_limit() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(0));
        let now = current_millis();
        let mut receivers = Vec::with_capacity(DEFAULT_SCAN_BATCH_LIMIT + 1);
        for opaque in 0..=DEFAULT_SCAN_BATCH_LIMIT {
            let (_task, rx) =
                fast_failure.enqueue_with_timestamp(FastFailureQueueKind::AdminBroker, opaque as i32, now);
            receivers.push(rx);
        }

        fast_failure.clean_expired_request();

        for (index, rx) in receivers.iter_mut().take(DEFAULT_SCAN_BATCH_LIMIT).enumerate() {
            let response = rx
                .try_recv()
                .expect("expired task should be cleaned in the first batch")
                .expect("response command");
            assert_eq!(response.opaque(), index as i32);
        }
        assert!(matches!(
            receivers
                .get_mut(DEFAULT_SCAN_BATCH_LIMIT)
                .expect("last receiver")
                .try_recv(),
            Err(TryRecvError::Empty)
        ));

        fast_failure.clean_expired_request();
        let response = receivers
            .pop()
            .expect("last receiver")
            .await
            .expect("second batch response")
            .expect("response command");
        assert_eq!(response.opaque(), DEFAULT_SCAN_BATCH_LIMIT as i32);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_enqueue_and_timeout_cleanup_returns_each_opaque_once() {
        let fast_failure = BrokerFastFailure::new(config_with_fast_failure_waits(0));
        let now = current_millis();
        let request_count = 512usize;
        let mut handles = Vec::with_capacity(request_count);

        for opaque in 0..request_count {
            let fast_failure = fast_failure.clone();
            handles.push(tokio::spawn(async move {
                fast_failure.enqueue_with_timestamp(FastFailureQueueKind::Pull, opaque as i32, now)
            }));
        }

        let mut receivers = Vec::with_capacity(request_count);
        for handle in handles {
            let (_task, rx) = handle.await.expect("enqueue task should finish");
            receivers.push(rx);
        }

        for _ in 0..=((request_count / DEFAULT_SCAN_BATCH_LIMIT) + 1) {
            fast_failure.clean_expired_request();
        }

        let mut seen = vec![false; request_count];
        for rx in receivers {
            let response = rx.await.expect("cleanup response").expect("response command");
            assert_eq!(response.code(), ResponseCode::SystemBusy.to_i32());
            let opaque = response.opaque() as usize;
            assert!(opaque < request_count);
            assert!(!seen[opaque], "opaque {opaque} should be returned once");
            seen[opaque] = true;
        }
        assert!(seen.into_iter().all(|value| value));
    }
}
