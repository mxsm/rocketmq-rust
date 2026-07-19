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

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_rust::RocketMQTokioMutex;
use serde::Serialize;
use std::sync::LazyLock;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::process_queue::REBALANCE_LOCK_INTERVAL;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use crate::consumer::listener::consume_return_type::ConsumeReturnType;
use crate::consumer::listener::message_listener_orderly::ArcMessageListenerOrderly;
use crate::consumer::message_queue_lock::MessageQueueLock;
use crate::consumer::mq_consumer_inner::MQConsumerInnerLocal;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::runtime::schedule_client_fixed_delay_task;
use crate::runtime::spawn_client_blocking_io;
use crate::runtime::spawn_client_task;
use crate::runtime::ClientScheduledTaskHandle;

static MAX_TIME_CONSUME_CONTINUOUSLY: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("rocketmq.client.maxTimeConsumeContinuously")
        .unwrap_or("60000".to_string())
        .parse()
        .unwrap_or(60000)
});

pub struct ConsumeMessageOrderlyService {
    pub(crate) default_mqpush_consumer_impl: Option<Weak<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: Arc<ClientConfig>,
    pub(crate) consumer_config: Arc<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcMessageListenerOrderly,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) global_lock: Arc<RocketMQTokioMutex<()>>,
    pub(crate) message_queue_lock: MessageQueueLock,
    pub(crate) lock_periodic_task_handle: Arc<Mutex<Option<OrderlyTaskHandle>>>,
    pub(crate) active_tasks: Arc<AtomicUsize>,
    pub(crate) concurrency_limiter: Arc<Semaphore>,
    pub(crate) max_concurrency: Arc<AtomicUsize>,
    orderly_task_tracker: TaskTracker,
    force_stop_token: CancellationToken,
    submitted_tasks: Arc<AtomicU64>,
}

pub(crate) struct OrderlyTaskHandle(ClientScheduledTaskHandle);

impl OrderlyTaskHandle {
    async fn shutdown(self, timeout: Duration) -> bool {
        let report = self.0.shutdown(timeout).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "orderly lock periodic task shutdown report is unhealthy"
            );
        }
        report.is_healthy()
    }

    fn abort(self) {
        let report = self.0.shutdown_now();
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "orderly lock periodic task shutdown_now report is unhealthy"
            );
        }
    }

    fn task_count(&self) -> usize {
        self.0.task_count()
    }

    fn schedule_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.0.schedule_snapshot()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderlyLockPeriodicLifecycleProbe {
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub scheduled_runs: u64,
    pub scheduled_skips: u64,
    pub scheduled_overlaps: u64,
    pub scheduled_failures: u64,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}

fn spawn_tracked_orderly_task<F>(
    thread_name: &'static str,
    tracker: &TaskTracker,
    force_stop_token: &CancellationToken,
    submitted_tasks: &Arc<AtomicU64>,
    task: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    submitted_tasks.fetch_add(1, Ordering::Relaxed);
    let force_stop_token = force_stop_token.clone();
    let tracked_task = tracker.track_future(async move {
        let mut task = Box::pin(task);
        tokio::select! {
            biased;
            _ = force_stop_token.cancelled() => {}
            _ = &mut task => {}
        }
    });

    if let Err(error) = spawn_client_task(thread_name, tracked_task) {
        warn!("Failed to spawn {} background task: {}", thread_name, error);
        warn!("Failed to track {} background task", thread_name);
    }
}

impl ConsumeMessageOrderlyService {
    pub fn new(
        client_config: Arc<ClientConfig>,
        consumer_config: Arc<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerOrderly,
        default_mqpush_consumer_impl: Option<Weak<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let core_pool_size = consumer_config.consume_thread_min as usize;
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            stopped: Arc::new(AtomicBool::new(false)),
            global_lock: Arc::new(Default::default()),
            message_queue_lock: Default::default(),
            lock_periodic_task_handle: Arc::new(Mutex::new(None)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            concurrency_limiter: Arc::new(Semaphore::new(core_pool_size)),
            max_concurrency: Arc::new(AtomicUsize::new(core_pool_size)),
            orderly_task_tracker: TaskTracker::new(),
            force_stop_token: CancellationToken::new(),
            submitted_tasks: Arc::new(AtomicU64::new(0)),
        }
    }

    fn resize_available_permits(semaphore: &Semaphore, old_total: usize, new_total: usize) {
        let available = semaphore.available_permits();
        let in_flight = old_total.saturating_sub(available);
        let target_available = new_total.saturating_sub(in_flight);

        match target_available.cmp(&available) {
            std::cmp::Ordering::Greater => semaphore.add_permits(target_available - available),
            std::cmp::Ordering::Less => {
                let _ = semaphore.forget_permits(available - target_available);
            }
            std::cmp::Ordering::Equal => {}
        }
    }

    fn consumer_impl(&self) -> Option<Arc<DefaultMQPushConsumerImpl>> {
        self.default_mqpush_consumer_impl.as_ref().and_then(Weak::upgrade)
    }

    pub async fn lock_mqperiodically(&self) {
        let lock = self.global_lock.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return;
        }

        let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
            warn!(
                "lockMQPeriodically skipped: DefaultMQPushConsumerImpl is not initialized, group={}",
                self.consumer_group
            );
            return;
        };
        default_mqpush_consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .lock_all()
            .await;

        drop(lock);
    }

    pub async fn lock_mq_periodically(&self) {
        self.lock_mqperiodically().await;
    }

    fn start_lock_periodic_with_schedule(&self, this: Arc<Self>, initial_delay: Duration, period: Duration) {
        let lock_handle = self.lock_periodic_task_handle.clone();
        let stopped = self.stopped.clone();
        let handle = schedule_client_fixed_delay_task(
            "rocketmq-client-orderly-lock-periodic",
            initial_delay,
            period,
            Duration::from_secs(5),
            move || {
                let service = this.clone();
                let stopped = stopped.clone();
                async move {
                    if !stopped.load(Ordering::Acquire) {
                        service.lock_mqperiodically().await;
                    }
                }
            },
        )
        .map(OrderlyTaskHandle)
        .map_err(|error| {
            warn!("Failed to spawn rocketmq-client-orderly-lock-periodic background task: {error}");
        })
        .ok();
        *lock_handle.lock() = handle;
    }

    fn lock_periodic_task_count(&self) -> usize {
        self.lock_periodic_task_handle
            .lock()
            .as_ref()
            .map(OrderlyTaskHandle::task_count)
            .unwrap_or_default()
    }

    fn lock_periodic_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.lock_periodic_task_handle
            .lock()
            .as_ref()
            .map(OrderlyTaskHandle::schedule_snapshot)
            .unwrap_or_default()
    }

    pub fn reset_namespace(&self, msgs: &mut [Arc<MessageExt>]) {
        let namespace = self.client_config.resolved_namespace().unwrap_or_default();
        if namespace.is_empty() {
            return;
        }

        for msg in msgs {
            let topic = msg.topic().to_string();
            Arc::make_mut(msg).set_topic(CheetahString::from_string(
                NamespaceUtil::without_namespace_with_namespace(topic.as_str(), namespace.as_str()),
            ));
        }
    }

    pub async fn unlock_all_mq(&self) {
        let lock = self.global_lock.lock().await;

        if let Some(default_mqpush_consumer_impl) = self.consumer_impl() {
            default_mqpush_consumer_impl
                .rebalance_impl
                .rebalance_impl_inner
                .unlock_all(false)
                .await;
        } else {
            warn!(
                "unlockAllMQ skipped: DefaultMQPushConsumerImpl is not initialized, group={}",
                self.consumer_group
            );
        }

        drop(lock);
    }

    pub async fn try_lock_later_and_reconsume(
        &self,
        consume_message_orderly_service: Arc<Self>,
        message_queue: &MessageQueue,
        process_queue: Arc<ProcessQueue>,
        delay_mills: u64,
    ) {
        let consume_message_orderly_service_cloned = consume_message_orderly_service.clone();
        let message_queue = message_queue.clone();
        spawn_tracked_orderly_task(
            "rocketmq-client-orderly-lock-reconsume",
            &self.orderly_task_tracker,
            &self.force_stop_token,
            &self.submitted_tasks,
            async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_mills)).await;

                if consume_message_orderly_service.lock_one_mq(&message_queue).await {
                    consume_message_orderly_service.submit_consume_request_later(
                        process_queue,
                        message_queue.clone(),
                        10,
                        consume_message_orderly_service_cloned,
                    );
                } else {
                    consume_message_orderly_service.submit_consume_request_later(
                        process_queue,
                        message_queue.clone(),
                        3_000,
                        consume_message_orderly_service_cloned,
                    );
                }
            },
        );
    }

    pub async fn lock_one_mq(&self, message_queue: &MessageQueue) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }

        let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
            warn!(
                "lockOneMQ skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}",
                self.consumer_group, message_queue
            );
            return false;
        };
        default_mqpush_consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .lock(message_queue)
            .await
    }

    fn submit_consume_request_later(
        &self,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        suspend_time_millis: i64,
        this: Arc<Self>,
    ) {
        let mut time_millis = suspend_time_millis;
        if time_millis == -1 {
            time_millis = self.consumer_config.suspend_current_queue_time_millis as i64
        }

        time_millis = time_millis.clamp(10, 30000);

        let delay = Duration::from_millis(time_millis as u64);
        let stopped = self.stopped.clone();

        spawn_tracked_orderly_task(
            "rocketmq-client-orderly-consume-delay",
            &self.orderly_task_tracker,
            &self.force_stop_token,
            &self.submitted_tasks,
            async move {
                tokio::time::sleep(delay).await;

                if stopped.load(Ordering::Acquire) {
                    warn!("Service stopped, discard delayed consume request");
                    return;
                }

                let this_ = this.clone();
                this.submit_consume_request(this_, vec![], process_queue, message_queue, true)
                    .await;
            },
        );
    }

    #[inline]
    fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            i32::MAX
        } else {
            self.consumer_config.max_reconsume_times
        }
    }

    pub async fn send_message_back(&self, msg: &MessageExt) -> bool {
        let mut new_msg = match Message::builder()
            .topic(mix_all::get_retry_topic(self.consumer_group.as_str()))
            .body(msg.get_body().cloned().unwrap_or_default())
            .build()
        {
            Ok(message) => message,
            Err(error) => {
                warn!(
                    "sendMessageBack skipped: failed to build retry message, group={} msg={} error={}",
                    self.consumer_group, msg, error
                );
                return false;
            }
        };
        MessageAccessor::set_properties(&mut new_msg, msg.get_properties().clone());
        let origin_msg_id = MessageAccessor::get_origin_message_id(msg).unwrap_or(msg.msg_id.clone());
        MessageAccessor::set_origin_message_id(&mut new_msg, origin_msg_id);
        new_msg.set_flag(msg.get_flag());
        MessageAccessor::put_property(
            &mut new_msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            msg.topic().to_owned(),
        );
        MessageAccessor::set_reconsume_time(
            &mut new_msg,
            CheetahString::from_string((msg.reconsume_times() + 1).to_string()),
        );
        MessageAccessor::set_max_reconsume_times(
            &mut new_msg,
            CheetahString::from_string(self.get_max_reconsume_times().to_string()),
        );
        MessageAccessor::clear_property(&mut new_msg, MessageConst::PROPERTY_TRANSACTION_PREPARED);
        new_msg.set_delay_time_level(3 + msg.reconsume_times());
        let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
            warn!(
                "sendMessageBack skipped: DefaultMQPushConsumerImpl is not initialized, group={} msg={}",
                self.consumer_group, msg
            );
            return false;
        };
        let Some(mut client_instance) = default_mqpush_consumer_impl.get_mq_client_factory() else {
            warn!(
                "sendMessageBack skipped: MQClientInstance is not initialized, group={} msg={}",
                self.consumer_group, msg
            );
            return false;
        };

        let result = client_instance.default_producer.send(new_msg).await;
        result.is_ok()
    }

    async fn check_reconsume_times(&self, msgs: &mut [Arc<MessageExt>]) -> bool {
        let mut suspend = false;
        if !msgs.is_empty() {
            for msg in msgs {
                let reconsume_times = msg.reconsume_times;
                if reconsume_times >= self.get_max_reconsume_times() {
                    MessageAccessor::set_reconsume_time(
                        Arc::make_mut(msg),
                        CheetahString::from_string(reconsume_times.to_string()),
                    );
                    if !self.send_message_back(msg).await {
                        suspend = true;
                        Arc::make_mut(msg).reconsume_times = reconsume_times + 1;
                    }
                } else {
                    suspend = true;
                    Arc::make_mut(msg).reconsume_times = reconsume_times + 1;
                }
            }
        }
        suspend
    }

    #[allow(deprecated)]
    async fn process_consume_result(
        &self,
        mut msgs: Vec<Arc<MessageExt>>,
        this: Arc<Self>,
        status: ConsumeOrderlyStatus,
        context: &ConsumeOrderlyContext,
        consume_request: &mut ConsumeRequest,
    ) -> bool {
        let msg_count = msgs.len() as u64;
        let (continue_consume, commit_offset) = if context.is_auto_commit() {
            match status {
                ConsumeOrderlyStatus::Success => (true, consume_request.process_queue.commit().await),
                ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::Commit => {
                    warn!(
                        "the message queue consume result is illegal, we think you want to ack these messages, so we \
                         will ack them: {}",
                        consume_request.message_queue
                    );
                    (true, consume_request.process_queue.commit().await)
                }
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                    if self.check_reconsume_times(&mut msgs).await {
                        consume_request.process_queue.make_message_to_consume_again(&msgs).await;
                        self.submit_consume_request_later(
                            consume_request.process_queue.clone(),
                            consume_request.message_queue.clone(),
                            context.get_suspend_current_queue_time_millis(),
                            this,
                        );
                        (false, -1)
                    } else {
                        (true, consume_request.process_queue.commit().await)
                    }
                }
            }
        } else {
            match status {
                ConsumeOrderlyStatus::Success => (true, -1),
                ConsumeOrderlyStatus::Commit => (true, consume_request.process_queue.commit().await),
                ConsumeOrderlyStatus::Rollback => {
                    consume_request.process_queue.rollback().await;
                    self.submit_consume_request_later(
                        consume_request.process_queue.clone(),
                        consume_request.message_queue.clone(),
                        context.get_suspend_current_queue_time_millis(),
                        this,
                    );
                    (false, -1)
                }
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                    if self.check_reconsume_times(&mut msgs).await {
                        consume_request.process_queue.make_message_to_consume_again(&msgs).await;
                        self.submit_consume_request_later(
                            consume_request.process_queue.clone(),
                            consume_request.message_queue.clone(),
                            context.get_suspend_current_queue_time_millis(),
                            this,
                        );
                        (false, -1)
                    } else {
                        (true, -1)
                    }
                }
            }
        };

        // Update per-topic/group consume throughput counters based on the orderly consume result.
        if let Some(impl_) = self.consumer_impl() {
            if let Some(client_instance) = impl_.get_mq_client_factory() {
                let mgr = client_instance.consumer_stats_manager();
                let topic = consume_request.message_queue.topic().as_str();
                let group = self.consumer_group.as_str();
                let is_ok = if context.is_auto_commit() {
                    matches!(
                        status,
                        ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::Commit
                    )
                } else {
                    matches!(status, ConsumeOrderlyStatus::Success)
                };
                if is_ok {
                    mgr.inc_consume_ok_tps(group, topic, msg_count);
                } else if matches!(status, ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) {
                    mgr.inc_consume_failed_tps(group, topic, msg_count);
                }
            }
        }

        if commit_offset >= 0 && !consume_request.process_queue.is_dropped() {
            let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
                warn!(
                    "orderly consume offset update skipped: DefaultMQPushConsumerImpl is not initialized, group={}, \
                     mq={}",
                    self.consumer_group, consume_request.message_queue
                );
                return continue_consume;
            };
            let Some(offset_store) = default_mqpush_consumer_impl.offset_store() else {
                warn!(
                    "orderly consume offset update skipped: OffsetStore is not initialized, group={}, mq={}",
                    self.consumer_group, consume_request.message_queue
                );
                return continue_consume;
            };
            offset_store
                .update_offset(&consume_request.message_queue, commit_offset, false)
                .await;
        }
        continue_consume
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessageOrderlyService {
    fn start(&self, this: Arc<Self>) {
        if MessageModel::Clustering == self.consumer_config.message_model {
            self.start_lock_periodic_with_schedule(
                this,
                Duration::from_millis(1_000),
                Duration::from_millis(*REBALANCE_LOCK_INTERVAL),
            );
        }
    }

    async fn shutdown(&self, await_terminate_millis: u64) {
        info!("{} ConsumeMessageOrderlyService shutdown started", self.consumer_group);

        self.stopped.store(true, Ordering::Release);
        self.concurrency_limiter.close();
        self.orderly_task_tracker.close();

        if MessageModel::Clustering == self.consumer_config.message_model {
            let handle = { self.lock_periodic_task_handle.lock().take() };
            if let Some(handle) = handle {
                let timeout = Duration::from_millis(await_terminate_millis);
                if !handle.shutdown(timeout).await {
                    warn!(
                        "{} orderly lock periodic task did not stop within {}ms; aborted",
                        self.consumer_group, await_terminate_millis
                    );
                }
            }
        }

        let timeout = Duration::from_millis(await_terminate_millis);
        let start_time = Instant::now();

        match tokio::time::timeout(timeout, self.orderly_task_tracker.wait()).await {
            Ok(()) => {}
            Err(_) => {
                warn!(
                    "{} ConsumeMessageOrderlyService shutdown timeout, {} active consume tasks, {} submitted tasks; \
                     forcing remaining task futures to stop",
                    self.consumer_group,
                    self.active_tasks.load(Ordering::Acquire),
                    self.submitted_tasks.load(Ordering::Acquire)
                );
                self.force_stop_token.cancel();
                if tokio::time::timeout(Duration::from_secs(1), self.orderly_task_tracker.wait())
                    .await
                    .is_err()
                {
                    warn!(
                        "{} ConsumeMessageOrderlyService force stop timed out, {} active consume tasks remain",
                        self.consumer_group,
                        self.active_tasks.load(Ordering::Acquire)
                    );
                }
            }
        }

        while self.active_tasks.load(Ordering::Acquire) > 0 {
            if start_time.elapsed() >= timeout {
                warn!(
                    "{} ConsumeMessageOrderlyService shutdown timeout, {} tasks still active",
                    self.consumer_group,
                    self.active_tasks.load(Ordering::Acquire)
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if MessageModel::Clustering == self.consumer_config.message_model {
            self.unlock_all_mq().await;
        }

        info!(
            "{} ConsumeMessageOrderlyService shutdown completed",
            self.consumer_group
        );
    }

    fn update_core_pool_size(&self, core_pool_size: usize) {
        if core_pool_size > 0
            && core_pool_size <= i16::MAX as usize
            && core_pool_size < self.consumer_config.consume_thread_max as usize
        {
            let old_size = self.max_concurrency.swap(core_pool_size, Ordering::AcqRel);
            Self::resize_available_permits(&self.concurrency_limiter, old_size, core_pool_size);
        }
    }

    fn get_core_pool_size(&self) -> usize {
        self.max_concurrency.load(Ordering::Acquire)
    }

    #[allow(deprecated)]
    async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        info!("consumeMessageDirectly receive new message: {}", msg);
        let mq = MessageQueue::from_parts(msg.topic().clone(), broker_name.unwrap_or_default(), msg.queue_id());
        let mut msgs = vec![Arc::new(msg)];
        let mut context = ConsumeOrderlyContext::new(mq);
        if let Some(default_mqpush_consumer_impl) = self.consumer_impl() {
            default_mqpush_consumer_impl.reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());
        } else {
            warn!(
                "consumeMessageDirectly namespace reset skipped: DefaultMQPushConsumerImpl is not initialized, \
                 group={}",
                self.consumer_group
            );
        }

        let begin_timestamp = Instant::now();

        let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
            msgs.first().map(|msg| msg.as_ref()),
            msgs.len(),
            self.consumer_group.as_str(),
            context.get_message_queue(),
            "orderly_direct",
        );
        let _entered = process_span.enter();
        let status = self.message_listener.consume_message(
            &msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>()[..],
            &mut context,
        );
        match &status {
            Ok(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit) => {
                crate::consumer::consumer_impl::observability::record_process_event(
                    &process_span,
                    "RocketMQ CONSUMER ACK",
                    "success",
                    msgs.len(),
                );
            }
            Ok(ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => {
                crate::consumer::consumer_impl::observability::record_process_event(
                    &process_span,
                    "RocketMQ CONSUMER RETRY",
                    "reconsume_later",
                    msgs.len(),
                );
            }
            Err(_) => {
                crate::consumer::consumer_impl::observability::record_process_event(
                    &process_span,
                    "RocketMQ CONSUMER RETRY",
                    "exception",
                    msgs.len(),
                );
            }
        }
        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        rocketmq_observability::metrics::client::record_consume(msgs.len(), consume_rt);

        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(true);
        result.set_auto_commit(context.is_auto_commit());
        match status {
            Ok(status) => match status {
                ConsumeOrderlyStatus::Success => {
                    result.set_consume_result(CMResult::CRSuccess);
                }
                ConsumeOrderlyStatus::Rollback => {
                    result.set_consume_result(CMResult::CRRollback);
                }
                ConsumeOrderlyStatus::Commit => {
                    result.set_consume_result(CMResult::CRCommit);
                }
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                    result.set_consume_result(CMResult::CRLater);
                }
            },
            Err(e) => {
                result.set_consume_result(CMResult::CRThrowException);
                result.set_remark(CheetahString::from_string(e.to_string()))
            }
        }
        result.set_spent_time_mills(consume_rt);
        info!("consumeMessageDirectly Result: {}", result);
        result
    }

    async fn submit_consume_request(
        &self,
        this: Arc<Self>,
        msgs: Vec<Arc<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        if self.stopped.load(Ordering::Acquire) {
            warn!("Service stopped, reject new consume request");
            return;
        }

        if !dispatch_to_consume {
            return;
        }
        let mut consume_request = ConsumeRequest {
            process_queue,
            message_queue,
            default_mqpush_consumer_impl: self.consumer_impl(),
            consumer_group: self.consumer_group.clone(),
        };
        let limiter = self.concurrency_limiter.clone();
        let stopped = self.stopped.clone();
        spawn_tracked_orderly_task(
            "rocketmq-client-orderly-consume",
            &self.orderly_task_tracker,
            &self.force_stop_token,
            &self.submitted_tasks,
            async move {
                if stopped.load(Ordering::Acquire) {
                    return;
                }

                let Ok(_permit) = limiter.acquire().await else {
                    return;
                };

                if stopped.load(Ordering::Acquire) {
                    return;
                }

                consume_request.run(this).await;
            },
        );
    }

    async fn submit_pop_consume_request(
        &self,
        this: Arc<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let _ = (this, msgs, process_queue, message_queue);
    }
}

struct ConsumeRequest {
    process_queue: Arc<ProcessQueue>,
    message_queue: MessageQueue,
    default_mqpush_consumer_impl: Option<Arc<DefaultMQPushConsumerImpl>>,
    consumer_group: CheetahString,
}

impl ConsumeRequest {
    #[allow(deprecated)]
    async fn run(&mut self, consume_message_orderly_service: Arc<ConsumeMessageOrderlyService>) {
        if consume_message_orderly_service.stopped.load(Ordering::Acquire) {
            warn!(
                "run, service stopped, discard consume request for {}",
                self.message_queue
            );
            return;
        }

        if self.process_queue.is_dropped() {
            warn!(
                "run, the message queue not be able to consume, because it's dropped. {}",
                self.message_queue
            );
            return;
        }

        consume_message_orderly_service
            .active_tasks
            .fetch_add(1, Ordering::AcqRel);
        let active_tasks = consume_message_orderly_service.active_tasks.clone();

        struct TaskGuard {
            active_tasks: Arc<AtomicUsize>,
        }

        impl Drop for TaskGuard {
            fn drop(&mut self) {
                self.active_tasks.fetch_sub(1, Ordering::AcqRel);
            }
        }

        let _guard = TaskGuard { active_tasks };

        let consume_message_orderly_service_inner = consume_message_orderly_service.clone();
        let lock = consume_message_orderly_service_inner
            .message_queue_lock
            .fetch_lock_object(&self.message_queue)
            .await;
        let locked = lock.lock().await;
        let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref().cloned() else {
            warn!(
                "orderly consume request skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}",
                self.consumer_group, self.message_queue
            );
            return;
        };
        let message_model = default_mqpush_consumer_impl.message_model();
        if MessageModel::Broadcasting == message_model
            || self.process_queue.is_locked() && !self.process_queue.is_lock_expired()
        {
            let consume_batch_size = consume_message_orderly_service_inner
                .consumer_config
                .consume_message_batch_max_size
                .max(1);
            let begin_time = Instant::now();
            loop {
                if self.process_queue.is_dropped() {
                    warn!(
                        "the message queue not be able to consume, because it's dropped. {}",
                        self.message_queue
                    );
                    break;
                }
                if MessageModel::Clustering == message_model && !self.process_queue.is_locked() {
                    warn!(
                        "the message queue not be able to consume, because it's not locked. {}",
                        self.message_queue
                    );
                    consume_message_orderly_service_inner
                        .try_lock_later_and_reconsume(
                            consume_message_orderly_service.clone(),
                            &self.message_queue,
                            self.process_queue.clone(),
                            10,
                        )
                        .await;
                    break;
                }

                if MessageModel::Clustering == message_model && self.process_queue.is_lock_expired() {
                    warn!(
                        "the message queue lock expired, so consume later {}",
                        self.message_queue
                    );
                    consume_message_orderly_service_inner
                        .try_lock_later_and_reconsume(
                            consume_message_orderly_service.clone(),
                            &self.message_queue,
                            self.process_queue.clone(),
                            10,
                        )
                        .await;
                    break;
                }
                let interval = begin_time.elapsed().as_millis() as u64;
                if interval > *MAX_TIME_CONSUME_CONTINUOUSLY {
                    consume_message_orderly_service_inner
                        .try_lock_later_and_reconsume(
                            consume_message_orderly_service.clone(),
                            &self.message_queue,
                            self.process_queue.clone(),
                            10,
                        )
                        .await;
                    break;
                }
                let mut msgs = self.process_queue.take_messages(consume_batch_size).await;
                default_mqpush_consumer_impl.reset_retry_and_namespace(
                    &mut msgs,
                    consume_message_orderly_service_inner.consumer_group.as_ref(),
                );
                if msgs.is_empty() {
                    break;
                }
                let mut consume_message_context = None;
                let mut status = None;
                if default_mqpush_consumer_impl.has_hook() {
                    consume_message_context = Some(
                        ConsumeMessageContext::new(self.consumer_group.clone(), &msgs)
                            .with_mq(self.message_queue.clone())
                            .with_namespace(
                                default_mqpush_consumer_impl
                                    .client_config_snapshot()
                                    .resolved_namespace()
                                    .unwrap_or_default(),
                            ),
                    );
                    if let Some(context) = consume_message_context.as_mut() {
                        default_mqpush_consumer_impl.execute_hook_before(context);
                    }
                }
                let begin_timestamp = Instant::now();
                let mut has_exception = false;
                let consume_lock = self.process_queue.consume_lock.clone().read_owned().await;
                if self.process_queue.is_dropped() {
                    warn!(
                        "consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                        self.message_queue
                    );
                    break;
                }
                let msgs_owned: Vec<MessageExt> = msgs.iter().map(|m| m.as_ref().clone()).collect();
                let listener = consume_message_orderly_service_inner.message_listener.clone();
                let mq_for_spawn = self.message_queue.clone();
                let consumer_group_for_span = self.consumer_group.clone();
                let (consume_result, context, process_span) =
                    spawn_client_blocking_io("client.orderly.consume", move || {
                        let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
                            msgs_owned.first(),
                            msgs_owned.len(),
                            consumer_group_for_span.as_str(),
                            &mq_for_spawn,
                            "orderly",
                        );
                        let mut ctx = ConsumeOrderlyContext::new(mq_for_spawn);
                        let vec: Vec<&MessageExt> = msgs_owned.iter().collect();
                        let result = {
                            let _entered = process_span.enter();
                            listener.consume_message(&vec, &mut ctx)
                        };
                        (result, ctx, process_span)
                    })
                    .await
                    .unwrap_or_else(|e| {
                        (
                            Err(rocketmq_error::RocketMQError::InvalidProperty(format!(
                                "orderly consume task panicked: {e}"
                            ))),
                            ConsumeOrderlyContext::new(self.message_queue.clone()),
                            crate::consumer::consumer_impl::observability::consumer_process_span(
                                msgs.first().map(|msg| msg.as_ref()),
                                msgs.len(),
                                self.consumer_group.as_str(),
                                &self.message_queue,
                                "orderly",
                            ),
                        )
                    });
                drop(consume_lock);
                match consume_result {
                    Ok(value) => {
                        status = Some(value);
                    }
                    Err(e) => {
                        has_exception = true;
                        tracing::error!(
                            "consumeMessage Orderly exception: {:?}, Group: {}, Msgs: {}, MQ: {}",
                            e,
                            self.consumer_group,
                            msgs.len(),
                            self.message_queue
                        );
                    }
                }
                if matches!(
                    status,
                    None | Some(ConsumeOrderlyStatus::Rollback)
                        | Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment)
                ) {
                    warn!(
                        "consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                        self.consumer_group,
                        msgs.len(),
                        self.message_queue,
                    );
                }
                let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
                rocketmq_observability::metrics::client::record_consume(msgs.len(), consume_rt);
                let return_type = match status {
                    None => {
                        if has_exception {
                            ConsumeReturnType::Exception
                        } else {
                            ConsumeReturnType::ReturnNull
                        }
                    }
                    Some(status_value) => {
                        if consume_rt
                            >= default_mqpush_consumer_impl.consumer_config_snapshot().consume_timeout * 60 * 1000
                        {
                            ConsumeReturnType::TimeOut
                        } else if status_value == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                            ConsumeReturnType::Failed
                        } else {
                            ConsumeReturnType::Success
                        }
                    }
                };
                if default_mqpush_consumer_impl.has_hook() {
                    if let Some(context) = consume_message_context.as_mut() {
                        context.props.insert(
                            CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE),
                            return_type.to_string().into(),
                        );
                    } else {
                        warn!(
                            "orderly consume hook context missing before return type update, group={}, mq={}",
                            self.consumer_group, self.message_queue
                        );
                    }
                }
                let final_status = status.unwrap_or(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
                match final_status {
                    ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit => {
                        crate::consumer::consumer_impl::observability::record_process_event(
                            &process_span,
                            "RocketMQ CONSUMER ACK",
                            "success",
                            msgs.len(),
                        );
                    }
                    ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                        crate::consumer::consumer_impl::observability::record_process_event(
                            &process_span,
                            "RocketMQ CONSUMER RETRY",
                            if has_exception { "exception" } else { "reconsume_later" },
                            msgs.len(),
                        );
                    }
                }
                if default_mqpush_consumer_impl.has_hook() {
                    if let Some(cmc) = consume_message_context.as_mut() {
                        cmc.success = final_status == ConsumeOrderlyStatus::Success
                            || final_status == ConsumeOrderlyStatus::Commit;
                        cmc.status = final_status.to_string().into();
                        cmc.access_channel = Some(default_mqpush_consumer_impl.client_config_snapshot().access_channel);
                        default_mqpush_consumer_impl.execute_hook_after(cmc);
                    } else {
                        warn!(
                            "orderly consume hook context missing before after-hook execution, group={}, mq={}",
                            self.consumer_group, self.message_queue
                        );
                    }
                }
                // Record message consume round-trip time.
                if let Some(client_instance) = default_mqpush_consumer_impl.get_mq_client_factory() {
                    client_instance.consumer_stats_manager().inc_consume_rt(
                        consume_message_orderly_service_inner.consumer_group.as_str(),
                        self.message_queue.topic().as_str(),
                        consume_rt,
                    );
                }
                let continue_consume = consume_message_orderly_service_inner
                    .process_consume_result(
                        msgs,
                        consume_message_orderly_service.clone(),
                        final_status,
                        &context,
                        self,
                    )
                    .await;
                if !continue_consume {
                    break;
                }
            }
        } else {
            if self.process_queue.is_dropped() {
                warn!(
                    "the message queue not be able to consume, because it's dropped. {}",
                    self.message_queue
                );
                return;
            }
            let consume_message_orderly_service_weak = consume_message_orderly_service_inner.clone();
            consume_message_orderly_service_inner
                .try_lock_later_and_reconsume(
                    consume_message_orderly_service_weak,
                    &self.message_queue,
                    self.process_queue.clone(),
                    100,
                )
                .await;
        }
        drop(locked);
    }
}

#[doc(hidden)]
pub async fn run_orderly_lock_periodic_lifecycle_probe() -> OrderlyLockPeriodicLifecycleProbe {
    let consumer_config = ConsumerConfig {
        message_model: MessageModel::Clustering,
        ..Default::default()
    };
    let listener: ArcMessageListenerOrderly =
        Arc::new(|_msgs: &[&MessageExt], _context: &mut ConsumeOrderlyContext| Ok(ConsumeOrderlyStatus::Success));
    let service = Arc::new(ConsumeMessageOrderlyService::new(
        Arc::new(ClientConfig::default()),
        Arc::new(consumer_config),
        CheetahString::from_static_str("orderly_lock_periodic_probe_group"),
        listener,
        None,
    ));
    let this = service.clone();
    service.start_lock_periodic_with_schedule(this, Duration::ZERO, Duration::from_millis(1));

    let mut snapshots = service.lock_periodic_snapshot();
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        snapshots = service.lock_periodic_snapshot();
    }

    let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
    let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
    let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
    let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
    let task_count_before_shutdown = service.lock_periodic_task_count();
    let shutdown_started_at = std::time::Instant::now();
    service.shutdown(1_000).await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = service.lock_periodic_task_count();
    let healthy = scheduled_runs > 0
        && scheduled_overlaps == 0
        && scheduled_failures == 0
        && task_count_before_shutdown > 0
        && task_count_after_shutdown == 0;

    OrderlyLockPeriodicLifecycleProbe {
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

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn message_queue() -> MessageQueue {
        MessageQueue::from_parts("topic", "broker-a", 0)
    }

    fn listener() -> ArcMessageListenerOrderly {
        Arc::new(|_msgs: &[&MessageExt], _context: &mut ConsumeOrderlyContext| Ok(ConsumeOrderlyStatus::Success))
    }

    fn consumer_group() -> CheetahString {
        CheetahString::from_static_str("group")
    }

    fn new_service(default_impl: Option<Arc<DefaultMQPushConsumerImpl>>) -> ConsumeMessageOrderlyService {
        ConsumeMessageOrderlyService::new(
            Arc::new(ClientConfig::default()),
            Arc::new(ConsumerConfig::default()),
            consumer_group(),
            listener(),
            default_impl.as_ref().map(Arc::downgrade),
        )
    }

    fn new_service_with_config(consumer_config: ConsumerConfig) -> ConsumeMessageOrderlyService {
        ConsumeMessageOrderlyService::new(
            Arc::new(ClientConfig::default()),
            Arc::new(consumer_config),
            consumer_group(),
            listener(),
            None,
        )
    }

    fn new_service_with_client_config(client_config: ClientConfig) -> ConsumeMessageOrderlyService {
        ConsumeMessageOrderlyService::new(
            Arc::new(client_config),
            Arc::new(ConsumerConfig::default()),
            consumer_group(),
            listener(),
            None,
        )
    }

    fn new_default_impl() -> Arc<DefaultMQPushConsumerImpl> {
        let consumer_config = ConsumerConfig::default();
        let consumer = Arc::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            consumer_config,
            None,
        ));
        consumer.initialize_self_reference();
        consumer
    }

    #[tokio::test]
    async fn lock_paths_without_default_impl_do_not_panic() {
        let service = new_service(None);

        service.lock_mqperiodically().await;
        service.lock_mq_periodically().await;
        assert!(!service.lock_one_mq(&message_queue()).await);
        service.unlock_all_mq().await;
    }

    #[tokio::test]
    async fn consume_message_directly_without_default_impl_does_not_panic() {
        let service = new_service(None);

        let result = service
            .consume_message_directly(MessageExt::default(), Some(CheetahString::from_static_str("broker-a")))
            .await;

        assert!(matches!(result.consume_result(), Some(CMResult::CRSuccess)));
    }

    #[tokio::test]
    async fn send_message_back_without_default_impl_returns_false() {
        let service = new_service(None);

        assert!(!service.send_message_back(&MessageExt::default()).await);
    }

    #[tokio::test]
    async fn shutdown_closes_concurrency_limiter_like_java_executor_shutdown() {
        let service = new_service(None);

        service.shutdown(100).await;

        assert!(service.concurrency_limiter.is_closed());
    }

    #[tokio::test]
    async fn tracked_orderly_task_shutdown_waits_for_completion() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();

        spawn_tracked_orderly_task(
            "rocketmq-client-orderly-tracker-test",
            &tracker,
            &token,
            &submitted,
            async move {
                completed_in_task.store(true, Ordering::Release);
            },
        );
        tracker.close();

        tokio::time::timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("tracked task should finish before timeout");

        assert!(completed.load(Ordering::Acquire));
        assert_eq!(submitted.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn tracked_orderly_task_force_stop_cancels_pending_task() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();

        spawn_tracked_orderly_task(
            "rocketmq-client-orderly-tracker-test",
            &tracker,
            &token,
            &submitted,
            async move {
                let _drop_flag = DropFlag(dropped_in_task);
                pending::<()>().await;
            },
        );
        tracker.close();

        assert!(tokio::time::timeout(Duration::from_millis(20), tracker.wait())
            .await
            .is_err());

        token.cancel();

        tokio::time::timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("force stop should release pending tracked task");

        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(submitted.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn orderly_lock_periodic_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_orderly_lock_periodic_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[test]
    fn start_without_tokio_runtime_does_not_spawn_panic() {
        let config = ConsumerConfig {
            message_model: MessageModel::Clustering,
            ..Default::default()
        };
        let service = Arc::new(new_service_with_config(config));
        let this = service.clone();

        service.start(this);
        service.stopped.store(true, Ordering::Release);
        let handle = { service.lock_periodic_task_handle.lock().take() };
        if let Some(handle) = handle {
            handle.abort();
        }
    }

    #[test]
    fn submit_consume_request_later_without_tokio_runtime_does_not_spawn_panic() {
        let service = Arc::new(new_service(None));
        let this = service.clone();

        service.submit_consume_request_later(Arc::new(ProcessQueue::new()), message_queue(), 10, this);
        service.stopped.store(true, Ordering::Release);
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn core_pool_size_starts_at_min_and_update_matches_java_bounds() {
        let config = ConsumerConfig {
            consume_thread_min: 2,
            consume_thread_max: 5,
            ..Default::default()
        };
        let service = new_service_with_config(config);

        assert_eq!(service.get_core_pool_size(), 2);

        service.update_core_pool_size(4);
        assert_eq!(service.get_core_pool_size(), 4);

        service.update_core_pool_size(5);
        assert_eq!(service.get_core_pool_size(), 4);

        service.update_core_pool_size(0);
        assert_eq!(service.get_core_pool_size(), 4);
    }

    #[test]
    fn inc_and_dec_core_pool_size_are_noops_like_java() {
        let config = ConsumerConfig {
            consume_thread_min: 2,
            consume_thread_max: 5,
            ..Default::default()
        };
        let service = new_service_with_config(config);

        service.inc_core_pool_size();
        service.dec_core_pool_size();

        assert_eq!(service.get_core_pool_size(), 2);
    }

    #[test]
    fn reset_namespace_removes_configured_namespace_like_java() {
        let mut client_config = ClientConfig::default();
        client_config.set_namespace(CheetahString::from_static_str("ns"));
        let service = new_service_with_client_config(client_config);
        let mut msg = MessageExt::default();
        msg.set_topic(CheetahString::from_static_str("ns%topic-a"));
        let mut msgs = vec![Arc::new(msg)];

        service.reset_namespace(msgs.as_mut_slice());

        assert_eq!(msgs[0].topic(), "topic-a");
    }

    #[tokio::test]
    async fn consume_request_without_default_impl_is_ignored_without_panic() {
        let service = Arc::new(new_service(None));
        let mut request = ConsumeRequest {
            process_queue: Arc::new(ProcessQueue::new()),
            message_queue: message_queue(),
            default_mqpush_consumer_impl: None,
            consumer_group: consumer_group(),
        };

        request.run(service.clone()).await;

        assert_eq!(service.active_tasks.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn process_consume_result_without_offset_store_does_not_panic() {
        let default_impl = new_default_impl();
        let service = new_service(Some(default_impl.clone()));
        let process_queue = Arc::new(ProcessQueue::new());
        let messages = vec![Arc::new(MessageExt::default())];
        process_queue.put_message(&messages).await;
        let msgs = process_queue.take_messages(1).await;
        let mut request = ConsumeRequest {
            process_queue,
            message_queue: message_queue(),
            default_mqpush_consumer_impl: Some(default_impl),
            consumer_group: consumer_group(),
        };
        let context = ConsumeOrderlyContext::new(message_queue());

        let continue_consume = service
            .process_consume_result(
                msgs,
                Arc::new(new_service(None)),
                ConsumeOrderlyStatus::Success,
                &context,
                &mut request,
            )
            .await;

        assert!(continue_consume);
    }
}
