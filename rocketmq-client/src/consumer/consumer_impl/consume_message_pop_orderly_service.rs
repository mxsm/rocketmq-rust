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
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use dashmap::DashSet;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_runtime::ScheduledTaskControl;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_rust::ArcMut;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::ack_result::AckResult;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use crate::consumer::listener::message_listener_orderly::ArcMessageListenerOrderly;
use crate::consumer::message_queue_lock::MessageQueueLock;
use crate::runtime::schedule_client_fixed_delay_controlled_task;
use crate::runtime::spawn_client_blocking_io;
use crate::runtime::spawn_client_task;
use crate::runtime::ClientScheduledTaskHandle;

pub struct ConsumeMessagePopOrderlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcMessageListenerOrderly,
    pub(crate) concurrency_limiter: Arc<Semaphore>,
    pub(crate) max_concurrency: Arc<AtomicUsize>,
    pub(self) consume_request_set: Arc<DashSet<ConsumeRequest>>,
    pub(crate) message_queue_lock: MessageQueueLock,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) active_tasks: Arc<AtomicUsize>,
    pub(crate) lock_refresh_task: Option<PopOrderlyTaskHandle>,
    pop_orderly_task_tracker: TaskTracker,
    force_stop_token: CancellationToken,
    submitted_tasks: Arc<AtomicU64>,
}

pub(crate) struct PopOrderlyTaskHandle(ClientScheduledTaskHandle);

impl PopOrderlyTaskHandle {
    async fn shutdown(self, timeout: Duration) -> bool {
        let report = self.0.shutdown(timeout).await;
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "pop orderly lock refresh task shutdown report is unhealthy"
            );
        }
        report.is_healthy()
    }

    fn abort(self) {
        let report = self.0.shutdown_now();
        if !report.is_healthy() {
            warn!(
                report = %report.to_json(),
                "pop orderly lock refresh task shutdown_now report is unhealthy"
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
pub struct PopOrderlyLockRefreshLifecycleProbe {
    pub task_count_before_self_stop: usize,
    pub task_count_after_self_stop: usize,
    pub task_count_after_shutdown: usize,
    pub scheduled_runs: u64,
    pub scheduled_skips: u64,
    pub scheduled_overlaps: u64,
    pub scheduled_failures: u64,
    pub shutdown_elapsed_us: u128,
    pub healthy: bool,
}

fn spawn_tracked_pop_orderly_task<F>(
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
        tokio::select! {
            biased;
            _ = force_stop_token.cancelled() => {}
            _ = task => {}
        }
    });

    if let Err(error) = spawn_client_task(thread_name, tracked_task) {
        warn!("Failed to spawn {} background task: {}", thread_name, error);
        warn!("Failed to track {} background task", thread_name);
    }
}

/// Default callback implementation for acknowledgment operations
struct DefaultAckCallback;

impl crate::consumer::ack_callback::AckCallback for DefaultAckCallback {
    fn on_success(&self, _ack_result: AckResult) {}

    fn on_exception(&self, e: rocketmq_error::RocketMQError) {
        error!("change_invisible_time callback exception: {}", e);
    }
}

#[allow(deprecated)]
fn record_orderly_process_event<E>(
    span: &tracing::Span,
    status: &Result<ConsumeOrderlyStatus, E>,
    message_count: usize,
) {
    match status {
        Ok(ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Commit) => {
            crate::consumer::consumer_impl::observability::record_process_event(
                span,
                "RocketMQ CONSUMER ACK",
                "success",
                message_count,
            );
        }
        Ok(ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::SuspendCurrentQueueAMoment) => {
            crate::consumer::consumer_impl::observability::record_process_event(
                span,
                "RocketMQ CONSUMER RETRY",
                "reconsume_later",
                message_count,
            );
        }
        Err(_) => {
            crate::consumer::consumer_impl::observability::record_process_event(
                span,
                "RocketMQ CONSUMER RETRY",
                "exception",
                message_count,
            );
        }
    }
}

impl ConsumeMessagePopOrderlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerOrderly,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let consume_thread = consumer_config.consume_thread_min;
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            concurrency_limiter: Arc::new(Semaphore::new(consume_thread as usize)),
            max_concurrency: Arc::new(AtomicUsize::new(consume_thread as usize)),
            consume_request_set: Arc::new(DashSet::new()),
            message_queue_lock: Default::default(),
            stopped: Arc::new(AtomicBool::new(false)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            lock_refresh_task: None,
            pop_orderly_task_tracker: TaskTracker::new(),
            force_stop_token: CancellationToken::new(),
            submitted_tasks: Arc::new(AtomicU64::new(0)),
        }
    }

    fn remove_consume_request(&mut self, request: &ConsumeRequest) {
        self.consume_request_set.remove(request);
    }

    pub async fn lock_mq_periodically(&mut self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }

        let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_mut() else {
            warn!(
                "lockMQPeriodically skipped: DefaultMQPushConsumerImpl is not initialized, group={}",
                self.consumer_group
            );
            return;
        };

        use crate::consumer::consumer_impl::re_balance::Rebalance;
        default_mqpush_consumer_impl
            .rebalance_impl
            .mut_from_ref()
            .lock_all()
            .await;
    }

    fn start_lock_refresh_with_schedule(&mut self, initial_delay: Duration, period: Duration) {
        let stopped = self.stopped.clone();
        let default_mqpush_consumer_impl = match &self.default_mqpush_consumer_impl {
            Some(impl_) => ArcMut::downgrade(impl_),
            None => return,
        };

        let handle = schedule_client_fixed_delay_controlled_task(
            "rocketmq-client-pop-orderly-lock-refresh",
            initial_delay,
            period,
            Duration::from_secs(5),
            move || {
                let stopped = stopped.clone();
                let default_mqpush_consumer_impl = default_mqpush_consumer_impl.clone();
                async move {
                    if stopped.load(Ordering::Acquire) {
                        return ScheduledTaskControl::Stop;
                    }

                    let Some(mut impl_) = default_mqpush_consumer_impl.upgrade() else {
                        return ScheduledTaskControl::Stop;
                    };

                    use crate::consumer::consumer_impl::re_balance::Rebalance;
                    impl_.rebalance_impl.lock_all().await;
                    ScheduledTaskControl::Continue
                }
            },
        )
        .map(PopOrderlyTaskHandle)
        .map_err(|error| {
            warn!("Failed to spawn rocketmq-client-pop-orderly-lock-refresh background task: {error}");
        })
        .ok();

        self.lock_refresh_task = handle;
    }

    fn lock_refresh_task_count(&self) -> usize {
        self.lock_refresh_task
            .as_ref()
            .map(PopOrderlyTaskHandle::task_count)
            .unwrap_or_default()
    }

    fn lock_refresh_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.lock_refresh_task
            .as_ref()
            .map(PopOrderlyTaskHandle::schedule_snapshot)
            .unwrap_or_default()
    }

    pub fn reset_namespace(&mut self, msgs: &mut [Arc<MessageExt>]) {
        let namespace = self.client_config.get_namespace().unwrap_or_default();
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

    fn submit_consume_request(&mut self, this: ArcMut<Self>, mut request: ConsumeRequest, force: bool) {
        if !force && !self.consume_request_set.insert(request.clone()) {
            return;
        }

        let stopped = self.stopped.clone();
        let active_tasks = self.active_tasks.clone();
        let concurrency_limiter = self.concurrency_limiter.clone();

        spawn_tracked_pop_orderly_task(
            "rocketmq-client-pop-orderly-consume",
            &self.pop_orderly_task_tracker,
            &self.force_stop_token,
            &self.submitted_tasks,
            async move {
                if stopped.load(Ordering::Acquire) {
                    return;
                }

                let _permit = match concurrency_limiter.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return,
                };

                active_tasks.fetch_add(1, Ordering::SeqCst);

                struct TaskGuard(Arc<AtomicUsize>);
                impl Drop for TaskGuard {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                let _guard = TaskGuard(active_tasks);

                request.run(this).await;
            },
        );
    }

    fn submit_consume_request_later(&self, this: ArcMut<Self>, request: ConsumeRequest, mut suspend_time_millis: u64) {
        suspend_time_millis = suspend_time_millis.clamp(10, 30_000);

        let stopped = self.stopped.clone();
        let consume_request_set = self.consume_request_set.clone();

        spawn_tracked_pop_orderly_task(
            "rocketmq-client-pop-orderly-consume-delay",
            &self.pop_orderly_task_tracker,
            &self.force_stop_token,
            &self.submitted_tasks,
            async move {
                tokio::time::sleep(Duration::from_millis(suspend_time_millis)).await;

                if stopped.load(Ordering::Acquire) {
                    return;
                }

                consume_request_set.insert(request.clone());
                this.mut_from_ref().submit_consume_request(this.clone(), request, true);
            },
        );
    }

    async fn unlock_all_message_queues(&self) {
        if let Some(ref impl_) = self.default_mqpush_consumer_impl {
            use crate::consumer::consumer_impl::re_balance::Rebalance;
            impl_.rebalance_impl.mut_from_ref().unlock_all(false).await;
        }
    }

    fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            i32::MAX
        } else {
            self.consumer_config.max_reconsume_times
        }
    }

    async fn check_reconsume_times(&self, msgs: &mut [Arc<MessageExt>]) -> bool {
        let mut suspend = false;
        let max_times = self.get_max_reconsume_times();

        for msg in msgs {
            let reconsume_times = msg.reconsume_times;
            if reconsume_times >= max_times {
                MessageAccessor::set_reconsume_time(
                    Arc::make_mut(msg),
                    CheetahString::from_string(reconsume_times.to_string()),
                );
                if !self.send_message_back(msg.as_ref()).await {
                    suspend = true;
                    Arc::make_mut(msg).reconsume_times = reconsume_times + 1;
                }
            } else {
                suspend = true;
                Arc::make_mut(msg).reconsume_times = reconsume_times + 1;
            }
        }

        suspend
    }

    async fn send_message_back(&self, msg: &MessageExt) -> bool {
        use rocketmq_common::common::message::message_single::Message;

        let retry_topic = format!("%RETRY%{}", self.consumer_group);

        let body = if let Some(body) = msg.body() {
            body.to_vec()
        } else {
            Vec::new()
        };

        let mut new_msg = match Message::builder().topic(&retry_topic).body(body).build() {
            Ok(message) => message,
            Err(error) => {
                error!(
                    "sendMessageBack skipped: failed to build retry message, group={} msg={:?}, error={}",
                    self.consumer_group, msg, error
                );
                return false;
            }
        };

        MessageAccessor::set_properties(&mut new_msg, msg.properties().clone());
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
            CheetahString::from_string(msg.reconsume_times.to_string()),
        );
        MessageAccessor::set_max_reconsume_times(
            &mut new_msg,
            CheetahString::from_string(self.get_max_reconsume_times().to_string()),
        );
        new_msg.set_delay_time_level(3 + msg.reconsume_times);

        if let Some(ref impl_) = self.default_mqpush_consumer_impl {
            if let Some(client_factory) = impl_.client_instance.as_ref() {
                if let Some(producer_impl) = client_factory.default_producer.default_mqproducer_impl.as_ref() {
                    match producer_impl.mut_from_ref().send(&mut new_msg).await {
                        Ok(_) => true,
                        Err(e) => {
                            error!("sendMessageBack failed: {:?}, msg: {:?}", e, msg);
                            false
                        }
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    async fn ack_message(&self, msg: &MessageExt) {
        if let Some(ref impl_) = self.default_mqpush_consumer_impl {
            impl_.mut_from_ref().ack_async(msg, &self.consumer_group).await;
        }
    }

    async fn change_invisible_time(&self, msg: &MessageExt, invisible_time: u64) {
        if let Some(ref impl_) = self.default_mqpush_consumer_impl {
            if let Some(extra_info) = msg.property(&CheetahString::from_static_str("POP_CK")) {
                let result = impl_
                    .mut_from_ref()
                    .change_pop_invisible_time_async(
                        msg.topic(),
                        &self.consumer_group,
                        &extra_info,
                        invisible_time,
                        DefaultAckCallback,
                    )
                    .await;

                if let Err(e) = result {
                    error!(
                        "change_invisible_time failed, msg: {:?}, time: {}, error: {}",
                        msg, invisible_time, e
                    );
                }
            } else {
                warn!("change_invisible_time: message missing POP_CK property");
            }
        }
    }

    async fn process_consume_result(
        &self,
        msgs: &[Arc<MessageExt>],
        status: Result<ConsumeOrderlyStatus, rocketmq_error::RocketMQError>,
        context: &ConsumeOrderlyContext,
    ) -> bool {
        let status = match status {
            Ok(s) => s,
            Err(e) => {
                error!("consume exception: {:?}", e);
                ConsumeOrderlyStatus::SuspendCurrentQueueAMoment
            }
        };

        match status {
            ConsumeOrderlyStatus::Success => {
                for msg in msgs {
                    self.ack_message(msg.as_ref()).await;
                }
                true
            }
            ConsumeOrderlyStatus::SuspendCurrentQueueAMoment => {
                let suspend_time = if context.get_suspend_current_queue_time_millis() > 0 {
                    context.get_suspend_current_queue_time_millis() as u64
                } else {
                    1000
                };
                for msg in msgs {
                    self.change_invisible_time(msg.as_ref(), suspend_time).await;
                }
                false
            }
            #[allow(deprecated)]
            ConsumeOrderlyStatus::Commit => {
                for msg in msgs {
                    self.ack_message(msg.as_ref()).await;
                }
                true
            }
            #[allow(deprecated)]
            ConsumeOrderlyStatus::Rollback => {
                warn!(
                    "Consumer group {} received deprecated Rollback status, reverting messages",
                    self.consumer_group
                );
                for msg in msgs {
                    self.change_invisible_time(msg.as_ref(), 1000).await;
                }
                false
            }
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopOrderlyService {
    fn start(&mut self, _this: ArcMut<Self>) {
        if self.consumer_config.message_model != MessageModel::Clustering {
            return;
        }

        self.start_lock_refresh_with_schedule(Duration::from_secs(1), Duration::from_millis(20_000));
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        info!(
            "{} ConsumeMessagePopOrderlyService shutdown started",
            self.consumer_group
        );

        self.stopped.store(true, Ordering::Release);

        self.concurrency_limiter.close();
        self.pop_orderly_task_tracker.close();

        if let Some(task) = self.lock_refresh_task.take() {
            let timeout = Duration::from_millis(await_terminate_millis);
            if !task.shutdown(timeout).await {
                warn!(
                    "{} pop orderly lock refresh task did not stop within {}ms; aborted",
                    self.consumer_group, await_terminate_millis
                );
            }
        }

        let timeout = Duration::from_millis(await_terminate_millis);
        let start_time = Instant::now();

        match tokio::time::timeout(timeout, self.pop_orderly_task_tracker.wait()).await {
            Ok(()) => {}
            Err(_) => {
                warn!(
                    "{} ConsumeMessagePopOrderlyService shutdown timeout, {} active consume tasks, {} submitted \
                     tasks; forcing remaining task futures to stop",
                    self.consumer_group,
                    self.active_tasks.load(Ordering::Acquire),
                    self.submitted_tasks.load(Ordering::Acquire)
                );
                self.force_stop_token.cancel();
                if tokio::time::timeout(Duration::from_secs(1), self.pop_orderly_task_tracker.wait())
                    .await
                    .is_err()
                {
                    warn!(
                        "{} ConsumeMessagePopOrderlyService force stop timed out, {} active consume tasks remain",
                        self.consumer_group,
                        self.active_tasks.load(Ordering::Acquire)
                    );
                }
            }
        }

        while self.active_tasks.load(Ordering::Acquire) > 0 {
            if start_time.elapsed() >= timeout {
                warn!(
                    "{} shutdown timeout, {} tasks still active",
                    self.consumer_group,
                    self.active_tasks.load(Ordering::Acquire)
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if self.consumer_config.message_model == MessageModel::Clustering {
            self.unlock_all_message_queues().await;
        }

        info!(
            "{} ConsumeMessagePopOrderlyService shutdown completed",
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
        if let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref() {
            default_mqpush_consumer_impl
                .mut_from_ref()
                .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());
        } else {
            warn!(
                "consumeMessageDirectly namespace reset skipped: DefaultMQPushConsumerImpl is not initialized, \
                 group={}",
                self.consumer_group
            );
        }

        let begin_timestamp = Instant::now();

        let listener = self.message_listener.clone();
        let msgs_cloned: Vec<MessageExt> = msgs.iter().map(|m| m.as_ref().clone()).collect();
        let group_for_span = self.consumer_group.clone();
        let blocking_result = spawn_client_blocking_io("client.pop_orderly.consume_direct", move || {
            let msg_refs: Vec<&MessageExt> = msgs_cloned.iter().collect();
            let mut ctx = ConsumeOrderlyContext::new(mq);
            let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
                msg_refs.first().copied(),
                msg_refs.len(),
                group_for_span.as_str(),
                ctx.get_message_queue(),
                "pop_orderly_direct",
            );
            let _entered = process_span.enter();
            let result = listener.consume_message(&msg_refs, &mut ctx);
            record_orderly_process_event(&process_span, &result, msg_refs.len());
            (result, ctx)
        })
        .await;

        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        rocketmq_observability::metrics::client::record_consume(msgs.len(), consume_rt);

        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(true);
        match blocking_result {
            Ok((status, context)) => {
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
            }
            Err(join_err) => {
                result.set_auto_commit(true);
                result.set_consume_result(CMResult::CRThrowException);
                result.set_remark(CheetahString::from_string(format!(
                    "consume_message panicked: {join_err}"
                )));
            }
        }
        result.set_spent_time_mills(consume_rt);
        info!("consumeMessageDirectly Result: {}", result);
        result
    }

    async fn submit_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<Arc<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        let _ = (this, msgs, process_queue, message_queue, dispatch_to_consume);
    }

    async fn submit_pop_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let arc_msgs: Vec<Arc<MessageExt>> = msgs.into_iter().map(Arc::new).collect();
        let request = ConsumeRequest::new(Arc::new(process_queue.clone()), message_queue.clone(), arc_msgs);
        this.mut_from_ref().submit_consume_request(this.clone(), request, false);
    }
}

#[derive(Clone)]
struct ConsumeRequest {
    process_queue: Arc<PopProcessQueue>,
    message_queue: MessageQueue,
    sharding_key_index: i32,
    msgs: Vec<Arc<MessageExt>>,
}

impl std::fmt::Debug for ConsumeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumeRequest")
            .field("message_queue", &self.message_queue)
            .field("sharding_key_index", &self.sharding_key_index)
            .finish()
    }
}

impl ConsumeRequest {
    pub fn new(process_queue: Arc<PopProcessQueue>, message_queue: MessageQueue, msgs: Vec<Arc<MessageExt>>) -> Self {
        Self {
            process_queue,
            message_queue,
            sharding_key_index: 0,
            msgs,
        }
    }

    pub async fn run(&mut self, mut consume_message_pop_orderly_service: ArcMut<ConsumeMessagePopOrderlyService>) {
        if self.process_queue.is_dropped() {
            warn!(
                "run, message queue not be able to consume, because it's dropped. {}",
                self.message_queue
            );
            consume_message_pop_orderly_service.remove_consume_request(self);
            return;
        }

        let lock = consume_message_pop_orderly_service
            .message_queue_lock
            .fetch_lock_object_with_sharding_key(&self.message_queue, self.sharding_key_index)
            .await;
        let _guard = lock.lock().await;

        let msgs = &mut self.msgs;

        if msgs.is_empty() {
            consume_message_pop_orderly_service.submit_consume_request_later(
                consume_message_pop_orderly_service.clone(),
                self.clone(),
                1000,
            );
            return;
        }

        let suspend = consume_message_pop_orderly_service.check_reconsume_times(msgs).await;
        if suspend {
            consume_message_pop_orderly_service.submit_consume_request_later(
                consume_message_pop_orderly_service.clone(),
                self.clone(),
                consume_message_pop_orderly_service
                    .consumer_config
                    .suspend_current_queue_time_millis,
            );
            return;
        }

        let begin_timestamp = Instant::now();
        let listener = consume_message_pop_orderly_service.message_listener.clone();
        let msgs_cloned: Vec<MessageExt> = msgs.iter().map(|m| m.as_ref().clone()).collect();
        let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
            msgs.first().map(|msg| msg.as_ref()),
            msgs.len(),
            consume_message_pop_orderly_service.consumer_group.as_str(),
            &self.message_queue,
            "pop_orderly",
        );
        let mq = self.message_queue.clone();
        let mq_for_fallback = self.message_queue.clone();
        let process_span_for_blocking = process_span.clone();
        let blocking_result = spawn_client_blocking_io("client.pop_orderly.consume", move || {
            let _entered = process_span_for_blocking.enter();
            let msg_refs: Vec<&MessageExt> = msgs_cloned.iter().collect();
            let mut ctx = ConsumeOrderlyContext::new(mq);
            let result = listener.consume_message(&msg_refs, &mut ctx);
            (result, ctx)
        })
        .await;
        let (status, context) = match blocking_result {
            Ok(pair) => pair,
            Err(e) => {
                error!(
                    "consume_message task panicked: {:?}, Group: {}, MQ: {}",
                    e, consume_message_pop_orderly_service.consumer_group, self.message_queue
                );
                (
                    Ok(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment),
                    ConsumeOrderlyContext::new(mq_for_fallback),
                )
            }
        };
        record_orderly_process_event(&process_span, &status, msgs.len());
        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        rocketmq_observability::metrics::client::record_consume(msgs.len(), consume_rt);

        let continue_consume = consume_message_pop_orderly_service
            .process_consume_result(msgs, status, &context)
            .await;

        if continue_consume {
            drop(_guard);
            consume_message_pop_orderly_service
                .mut_from_ref()
                .submit_consume_request(consume_message_pop_orderly_service.clone(), self.clone(), false);
        } else {
            let suspend_time = if context.get_suspend_current_queue_time_millis() > 0 {
                context.get_suspend_current_queue_time_millis() as u64
            } else {
                consume_message_pop_orderly_service
                    .consumer_config
                    .suspend_current_queue_time_millis
            };
            drop(_guard);
            consume_message_pop_orderly_service.submit_consume_request_later(
                consume_message_pop_orderly_service.clone(),
                self.clone(),
                suspend_time,
            );
        }
    }
}

impl PartialEq for ConsumeRequest {
    fn eq(&self, other: &Self) -> bool {
        if self.sharding_key_index != other.sharding_key_index {
            return false;
        }

        if !Arc::ptr_eq(&self.process_queue, &other.process_queue) {
            return false;
        }

        self.message_queue == other.message_queue
    }
}

impl Eq for ConsumeRequest {}

impl Hash for ConsumeRequest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sharding_key_index.hash(state);
        self.process_queue.as_ref().hash(state);
        self.message_queue.hash(state);
    }
}

#[doc(hidden)]
pub async fn run_pop_orderly_lock_refresh_lifecycle_probe() -> PopOrderlyLockRefreshLifecycleProbe {
    let default_impl = ArcMut::new(DefaultMQPushConsumerImpl::new(
        ClientConfig::default(),
        ArcMut::new(ConsumerConfig {
            message_model: MessageModel::Clustering,
            ..Default::default()
        }),
        None,
    ));
    let listener: ArcMessageListenerOrderly =
        Arc::new(|_msgs: &[&MessageExt], _context: &mut ConsumeOrderlyContext| Ok(ConsumeOrderlyStatus::Success));
    let mut service = ArcMut::new(ConsumeMessagePopOrderlyService::new(
        ArcMut::new(ClientConfig::default()),
        ArcMut::new(ConsumerConfig {
            message_model: MessageModel::Clustering,
            ..Default::default()
        }),
        CheetahString::from_static_str("pop_orderly_lock_refresh_probe_group"),
        listener,
        Some(default_impl),
    ));

    service.start_lock_refresh_with_schedule(Duration::ZERO, Duration::from_millis(1));

    let mut snapshots = service.lock_refresh_snapshot();
    for _ in 0..100 {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        snapshots = service.lock_refresh_snapshot();
    }

    let task_count_before_self_stop = service.lock_refresh_task_count();
    service.default_mqpush_consumer_impl = None;

    for _ in 0..100 {
        if service.lock_refresh_task_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    snapshots = service.lock_refresh_snapshot();
    let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
    let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
    let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
    let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();
    let task_count_after_self_stop = service.lock_refresh_task_count();
    let shutdown_started_at = Instant::now();
    service.shutdown(1_000).await;
    let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
    let task_count_after_shutdown = service.lock_refresh_task_count();
    let healthy = scheduled_runs >= 2
        && scheduled_overlaps == 0
        && scheduled_failures == 0
        && task_count_before_self_stop > 0
        && task_count_after_self_stop == 0
        && task_count_after_shutdown == 0;

    PopOrderlyLockRefreshLifecycleProbe {
        task_count_before_self_stop,
        task_count_after_self_stop,
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
    use super::*;
    use std::future::pending;
    use std::sync::Arc;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn listener() -> ArcMessageListenerOrderly {
        Arc::new(|_msgs: &[&MessageExt], _context: &mut ConsumeOrderlyContext| Ok(ConsumeOrderlyStatus::Success))
    }

    fn new_service(default_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>) -> ConsumeMessagePopOrderlyService {
        ConsumeMessagePopOrderlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(ConsumerConfig::default()),
            CheetahString::from_static_str("group"),
            listener(),
            default_impl,
        )
    }

    fn new_service_with_config(consumer_config: ConsumerConfig) -> ConsumeMessagePopOrderlyService {
        ConsumeMessagePopOrderlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(consumer_config),
            CheetahString::from_static_str("group"),
            listener(),
            None,
        )
    }

    fn new_default_impl() -> ArcMut<DefaultMQPushConsumerImpl> {
        let consumer_config = ArcMut::new(ConsumerConfig::default());
        ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            consumer_config,
            None,
        ))
    }

    fn message_queue() -> MessageQueue {
        MessageQueue::from_parts("topic", "broker-a", 0)
    }

    fn consume_request() -> ConsumeRequest {
        ConsumeRequest::new(Arc::new(PopProcessQueue::new()), message_queue(), vec![])
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
    async fn lock_mq_periodically_without_default_impl_does_not_panic() {
        let mut service = new_service(None);

        service.lock_mq_periodically().await;
    }

    #[tokio::test]
    async fn shutdown_closes_concurrency_limiter_like_java_executor_shutdown() {
        let mut service = new_service(None);

        service.shutdown(100).await;

        assert!(service.concurrency_limiter.is_closed());
    }

    #[tokio::test]
    async fn tracked_pop_orderly_task_shutdown_waits_for_completion() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();

        spawn_tracked_pop_orderly_task(
            "rocketmq-client-pop-orderly-tracker-test",
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
    async fn tracked_pop_orderly_task_force_stop_cancels_pending_task() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();

        spawn_tracked_pop_orderly_task(
            "rocketmq-client-pop-orderly-tracker-test",
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
    async fn pop_orderly_lock_refresh_lifecycle_probe_reports_self_stop() {
        let probe = run_pop_orderly_lock_refresh_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_after_self_stop, 0, "{probe:?}");
        assert_eq!(probe.task_count_after_shutdown, 0, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }

    #[test]
    fn start_without_tokio_runtime_does_not_spawn_panic() {
        let mut service = new_service(Some(new_default_impl()));
        let this = ArcMut::new(new_service(None));

        service.start(this);
        service.stopped.store(true, Ordering::Release);
        if let Some(handle) = service.lock_refresh_task.take() {
            handle.abort();
        }
    }

    #[test]
    fn submit_consume_request_without_tokio_runtime_does_not_spawn_panic() {
        let mut service = new_service(None);
        let this = ArcMut::new(new_service(None));
        service.stopped.store(true, Ordering::Release);

        ConsumeMessagePopOrderlyService::submit_consume_request(&mut service, this, consume_request(), true);
        std::thread::sleep(Duration::from_millis(20));
    }

    #[test]
    fn submit_consume_request_later_without_tokio_runtime_does_not_spawn_panic() {
        let service = ArcMut::new(new_service(None));
        let this = service.clone();

        service.submit_consume_request_later(this, consume_request(), 10);
        service.stopped.store(true, Ordering::Release);
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn reset_namespace_removes_configured_namespace_like_java() {
        let mut service = new_service(None);
        service
            .client_config
            .set_namespace(CheetahString::from_static_str("ns"));
        let mut msg = MessageExt::default();
        msg.set_topic(CheetahString::from_static_str("ns%topic-a"));
        let mut msgs = vec![Arc::new(msg)];

        service.reset_namespace(msgs.as_mut_slice());

        assert_eq!(msgs[0].topic(), "topic-a");
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
    fn test_consume_request_equality() {
        let pq1 = Arc::new(PopProcessQueue::new());
        let pq2 = Arc::new(PopProcessQueue::new());
        let mq1 = MessageQueue::from_parts(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("broker"),
            0,
        );
        let mq2 = MessageQueue::from_parts(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("broker"),
            1,
        );

        let r1 = ConsumeRequest::new(pq1.clone(), mq1.clone(), vec![]);
        let r2 = ConsumeRequest::new(pq1.clone(), mq1.clone(), vec![]);
        let r3 = ConsumeRequest::new(pq2.clone(), mq1.clone(), vec![]);
        let r4 = ConsumeRequest::new(pq1.clone(), mq2, vec![]);

        assert_eq!(r1, r2, "Same process_queue and message_queue should be equal");
        assert_ne!(r1, r3, "Different process_queue should not be equal");
        assert_ne!(r1, r4, "Different message_queue should not be equal");
    }

    #[test]
    fn test_suspend_time_clamping() {
        assert!(5u64.clamp(10, 30_000) == 10, "Value below min should clamp to min");
        assert!(
            50_000u64.clamp(10, 30_000) == 30_000,
            "Value above max should clamp to max"
        );
        assert!(
            1000u64.clamp(10, 30_000) == 1000,
            "Value in range should stay unchanged"
        );
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_consume_request_hash_consistency() {
        use std::collections::HashSet;

        let pq1 = Arc::new(PopProcessQueue::new());
        let mq = MessageQueue::from_parts(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("broker"),
            0,
        );

        let r1 = ConsumeRequest::new(pq1.clone(), mq.clone(), vec![]);
        let r2 = ConsumeRequest::new(pq1.clone(), mq.clone(), vec![]);

        let mut set = HashSet::new();
        assert!(set.insert(r1.clone()));
        assert!(!set.insert(r2), "Equal items should hash to same value");
        assert!(set.contains(&r1));
    }

    #[test]
    fn test_max_reconsume_times_default() {
        let consumer_config = ConsumerConfig::default();
        let max_times = if consumer_config.max_reconsume_times == -1 {
            i32::MAX
        } else {
            consumer_config.max_reconsume_times
        };
        assert!(max_times > 0, "Max reconsume times should be positive");
    }

    #[test]
    fn test_partial_eq_logic() {
        let pq1 = Arc::new(PopProcessQueue::new());
        let pq2 = Arc::new(PopProcessQueue::new());
        let mq = MessageQueue::from_parts(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("broker"),
            0,
        );

        let r1 = ConsumeRequest::new(pq1.clone(), mq.clone(), vec![]);
        let r2 = ConsumeRequest::new(pq2, mq, vec![]);

        assert_ne!(
            r1, r2,
            "Different process queues with same message queue should not be equal"
        );
    }
}
