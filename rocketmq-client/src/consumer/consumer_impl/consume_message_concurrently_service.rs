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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::consume_return_type::ConsumeReturnType;
use crate::consumer::listener::message_listener_concurrently::ArcMessageListenerConcurrently;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::runtime::spawn_client_blocking_io;
use crate::runtime::spawn_client_task;
use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientTrackedTaskHandle;

pub struct ConsumeMessageConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: RwLock<Option<Weak<DefaultMQPushConsumerImpl>>>,
    pub(crate) client_config: Arc<ClientConfig>,
    pub(crate) consumer_config: Arc<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcMessageListenerConcurrently,
    /// Semaphore used for two purposes:
    ///   1. Backpressure: limits the number of concurrently executing consume tasks to the
    ///      Java-equivalent core pool size. New tasks that cannot acquire a permit are retried
    ///      after 5 s.
    ///   2. Permit ownership: each tracked consume task holds a permit until its future exits.
    consume_semaphore: Arc<Semaphore>,
    max_concurrency: Arc<AtomicUsize>,
    /// Token cancelled by `shutdown()` to prevent new task submissions.
    shutdown_token: CancellationToken,
    /// Background task matching Java's cleanExpireMsgExecutors lifecycle.
    clean_expire_task_handle: Arc<Mutex<Option<ConcurrentTaskHandle>>>,
    consume_task_tracker: TaskTracker,
    force_stop_token: CancellationToken,
    submitted_tasks: Arc<AtomicU64>,
}

enum ConcurrentTaskHandle {
    Tracked(ClientTrackedTaskHandle),
}

impl ConcurrentTaskHandle {
    async fn shutdown(self, timeout: Duration) -> bool {
        match self {
            Self::Tracked(handle) => {
                let report = handle.shutdown(timeout).await;
                if !report.is_healthy() {
                    warn!(
                        report = %report.to_json(),
                        "concurrent clean-expire task shutdown report is unhealthy"
                    );
                }
                report.is_healthy()
            }
        }
    }

    fn abort(self) {
        match self {
            Self::Tracked(handle) => {
                let report = handle.shutdown_now();
                if !report.is_healthy() {
                    warn!(
                        report = %report.to_json(),
                        "concurrent clean-expire task immediate shutdown report is unhealthy"
                    );
                }
            }
        }
    }

    fn task_count(&self) -> usize {
        match self {
            Self::Tracked(handle) => handle.task_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConcurrentCleanExpireLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
}

fn spawn_concurrent_lifecycle_task<F>(thread_name: &'static str, task: F) -> Option<ConcurrentTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    match spawn_client_tracked_task(thread_name, task) {
        Ok(handle) => Some(ConcurrentTaskHandle::Tracked(handle)),
        Err(error) => {
            warn!("Failed to spawn {} background task: {}", thread_name, error);
            None
        }
    }
}

fn spawn_tracked_concurrent_task<F>(
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

impl ConsumeMessageConcurrentlyService {
    pub fn new(
        client_config: Arc<ClientConfig>,
        consumer_config: Arc<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<Weak<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let core_pool_size = consumer_config.consume_thread_min as usize;
        Self {
            default_mqpush_consumer_impl: RwLock::new(default_mqpush_consumer_impl),
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            consume_semaphore: Arc::new(Semaphore::new(core_pool_size)),
            max_concurrency: Arc::new(AtomicUsize::new(core_pool_size)),
            shutdown_token: CancellationToken::new(),
            clean_expire_task_handle: Arc::new(Mutex::new(None)),
            consume_task_tracker: TaskTracker::new(),
            force_stop_token: CancellationToken::new(),
            submitted_tasks: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown_token.is_cancelled()
    }

    fn clean_expire_task_count(&self) -> usize {
        self.clean_expire_task_handle
            .lock()
            .as_ref()
            .map(ConcurrentTaskHandle::task_count)
            .unwrap_or_default()
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
}

impl ConsumeMessageConcurrentlyService {
    fn consumer_impl(&self) -> Option<Arc<DefaultMQPushConsumerImpl>> {
        self.default_mqpush_consumer_impl
            .read()
            .as_ref()
            .and_then(Weak::upgrade)
    }

    async fn clean_expire_msg(&self) {
        let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
            warn!(
                "clean expired messages skipped: DefaultMQPushConsumerImpl is not initialized, group={}",
                self.consumer_group
            );
            return;
        };

        let process_queue_table = default_mqpush_consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await;
        for process_queue in process_queue_table.values() {
            process_queue
                .clean_expired_msg(Some(default_mqpush_consumer_impl.clone()))
                .await;
        }
    }

    async fn process_consume_result(
        &self,
        this: Arc<Self>,
        status: ConsumeConcurrentlyStatus,
        context: &ConsumeConcurrentlyContext,
        consume_request: &mut ConsumeRequest,
    ) {
        if consume_request.msgs.is_empty() {
            return;
        }
        let ack_index = normalize_ack_index(status, context.ack_index, consume_request.msgs.len());

        // Update per-topic/group consume throughput counters, split by ack index.
        if let Some(impl_) = self.consumer_impl() {
            if let Some(client_instance) = impl_.get_mq_client_factory() {
                let mgr = client_instance.consumer_stats_manager();
                let topic = consume_request.message_queue.topic().as_str();
                let group = self.consumer_group.as_str();
                let total = consume_request.msgs.len() as u64;
                match status {
                    ConsumeConcurrentlyStatus::ConsumeSuccess => {
                        let ok = (ack_index + 1).max(0) as u64;
                        mgr.inc_consume_ok_tps(group, topic, ok);
                        if total > ok {
                            mgr.inc_consume_failed_tps(group, topic, total - ok);
                        }
                    }
                    ConsumeConcurrentlyStatus::ReconsumeLater => {
                        mgr.inc_consume_failed_tps(group, topic, total);
                    }
                }
            }
        }

        match self.consumer_config.message_model {
            MessageModel::Broadcasting => {
                for i in ((ack_index + 1) as usize)..consume_request.msgs.len() {
                    let msg = consume_request.msgs[i].as_ref();
                    warn!(
                        "BROADCASTING, the message consume failed, drop it, topic={}, msgId={}, queueOffset={}",
                        msg.topic(),
                        msg.msg_id(),
                        msg.queue_offset()
                    );
                }
            }
            MessageModel::Clustering => {
                let mut msg_back_failed = Vec::new();
                let pending = consume_request.msgs.split_off((ack_index + 1) as usize);
                for mut msg in pending {
                    if !consume_request.process_queue.contains_message(&msg).await {
                        info!(
                            "Message is not found in its process queue; skip send-back-procedure, topic={}, \
                             brokerName={}, queueId={}, queueOffset={}",
                            msg.topic(),
                            msg.broker_name(),
                            msg.queue_id(),
                            msg.queue_offset()
                        );
                        consume_request.msgs.push(msg);
                        continue;
                    }

                    let sent = self.send_message_back(Arc::make_mut(&mut msg), context).await;
                    if sent {
                        consume_request.msgs.push(msg);
                    } else {
                        let times = msg.reconsume_times() + 1;
                        Arc::make_mut(&mut msg).set_reconsume_times(times);
                        msg_back_failed.push(msg);
                    }
                }
                if !msg_back_failed.is_empty() {
                    self.submit_consume_request_later(
                        msg_back_failed,
                        this,
                        consume_request.process_queue.clone(),
                        consume_request.message_queue.clone(),
                    );
                }
            }
        }
        let offset = consume_request
            .process_queue
            .remove_message(&consume_request.msgs)
            .await;
        if offset >= 0 && !consume_request.process_queue.is_dropped() {
            let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
                warn!(
                    "consume offset update skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}",
                    self.consumer_group, consume_request.message_queue
                );
                return;
            };
            let Some(offset_store) = default_mqpush_consumer_impl.offset_store() else {
                warn!(
                    "consume offset update skipped: OffsetStore is not initialized, group={}, mq={}",
                    self.consumer_group, consume_request.message_queue
                );
                return;
            };
            offset_store
                .update_offset(&consume_request.message_queue, offset, true)
                .await;
        }
    }

    fn submit_consume_request_later(
        &self,
        msgs: Vec<Arc<MessageExt>>,
        this: Arc<Self>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
    ) {
        let shutdown_token = self.shutdown_token.clone();
        spawn_tracked_concurrent_task(
            "rocketmq-client-concurrent-consume-delay",
            &self.consume_task_tracker,
            &self.force_stop_token,
            &self.submitted_tasks,
            async move {
                tokio::select! {
                    _ = shutdown_token.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
                this.submit_consume_request(this.clone(), msgs, process_queue, message_queue, true)
                    .await;
            },
        );
    }

    pub async fn send_message_back(&self, msg: &mut MessageExt, context: &ConsumeConcurrentlyContext) -> bool {
        let delay_level = context.delay_level_when_next_consume;
        let mut client_config = (*self.client_config).clone();
        msg.set_topic(client_config.with_namespace(msg.topic().as_str()));

        let Some(default_mqpush_consumer_impl) = self.consumer_impl() else {
            error!(
                "sendMessageBack skipped: DefaultMQPushConsumerImpl is not initialized, group: {} msg: {}",
                self.consumer_group, msg
            );
            return false;
        };
        default_mqpush_consumer_impl
            .send_message_back(
                msg,
                delay_level,
                &client_config.queue_with_namespace(context.get_message_queue().clone()),
            )
            .await
            .is_ok()
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessageConcurrentlyService {
    fn start(&self, this: Arc<Self>) {
        let shutdown_token = self.shutdown_token.clone();
        let handle = spawn_concurrent_lifecycle_task("rocketmq-client-concurrent-clean-expire", async move {
            let timeout = this.consumer_config.consume_timeout;
            let interval = Duration::from_secs(timeout.saturating_mul(60));
            loop {
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
                if shutdown_token.is_cancelled() {
                    break;
                }
                this.clean_expire_msg().await;
            }
        });
        *self.clean_expire_task_handle.lock() = handle;
    }

    async fn shutdown(&self, await_terminate_millis: u64) {
        self.shutdown_token.cancel();
        self.consume_task_tracker.close();
        let shutdown_timeout = Duration::from_millis(await_terminate_millis);
        let clean_expire_handle = { self.clean_expire_task_handle.lock().take() };
        if let Some(handle) = clean_expire_handle {
            if !handle.shutdown(shutdown_timeout).await {
                warn!(
                    "ConsumeMessageConcurrentlyService clean-expire task did not stop within {}ms; aborted",
                    await_terminate_millis
                );
            }
        }

        match tokio::time::timeout(shutdown_timeout, self.consume_task_tracker.wait()).await {
            Ok(()) => {
                info!("ConsumeMessageConcurrentlyService shutdown gracefully");
            }
            Err(_elapsed) => {
                warn!(
                    "ConsumeMessageConcurrentlyService shutdown timed out after {}ms; {} submitted consume tasks may \
                     still be running",
                    await_terminate_millis,
                    self.submitted_tasks.load(Ordering::Acquire)
                );
                self.force_stop_token.cancel();
                if tokio::time::timeout(Duration::from_secs(1), self.consume_task_tracker.wait())
                    .await
                    .is_err()
                {
                    warn!("ConsumeMessageConcurrentlyService force stop timed out");
                }
            }
        }
    }

    fn update_core_pool_size(&self, core_pool_size: usize) {
        if core_pool_size > 0
            && core_pool_size <= i16::MAX as usize
            && core_pool_size < self.consumer_config.consume_thread_max as usize
        {
            let old_size = self.max_concurrency.swap(core_pool_size, Ordering::AcqRel);
            Self::resize_available_permits(&self.consume_semaphore, old_size, core_pool_size);
        }
    }

    fn get_core_pool_size(&self) -> usize {
        self.max_concurrency.load(Ordering::Acquire)
    }

    async fn consume_message_directly(
        &self,
        mut msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        info!("consumeMessageDirectly receive new message: {}", msg);
        msg.broker_name = broker_name.unwrap_or_default();
        let mq = MessageQueue::from_parts(msg.topic().clone(), msg.broker_name.clone(), msg.queue_id());
        let mut msgs = vec![Arc::new(msg)];
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

        let listener = self.message_listener.clone();
        let group_for_span = self.consumer_group.clone();
        let status = spawn_client_blocking_io("client.concurrent.consume_direct", move || {
            let context = ConsumeConcurrentlyContext::new(mq);
            let msgs_refs: Vec<&MessageExt> = msgs.iter().map(|m| m.as_ref()).collect();
            let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
                msgs_refs.first().copied(),
                msgs_refs.len(),
                group_for_span.as_str(),
                &context.message_queue,
                "concurrent_direct",
            );
            let _entered = process_span.enter();
            let result = listener.consume_message(&msgs_refs, &context);
            match &result {
                Ok(ConsumeConcurrentlyStatus::ConsumeSuccess) => {
                    crate::consumer::consumer_impl::observability::record_process_event(
                        &process_span,
                        "RocketMQ CONSUMER ACK",
                        "success",
                        msgs_refs.len(),
                    );
                }
                Ok(ConsumeConcurrentlyStatus::ReconsumeLater) => {
                    crate::consumer::consumer_impl::observability::record_process_event(
                        &process_span,
                        "RocketMQ CONSUMER RETRY",
                        "reconsume_later",
                        msgs_refs.len(),
                    );
                }
                Err(_) => {
                    crate::consumer::consumer_impl::observability::record_process_event(
                        &process_span,
                        "RocketMQ CONSUMER RETRY",
                        "exception",
                        msgs_refs.len(),
                    );
                }
            }
            result
        })
        .await
        .unwrap_or_else(|join_err| {
            error!("consume_message_directly task panicked: {:?}", join_err);
            Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "consume_message_directly task panicked: {join_err:?}"
            )))
        });

        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        rocketmq_observability::metrics::client::record_consume(1, consume_rt);

        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(false);
        result.set_auto_commit(true);
        match status {
            Ok(status) => match status {
                ConsumeConcurrentlyStatus::ConsumeSuccess => {
                    result.set_consume_result(CMResult::CRSuccess);
                }
                ConsumeConcurrentlyStatus::ReconsumeLater => {
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
        if self.shutdown_token.is_cancelled() {
            warn!(
                "ConsumeMessageConcurrentlyService is shutting down; dropping {} message(s) for queue {}",
                msgs.len(),
                message_queue
            );
            return;
        }

        let consume_batch_size = self.consumer_config.consume_message_batch_max_size;
        if msgs.len() <= consume_batch_size as usize {
            self.spawn_consume_task(this, msgs, process_queue, message_queue, dispatch_to_consume);
        } else {
            msgs.chunks(consume_batch_size as usize)
                .map(|chunk| chunk.to_vec())
                .for_each(|chunk| {
                    self.spawn_consume_task(
                        this.clone(),
                        chunk,
                        process_queue.clone(),
                        message_queue.clone(),
                        dispatch_to_consume,
                    );
                });
        }
    }

    async fn submit_pop_consume_request(
        &self,
        this: Arc<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let _ = (this, msgs, process_queue, message_queue);
    }
}

impl ConsumeMessageConcurrentlyService {
    /// Attempts to acquire a semaphore permit and, if successful, spawns a consume task on the
    /// current Tokio runtime.  When all `consume_thread_max` permits are in use the request is
    /// retried after 5 s, matching the Java SDK's `RejectedExecutionException` path.
    fn spawn_consume_task(
        &self,
        this: Arc<Self>,
        msgs: Vec<Arc<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        if self.shutdown_token.is_cancelled() {
            return;
        }

        match Arc::clone(&self.consume_semaphore).try_acquire_owned() {
            Ok(permit) => {
                let mut consume_request = ConsumeRequest {
                    msgs,
                    message_listener: self.message_listener.clone(),
                    process_queue,
                    message_queue,
                    dispatch_to_consume,
                    consumer_group: self.consumer_group.clone(),
                    default_mqpush_consumer_impl: self.consumer_impl(),
                };
                spawn_tracked_concurrent_task(
                    "rocketmq-client-concurrent-consume",
                    &self.consume_task_tracker,
                    &self.force_stop_token,
                    &self.submitted_tasks,
                    async move {
                        let _permit = permit;
                        consume_request.run(this).await;
                    },
                );
            }
            Err(_saturated) => {
                warn!(
                    "consume semaphore saturated for group {}; will retry {} message(s) in 5 s",
                    self.consumer_group,
                    msgs.len()
                );
                self.submit_consume_request_later(msgs, this, process_queue, message_queue);
            }
        }
    }
}

struct ConsumeRequest {
    msgs: Vec<Arc<MessageExt>>,
    message_listener: ArcMessageListenerConcurrently,
    process_queue: Arc<ProcessQueue>,
    message_queue: MessageQueue,
    dispatch_to_consume: bool,
    consumer_group: CheetahString,
    default_mqpush_consumer_impl: Option<Arc<DefaultMQPushConsumerImpl>>,
}

impl ConsumeRequest {
    async fn run(&mut self, consume_message_concurrently_service: Arc<ConsumeMessageConcurrentlyService>) {
        if self.process_queue.is_dropped() {
            info!(
                "the message queue not be able to consume, because it's dropped. group={} {}",
                self.consumer_group, self.message_queue,
            );
            return;
        }
        let mut context = ConsumeConcurrentlyContext {
            message_queue: self.message_queue.clone(),
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        };

        let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref().cloned() else {
            warn!(
                "consume request skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}, msgs={}",
                self.consumer_group,
                self.message_queue,
                self.msgs.len()
            );
            return;
        };
        DefaultMQPushConsumerImpl::try_reset_pop_retry_topic(&mut self.msgs, self.consumer_group.as_str());
        default_mqpush_consumer_impl.reset_retry_and_namespace(&mut self.msgs, self.consumer_group.as_str());

        let mut consume_message_context = None;

        let has_hook = default_mqpush_consumer_impl.has_hook();

        let begin_timestamp = Instant::now();
        let mut has_exception = false;
        let mut status = None;
        let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
            self.msgs.first().map(|msg| msg.as_ref()),
            self.msgs.len(),
            self.consumer_group.as_str(),
            &self.message_queue,
            "concurrent",
        );

        if !self.msgs.is_empty() {
            let start_timestamp = current_millis();
            self.process_queue
                .mark_messages_consuming(&self.msgs, start_timestamp)
                .await;
            let start_ts = CheetahString::from_string(start_timestamp.to_string());
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(Arc::make_mut(msg), start_ts.clone());
            }

            if has_hook {
                consume_message_context = Some(
                    ConsumeMessageContext::new(self.consumer_group.clone(), &self.msgs)
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

            let listener = self.message_listener.clone();
            let msgs_for_blocking = self.msgs.clone();
            let group_for_err = self.consumer_group.clone();
            let process_span_for_blocking = process_span.clone();

            let (blocking_status, blocking_has_exception, returned_context) =
                spawn_client_blocking_io("client.concurrent.consume", move || {
                    let _entered = process_span_for_blocking.enter();
                    let msgs_refs: Vec<&MessageExt> = msgs_for_blocking.iter().map(|m| m.as_ref()).collect();
                    match listener.consume_message(&msgs_refs, &context) {
                        Ok(s) => (Some(s), false, context),
                        Err(e) => {
                            error!(
                                "consumeMessage exception: {:?}, Group: {}, Msgs: {}, MQ: {}",
                                e,
                                group_for_err,
                                msgs_refs.len(),
                                context.message_queue
                            );
                            (None, true, context)
                        }
                    }
                })
                .await
                .unwrap_or_else(|join_err| {
                    error!(
                        "consume_message task panicked: {:?}, Group: {}, MQ: {}",
                        join_err, self.consumer_group, self.message_queue
                    );
                    let fallback = ConsumeConcurrentlyContext {
                        message_queue: self.message_queue.clone(),
                        delay_level_when_next_consume: 0,
                        ack_index: i32::MAX,
                    };
                    (None, true, fallback)
                });
            context = returned_context;
            status = blocking_status;
            has_exception = blocking_has_exception;
        }

        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        rocketmq_observability::metrics::client::record_consume(self.msgs.len(), consume_rt);

        let return_type = classify_concurrent_consume_return_type(
            status,
            has_exception,
            consume_rt,
            default_mqpush_consumer_impl.consumer_config_snapshot().consume_timeout,
        );

        if has_hook {
            if let Some(context) = consume_message_context.as_mut() {
                context.props.insert(
                    CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE),
                    return_type.to_string().into(),
                );
            } else {
                warn!(
                    "consume hook context missing before return type update, group={}, mq={}",
                    self.consumer_group, self.message_queue
                );
            }
        }

        let final_status = if let Some(status) = status {
            status
        } else {
            warn!(
                "consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                self.consumer_group,
                self.msgs.len(),
                self.message_queue
            );
            ConsumeConcurrentlyStatus::ReconsumeLater
        };
        match final_status {
            ConsumeConcurrentlyStatus::ConsumeSuccess => {
                crate::consumer::consumer_impl::observability::record_process_event(
                    &process_span,
                    "RocketMQ CONSUMER ACK",
                    "success",
                    self.msgs.len(),
                );
            }
            ConsumeConcurrentlyStatus::ReconsumeLater => {
                crate::consumer::consumer_impl::observability::record_process_event(
                    &process_span,
                    "RocketMQ CONSUMER RETRY",
                    if has_exception { "exception" } else { "reconsume_later" },
                    self.msgs.len(),
                );
            }
        }

        if has_hook {
            if let Some(cmc) = consume_message_context.as_mut() {
                cmc.status = final_status.to_string().into();
                cmc.success = final_status == ConsumeConcurrentlyStatus::ConsumeSuccess;
                cmc.access_channel = Some(default_mqpush_consumer_impl.client_config_snapshot().access_channel);
                default_mqpush_consumer_impl.execute_hook_after(cmc);
            } else {
                warn!(
                    "consume hook context missing before after-hook execution, group={}, mq={}",
                    self.consumer_group, self.message_queue
                );
            }
        }

        // Record message consume round-trip time.
        if let Some(client_instance) = default_mqpush_consumer_impl.get_mq_client_factory() {
            client_instance.consumer_stats_manager().inc_consume_rt(
                self.consumer_group.as_str(),
                self.message_queue.topic().as_str(),
                consume_rt,
            );
        }

        if self.process_queue.is_dropped() {
            warn!(
                "processQueue is dropped without process consume result. messageQueue={}, msgs={}",
                self.message_queue,
                self.msgs.len()
            );
        } else {
            let this = consume_message_concurrently_service.clone();
            consume_message_concurrently_service
                .process_consume_result(this, final_status, &context, self)
                .await;
        }
    }
}

fn normalize_ack_index(status: ConsumeConcurrentlyStatus, ack_index: i32, message_count: usize) -> i32 {
    if message_count == 0 {
        return -1;
    }
    match status {
        ConsumeConcurrentlyStatus::ConsumeSuccess => ack_index.clamp(-1, message_count as i32 - 1),
        ConsumeConcurrentlyStatus::ReconsumeLater => -1,
    }
}

fn classify_concurrent_consume_return_type(
    status: Option<ConsumeConcurrentlyStatus>,
    has_exception: bool,
    consume_rt: u64,
    consume_timeout_minutes: u64,
) -> ConsumeReturnType {
    match status {
        None => {
            if has_exception {
                ConsumeReturnType::Exception
            } else {
                ConsumeReturnType::ReturnNull
            }
        }
        Some(ConsumeConcurrentlyStatus::ReconsumeLater) => {
            if consume_rt >= consume_timeout_minutes.saturating_mul(60).saturating_mul(1000) {
                ConsumeReturnType::TimeOut
            } else {
                ConsumeReturnType::Failed
            }
        }
        Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => {
            if consume_rt >= consume_timeout_minutes.saturating_mul(60).saturating_mul(1000) {
                ConsumeReturnType::TimeOut
            } else {
                ConsumeReturnType::Success
            }
        }
    }
}

#[doc(hidden)]
pub async fn run_concurrent_clean_expire_lifecycle_probe() -> ConcurrentCleanExpireLifecycleProbe {
    let listener: ArcMessageListenerConcurrently =
        Arc::new(|_msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        });
    let service = ConsumeMessageConcurrentlyService::new(
        Arc::new(ClientConfig::default()),
        Arc::new(ConsumerConfig::default()),
        CheetahString::from_static_str("concurrent-clean-expire-probe"),
        listener.clone(),
        None,
    );
    let this = Arc::new(ConsumeMessageConcurrentlyService::new(
        Arc::new(ClientConfig::default()),
        Arc::new(ConsumerConfig::default()),
        CheetahString::from_static_str("concurrent-clean-expire-probe"),
        listener,
        None,
    ));

    service.start(this);
    let task_count_before_shutdown = service.clean_expire_task_count();

    let shutdown_started = Instant::now();
    service.shutdown(1_000).await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = service.clean_expire_task_count();

    ConcurrentCleanExpireLifecycleProbe {
        healthy: task_count_before_shutdown == 1 && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::AtomicBool;

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

    fn listener() -> ArcMessageListenerConcurrently {
        Arc::new(|_msgs: &[&MessageExt], _context: &ConsumeConcurrentlyContext| {
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        })
    }

    fn consumer_group() -> CheetahString {
        CheetahString::from_static_str("group")
    }

    fn new_service(default_impl: Option<Arc<DefaultMQPushConsumerImpl>>) -> ConsumeMessageConcurrentlyService {
        ConsumeMessageConcurrentlyService::new(
            Arc::new(ClientConfig::default()),
            Arc::new(ConsumerConfig::default()),
            consumer_group(),
            listener(),
            default_impl.as_ref().map(Arc::downgrade),
        )
    }

    fn new_service_with_config(consumer_config: ConsumerConfig) -> ConsumeMessageConcurrentlyService {
        ConsumeMessageConcurrentlyService::new(
            Arc::new(ClientConfig::default()),
            Arc::new(consumer_config),
            consumer_group(),
            listener(),
            None,
        )
    }

    #[test]
    fn service_keeps_shared_startup_config_generation() {
        let client_config = Arc::new(ClientConfig::default());
        let consumer_config = Arc::new(ConsumerConfig::default());
        let service = ConsumeMessageConcurrentlyService::new(
            client_config.clone(),
            consumer_config.clone(),
            consumer_group(),
            listener(),
            None,
        );

        assert!(Arc::ptr_eq(&client_config, &service.client_config));
        assert!(Arc::ptr_eq(&consumer_config, &service.consumer_config));
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

    fn consume_request(default_impl: Option<Arc<DefaultMQPushConsumerImpl>>) -> ConsumeRequest {
        ConsumeRequest {
            msgs: vec![Arc::new(MessageExt::default())],
            message_listener: listener(),
            process_queue: Arc::new(ProcessQueue::new()),
            message_queue: message_queue(),
            dispatch_to_consume: true,
            consumer_group: consumer_group(),
            default_mqpush_consumer_impl: default_impl,
        }
    }

    #[tokio::test]
    async fn send_message_back_without_default_impl_returns_false() {
        let service = new_service(None);
        let mut msg = MessageExt::default();
        let context = ConsumeConcurrentlyContext::new(message_queue());

        assert!(!service.send_message_back(&mut msg, &context).await);
    }

    #[test]
    fn normalize_ack_index_clamps_public_listener_values() {
        assert_eq!(
            normalize_ack_index(ConsumeConcurrentlyStatus::ConsumeSuccess, -3, 2),
            -1
        );
        assert_eq!(normalize_ack_index(ConsumeConcurrentlyStatus::ConsumeSuccess, 99, 2), 1);
        assert_eq!(
            normalize_ack_index(ConsumeConcurrentlyStatus::ReconsumeLater, 99, 2),
            -1
        );
        assert_eq!(normalize_ack_index(ConsumeConcurrentlyStatus::ConsumeSuccess, 0, 0), -1);
    }

    #[test]
    fn concurrent_consume_return_type_timeout_matches_java_boundary() {
        assert_eq!(
            classify_concurrent_consume_return_type(Some(ConsumeConcurrentlyStatus::ConsumeSuccess), false, 59_999, 1),
            ConsumeReturnType::Success
        );
        assert_eq!(
            classify_concurrent_consume_return_type(Some(ConsumeConcurrentlyStatus::ConsumeSuccess), false, 60_000, 1),
            ConsumeReturnType::TimeOut
        );
        assert_eq!(
            classify_concurrent_consume_return_type(Some(ConsumeConcurrentlyStatus::ReconsumeLater), false, 59_999, 1),
            ConsumeReturnType::Failed
        );
    }

    #[test]
    fn concurrent_consume_return_type_preserves_null_and_exception_classification() {
        assert_eq!(
            classify_concurrent_consume_return_type(None, true, 60_000, 1),
            ConsumeReturnType::Exception
        );
        assert_eq!(
            classify_concurrent_consume_return_type(None, false, 60_000, 1),
            ConsumeReturnType::ReturnNull
        );
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

    #[tokio::test]
    async fn consume_message_directly_without_default_impl_does_not_panic() {
        let service = new_service(None);

        let result = service
            .consume_message_directly(MessageExt::default(), Some(CheetahString::from_static_str("broker-a")))
            .await;

        assert!(matches!(result.consume_result(), Some(CMResult::CRSuccess)));
    }

    #[test]
    fn default_consumer_self_reference_can_be_refreshed_after_service_creation() {
        let service = new_service(None);
        assert!(service.default_mqpush_consumer_impl.read().is_none());

        *service.default_mqpush_consumer_impl.write() = Some(Arc::downgrade(&new_default_impl()));

        assert!(service.default_mqpush_consumer_impl.read().is_some());
    }

    #[tokio::test]
    async fn consume_request_without_default_impl_is_ignored_without_panic() {
        let service = Arc::new(new_service(None));
        let mut request = consume_request(None);

        request.run(service).await;
    }

    #[tokio::test]
    async fn shutdown_aborts_clean_expire_task_like_java_executor_shutdown() {
        let service = new_service(None);

        service.start(Arc::new(new_service(None)));

        assert!(service.clean_expire_task_handle.lock().is_some());

        service.shutdown(100).await;

        assert!(service.clean_expire_task_handle.lock().is_none());
    }

    #[tokio::test]
    async fn concurrent_task_shutdown_waits_for_worker_completion() {
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();
        let handle = spawn_concurrent_lifecycle_task("rocketmq-client-concurrent-task-test", async move {
            completed_in_task.store(true, Ordering::Release);
        })
        .expect("test task should spawn");

        assert!(handle.shutdown(Duration::from_secs(1)).await);
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn concurrent_task_shutdown_aborts_after_timeout() {
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let handle = spawn_concurrent_lifecycle_task("rocketmq-client-concurrent-task-test", async move {
            let _drop_flag = DropFlag(dropped_in_task);
            pending::<()>().await;
        })
        .expect("test task should spawn");

        assert!(!handle.shutdown(Duration::from_millis(20)).await);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn concurrent_clean_expire_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_concurrent_clean_expire_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 1);
        assert_eq!(probe.task_count_after_shutdown, 0);
    }

    #[tokio::test]
    async fn tracked_concurrent_task_shutdown_waits_for_completion() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();

        spawn_tracked_concurrent_task(
            "rocketmq-client-concurrent-tracker-test",
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
    async fn tracked_concurrent_task_force_stop_cancels_pending_task() {
        let tracker = TaskTracker::new();
        let token = CancellationToken::new();
        let submitted = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();

        spawn_tracked_concurrent_task(
            "rocketmq-client-concurrent-tracker-test",
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

    #[test]
    fn start_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);

        service.start(Arc::new(new_service(None)));

        assert!(service.clean_expire_task_handle.lock().is_some());
        service.shutdown_token.cancel();
        let handle = { service.clean_expire_task_handle.lock().take() };
        if let Some(handle) = handle {
            handle.abort();
        }
    }

    #[test]
    fn submit_consume_request_later_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);
        let this = Arc::new(new_service(None));

        service.submit_consume_request_later(vec![], this, Arc::new(ProcessQueue::new()), message_queue());
        service.shutdown_token.cancel();
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn spawn_consume_task_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);

        service.spawn_consume_task(
            Arc::new(new_service(None)),
            vec![Arc::new(MessageExt::default())],
            Arc::new(ProcessQueue::new()),
            message_queue(),
            true,
        );
        std::thread::sleep(Duration::from_millis(30));
    }

    #[tokio::test]
    async fn process_consume_result_without_offset_store_does_not_panic() {
        let default_impl = new_default_impl();
        let service = new_service(Some(default_impl.clone()));
        let process_queue = Arc::new(ProcessQueue::new());
        let msg = Arc::new(MessageExt::default());
        let messages = vec![msg.clone()];
        process_queue.put_message(&messages).await;
        let mut request = ConsumeRequest {
            msgs: vec![msg],
            message_listener: listener(),
            process_queue,
            message_queue: message_queue(),
            dispatch_to_consume: true,
            consumer_group: consumer_group(),
            default_mqpush_consumer_impl: Some(default_impl),
        };
        let context = ConsumeConcurrentlyContext::new(message_queue());

        service
            .process_consume_result(
                Arc::new(new_service(None)),
                ConsumeConcurrentlyStatus::ConsumeSuccess,
                &context,
                &mut request,
            )
            .await;
    }
}
