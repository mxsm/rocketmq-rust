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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::ArcMut;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
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

pub struct ConsumeMessageConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcMessageListenerConcurrently,
    /// Semaphore used for two purposes:
    ///   1. Backpressure: limits the number of concurrently executing consume tasks to the
    ///      Java-equivalent core pool size. New tasks that cannot acquire a permit are retried
    ///      after 5 s.
    ///   2. Graceful shutdown: `shutdown()` acquires all permits, which is only possible once every
    ///      in-flight task has released its permit.
    consume_semaphore: Arc<Semaphore>,
    max_concurrency: Arc<AtomicUsize>,
    /// Token cancelled by `shutdown()` to prevent new task submissions.
    shutdown_token: CancellationToken,
    /// Background task matching Java's cleanExpireMsgExecutors lifecycle.
    clean_expire_task_handle: Arc<Mutex<Option<ConcurrentTaskHandle>>>,
}

enum ConcurrentTaskHandle {
    Tokio(tokio::task::JoinHandle<()>),
    Thread(thread::JoinHandle<()>),
}

impl ConcurrentTaskHandle {
    fn shutdown(self) {
        match self {
            Self::Tokio(handle) => handle.abort(),
            Self::Thread(handle) => {
                if handle.is_finished() {
                    let _ = handle.join();
                }
            }
        }
    }
}

fn spawn_concurrent_task<F>(thread_name: &'static str, task: F) -> Option<ConcurrentTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return Some(ConcurrentTaskHandle::Tokio(handle.spawn(task)));
    }

    match thread::Builder::new().name(thread_name.to_string()).spawn(move || {
        match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime.block_on(task),
            Err(error) => warn!("Failed to build {} runtime: {}", thread_name, error),
        }
    }) {
        Ok(handle) => Some(ConcurrentTaskHandle::Thread(handle)),
        Err(error) => {
            warn!("Failed to spawn {} background thread: {}", thread_name, error);
            None
        }
    }
}

fn spawn_detached_concurrent_task<F>(thread_name: &'static str, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    drop(spawn_concurrent_task(thread_name, task));
}

impl ConsumeMessageConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let core_pool_size = consumer_config.consume_thread_min as usize;
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            consume_semaphore: Arc::new(Semaphore::new(core_pool_size)),
            max_concurrency: Arc::new(AtomicUsize::new(core_pool_size)),
            shutdown_token: CancellationToken::new(),
            clean_expire_task_handle: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown_token.is_cancelled()
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
    async fn clean_expire_msg(&mut self) {
        let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.clone() else {
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
                .clean_expired_msg(self.default_mqpush_consumer_impl.clone())
                .await;
        }
    }

    async fn process_consume_result(
        &mut self,
        this: ArcMut<Self>,
        status: ConsumeConcurrentlyStatus,
        context: &ConsumeConcurrentlyContext,
        consume_request: &mut ConsumeRequest,
    ) {
        if consume_request.msgs.is_empty() {
            return;
        }
        let ack_index = normalize_ack_index(status, context.ack_index, consume_request.msgs.len());

        // Update per-topic/group consume throughput counters, split by ack index.
        if let Some(impl_) = self.default_mqpush_consumer_impl.as_ref() {
            if let Some(client_instance) = impl_.client_instance.as_ref() {
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

                    let sent = self.send_message_back(&mut msg, context).await;
                    if sent {
                        consume_request.msgs.push(msg);
                    } else {
                        let times = msg.reconsume_times() + 1;
                        msg.set_reconsume_times(times);
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
            let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_mut() else {
                warn!(
                    "consume offset update skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}",
                    self.consumer_group, consume_request.message_queue
                );
                return;
            };
            let Some(offset_store) = default_mqpush_consumer_impl.offset_store.as_mut() else {
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
        msgs: Vec<ArcMut<MessageExt>>,
        this: ArcMut<Self>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
    ) {
        let shutdown_token = self.shutdown_token.clone();
        spawn_detached_concurrent_task("rocketmq-client-concurrent-consume-delay", async move {
            tokio::select! {
                _ = shutdown_token.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            this.submit_consume_request(this.clone(), msgs, process_queue, message_queue, true)
                .await;
        });
    }

    pub async fn send_message_back(&mut self, msg: &mut MessageExt, context: &ConsumeConcurrentlyContext) -> bool {
        let delay_level = context.delay_level_when_next_consume;
        msg.set_topic(self.client_config.with_namespace(msg.topic().as_str()));

        let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_mut() else {
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
                &self
                    .client_config
                    .queue_with_namespace(context.get_message_queue().clone()),
            )
            .await
            .is_ok()
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessageConcurrentlyService {
    fn start(&mut self, mut this: ArcMut<Self>) {
        let shutdown_token = self.shutdown_token.clone();
        let handle = spawn_concurrent_task("rocketmq-client-concurrent-clean-expire", async move {
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

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        self.shutdown_token.cancel();
        if let Some(handle) = self.clean_expire_task_handle.lock().take() {
            handle.shutdown();
        }
        let max = self.max_concurrency.load(Ordering::Acquire) as u32;
        match tokio::time::timeout(
            Duration::from_millis(await_terminate_millis),
            Arc::clone(&self.consume_semaphore).acquire_many_owned(max),
        )
        .await
        {
            Ok(Ok(_permits)) => {
                info!("ConsumeMessageConcurrentlyService shutdown gracefully");
            }
            Ok(Err(_closed)) => {
                // Semaphore was closed externally; treat as clean.
            }
            Err(_elapsed) => {
                warn!(
                    "ConsumeMessageConcurrentlyService shutdown timed out after {}ms; some consume tasks may still be \
                     running",
                    await_terminate_millis
                );
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
        let mut msgs = vec![ArcMut::new(msg)];
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
        let group_for_span = self.consumer_group.clone();
        let status = tokio::task::spawn_blocking(move || {
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
        crate::observability_metrics::record_consume(1, consume_rt);

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
        this: ArcMut<Self>,
        msgs: Vec<ArcMut<MessageExt>>,
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
        this: ArcMut<Self>,
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
        this: ArcMut<Self>,
        msgs: Vec<ArcMut<MessageExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        match Arc::clone(&self.consume_semaphore).try_acquire_owned() {
            Ok(permit) => {
                let mut consume_request = ConsumeRequest {
                    msgs,
                    message_listener: self.message_listener.clone(),
                    process_queue,
                    message_queue,
                    dispatch_to_consume,
                    consumer_group: self.consumer_group.clone(),
                    default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
                };
                spawn_detached_concurrent_task("rocketmq-client-concurrent-consume", async move {
                    let _permit = permit;
                    consume_request.run(this).await;
                });
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
    msgs: Vec<ArcMut<MessageExt>>,
    message_listener: ArcMessageListenerConcurrently,
    process_queue: Arc<ProcessQueue>,
    message_queue: MessageQueue,
    dispatch_to_consume: bool,
    consumer_group: CheetahString,
    default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
}

impl ConsumeRequest {
    async fn run(&mut self, mut consume_message_concurrently_service: ArcMut<ConsumeMessageConcurrentlyService>) {
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

        let Some(mut default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref().cloned() else {
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
            let start_ts = CheetahString::from_string(current_millis().to_string());
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(msg.as_mut(), start_ts.clone());
            }

            if has_hook {
                consume_message_context = Some(
                    ConsumeMessageContext::new(self.consumer_group.clone(), &self.msgs)
                        .with_mq(self.message_queue.clone())
                        .with_namespace(
                            default_mqpush_consumer_impl
                                .client_config
                                .get_namespace()
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

            let (blocking_status, blocking_has_exception, returned_context) = tokio::task::spawn_blocking(move || {
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
        crate::observability_metrics::record_consume(self.msgs.len(), consume_rt);

        let return_type = classify_concurrent_consume_return_type(
            status,
            has_exception,
            consume_rt,
            default_mqpush_consumer_impl.consumer_config.consume_timeout,
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
                cmc.access_channel = Some(default_mqpush_consumer_impl.client_config.access_channel);
                default_mqpush_consumer_impl.execute_hook_after(cmc);
            } else {
                warn!(
                    "consume hook context missing before after-hook execution, group={}, mq={}",
                    self.consumer_group, self.message_queue
                );
            }
        }

        // Record message consume round-trip time.
        if let Some(client_instance) = default_mqpush_consumer_impl.client_instance.as_ref() {
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

#[cfg(test)]
mod tests {
    use super::*;

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

    fn new_service(default_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>) -> ConsumeMessageConcurrentlyService {
        ConsumeMessageConcurrentlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(ConsumerConfig::default()),
            consumer_group(),
            listener(),
            default_impl,
        )
    }

    fn new_service_with_config(consumer_config: ConsumerConfig) -> ConsumeMessageConcurrentlyService {
        ConsumeMessageConcurrentlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(consumer_config),
            consumer_group(),
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

    fn consume_request(default_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>) -> ConsumeRequest {
        ConsumeRequest {
            msgs: vec![ArcMut::new(MessageExt::default())],
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
        let mut service = new_service(None);
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

    #[tokio::test]
    async fn consume_request_without_default_impl_is_ignored_without_panic() {
        let service = ArcMut::new(new_service(None));
        let mut request = consume_request(None);

        request.run(service).await;
    }

    #[tokio::test]
    async fn shutdown_aborts_clean_expire_task_like_java_executor_shutdown() {
        let mut service = new_service(None);

        service.start(ArcMut::new(new_service(None)));

        assert!(service.clean_expire_task_handle.lock().is_some());

        service.shutdown(100).await;

        assert!(service.clean_expire_task_handle.lock().is_none());
    }

    #[test]
    fn start_without_tokio_runtime_does_not_spawn_panic() {
        let mut service = new_service(None);

        service.start(ArcMut::new(new_service(None)));

        assert!(service.clean_expire_task_handle.lock().is_some());
        service.shutdown_token.cancel();
        let handle = { service.clean_expire_task_handle.lock().take() };
        if let Some(handle) = handle {
            handle.shutdown();
        }
    }

    #[test]
    fn submit_consume_request_later_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);
        let this = ArcMut::new(new_service(None));

        service.submit_consume_request_later(vec![], this, Arc::new(ProcessQueue::new()), message_queue());
        service.shutdown_token.cancel();
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn spawn_consume_task_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);

        service.spawn_consume_task(
            ArcMut::new(new_service(None)),
            vec![ArcMut::new(MessageExt::default())],
            Arc::new(ProcessQueue::new()),
            message_queue(),
            true,
        );
        std::thread::sleep(Duration::from_millis(30));
    }

    #[tokio::test]
    async fn process_consume_result_without_offset_store_does_not_panic() {
        let default_impl = new_default_impl();
        let mut service = new_service(Some(default_impl.clone()));
        let process_queue = Arc::new(ProcessQueue::new());
        let msg = ArcMut::new(MessageExt::default());
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
                ArcMut::new(new_service(None)),
                ConsumeConcurrentlyStatus::ConsumeSuccess,
                &context,
                &mut request,
            )
            .await;
    }
}
