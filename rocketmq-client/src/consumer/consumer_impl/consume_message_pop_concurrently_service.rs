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

use std::error::Error;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_rust::ArcMut;
use tokio::sync::Semaphore;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::ack_callback::AckCallback;
use crate::consumer::ack_result::AckResult;
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

fn spawn_detached_pop_concurrent_task<F>(thread_name: &'static str, task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        drop(handle.spawn(task));
        return;
    }

    match thread::Builder::new().name(thread_name.to_string()).spawn(move || {
        match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime.block_on(task),
            Err(error) => warn!("Failed to build {} runtime: {}", thread_name, error),
        }
    }) {
        Ok(handle) => drop(handle),
        Err(error) => warn!("Failed to spawn {} background thread: {}", thread_name, error),
    }
}

pub struct ConsumeMessagePopConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcMessageListenerConcurrently,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) active_tasks: Arc<AtomicUsize>,
    pub(crate) concurrency_limiter: Arc<Semaphore>,
    pub(crate) max_concurrency: Arc<AtomicUsize>,
}

impl ConsumeMessagePopConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let consume_thread = consumer_config.consume_thread_min;
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            stopped: Arc::new(AtomicBool::new(false)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            concurrency_limiter: Arc::new(Semaphore::new(consume_thread as usize)),
            max_concurrency: Arc::new(AtomicUsize::new(consume_thread as usize)),
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

    fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            16
        } else {
            self.consumer_config.max_reconsume_times
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopConcurrentlyService {
    fn start(&mut self, this: ArcMut<Self>) {}

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        info!(
            "{} ConsumeMessagePopConcurrentlyService shutdown started",
            self.consumer_group
        );

        self.stopped.store(true, Ordering::Release);
        self.concurrency_limiter.close();

        let timeout = Duration::from_millis(await_terminate_millis);
        let start_time = Instant::now();

        while self.active_tasks.load(Ordering::Acquire) > 0 {
            if start_time.elapsed() >= timeout {
                warn!(
                    "{} ConsumeMessagePopConcurrentlyService shutdown timeout, {} tasks still active",
                    self.consumer_group,
                    self.active_tasks.load(Ordering::Acquire)
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!(
            "{} ConsumeMessagePopConcurrentlyService shutdown completed",
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

    async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        info!("consumeMessageDirectly receive new message: {}", msg);

        let mq = MessageQueue::from_parts(msg.topic().clone(), broker_name.unwrap_or_default(), msg.queue_id());
        let mut msgs = vec![ArcMut::new(msg)];
        let context = ConsumeConcurrentlyContext::new(mq);
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
        let status_result = tokio::task::spawn_blocking(move || {
            let msgs_refs: Vec<&MessageExt> = msgs_cloned.iter().collect();
            let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
                msgs_refs.first().copied(),
                msgs_refs.len(),
                group_for_span.as_str(),
                &context.message_queue,
                "pop_concurrent_direct",
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
        .await;
        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        crate::observability_metrics::record_consume(1, consume_rt);

        let mut result = ConsumeMessageDirectlyResult::default();
        result.set_order(false);
        result.set_auto_commit(true);
        match status_result {
            Ok(Ok(status)) => match status {
                ConsumeConcurrentlyStatus::ConsumeSuccess => {
                    result.set_consume_result(CMResult::CRSuccess);
                }
                ConsumeConcurrentlyStatus::ReconsumeLater => {
                    result.set_consume_result(CMResult::CRLater);
                }
            },
            Ok(Err(e)) => {
                result.set_consume_result(CMResult::CRThrowException);
                result.set_remark(CheetahString::from_string(e.to_string()))
            }
            Err(join_err) => {
                result.set_consume_result(CMResult::CRThrowException);
                result.set_remark(CheetahString::from_string(format!(
                    "consume_message panicked: {join_err}"
                )))
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
        let _ = (this, msgs, process_queue, message_queue, dispatch_to_consume);
    }

    async fn submit_pop_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let consume_batch_size = self.consumer_config.consume_message_batch_max_size.max(1);
        if msgs.len() <= consume_batch_size as usize {
            let request = ConsumeRequest::new(
                msgs,
                Arc::new(process_queue.clone()),
                message_queue.clone(),
                self.consumer_group.clone(),
                self.message_listener.clone(),
                self.default_mqpush_consumer_impl.clone(),
            );
            let limiter = self.concurrency_limiter.clone();
            let stopped = self.stopped.clone();
            spawn_detached_pop_concurrent_task("rocketmq-client-pop-concurrent-consume", async move {
                if stopped.load(Ordering::Acquire) {
                    return;
                }

                let permit = match limiter.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        warn!("Failed to acquire permit, semaphore closed");
                        return;
                    }
                };

                request.run(this).await;
                drop(permit);
            });
        } else {
            let consumer_group = self.consumer_group.clone();
            let message_listener = self.message_listener.clone();
            let default_impl = self.default_mqpush_consumer_impl.clone();
            let limiter = self.concurrency_limiter.clone();
            let stopped = self.stopped.clone();
            msgs.chunks(consume_batch_size as usize)
                .map(|t| t.to_vec())
                .for_each(|chunk| {
                    let consume_request = ConsumeRequest::new(
                        chunk,
                        Arc::new(process_queue.clone()),
                        message_queue.clone(),
                        consumer_group.clone(),
                        message_listener.clone(),
                        default_impl.clone(),
                    );
                    let pop_service = this.clone();
                    let limiter_clone = limiter.clone();
                    let stopped_clone = stopped.clone();
                    spawn_detached_pop_concurrent_task("rocketmq-client-pop-concurrent-consume", async move {
                        if stopped_clone.load(Ordering::Acquire) {
                            return;
                        }

                        let permit = match limiter_clone.acquire().await {
                            Ok(p) => p,
                            Err(_) => {
                                warn!("Failed to acquire permit, semaphore closed");
                                return;
                            }
                        };

                        consume_request.run(pop_service).await;
                        drop(permit);
                    });
                });
        }
    }
}

impl ConsumeMessagePopConcurrentlyService {
    /// Submit consume request after 5 seconds delay for retry
    async fn submit_pop_consume_request_later(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: Arc<PopProcessQueue>,
        message_queue: MessageQueue,
    ) {
        let stopped = self.stopped.clone();

        spawn_detached_pop_concurrent_task("rocketmq-client-pop-concurrent-consume-delay", async move {
            if stopped.load(Ordering::Acquire) {
                warn!("Service stopped, discard delayed consume request for {}", message_queue);
                return;
            }

            tokio::time::sleep(Duration::from_millis(5000)).await;

            if stopped.load(Ordering::Acquire) {
                warn!("Service stopped, discard delayed consume request for {}", message_queue);
                return;
            }

            this.submit_pop_consume_request(this.clone(), msgs, &process_queue, &message_queue)
                .await;
        });
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

        if ack_index >= 0 {
            for i in 0..=ack_index {
                let msg = &consume_request.msgs[i as usize];
                if let Some(default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_mut() {
                    default_mqpush_consumer_impl
                        .ack_async(msg.as_ref(), &self.consumer_group)
                        .await;
                } else {
                    warn!(
                        "pop consume ack skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}",
                        self.consumer_group, consume_request.message_queue
                    );
                }
                consume_request.process_queue.ack();
            }
        }

        for i in (ack_index + 1) as usize..consume_request.msgs.len() {
            let msg = &consume_request.msgs[i];
            consume_request.process_queue.ack();
            if msg.reconsume_times >= self.get_max_reconsume_times() {
                self.check_need_ack_or_delay(msg).await;
                continue;
            }

            let delay_level = context.delay_level_when_next_consume;
            let consumer_group = self.consumer_group.clone();
            self.change_pop_invisible_time(msg, &consumer_group, delay_level).await;
        }
    }

    async fn check_need_ack_or_delay(&mut self, message: &MessageExt) {
        let Some(mut default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref().cloned() else {
            warn!(
                "pop consume retry handling skipped: DefaultMQPushConsumerImpl is not initialized, group={}, msg={}",
                self.consumer_group, message
            );
            return;
        };
        let delay_level_table = default_mqpush_consumer_impl.pop_delay_level.clone();
        let delay_time = i64::from(delay_level_table[delay_level_table.len() - 1]) * 1000 * 2;
        let msg_delay_time = java_signed_elapsed_millis(current_millis(), message.born_timestamp);
        if msg_delay_time > delay_time {
            warn!("Consume too many times, ack message async. message {}", message);
            default_mqpush_consumer_impl
                .ack_async(message, &self.consumer_group)
                .await;
        } else {
            let mut delay_level = (delay_level_table.len() as i32) - 1;
            while delay_level >= 0 {
                if msg_delay_time >= i64::from(delay_level_table[delay_level as usize]) * 1000 {
                    delay_level += 1;
                    break;
                }
                delay_level -= 1;
            }
            let keys = message.get_keys().unwrap_or_default();
            let consumer_group = self.consumer_group.clone();
            self.change_pop_invisible_time(message, &consumer_group, delay_level)
                .await;
            warn!(
                "Consume too many times, but delay time {} not enough. changePopInvisibleTime to delayLevel {} . \
                 message key:{}",
                msg_delay_time, delay_level, keys
            )
        }
    }

    async fn change_pop_invisible_time(
        &mut self,
        message: &MessageExt,
        consumer_group: &CheetahString,
        mut delay_level: i32,
    ) {
        if delay_level == 0 {
            delay_level = message.reconsume_times;
        }
        let Some(mut default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref().cloned() else {
            warn!(
                "changePopInvisibleTime skipped: DefaultMQPushConsumerImpl is not initialized, group={} msg={}",
                consumer_group, message
            );
            return;
        };
        let delay_level_table = default_mqpush_consumer_impl.pop_delay_level.clone();
        let delay_second = if delay_level < 0 || delay_level as usize >= delay_level_table.len() {
            delay_level_table[delay_level_table.len() - 1]
        } else {
            delay_level_table[delay_level as usize]
        };
        let extra_info = message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK));

        struct DefaultAckCallback;

        impl AckCallback for DefaultAckCallback {
            fn on_success(&self, ack_result: AckResult) {}

            fn on_exception(&self, e: Box<dyn Error>) {
                error!("changePopInvisibleTime exception: {}", e);
            }
        }

        let result = default_mqpush_consumer_impl
            .change_pop_invisible_time_async(
                message.topic(),
                consumer_group,
                &extra_info.unwrap_or_default(),
                (delay_second * 1000) as u64,
                DefaultAckCallback,
            )
            .await;

        if let Err(e) = result {
            error!(
                "changePopInvisibleTimeAsync fail, group:{} msg:{} errorInfo:{}",
                consumer_group, message, e
            );
        }
    }
}

struct ConsumeRequest {
    msgs: Vec<ArcMut<MessageExt>>,
    process_queue: Arc<PopProcessQueue>,
    message_queue: MessageQueue,
    pop_time: u64,
    invisible_time: u64,
    consumer_group: CheetahString,
    message_listener: ArcMessageListenerConcurrently,
    default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
}

impl ConsumeRequest {
    pub fn new(
        msgs: Vec<MessageExt>,
        process_queue: Arc<PopProcessQueue>,
        message_queue: MessageQueue,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let mut pop_time = 0u64;
        let mut invisible_time = 0u64;

        if let Some(first) = msgs.first() {
            if let Some(extra_info) = first.property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK)) {
                let extra_info_strs = ExtraInfoUtil::split(&extra_info);
                if let Ok(pt) = ExtraInfoUtil::get_pop_time(&extra_info_strs) {
                    pop_time = pt as u64;
                }
                if let Ok(it) = ExtraInfoUtil::get_invisible_time(&extra_info_strs) {
                    invisible_time = it as u64;
                }
            }
        }

        let msgs_arc: Vec<ArcMut<MessageExt>> = msgs.into_iter().map(ArcMut::new).collect();

        Self {
            msgs: msgs_arc,
            process_queue,
            message_queue,
            pop_time,
            invisible_time,
            consumer_group,
            message_listener,
            default_mqpush_consumer_impl,
        }
    }

    #[inline]
    pub fn is_pop_timeout(&self) -> bool {
        if self.msgs.is_empty() || self.pop_time == 0 || self.invisible_time == 0 {
            return true;
        }
        current_millis().saturating_sub(self.pop_time) >= self.invisible_time
    }

    pub async fn run(mut self, mut consume_message_concurrently_service: ArcMut<ConsumeMessagePopConcurrentlyService>) {
        if consume_message_concurrently_service.stopped.load(Ordering::Acquire) {
            warn!(
                "run, service stopped, discard consume request for {}",
                self.message_queue
            );
            return;
        }

        consume_message_concurrently_service
            .active_tasks
            .fetch_add(1, Ordering::SeqCst);
        let active_tasks = consume_message_concurrently_service.active_tasks.clone();

        struct TaskGuard {
            active_tasks: Arc<AtomicUsize>,
        }

        impl Drop for TaskGuard {
            fn drop(&mut self) {
                self.active_tasks.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let _guard = TaskGuard {
            active_tasks: active_tasks.clone(),
        };

        if self.process_queue.is_dropped() {
            info!(
                "the message queue not be able to consume, because it's dropped(pop). group={} {}",
                self.consumer_group, self.message_queue
            );
            return;
        }
        if self.is_pop_timeout() {
            info!(
                "the pop message time out so abort consume. popTime={} invisibleTime={}, group={} {}",
                self.pop_time, self.invisible_time, self.consumer_group, self.message_queue
            );
            self.process_queue.inc_found_msg(self.msgs.len());
            return;
        }
        let Some(mut default_mqpush_consumer_impl) = self.default_mqpush_consumer_impl.as_ref().cloned() else {
            warn!(
                "pop consume request skipped: DefaultMQPushConsumerImpl is not initialized, group={}, mq={}, msgs={}",
                self.consumer_group,
                self.message_queue,
                self.msgs.len()
            );
            return;
        };
        default_mqpush_consumer_impl.reset_retry_and_namespace(&mut self.msgs, self.consumer_group.as_str());
        let mut consume_message_context = None;

        let begin_timestamp = Instant::now();
        let mut has_exception = false;
        let mut status = None;

        if !self.msgs.is_empty() {
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(
                    msg.as_mut(),
                    CheetahString::from_string(current_millis().to_string()),
                );
            }
        }

        if default_mqpush_consumer_impl.has_hook() {
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
        let msgs_cloned: Vec<MessageExt> = self.msgs.iter().map(|m| m.as_ref().clone()).collect();
        let process_span = crate::consumer::consumer_impl::observability::consumer_process_span(
            msgs_cloned.first(),
            msgs_cloned.len(),
            self.consumer_group.as_str(),
            &self.message_queue,
            "pop_concurrent",
        );
        let context = ConsumeConcurrentlyContext {
            message_queue: self.message_queue.clone(),
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        };
        let process_span_for_blocking = process_span.clone();
        let blocking_result = tokio::task::spawn_blocking(move || {
            let _entered = process_span_for_blocking.enter();
            let msgs_refs: Vec<&MessageExt> = msgs_cloned.iter().collect();
            let result = listener.consume_message(&msgs_refs, &context);
            (result, context)
        })
        .await;
        let context = match blocking_result {
            Ok((Ok(value), ctx)) => {
                status = Some(value);
                ctx
            }
            Ok((Err(e), ctx)) => {
                has_exception = true;
                error!(
                    "consumeMessage exception: {:?}, Group: {}, Msgs: {}, MQ: {}",
                    e,
                    self.consumer_group,
                    self.msgs.len(),
                    self.message_queue
                );
                ctx
            }
            Err(join_err) => {
                has_exception = true;
                error!(
                    "consumeMessage task panicked: {:?}, Group: {}, Msgs: {}, MQ: {}",
                    join_err,
                    self.consumer_group,
                    self.msgs.len(),
                    self.message_queue
                );
                ConsumeConcurrentlyContext {
                    message_queue: self.message_queue.clone(),
                    delay_level_when_next_consume: 0,
                    ack_index: i32::MAX,
                }
            }
        };
        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        crate::observability_metrics::record_consume(self.msgs.len(), consume_rt);
        let return_type = classify_pop_consume_return_type(status, has_exception, consume_rt, self.invisible_time);

        if default_mqpush_consumer_impl.has_hook() {
            if let Some(context) = consume_message_context.as_mut() {
                context.props.insert(
                    CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE),
                    return_type.to_string().into(),
                );
            } else {
                warn!(
                    "pop consume hook context missing before return type update, group={}, mq={}",
                    self.consumer_group, self.message_queue
                );
            }
        }

        let final_status = status.unwrap_or(ConsumeConcurrentlyStatus::ReconsumeLater);
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

        if default_mqpush_consumer_impl.has_hook() {
            if let Some(cmc) = consume_message_context.as_mut() {
                cmc.status = final_status.to_string().into();
                cmc.success = final_status == ConsumeConcurrentlyStatus::ConsumeSuccess;
                cmc.access_channel = Some(default_mqpush_consumer_impl.client_config.access_channel);
                default_mqpush_consumer_impl.execute_hook_after(cmc);
            } else {
                warn!(
                    "pop consume hook context missing before after-hook execution, group={}, mq={}",
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
                "the message queue not be able to consume, because it's dropped. group={} {}",
                self.consumer_group, self.message_queue,
            );
        } else if self.is_pop_timeout() {
            self.process_queue.inc_found_msg(self.msgs.len());
            warn!(
                "processQueue invalid or popTimeout. isDropped={}, isPopTimeout={}, messageQueue={}, msgs={}",
                self.process_queue.is_dropped(),
                self.is_pop_timeout(),
                self.message_queue,
                self.msgs.len()
            );
        } else {
            let this = consume_message_concurrently_service.clone();

            consume_message_concurrently_service
                .process_consume_result(this, final_status, &context, &mut self)
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

fn java_signed_elapsed_millis(now_millis: u64, born_timestamp: i64) -> i64 {
    i64::try_from(now_millis)
        .unwrap_or(i64::MAX)
        .saturating_sub(born_timestamp)
}

fn classify_pop_consume_return_type(
    status: Option<ConsumeConcurrentlyStatus>,
    has_exception: bool,
    consume_rt: u64,
    invisible_time: u64,
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
            if consume_rt >= invisible_time.saturating_mul(1000) {
                ConsumeReturnType::TimeOut
            } else {
                ConsumeReturnType::Failed
            }
        }
        Some(ConsumeConcurrentlyStatus::ConsumeSuccess) => {
            if consume_rt >= invisible_time.saturating_mul(1000) {
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

    fn new_service(default_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>) -> ConsumeMessagePopConcurrentlyService {
        ConsumeMessagePopConcurrentlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(ConsumerConfig::default()),
            consumer_group(),
            listener(),
            default_impl,
        )
    }

    fn new_service_with_config(consumer_config: ConsumerConfig) -> ConsumeMessagePopConcurrentlyService {
        ConsumeMessagePopConcurrentlyService::new(
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

    fn pop_message() -> MessageExt {
        let mut message = MessageExt::default();
        let extra_info = ExtraInfoUtil::build_extra_info(0, current_millis() as i64, 60_000, 0, "topic", "broker-a", 0);
        MessageAccessor::put_property(
            &mut message,
            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
            CheetahString::from_string(extra_info),
        );
        message
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
    fn java_signed_elapsed_millis_matches_java_long_subtraction_without_wrapping() {
        assert_eq!(java_signed_elapsed_millis(1_500, 1_000), 500);
        assert_eq!(java_signed_elapsed_millis(1_000, 1_500), -500);
        assert_eq!(java_signed_elapsed_millis(1_000, -500), 1_500);
        assert_eq!(java_signed_elapsed_millis(u64::MAX, -1), i64::MAX);
    }

    #[test]
    fn max_reconsume_times_defaults_to_java_push_impl_value() {
        let service = new_service(None);

        assert_eq!(service.get_max_reconsume_times(), 16);
    }

    #[test]
    fn max_reconsume_times_preserves_configured_value() {
        let config = ConsumerConfig {
            max_reconsume_times: 3,
            ..Default::default()
        };
        let service = new_service_with_config(config);

        assert_eq!(service.get_max_reconsume_times(), 3);
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
    fn submit_single_pop_consume_request_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);
        service.stopped.store(true, Ordering::Release);

        futures::executor::block_on(service.submit_pop_consume_request(
            ArcMut::new(new_service(None)),
            vec![pop_message()],
            &PopProcessQueue::new(),
            &message_queue(),
        ));
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn submit_split_pop_consume_request_without_tokio_runtime_does_not_spawn_panic() {
        let config = ConsumerConfig {
            consume_message_batch_max_size: 1,
            ..Default::default()
        };
        let service = new_service_with_config(config);
        service.stopped.store(true, Ordering::Release);

        futures::executor::block_on(service.submit_pop_consume_request(
            ArcMut::new(new_service(None)),
            vec![pop_message(), pop_message()],
            &PopProcessQueue::new(),
            &message_queue(),
        ));
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn submit_pop_consume_request_later_without_tokio_runtime_does_not_spawn_panic() {
        let service = new_service(None);
        service.stopped.store(true, Ordering::Release);

        futures::executor::block_on(service.submit_pop_consume_request_later(
            ArcMut::new(new_service(None)),
            vec![pop_message()],
            Arc::new(PopProcessQueue::new()),
            message_queue(),
        ));
        std::thread::sleep(Duration::from_millis(30));
    }

    #[test]
    fn pop_consume_return_type_timeout_uses_java_invisible_time_multiplier() {
        assert_eq!(
            classify_pop_consume_return_type(
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess),
                false,
                59_999_999,
                60_000
            ),
            ConsumeReturnType::Success
        );
        assert_eq!(
            classify_pop_consume_return_type(
                Some(ConsumeConcurrentlyStatus::ConsumeSuccess),
                false,
                60_000_000,
                60_000
            ),
            ConsumeReturnType::TimeOut
        );
        assert_eq!(
            classify_pop_consume_return_type(
                Some(ConsumeConcurrentlyStatus::ReconsumeLater),
                false,
                59_999_999,
                60_000
            ),
            ConsumeReturnType::Failed
        );
    }

    #[test]
    fn pop_consume_return_type_preserves_null_and_exception_classification() {
        assert_eq!(
            classify_pop_consume_return_type(None, true, 60_000_000, 60_000),
            ConsumeReturnType::Exception
        );
        assert_eq!(
            classify_pop_consume_return_type(None, false, 60_000_000, 60_000),
            ConsumeReturnType::ReturnNull
        );
    }

    #[tokio::test]
    async fn consume_message_directly_without_default_impl_does_not_panic() {
        let service = new_service(None);

        let result = service
            .consume_message_directly(pop_message(), Some(CheetahString::from_static_str("broker-a")))
            .await;

        assert!(matches!(result.consume_result(), Some(CMResult::CRSuccess)));
    }

    #[tokio::test]
    async fn consume_request_without_default_impl_is_ignored_without_panic() {
        let service = ArcMut::new(new_service(None));
        let process_queue = Arc::new(PopProcessQueue::new());
        let request = ConsumeRequest::new(
            vec![pop_message()],
            process_queue,
            message_queue(),
            consumer_group(),
            listener(),
            None,
        );

        request.run(service.clone()).await;

        assert_eq!(service.active_tasks.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn shutdown_closes_concurrency_limiter_like_java_executor_shutdown() {
        let mut service = new_service(None);

        service.shutdown(100).await;

        assert!(service.concurrency_limiter.is_closed());
    }

    #[tokio::test]
    async fn process_consume_result_without_default_impl_does_not_panic() {
        let mut service = new_service(None);
        let process_queue = Arc::new(PopProcessQueue::new());
        process_queue.inc_found_msg(1);
        let mut request = ConsumeRequest::new(
            vec![pop_message()],
            process_queue.clone(),
            message_queue(),
            consumer_group(),
            listener(),
            None,
        );
        let context = ConsumeConcurrentlyContext::new(message_queue());

        service
            .process_consume_result(
                ArcMut::new(new_service(None)),
                ConsumeConcurrentlyStatus::ConsumeSuccess,
                &context,
                &mut request,
            )
            .await;

        assert_eq!(process_queue.get_wai_ack_msg_count(), 0);
    }

    #[tokio::test]
    async fn process_consume_result_without_client_instance_does_not_panic() {
        let default_impl = new_default_impl();
        let mut service = new_service(Some(default_impl.clone()));
        let process_queue = Arc::new(PopProcessQueue::new());
        process_queue.inc_found_msg(1);
        let mut request = ConsumeRequest::new(
            vec![pop_message()],
            process_queue.clone(),
            message_queue(),
            consumer_group(),
            listener(),
            Some(default_impl),
        );
        let context = ConsumeConcurrentlyContext::new(message_queue());

        service
            .process_consume_result(
                ArcMut::new(new_service(None)),
                ConsumeConcurrentlyStatus::ConsumeSuccess,
                &context,
                &mut request,
            )
            .await;

        assert_eq!(process_queue.get_wai_ack_msg_count(), 0);
    }
}
