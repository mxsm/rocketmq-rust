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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
        let consume_thread = consumer_config.consume_thread_max;
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
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopConcurrentlyService {
    fn start(&mut self, this: ArcMut<Self>) {}

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        info!(
            "{} ConsumeMessagePopConcurrentlyService shutdown started",
            self.consumer_group
        );

        self.stopped.store(true, Ordering::Release);

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
        if core_pool_size > 0 && core_pool_size <= u16::MAX as usize {
            let old_size = self.max_concurrency.load(Ordering::Acquire);
            self.max_concurrency.store(core_pool_size, Ordering::Release);

            if core_pool_size > old_size {
                let diff = core_pool_size - old_size;
                self.concurrency_limiter.add_permits(diff);
                info!(
                    "{} ConsumeMessagePopConcurrentlyService increase core pool size from {} to {}",
                    self.consumer_group, old_size, core_pool_size
                );
            } else if core_pool_size < old_size {
                info!(
                    "{} ConsumeMessagePopConcurrentlyService decrease core pool size from {} to {} (will take effect \
                     gradually)",
                    self.consumer_group, old_size, core_pool_size
                );
            }
        }
    }

    fn inc_core_pool_size(&self) {
        let current = self.max_concurrency.load(Ordering::Acquire);
        if current < u16::MAX as usize {
            self.update_core_pool_size(current + 1);
        }
    }

    fn dec_core_pool_size(&self) {
        let current = self.max_concurrency.load(Ordering::Acquire);
        if current > 1 {
            self.update_core_pool_size(current - 1);
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
        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());

        let begin_timestamp = Instant::now();

        let listener = self.message_listener.clone();
        let msgs_cloned: Vec<MessageExt> = msgs.iter().map(|m| m.as_ref().clone()).collect();
        let status_result = tokio::task::spawn_blocking(move || {
            let msgs_refs: Vec<&MessageExt> = msgs_cloned.iter().collect();
            listener.consume_message(&msgs_refs, &context)
        })
        .await;
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
        result.set_spent_time_mills(begin_timestamp.elapsed().as_millis() as u64);
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
        unimplemented!("ConsumeMessagePopConcurrentlyService.submit_consume_request is not supported")
    }

    async fn submit_pop_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let consume_batch_size = self.consumer_config.consume_message_batch_max_size;
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
            tokio::spawn(async move {
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
                    tokio::spawn(async move {
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

        tokio::spawn(async move {
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
        let mut ack_index = context.ack_index;
        match status {
            ConsumeConcurrentlyStatus::ConsumeSuccess => {
                if ack_index >= consume_request.msgs.len() as i32 {
                    ack_index = consume_request.msgs.len() as i32 - 1;
                }
            }
            ConsumeConcurrentlyStatus::ReconsumeLater => {
                ack_index = -1;
            }
        }

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
                self.default_mqpush_consumer_impl
                    .as_mut()
                    .unwrap()
                    .ack_async(msg.as_ref(), &self.consumer_group)
                    .await;
                consume_request.process_queue.ack();
            }
        }

        for i in (ack_index + 1) as usize..consume_request.msgs.len() {
            let msg = &consume_request.msgs[i];
            consume_request.process_queue.ack();
            if msg.reconsume_times >= self.consumer_config.max_reconsume_times {
                self.check_need_ack_or_delay(msg).await;
                continue;
            }

            let delay_level = context.delay_level_when_next_consume;
            let consumer_group = self.consumer_group.clone();
            self.change_pop_invisible_time(msg, &consumer_group, delay_level).await;
        }
    }

    async fn check_need_ack_or_delay(&mut self, message: &MessageExt) {
        let delay_level_table = self
            .default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .pop_delay_level
            .as_ref();
        let delay_time = delay_level_table[delay_level_table.len() - 1] * 1000 * 2;
        let msg_delay_time = current_millis() - message.born_timestamp as u64;
        if msg_delay_time > delay_time as u64 {
            warn!("Consume too many times, ack message async. message {}", message);
            self.default_mqpush_consumer_impl
                .as_mut()
                .unwrap()
                .ack_async(message, &self.consumer_group)
                .await;
        } else {
            let mut delay_level = (delay_level_table.len() as i32) - 1;
            while delay_level >= 0 {
                if msg_delay_time >= (delay_level_table[delay_level as usize] * 1000) as u64 {
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
        let delay_level_table = self
            .default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .pop_delay_level
            .as_ref();
        let delay_second = if delay_level as usize > delay_level_table.len() {
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

        let result = self
            .default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
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
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
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
            default_mqpush_consumer_impl.execute_hook_before(consume_message_context.as_mut().unwrap());
        }

        let listener = self.message_listener.clone();
        let msgs_cloned: Vec<MessageExt> = self.msgs.iter().map(|m| m.as_ref().clone()).collect();
        let context = ConsumeConcurrentlyContext {
            message_queue: self.message_queue.clone(),
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        };
        let blocking_result = tokio::task::spawn_blocking(move || {
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
        let return_type = match status {
            None => {
                if has_exception {
                    ConsumeReturnType::Exception
                } else {
                    ConsumeReturnType::ReturnNull
                }
            }
            Some(s) => {
                if consume_rt >= self.invisible_time {
                    ConsumeReturnType::TimeOut
                } else if s == ConsumeConcurrentlyStatus::ReconsumeLater {
                    ConsumeReturnType::Failed
                } else {
                    ConsumeReturnType::Success
                }
            }
        };

        if default_mqpush_consumer_impl.has_hook() {
            consume_message_context.as_mut().unwrap().props.insert(
                CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE),
                return_type.to_string().into(),
            );
        }

        if status.is_none() {
            status = Some(ConsumeConcurrentlyStatus::ReconsumeLater);
        }

        if default_mqpush_consumer_impl.has_hook() {
            let cmc = consume_message_context.as_mut().unwrap();
            cmc.status = status.unwrap().to_string().into();
            cmc.success = status.unwrap() == ConsumeConcurrentlyStatus::ConsumeSuccess;
            cmc.access_channel = Some(default_mqpush_consumer_impl.client_config.access_channel);
            default_mqpush_consumer_impl.execute_hook_after(consume_message_context.as_mut().unwrap());
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
                .process_consume_result(this, status.unwrap(), &context, &mut self)
                .await;
        }
    }
}
