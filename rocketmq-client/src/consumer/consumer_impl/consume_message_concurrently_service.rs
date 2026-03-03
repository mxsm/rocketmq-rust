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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
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
    ///   1. Backpressure: limits the number of concurrently executing consume tasks to
    ///      `consume_thread_max`. New tasks that cannot acquire a permit are retried after 5 s.
    ///   2. Graceful shutdown: `shutdown()` acquires all permits, which is only possible once every
    ///      in-flight task has released its permit.
    consume_semaphore: Arc<Semaphore>,
    /// Token cancelled by `shutdown()` to prevent new task submissions.
    shutdown_token: CancellationToken,
}

impl ConsumeMessageConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let max_concurrent = consumer_config.consume_thread_max as usize;
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            consume_semaphore: Arc::new(Semaphore::new(max_concurrent)),
            shutdown_token: CancellationToken::new(),
        }
    }
}

impl ConsumeMessageConcurrentlyService {
    async fn clean_expire_msg(&mut self) {
        let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.clone().unwrap();

        let process_queue_table = default_mqpush_consumer_impl
            .rebalance_impl
            .rebalance_impl_inner
            .process_queue_table
            .read()
            .await;
        for (_, process_queue) in process_queue_table.iter() {
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
            let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_mut().unwrap();
            default_mqpush_consumer_impl
                .offset_store
                .as_mut()
                .unwrap()
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
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            this.submit_consume_request(this.clone(), msgs, process_queue, message_queue, true)
                .await;
        });
    }

    pub async fn send_message_back(&mut self, msg: &mut MessageExt, context: &ConsumeConcurrentlyContext) -> bool {
        let delay_level = context.delay_level_when_next_consume;
        msg.set_topic(self.client_config.with_namespace(msg.topic().as_str()));

        self.default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
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
        tokio::spawn(async move {
            let timeout = this.consumer_config.consume_timeout;
            let mut interval = tokio::time::interval(Duration::from_secs(timeout * 60));
            interval.tick().await;
            loop {
                interval.tick().await;
                this.clean_expire_msg().await;
            }
        });
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        self.shutdown_token.cancel();
        let max = self.consumer_config.consume_thread_max;
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

    async fn consume_message_directly(
        &self,
        mut msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        info!("consumeMessageDirectly receive new message: {}", msg);
        msg.broker_name = broker_name.unwrap_or_default();
        let mq = MessageQueue::from_parts(msg.topic().clone(), msg.broker_name.clone(), msg.queue_id());
        let mut msgs = vec![ArcMut::new(msg)];
        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());

        let begin_timestamp = Instant::now();

        let listener = self.message_listener.clone();
        let status = tokio::task::spawn_blocking(move || {
            let context = ConsumeConcurrentlyContext::new(mq);
            let msgs_refs: Vec<&MessageExt> = msgs.iter().map(|m| m.as_ref()).collect();
            listener.consume_message(&msgs_refs, &context)
        })
        .await
        .unwrap_or_else(|join_err| {
            error!("consume_message_directly task panicked: {:?}", join_err);
            Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                "consume_message_directly task panicked: {join_err:?}"
            )))
        });

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
        unimplemented!("ConsumeMessageConcurrentlyService not support submit_pop_consume_request");
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
                tokio::spawn(async move {
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

        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        DefaultMQPushConsumerImpl::try_reset_pop_retry_topic(&mut self.msgs, self.consumer_group.as_str());
        default_mqpush_consumer_impl.reset_retry_and_namespace(&mut self.msgs, self.consumer_group.as_str());

        let mut consume_message_context = None;

        let has_hook = default_mqpush_consumer_impl.has_hook();

        let begin_timestamp = Instant::now();
        let mut has_exception = false;
        let mut status = None;

        if !self.msgs.is_empty() {
            let start_ts = CheetahString::from_string(current_millis().to_string());
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(msg.as_mut(), start_ts.clone());
            }

            if has_hook {
                consume_message_context = Some(ConsumeMessageContext {
                    consumer_group: self.consumer_group.clone(),
                    msg_list: &self.msgs,
                    mq: Some(self.message_queue.clone()),
                    success: false,
                    status: CheetahString::new(),
                    mq_trace_context: None,
                    props: Default::default(),
                    namespace: default_mqpush_consumer_impl
                        .client_config
                        .get_namespace()
                        .unwrap_or_default(),
                    access_channel: Default::default(),
                });
                default_mqpush_consumer_impl.execute_hook_before(consume_message_context.as_ref().unwrap());
            }

            let listener = self.message_listener.clone();
            let msgs_for_blocking = self.msgs.clone();
            let group_for_err = self.consumer_group.clone();

            let (blocking_status, blocking_has_exception, returned_context) = tokio::task::spawn_blocking(move || {
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

        let return_type = if let Some(s) = status {
            if consume_rt > default_mqpush_consumer_impl.consumer_config.consume_timeout * 60 * 1000 {
                ConsumeReturnType::TimeOut
            } else if s == ConsumeConcurrentlyStatus::ReconsumeLater {
                ConsumeReturnType::Failed
            } else {
                ConsumeReturnType::Success
            }
        } else if has_exception {
            ConsumeReturnType::Exception
        } else {
            ConsumeReturnType::ReturnNull
        };

        if has_hook {
            consume_message_context.as_mut().unwrap().props.insert(
                CheetahString::from_static_str(mix_all::CONSUME_CONTEXT_TYPE),
                return_type.to_string().into(),
            );
        }

        if status.is_none() {
            warn!(
                "consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                self.consumer_group,
                self.msgs.len(),
                self.message_queue
            );
            status = Some(ConsumeConcurrentlyStatus::ReconsumeLater);
        }

        if has_hook {
            let cmc = consume_message_context.as_mut().unwrap();
            let s = status.unwrap();
            cmc.status = s.to_string().into();
            cmc.success = s == ConsumeConcurrentlyStatus::ConsumeSuccess;
            cmc.access_channel = Some(default_mqpush_consumer_impl.client_config.access_channel);
            default_mqpush_consumer_impl.execute_hook_after(consume_message_context.as_ref().unwrap());
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
                .process_consume_result(this, status.unwrap(), &context, self)
                .await;
        }
    }
}
