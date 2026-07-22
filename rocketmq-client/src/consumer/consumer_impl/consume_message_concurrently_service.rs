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

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use tracing::info;
use tracing::warn;
use tokio::task::JoinHandle;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::consume_message_service::ShutdownReport;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use crate::consumer::listener::consume_return_type::ConsumeReturnType;
use crate::consumer::listener::message_listener_concurrently::ArcBoxMessageListenerConcurrently;
use crate::hook::consume_message_context::ConsumeMessageContext;

pub struct ConsumeMessageConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerConcurrently,
    pub(crate) consume_runtime: Option<RocketMQRuntime>,
    accepting: AtomicBool,
    cleaner_task: Mutex<Option<JoinHandle<()>>>,
    consume_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl ConsumeMessageConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let consume_thread = consumer_config.consume_thread_max;
        let consumer_group_tag = format!("{}_{}", "ConsumeMessageThread_", consumer_group);
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            consume_runtime: Some(RocketMQRuntime::new_multi(
                consume_thread as usize,
                consumer_group_tag.as_str(),
            )),
            accepting: AtomicBool::new(true),
            cleaner_task: Mutex::new(None),
            consume_tasks: Mutex::new(Vec::new()),
        }
    }
}

impl ConsumeMessageConcurrentlyService {
    fn track_consume_task(&self, task: JoinHandle<()>) {
        let mut tasks = self.consume_tasks.lock().expect("consume task registry poisoned");
        tasks.retain(|existing| !existing.is_finished());
        tasks.push(task);
    }

    fn reject_request(&self, process_queue: &ProcessQueue) {
        process_queue.set_dropped(true);
        warn!("rejecting consume request because consumer shutdown has started");
    }

    async fn clean_expire_msg(&mut self) {
        let default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.clone().unwrap();

        let process_queues = {
            let process_queue_table = default_mqpush_consumer_impl
                .rebalance_impl
                .rebalance_impl_inner
                .process_queue_table
                .read()
                .await;
            process_queue_table.values().cloned().collect::<Vec<_>>()
        };
        for process_queue in process_queues {
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

        match self.consumer_config.message_model {
            MessageModel::Broadcasting => {
                for i in ((ack_index + 1) as usize)..consume_request.msgs.len() {
                    warn!("BROADCASTING, the message consume failed, drop it");
                }
            }
            MessageModel::Clustering => {
                let len = consume_request.msgs.len();
                let mut msg_back_failed = Vec::with_capacity(len);
                let mut msg_back_success = Vec::with_capacity(len);
                let failed_msgs = consume_request.msgs.split_off((ack_index + 1) as usize);
                for mut msg in failed_msgs {
                    if !consume_request.process_queue.contains_message(&msg).await {
                        /*info!("Message is not found in its process queue; skip send-back-procedure, topic={}, "
                            + "brokerName={}, queueId={}, queueOffset={}", msg.get_topic(), msg.get_broker_name(),
                        msg.getQueueId(), msg.getQueueOffset());*/
                        continue;
                    }

                    let result = self.send_message_back(&mut msg, context).await;
                    if !result {
                        let reconsume_times = msg.reconsume_times() + 1;
                        msg.set_reconsume_times(reconsume_times);
                        msg_back_failed.push(msg);
                    } else {
                        msg_back_success.push(msg);
                    }
                }
                if !msg_back_failed.is_empty() {
                    consume_request.msgs.append(&mut msg_back_success);
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
        if !self.accepting.load(Ordering::Acquire) {
            self.reject_request(process_queue.as_ref());
            return;
        }
        if let Some(runtime) = self.consume_runtime.as_ref() {
            let task = runtime.get_handle().spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let this_ = this.clone();

                this.submit_consume_request(this_, msgs, process_queue, message_queue, true)
                    .await;
            });
            self.track_consume_task(task);
        }
    }

    pub async fn send_message_back(&mut self, msg: &mut MessageExt, context: &ConsumeConcurrentlyContext) -> bool {
        let delay_level = context.delay_level_when_next_consume;
        msg.set_topic(self.client_config.with_namespace(msg.get_topic().as_str()));

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
        if !self.accepting.load(Ordering::Acquire) {
            return;
        }
        if let Some(runtime) = self.consume_runtime.as_ref() {
            let task = runtime.get_handle().spawn(async move {
                let timeout = this.consumer_config.consume_timeout;
                let mut interval = tokio::time::interval(Duration::from_secs(timeout * 60));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    this.clean_expire_msg().await;
                }
            });
            *self.cleaner_task.lock().expect("cleaner task registry poisoned") = Some(task);
        }
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) -> ShutdownReport {
        let started = Instant::now();
        let mut report = ShutdownReport::default();
        if !self.accepting.swap(false, Ordering::AcqRel) {
            return report;
        }
        if let Some(cleaner) = self.cleaner_task.lock().expect("cleaner task registry poisoned").take() {
            cleaner.abort();
        }
        if let Some(consumer) = self.default_mqpush_consumer_impl.as_ref() {
            let queues = {
                let table = consumer
                    .rebalance_impl
                    .rebalance_impl_inner
                    .process_queue_table
                    .read()
                    .await;
                table.values().cloned().collect::<Vec<_>>()
            };
            for queue in queues {
                queue.set_dropped(true);
            }
        }
        let mut tasks = std::mem::take(&mut *self.consume_tasks.lock().expect("consume task registry poisoned"));
        let budget = Duration::from_millis(await_terminate_millis);
        for mut task in tasks.drain(..) {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() || tokio::time::timeout(remaining, &mut task).await.is_err() {
                task.abort();
                report.consume_tasks_aborted += 1;
            } else {
                report.consume_tasks_drained += 1;
            }
        }
        if let Some(runtime) = self.consume_runtime.take() {
            let timeout = budget.saturating_sub(started.elapsed());
            // Tokio forbids dropping a Runtime from another runtime's async
            // context. Move the private consume runtime to a plain thread so
            // its bounded shutdown cannot panic the public consumer teardown.
            let _ = std::thread::spawn(move || runtime.shutdown_timeout(timeout)).join();
        }
        report.elapsed = started.elapsed();
        report
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
        let context = ConsumeConcurrentlyContext::new(mq);
        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());

        let begin_timestamp = Instant::now();

        let status = self.message_listener.consume_message(
            &msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>(),
            &context,
        );
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
        let consume_batch_size = self.consumer_config.consume_message_batch_max_size;
        if !self.accepting.load(Ordering::Acquire) {
            self.reject_request(process_queue.as_ref());
            return;
        }
        if msgs.len() <= consume_batch_size as usize {
            let mut consume_request = ConsumeRequest {
                msgs,
                message_listener: self.message_listener.clone(),
                process_queue,
                message_queue,
                dispatch_to_consume,
                consumer_group: self.consumer_group.clone(),
                default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
            };

            if let Some(runtime) = self.consume_runtime.as_ref() {
                let task = runtime
                    .get_handle()
                    .spawn(async move { consume_request.run(this).await });
                self.track_consume_task(task);
            }
        } else {
            msgs.chunks(consume_batch_size as usize)
                .map(|t| t.to_vec())
                .for_each(|msgs| {
                    let mut consume_request = ConsumeRequest {
                        msgs,
                        message_listener: self.message_listener.clone(),
                        process_queue: process_queue.clone(),
                        message_queue: message_queue.clone(),
                        dispatch_to_consume,
                        consumer_group: self.consumer_group.clone(),
                        default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
                    };
                    let consume_message_concurrently_service = this.clone();
                    if self.accepting.load(Ordering::Acquire) {
                        if let Some(runtime) = self.consume_runtime.as_ref() {
                            let task = runtime
                                .get_handle()
                                .spawn(async move { consume_request.run(consume_message_concurrently_service).await });
                            self.track_consume_task(task);
                        }
                    }
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

struct ConsumeRequest {
    msgs: Vec<ArcMut<MessageExt>>,
    message_listener: ArcBoxMessageListenerConcurrently,
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
        let context = ConsumeConcurrentlyContext {
            message_queue: self.message_queue.clone(),
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        };

        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        let consumer_group = self.consumer_group.clone();
        DefaultMQPushConsumerImpl::try_reset_pop_retry_topic(&mut self.msgs, consumer_group.as_str());
        default_mqpush_consumer_impl.reset_retry_and_namespace(&mut self.msgs, consumer_group.as_str());

        let mut consume_message_context = None;

        let begin_timestamp = Instant::now();
        let mut has_exception = false;

        let mut status = None;

        if !self.msgs.is_empty() {
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(
                    msg.as_mut(),
                    CheetahString::from_string(get_current_millis().to_string()),
                );
            }
            if default_mqpush_consumer_impl.has_hook() {
                let queue = self.message_queue.clone();
                consume_message_context = Some(ConsumeMessageContext {
                    consumer_group,
                    msg_list: &self.msgs,
                    mq: Some(queue),
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
                default_mqpush_consumer_impl.execute_hook_before(&mut consume_message_context);
            }
            let vec = self.msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>();
            match self.message_listener.consume_message(&vec, &context) {
                Ok(value) => {
                    status = Some(value);
                }
                Err(_) => {
                    has_exception = true;
                }
            }
        }

        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;

        let return_type = if let Some(s) = status {
            if consume_rt > default_mqpush_consumer_impl.consumer_config.consume_timeout * 60 * 1000 {
                ConsumeReturnType::TimeOut
            } else if s == ConsumeConcurrentlyStatus::ReconsumeLater {
                ConsumeReturnType::Failed
            } else {
                // Must be ConsumeSuccess
                ConsumeReturnType::Success
            }
        } else if has_exception {
            ConsumeReturnType::Exception
        } else {
            ConsumeReturnType::ReturnNull
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
            default_mqpush_consumer_impl.execute_hook_after(&mut consume_message_context);
        }

        if self.process_queue.is_dropped() {
            warn!(
                "the message queue not be able to consume, because it's dropped. group={} {}",
                self.consumer_group, self.message_queue,
            );
        } else {
            let this = consume_message_concurrently_service.clone();

            consume_message_concurrently_service
                .process_consume_result(this, status.unwrap(), &context, self)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;

    struct NoopListener;

    impl MessageListenerConcurrently for NoopListener {
        fn consume_message(
            &self,
            _msgs: &[&MessageExt],
            _context: &ConsumeConcurrentlyContext,
        ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus> {
            Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
        }
    }

    #[tokio::test]
    async fn shutdown_stops_accepting_and_takes_the_private_runtime() {
        let listener: ArcBoxMessageListenerConcurrently = Arc::new(Box::new(NoopListener));
        let mut service = ConsumeMessageConcurrentlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(ConsumerConfig::default()),
            CheetahString::from_static_str("shutdown-test"),
            listener,
            None,
        );

        service.shutdown(1).await;
        assert!(!service.accepting.load(Ordering::Acquire));
        assert!(service.consume_runtime.is_none());
        service.shutdown(1).await;
        assert!(service.consume_runtime.is_none());
    }

    #[tokio::test]
    async fn shutdown_aborts_tracked_tasks_at_its_deadline() {
        let listener: ArcBoxMessageListenerConcurrently = Arc::new(Box::new(NoopListener));
        let mut service = ConsumeMessageConcurrentlyService::new(
            ArcMut::new(ClientConfig::default()),
            ArcMut::new(ConsumerConfig::default()),
            CheetahString::from_static_str("shutdown-deadline-test"),
            listener,
            None,
        );
        let task = service
            .consume_runtime
            .as_ref()
            .unwrap()
            .get_handle()
            .spawn(std::future::pending());
        service.track_consume_task(task);

        let report = service.shutdown(10).await;

        assert_eq!(report.consume_tasks_drained, 0);
        assert_eq!(report.consume_tasks_aborted, 1);
        assert!(service.consume_runtime.is_none());
        assert!(report.elapsed < Duration::from_secs(1));
    }
}
