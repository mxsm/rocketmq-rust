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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use once_cell::sync::Lazy;
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
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_rust::RocketMQTokioMutex;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::consumer_impl::process_queue::REBALANCE_LOCK_INTERVAL;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use crate::consumer::listener::consume_return_type::ConsumeReturnType;
use crate::consumer::listener::message_listener_orderly::ArcBoxMessageListenerOrderly;
use crate::consumer::message_queue_lock::MessageQueueLock;
use crate::consumer::mq_consumer_inner::MQConsumerInnerLocal;
use crate::hook::consume_message_context::ConsumeMessageContext;
use crate::producer::mq_producer::MQProducer;

static MAX_TIME_CONSUME_CONTINUOUSLY: Lazy<u64> = Lazy::new(|| {
    std::env::var("rocketmq.client.maxTimeConsumeContinuously")
        .unwrap_or("60000".to_string())
        .parse()
        .unwrap_or(60000)
});

pub struct ConsumeMessageOrderlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerOrderly,
    pub(crate) consume_runtime: RocketMQRuntime,
    pub(crate) stopped: AtomicBool,
    pub(crate) global_lock: Arc<RocketMQTokioMutex<()>>,
    pub(crate) message_queue_lock: MessageQueueLock,
}

impl ConsumeMessageOrderlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerOrderly,
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
            consume_runtime: RocketMQRuntime::new_multi(consume_thread as usize, consumer_group_tag.as_str()),
            stopped: AtomicBool::new(false),
            global_lock: Arc::new(Default::default()),
            message_queue_lock: Default::default(),
        }
    }

    pub async fn lock_mqperiodically(&mut self) {
        let lock = self.global_lock.lock().await;
        if self.stopped.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }

        self.default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .rebalance_impl
            .rebalance_impl_inner
            .lock_all()
            .await;

        drop(lock);
    }

    pub async fn unlock_all_mq(&mut self) {
        let lock = self.global_lock.lock().await;

        self.default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .rebalance_impl
            .rebalance_impl_inner
            .unlock_all(false)
            .await;

        drop(lock);
    }

    pub async fn try_lock_later_and_reconsume(
        &mut self,
        mut consume_message_orderly_service: ArcMut<Self>,
        message_queue: &MessageQueue,
        process_queue: Arc<ProcessQueue>,
        delay_mills: u64,
    ) {
        let consume_message_orderly_service_cloned = consume_message_orderly_service.clone();
        let message_queue = message_queue.clone();
        self.consume_runtime.get_handle().spawn(async move {
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
        });
    }

    pub async fn lock_one_mq(&self, message_queue: &MessageQueue) -> bool {
        if self.stopped.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }

        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .rebalance_impl
            .rebalance_impl_inner
            .lock(message_queue)
            .await
    }

    fn submit_consume_request_later(
        &mut self,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        suspend_time_millis: i64,
        this: ArcMut<Self>,
    ) {
        let mut time_millis = suspend_time_millis;
        if time_millis == -1 {
            time_millis = self
                .default_mqpush_consumer_impl
                .as_mut()
                .unwrap()
                .consumer_config
                .suspend_current_queue_time_millis as i64
        }

        time_millis = time_millis.clamp(10, 30000);

        let delay = Duration::from_millis(time_millis as u64);

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            // Call the submit_consume_request function here
            // ConsumeMessageOrderlyService::submit_consume_request(None, process_queue_clone,
            // message_queue_clone, true).await;
            let this_ = this.clone();

            this.submit_consume_request(this_, vec![], process_queue, message_queue, true)
                .await;
        });
    }

    #[inline]
    fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            i32::MAX
        } else {
            self.consumer_config.max_reconsume_times
        }
    }

    pub async fn send_message_back(&mut self, msg: &MessageExt) -> bool {
        let mut new_msg = Message::new(
            mix_all::get_retry_topic(self.consumer_group.as_str()),
            msg.get_body().unwrap(),
        );
        MessageAccessor::set_properties(&mut new_msg, msg.get_properties().clone());
        let origin_msg_id = MessageAccessor::get_origin_message_id(msg).unwrap_or(msg.msg_id.clone());
        MessageAccessor::set_origin_message_id(&mut new_msg, origin_msg_id);
        new_msg.set_flag(msg.get_flag());
        MessageAccessor::put_property(
            &mut new_msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_RETRY_TOPIC),
            msg.get_topic().to_owned(),
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
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();

        let result = default_mqpush_consumer_impl
            .client_instance
            .as_mut()
            .unwrap()
            .default_producer
            .send(new_msg)
            .await;
        result.is_ok()
    }

    async fn check_reconsume_times(&mut self, msgs: &mut [ArcMut<MessageExt>]) -> bool {
        let mut suspend = false;
        if !msgs.is_empty() {
            for msg in msgs {
                let reconsume_times = msg.reconsume_times;
                if reconsume_times >= self.get_max_reconsume_times() {
                    MessageAccessor::set_reconsume_time(
                        msg.as_mut(),
                        CheetahString::from_string(reconsume_times.to_string()),
                    );
                    if !self.send_message_back(msg).await {
                        suspend = true;
                        msg.reconsume_times = reconsume_times + 1;
                    }
                } else {
                    suspend = true;
                    msg.reconsume_times = reconsume_times + 1;
                }
            }
        }
        suspend
    }

    #[allow(deprecated)]
    async fn process_consume_result(
        &mut self,
        mut msgs: Vec<ArcMut<MessageExt>>,
        this: ArcMut<Self>,
        status: ConsumeOrderlyStatus,
        context: &ConsumeOrderlyContext,
        consume_request: &mut ConsumeRequest,
    ) -> bool {
        let (continue_consume, commit_offset) = if context.is_auto_commit() {
            match status {
                ConsumeOrderlyStatus::Success | ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::Commit => {
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
                ConsumeOrderlyStatus::Rollback => (true, consume_request.process_queue.commit().await),
                ConsumeOrderlyStatus::Commit => {
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

        if commit_offset >= 0 && consume_request.process_queue.is_dropped() {
            let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_ref().unwrap().clone();

            default_mqpush_consumer_impl
                .offset_store
                .as_mut()
                .unwrap()
                .update_offset(&consume_request.message_queue, commit_offset, false)
                .await;
        }
        continue_consume
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessageOrderlyService {
    fn start(&mut self, mut this: ArcMut<Self>) {
        if MessageModel::Clustering == self.consumer_config.message_model {
            self.consume_runtime.get_handle().spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;
                loop {
                    this.lock_mqperiodically().await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(*REBALANCE_LOCK_INTERVAL)).await;
                }
            });
        }
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        if MessageModel::Clustering == self.consumer_config.message_model {
            self.unlock_all_mq().await;
        }
    }

    #[allow(deprecated)]
    async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        info!("consumeMessageDirectly receive new message: {}", msg);
        let mq = MessageQueue::from_parts(msg.topic().clone(), broker_name.unwrap_or_default(), msg.queue_id());
        let mut msgs = vec![ArcMut::new(msg)];
        let mut context = ConsumeOrderlyContext::new(mq);
        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());

        let begin_timestamp = Instant::now();

        let status = self.message_listener.consume_message(
            &msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>()[..],
            &mut context,
        );
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
        if !dispatch_to_consume {
            return;
        }
        let mut consume_request = ConsumeRequest {
            process_queue,
            message_queue,
            default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
            consumer_group: self.consumer_group.clone(),
        };
        self.consume_runtime.get_handle().spawn(async move {
            consume_request.run(this).await;
        });
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
    process_queue: Arc<ProcessQueue>,
    message_queue: MessageQueue,
    default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    consumer_group: CheetahString,
}

impl ConsumeRequest {
    #[allow(deprecated)]
    async fn run(&mut self, consume_message_orderly_service: ArcMut<ConsumeMessageOrderlyService>) {
        if self.process_queue.is_dropped() {
            warn!(
                "run, the message queue not be able to consume, because it's dropped. {}",
                self.message_queue
            );
            return;
        }

        let mut consume_message_orderly_service_inner = consume_message_orderly_service.clone();
        let lock = consume_message_orderly_service_inner
            .message_queue_lock
            .fetch_lock_object(&self.message_queue)
            .await;
        let locked = lock.lock().await;
        let mut default_mqpush_consumer_impl = self.default_mqpush_consumer_impl.as_mut().unwrap().clone();
        if MessageModel::Broadcasting == default_mqpush_consumer_impl.message_model()
            || self.process_queue.is_locked() && !self.process_queue.is_lock_expired()
        {
            let begin_time = Instant::now();
            loop {
                if self.process_queue.is_dropped() {
                    warn!(
                        "the message queue not be able to consume, because it's dropped. {}",
                        self.message_queue
                    );
                    break;
                }
                if MessageModel::Clustering == default_mqpush_consumer_impl.message_model()
                    && !self.process_queue.is_locked()
                {
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

                if MessageModel::Clustering == default_mqpush_consumer_impl.message_model()
                    && self.process_queue.is_lock_expired()
                {
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
                let consume_batch_size = consume_message_orderly_service_inner
                    .consumer_config
                    .consume_message_batch_max_size;
                let mut msgs = self.process_queue.take_messages(consume_batch_size).await;
                default_mqpush_consumer_impl.reset_retry_and_namespace(
                    &mut msgs,
                    consume_message_orderly_service_inner.consumer_group.as_ref(),
                );
                if msgs.is_empty() {
                    break;
                }
                let mut context = ConsumeOrderlyContext::new(self.message_queue.clone());
                let mut consume_message_context = None;
                let mut status = None;
                if default_mqpush_consumer_impl.has_hook() {
                    let queue = self.message_queue.clone();
                    consume_message_context = Some(ConsumeMessageContext {
                        consumer_group: self.consumer_group.clone(),
                        msg_list: &msgs,
                        mq: Some(queue),
                        success: false,
                        status: CheetahString::from_static_str(""),
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
                let begin_timestamp = Instant::now();
                let mut has_exception = false;
                let consume_lock = self.process_queue.consume_lock.write().await;
                if self.process_queue.is_dropped() {
                    warn!(
                        "consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                        self.message_queue
                    );
                    break;
                }
                let vec = &msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>()[..];

                match consume_message_orderly_service_inner
                    .message_listener
                    .consume_message(vec, &mut context)
                {
                    Ok(value) => {
                        status = Some(value);
                    }
                    Err(_) => {
                        has_exception = true;
                    }
                }
                drop(consume_lock);
                if status.is_none()
                    || *status.as_ref().unwrap() == ConsumeOrderlyStatus::Rollback
                    || *status.as_ref().unwrap() == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment
                {
                    warn!(
                        "consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                        self.consumer_group,
                        msgs.len(),
                        self.message_queue,
                    );
                }
                let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
                let return_type = match status {
                    None => {
                        if has_exception {
                            ConsumeReturnType::Exception
                        } else {
                            ConsumeReturnType::ReturnNull
                        }
                    }
                    Some(status_value) => {
                        if consume_rt >= default_mqpush_consumer_impl.consumer_config.consume_timeout * 60 * 1000 {
                            ConsumeReturnType::TimeOut
                        } else if status_value == ConsumeOrderlyStatus::SuspendCurrentQueueAMoment {
                            ConsumeReturnType::Failed
                        } else if status_value == ConsumeOrderlyStatus::Success {
                            ConsumeReturnType::Success
                        } else {
                            // Handle other status cases
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
                    status = Some(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
                }
                if default_mqpush_consumer_impl.has_hook() {
                    let status = *status.as_ref().unwrap();
                    consume_message_context.as_mut().unwrap().success =
                        status == ConsumeOrderlyStatus::Success || status == ConsumeOrderlyStatus::Commit;
                    consume_message_context.as_mut().unwrap().status = status.to_string().into();
                    default_mqpush_consumer_impl.execute_hook_after(&mut consume_message_context);
                }
                let continue_consume = consume_message_orderly_service_inner
                    .process_consume_result(
                        msgs,
                        consume_message_orderly_service.clone(),
                        status.unwrap(),
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
