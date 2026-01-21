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

use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
use crate::consumer::consumer_impl::process_queue::ProcessQueue;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use crate::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use crate::consumer::listener::message_listener_orderly::ArcBoxMessageListenerOrderly;
use crate::consumer::message_queue_lock::MessageQueueLock;

pub struct ConsumeMessagePopOrderlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerOrderly,
    pub(crate) consume_runtime: RocketMQRuntime,
    pub(self) consume_request_set: HashSet<ConsumeRequest>,
    pub(crate) message_queue_lock: MessageQueueLock,
    pub(crate) consume_request_lock: MessageQueueLock,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) active_tasks: Arc<AtomicUsize>,
    pub(crate) lock_refresh_task: Option<tokio::task::JoinHandle<()>>,
}

impl ConsumeMessagePopOrderlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerOrderly,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let consume_thread = consumer_config.consume_thread_max;
        let consumer_group_tag = format!("{}_{}", "PopConsumeMessageThread_", consumer_group);
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            consume_runtime: RocketMQRuntime::new_multi(consume_thread as usize, consumer_group_tag.as_str()),
            consume_request_set: Default::default(),
            message_queue_lock: Default::default(),
            consume_request_lock: Default::default(),
            stopped: Arc::new(AtomicBool::new(false)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            lock_refresh_task: None,
        }
    }

    fn remove_consume_request(&mut self, request: &ConsumeRequest) {
        self.consume_request_set.remove(request);
    }

    fn submit_consume_request(&mut self, this: ArcMut<Self>, mut request: ConsumeRequest, force: bool) {
        let stopped = self.stopped.clone();
        let active_tasks = self.active_tasks.clone();

        tokio::spawn(async move {
            if stopped.load(Ordering::Acquire) {
                return;
            }

            active_tasks.fetch_add(1, Ordering::SeqCst);

            struct TaskGuard(Arc<AtomicUsize>);
            impl Drop for TaskGuard {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _guard = TaskGuard(active_tasks);

            request.run(this).await;
        });
    }

    fn submit_consume_request_later(&self, this: ArcMut<Self>, request: ConsumeRequest, mut suspend_time_millis: u64) {
        suspend_time_millis = suspend_time_millis.clamp(10, 30_000);

        let stopped = self.stopped.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(suspend_time_millis)).await;

            if stopped.load(Ordering::Acquire) {
                return;
            }

            this.mut_from_ref().submit_consume_request(this.clone(), request, true);
        });
    }

    async fn unlock_all_message_queues(&self) {
        if let Some(ref impl_) = self.default_mqpush_consumer_impl {
            use crate::consumer::consumer_impl::re_balance::Rebalance;
            impl_.rebalance_impl.mut_from_ref().unlock_all(false);
        }
    }

    fn get_max_reconsume_times(&self) -> i32 {
        if self.consumer_config.max_reconsume_times == -1 {
            i32::MAX
        } else {
            self.consumer_config.max_reconsume_times
        }
    }

    async fn check_reconsume_times(&self, msgs: &[ArcMut<MessageExt>]) -> bool {
        let mut suspend = false;
        let max_times = self.get_max_reconsume_times();

        for msg in msgs {
            let msg_mut = msg.mut_from_ref();
            if msg_mut.reconsume_times >= max_times && !self.send_message_back(msg.as_ref()).await {
                suspend = true;
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

        let mut new_msg = Message::new(&retry_topic, &body);

        new_msg.set_delay_time_level(3 + msg.reconsume_times);

        let properties = msg.properties();
        new_msg.set_properties(properties.clone());

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
            warn!(
                "change_invisible_time not fully implemented, msg: {:?}, time: {}",
                msg, invisible_time
            );
        }
    }

    async fn process_consume_result(
        &self,
        msgs: &[ArcMut<MessageExt>],
        status: Result<ConsumeOrderlyStatus, rocketmq_error::RocketMQError>,
        _context: &ConsumeOrderlyContext,
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
                for msg in msgs {
                    self.change_invisible_time(msg.as_ref(), 1000).await;
                }
                false
            }
            #[allow(deprecated)]
            ConsumeOrderlyStatus::Rollback | ConsumeOrderlyStatus::Commit => {
                warn!("Deprecated status: {:?}, treating as NACK", status);
                for msg in msgs {
                    self.change_invisible_time(msg.as_ref(), 1000).await;
                }
                false
            }
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopOrderlyService {
    fn start(&mut self, this: ArcMut<Self>) {
        use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

        if self.consumer_config.message_model != MessageModel::Clustering {
            return;
        }

        let stopped = self.stopped.clone();
        let default_mqpush_consumer_impl = match &self.default_mqpush_consumer_impl {
            Some(impl_) => ArcMut::downgrade(impl_),
            None => return,
        };

        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let mut interval = tokio::time::interval(Duration::from_millis(20_000));

            loop {
                interval.tick().await;

                if stopped.load(Ordering::Acquire) {
                    break;
                }

                if let Some(impl_) = default_mqpush_consumer_impl.upgrade() {
                    use crate::consumer::consumer_impl::re_balance::Rebalance;
                    impl_.rebalance_impl.lock_all();
                } else {
                    break;
                }
            }
        });

        self.lock_refresh_task = Some(handle);
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

        info!(
            "{} ConsumeMessagePopOrderlyService shutdown started",
            self.consumer_group
        );

        self.stopped.store(true, Ordering::Release);

        if let Some(task) = self.lock_refresh_task.take() {
            task.abort();
        }

        let timeout = Duration::from_millis(await_terminate_millis);
        let start_time = Instant::now();

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
        unimplemented!("ConsumeMessagePopOrderlyService not support submit_consume_request")
    }

    async fn submit_pop_consume_request(
        &self,
        this: ArcMut<Self>,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        let request = ConsumeRequest::new(Arc::new(process_queue.clone()), message_queue.clone());
        this.mut_from_ref().submit_consume_request(this.clone(), request, false);
    }
}

#[derive(Clone)]
struct ConsumeRequest {
    process_queue: Arc<PopProcessQueue>,
    message_queue: MessageQueue,
    sharding_key_index: i32,
}

impl ConsumeRequest {
    pub fn new(process_queue: Arc<PopProcessQueue>, message_queue: MessageQueue) -> Self {
        Self {
            process_queue,
            message_queue,
            sharding_key_index: 0,
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

        let msgs: Vec<ArcMut<MessageExt>> = vec![];

        if msgs.is_empty() {
            consume_message_pop_orderly_service.submit_consume_request_later(
                consume_message_pop_orderly_service.clone(),
                self.clone(),
                1000,
            );
            return;
        }

        let suspend = consume_message_pop_orderly_service.check_reconsume_times(&msgs).await;
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

        let mut context = ConsumeOrderlyContext::new(self.message_queue.clone());
        let msg_refs: Vec<&MessageExt> = msgs.iter().map(|msg| msg.as_ref()).collect();
        let status = consume_message_pop_orderly_service
            .message_listener
            .consume_message(&msg_refs, &mut context);

        let continue_consume = consume_message_pop_orderly_service
            .process_consume_result(&msgs, status, &context)
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
        self.sharding_key_index == other.sharding_key_index && Arc::ptr_eq(&self.process_queue, &other.process_queue)
            || (self.message_queue.eq(&other.message_queue))
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
