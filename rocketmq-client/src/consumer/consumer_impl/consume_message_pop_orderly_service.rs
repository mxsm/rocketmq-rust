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

use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use dashmap::DashSet;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_rust::ArcMut;
use tokio::sync::Semaphore;
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
use crate::consumer::listener::message_listener_orderly::ArcBoxMessageListenerOrderly;
use crate::consumer::message_queue_lock::MessageQueueLock;

pub struct ConsumeMessagePopOrderlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerOrderly,
    pub(crate) concurrency_limiter: Arc<Semaphore>,
    pub(self) consume_request_set: Arc<DashSet<ConsumeRequest>>,
    pub(crate) message_queue_lock: MessageQueueLock,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) active_tasks: Arc<AtomicUsize>,
    pub(crate) lock_refresh_task: Option<tokio::task::JoinHandle<()>>,
}

/// Default callback implementation for acknowledgment operations
struct DefaultAckCallback;

impl crate::consumer::ack_callback::AckCallback for DefaultAckCallback {
    fn on_success(&self, _ack_result: AckResult) {}

    fn on_exception(&self, e: Box<dyn std::error::Error>) {
        error!("change_invisible_time callback exception: {}", e);
    }
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
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
            concurrency_limiter: Arc::new(Semaphore::new(consume_thread as usize)),
            consume_request_set: Arc::new(DashSet::new()),
            message_queue_lock: Default::default(),
            stopped: Arc::new(AtomicBool::new(false)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            lock_refresh_task: None,
        }
    }

    fn remove_consume_request(&mut self, request: &ConsumeRequest) {
        self.consume_request_set.remove(request);
    }

    fn submit_consume_request(&mut self, this: ArcMut<Self>, mut request: ConsumeRequest, force: bool) {
        if !force && !self.consume_request_set.insert(request.clone()) {
            return;
        }

        let stopped = self.stopped.clone();
        let active_tasks = self.active_tasks.clone();
        let concurrency_limiter = self.concurrency_limiter.clone();

        tokio::spawn(async move {
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
        });
    }

    fn submit_consume_request_later(&self, this: ArcMut<Self>, request: ConsumeRequest, mut suspend_time_millis: u64) {
        suspend_time_millis = suspend_time_millis.clamp(10, 30_000);

        let stopped = self.stopped.clone();
        let consume_request_set = self.consume_request_set.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(suspend_time_millis)).await;

            if stopped.load(Ordering::Acquire) {
                return;
            }

            consume_request_set.insert(request.clone());
            this.mut_from_ref().submit_consume_request(this.clone(), request, true);
        });
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

    async fn check_reconsume_times(&self, msgs: &[ArcMut<MessageExt>]) -> bool {
        let mut suspend = false;
        let max_times = self.get_max_reconsume_times();

        for msg in msgs {
            let msg_mut = msg.mut_from_ref();
            if msg_mut.reconsume_times >= max_times {
                if !self.send_message_back(msg.as_ref()).await {
                    suspend = true;
                    msg_mut.reconsume_times += 1;
                }
            } else {
                suspend = true;
                msg_mut.reconsume_times += 1;
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

        let mut new_msg = Message::builder()
            .topic(&retry_topic)
            .body(body.clone())
            .build_unchecked();

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

                if let Some(mut impl_) = default_mqpush_consumer_impl.upgrade() {
                    use crate::consumer::consumer_impl::re_balance::Rebalance;
                    impl_.rebalance_impl.lock_all().await;
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

        self.concurrency_limiter.close();

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
        let arc_msgs: Vec<ArcMut<MessageExt>> = msgs.into_iter().map(ArcMut::new).collect();
        let request = ConsumeRequest::new(Arc::new(process_queue.clone()), message_queue.clone(), arc_msgs);
        this.mut_from_ref().submit_consume_request(this.clone(), request, false);
    }
}

#[derive(Clone)]
struct ConsumeRequest {
    process_queue: Arc<PopProcessQueue>,
    message_queue: MessageQueue,
    sharding_key_index: i32,
    msgs: Vec<ArcMut<MessageExt>>,
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
    pub fn new(
        process_queue: Arc<PopProcessQueue>,
        message_queue: MessageQueue,
        msgs: Vec<ArcMut<MessageExt>>,
    ) -> Self {
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

        let msgs = &self.msgs;

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

        let mut context = ConsumeOrderlyContext::new(self.message_queue.clone());
        let msg_refs: Vec<&MessageExt> = msgs.iter().map(|msg| msg.as_ref()).collect();
        let status = consume_message_pop_orderly_service
            .message_listener
            .consume_message(&msg_refs, &mut context);

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

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
