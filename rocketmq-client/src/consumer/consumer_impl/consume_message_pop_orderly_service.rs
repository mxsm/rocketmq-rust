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
use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
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
        }
    }

    fn remove_consume_request(&mut self, request: &ConsumeRequest) {
        self.consume_request_set.remove(request);
    }

    async fn submit_consume_request(&mut self, this: ArcMut<Self>, mut request: ConsumeRequest, force: bool) {
        let lock = self
            .consume_request_lock
            .fetch_lock_object_with_sharding_key(&request.message_queue, request.sharding_key_index)
            .await;
        let _lock = lock.lock().await;
        let is_new_req = self.consume_request_set.insert(request.clone());
        if is_new_req || force {
            self.consume_runtime.get_handle().spawn(async move {
                request.run(this).await;
            });
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopOrderlyService {
    fn start(&mut self, this: ArcMut<Self>) {}

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        todo!()
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
        ConsumeRequest::new(Arc::new(process_queue.clone()), message_queue.clone());
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
        let _ = consume_message_pop_orderly_service
            .message_queue_lock
            .fetch_lock_object_with_sharding_key(&self.message_queue, self.sharding_key_index)
            .await;
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
