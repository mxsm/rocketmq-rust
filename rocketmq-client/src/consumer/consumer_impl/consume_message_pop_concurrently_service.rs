/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_rust::ArcMut;
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
use crate::consumer::listener::message_listener_concurrently::ArcBoxMessageListenerConcurrently;
use crate::hook::consume_message_context::ConsumeMessageContext;

pub struct ConsumeMessagePopConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerConcurrently,
}

impl ConsumeMessagePopConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group,
            message_listener,
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopConcurrentlyService {
    fn start(&mut self, this: ArcMut<Self>) {
        //todo!()
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        todo!()
    }

    fn update_core_pool_size(&self, core_pool_size: usize) {
        todo!()
    }

    fn inc_core_pool_size(&self) {
        todo!()
    }

    fn dec_core_pool_size(&self) {
        todo!()
    }

    fn get_core_pool_size(&self) -> usize {
        todo!()
    }

    async fn consume_message_directly(
        &self,
        msg: MessageExt,
        broker_name: Option<CheetahString>,
    ) -> ConsumeMessageDirectlyResult {
        info!("consumeMessageDirectly receive new message: {}", msg);

        let mq = MessageQueue::from_parts(
            msg.topic().clone(),
            broker_name.unwrap_or_default(),
            msg.queue_id(),
        );
        let mut msgs = vec![ArcMut::new(MessageClientExt::new(msg))];
        let context = ConsumeConcurrentlyContext::new(mq);
        self.default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .reset_retry_and_namespace(msgs.as_mut_slice(), self.consumer_group.as_str());

        let begin_timestamp = Instant::now();

        let status = self.message_listener.consume_message(
            &msgs
                .iter()
                .map(|msg| &msg.message_ext_inner)
                .collect::<Vec<&MessageExt>>(),
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
        msgs: Vec<ArcMut<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        unimplemented!(
            "ConsumeMessagePopConcurrentlyService.submit_consume_request is not supported"
        )
    }

    async fn submit_pop_consume_request(
        &self,
        msgs: Vec<MessageExt>,
        process_queue: &PopProcessQueue,
        message_queue: &MessageQueue,
    ) {
        todo!()
    }
}

impl ConsumeMessagePopConcurrentlyService {
    async fn process_consume_result(
        &mut self,
        this: ArcMut<Self>,
        status: ConsumeConcurrentlyStatus,
        context: &ConsumeConcurrentlyContext,
        consume_request: &mut ConsumeRequest,
    ) {
        unimplemented!("ConsumeMessagePopConcurrentlyService.process_consume_result")
    }
}

struct ConsumeRequest {
    msgs: Vec<ArcMut<MessageClientExt>>,
    process_queue: Arc<PopProcessQueue>,
    message_queue: MessageQueue,
    pop_time: u64,
    invisible_time: u64,
    consumer_group: CheetahString,
    message_listener: ArcBoxMessageListenerConcurrently,
    default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
}

impl ConsumeRequest {
    pub fn new(
        msgs: Vec<ArcMut<MessageClientExt>>,
        process_queue: Arc<PopProcessQueue>,
        message_queue: MessageQueue,
        pop_time: u64,
        invisible_time: u64,
    ) -> Self {
        unimplemented!()
    }

    #[inline]
    pub fn is_pop_timeout(&self) -> bool {
        if self.msgs.is_empty() || self.pop_time == 0 || self.invisible_time == 0 {
            return true;
        }
        get_current_millis().saturating_sub(self.pop_time) >= self.invisible_time
    }

    pub async fn run(
        &mut self,
        mut consume_message_concurrently_service: ArcMut<ConsumeMessagePopConcurrentlyService>,
    ) {
        if self.process_queue.is_dropped() {
            info!(
                "the message queue not be able to consume, because it's dropped(pop). group={} {}",
                self.consumer_group, self.message_queue
            );
            return;
        }
        if self.is_pop_timeout() {
            info!(
                "the pop message time out so abort consume. popTime={} invisibleTime={}, group={} \
                 {}",
                self.pop_time, self.invisible_time, self.consumer_group, self.message_queue
            );
            self.process_queue.dec_found_msg(self.msgs.len());
            return;
        }
        let context = ConsumeConcurrentlyContext {
            message_queue: self.message_queue.clone(),
            delay_level_when_next_consume: 0,
            ack_index: i32::MAX,
        };

        let mut default_mqpush_consumer_impl =
            self.default_mqpush_consumer_impl.as_ref().unwrap().clone();
        default_mqpush_consumer_impl
            .reset_retry_and_namespace(&mut self.msgs, self.consumer_group.as_str());
        let mut consume_message_context = None;

        let begin_timestamp = Instant::now();
        let mut has_exception = false;
        let mut return_type = ConsumeReturnType::Success;
        let mut status = None;

        if !self.msgs.is_empty() {
            for msg in self.msgs.iter_mut() {
                MessageAccessor::set_consume_start_time_stamp(
                    &mut msg.message_ext_inner,
                    CheetahString::from_string(get_current_millis().to_string()),
                );
            }
        }

        if default_mqpush_consumer_impl.has_hook() {
            let queue = self.message_queue.clone();
            consume_message_context = Some(ConsumeMessageContext {
                consumer_group: self.consumer_group.clone(),
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
        let vec = self
            .msgs
            .iter()
            .map(|msg| &msg.message_ext_inner)
            .collect::<Vec<&MessageExt>>();
        match self.message_listener.consume_message(&vec, &context) {
            Ok(value) => {
                status = Some(value);
            }
            Err(_) => {
                has_exception = true;
            }
        }
        let consume_rt = begin_timestamp.elapsed().as_millis() as u64;
        if status.is_none() {
            if has_exception {
                return_type = ConsumeReturnType::Exception;
            } else {
                return_type = ConsumeReturnType::ReturnNull;
            }
        } else if consume_rt
            > default_mqpush_consumer_impl.consumer_config.consume_timeout * 60 * 1000
        {
            return_type = ConsumeReturnType::TimeOut;
        } else if status.unwrap() == ConsumeConcurrentlyStatus::ReconsumeLater {
            return_type = ConsumeReturnType::Failed;
        } else if status.unwrap() == ConsumeConcurrentlyStatus::ConsumeSuccess {
            return_type = ConsumeReturnType::Success;
        }

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
