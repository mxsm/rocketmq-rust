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
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
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

pub struct ConsumeMessageConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<WeakArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: Arc<String>,
    pub(crate) message_listener: ArcBoxMessageListenerConcurrently,
    pub(crate) consume_runtime: RocketMQRuntime,
}

impl ConsumeMessageConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: String,
        message_listener: ArcBoxMessageListenerConcurrently,
        default_mqpush_consumer_impl: Option<WeakArcMut<DefaultMQPushConsumerImpl>>,
    ) -> Self {
        let consume_thread = consumer_config.consume_thread_max;
        let consumer_group_tag = format!("{}_{}", "ConsumeMessageThread_", consumer_group);
        Self {
            default_mqpush_consumer_impl,
            client_config,
            consumer_config,
            consumer_group: Arc::new(consumer_group),
            message_listener,
            consume_runtime: RocketMQRuntime::new_multi(
                consume_thread as usize,
                consumer_group_tag.as_str(),
            ),
        }
    }
}

impl ConsumeMessageConcurrentlyService {
    async fn clean_expire_msg(&mut self) {
        if let Some(default_mqpush_consumer_impl) = self
            .default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .upgrade()
        {
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
    }

    async fn process_consume_result(
        &mut self,
        this: WeakArcMut<Self>,
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
                    if !consume_request
                        .process_queue
                        .contains_message(&msg.message_ext_inner)
                        .await
                    {
                        /*info!("Message is not found in its process queue; skip send-back-procedure, topic={}, "
                            + "brokerName={}, queueId={}, queueOffset={}", msg.get_topic(), msg.get_broker_name(),
                        msg.getQueueId(), msg.getQueueOffset());*/
                        continue;
                    }

                    let result = self
                        .send_message_back(&mut msg.message_ext_inner, context)
                        .await;
                    if !result {
                        let reconsume_times = msg.message_ext_inner.reconsume_times() + 1;
                        msg.message_ext_inner.set_reconsume_times(reconsume_times);
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
            if let Some(mut default_mqpush_consumer_impl) = self
                .default_mqpush_consumer_impl
                .as_ref()
                .unwrap()
                .upgrade()
            {
                default_mqpush_consumer_impl
                    .offset_store
                    .as_mut()
                    .unwrap()
                    .update_offset(&consume_request.message_queue, offset, true)
                    .await;
            }
        }
    }

    fn submit_consume_request_later(
        &self,
        msgs: Vec<ArcMut<MessageClientExt>>,
        this: WeakArcMut<Self>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
    ) {
        self.consume_runtime.get_handle().spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let this_ = this.clone();
            if let Some(this) = this.upgrade() {
                this.submit_consume_request(this_, msgs, process_queue, message_queue, true)
                    .await;
            }
        });
    }

    pub async fn send_message_back(
        &mut self,
        msg: &mut MessageExt,
        context: &ConsumeConcurrentlyContext,
    ) -> bool {
        let delay_level = context.delay_level_when_next_consume;
        msg.set_topic(CheetahString::from_string(
            self.client_config.with_namespace(msg.get_topic().as_str()),
        ));
        match self
            .default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .upgrade()
        {
            None => false,
            Some(mut default_mqpush_consumer_impl) => default_mqpush_consumer_impl
                .send_message_back(
                    msg,
                    delay_level,
                    &self
                        .client_config
                        .queue_with_namespace(context.get_message_queue().clone()),
                )
                .await
                .is_ok(),
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessageConcurrentlyService {
    fn start(&mut self, this: WeakArcMut<Self>) {
        self.consume_runtime.get_handle().spawn(async move {
            if let Some(mut this) = this.upgrade() {
                let timeout = this.consumer_config.consume_timeout;
                let mut interval = tokio::time::interval(Duration::from_secs(timeout * 60));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    this.clean_expire_msg().await;
                }
            }
        });
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        // todo!()
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
        msg: &MessageExt,
        broker_name: &str,
    ) -> ConsumeMessageDirectlyResult {
        todo!()
    }

    async fn submit_consume_request(
        &self,
        this: WeakArcMut<Self>,
        msgs: Vec<ArcMut<MessageClientExt>>,
        process_queue: Arc<ProcessQueue>,
        message_queue: MessageQueue,
        dispatch_to_consume: bool,
    ) {
        let consume_batch_size = self.consumer_config.consume_message_batch_max_size;
        if msgs.len() <= consume_batch_size as usize {
            let mut consume_request = ConsumeRequest {
                msgs: msgs.clone(),
                message_listener: self.message_listener.clone(),
                process_queue,
                message_queue,
                dispatch_to_consume,
                consumer_group: self.consumer_group.as_ref().clone(),
                default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
            };

            self.consume_runtime
                .get_handle()
                .spawn(async move { consume_request.run(this).await });
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
                        consumer_group: self.consumer_group.as_ref().clone(),
                        default_mqpush_consumer_impl: self.default_mqpush_consumer_impl.clone(),
                    };
                    let consume_message_concurrently_service = this.clone();
                    self.consume_runtime.get_handle().spawn(async move {
                        consume_request
                            .run(consume_message_concurrently_service)
                            .await
                    });
                });
        }
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

struct ConsumeRequest {
    msgs: Vec<ArcMut<MessageClientExt>>,
    message_listener: ArcBoxMessageListenerConcurrently,
    process_queue: Arc<ProcessQueue>,
    message_queue: MessageQueue,
    dispatch_to_consume: bool,
    consumer_group: String,
    default_mqpush_consumer_impl: Option<WeakArcMut<DefaultMQPushConsumerImpl>>,
}

impl ConsumeRequest {
    async fn run(
        &mut self,
        consume_message_concurrently_service: WeakArcMut<ConsumeMessageConcurrentlyService>,
    ) {
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

        let default_mqpush_consumer_impl = self
            .default_mqpush_consumer_impl
            .as_ref()
            .unwrap()
            .upgrade();
        if default_mqpush_consumer_impl.is_none() {
            return;
        }
        let mut default_mqpush_consumer_impl = default_mqpush_consumer_impl.unwrap();
        let consumer_group = self.consumer_group.clone();
        DefaultMQPushConsumerImpl::try_reset_pop_retry_topic(
            &mut self.msgs,
            consumer_group.as_str(),
        );
        default_mqpush_consumer_impl
            .reset_retry_and_namespace(&mut self.msgs, consumer_group.as_str());

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
            if default_mqpush_consumer_impl.has_hook() {
                let queue = self.message_queue.clone();
                consume_message_context = Some(ConsumeMessageContext {
                    consumer_group,
                    msg_list: &self.msgs,
                    mq: Some(queue),
                    success: false,
                    status: "".to_string(),
                    mq_trace_context: None,
                    props: Default::default(),
                    namespace: default_mqpush_consumer_impl
                        .client_config
                        .get_namespace()
                        .unwrap_or("".to_string()),
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
                mix_all::CONSUME_CONTEXT_TYPE.to_string(),
                return_type.to_string(),
            );
        }

        if status.is_none() {
            status = Some(ConsumeConcurrentlyStatus::ReconsumeLater);
        }

        if default_mqpush_consumer_impl.has_hook() {
            let cmc = consume_message_context.as_mut().unwrap();
            cmc.status = status.unwrap().to_string();
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
            if let Some(mut consume_message_concurrently_service) = this.upgrade() {
                consume_message_concurrently_service
                    .process_consume_result(this, status.unwrap(), &context, self)
                    .await;
            }
        }
    }
}
