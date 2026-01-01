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
use std::sync::Arc;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_client_ext::MessageClientExt;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::body::cm_result::CMResult;
use rocketmq_remoting::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
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
use crate::consumer::listener::message_listener_concurrently::ArcBoxMessageListenerConcurrently;
use crate::hook::consume_message_context::ConsumeMessageContext;

pub struct ConsumeMessagePopConcurrentlyService {
    pub(crate) default_mqpush_consumer_impl: Option<ArcMut<DefaultMQPushConsumerImpl>>,
    pub(crate) client_config: ArcMut<ClientConfig>,
    pub(crate) consumer_config: ArcMut<ConsumerConfig>,
    pub(crate) consumer_group: CheetahString,
    pub(crate) message_listener: ArcBoxMessageListenerConcurrently,
    pub(crate) pop_consume_runtime: RocketMQRuntime,
}

impl ConsumeMessagePopConcurrentlyService {
    pub fn new(
        client_config: ArcMut<ClientConfig>,
        consumer_config: ArcMut<ConsumerConfig>,
        consumer_group: CheetahString,
        message_listener: ArcBoxMessageListenerConcurrently,
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
            pop_consume_runtime: RocketMQRuntime::new_multi(consume_thread as usize, consumer_group_tag.as_str()),
        }
    }
}

impl ConsumeMessageServiceTrait for ConsumeMessagePopConcurrentlyService {
    fn start(&mut self, this: ArcMut<Self>) {
        // nothing to do need
    }

    async fn shutdown(&mut self, await_terminate_millis: u64) {
        todo!()
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
        let msgs = msgs
            .into_iter()
            .map(|msg| ArcMut::new(MessageClientExt::new(msg)))
            .collect::<Vec<ArcMut<MessageClientExt>>>();
        if msgs.len() < consume_batch_size as usize {
            let mut request = ConsumeRequest::new(msgs, Arc::new(process_queue.clone()), message_queue.clone());
            self.pop_consume_runtime.get_handle().spawn(async move {
                request.run(this).await;
            });
        } else {
            msgs.chunks(consume_batch_size as usize)
                .map(|t| t.to_vec())
                .for_each(|msgs| {
                    let mut consume_request =
                        ConsumeRequest::new(msgs, Arc::new(process_queue.clone()), message_queue.clone());
                    let pop_consume_message_concurrently_service = this.clone();
                    self.pop_consume_runtime
                        .get_handle()
                        .spawn(async move { consume_request.run(pop_consume_message_concurrently_service).await });
                });
        }
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
        if consume_request.msgs.is_empty() {
            return;
        }
        let mut ack_index = context.ack_index;
        match status {
            ConsumeConcurrentlyStatus::ConsumeSuccess => {
                if ack_index >= consume_request.msgs.len() as i32 {
                    ack_index = consume_request.msgs.len() as i32 - 1;
                }
                /*int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, failed);*/
            }
            ConsumeConcurrentlyStatus::ReconsumeLater => {
                //this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, failed);
                // Java code
                ack_index = -1;
            }
        }

        //ack if consume success
        for i in 0..ack_index {
            let msg = &consume_request.msgs[i as usize];
            self.default_mqpush_consumer_impl
                .as_mut()
                .unwrap()
                .ack_async(msg.as_ref(), &self.consumer_group)
                .await;
            consume_request.process_queue.ack();
        }

        //consume later if consume fail
        for i in (ack_index + 1) as usize..consume_request.msgs.len() {
            let msg = &consume_request.msgs[i];
            consume_request.process_queue.ack();

            // More than maxReconsumeTimes
            if msg.reconsume_times >= self.consumer_config.max_reconsume_times {
                self.check_need_ack_or_delay(msg).await;
                continue;
            }

            let delay_level = context.delay_level_when_next_consume;
            let consumer_group = &self.consumer_group.clone();
            self.change_pop_invisible_time(msg, consumer_group, delay_level).await;
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
        let msg_delay_time = get_current_millis() - message.born_timestamp as u64;
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
        let extra_info = message.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK));

        struct DefaultAckCallback;

        impl AckCallback for DefaultAckCallback {
            fn on_success(&self, ack_result: AckResult) {
                //nothing to do
            }

            fn on_exception(&self, e: Box<dyn Error>) {
                error!("changePopInvisibleTime exception: {}", e);
            }
        }

        let result = self
            .default_mqpush_consumer_impl
            .as_mut()
            .unwrap()
            .change_pop_invisible_time_async(
                message.get_topic(),
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
    //msgs: Vec<MessageExt>,
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
                "the pop message time out so abort consume. popTime={} invisibleTime={}, group={} {}",
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
        let vec = self.msgs.iter().map(|msg| msg.as_ref()).collect::<Vec<&MessageExt>>();
        match self.message_listener.consume_message(&vec, &context) {
            Ok(value) => {
                status = Some(value);
            }
            Err(_) => {
                has_exception = true;
            }
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
            Some(s) => {
                if consume_rt > default_mqpush_consumer_impl.consumer_config.consume_timeout * 60 * 1000 {
                    ConsumeReturnType::TimeOut
                } else if s == ConsumeConcurrentlyStatus::ReconsumeLater {
                    ConsumeReturnType::Failed
                } else {
                    // Must be ConsumeSuccess
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
