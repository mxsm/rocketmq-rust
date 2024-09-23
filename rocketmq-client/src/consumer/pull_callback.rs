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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use tracing::warn;

use crate::consumer::consumer_impl::consume_message_service::ConsumeMessageServiceTrait;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::pull_status::PullStatus;
use crate::error::MQClientError;

pub type PullCallbackFn =
    Arc<dyn FnOnce(Option<PullResultExt>, Option<Box<dyn std::error::Error + Send>>) + Send + Sync>;

#[trait_variant::make(PullCallback: Send)]
pub trait PullCallbackLocal: Sync {
    async fn on_success(&mut self, pull_result: PullResultExt);
    fn on_exception(&mut self, e: Box<dyn std::error::Error + Send>);
}

pub(crate) struct DefaultPullCallback {
    pub(crate) push_consumer_impl: DefaultMQPushConsumerImpl,
    pub(crate) message_queue_inner: Option<MessageQueue>,
    pub(crate) subscription_data: Option<SubscriptionData>,
    pub(crate) pull_request: Option<PullRequest>,
}

impl DefaultPullCallback {
    /*fn kkk(){
        let pull_callback =
            |pull_result_ext: Option<PullResultExt>,
             err: Option<Box<dyn std::error::Error + Send>>| {
                tokio::spawn(async move {
                    if let Some(mut pull_result_ext) = pull_result_ext {
                        this.pull_api_wrapper.as_mut().unwrap().process_pull_result(
                            &message_queue_inner,
                            &mut pull_result_ext,
                            subscription_data.as_ref().unwrap(),
                        );
                        match pull_result_ext.pull_result.pull_status {
                            PullStatus::Found => {
                                let prev_request_offset = pull_request.next_offset;
                                pull_request.set_next_offset(
                                    pull_result_ext.pull_result.next_begin_offset as i64,
                                );
                                /*let pull_rt = get_current_millis() - begin_timestamp.elapsed().as_millis() as u64;
                                self.client_instance.as_mut().unwrap().*/
                                let mut first_msg_offset = i64::MAX;
                                if pull_result_ext.pull_result.msg_found_list.is_empty() {
                                    this.execute_pull_request_immediately(pull_request).await;
                                } else {
                                    first_msg_offset = pull_result_ext
                                        .pull_result
                                        .msg_found_list()
                                        .first()
                                        .unwrap()
                                        .message_ext_inner
                                        .queue_offset;
                                    // DefaultMQPushConsumerImpl.this.getConsumerStatsManager().
                                    // incPullTPS(pullRequest.getConsumerGroup(),
                                    //
                                    // pullRequest.getMessageQueue().getTopic(),
                                    // pullResult.getMsgFoundList().size());
                                    let vec = pull_result_ext
                                        .pull_result
                                        .msg_found_list
                                        .clone()
                                        .into_iter()
                                        .map(|msg| msg.message_ext_inner)
                                        .collect();
                                    let dispatch_to_consume =
                                        pull_request.process_queue.put_message(vec).await;
                                    this.consume_message_concurrently_service
                                        .as_mut()
                                        .unwrap()
                                        .consume_message_concurrently_service
                                        .submit_consume_request(
                                            pull_result_ext.pull_result.msg_found_list,
                                            pull_request.get_process_queue().clone(),
                                            pull_request.get_message_queue().clone(),
                                            dispatch_to_consume,
                                        )
                                        .await;
                                    if this.consumer_config.pull_interval > 0 {
                                        this.execute_pull_request_later(
                                            pull_request,
                                            this.consumer_config.pull_interval,
                                        );
                                    } else {
                                        this.execute_pull_request_immediately(pull_request).await;
                                    }
                                }
                                if pull_result_ext.pull_result.next_begin_offset
                                    < prev_request_offset as u64
                                    || first_msg_offset < prev_request_offset
                                {
                                    warn!(
                                        "[BUG] pull message result maybe data wrong, \
                                         nextBeginOffset: {} firstMsgOffset: {} \
                                         prevRequestOffset: {}",
                                        pull_result_ext.pull_result.next_begin_offset,
                                        prev_request_offset,
                                        prev_request_offset
                                    );
                                }
                            }
                            PullStatus::NoNewMsg | PullStatus::NoMatchedMsg => {
                                pull_request.next_offset =
                                    pull_result_ext.pull_result.next_begin_offset as i64;
                                this.correct_tags_offset(&pull_request).await;
                                this.execute_pull_request_immediately(pull_request).await;
                            }

                            PullStatus::OffsetIllegal => {
                                warn!(
                                    "the pull request offset illegal, {},{}",
                                    pull_result_ext.pull_result,
                                    pull_result_ext.pull_result.pull_status
                                );
                                pull_request.next_offset =
                                    pull_result_ext.pull_result.next_begin_offset as i64;
                                pull_request.process_queue.set_dropped(true);
                                tokio::spawn(async move {
                                    let offset_store = this.offset_store.as_mut().unwrap();
                                    offset_store
                                        .update_and_freeze_offset(
                                            pull_request.get_message_queue(),
                                            pull_request.next_offset,
                                        )
                                        .await;
                                    offset_store.persist(pull_request.get_message_queue()).await;
                                    this.rebalance_impl
                                        .remove_process_queue(pull_request.get_message_queue())
                                        .await;
                                    this.rebalance_impl
                                        .rebalance_impl_inner
                                        .client_instance
                                        .as_ref()
                                        .unwrap()
                                        .re_balance_immediately()
                                });
                            }
                        };
                        return;
                    }

                    if let Some(err) = err {
                        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
                            if let Some(er) = err.downcast_ref::<MQClientError>() {
                                match er {
                                    MQClientError::MQBrokerError(code, msg, addr) => {
                                        if ResponseCode::from(*code)
                                            == ResponseCode::SubscriptionNotLatest
                                        {
                                            warn!(
                                                "the subscription is not latest, group={}",
                                                this.consumer_config.consumer_group,
                                            );
                                        } else {
                                            warn!(
                                                "execute the pull request exception, group={}",
                                                this.consumer_config.consumer_group
                                            );
                                        }
                                    }
                                    _ => {
                                        warn!(
                                            "execute the pull request exception, group={}",
                                            this.consumer_config.consumer_group
                                        );
                                    }
                                }
                            } else {
                                warn!(
                                    "execute the pull request exception, group={}",
                                    this.consumer_config.consumer_group
                                );
                            }
                        }
                        if let Some(er) = err.downcast_ref::<MQClientError>() {
                            match er {
                                MQClientError::MQBrokerError(code, msg, addr) => {
                                    if ResponseCode::from(*code) == ResponseCode::FlowControl {
                                        this.execute_pull_request_later(
                                            pull_request,
                                            PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
                                        );
                                    } else {
                                        this.execute_pull_request_later(
                                            pull_request,
                                            this.pull_time_delay_mills_when_exception,
                                        );
                                    }
                                }
                                _ => {
                                    this.execute_pull_request_later(
                                        pull_request,
                                        this.pull_time_delay_mills_when_exception,
                                    );
                                }
                            }
                        } else {
                            this.execute_pull_request_later(
                                pull_request,
                                this.pull_time_delay_mills_when_exception,
                            );
                        }
                    }
                });
            };
    }*/
}

impl PullCallback for DefaultPullCallback {
    async fn on_success(&mut self, mut pull_result_ext: PullResultExt) {
        let message_queue_inner = self.message_queue_inner.take().unwrap();
        let subscription_data = self.subscription_data.take().unwrap();
        let mut pull_request = self.pull_request.take().unwrap();

        self.push_consumer_impl
            .pull_api_wrapper
            .as_mut()
            .unwrap()
            .process_pull_result(
                &message_queue_inner,
                &mut pull_result_ext,
                &subscription_data,
            );
        match pull_result_ext.pull_result.pull_status {
            PullStatus::Found => {
                let prev_request_offset = pull_request.next_offset;
                pull_request.set_next_offset(pull_result_ext.pull_result.next_begin_offset as i64);
                /*let pull_rt = get_current_millis() - begin_timestamp.elapsed().as_millis() as u64;
                self.client_instance.as_mut().unwrap().*/
                let mut first_msg_offset = i64::MAX;
                if pull_result_ext.pull_result.msg_found_list.is_empty() {
                    self.push_consumer_impl
                        .execute_pull_request_immediately(pull_request)
                        .await;
                } else {
                    first_msg_offset = pull_result_ext
                        .pull_result
                        .msg_found_list()
                        .first()
                        .unwrap()
                        .message_ext_inner
                        .queue_offset;
                    // DefaultMQPushConsumerImpl.self.push_consumer_impl.getConsumerStatsManager().
                    // incPullTPS(pullRequest.getConsumerGroup(),
                    //
                    // pullRequest.getMessageQueue().getTopic(),
                    // pullResult.getMsgFoundList().size());
                    let vec = pull_result_ext.pull_result.msg_found_list.clone();
                    let dispatch_to_consume = pull_request.process_queue.put_message(vec).await;
                    let consume_message_concurrently_service_inner = self
                        .push_consumer_impl
                        .consume_message_concurrently_service
                        .as_mut()
                        .unwrap()
                        .consume_message_concurrently_service
                        .clone();
                    self.push_consumer_impl
                        .consume_message_concurrently_service
                        .as_mut()
                        .unwrap()
                        .consume_message_concurrently_service
                        .submit_consume_request(
                            consume_message_concurrently_service_inner,
                            pull_result_ext.pull_result.msg_found_list,
                            pull_request.get_process_queue().clone(),
                            pull_request.get_message_queue().clone(),
                            dispatch_to_consume,
                        )
                        .await;
                    if self.push_consumer_impl.consumer_config.pull_interval > 0 {
                        self.push_consumer_impl.execute_pull_request_later(
                            pull_request,
                            self.push_consumer_impl.consumer_config.pull_interval,
                        );
                    } else {
                        self.push_consumer_impl
                            .execute_pull_request_immediately(pull_request)
                            .await;
                    }
                }
                if pull_result_ext.pull_result.next_begin_offset < prev_request_offset as u64
                    || first_msg_offset < prev_request_offset
                {
                    warn!(
                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} \
                         firstMsgOffset: {} prevRequestOffset: {}",
                        pull_result_ext.pull_result.next_begin_offset,
                        prev_request_offset,
                        prev_request_offset
                    );
                }
            }
            PullStatus::NoNewMsg | PullStatus::NoMatchedMsg => {
                pull_request.next_offset = pull_result_ext.pull_result.next_begin_offset as i64;
                self.push_consumer_impl
                    .correct_tags_offset(&pull_request)
                    .await;
                self.push_consumer_impl
                    .execute_pull_request_immediately(pull_request)
                    .await;
            }

            PullStatus::OffsetIllegal => {
                warn!(
                    "the pull request offset illegal, {},{}",
                    pull_result_ext.pull_result, pull_result_ext.pull_result.pull_status
                );
                pull_request.next_offset = pull_result_ext.pull_result.next_begin_offset as i64;
                pull_request.process_queue.set_dropped(true);

                let offset_store = self.push_consumer_impl.offset_store.as_mut().unwrap();
                offset_store
                    .update_and_freeze_offset(
                        pull_request.get_message_queue(),
                        pull_request.next_offset,
                    )
                    .await;
                offset_store.persist(pull_request.get_message_queue()).await;
                self.push_consumer_impl
                    .rebalance_impl
                    .remove_process_queue(pull_request.get_message_queue())
                    .await;
                self.push_consumer_impl
                    .rebalance_impl
                    .rebalance_impl_inner
                    .client_instance
                    .as_ref()
                    .unwrap()
                    .re_balance_immediately()
            }
        };
    }

    fn on_exception(&mut self, err: Box<dyn std::error::Error + Send>) {
        let message_queue_inner = self.message_queue_inner.take().unwrap();
        let pull_request = self.pull_request.take().unwrap();
        let topic = message_queue_inner.get_topic().to_string();
        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if let Some(er) = err.downcast_ref::<MQClientError>() {
                match er {
                    MQClientError::MQBrokerError(code, msg, addr) => {
                        if ResponseCode::from(*code) == ResponseCode::SubscriptionNotLatest {
                            warn!(
                                "the subscription is not latest, group={}",
                                self.push_consumer_impl.consumer_config.consumer_group,
                            );
                        } else {
                            warn!(
                                "execute the pull request exception, group={}",
                                self.push_consumer_impl.consumer_config.consumer_group
                            );
                        }
                    }
                    _ => {
                        warn!(
                            "execute the pull request exception, group={}",
                            self.push_consumer_impl.consumer_config.consumer_group
                        );
                    }
                }
            } else {
                warn!(
                    "execute the pull request exception, group={}",
                    self.push_consumer_impl.consumer_config.consumer_group
                );
            }
        }
        let time_delay = if let Some(er) = err.downcast_ref::<MQClientError>() {
            match er {
                MQClientError::MQBrokerError(code, _, _) => {
                    if ResponseCode::from(*code) == ResponseCode::FlowControl {
                        /*self.push_consumer_impl.execute_pull_request_later(
                            pull_request,
                            PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL,
                        );*/
                        PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL
                    } else {
                        /*self.push_consumer_impl.execute_pull_request_later(
                            pull_request,
                            self.push_consumer_impl.pull_time_delay_mills_when_exception,
                        );*/
                        self.push_consumer_impl.pull_time_delay_mills_when_exception
                    }
                }
                _ => {
                    /*self.push_consumer_impl.execute_pull_request_later(
                        pull_request,
                        self.push_consumer_impl.pull_time_delay_mills_when_exception,
                    );*/
                    self.push_consumer_impl.pull_time_delay_mills_when_exception
                }
            }
        } else {
            /*self.push_consumer_impl.execute_pull_request_later(
                pull_request,
                self.push_consumer_impl.pull_time_delay_mills_when_exception,
            );*/
            self.push_consumer_impl.pull_time_delay_mills_when_exception
        };

        self.push_consumer_impl
            .execute_pull_request_later(pull_request, time_delay);
    }
}
