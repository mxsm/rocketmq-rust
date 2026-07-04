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

use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::mix_all;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_rust::ArcMut;
use tracing::warn;

use crate::consumer::consumer_impl::default_mq_push_consumer_impl::DefaultMQPushConsumerImpl;
use crate::consumer::consumer_impl::default_mq_push_consumer_impl::PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::consumer_impl::re_balance::Rebalance;
use crate::consumer::pull_status::PullStatus;

pub type PullCallbackFn =
    Arc<dyn FnOnce(Option<PullResultExt>, Option<Box<dyn std::error::Error + Send>>) + Send + Sync>;

fn broker_response_code(error: &(dyn Error + Send + 'static)) -> Option<ResponseCode> {
    error.downcast_ref::<RocketMQError>().and_then(|error| match error {
        RocketMQError::BrokerOperationFailed { code, .. } => Some(ResponseCode::from(*code)),
        _ => None,
    })
}

#[trait_variant::make(PullCallback: Send)]
pub trait PullCallbackLocal: Sync {
    async fn on_success(&mut self, pull_result: PullResultExt);
    fn on_exception(&mut self, e: Box<dyn std::error::Error + Send>);
}

pub(crate) struct DefaultPullCallback {
    pub(crate) push_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    pub(crate) message_queue_inner: Option<MessageQueue>,
    pub(crate) subscription_data: Option<SubscriptionData>,
    pub(crate) pull_request: Option<PullRequest>,
}

impl PullCallback for DefaultPullCallback {
    async fn on_success(&mut self, mut pull_result_ext: PullResultExt) {
        let mut push_consumer_impl = self.push_consumer_impl.clone();

        let Some(message_queue_inner) = self.message_queue_inner.take() else {
            warn!("pull callback success ignored: message queue is missing");
            return;
        };
        let Some(subscription_data) = self.subscription_data.take() else {
            warn!(
                "pull callback success ignored: subscription data is missing, mq={}",
                message_queue_inner
            );
            if let Some(pull_request) = self.pull_request.take() {
                let delay = push_consumer_impl.pull_time_delay_mills_when_exception;
                push_consumer_impl.execute_pull_request_later(pull_request, delay);
            }
            return;
        };
        let Some(mut pull_request) = self.pull_request.take() else {
            warn!(
                "pull callback success ignored: pull request is missing, mq={}",
                message_queue_inner
            );
            return;
        };

        let Some(pull_api_wrapper) = push_consumer_impl.pull_api_wrapper.as_mut() else {
            warn!(
                "pull callback success ignored: PullAPIWrapper is not initialized, mq={}",
                message_queue_inner
            );
            let delay = push_consumer_impl.pull_time_delay_mills_when_exception;
            push_consumer_impl.execute_pull_request_later(pull_request, delay);
            return;
        };
        pull_api_wrapper.process_pull_result(&message_queue_inner, &mut pull_result_ext, &subscription_data);
        match pull_result_ext.pull_result.pull_status {
            PullStatus::Found => {
                let prev_request_offset = pull_request.next_offset;
                pull_request.set_next_offset(pull_result_ext.pull_result.next_begin_offset as i64);
                let mut first_msg_offset = i64::MAX;
                if pull_result_ext
                    .pull_result
                    .msg_found_list
                    .as_ref()
                    .is_none_or(|v| v.is_empty())
                {
                    push_consumer_impl.execute_pull_request_immediately(pull_request).await;
                } else {
                    let msg_found_list = pull_result_ext.pull_result.msg_found_list.take().unwrap_or_default();
                    if msg_found_list.is_empty() {
                        push_consumer_impl.execute_pull_request_immediately(pull_request).await;
                        return;
                    }
                    first_msg_offset = msg_found_list.first().map_or(i64::MAX, |msg| msg.queue_offset);
                    if push_consumer_impl.consume_message_service.is_none() {
                        warn!(
                            "pull callback found messages but ConsumeMessageService is not initialized, mq={}",
                            message_queue_inner
                        );
                        let delay = push_consumer_impl.pull_time_delay_mills_when_exception;
                        push_consumer_impl.execute_pull_request_later(pull_request, delay);
                        return;
                    }
                    let dispatch_to_consume = pull_request.process_queue.put_message(&msg_found_list).await;
                    if let Some(consume_message_service) = push_consumer_impl.consume_message_service.as_mut() {
                        consume_message_service
                            .submit_consume_request(
                                msg_found_list,
                                pull_request.get_process_queue().clone(),
                                pull_request.get_message_queue().clone(),
                                dispatch_to_consume,
                            )
                            .await;
                    }
                    let pull_interval = push_consumer_impl.consumer_config.pull_interval;
                    if pull_interval > 0 {
                        push_consumer_impl.execute_pull_request_later(pull_request, pull_interval);
                    } else {
                        push_consumer_impl.execute_pull_request_immediately(pull_request).await;
                    }
                }
                if pull_result_ext.pull_result.next_begin_offset < prev_request_offset as u64
                    || first_msg_offset < prev_request_offset
                {
                    warn!(
                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} \
                         prevRequestOffset: {}",
                        pull_result_ext.pull_result.next_begin_offset, prev_request_offset, prev_request_offset
                    );
                }
            }
            PullStatus::NoNewMsg | PullStatus::NoMatchedMsg => {
                pull_request.next_offset = pull_result_ext.pull_result.next_begin_offset as i64;
                push_consumer_impl.correct_tags_offset(&pull_request).await;
                push_consumer_impl.execute_pull_request_immediately(pull_request).await;
            }

            PullStatus::OffsetIllegal => {
                warn!(
                    "the pull request offset illegal, {},{}",
                    pull_result_ext.pull_result, pull_result_ext.pull_result.pull_status
                );
                pull_request.next_offset = pull_result_ext.pull_result.next_begin_offset as i64;
                pull_request.process_queue.set_dropped(true);

                let Some(offset_store) = push_consumer_impl.offset_store.as_mut() else {
                    warn!(
                        "offset illegal repair skipped: OffsetStore is not initialized, {}",
                        pull_request
                    );
                    return;
                };
                offset_store
                    .update_and_freeze_offset(pull_request.get_message_queue(), pull_request.next_offset)
                    .await;
                offset_store.persist(pull_request.get_message_queue()).await;
                push_consumer_impl
                    .rebalance_impl
                    .remove_process_queue(pull_request.get_message_queue())
                    .await;
                match push_consumer_impl
                    .rebalance_impl
                    .rebalance_impl_inner
                    .client_instance
                    .as_ref()
                {
                    Some(client_instance) => client_instance.re_balance_immediately(),
                    None => {
                        warn!("offset illegal repair skipped immediate rebalance: MQClientInstance is not initialized")
                    }
                }
            }
        };
    }

    fn on_exception(&mut self, err: Box<dyn std::error::Error + Send>) {
        let Some(message_queue_inner) = self.message_queue_inner.take() else {
            warn!(
                "pull callback exception ignored: message queue is missing, error={}",
                err
            );
            return;
        };
        let Some(pull_request) = self.pull_request.take() else {
            warn!(
                "pull callback exception ignored: pull request is missing, mq={}, error={}",
                message_queue_inner, err
            );
            return;
        };
        let topic = message_queue_inner.topic_str();
        let broker_code = broker_response_code(err.as_ref());
        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if broker_code == Some(ResponseCode::SubscriptionNotLatest) {
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
        let time_delay = if broker_code == Some(ResponseCode::FlowControl) {
            PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL
        } else {
            self.push_consumer_impl.pull_time_delay_mills_when_exception
        };

        self.push_consumer_impl
            .execute_pull_request_later(pull_request, time_delay);
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;

    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;
    use crate::consumer::pull_result::PullResult;

    fn new_callback() -> DefaultPullCallback {
        let consumer_config = ArcMut::new(ConsumerConfig::default());
        let push_consumer_impl = ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            consumer_config,
            None,
        ));
        DefaultPullCallback {
            push_consumer_impl,
            message_queue_inner: None,
            subscription_data: None,
            pull_request: None,
        }
    }

    #[test]
    fn broker_response_code_reads_typed_broker_error() {
        let error = rocketmq_error::RocketMQError::broker_operation_failed(
            "PULL_MESSAGE",
            ResponseCode::FlowControl.to_i32(),
            "flow control",
        );

        assert_eq!(broker_response_code(&error), Some(ResponseCode::FlowControl));
    }

    #[tokio::test]
    async fn success_without_message_queue_is_ignored_without_panic() {
        let mut callback = new_callback();
        let pull_result_ext = PullResultExt {
            pull_result: PullResult::new(PullStatus::NoNewMsg, 0, 0, 0, None),
            suggest_which_broker_id: 0,
            message_binary: Some(Bytes::new()),
            offset_delta: None,
        };

        PullCallback::on_success(&mut callback, pull_result_ext).await;
    }

    #[test]
    fn exception_without_pull_request_is_ignored_without_panic() {
        let mut callback = new_callback();
        callback.message_queue_inner = Some(MessageQueue::from_parts("topic", "broker-a", 0));

        PullCallback::on_exception(&mut callback, Box::new(io::Error::other("test error")));
    }
}
