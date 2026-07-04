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

use std::future::Future;
use std::pin::Pin;
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
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::pop_result::PopResult;
use crate::consumer::pop_status::PopStatus;

/// Trait for handling the results of a pop operation.
///
/// This trait defines the methods for handling successful and error results of a pop operation.
#[trait_variant::make(PopCallback: Send)]
pub trait PopCallbackInner {
    /// Called when the pop operation is successful.
    ///
    /// # Arguments
    ///
    /// * `pop_result` - The result of the pop operation.
    async fn on_success(&mut self, pop_result: PopResult);

    /// Called when the pop operation encounters an error.
    ///
    /// # Arguments
    ///
    /// * `e` - The error encountered during the pop operation.
    fn on_error(&mut self, e: RocketMQError);
}

/*impl<F, Fut> PopCallback for F
where
    F: Fn(Option<PopResult>, Option<RocketMQError>) -> Fut + Send + Sync,
    Fut: Future<Output = ()> + Send,
{
    /// Calls the function with the pop result when the pop operation is successful.
    ///
    /// # Arguments
    ///
    /// * `pop_result` - The result of the pop operation.
    async fn on_success(&mut self, pop_result: PopResult) {
        (*self)(Some(pop_result), None).await;
    }

    /// Does nothing when the pop operation encounters an error.
    ///
    /// # Arguments
    ///
    /// * `e` - The error encountered during the pop operation.
    fn on_error(&self, e: RocketMQError) {
        (*self)(None, Some(e));
    }
}*/

/// Type alias for a callback function that handles the result of a pop operation.
///
/// This type alias defines a callback function that takes a `PopResult` and returns a boxed future.
pub type PopCallbackFn = Arc<
    dyn Fn(Option<PopResult>, Option<RocketMQError>) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> + Send + Sync,
>;

fn broker_response_code(error: &RocketMQError) -> Option<ResponseCode> {
    match error {
        RocketMQError::BrokerOperationFailed { code, .. } => Some(ResponseCode::from(*code)),
        _ => None,
    }
}

pub struct DefaultPopCallback {
    pub(crate) push_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    pub(crate) message_queue_inner: Option<MessageQueue>,
    pub(crate) subscription_data: Option<SubscriptionData>,
    pub(crate) pop_request: Option<PopRequest>,
}

impl PopCallback for DefaultPopCallback {
    async fn on_success(&mut self, pop_result: PopResult) {
        let mut push_consumer_impl = self.push_consumer_impl.clone();

        let Some(message_queue_inner) = self.message_queue_inner.take() else {
            warn!("pop callback success ignored: message queue is missing");
            return;
        };
        let Some(subscription_data) = self.subscription_data.take() else {
            warn!(
                "pop callback success ignored: subscription data is missing, mq={}",
                message_queue_inner
            );
            if let Some(pop_request) = self.pop_request.take() {
                let delay = push_consumer_impl.pull_time_delay_mills_when_exception;
                push_consumer_impl.execute_pop_request_later(pop_request, delay);
            }
            return;
        };
        let Some(pop_request) = self.pop_request.take() else {
            warn!(
                "pop callback success ignored: pop request is missing, mq={}",
                message_queue_inner
            );
            return;
        };

        let pop_result = push_consumer_impl
            .process_pop_result(pop_result, &subscription_data)
            .await;
        match pop_result.pop_status {
            PopStatus::Found => {
                if pop_result.msg_found_list.as_ref().is_none_or(|value| value.is_empty()) {
                    push_consumer_impl.execute_pop_request_immediately(pop_request).await;
                } else {
                    let Some(consume_message_pop_service) = push_consumer_impl.consume_message_pop_service.as_mut()
                    else {
                        warn!(
                            "pop callback found messages but ConsumeMessagePopService is not initialized, mq={}",
                            message_queue_inner
                        );
                        let delay = push_consumer_impl.pull_time_delay_mills_when_exception;
                        push_consumer_impl.execute_pop_request_later(pop_request, delay);
                        return;
                    };
                    consume_message_pop_service
                        .submit_pop_consume_request(
                            pop_result.msg_found_list.unwrap_or_default(),
                            pop_request.get_pop_process_queue(),
                            pop_request.get_message_queue(),
                        )
                        .await;
                    let pull_interval = push_consumer_impl.consumer_config.pull_interval;
                    if pull_interval > 0 {
                        push_consumer_impl.execute_pop_request_later(pop_request, pull_interval);
                    } else {
                        push_consumer_impl.execute_pop_request_immediately(pop_request).await;
                    }
                }
            }
            PopStatus::NoNewMsg | PopStatus::PollingNotFound => {
                push_consumer_impl.execute_pop_request_immediately(pop_request).await;
            }
            PopStatus::PollingFull => {
                let pull_time_delay_mills_when_exception = push_consumer_impl.pull_time_delay_mills_when_exception;
                push_consumer_impl.execute_pop_request_later(pop_request, pull_time_delay_mills_when_exception);
            }
        }
    }

    fn on_error(&mut self, err: RocketMQError) {
        let mut push_consumer_impl = self.push_consumer_impl.clone();

        let Some(message_queue_inner) = self.message_queue_inner.take() else {
            warn!("pop callback exception ignored: message queue is missing, error={err}");
            return;
        };
        let Some(pop_request) = self.pop_request.take() else {
            warn!(
                "pop callback exception ignored: pop request is missing, mq={}, error={}",
                message_queue_inner, err
            );
            return;
        };
        let topic = message_queue_inner.topic_str();
        let broker_code = broker_response_code(&err);
        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if broker_code == Some(ResponseCode::SubscriptionNotLatest) {
                warn!(
                    "the subscription is not latest, group={}",
                    push_consumer_impl.consumer_config.consumer_group,
                );
            } else {
                warn!(
                    "execute the pop request exception, group={}",
                    push_consumer_impl.consumer_config.consumer_group
                );
            }
        }
        let time_delay = if broker_code == Some(ResponseCode::FlowControl) {
            PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL
        } else {
            push_consumer_impl.pull_time_delay_mills_when_exception
        };

        push_consumer_impl.execute_pop_request_later(pop_request, time_delay);
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_ext::MessageExt;

    use super::*;
    use crate::base::client_config::ClientConfig;
    use crate::consumer::consumer_impl::pop_process_queue::PopProcessQueue;
    use crate::consumer::default_mq_push_consumer::ConsumerConfig;

    fn message_queue() -> MessageQueue {
        MessageQueue::from_parts("topic", "broker-a", 0)
    }

    fn pop_request() -> PopRequest {
        PopRequest::new(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("group"),
            message_queue(),
            PopProcessQueue::new(),
            0,
        )
    }

    fn new_callback() -> DefaultPopCallback {
        let consumer_config = ArcMut::new(ConsumerConfig::default());
        let push_consumer_impl = ArcMut::new(DefaultMQPushConsumerImpl::new(
            ClientConfig::default(),
            consumer_config,
            None,
        ));
        DefaultPopCallback {
            push_consumer_impl,
            message_queue_inner: None,
            subscription_data: None,
            pop_request: None,
        }
    }

    #[test]
    fn broker_response_code_reads_broker_error_without_downcast() {
        let error = rocketmq_error::RocketMQError::broker_operation_failed(
            "POP_MESSAGE",
            ResponseCode::SubscriptionNotLatest.to_i32(),
            "subscription not latest",
        );

        assert_eq!(broker_response_code(&error), Some(ResponseCode::SubscriptionNotLatest));
    }

    #[tokio::test]
    async fn success_without_message_queue_is_ignored_without_panic() {
        let mut callback = new_callback();

        PopCallback::on_success(&mut callback, PopResult::default()).await;
    }

    #[tokio::test]
    async fn success_without_subscription_data_is_delayed_without_panic() {
        let mut callback = new_callback();
        callback.message_queue_inner = Some(message_queue());
        callback.pop_request = Some(pop_request());

        PopCallback::on_success(&mut callback, PopResult::default()).await;
    }

    #[tokio::test]
    async fn success_without_pop_request_is_ignored_without_panic() {
        let mut callback = new_callback();
        callback.message_queue_inner = Some(message_queue());
        callback.subscription_data = Some(SubscriptionData::default());

        PopCallback::on_success(&mut callback, PopResult::default()).await;
    }

    #[tokio::test]
    async fn found_messages_without_pop_service_are_delayed_without_panic() {
        let mut callback = new_callback();
        callback.message_queue_inner = Some(message_queue());
        callback.subscription_data = Some(SubscriptionData::default());
        callback.pop_request = Some(pop_request());
        let pop_result = PopResult {
            msg_found_list: Some(vec![MessageExt::default()]),
            pop_status: PopStatus::Found,
            ..Default::default()
        };

        PopCallback::on_success(&mut callback, pop_result).await;
    }

    #[test]
    fn error_without_message_queue_is_ignored_without_panic() {
        let mut callback = new_callback();

        PopCallback::on_error(&mut callback, RocketMQError::illegal_argument("test error"));
    }

    #[test]
    fn error_without_pop_request_is_ignored_without_panic() {
        let mut callback = new_callback();
        callback.message_queue_inner = Some(message_queue());

        PopCallback::on_error(&mut callback, RocketMQError::illegal_argument("test error"));
    }
}
