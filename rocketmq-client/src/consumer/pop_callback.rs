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
use rocketmq_error::RocketmqError;
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
    fn on_error(&mut self, e: Box<dyn std::error::Error + Send>);
}

/*impl<F, Fut> PopCallback for F
where
    F: Fn(Option<PopResult>, Option<Box<dyn std::error::Error + Send>>) -> Fut + Send + Sync,
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
    fn on_error(&self, e: Box<dyn std::error::Error + Send>) {
        (*self)(None, Some(e));
    }
}*/

/// Type alias for a callback function that handles the result of a pop operation.
///
/// This type alias defines a callback function that takes a `PopResult` and returns a boxed future.
pub type PopCallbackFn = Arc<
    dyn Fn(Option<PopResult>, Option<Box<dyn std::error::Error>>) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>>
        + Send
        + Sync,
>;

pub struct DefaultPopCallback {
    pub(crate) push_consumer_impl: ArcMut<DefaultMQPushConsumerImpl>,
    pub(crate) message_queue_inner: Option<MessageQueue>,
    pub(crate) subscription_data: Option<SubscriptionData>,
    pub(crate) pop_request: Option<PopRequest>,
}

impl PopCallback for DefaultPopCallback {
    async fn on_success(&mut self, pop_result: PopResult) {
        let mut push_consumer_impl = self.push_consumer_impl.clone();

        let message_queue_inner = self.message_queue_inner.take().unwrap();
        let subscription_data = self.subscription_data.take().unwrap();
        let pop_request = self.pop_request.take().unwrap();

        let pop_result = push_consumer_impl
            .process_pop_result(pop_result, &subscription_data)
            .await;
        match pop_result.pop_status {
            PopStatus::Found => {
                if pop_result.msg_found_list.as_ref().is_none_or(|value| value.is_empty()) {
                    push_consumer_impl.execute_pop_request_immediately(pop_request).await;
                } else {
                    push_consumer_impl
                        .consume_message_pop_service
                        .as_mut()
                        .unwrap()
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

    fn on_error(&mut self, err: Box<dyn std::error::Error + Send>) {
        let mut push_consumer_impl = self.push_consumer_impl.clone();

        let message_queue_inner = self.message_queue_inner.take().unwrap();
        let pop_request = self.pop_request.take().unwrap();
        let topic = message_queue_inner.get_topic();
        if !topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            if let Some(er) = err.downcast_ref::<RocketmqError>() {
                match er {
                    RocketmqError::MQClientBrokerError(broker_error) => {
                        if ResponseCode::from(broker_error.response_code()) == ResponseCode::SubscriptionNotLatest {
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
                    _ => {
                        warn!(
                            "execute the pop request exception, group={}",
                            push_consumer_impl.consumer_config.consumer_group
                        );
                    }
                }
            } else {
                warn!(
                    "execute the pull request exception, group={}",
                    push_consumer_impl.consumer_config.consumer_group
                );
            }
        }
        let time_delay = if let Some(er) = err.downcast_ref::<RocketmqError>() {
            match er {
                RocketmqError::MQClientBrokerError(broker_error) => {
                    if ResponseCode::from(broker_error.response_code()) == ResponseCode::FlowControl {
                        PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL
                    } else {
                        push_consumer_impl.pull_time_delay_mills_when_exception
                    }
                }
                _ => push_consumer_impl.pull_time_delay_mills_when_exception,
            }
        } else {
            push_consumer_impl.pull_time_delay_mills_when_exception
        };

        push_consumer_impl.execute_pop_request_later(pop_request, time_delay);
    }
}
