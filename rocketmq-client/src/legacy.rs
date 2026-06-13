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

#![allow(deprecated)]

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::producer::local_transaction_state::LocalTransactionState;

const MODERN_LITE_PULL_CONSUMER: &str = "DefaultLitePullConsumer";
const MODERN_TRANSACTION_LISTENER: &str = "TransactionListener";
const MODERN_TRACE_HOOKS: &str = "RocketMQ trace hooks";

fn unsupported_legacy_api(api: &str, replacement: &str) -> RocketMQError {
    RocketMQError::illegal_argument(format!(
        "{api} is deprecated in the RocketMQ Java client and is not supported by rocketmq-client-rust; use \
         {replacement} instead"
    ))
}

fn unsupported_impl_api(api: &str) -> RocketMQError {
    RocketMQError::illegal_argument(format!(
        "{api} is a RocketMQ Java impl-package type and is not part of the rocketmq-client-rust public API"
    ))
}

#[deprecated(
    since = "0.9.0",
    note = "Java DefaultMQPullConsumer is deprecated; use DefaultLitePullConsumer"
)]
#[derive(Debug, Clone, Default)]
pub struct DefaultMQPullConsumer {
    consumer_group: Option<CheetahString>,
}

impl DefaultMQPullConsumer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_consumer_group(consumer_group: impl Into<CheetahString>) -> Self {
        Self {
            consumer_group: Some(consumer_group.into()),
        }
    }

    pub fn consumer_group(&self) -> Option<&CheetahString> {
        self.consumer_group.as_ref()
    }

    pub fn start(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "DefaultMQPullConsumer",
            MODERN_LITE_PULL_CONSUMER,
        ))
    }

    pub fn shutdown(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "DefaultMQPullConsumer",
            MODERN_LITE_PULL_CONSUMER,
        ))
    }

    pub fn default_mq_pull_consumer_impl(&self) -> RocketMQResult<DefaultMQPullConsumerImpl> {
        Err(unsupported_legacy_api(
            "DefaultMQPullConsumerImpl",
            MODERN_LITE_PULL_CONSUMER,
        ))
    }
}

#[deprecated(since = "0.9.0", note = "Java MQPullConsumer is deprecated; use LitePullConsumer")]
pub trait MQPullConsumer {
    fn start(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api("MQPullConsumer", MODERN_LITE_PULL_CONSUMER))
    }

    fn shutdown(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api("MQPullConsumer", MODERN_LITE_PULL_CONSUMER))
    }
}

impl MQPullConsumer for DefaultMQPullConsumer {}

#[deprecated(
    since = "0.9.0",
    note = "Java DefaultMQPullConsumerImpl is deprecated; use DefaultLitePullConsumer"
)]
#[derive(Debug, Clone, Default)]
pub struct DefaultMQPullConsumerImpl;

impl DefaultMQPullConsumerImpl {
    pub fn new() -> RocketMQResult<Self> {
        Err(unsupported_legacy_api(
            "DefaultMQPullConsumerImpl",
            MODERN_LITE_PULL_CONSUMER,
        ))
    }

    pub fn rebalance_impl(&self) -> RocketMQResult<RebalancePullImpl> {
        Err(unsupported_legacy_api("RebalancePullImpl", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java MQPullConsumerScheduleService is deprecated; use DefaultLitePullConsumer"
)]
#[derive(Debug, Clone)]
pub struct MQPullConsumerScheduleService {
    consumer_group: CheetahString,
}

impl MQPullConsumerScheduleService {
    pub fn new(consumer_group: impl Into<CheetahString>) -> Self {
        Self {
            consumer_group: consumer_group.into(),
        }
    }

    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn start(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "MQPullConsumerScheduleService",
            MODERN_LITE_PULL_CONSUMER,
        ))
    }

    pub fn shutdown(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "MQPullConsumerScheduleService",
            MODERN_LITE_PULL_CONSUMER,
        ))
    }

    pub fn register_pull_task_callback<C>(
        &mut self,
        _topic: impl Into<CheetahString>,
        _callback: C,
    ) -> RocketMQResult<()>
    where
        C: PullTaskCallback,
    {
        Err(unsupported_legacy_api("PullTaskCallback", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java PullTaskCallback is deprecated with MQPullConsumerScheduleService"
)]
pub trait PullTaskCallback {
    fn do_pull_task(&self, _message_queue: &MessageQueue, _context: &mut PullTaskContext) -> RocketMQResult<()> {
        Err(unsupported_legacy_api("PullTaskCallback", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java PullTaskContext is deprecated with MQPullConsumerScheduleService"
)]
#[derive(Debug, Clone)]
pub struct PullTaskContext {
    pull_next_delay_time_millis: i32,
}

impl Default for PullTaskContext {
    fn default() -> Self {
        Self {
            pull_next_delay_time_millis: 200,
        }
    }
}

impl PullTaskContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pull_next_delay_time_millis(&self) -> i32 {
        self.pull_next_delay_time_millis
    }

    pub fn get_pull_next_delay_time_millis(&self) -> i32 {
        self.pull_next_delay_time_millis()
    }

    pub fn set_pull_next_delay_time_millis(&mut self, pull_next_delay_time_millis: i32) {
        self.pull_next_delay_time_millis = pull_next_delay_time_millis;
    }

    pub fn get_pull_consumer(&self) -> RocketMQResult<DefaultMQPullConsumer> {
        Err(unsupported_legacy_api("MQPullConsumer", MODERN_LITE_PULL_CONSUMER))
    }

    pub fn set_pull_consumer<C>(&mut self, _pull_consumer: C) -> RocketMQResult<()>
    where
        C: MQPullConsumer,
    {
        Err(unsupported_legacy_api("MQPullConsumer", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java PullTaskImpl is deprecated; use DefaultLitePullConsumer"
)]
#[derive(Debug, Clone, Default)]
pub struct PullTaskImpl {
    message_queue: Option<MessageQueue>,
}

impl PullTaskImpl {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue: Some(message_queue),
        }
    }

    pub fn message_queue(&self) -> Option<&MessageQueue> {
        self.message_queue.as_ref()
    }

    pub fn run(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api("PullTaskImpl", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java RebalancePullImpl is deprecated with DefaultMQPullConsumer"
)]
#[derive(Debug, Clone, Default)]
pub struct RebalancePullImpl;

impl RebalancePullImpl {
    pub fn new() -> RocketMQResult<Self> {
        Err(unsupported_legacy_api("RebalancePullImpl", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(since = "0.9.0", note = "Java MQHelper depends on DefaultMQPullConsumer")]
#[derive(Debug, Clone, Default)]
pub struct MQHelper;

impl MQHelper {
    pub fn reset_offset_by_timestamp(
        _message_model: impl Into<CheetahString>,
        _consumer_group: impl Into<CheetahString>,
        _topic: impl Into<CheetahString>,
        _timestamp: u64,
    ) -> RocketMQResult<()> {
        Err(unsupported_legacy_api("MQHelper", MODERN_LITE_PULL_CONSUMER))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java TransactionCheckListener is deprecated; use TransactionListener"
)]
pub trait TransactionCheckListener {
    fn check_local_transaction_state(&self, _message: &MessageExt) -> RocketMQResult<LocalTransactionState> {
        Err(unsupported_legacy_api(
            "TransactionCheckListener",
            MODERN_TRANSACTION_LISTENER,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct SendMessageOpenTracingHookImpl;

impl SendMessageOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "SendMessageOpenTracingHook"
    }

    pub fn unsupported(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "SendMessageOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct ConsumeMessageOpenTracingHookImpl;

impl ConsumeMessageOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "ConsumeMessageOpenTracingHook"
    }

    pub fn unsupported(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "ConsumeMessageOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[deprecated(
    since = "0.9.0",
    note = "Java OpenTracing hooks are not part of the modern RocketMQ client trace API"
)]
#[derive(Debug, Clone, Default)]
pub struct EndTransactionOpenTracingHookImpl;

impl EndTransactionOpenTracingHookImpl {
    pub fn new<T>(_tracer: T) -> Self {
        Self
    }

    pub fn hook_name(&self) -> &'static str {
        "EndTransactionOpenTracingHook"
    }

    pub fn unsupported(&self) -> RocketMQResult<()> {
        Err(unsupported_legacy_api(
            "EndTransactionOpenTracingHookImpl",
            MODERN_TRACE_HOOKS,
        ))
    }
}

#[derive(Debug, Clone, Default)]
pub struct DoNothingClientRemotingProcessor;

impl DoNothingClientRemotingProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_request(&self) -> Option<()> {
        None
    }
}

#[derive(Debug, Clone, Default)]
pub struct RebalanceImpl;

impl RebalanceImpl {
    pub fn new() -> RocketMQResult<Self> {
        Err(unsupported_impl_api("RebalanceImpl"))
    }

    pub fn do_rebalance(&self) -> RocketMQResult<()> {
        Err(unsupported_impl_api("RebalanceImpl"))
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumeRequest;

impl ConsumeRequest {
    pub fn new() -> Self {
        Self
    }

    pub fn run(&self) -> RocketMQResult<()> {
        Err(unsupported_impl_api("ConsumeRequest"))
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[test]
    fn legacy_pull_consumer_returns_typed_unsupported_error() {
        let consumer = DefaultMQPullConsumer::with_consumer_group("LegacyGroup");

        assert_eq!(
            consumer.consumer_group().map(|group| group.as_str()),
            Some("LegacyGroup")
        );
        let error = consumer.start().expect_err("deprecated pull consumer should not start");

        assert!(error.to_string().contains("DefaultMQPullConsumer"));
        assert!(error.to_string().contains("DefaultLitePullConsumer"));
    }

    #[test]
    fn pull_task_context_preserves_java_default_delay() {
        let mut context = PullTaskContext::new();

        assert_eq!(context.get_pull_next_delay_time_millis(), 200);
        context.set_pull_next_delay_time_millis(500);
        assert_eq!(context.pull_next_delay_time_millis(), 500);
    }

    #[test]
    fn legacy_schedule_service_register_callback_returns_typed_error() {
        struct Callback;
        impl PullTaskCallback for Callback {}

        let mut service = MQPullConsumerScheduleService::new("LegacyGroup");
        let error = service
            .register_pull_task_callback("TopicA", Callback)
            .expect_err("deprecated schedule service should reject callbacks");

        assert!(error.to_string().contains("PullTaskCallback"));
        assert!(error.to_string().contains("DefaultLitePullConsumer"));
    }

    #[test]
    fn legacy_open_tracing_hooks_keep_java_hook_names() {
        assert_eq!(
            SendMessageOpenTracingHookImpl::new(()).hook_name(),
            "SendMessageOpenTracingHook"
        );
        assert_eq!(
            ConsumeMessageOpenTracingHookImpl::new(()).hook_name(),
            "ConsumeMessageOpenTracingHook"
        );
        assert_eq!(
            EndTransactionOpenTracingHookImpl::new(()).hook_name(),
            "EndTransactionOpenTracingHook"
        );
    }

    #[test]
    fn transaction_check_listener_returns_typed_unsupported_error() {
        struct Listener;
        impl TransactionCheckListener for Listener {}

        let error = Listener
            .check_local_transaction_state(&MessageExt::default())
            .expect_err("deprecated transaction listener should reject checks");

        assert!(error.to_string().contains("TransactionCheckListener"));
        assert!(error.to_string().contains("TransactionListener"));
    }

    #[test]
    fn impl_package_markers_do_not_panic() {
        assert!(DoNothingClientRemotingProcessor::new().process_request().is_none());

        let rebalance_error = RebalanceImpl::new().expect_err("impl-package RebalanceImpl should be unsupported");
        assert!(rebalance_error.to_string().contains("impl-package type"));

        let consume_error = ConsumeRequest::new()
            .run()
            .expect_err("impl-package ConsumeRequest should be unsupported");
        assert!(consume_error.to_string().contains("ConsumeRequest"));
    }
}
