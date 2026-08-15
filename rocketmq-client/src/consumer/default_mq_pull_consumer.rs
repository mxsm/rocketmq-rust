// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_queue::MessageQueue;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::default_mq_pull_consumer_impl::DefaultMQPullConsumerImpl;
use crate::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use crate::consumer::default_mq_pull_consumer_builder::DefaultMQPullConsumerBuilder;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::consumer::message_selector::MessageSelector;
use crate::consumer::pull_result::PullResult;
use crate::runtime::ClientRuntime;

/// Validated parameters for one Classic Pull request.
#[derive(Debug, Clone)]
pub struct PullOptions {
    message_queue: MessageQueue,
    selector: MessageSelector,
    offset: i64,
    max_messages: i32,
    max_size_in_bytes: i32,
    timeout: Duration,
    broker_suspend_timeout: Duration,
    block_if_not_found: bool,
}

impl PullOptions {
    /// Creates validated pull options with Java-compatible ordinary-pull defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue has no topic or broker, the offset is negative, or the
    /// requested message count is not positive.
    pub fn new(
        message_queue: MessageQueue,
        selector: MessageSelector,
        offset: i64,
        max_messages: i32,
    ) -> RocketMQResult<Self> {
        let options = Self {
            message_queue,
            selector,
            offset,
            max_messages,
            max_size_in_bytes: i32::MAX,
            timeout: Duration::from_secs(10),
            broker_suspend_timeout: Duration::from_secs(20),
            block_if_not_found: false,
        };
        options.validate()?;
        Ok(options)
    }

    /// Sets the maximum response payload size accepted from the broker.
    pub fn max_size_in_bytes(mut self, max_size_in_bytes: i32) -> Self {
        self.max_size_in_bytes = max_size_in_bytes;
        self
    }

    /// Sets the client-side request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the broker-side long-poll suspension timeout.
    pub fn broker_suspend_timeout(mut self, timeout: Duration) -> Self {
        self.broker_suspend_timeout = timeout;
        self
    }

    /// Enables or disables block-if-not-found long polling.
    pub fn block_if_not_found(mut self, block_if_not_found: bool) -> Self {
        self.block_if_not_found = block_if_not_found;
        self
    }

    /// Validates the complete option set.
    ///
    /// # Errors
    ///
    /// Returns an error for an incomplete queue, negative offset, non-positive count or size,
    /// zero timeout, or a block-if-not-found timeout that cannot outlive broker suspension.
    pub fn validate(&self) -> RocketMQResult<()> {
        if self.message_queue.topic().is_empty() {
            return Err(crate::mq_client_err!("message queue topic is empty"));
        }
        if self.message_queue.broker_name().is_empty() {
            return Err(crate::mq_client_err!("message queue broker name is empty"));
        }
        if self.offset < 0 {
            return Err(crate::mq_client_err!("offset < 0"));
        }
        if self.max_messages <= 0 {
            return Err(crate::mq_client_err!("maxNums <= 0"));
        }
        if self.max_size_in_bytes <= 0 {
            return Err(crate::mq_client_err!("maxSizeInBytes <= 0"));
        }
        if self.timeout.is_zero() {
            return Err(crate::mq_client_err!("pull timeout must be positive"));
        }
        if self.broker_suspend_timeout.is_zero() {
            return Err(crate::mq_client_err!("broker suspend timeout must be positive"));
        }
        if self.block_if_not_found && self.timeout <= self.broker_suspend_timeout {
            return Err(crate::mq_client_err!(
                "block-if-not-found timeout must exceed broker suspend timeout"
            ));
        }
        Ok(())
    }

    /// Returns the target message queue.
    pub fn message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    /// Returns the server-side selector.
    pub fn selector(&self) -> &MessageSelector {
        &self.selector
    }

    /// Returns the requested queue offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the maximum number of messages requested.
    pub fn max_messages(&self) -> i32 {
        self.max_messages
    }

    /// Returns the maximum response payload size.
    pub fn max_size_in_bytes_value(&self) -> i32 {
        self.max_size_in_bytes
    }

    /// Returns the client-side timeout.
    pub fn timeout_value(&self) -> Duration {
        self.timeout
    }

    /// Returns the broker-side suspension timeout.
    pub fn broker_suspend_timeout_value(&self) -> Duration {
        self.broker_suspend_timeout
    }

    /// Returns whether block-if-not-found is enabled.
    pub fn is_block_if_not_found(&self) -> bool {
        self.block_if_not_found
    }

    pub(crate) fn with_message_queue(mut self, message_queue: MessageQueue) -> Self {
        self.message_queue = message_queue;
        self
    }
}

/// Callback for an asynchronous Classic Pull request.
pub trait ClassicPullCallback: Send + Sync + 'static {
    /// Receives one fully decoded and filtered pull result.
    fn on_success(&self, pull_result: PullResult);

    /// Receives a transport, broker, timeout, or validation error.
    fn on_exception(&self, error: RocketMQError);
}

/// Function callback accepted by [`DefaultMQPullConsumer::pull_async_with_options`].
impl<F> ClassicPullCallback for F
where
    F: Fn(Result<PullResult, RocketMQError>) + Send + Sync + 'static,
{
    fn on_success(&self, pull_result: PullResult) {
        self(Ok(pull_result));
    }

    fn on_exception(&self, error: RocketMQError) {
        self(Err(error));
    }
}

/// Java Classic Pull compatibility facade backed by the shared Rust client runtime.
#[deprecated(
    since = "0.9.0",
    note = "Classic Pull is retained for compatibility; prefer DefaultLitePullConsumer for new applications"
)]
#[derive(Clone, Default)]
pub struct DefaultMQPullConsumer {
    consumer_group: Option<CheetahString>,
    implementation: Option<DefaultMQPullConsumerImpl>,
}

#[allow(deprecated)]
impl DefaultMQPullConsumer {
    /// Creates a detached compatibility value.
    ///
    /// Use [`Self::builder`] to obtain a runnable consumer. The detached constructor is retained
    /// for source compatibility and fails closed on runtime operations.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a detached compatibility value with a group name.
    pub fn with_consumer_group(consumer_group: impl Into<CheetahString>) -> Self {
        Self {
            consumer_group: Some(consumer_group.into()),
            implementation: None,
        }
    }

    /// Returns a builder that requires an application-owned runtime.
    pub fn builder(client_runtime: Arc<ClientRuntime>) -> DefaultMQPullConsumerBuilder {
        DefaultMQPullConsumerBuilder::new(client_runtime)
    }

    pub(crate) fn from_lite_consumer(
        consumer_group: CheetahString,
        lite_consumer: DefaultLitePullConsumer,
        consumer_pull_timeout: Duration,
        broker_suspend_timeout: Duration,
        consumer_timeout_when_suspend: Duration,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            consumer_group: Some(consumer_group),
            implementation: Some(DefaultMQPullConsumerImpl::from_lite_consumer(
                lite_consumer,
                consumer_pull_timeout,
                broker_suspend_timeout,
                consumer_timeout_when_suspend,
            )?),
        })
    }

    fn implementation(&self) -> RocketMQResult<&DefaultMQPullConsumerImpl> {
        self.implementation.as_ref().ok_or_else(|| {
            RocketMQError::not_initialized(
                "DefaultMQPullConsumer has no ClientRuntime; create it with DefaultMQPullConsumer::builder",
            )
        })
    }

    /// Returns the configured consumer group, if present.
    pub fn consumer_group(&self) -> Option<&CheetahString> {
        self.consumer_group.as_ref()
    }

    /// Returns the client configuration used by this consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached compatibility value.
    pub fn client_config(&self) -> RocketMQResult<Arc<ClientConfig>> {
        self.implementation()?.client_config()
    }

    /// Starts the consumer.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value, a stable lifecycle error when the
    /// consumer was already started or shut down, or the underlying client startup error.
    pub async fn start(&self) -> RocketMQResult<()> {
        self.implementation()?.start().await
    }

    /// Shuts the consumer down and awaits its runtime-owned client and rebalance tasks.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached value or an underlying client shutdown
    /// error. Repeated shutdown is idempotent.
    pub async fn shutdown(&self) -> RocketMQResult<()> {
        self.implementation()?.shutdown().await
    }

    /// Returns whether the consumer is running.
    pub async fn is_running(&self) -> bool {
        match &self.implementation {
            Some(implementation) => implementation.is_running().await,
            None => false,
        }
    }

    /// Pulls by tag expression using the default ordinary-pull timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running, the arguments are invalid, route or
    /// transport resolution fails, the broker rejects the request, or the request times out.
    pub async fn pull(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> RocketMQResult<PullResult> {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_pull_timeout())
        .broker_suspend_timeout(implementation.broker_suspend_timeout());
        implementation.pull_with_options(options).await
    }

    /// Pulls with an explicit TAG or SQL selector.
    ///
    /// # Errors
    ///
    /// Returns the same validation, lifecycle, transport, broker, and timeout errors as
    /// [`Self::pull`].
    pub async fn pull_with_selector(
        &self,
        message_queue: &MessageQueue,
        selector: MessageSelector,
        offset: i64,
        max_messages: i32,
    ) -> RocketMQResult<PullResult> {
        let implementation = self.implementation()?;
        implementation
            .pull_with_options(
                PullOptions::new(message_queue.clone(), selector, offset, max_messages)?
                    .timeout(implementation.consumer_pull_timeout())
                    .broker_suspend_timeout(implementation.broker_suspend_timeout()),
            )
            .await
    }

    /// Starts an asynchronous tag pull using the Java-compatible narrow signature.
    ///
    /// # Errors
    ///
    /// Returns an error before scheduling when validation, lifecycle, route resolution, or the
    /// initial transport request fails. Later failures are delivered to the callback.
    pub async fn pull_async<C>(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
        callback: C,
    ) -> RocketMQResult<()>
    where
        C: ClassicPullCallback,
    {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_pull_timeout())
        .broker_suspend_timeout(implementation.broker_suspend_timeout());
        implementation.pull_async_with_options(options, callback).await
    }

    /// Starts an asynchronous pull with an explicit TAG or SQL selector.
    ///
    /// # Errors
    ///
    /// Returns an error before scheduling when validation, lifecycle, route resolution, or the
    /// initial transport request fails. Later failures are delivered to the callback.
    pub async fn pull_async_with_selector<C>(
        &self,
        message_queue: &MessageQueue,
        selector: MessageSelector,
        offset: i64,
        max_messages: i32,
        callback: C,
    ) -> RocketMQResult<()>
    where
        C: ClassicPullCallback,
    {
        let implementation = self.implementation()?;
        let options = PullOptions::new(message_queue.clone(), selector, offset, max_messages)?
            .timeout(implementation.consumer_pull_timeout())
            .broker_suspend_timeout(implementation.broker_suspend_timeout());
        implementation.pull_async_with_options(options, callback).await
    }

    /// Pulls using a fully typed option set.
    ///
    /// # Errors
    ///
    /// Returns a validation, lifecycle, route, transport, broker, or timeout error.
    pub async fn pull_with_options(&self, options: PullOptions) -> RocketMQResult<PullResult> {
        self.implementation()?.pull_with_options(options).await
    }

    /// Pulls with broker suspension when no message is immediately available.
    ///
    /// # Errors
    ///
    /// Returns a validation, lifecycle, route, transport, broker, or timeout error.
    pub async fn pull_block_if_not_found(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> RocketMQResult<PullResult> {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_timeout_when_suspend())
        .broker_suspend_timeout(implementation.broker_suspend_timeout())
        .block_if_not_found(true);
        implementation.pull_with_options(options).await
    }

    /// Starts an asynchronous pull whose completion runs on the client's callback executor.
    ///
    /// # Errors
    ///
    /// Returns an error before scheduling when validation, lifecycle, route resolution, or the
    /// initial transport request fails. Later failures are delivered to the callback.
    pub async fn pull_async_with_options<C>(&self, options: PullOptions, callback: C) -> RocketMQResult<()>
    where
        C: ClassicPullCallback,
    {
        self.implementation()?.pull_async_with_options(options, callback).await
    }

    /// Starts an asynchronous block-if-not-found pull.
    ///
    /// # Errors
    ///
    /// Returns an error before scheduling when validation, lifecycle, route resolution, or the
    /// initial transport request fails. Later failures are delivered to the callback.
    pub async fn pull_block_if_not_found_async<C>(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
        callback: C,
    ) -> RocketMQResult<()>
    where
        C: ClassicPullCallback,
    {
        let implementation = self.implementation()?;
        let options = PullOptions::new(
            message_queue.clone(),
            MessageSelector::by_tag(sub_expression),
            offset,
            max_messages,
        )?
        .timeout(implementation.consumer_timeout_when_suspend())
        .broker_suspend_timeout(implementation.broker_suspend_timeout())
        .block_if_not_found(true);
        implementation.pull_async_with_options(options, callback).await
    }

    /// Fetches all readable queues for a topic.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or topic route lookup fails.
    pub async fn fetch_subscribe_message_queues(&self, topic: &str) -> RocketMQResult<Vec<MessageQueue>> {
        self.implementation()?.fetch_subscribe_message_queues(topic).await
    }

    /// Registers a listener for stable topic queue-set changes.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank topic, a detached consumer, or a failed running-consumer
    /// subscription update.
    pub async fn register_message_queue_listener<L>(&self, topic: &str, listener: L) -> RocketMQResult<()>
    where
        L: MessageQueueListener + 'static,
    {
        self.implementation()?
            .register_message_queue_listener(topic, Arc::new(listener))
            .await
    }

    /// Updates the local consume offset for a queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running, the offset is negative, or the offset
    /// store is unavailable.
    pub async fn update_consume_offset(&self, message_queue: &MessageQueue, offset: i64) -> RocketMQResult<()> {
        self.implementation()?
            .update_consume_offset(message_queue, offset)
            .await
    }

    /// Fetches a queue offset from memory or the configured offset store.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running, the offset store is unavailable, or a
    /// broker-backed read fails.
    pub async fn fetch_consume_offset(&self, message_queue: &MessageQueue, from_store: bool) -> RocketMQResult<i64> {
        self.implementation()?
            .fetch_consume_offset(message_queue, from_store)
            .await
    }

    /// Searches the queue offset nearest to a store timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the broker query fails.
    pub async fn search_offset(&self, message_queue: &MessageQueue, timestamp: u64) -> RocketMQResult<i64> {
        self.implementation()?.search_offset(message_queue, timestamp).await
    }

    /// Returns the maximum queue offset.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the broker query fails.
    pub async fn max_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        self.implementation()?.max_offset(message_queue).await
    }

    /// Returns the minimum queue offset.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer is not running or the broker query fails.
    pub async fn min_offset(&self, message_queue: &MessageQueue) -> RocketMQResult<i64> {
        self.implementation()?.min_offset(message_queue).await
    }

    /// Returns the functional implementation handle.
    ///
    /// # Errors
    ///
    /// Returns an initialization error for a detached compatibility value.
    pub fn default_mq_pull_consumer_impl(&self) -> RocketMQResult<DefaultMQPullConsumerImpl> {
        self.implementation().cloned()
    }
}

/// Common async surface implemented by Classic Pull facades.
#[allow(async_fn_in_trait)]
#[deprecated(
    since = "0.9.0",
    note = "Classic Pull is retained for compatibility; prefer DefaultLitePullConsumer for new applications"
)]
pub trait MQPullConsumer: Send + Sync {
    /// Starts the consumer.
    async fn start(&self) -> RocketMQResult<()>;

    /// Shuts the consumer down.
    async fn shutdown(&self) -> RocketMQResult<()>;

    /// Pulls one queue by tag expression.
    async fn pull(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> RocketMQResult<PullResult>;
}

#[allow(deprecated)]
impl MQPullConsumer for DefaultMQPullConsumer {
    async fn start(&self) -> RocketMQResult<()> {
        DefaultMQPullConsumer::start(self).await
    }

    async fn shutdown(&self) -> RocketMQResult<()> {
        DefaultMQPullConsumer::shutdown(self).await
    }

    async fn pull(
        &self,
        message_queue: &MessageQueue,
        sub_expression: &str,
        offset: i64,
        max_messages: i32,
    ) -> RocketMQResult<PullResult> {
        DefaultMQPullConsumer::pull(self, message_queue, sub_expression, offset, max_messages).await
    }
}
