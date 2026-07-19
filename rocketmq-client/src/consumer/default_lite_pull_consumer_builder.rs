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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::mix_all;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::runtime::RPCHook;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::default_lite_pull_consume_timestamp;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::validate_lite_pull_consume_from_where;
use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::LitePullConsumerConfig;
use crate::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use crate::consumer::rebalance_strategy::allocate_message_queue_averagely::AllocateMessageQueueAveragely;
use crate::trace::trace_dispatcher::TraceDispatcher;

pub(crate) const MIN_AUTOCOMMIT_INTERVAL_MILLIS: u64 = 1000;

/// Builder for creating a [`DefaultLitePullConsumer`] with customized configuration.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_client::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
///
/// let consumer = DefaultLitePullConsumer::builder()
///     .consumer_group("my_consumer_group")
///     .name_server_addr("127.0.0.1:9876")
///     .pull_batch_size(32)
///     .auto_commit(true)
///     .build()?;
/// ```
pub struct DefaultLitePullConsumerBuilder {
    // Client configuration
    name_server_addr: Option<CheetahString>,
    client_ip: Option<CheetahString>,
    instance_name: Option<CheetahString>,
    namespace: Option<CheetahString>,
    access_channel: Option<CheetahString>,
    use_tls: bool,

    // Consumer configuration
    consumer_group: Option<CheetahString>,
    message_model: MessageModel,
    consume_from_where: ConsumeFromWhere,
    consume_timestamp: Option<CheetahString>,
    allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,
    unit_mode: bool,

    // Pull configuration
    pull_batch_size: i32,
    pull_thread_nums: usize,
    connect_broker_by_user: bool,
    default_broker_id: u64,

    // Flow control
    pull_threshold_for_queue: i64,
    pull_threshold_size_for_queue: i32,
    pull_threshold_for_all: i64,
    consume_max_span: i64,

    // Backoff delays
    pull_time_delay_millis_when_exception: u64,
    pull_time_delay_millis_when_cache_flow_control: u64,
    pull_time_delay_millis_when_broker_flow_control: u64,
    broker_suspend_max_time_millis: u64,
    consumer_timeout_millis_when_suspend: u64,
    consumer_pull_timeout_millis: u64,

    // Poll configuration
    poll_timeout_millis: u64,

    // Auto-commit
    auto_commit: bool,
    auto_commit_interval_millis: u64,

    // Miscellaneous
    topic_metadata_check_interval_millis: u64,
    message_request_mode: MessageRequestMode,

    // Advanced
    rpc_hook: Option<Arc<dyn RPCHook>>,
    trace_dispatcher: Option<Arc<dyn TraceDispatcher + Send + Sync>>,
    enable_msg_trace: bool,
    custom_trace_topic: Option<CheetahString>,
}

impl Default for DefaultLitePullConsumerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultLitePullConsumerBuilder {
    /// Creates a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            name_server_addr: None,
            client_ip: None,
            instance_name: None,
            namespace: None,
            access_channel: None,
            use_tls: false,
            consumer_group: None,
            message_model: MessageModel::Clustering,
            consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
            consume_timestamp: Some(default_lite_pull_consume_timestamp()),
            allocate_message_queue_strategy: Arc::new(AllocateMessageQueueAveragely),
            unit_mode: false,
            pull_batch_size: 10,
            pull_thread_nums: 20,
            connect_broker_by_user: false,
            default_broker_id: mix_all::MASTER_ID,
            pull_threshold_for_queue: 1000,
            pull_threshold_size_for_queue: 100,
            pull_threshold_for_all: 10000,
            consume_max_span: 2000,
            pull_time_delay_millis_when_exception: 1000,
            pull_time_delay_millis_when_cache_flow_control: 50,
            pull_time_delay_millis_when_broker_flow_control: 20,
            broker_suspend_max_time_millis: 20_000,
            consumer_timeout_millis_when_suspend: 30_000,
            consumer_pull_timeout_millis: 10_000,
            poll_timeout_millis: 5000,
            auto_commit: true,
            auto_commit_interval_millis: 5000,
            topic_metadata_check_interval_millis: 30000,
            message_request_mode: MessageRequestMode::Pull,
            rpc_hook: None,
            trace_dispatcher: None,
            enable_msg_trace: false,
            custom_trace_topic: None,
        }
    }

    /// Sets the name server address (required).
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// builder.name_server_addr("127.0.0.1:9876");
    /// ```
    pub fn name_server_addr(mut self, addr: impl Into<CheetahString>) -> Self {
        self.name_server_addr = Some(addr.into());
        self
    }

    /// Sets the consumer group name (required).
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// builder.consumer_group("my_consumer_group");
    /// ```
    pub fn consumer_group(mut self, group: impl Into<CheetahString>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    /// Sets the client IP address (optional, auto-detected by default).
    pub fn client_ip(mut self, ip: impl Into<CheetahString>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    /// Sets the instance name (optional, auto-generated by default).
    pub fn instance_name(mut self, name: impl Into<CheetahString>) -> Self {
        self.instance_name = Some(name.into());
        self
    }

    /// Sets the namespace (optional).
    pub fn namespace(mut self, namespace: impl Into<CheetahString>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Sets whether client remoting connections should use TLS.
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Sets the message model (default: Clustering).
    pub fn message_model(mut self, model: MessageModel) -> Self {
        self.message_model = model;
        self
    }

    /// Sets where to start consuming from when no offset exists (default: LastOffset).
    pub fn consume_from_where(mut self, consume_from: ConsumeFromWhere) -> Self {
        self.consume_from_where = consume_from;
        self
    }

    /// Sets the timestamp to consume from (for CONSUME_FROM_TIMESTAMP mode).
    pub fn consume_timestamp(mut self, timestamp: impl Into<CheetahString>) -> Self {
        self.consume_timestamp = Some(timestamp.into());
        self
    }

    /// Sets the message queue allocation strategy.
    pub fn allocate_message_queue_strategy(
        mut self,
        strategy: Arc<dyn AllocateMessageQueueStrategy + Send + Sync>,
    ) -> Self {
        self.allocate_message_queue_strategy = strategy;
        self
    }

    /// Sets whether the subscription group runs in unit mode (default: false).
    pub fn unit_mode(mut self, unit_mode: bool) -> Self {
        self.unit_mode = unit_mode;
        self
    }

    /// Sets the number of messages to pull in a single request (default: 10, range: 1-1024).
    pub fn pull_batch_size(mut self, size: i32) -> Self {
        self.pull_batch_size = size;
        self
    }

    /// Sets the number of concurrent pull threads (default: 20).
    pub fn pull_thread_nums(mut self, nums: usize) -> Self {
        self.pull_thread_nums = nums;
        self
    }

    /// Sets whether pulls should always use the configured default broker ID.
    pub fn connect_broker_by_user(mut self, connect: bool) -> Self {
        self.connect_broker_by_user = connect;
        self
    }

    /// Sets the broker ID used when user-controlled broker selection is enabled.
    pub fn default_broker_id(mut self, broker_id: u64) -> Self {
        self.default_broker_id = broker_id;
        self
    }

    /// Sets the maximum number of messages cached per queue (default: 1000).
    pub fn pull_threshold_for_queue(mut self, threshold: i64) -> Self {
        self.pull_threshold_for_queue = threshold;
        self
    }

    /// Sets the maximum size in MiB of messages cached per queue (default: 100).
    pub fn pull_threshold_size_for_queue(mut self, threshold: i32) -> Self {
        self.pull_threshold_size_for_queue = threshold;
        self
    }

    /// Sets the maximum total number of cached messages across all queues (default: 10000).
    pub fn pull_threshold_for_all(mut self, threshold: i64) -> Self {
        self.pull_threshold_for_all = threshold;
        self
    }

    /// Sets the maximum offset span allowed in a process queue (default: 2000).
    pub fn consume_max_span(mut self, span: i64) -> Self {
        self.consume_max_span = span;
        self
    }

    /// Sets the delay when pull encounters an exception (default: 1000ms).
    pub fn pull_time_delay_millis_when_exception(mut self, delay: u64) -> Self {
        self.pull_time_delay_millis_when_exception = delay;
        self
    }

    /// Sets the delay when cache flow control is triggered (default: 50ms).
    pub fn pull_time_delay_millis_when_cache_flow_control(mut self, delay: u64) -> Self {
        self.pull_time_delay_millis_when_cache_flow_control = delay;
        self
    }

    /// Sets the delay when broker flow control is triggered (default: 20ms).
    pub fn pull_time_delay_millis_when_broker_flow_control(mut self, delay: u64) -> Self {
        self.pull_time_delay_millis_when_broker_flow_control = delay;
        self
    }

    /// Sets the maximum time that the broker may suspend a long-poll pull request (default:
    /// 20000ms).
    pub fn broker_suspend_max_time_millis(mut self, timeout: u64) -> Self {
        self.broker_suspend_max_time_millis = timeout;
        self
    }

    /// Sets the consumer-side timeout for suspended long-poll pull requests (default: 30000ms).
    pub fn consumer_timeout_millis_when_suspend(mut self, timeout: u64) -> Self {
        self.consumer_timeout_millis_when_suspend = timeout;
        self
    }

    /// Sets the lite pull RPC timeout in milliseconds (default: 10000ms).
    pub fn consumer_pull_timeout_millis(mut self, timeout: u64) -> Self {
        self.consumer_pull_timeout_millis = timeout;
        self
    }

    /// Sets the default poll timeout in milliseconds (default: 5000).
    pub fn poll_timeout_millis(mut self, timeout: u64) -> Self {
        self.poll_timeout_millis = timeout;
        self
    }

    /// Sets whether to automatically commit offsets (default: true).
    pub fn auto_commit(mut self, enable: bool) -> Self {
        self.auto_commit = enable;
        self
    }

    /// Sets the interval between automatic offset commits (default: 5000ms, minimum: 1000ms).
    pub fn auto_commit_interval_millis(mut self, interval: u64) -> Self {
        if interval >= MIN_AUTOCOMMIT_INTERVAL_MILLIS {
            self.auto_commit_interval_millis = interval;
        }
        self
    }

    /// Sets the interval for checking topic metadata changes (default: 30000ms).
    pub fn topic_metadata_check_interval_millis(mut self, interval: u64) -> Self {
        self.topic_metadata_check_interval_millis = interval;
        self
    }

    /// Sets the message request mode (default: Pull).
    pub fn message_request_mode(mut self, mode: MessageRequestMode) -> Self {
        self.message_request_mode = mode;
        self
    }

    /// Sets the RPC hook for request/response interception.
    pub fn rpc_hook(mut self, hook: Arc<dyn RPCHook>) -> Self {
        self.rpc_hook = Some(hook);
        self
    }

    /// Enables message trace with the default trace topic.
    pub fn enable_msg_trace(mut self) -> Self {
        self.enable_msg_trace = true;
        self
    }

    /// Enables message trace with a custom trace topic.
    pub fn enable_msg_trace_with_topic(mut self, trace_topic: impl Into<CheetahString>) -> Self {
        self.enable_msg_trace = true;
        self.custom_trace_topic = Some(trace_topic.into());
        self
    }

    /// Sets a custom trace dispatcher.
    pub fn trace_dispatcher(mut self, dispatcher: Arc<dyn TraceDispatcher + Send + Sync>) -> Self {
        self.trace_dispatcher = Some(dispatcher);
        self
    }

    /// Builds the [`DefaultLitePullConsumer`].
    pub fn build(self) -> RocketMQResult<DefaultLitePullConsumer> {
        let Some(consumer_group) = self.consumer_group else {
            return Err(mq_client_err!("consumer_group is required"));
        };
        validate_lite_pull_consume_from_where(self.consume_from_where)?;

        let mut client_config = ClientConfig::default();
        client_config.set_enable_stream_request_type(true);
        if let Some(name_server_addr) = self.name_server_addr {
            client_config.set_namesrv_addr(name_server_addr);
        }

        if let Some(ip) = self.client_ip {
            client_config.set_client_ip(ip);
        }

        if let Some(name) = self.instance_name {
            client_config.set_instance_name(name);
        }

        if let Some(namespace) = self.namespace {
            client_config.set_namespace(namespace);
        }
        client_config.set_use_tls(self.use_tls);

        let consumer_config = LitePullConsumerConfig {
            consumer_group,
            message_model: self.message_model,
            consume_from_where: self.consume_from_where,
            consume_timestamp: self.consume_timestamp,
            allocate_message_queue_strategy: self.allocate_message_queue_strategy,
            unit_mode: self.unit_mode,
            pull_batch_size: self.pull_batch_size,
            pull_thread_nums: self.pull_thread_nums,
            connect_broker_by_user: self.connect_broker_by_user,
            default_broker_id: self.default_broker_id,
            pull_threshold_for_queue: self.pull_threshold_for_queue,
            pull_threshold_size_for_queue: self.pull_threshold_size_for_queue,
            pull_threshold_for_all: self.pull_threshold_for_all,
            consume_max_span: self.consume_max_span,
            pull_time_delay_millis_when_exception: self.pull_time_delay_millis_when_exception,
            pull_time_delay_millis_when_cache_flow_control: self.pull_time_delay_millis_when_cache_flow_control,
            pull_time_delay_millis_when_broker_flow_control: self.pull_time_delay_millis_when_broker_flow_control,
            broker_suspend_max_time_millis: self.broker_suspend_max_time_millis,
            consumer_timeout_millis_when_suspend: self.consumer_timeout_millis_when_suspend,
            consumer_pull_timeout_millis: self.consumer_pull_timeout_millis,
            poll_timeout_millis: self.poll_timeout_millis,
            auto_commit: self.auto_commit,
            auto_commit_interval_millis: self.auto_commit_interval_millis,
            topic_metadata_check_interval_millis: self.topic_metadata_check_interval_millis,
            message_request_mode: self.message_request_mode,
        };

        Ok(DefaultLitePullConsumer::new(
            client_config,
            consumer_config,
            self.rpc_hook,
            self.trace_dispatcher,
            self.enable_msg_trace,
            self.custom_trace_topic,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_without_consumer_group_returns_error() {
        let result = DefaultLitePullConsumerBuilder::new().build();
        match result {
            Ok(_) => panic!("builder should reject missing consumer group"),
            Err(error) => assert!(error.to_string().contains("consumer_group is required")),
        }
    }

    #[test]
    fn build_without_name_server_addr_uses_default_client_config() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use default client config");

        assert_eq!(consumer.consumer_group().as_str(), "lite_pull_group");
    }

    #[test]
    fn build_enables_stream_request_type_like_java_lite_pull_constructor() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java LitePull client id mode");

        assert!(consumer.client_config().is_enable_stream_request_type());
        assert!(consumer.client_config().build_mq_client_id().ends_with("@STREAM"));
    }

    #[test]
    fn build_use_tls_initializes_client_config_like_java_client_config() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_tls_group")
            .use_tls(true)
            .build()
            .expect("builder should apply TLS flag");

        assert!(consumer.is_use_tls());
        assert!(consumer.client_config().is_use_tls());
    }

    #[test]
    #[allow(deprecated)]
    fn build_rejects_unsupported_consume_from_where_like_java_lite_pull() {
        let result = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .consume_from_where(ConsumeFromWhere::ConsumeFromMinOffset)
            .build();

        match result {
            Ok(_) => panic!("Java LitePull rejects legacy consumeFromWhere values"),
            Err(error) => assert!(error.to_string().contains("Invalid ConsumeFromWhere Value")),
        }
    }

    #[test]
    fn default_pull_threshold_for_all_matches_java() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java default pull threshold");

        assert_eq!(consumer.consumer_config().pull_threshold_for_all, 10000);
        assert_eq!(LitePullConsumerConfig::default().pull_threshold_for_all, 10000);
    }

    #[test]
    fn pull_thread_nums_defaults_and_overrides_match_java_lite_pull() {
        let default_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java default pull thread count");

        assert_eq!(default_consumer.consumer_config().pull_thread_nums, 20);
        assert_eq!(LitePullConsumerConfig::default().pull_thread_nums, 20);

        let custom_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .pull_thread_nums(7)
            .build()
            .expect("builder should preserve configured pull thread count");

        assert_eq!(custom_consumer.consumer_config().pull_thread_nums, 7);
    }

    #[tokio::test]
    async fn broker_selection_defaults_and_overrides_match_java_lite_pull() {
        let default_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java default broker selection");

        assert!(!default_consumer.is_connect_broker_by_user().await);
        assert_eq!(default_consumer.default_broker_id().await, mix_all::MASTER_ID);
        assert!(!LitePullConsumerConfig::default().connect_broker_by_user);
        assert_eq!(LitePullConsumerConfig::default().default_broker_id, mix_all::MASTER_ID);

        let custom_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .connect_broker_by_user(true)
            .default_broker_id(2)
            .build()
            .expect("builder should preserve configured broker selection");

        assert!(custom_consumer.is_connect_broker_by_user().await);
        assert_eq!(custom_consumer.default_broker_id().await, 2);
    }

    #[tokio::test]
    async fn unit_mode_defaults_and_overrides_match_java_lite_pull() {
        let default_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java default unit mode");

        assert!(!default_consumer.is_unit_mode().await);
        assert!(!default_consumer.consumer_config().unit_mode);
        assert!(!LitePullConsumerConfig::default().unit_mode);

        let custom_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .unit_mode(true)
            .build()
            .expect("builder should preserve configured unit mode");

        assert!(custom_consumer.is_unit_mode().await);
        assert!(custom_consumer.consumer_config().unit_mode);
    }

    #[test]
    fn long_polling_timeout_defaults_and_overrides_match_java_lite_pull() {
        let default_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java default long polling timeouts");

        assert_eq!(
            default_consumer.consumer_config().broker_suspend_max_time_millis,
            20_000
        );
        assert_eq!(
            default_consumer.consumer_config().consumer_timeout_millis_when_suspend,
            30_000
        );
        assert_eq!(default_consumer.consumer_config().consumer_pull_timeout_millis, 10_000);
        assert_eq!(LitePullConsumerConfig::default().broker_suspend_max_time_millis, 20_000);
        assert_eq!(
            LitePullConsumerConfig::default().consumer_timeout_millis_when_suspend,
            30_000
        );
        assert_eq!(LitePullConsumerConfig::default().consumer_pull_timeout_millis, 10_000);

        let custom_consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .broker_suspend_max_time_millis(12_345)
            .consumer_timeout_millis_when_suspend(23_456)
            .consumer_pull_timeout_millis(34_567)
            .build()
            .expect("builder should keep configured long polling timeouts");

        assert_eq!(custom_consumer.consumer_config().broker_suspend_max_time_millis, 12_345);
        assert_eq!(
            custom_consumer.consumer_config().consumer_timeout_millis_when_suspend,
            23_456
        );
        assert_eq!(custom_consumer.consumer_config().consumer_pull_timeout_millis, 34_567);
    }

    #[test]
    fn default_consume_timestamp_matches_java_shape() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .build()
            .expect("builder should use Java default consume timestamp");
        let consumer_config = consumer.consumer_config();
        let timestamp = consumer_config
            .consume_timestamp
            .as_ref()
            .expect("Java LitePull default consume timestamp should be present");

        assert!(rocketmq_common::utils::util_all::parse_date(
            timestamp,
            rocketmq_common::utils::util_all::YYYYMMDDHHMMSS
        )
        .is_some());
        let config = LitePullConsumerConfig::default();
        let config_timestamp = config
            .consume_timestamp
            .as_ref()
            .expect("Java LitePull config default consume timestamp should be present");
        assert!(rocketmq_common::utils::util_all::parse_date(
            config_timestamp,
            rocketmq_common::utils::util_all::YYYYMMDDHHMMSS
        )
        .is_some());
    }

    #[test]
    fn auto_commit_interval_below_java_minimum_keeps_default() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .auto_commit_interval_millis(MIN_AUTOCOMMIT_INTERVAL_MILLIS - 1)
            .build()
            .expect("builder should ignore invalid auto commit interval");

        assert_eq!(consumer.consumer_config().auto_commit_interval_millis, 5000);
    }

    #[test]
    fn auto_commit_interval_below_java_minimum_keeps_previous_valid_value() {
        let consumer = DefaultLitePullConsumerBuilder::new()
            .consumer_group("lite_pull_group")
            .auto_commit_interval_millis(2000)
            .auto_commit_interval_millis(MIN_AUTOCOMMIT_INTERVAL_MILLIS - 1)
            .build()
            .expect("builder should keep previous valid auto commit interval");

        assert_eq!(consumer.consumer_config().auto_commit_interval_millis, 2000);
    }
}
