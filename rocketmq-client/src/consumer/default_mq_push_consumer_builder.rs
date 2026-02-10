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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;

use crate::base::client_config::ClientConfig;
use crate::consumer::allocate_message_queue_strategy::AllocateMessageQueueStrategy;
use crate::consumer::default_mq_push_consumer::ConsumerConfig;
use crate::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use crate::consumer::message_queue_listener::MessageQueueListener;
use crate::trace::trace_dispatcher::TraceDispatcher;

#[derive(Default)]
pub struct DefaultMQPushConsumerBuilder {
    client_config: ClientConfig,
    consumer_group: Option<CheetahString>,
    message_model: Option<MessageModel>,
    consume_from_where: Option<ConsumeFromWhere>,
    consume_timestamp: Option<CheetahString>,
    allocate_message_queue_strategy: Option<Arc<dyn AllocateMessageQueueStrategy>>,
    subscription: Option<ArcMut<HashMap<CheetahString, CheetahString>>>,

    message_queue_listener: Option<Arc<Box<dyn MessageQueueListener>>>,

    consume_thread_min: Option<u32>,
    consume_thread_max: Option<u32>,
    adjust_thread_pool_nums_threshold: Option<u64>,
    consume_concurrently_max_span: Option<u32>,
    pull_threshold_for_queue: Option<u32>,
    pop_threshold_for_queue: Option<u32>,
    pull_threshold_size_for_queue: Option<u32>,
    pull_threshold_for_topic: Option<i32>,
    pull_threshold_size_for_topic: Option<i32>,
    pull_interval: Option<u64>,
    consume_message_batch_max_size: Option<u32>,
    pull_batch_size: Option<u32>,
    pull_batch_size_in_bytes: Option<u32>,
    post_subscription_when_pull: Option<bool>,
    unit_mode: Option<bool>,
    max_reconsume_times: Option<i32>,
    suspend_current_queue_time_millis: Option<u64>,
    consume_timeout: Option<u64>,
    pop_invisible_time: Option<u64>,
    pop_batch_nums: Option<u32>,
    await_termination_millis_when_shutdown: Option<u64>,
    trace_dispatcher: Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>>,
    client_rebalance: Option<bool>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
}

impl DefaultMQPushConsumerBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn name_server_addr(mut self, name_server_addr: impl Into<CheetahString>) -> Self {
        self.client_config.namesrv_addr = Some(name_server_addr.into());
        self.client_config
            .namespace_initialized
            .store(false, std::sync::atomic::Ordering::Release);
        self
    }

    #[inline]
    pub fn client_config(mut self, client_config: ClientConfig) -> Self {
        self.client_config = client_config;
        self
    }

    // Methods to set each field
    #[inline]
    pub fn consumer_group(mut self, consumer_group: impl Into<CheetahString>) -> Self {
        self.consumer_group = Some(consumer_group.into());
        self
    }

    #[inline]
    pub fn message_model(mut self, message_model: MessageModel) -> Self {
        self.message_model = Some(message_model);
        self
    }

    #[inline]
    pub fn consume_from_where(mut self, consume_from_where: ConsumeFromWhere) -> Self {
        self.consume_from_where = Some(consume_from_where);
        self
    }

    #[inline]
    pub fn consume_timestamp(mut self, consume_timestamp: impl Into<CheetahString>) -> Self {
        self.consume_timestamp = Some(consume_timestamp.into());
        self
    }

    #[inline]
    pub fn allocate_message_queue_strategy(
        mut self,
        allocate_message_queue_strategy: Arc<dyn AllocateMessageQueueStrategy>,
    ) -> Self {
        self.allocate_message_queue_strategy = Some(allocate_message_queue_strategy);
        self
    }

    #[inline]
    pub fn message_queue_listener(
        mut self,
        message_queue_listener: Option<Arc<Box<dyn MessageQueueListener>>>,
    ) -> Self {
        self.message_queue_listener = message_queue_listener;
        self
    }

    #[inline]
    pub fn consume_thread_min(mut self, consume_thread_min: u32) -> Self {
        self.consume_thread_min = Some(consume_thread_min);
        self
    }

    #[inline]
    pub fn consume_thread_max(mut self, consume_thread_max: u32) -> Self {
        self.consume_thread_max = Some(consume_thread_max);
        self
    }

    #[inline]
    pub fn adjust_thread_pool_nums_threshold(mut self, adjust_thread_pool_nums_threshold: u64) -> Self {
        self.adjust_thread_pool_nums_threshold = Some(adjust_thread_pool_nums_threshold);
        self
    }

    #[inline]
    pub fn consume_concurrently_max_span(mut self, consume_concurrently_max_span: u32) -> Self {
        self.consume_concurrently_max_span = Some(consume_concurrently_max_span);
        self
    }

    #[inline]
    pub fn pull_threshold_for_queue(mut self, pull_threshold_for_queue: u32) -> Self {
        self.pull_threshold_for_queue = Some(pull_threshold_for_queue);
        self
    }

    #[inline]
    pub fn pop_threshold_for_queue(mut self, pop_threshold_for_queue: u32) -> Self {
        self.pop_threshold_for_queue = Some(pop_threshold_for_queue);
        self
    }

    #[inline]
    pub fn pull_threshold_size_for_queue(mut self, pull_threshold_size_for_queue: u32) -> Self {
        self.pull_threshold_size_for_queue = Some(pull_threshold_size_for_queue);
        self
    }

    #[inline]
    pub fn pull_threshold_for_topic(mut self, pull_threshold_for_topic: i32) -> Self {
        self.pull_threshold_for_topic = Some(pull_threshold_for_topic);
        self
    }

    #[inline]
    pub fn pull_threshold_size_for_topic(mut self, pull_threshold_size_for_topic: i32) -> Self {
        self.pull_threshold_size_for_topic = Some(pull_threshold_size_for_topic);
        self
    }

    #[inline]
    pub fn pull_interval(mut self, pull_interval: u64) -> Self {
        self.pull_interval = Some(pull_interval);
        self
    }

    #[inline]
    pub fn consume_message_batch_max_size(mut self, consume_message_batch_max_size: u32) -> Self {
        self.consume_message_batch_max_size = Some(consume_message_batch_max_size);
        self
    }

    #[inline]
    pub fn pull_batch_size(mut self, pull_batch_size: u32) -> Self {
        self.pull_batch_size = Some(pull_batch_size);
        self
    }

    #[inline]
    pub fn pull_batch_size_in_bytes(mut self, pull_batch_size_in_bytes: u32) -> Self {
        self.pull_batch_size_in_bytes = Some(pull_batch_size_in_bytes);
        self
    }

    #[inline]
    pub fn post_subscription_when_pull(mut self, post_subscription_when_pull: bool) -> Self {
        self.post_subscription_when_pull = Some(post_subscription_when_pull);
        self
    }

    #[inline]
    pub fn unit_mode(mut self, unit_mode: bool) -> Self {
        self.unit_mode = Some(unit_mode);
        self
    }

    #[inline]
    pub fn max_reconsume_times(mut self, max_reconsume_times: i32) -> Self {
        self.max_reconsume_times = Some(max_reconsume_times);
        self
    }

    #[inline]
    pub fn suspend_current_queue_time_millis(mut self, suspend_current_queue_time_millis: u64) -> Self {
        self.suspend_current_queue_time_millis = Some(suspend_current_queue_time_millis);
        self
    }

    #[inline]
    pub fn consume_timeout(mut self, consume_timeout: u64) -> Self {
        self.consume_timeout = Some(consume_timeout);
        self
    }

    #[inline]
    pub fn pop_invisible_time(mut self, pop_invisible_time: u64) -> Self {
        self.pop_invisible_time = Some(pop_invisible_time);
        self
    }

    #[inline]
    pub fn pop_batch_nums(mut self, pop_batch_nums: u32) -> Self {
        self.pop_batch_nums = Some(pop_batch_nums);
        self
    }

    #[inline]
    pub fn await_termination_millis_when_shutdown(mut self, await_termination_millis_when_shutdown: u64) -> Self {
        self.await_termination_millis_when_shutdown = Some(await_termination_millis_when_shutdown);
        self
    }

    #[inline]
    pub fn trace_dispatcher(mut self, trace_dispatcher: Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>>) -> Self {
        self.trace_dispatcher = trace_dispatcher;
        self
    }

    #[inline]
    pub fn client_rebalance(mut self, client_rebalance: bool) -> Self {
        self.client_rebalance = Some(client_rebalance);
        self
    }

    #[inline]
    pub fn rpc_hook(mut self, rpc_hook: Option<Arc<dyn RPCHook>>) -> Self {
        self.rpc_hook = rpc_hook;
        self
    }

    // Build method to create a DefaultMQPushConsumer instance
    pub fn build(mut self) -> DefaultMQPushConsumer {
        let mut consumer_config = ConsumerConfig::default();

        // Set optional fields with consistent pattern
        if let Some(value) = self.consumer_group {
            consumer_config.consumer_group = value;
        }
        if let Some(value) = self.message_model {
            consumer_config.message_model = value;
        }
        if let Some(value) = self.consume_from_where {
            consumer_config.consume_from_where = value;
        }
        consumer_config.consume_timestamp = self.consume_timestamp.take();
        consumer_config.allocate_message_queue_strategy = self.allocate_message_queue_strategy.take();
        if let Some(value) = self.subscription {
            consumer_config.subscription = value;
        }
        consumer_config.message_queue_listener = self.message_queue_listener.take();

        // Thread pool configuration
        if let Some(value) = self.consume_thread_min {
            consumer_config.consume_thread_min = value;
        }
        if let Some(value) = self.consume_thread_max {
            consumer_config.consume_thread_max = value;
        }
        if let Some(value) = self.adjust_thread_pool_nums_threshold {
            consumer_config.adjust_thread_pool_nums_threshold = value;
        }

        // Pull thresholds
        if let Some(value) = self.consume_concurrently_max_span {
            consumer_config.consume_concurrently_max_span = value;
        }
        if let Some(value) = self.pull_threshold_for_queue {
            consumer_config.pull_threshold_for_queue = value;
        }
        if let Some(value) = self.pop_threshold_for_queue {
            consumer_config.pop_threshold_for_queue = value;
        }
        if let Some(value) = self.pull_threshold_size_for_queue {
            consumer_config.pull_threshold_size_for_queue = value;
        }
        if let Some(value) = self.pull_threshold_for_topic {
            consumer_config.pull_threshold_for_topic = value;
        }
        if let Some(value) = self.pull_threshold_size_for_topic {
            consumer_config.pull_threshold_size_for_topic = value;
        }
        if let Some(value) = self.pull_interval {
            consumer_config.pull_interval = value;
        }

        // Batch configuration
        if let Some(value) = self.consume_message_batch_max_size {
            consumer_config.consume_message_batch_max_size = value;
        }
        if let Some(value) = self.pull_batch_size {
            consumer_config.pull_batch_size = value;
        }
        if let Some(value) = self.pull_batch_size_in_bytes {
            consumer_config.pull_batch_size_in_bytes = value;
        }

        // Other configuration
        if let Some(value) = self.post_subscription_when_pull {
            consumer_config.post_subscription_when_pull = value;
        }
        if let Some(value) = self.unit_mode {
            consumer_config.unit_mode = value;
        }
        if let Some(value) = self.max_reconsume_times {
            consumer_config.max_reconsume_times = value;
        }
        if let Some(value) = self.suspend_current_queue_time_millis {
            consumer_config.suspend_current_queue_time_millis = value;
        }
        if let Some(value) = self.consume_timeout {
            consumer_config.consume_timeout = value;
        }
        if let Some(value) = self.pop_invisible_time {
            consumer_config.pop_invisible_time = value;
        }
        if let Some(value) = self.pop_batch_nums {
            consumer_config.pop_batch_nums = value;
        }
        if let Some(value) = self.await_termination_millis_when_shutdown {
            consumer_config.await_termination_millis_when_shutdown = value;
        }

        // Trace and RPC configuration
        consumer_config.trace_dispatcher = self.trace_dispatcher.clone();
        if let Some(value) = self.client_rebalance {
            consumer_config.client_rebalance = value;
        }
        consumer_config.rpc_hook = self.rpc_hook.clone();

        DefaultMQPushConsumer::new(self.client_config, consumer_config)
    }
}
