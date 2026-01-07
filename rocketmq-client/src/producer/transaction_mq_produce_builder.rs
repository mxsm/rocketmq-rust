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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::compression::compression_type::CompressionType;
use rocketmq_common::common::compression::compressor::Compressor;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_runtime::RocketMQRuntime;

use crate::base::client_config::ClientConfig;
use crate::producer::default_mq_producer::DefaultMQProducer;
use crate::producer::produce_accumulator::ProduceAccumulator;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::transaction_listener::TransactionListener;
use crate::producer::transaction_mq_producer::TransactionMQProducer;
use crate::producer::transaction_mq_producer::TransactionProducerConfig;
use crate::trace::trace_dispatcher::TraceDispatcher;

#[derive(Default)]
pub struct TransactionMQProducerBuilder {
    client_config: Option<ClientConfig>,
    default_mqproducer_impl: Option<DefaultMQProducerImpl>,
    retry_response_codes: Option<HashSet<i32>>,
    producer_group: Option<CheetahString>,
    topics: Option<Vec<CheetahString>>,
    create_topic_key: Option<CheetahString>,
    default_topic_queue_nums: Option<u32>,
    send_msg_timeout: Option<u32>,
    compress_msg_body_over_howmuch: Option<u32>,
    retry_times_when_send_failed: Option<u32>,
    retry_times_when_send_async_failed: Option<u32>,
    retry_another_broker_when_not_store_ok: Option<bool>,
    max_message_size: Option<u32>,
    trace_dispatcher: Option<Arc<Box<dyn TraceDispatcher + Send + Sync>>>,
    auto_batch: Option<bool>,
    produce_accumulator: Option<ProduceAccumulator>,
    enable_backpressure_for_async_mode: Option<bool>,
    back_pressure_for_async_send_num: Option<u32>,
    back_pressure_for_async_send_size: Option<u32>,
    rpc_hook: Option<Arc<dyn RPCHook>>,
    compress_level: Option<i32>,
    compress_type: Option<CompressionType>,
    compressor: Option<&'static (dyn Compressor + Send + Sync)>,
    transaction_listener: Option<Arc<Box<dyn TransactionListener>>>,
    check_runtime: Option<Arc<RocketMQRuntime>>,
}

impl TransactionMQProducerBuilder {
    pub fn new() -> Self {
        Self {
            client_config: Some(Default::default()),
            default_mqproducer_impl: None,
            retry_response_codes: None,
            producer_group: None,
            topics: None,
            create_topic_key: None,
            default_topic_queue_nums: None,
            send_msg_timeout: None,
            compress_msg_body_over_howmuch: None,
            retry_times_when_send_failed: None,
            retry_times_when_send_async_failed: None,
            retry_another_broker_when_not_store_ok: None,
            max_message_size: None,
            trace_dispatcher: None,
            auto_batch: None,
            produce_accumulator: None,
            enable_backpressure_for_async_mode: None,
            back_pressure_for_async_send_num: None,
            back_pressure_for_async_send_size: None,
            rpc_hook: None,
            compress_level: None,
            compress_type: None,
            compressor: None,
            transaction_listener: None,
            check_runtime: None,
        }
    }

    pub fn client_config(mut self, client_config: ClientConfig) -> Self {
        self.client_config = Some(client_config);
        self
    }

    pub fn default_mqproducer_impl(mut self, default_mqproducer_impl: DefaultMQProducerImpl) -> Self {
        self.default_mqproducer_impl = Some(default_mqproducer_impl);
        self
    }

    pub fn retry_response_codes(mut self, retry_response_codes: HashSet<i32>) -> Self {
        self.retry_response_codes = Some(retry_response_codes);
        self
    }

    pub fn producer_group(mut self, producer_group: impl Into<CheetahString>) -> Self {
        self.producer_group = Some(producer_group.into());
        self
    }

    pub fn topics(mut self, topics: Vec<impl Into<CheetahString>>) -> Self {
        self.topics = Some(topics.into_iter().map(|t| t.into()).collect());
        self
    }

    pub fn name_server_addr(mut self, name_server_addr: impl Into<CheetahString>) -> Self {
        if let Some(client_config) = self.client_config.as_mut() {
            client_config.namesrv_addr = Some(name_server_addr.into());
            client_config
                .namespace_initialized
                .store(false, std::sync::atomic::Ordering::Release);
        }
        self
    }

    pub fn create_topic_key(mut self, create_topic_key: impl Into<CheetahString>) -> Self {
        self.create_topic_key = Some(create_topic_key.into());
        self
    }

    pub fn default_topic_queue_nums(mut self, default_topic_queue_nums: u32) -> Self {
        self.default_topic_queue_nums = Some(default_topic_queue_nums);
        self
    }

    pub fn send_msg_timeout(mut self, send_msg_timeout: u32) -> Self {
        self.send_msg_timeout = Some(send_msg_timeout);
        self
    }

    pub fn compress_msg_body_over_howmuch(mut self, compress_msg_body_over_howmuch: u32) -> Self {
        self.compress_msg_body_over_howmuch = Some(compress_msg_body_over_howmuch);
        self
    }

    pub fn retry_times_when_send_failed(mut self, retry_times_when_send_failed: u32) -> Self {
        self.retry_times_when_send_failed = Some(retry_times_when_send_failed);
        self
    }

    pub fn retry_times_when_send_async_failed(mut self, retry_times_when_send_async_failed: u32) -> Self {
        self.retry_times_when_send_async_failed = Some(retry_times_when_send_async_failed);
        self
    }

    pub fn retry_another_broker_when_not_store_ok(mut self, retry_another_broker_when_not_store_ok: bool) -> Self {
        self.retry_another_broker_when_not_store_ok = Some(retry_another_broker_when_not_store_ok);
        self
    }

    pub fn max_message_size(mut self, max_message_size: u32) -> Self {
        self.max_message_size = Some(max_message_size);
        self
    }

    pub fn trace_dispatcher(mut self, trace_dispatcher: Arc<Box<dyn TraceDispatcher + Send + Sync>>) -> Self {
        self.trace_dispatcher = Some(trace_dispatcher);
        self
    }

    pub fn auto_batch(mut self, auto_batch: bool) -> Self {
        self.auto_batch = Some(auto_batch);
        self
    }

    pub fn produce_accumulator(mut self, produce_accumulator: ProduceAccumulator) -> Self {
        self.produce_accumulator = Some(produce_accumulator);
        self
    }

    pub fn enable_backpressure_for_async_mode(mut self, enable_backpressure_for_async_mode: bool) -> Self {
        self.enable_backpressure_for_async_mode = Some(enable_backpressure_for_async_mode);
        self
    }

    pub fn back_pressure_for_async_send_num(mut self, back_pressure_for_async_send_num: u32) -> Self {
        self.back_pressure_for_async_send_num = Some(back_pressure_for_async_send_num);
        self
    }

    pub fn back_pressure_for_async_send_size(mut self, back_pressure_for_async_send_size: u32) -> Self {
        self.back_pressure_for_async_send_size = Some(back_pressure_for_async_send_size);
        self
    }

    pub fn rpc_hook(mut self, rpc_hook: Arc<dyn RPCHook>) -> Self {
        self.rpc_hook = Some(rpc_hook);
        self
    }

    pub fn compress_level(mut self, compress_level: i32) -> Self {
        self.compress_level = Some(compress_level);
        self
    }

    pub fn compress_type(mut self, compress_type: CompressionType) -> Self {
        self.compress_type = Some(compress_type);
        self
    }

    pub fn compressor(mut self, compressor: &'static (dyn Compressor + Send + Sync)) -> Self {
        self.compressor = Some(compressor);
        self
    }

    pub fn transaction_listener(mut self, transaction_listener: impl TransactionListener) -> Self {
        self.transaction_listener = Some(Arc::new(Box::new(transaction_listener)));
        self
    }

    pub fn build(self) -> TransactionMQProducer {
        let mut mq_producer = DefaultMQProducer::default();
        if let Some(client_config) = self.client_config {
            mq_producer.set_client_config(client_config);
        }

        if let Some(retry_response_codes) = self.retry_response_codes {
            mq_producer.set_retry_response_codes(retry_response_codes);
        }
        if let Some(producer_group) = self.producer_group {
            mq_producer.set_producer_group(producer_group);
        }
        if let Some(topics) = self.topics {
            mq_producer.set_topics(topics);
        }
        if let Some(create_topic_key) = self.create_topic_key {
            mq_producer.set_create_topic_key(create_topic_key);
        }
        if let Some(default_topic_queue_nums) = self.default_topic_queue_nums {
            mq_producer.set_default_topic_queue_nums(default_topic_queue_nums);
        }
        if let Some(send_msg_timeout) = self.send_msg_timeout {
            mq_producer.set_send_msg_timeout(send_msg_timeout);
        }
        if let Some(compress_msg_body_over_howmuch) = self.compress_msg_body_over_howmuch {
            mq_producer.set_compress_msg_body_over_howmuch(compress_msg_body_over_howmuch);
        }
        if let Some(retry_times_when_send_failed) = self.retry_times_when_send_failed {
            mq_producer.set_retry_times_when_send_failed(retry_times_when_send_failed);
        }
        if let Some(retry_times_when_send_async_failed) = self.retry_times_when_send_async_failed {
            mq_producer.set_retry_times_when_send_async_failed(retry_times_when_send_async_failed);
        }
        if let Some(retry_another_broker_when_not_store_ok) = self.retry_another_broker_when_not_store_ok {
            mq_producer.set_retry_another_broker_when_not_store_ok(retry_another_broker_when_not_store_ok);
        }
        if let Some(max_message_size) = self.max_message_size {
            mq_producer.set_max_message_size(max_message_size);
        }

        if let Some(trace_dispatcher) = self.trace_dispatcher {
            mq_producer.set_trace_dispatcher(trace_dispatcher);
        }
        if let Some(auto_batch) = self.auto_batch {
            mq_producer.set_auto_batch(auto_batch);
        }
        if let Some(produce_accumulator) = self.produce_accumulator {
            mq_producer.set_produce_accumulator(produce_accumulator);
        }

        if let Some(enable_backpressure_for_async_mode) = self.enable_backpressure_for_async_mode {
            mq_producer.set_enable_backpressure_for_async_mode(enable_backpressure_for_async_mode);
        }
        if let Some(back_pressure_for_async_send_num) = self.back_pressure_for_async_send_num {
            mq_producer.set_back_pressure_for_async_send_num(back_pressure_for_async_send_num);
        }
        if let Some(back_pressure_for_async_send_size) = self.back_pressure_for_async_send_size {
            mq_producer.set_back_pressure_for_async_send_size(back_pressure_for_async_send_size);
        }
        if let Some(rpc_hook) = self.rpc_hook {
            mq_producer.set_rpc_hook(rpc_hook);
        }
        if let Some(compress_level) = self.compress_level {
            mq_producer.set_compress_level(compress_level);
        }
        if let Some(compress_type) = self.compress_type {
            mq_producer.set_compress_type(compress_type);
        }
        if let Some(compressor) = self.compressor {
            mq_producer.set_compressor(Some(compressor));
        }

        if let Some(default_mqproducer_impl) = self.default_mqproducer_impl {
            mq_producer.set_default_mqproducer_impl(default_mqproducer_impl);
        } else {
            let producer_impl = DefaultMQProducerImpl::new(
                mq_producer.client_config().clone(),
                mq_producer.producer_config().clone(),
                mq_producer.rpc_hook().clone(),
            );
            mq_producer.set_default_mqproducer_impl(producer_impl);
        }
        let transaction_producer_config = TransactionProducerConfig {
            transaction_listener: self.transaction_listener,
            check_thread_pool_min_size: 0,
            check_thread_pool_max_size: 0,
            check_request_hold_max: 0,
            check_runtime: self.check_runtime,
        };
        TransactionMQProducer::new(transaction_producer_config, mq_producer)
    }

    pub fn check_runtime(&mut self, check_runtime: RocketMQRuntime) {
        self.check_runtime = Some(Arc::new(check_runtime));
    }
}
