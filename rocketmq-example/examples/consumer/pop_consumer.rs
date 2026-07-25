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

#![recursion_limit = "256"]

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_runtime::wait_for_signal;
use rocketmq_tools::client_adapter::AdminBuilder;
use rocketmq_tools::core::consumer::ConsumerAdmin;
use rocketmq_tools::core::consumer::ConsumerRequestMode;
use rocketmq_tools::core::consumer::SetConsumerRequestModeRequest;
use tracing::info;
pub const MESSAGE_COUNT: usize = 1;
pub const CONSUMER_GROUP: &str = "please_rename_unique_group_name_4";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

#[path = "../support/mod.rs"]
mod support;

pub fn main() -> RocketMQResult<()> {
    support::run(run)
}

async fn run(client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>) -> RocketMQResult<()> {
    let telemetry_guard =
        rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
            .expect("telemetry logging bootstrap should initialize");
    switch_pop_consumer(client_runtime.clone()).await?;

    // create a producer builder with default configuration
    let builder = DefaultMQPushConsumer::builder(client_runtime.clone());

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        // disable client side load balance, also is pop consumer
        .client_rebalance(false)
        .build();
    consumer.subscribe(TOPIC, "*").await?;
    consumer.register_message_listener_concurrently(MyMessageListener);
    consumer.start().await?;
    let _ = wait_for_signal().await;
    telemetry_guard
        .shutdown()
        .into_result()
        .expect("telemetry logging shutdown should succeed");

    Ok(())
}

async fn switch_pop_consumer(
    client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
) -> RocketMQResult<()> {
    let mut admin = AdminBuilder::new(client_runtime)
        .namesrv_addr(DEFAULT_NAMESRVADDR)
        .build_and_start()
        .await
        .map_err(|error| rocketmq_error::RocketMQError::Internal(error.to_string()))?;
    let request = SetConsumerRequestModeRequest::try_new(TOPIC, CONSUMER_GROUP, ConsumerRequestMode::Pop, 8, 3_000)
        .map_err(|error| rocketmq_error::RocketMQError::Internal(error.to_string()))?;
    let result = admin.set_consumer_request_mode(&request).await;
    admin.shutdown().await;
    result
        .map(|_| ())
        .map_err(|error| rocketmq_error::RocketMQError::Internal(error.to_string()))
}

pub struct MyMessageListener;

impl MessageListenerConcurrently for MyMessageListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in msgs {
            info!("Receive message: {:?}", msg);
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
