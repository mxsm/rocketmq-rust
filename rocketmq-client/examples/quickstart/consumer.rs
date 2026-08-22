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

#![recursion_limit = "512"]

#[path = "../support/mod.rs"]
mod support;

use rocketmq_client_rust::ConsumeConcurrentlyContext;
use rocketmq_client_rust::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageListenerConcurrently;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_ext::MessageExt;
use tracing::info;

pub const MESSAGE_COUNT: usize = 1;
pub const CONSUMER_GROUP: &str = "please_rename_unique_group_name_4";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

#[tokio::main]
pub async fn main() -> RocketMQResult<()> {
    let example_runtime = support::ExampleClientRuntime::try_new("consumer")?;
    let client_runtime = example_runtime.client_runtime();
    // create a producer builder with default configuration
    let builder = DefaultMQPushConsumer::builder(client_runtime.clone());

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();
    consumer.subscribe(TOPIC, "*").await?;
    consumer.register_message_listener_concurrently(MyMessageListener);
    consumer.start().await?;
    let _ = tokio::signal::ctrl_c().await;

    consumer.shutdown().await;
    example_runtime.shutdown().await;

    Ok(())
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
