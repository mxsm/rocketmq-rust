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

use std::sync::atomic::AtomicI64;
use std::sync::Arc;

#[allow(unused_imports)]
use rocketmq_client_rust::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::ConsumeOrderlyContext;
use rocketmq_client_rust::ConsumeOrderlyStatus;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageListenerOrderly;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use tracing::info;

pub const MESSAGE_COUNT: usize = 1;
pub const CONSUMER_GROUP: &str = "please_rename_unique_group_name_3";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

#[tokio::main]
pub async fn main() -> RocketMQResult<()> {
    let example_runtime = support::ExampleClientRuntime::try_new("ordermessage-consumer")?;
    let client_runtime = example_runtime.client_runtime();
    // create a producer builder with default configuration
    let builder = DefaultMQPushConsumer::builder(client_runtime.clone());

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .message_model(MessageModel::Clustering)
        .build();
    consumer.subscribe(TOPIC, TAG).await?;
    consumer.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    consumer.register_message_listener_orderly(MyMessageListener::new());
    consumer.start().await?;
    let _ = tokio::signal::ctrl_c().await;

    consumer.shutdown().await;
    example_runtime.shutdown().await;

    Ok(())
}

pub struct MyMessageListener {
    consume_times: Arc<AtomicI64>,
}

impl Default for MyMessageListener {
    fn default() -> Self {
        Self::new()
    }
}

impl MyMessageListener {
    pub fn new() -> Self {
        Self {
            consume_times: Arc::new(AtomicI64::new(0)),
        }
    }
}

impl MessageListenerOrderly for MyMessageListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> RocketMQResult<ConsumeOrderlyStatus> {
        context.set_auto_commit(true);
        for msg in msgs {
            println!("Receive message: {:?}", msg);
            info!("Receive message: {:?}", msg);
        }
        if self.consume_times.load(std::sync::atomic::Ordering::Acquire) % 2 == 0 {
            return Ok(ConsumeOrderlyStatus::Success);
        } else if self.consume_times.load(std::sync::atomic::Ordering::Acquire) % 5 == 0 {
            context.set_suspend_current_queue_time_millis(3000);
            return Ok(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        }
        Ok(ConsumeOrderlyStatus::Success)
    }
}
