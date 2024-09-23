/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use rocketmq_client::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_client::Result;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_rust::rocketmq;
use tracing::info;

pub const MESSAGE_COUNT: usize = 1;
pub const CONSUMER_GROUP: &str = "please_rename_unique_group_name_4";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

#[rocketmq::main]
pub async fn main() -> Result<()> {
    //init logger
    rocketmq_common::log::init_logger();

    // create a producer builder with default configuration
    let builder = DefaultMQPushConsumer::builder();

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();
    consumer.subscribe(TOPIC, "*")?;
    consumer.register_message_listener_concurrently(MyMessageListener);
    consumer.start().await?;
    let _ = tokio::signal::ctrl_c().await;
    Ok(())
}

pub struct MyMessageListener;

impl MessageListenerConcurrently for MyMessageListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> Result<ConsumeConcurrentlyStatus> {
        for msg in msgs {
            info!("Receive message: {:?}", msg);
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
