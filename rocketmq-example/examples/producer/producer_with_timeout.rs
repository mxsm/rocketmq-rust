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

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const MESSAGE_COUNT: usize = 1;
pub const PRODUCER_GROUP: &str = "producer_with_timeout";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "ProducerTimeoutTest";
pub const TAG: &str = "TagA";

// Send timeout in milliseconds
pub const SEND_TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    // init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    // Example 1: Set delay level (1-18 for predefined levels)
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Hello RocketMQ with delay level")
        .delay_level(3)
        .build()?;
    let send_result = producer.send_with_timeout(message, SEND_TIMEOUT_MS).await?;
    println!("send result with delay level: {:?}", send_result);

    // Example 2: Set delay time in seconds
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Hello RocketMQ with delay seconds")
        .delay_secs(60)
        .build()?;
    let send_result = producer.send_with_timeout(message, SEND_TIMEOUT_MS).await?;
    println!("send result with delay seconds: {:?}", send_result);

    // Example 3: Set delay time in milliseconds
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Hello RocketMQ with delay millis")
        .delay_millis(5000)
        .build()?;
    let send_result = producer.send_with_timeout(message, SEND_TIMEOUT_MS).await?;
    println!("send result with delay millis: {:?}", send_result);

    // Example 4: Set delivery time (absolute timestamp in milliseconds)
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let deliver_time = current_time + 10_000; // deliver after 10 seconds
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .body("Hello RocketMQ with deliver time")
        .deliver_time_ms(deliver_time)
        .build()?;
    let send_result = producer.send_with_timeout(message, SEND_TIMEOUT_MS).await?;
    println!("send result with deliver time: {:?}", send_result);

    producer.shutdown().await;
    Ok(())
}
