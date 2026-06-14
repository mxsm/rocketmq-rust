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

//! Demonstrates delayed message send modes.

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_delay_send_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "DelaySendTestTopic";
pub const TAG: &str = "DelayTag";
pub const TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    send_delay_level(&mut producer).await?;
    send_delay_seconds(&mut producer).await?;
    send_delay_millis(&mut producer).await?;
    send_deliver_time(&mut producer).await?;

    producer.shutdown().await;
    Ok(())
}

async fn send_delay_level(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .key("delay-level-key")
        .body("message delayed by broker delay level")
        .delay_level(3)
        .build()?;

    let result = producer.send_with_timeout(message, TIMEOUT_MS).await?;
    println!("delay_level send result: {:?}", result);
    Ok(())
}

async fn send_delay_seconds(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .key("delay-seconds-key")
        .body("message delayed by seconds")
        .delay_secs(5)
        .build()?;

    let result = producer.send_with_timeout(message, TIMEOUT_MS).await?;
    println!("delay_secs send result: {:?}", result);
    Ok(())
}

async fn send_delay_millis(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .key("delay-millis-key")
        .body("message delayed by milliseconds")
        .delay_millis(5000)
        .build()?;

    let result = producer.send_with_timeout(message, TIMEOUT_MS).await?;
    println!("delay_millis send result: {:?}", result);
    Ok(())
}

async fn send_deliver_time(producer: &mut DefaultMQProducer) -> RocketMQResult<()> {
    let deliver_time_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after UNIX_EPOCH")
        .as_millis() as u64
        + 10_000;

    let message = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .key("deliver-time-key")
        .body("message delivered at an absolute timestamp")
        .deliver_time_ms(deliver_time_ms)
        .build()?;

    let result = producer.send_with_timeout(message, TIMEOUT_MS).await?;
    println!("deliver_time_ms send result: {:?}", result);
    Ok(())
}
