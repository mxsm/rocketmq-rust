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

use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::thread::sleep;

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;
use tracing::info;

pub const PRODUCER_GROUP: &str = "BatchProducerGroupName";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "TagA";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let builder = DefaultMQProducer::builder();

    let mut producer = builder
        .producer_group(PRODUCER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .build();
    producer.start().await?;

    let messages = vec![
        Message::with_keys(TOPIC, TAG, "OrderID001", "Hello world 0".as_bytes()),
        Message::with_keys(TOPIC, TAG, "OrderID002", "Hello world 1".as_bytes()),
        Message::with_keys(TOPIC, TAG, "OrderID003", "Hello world 2".as_bytes()),
    ];
    let counter = Arc::new(AtomicI32::new(0));
    for _ in 0..100 {
        let vec = messages.clone();
        let counter1 = counter.clone();
        producer
            .send_batch_with_callback(vec, move |result, _err| {
                info!("send result: {:?}", result);
                counter1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            })
            .await?;
    }
    let counter1 = counter.clone();
    producer
        .send_batch_with_callback(messages, move |result, _err| {
            info!("send result: {:?}", result);
            counter1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
        .await?;

    /*    let (done_tx, mut done_rx) = tokio::sync::broadcast::channel(1);
    let done_tx_clone = done_tx.clone();
    producer
        .send_batch_with_callback(messages, move |result, err| {
            println!("send result: {:?}", result);
            println!("send error: {:?}", err);
            done_tx_clone.send(()).unwrap();
        })
        .await?;
    let mut messages = Vec::new();
    messages.push(Message::with_keys(
        TOPIC,
        TAG,
        "OrderID001",
        "Hello world 0".as_bytes(),
    ));
    messages.push(Message::with_keys(
        TOPIC,
        TAG,
        "OrderID002",
        "Hello world 1".as_bytes(),
    ));
    messages.push(Message::with_keys(
        TOPIC,
        TAG,
        "OrderID003",
        "Hello world 2".as_bytes(),
    ));
    producer
        .send_batch_with_callback(messages, move |result, err| {
            println!("send result: {:?}", result);
            println!("send error: {:?}", err);
            done_tx.send(()).unwrap();
        })
        .await?;
    let _ = done_rx.recv().await;*/
    sleep(std::time::Duration::from_secs(6));
    let i = counter.load(std::sync::atomic::Ordering::SeqCst);
    println!("send message count: {}", i);
    Ok(())
}
