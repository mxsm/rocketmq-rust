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
pub const PRODUCER_GROUP: &str = "producer_simple";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "ProducerSimpleTest";
pub const TAG: &str = "TagA";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    for _ in 0..10 {
        let message = Message::builder()
            .topic(TOPIC)
            .tags(TAG)
            .body("Hello RocketMQ")
            .build()?;
        let send_result = producer.send(message).await?;
        println!("send result: {:?}", send_result);
    }
    producer.shutdown().await;
    Ok(())
}
