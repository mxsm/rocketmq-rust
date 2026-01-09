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
pub const PRODUCER_GROUP: &str = "please_rename_unique_group_name";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";

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

    let tags = ["TagA", "TagB", "TagC", "TagD", "TagE"];

    for i in 0..100 {
        let message = Message::with_keys(
            TOPIC,
            tags[i % tags.len()],
            format!("KEY{}", i),
            format!("Hello RocketMQ {}", i).as_bytes(),
        );
        let order_id = i % 10;
        let send_result = producer
            .send_with_selector(
                message,
                |mqs, _msg, arg| {
                    let id = arg.downcast_ref::<usize>().unwrap();
                    let index = id % mqs.len();
                    Some(mqs[index].clone())
                },
                order_id,
            )
            .await?;
        println!(
            "send result: {}",
            send_result.expect("send_with_selector should return a result for synchronous sends")
        );
    }
    producer.shutdown().await;

    Ok(())
}
