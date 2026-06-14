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

//! Demonstrates synchronous request/reply send.

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_request_send_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "RequestSendTestTopic";
pub const TAG: &str = "RequestTag";
pub const REQUEST_TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    let request = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .key("request-key")
        .body("request body")
        .build()?;

    let response = producer.request(request, REQUEST_TIMEOUT_MS).await?;
    let body = response
        .get_body()
        .map(|body| String::from_utf8_lossy(body.as_ref()).into_owned())
        .unwrap_or_default();
    println!("request response: topic={}, body={}", response.topic(), body);

    producer.shutdown().await;
    Ok(())
}
