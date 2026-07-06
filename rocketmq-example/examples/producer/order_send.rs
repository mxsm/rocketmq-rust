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

//! Demonstrates ordered sends by routing the same order id to the same queue.

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;

pub const PRODUCER_GROUP: &str = "producer_order_send_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "OrderSendTestTopic";
pub const TAG: &str = "OrderTag";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    let telemetry_guard =
        rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
            .expect("telemetry logging bootstrap should initialize");
    let mut producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();

    producer.start().await?;

    for order_id in 0..4 {
        for step in ["created", "paid", "packed", "shipped"] {
            let message = Message::builder()
                .topic(TOPIC)
                .tags(TAG)
                .key(format!("order-{order_id}"))
                .body(format!("order_id={order_id}, step={step}"))
                .build()?;

            let result = producer
                .send_with_selector(
                    message,
                    |queues, _message, selected_order_id: &usize| {
                        let index = selected_order_id % queues.len();
                        Some(queues[index].clone())
                    },
                    order_id,
                )
                .await?;

            println!("order_id={order_id}, step={step}, send result: {:?}", result);
        }
    }

    producer.shutdown().await;
    telemetry_guard
        .shutdown()
        .expect("telemetry logging shutdown should succeed");

    Ok(())
}
