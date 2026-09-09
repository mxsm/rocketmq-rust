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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_client_rust::{ClientRuntime, ClientRuntimeConfig, DefaultLitePullConsumer, DefaultMQProducer};
use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::result::SendStatus;
use rocketmq_observability::TelemetryRuntimeGuard;
use rocketmq_runtime::RuntimeOwner;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const TOPIC: &str = "DocsFirstMessage";
const GROUP: &str = "docs_first_message_consumer";
const NAMESERVER: &str = "127.0.0.1:9876";

fn main() -> Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode != "produce" && mode != "consume" {
        return Err(std::io::Error::other("usage: rocketmq-doc-first-message <produce|consume>").into());
    }

    let owner = RuntimeOwner::new()?;
    let telemetry = TelemetryRuntimeGuard::noop();
    let client = ClientRuntime::try_new(
        owner.root_context().component("first-message-client"),
        ClientRuntimeConfig::default(),
        telemetry.handle(),
    )?;
    let (operation, client_report) = owner.block_on(async {
        let result = if mode == "produce" {
            produce(Arc::clone(&client)).await
        } else {
            consume(Arc::clone(&client)).await
        };
        (result, client.shutdown().await)
    });
    let runtime_result = owner.shutdown_runtime_blocking();
    let telemetry_result = telemetry.shutdown().into_result();
    operation?;
    let runtime_report = runtime_result?;
    telemetry_result?;
    if !client_report.is_healthy() || !runtime_report.is_healthy() {
        return Err(std::io::Error::other("client or runtime shutdown was incomplete").into());
    }
    Ok(())
}

async fn produce(client: Arc<ClientRuntime>) -> Result<()> {
    let mut producer = DefaultMQProducer::builder(client)
        .producer_group("docs_first_message_producer")
        .name_server_addr(NAMESERVER)
        .build();
    let result = async {
        producer.start().await?;
        for index in 0..5 {
            let body = format!("documentation message {index}");
            let message = Message::builder().topic(TOPIC).body(body).build()?;
            let result = producer.send_with_timeout(message, 3_000).await?;
            let result = result.ok_or_else(|| std::io::Error::other("send returned no result"))?;
            println!(
                "SEND {index}: status={} id={:?} queue_offset={}",
                result.send_status, result.msg_id, result.queue_offset
            );
            if result.send_status != SendStatus::SendOk {
                return Err(std::io::Error::other("send did not return SEND_OK").into());
            }
        }
        Ok(())
    }
    .await;
    producer.shutdown().await;
    result
}

async fn consume(client: Arc<ClientRuntime>) -> Result<()> {
    let consumer = DefaultLitePullConsumer::builder(client)
        .consumer_group(GROUP)
        .name_server_addr(NAMESERVER)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .auto_commit(false)
        .poll_timeout_millis(1_000)
        .build()?;
    let result = async {
        consumer.subscribe(TOPIC).await?;
        consumer.start().await?;
        println!("CONSUMER_STARTED topic={TOPIC} group={GROUP}; send from a second terminal");
        let deadline = tokio::time::sleep(Duration::from_secs(60));
        tokio::pin!(deadline);
        let mut count = 0;
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    return Err(std::io::Error::other("no complete five-message batch within 60 seconds").into());
                }
                signal = tokio::signal::ctrl_c() => {
                    signal?;
                    return Ok(());
                }
                messages = consumer.poll_with_timeout(1_000) => {
                    for message in &messages {
                        println!("RECEIVED id={}", message.msg_id());
                    }
                    if !messages.is_empty() {
                        consumer.commit_all().await?;
                        count += messages.len();
                        println!("OFFSET_COMMIT_REQUESTED received={count}");
                        if count >= 5 {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    .await;
    consumer.shutdown().await;
    result
}
