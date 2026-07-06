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

//! Demonstrates request/reply send with a callback.

use std::time::Duration;

use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;
use tokio::sync::mpsc;

pub const PRODUCER_GROUP: &str = "producer_request_callback_send_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "RequestSendTestTopic";
pub const TAG: &str = "RequestTag";
pub const REQUEST_TIMEOUT_MS: u64 = 3000;

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

    let request = Message::builder()
        .topic(TOPIC)
        .tags(TAG)
        .key("request-callback-key")
        .body("request callback body")
        .build()?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    producer
        .request_with_callback(
            request,
            move |response, error| match (response, error) {
                (Some(message), None) => {
                    let body = message
                        .get_body()
                        .map(|body| String::from_utf8_lossy(body.as_ref()).into_owned())
                        .unwrap_or_default();
                    println!("request callback response: topic={}, body={}", message.topic(), body);
                    let _ = tx.send(Ok(()));
                }
                (_, Some(error)) => {
                    println!("request callback error: {error}");
                    let _ = tx.send(Err(error.to_string()));
                }
                _ => {
                    println!("request callback send completed; waiting for reply");
                }
            },
            REQUEST_TIMEOUT_MS,
        )
        .await?;

    match tokio::time::timeout(Duration::from_millis(REQUEST_TIMEOUT_MS.saturating_mul(3)), rx.recv()).await {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(error))) => {
            return Err(RocketMQError::response_process_failed("request_callback_reply", error));
        }
        Ok(None) => {
            return Err(RocketMQError::response_process_failed(
                "request_callback_reply",
                "request callback channel closed before receiving a reply",
            ));
        }
        Err(_) => {
            return Err(RocketMQError::Timeout {
                operation: "request_callback_reply",
                timeout_ms: REQUEST_TIMEOUT_MS.saturating_mul(3),
            });
        }
    }

    producer.shutdown().await;
    telemetry_guard
        .shutdown()
        .expect("telemetry logging shutdown should succeed");

    Ok(())
}
