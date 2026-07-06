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

#![recursion_limit = "256"]

use std::env;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;

const DEFAULT_NAMESRV_ADDR: &str = "127.0.0.1:9876";
const DEFAULT_TOPIC: &str = "RustExampleRequestCodeSmokeTopic";
const DEFAULT_PRODUCER_GROUP: &str = "rust_example_request_code_smoke_producer";
const DEFAULT_CONSUMER_GROUP_PREFIX: &str = "rust_example_request_code_smoke_consumer";
const DEFAULT_MESSAGE_COUNT: usize = 1;
const DEFAULT_SEND_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_POLL_TIMEOUT_MS: u64 = 3_000;
const DEFAULT_POLL_ATTEMPTS: usize = 10;

struct SmokeConfig {
    namesrv_addr: String,
    topic: String,
    producer_group: String,
    consumer_group: String,
    message_count: usize,
    send_timeout_ms: u64,
    poll_timeout_ms: u64,
    poll_attempts: usize,
    run_id: String,
}

impl SmokeConfig {
    fn from_env() -> RocketMQResult<Self> {
        let run_id = env::var("ROCKETMQ_SMOKE_RUN_ID").unwrap_or_else(|_| current_millis().to_string());
        let consumer_group =
            env::var("ROCKETMQ_CONSUMER_GROUP").unwrap_or_else(|_| format!("{DEFAULT_CONSUMER_GROUP_PREFIX}_{run_id}"));

        Ok(Self {
            namesrv_addr: env_or("ROCKETMQ_NAMESRV_ADDR", DEFAULT_NAMESRV_ADDR),
            topic: env_or("ROCKETMQ_TOPIC", DEFAULT_TOPIC),
            producer_group: env_or("ROCKETMQ_PRODUCER_GROUP", DEFAULT_PRODUCER_GROUP),
            consumer_group,
            message_count: env_usize("ROCKETMQ_MESSAGE_COUNT", DEFAULT_MESSAGE_COUNT)?,
            send_timeout_ms: env_u64("ROCKETMQ_SEND_TIMEOUT_MS", DEFAULT_SEND_TIMEOUT_MS)?,
            poll_timeout_ms: env_u64("ROCKETMQ_POLL_TIMEOUT_MS", DEFAULT_POLL_TIMEOUT_MS)?,
            poll_attempts: env_usize("ROCKETMQ_POLL_ATTEMPTS", DEFAULT_POLL_ATTEMPTS)?,
            run_id,
        })
    }
}

#[allow(deprecated)]
#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let config = SmokeConfig::from_env()?;
    run_smoke(config).await
}

async fn run_smoke(config: SmokeConfig) -> RocketMQResult<()> {
    let mut producer = DefaultMQProducer::builder()
        .producer_group(config.producer_group.as_str())
        .name_server_addr(config.namesrv_addr.as_str())
        .build();

    producer.start().await?;

    let mut expected_bodies = Vec::with_capacity(config.message_count);
    for index in 0..config.message_count {
        let body = format!("request-code-smoke:{}:{index}", config.run_id);
        let message = Message::builder()
            .topic(config.topic.as_str())
            .tags("Smoke")
            .keys(vec![config.run_id.clone()])
            .body_slice(body.as_bytes())
            .build()?;

        let result = producer.send_with_timeout(message, config.send_timeout_ms).await?;
        if result.is_none() {
            producer.shutdown().await;
            return Err(RocketMQError::response_process_failed(
                "request_code_smoke_send",
                "producer returned no send result for request-code smoke",
            ));
        }
        expected_bodies.push(body);
    }

    let received_bodies = Arc::new(Mutex::new(Vec::with_capacity(config.message_count)));
    let listener = SmokeListener {
        run_id: config.run_id.clone(),
        received_bodies: received_bodies.clone(),
    };

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(config.consumer_group.as_str())
        .name_server_addr(config.namesrv_addr.as_str())
        .message_model(MessageModel::Clustering)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_thread_min(1)
        .consume_thread_max(1)
        .consume_message_batch_max_size(config.message_count as u32)
        .pull_batch_size(config.message_count as u32)
        .build();

    consumer.subscribe(config.topic.as_str(), "*").await?;
    consumer.register_message_listener_concurrently(listener);
    consumer.start().await?;

    for _ in 0..config.poll_attempts {
        let received_count = received_bodies
            .lock()
            .map_err(|err| {
                RocketMQError::response_process_failed(
                    "request_code_smoke_received_body_lock",
                    format!("received body lock poisoned: {err}"),
                )
            })?
            .len();

        if received_count >= expected_bodies.len() {
            break;
        }

        tokio::time::sleep(Duration::from_millis(config.poll_timeout_ms)).await;
    }

    let received_bodies = received_bodies
        .lock()
        .map_err(|err| {
            RocketMQError::response_process_failed(
                "request_code_smoke_received_body_lock",
                format!("received body lock poisoned: {err}"),
            )
        })?
        .clone();

    producer.shutdown().await;

    let missing = expected_bodies
        .iter()
        .filter(|expected| !received_bodies.iter().any(|received| received == *expected))
        .cloned()
        .collect::<Vec<_>>();

    if !missing.is_empty() {
        return Err(RocketMQError::response_process_failed(
            "request_code_smoke_receive",
            format!(
                "request-code smoke did not receive expected messages: missing={missing:?}, \
                 received={received_bodies:?}"
            ),
        ));
    }

    println!(
        "INTEROP_OK rustExampleSmoke namesrv={} topic={} sent={} received={}",
        config.namesrv_addr,
        config.topic,
        expected_bodies.len(),
        received_bodies.len()
    );

    Ok(())
}

struct SmokeListener {
    run_id: String,
    received_bodies: Arc<Mutex<Vec<String>>>,
}

impl MessageListenerConcurrently for SmokeListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        let mut received_bodies = self.received_bodies.lock().map_err(|err| {
            RocketMQError::response_process_failed(
                "request_code_smoke_received_body_lock",
                format!("received body lock poisoned: {err}"),
            )
        })?;

        for message in msgs {
            if let Some(body) = message.get_body() {
                let body = String::from_utf8_lossy(body).to_string();
                if body.contains(&self.run_id) {
                    received_bodies.push(body);
                }
            }
        }

        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_usize(name: &str, default: usize) -> RocketMQResult<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|err| RocketMQError::illegal_argument(format!("{name} must be usize: {err}"))),
        Err(_) => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> RocketMQResult<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|err| RocketMQError::illegal_argument(format!("{name} must be u64: {err}"))),
        Err(_) => Ok(default),
    }
}

fn current_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
