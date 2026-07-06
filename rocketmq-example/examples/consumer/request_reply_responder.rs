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

//! Request/reply responder example.

#![recursion_limit = "512"]

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::utils::message_util::MessageUtil;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use tracing::error;
use tracing::info;

pub const CONSUMER_GROUP: &str = "consumer_request_reply_group";
pub const PRODUCER_GROUP: &str = "producer_request_reply_responder_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "RequestSendTestTopic";
pub const TAG: &str = "*";
pub const REPLY_TIMEOUT_MS: u64 = 3000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    let telemetry_guard =
        rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
            .expect("telemetry logging bootstrap should initialize");
    let mut reply_producer = DefaultMQProducer::builder()
        .producer_group(PRODUCER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .build();
    reply_producer.start().await?;

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(CONSUMER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .message_model(MessageModel::Clustering)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_message_batch_max_size(1)
        .build();

    consumer.subscribe(TOPIC, TAG).await?;
    consumer.register_message_listener_concurrently(RequestReplyListener {
        reply_producer: reply_producer.clone(),
    });
    consumer.start().await?;

    info!(
        "request/reply responder started. group={}, topic={}",
        CONSUMER_GROUP, TOPIC
    );
    let _ = wait_for_signal().await;

    consumer.shutdown().await;
    reply_producer.shutdown().await;
    telemetry_guard
        .shutdown()
        .expect("telemetry logging shutdown should succeed");

    Ok(())
}

#[derive(Clone)]
struct RequestReplyListener {
    reply_producer: DefaultMQProducer,
}

impl MessageListenerConcurrently for RequestReplyListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for message in messages {
            let request = message.message.clone();
            let mut producer = self.reply_producer.clone();
            let response_body = format!("reply to {}", message.msg_id());

            tokio::spawn(async move {
                match MessageUtil::create_reply_message(&request, response_body.as_bytes()) {
                    Ok(reply) => match producer.send_with_timeout(reply, REPLY_TIMEOUT_MS).await {
                        Ok(result) => info!("request reply sent: {:?}", result),
                        Err(error) => error!("request reply send failed: {error}"),
                    },
                    Err(error) => error!("create reply message failed: {error}"),
                }
            });
        }

        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
