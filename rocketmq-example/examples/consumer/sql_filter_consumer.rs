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

//! Push consumer example using a SQL92 selector.

#![recursion_limit = "512"]

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::message_selector::MessageSelector;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use tracing::info;

pub const CONSUMER_GROUP: &str = "consumer_sql_filter_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "SqlFilterConsumerTestTopic";
pub const SQL_EXPRESSION: &str = "region = 'cn' AND priority >= 3";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    let telemetry_guard =
        rocketmq_observability::install_global(&rocketmq_observability::TelemetryBootstrapConfig::default())
            .expect("telemetry logging bootstrap should initialize");
    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(CONSUMER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .message_model(MessageModel::Clustering)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_message_batch_max_size(1)
        .build();

    consumer
        .subscribe_with_selector(TOPIC, Some(MessageSelector::by_sql(SQL_EXPRESSION)))
        .await?;
    consumer.register_message_listener_concurrently(SqlFilterListener);
    consumer.start().await?;

    info!(
        "sql filter consumer started. group={}, topic={}, expression={}",
        CONSUMER_GROUP, TOPIC, SQL_EXPRESSION
    );
    let _ = wait_for_signal().await;
    consumer.shutdown().await;
    telemetry_guard
        .shutdown()
        .into_result()
        .expect("telemetry logging shutdown should succeed");

    Ok(())
}

struct SqlFilterListener;

impl MessageListenerConcurrently for SqlFilterListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for message in messages {
            info!(
                "sql filter receive msg_id={}, properties={:?}, body={}",
                message.msg_id(),
                message.get_properties(),
                message_body(message)
            );
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

fn message_body(message: &MessageExt) -> String {
    message
        .get_body()
        .map(|body| String::from_utf8_lossy(body.as_ref()).into_owned())
        .unwrap_or_default()
}
