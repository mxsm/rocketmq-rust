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

//! Ordered push consumer example.

#![recursion_limit = "512"]

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use rocketmq_client_rust::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use tracing::info;

pub const CONSUMER_GROUP: &str = "consumer_orderly_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "OrderSendTestTopic";
pub const TAG: &str = "*";

#[allow(deprecated)]
#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group(CONSUMER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .message_model(MessageModel::Clustering)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_message_batch_max_size(1)
        .build();

    consumer.subscribe(TOPIC, TAG).await?;
    consumer.register_message_listener_orderly(OrderlyListener);
    consumer.start().await?;

    info!("orderly consumer started. group={}, topic={}", CONSUMER_GROUP, TOPIC);
    let _ = wait_for_signal().await;
    consumer.shutdown().await;
    Ok(())
}

struct OrderlyListener;

impl MessageListenerOrderly for OrderlyListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> RocketMQResult<ConsumeOrderlyStatus> {
        context.set_auto_commit(true);
        for message in messages {
            info!(
                "orderly receive queue={} offset={} msg_id={} keys={:?} body={}",
                context.get_message_queue(),
                message.queue_offset(),
                message.msg_id(),
                message.get_keys(),
                message_body(message)
            );
        }
        Ok(ConsumeOrderlyStatus::Success)
    }
}

fn message_body(message: &MessageExt) -> String {
    message
        .get_body()
        .map(|body| String::from_utf8_lossy(body.as_ref()).into_owned())
        .unwrap_or_default()
}
