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

use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
#[allow(unused_imports)]
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::consume_orderly_context::ConsumeOrderlyContext;
use rocketmq_client_rust::consumer::listener::consume_orderly_status::ConsumeOrderlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_orderly::MessageListenerOrderly;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use tracing::info;

pub const MESSAGE_COUNT: usize = 1;
pub const CONSUMER_GROUP: &str = "please_rename_unique_group_name_3";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    // create a producer builder with default configuration
    let builder = DefaultMQPushConsumer::builder();

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        .message_model(MessageModel::Clustering)
        .build();
    consumer.subscribe(TOPIC, TAG)?;
    consumer.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
    consumer.register_message_listener_orderly(MyMessageListener::new());
    consumer.start().await?;
    let _ = tokio::signal::ctrl_c().await;
    Ok(())
}

pub struct MyMessageListener {
    consume_times: Arc<AtomicI64>,
}

impl Default for MyMessageListener {
    fn default() -> Self {
        Self::new()
    }
}

impl MyMessageListener {
    pub fn new() -> Self {
        Self {
            consume_times: Arc::new(AtomicI64::new(0)),
        }
    }
}

impl MessageListenerOrderly for MyMessageListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeOrderlyContext,
    ) -> RocketMQResult<ConsumeOrderlyStatus> {
        context.set_auto_commit(true);
        for msg in msgs {
            println!("Receive message: {:?}", msg);
            info!("Receive message: {:?}", msg);
        }
        if self.consume_times.load(std::sync::atomic::Ordering::Acquire) % 2 == 0 {
            return Ok(ConsumeOrderlyStatus::Success);
        } else if self.consume_times.load(std::sync::atomic::Ordering::Acquire) % 5 == 0 {
            context.set_suspend_current_queue_time_millis(3000);
            return Ok(ConsumeOrderlyStatus::SuspendCurrentQueueAMoment);
        }
        Ok(ConsumeOrderlyStatus::Success)
    }
}
