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

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use rocketmq_tools::admin::default_mq_admin_ext::DefaultMQAdminExt;
use tracing::info;
pub const MESSAGE_COUNT: usize = 1;
pub const CONSUMER_GROUP: &str = "please_rename_unique_group_name_4";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "TopicTest";
pub const TAG: &str = "*";

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    //init logger
    rocketmq_common::log::init_logger()?;

    switch_pop_consumer().await?;

    // create a producer builder with default configuration
    let builder = DefaultMQPushConsumer::builder();

    let mut consumer = builder
        .consumer_group(CONSUMER_GROUP.to_string())
        .name_server_addr(DEFAULT_NAMESRVADDR.to_string())
        // disable client side load balance, also is pop consumer
        .client_rebalance(false)
        .build();
    consumer.subscribe(TOPIC, "*")?;
    consumer.register_message_listener_concurrently(MyMessageListener);
    consumer.start().await?;
    let _ = wait_for_signal().await;
    Ok(())
}

async fn switch_pop_consumer() -> RocketMQResult<()> {
    let mut mq_admin_ext = DefaultMQAdminExt::new();
    mq_admin_ext.client_config_mut().namesrv_addr = Some(CheetahString::from_static_str(DEFAULT_NAMESRVADDR));
    MQAdminExt::start(&mut mq_admin_ext).await.unwrap();
    let broker_datas = MQAdminExt::examine_topic_route_info(&mq_admin_ext, CheetahString::from_static_str(TOPIC))
        .await
        .unwrap()
        .unwrap();
    for broker_data in broker_datas.broker_datas {
        let broker_addrs = broker_data
            .broker_addrs()
            .values()
            .cloned()
            .collect::<HashSet<CheetahString>>();
        for broker_addr in broker_addrs {
            MQAdminExt::set_message_request_mode(
                &mq_admin_ext,
                broker_addr,
                CheetahString::from_static_str(TOPIC),
                CheetahString::from_static_str(CONSUMER_GROUP),
                MessageRequestMode::Pop,
                8,
                3_000,
            )
            .await
            .unwrap();
        }
    }
    mq_admin_ext.shutdown().await;
    Ok(())
}

pub struct MyMessageListener;

impl MessageListenerConcurrently for MyMessageListener {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for msg in msgs {
            info!("Receive message: {:?}", msg);
        }
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}
