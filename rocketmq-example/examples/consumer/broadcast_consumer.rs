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

//! Broadcast push consumer example.

#![recursion_limit = "512"]

use std::path::PathBuf;

use cheetah_string::CheetahString;
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;
use rocketmq_client_rust::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use rocketmq_client_rust::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::consumer::listener::message_listener_concurrently::MessageListenerConcurrently;
use rocketmq_client_rust::consumer::mq_push_consumer::MQPushConsumer;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use tracing::info;

pub const CONSUMER_GROUP: &str = "consumer_broadcast_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "BroadcastConsumerTestTopic";
pub const TAG: &str = "*";

#[allow(deprecated)]
#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let client_config = broadcast_client_config()?;

    let mut consumer = DefaultMQPushConsumer::builder()
        .client_config(client_config)
        .consumer_group(CONSUMER_GROUP)
        .message_model(MessageModel::Broadcasting)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .consume_message_batch_max_size(1)
        .build();

    consumer.subscribe(TOPIC, TAG).await?;
    consumer.register_message_listener_concurrently(BroadcastListener);
    consumer.start().await?;

    info!("broadcast consumer started. group={}, topic={}", CONSUMER_GROUP, TOPIC);
    let _ = wait_for_signal().await;
    consumer.shutdown().await;
    Ok(())
}

fn broadcast_client_config() -> RocketMQResult<ClientConfig> {
    let mut client_config = ClientConfig::new();
    client_config.set_namesrv_addr(CheetahString::from_static_str(DEFAULT_NAMESRVADDR));
    client_config.client_ip = Some(CheetahString::from_static_str("127.0.0.1"));
    client_config.set_instance_name(CheetahString::from_static_str("broadcast_example"));

    let offset_dir = local_offset_store_dir()
        .join(client_config.build_mq_client_id())
        .join(CONSUMER_GROUP);
    std::fs::create_dir_all(&offset_dir)?;
    let offset_file = offset_dir.join("offsets.json");
    if !offset_file.exists() {
        std::fs::write(offset_file, r#"{"offsetTable":{}}"#)?;
    }

    Ok(client_config)
}

fn local_offset_store_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("rocketmq.client.localOffsetStoreDir") {
        return PathBuf::from(dir);
    }

    #[cfg(target_os = "windows")]
    {
        std::env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("C:\\tmp"))
            .join(".rocketmq_offsets")
    }

    #[cfg(not(target_os = "windows"))]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(".rocketmq_offsets")
    }
}

struct BroadcastListener;

impl MessageListenerConcurrently for BroadcastListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        for message in messages {
            info!(
                "broadcast receive msg_id={}, topic={}, tags={}, body={}",
                message.msg_id(),
                message.topic(),
                message.tags().unwrap_or_default(),
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
