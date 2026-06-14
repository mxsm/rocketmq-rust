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

//! Lite pull consumer example with manual offset commit.

use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use rocketmq_client_rust::consumer::message_selector::MessageSelector;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_rust::rocketmq;
use rocketmq_rust::wait_for_signal;
use tracing::info;

pub const CONSUMER_GROUP: &str = "consumer_lite_pull_group";
pub const DEFAULT_NAMESRVADDR: &str = "127.0.0.1:9876";
pub const TOPIC: &str = "LitePullConsumerTestTopic";
pub const TAG_EXPRESSION: &str = "LitePullTag";
pub const POLL_TIMEOUT_MS: u64 = 1000;

#[rocketmq::main]
pub async fn main() -> RocketMQResult<()> {
    rocketmq_common::log::init_logger()?;

    let consumer = DefaultLitePullConsumer::builder()
        .consumer_group(CONSUMER_GROUP)
        .name_server_addr(DEFAULT_NAMESRVADDR)
        .message_model(MessageModel::Clustering)
        .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
        .pull_batch_size(16)
        .poll_timeout_millis(POLL_TIMEOUT_MS)
        .auto_commit(false)
        .build()?;

    consumer
        .subscribe_with_selector(TOPIC, Some(MessageSelector::by_tag(TAG_EXPRESSION)))
        .await?;
    consumer.start().await?;

    info!(
        "lite pull consumer started. group={}, topic={}, expression={}",
        CONSUMER_GROUP, TOPIC, TAG_EXPRESSION
    );

    let shutdown = wait_for_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            messages = consumer.poll_with_timeout(POLL_TIMEOUT_MS) => {
                if messages.is_empty() {
                    continue;
                }
                for message in &messages {
                    log_message(message);
                }
                consumer.commit_all().await?;
            }
        }
    }

    consumer.shutdown().await;
    Ok(())
}

fn log_message(message: &MessageExt) {
    info!(
        "lite pull receive msg_id={}, topic={}, tags={}, body={}",
        message.msg_id(),
        message.topic(),
        message.tags().unwrap_or_default(),
        message_body(message)
    );
}

fn message_body(message: &MessageExt) -> String {
    message
        .get_body()
        .map(|body| String::from_utf8_lossy(body.as_ref()).into_owned())
        .unwrap_or_default()
}
