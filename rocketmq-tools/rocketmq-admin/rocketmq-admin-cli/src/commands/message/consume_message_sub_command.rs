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

use chrono::NaiveDateTime;
use clap::Parser;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::UtilAll::parse_date;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::message::ConsumeMessagesRequest;
use rocketmq_admin_core::core::message::MessagePullEvent;
use rocketmq_admin_core::core::message::MessageService;

const DEFAULT_MESSAGE_COUNT: i64 = 128;

#[derive(Debug, Clone, Parser)]
pub struct ConsumeMessageSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'b', long = "brokerName", required = false, help = "Broker name")]
    broker_name: Option<String>,

    #[arg(short = 'i', long = "queueId", required = false, help = "Queue Id")]
    queue_id: Option<i32>,

    #[arg(short = 'o', long = "offset", required = false, help = "Queue offset")]
    offset: Option<i64>,

    #[arg(short = 'g', long = "consumerGroup", required = false, help = "Consumer group name")]
    consumer_group: Option<String>,

    #[arg(
        short = 's',
        long = "beginTimestamp",
        required = false,
        help = "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    begin_timestamp: Option<String>,

    #[arg(
        short = 'e',
        long = "endTimestamp",
        required = false,
        help = "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    end_timestamp: Option<String>,

    #[arg(
        short = 'c',
        long = "MessageNumber",
        required = false,
        default_value_t = DEFAULT_MESSAGE_COUNT,
        help = "Number of messages to be consumed"
    )]
    message_number: i64,
}

impl ConsumeMessageSubCommand {
    fn timestamp_format(value: &str) -> RocketMQResult<i64> {
        if let Ok(ts) = value.trim().parse::<i64>() {
            return Ok(ts);
        }

        let date_str = value.trim().replace('#', " ");
        if let Some(dt) = parse_date(&date_str, YYYY_MM_DD_HH_MM_SS_SSS) {
            return Ok(dt.and_utc().timestamp_millis());
        }

        if let Ok(dt) = NaiveDateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S") {
            return Ok(dt.and_utc().timestamp_millis());
        }

        Err(RocketMQError::IllegalArgument(format!(
            "Invalid timestamp format: {}",
            value
        )))
    }

    fn print_messages(msgs: &[rocketmq_rust::ArcMut<MessageExt>]) {
        for msg in msgs {
            let body_str = match msg.body() {
                Some(body) => String::from_utf8(body.to_vec()).unwrap_or_else(|_| "BINARY".to_string()),
                None => "EMPTY".to_string(),
            };
            let msg_ref: &MessageExt = msg;
            println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg_ref, body_str);
        }
    }
}

impl CommandExecute for ConsumeMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let begin_timestamp = self
            .begin_timestamp
            .as_deref()
            .map(Self::timestamp_format)
            .transpose()?;
        let end_timestamp = self.end_timestamp.as_deref().map(Self::timestamp_format).transpose()?;
        let request = ConsumeMessagesRequest::try_new(
            self.topic.clone(),
            self.broker_name.clone(),
            self.queue_id,
            self.offset,
            self.consumer_group.clone(),
            begin_timestamp,
            end_timestamp,
            self.message_number,
        )?;

        MessageService::consume_messages_by_request_with_rpc_hook(request, rpc_hook, |event| {
            match event {
                MessagePullEvent::ConsumeOk => println!("Consume ok"),
                MessagePullEvent::Messages { messages } => Self::print_messages(&messages),
                MessagePullEvent::NoMatched { mq, status, offset } => {
                    println!("{} no matched msg. status={:?}, offset={}", mq, status, offset)
                }
                MessagePullEvent::Finished { mq, status, offset } => {
                    println!("{} print msg finished. status={:?}, offset={}", mq, status, offset)
                }
                MessagePullEvent::PullError { error } => eprintln!("{}", error),
                MessagePullEvent::OffsetNotMatched { mq, offset } => {
                    println!("{} no matched msg, offset={}", mq, offset)
                }
                MessagePullEvent::CountLimit {
                    message_number,
                    queue_id: Some(queue_id),
                } => {
                    println!(
                        "The older {} message of the {} queue will be provided",
                        message_number, queue_id
                    )
                }
                MessagePullEvent::CountLimit {
                    message_number,
                    queue_id: None,
                } => println!("The older {} message will be provided", message_number),
                _ => {}
            }
            Ok(())
        })
        .await
    }
}
