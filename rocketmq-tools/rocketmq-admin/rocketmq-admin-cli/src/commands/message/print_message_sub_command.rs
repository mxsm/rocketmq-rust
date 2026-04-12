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

use clap::ArgAction;
use clap::Parser;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::UtilAll::parse_date;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::message::MessagePullEvent;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_admin_core::core::message::PrintMessagesRequest;

#[derive(Debug, Clone, Parser)]
pub struct PrintMessageSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'c',
        long = "charsetName",
        required = false,
        default_value = "UTF-8",
        help = "CharsetName(eg: UTF-8, GBK)"
    )]
    charset_name: String,

    #[arg(
        short = 's',
        long = "subExpression",
        required = false,
        default_value = "*",
        help = "Subscribe Expression(eg: TagA || TagB)"
    )]
    sub_expression: String,

    #[arg(
        short = 'b',
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
        short = 'd',
        long = "printBody",
        required = false,
        default_value_t = true,
        action = ArgAction::Set,
        help = "Print body. eg: true | false(default true)"
    )]
    print_body: bool,

    #[arg(
        short = 'l',
        long = "lmqParentTopic",
        required = false,
        help = "Lmq parent topic, lmq is used to find the route"
    )]
    lmq_parent_topic: Option<String>,
}

fn timestamp_format(value: &str) -> u64 {
    if let Ok(ts) = value.parse::<u64>() {
        return ts;
    }

    let date_str = value.replace('#', " ");
    if let Some(dt) = parse_date(&date_str, YYYY_MM_DD_HH_MM_SS_SSS) {
        return dt.and_utc().timestamp_millis() as u64;
    }

    0
}

fn print_message(msgs: &[rocketmq_rust::ArcMut<MessageExt>], _charset_name: &str, print_body: bool) {
    for msg in msgs {
        let body = if print_body {
            match msg.body() {
                Some(bytes) => String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| "BINARY".to_string()),
                None => "EMPTY".to_string(),
            }
        } else {
            "NOT PRINT BODY".to_string()
        };

        let msg_ref: &MessageExt = msg;
        println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg_ref, body);
    }
}

impl CommandExecute for PrintMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let begin_timestamp = self.begin_timestamp.as_deref().map(str::trim).map(timestamp_format);
        let end_timestamp = self.end_timestamp.as_deref().map(str::trim).map(timestamp_format);
        let request = PrintMessagesRequest::try_new(
            self.topic.clone(),
            self.sub_expression.clone(),
            begin_timestamp,
            end_timestamp,
            self.lmq_parent_topic.clone(),
        )?;
        let charset_name = self.charset_name.trim().to_string();
        let print_body = self.print_body;

        MessageService::print_messages_by_request_with_rpc_hook(request, rpc_hook, |event| {
            match event {
                MessagePullEvent::QueueRange {
                    mq,
                    min_offset,
                    max_offset,
                } => println!("minOffset={}, maxOffset={}, {}", min_offset, max_offset, mq),
                MessagePullEvent::Messages { messages } => print_message(&messages, &charset_name, print_body),
                MessagePullEvent::NoMatched { mq, status, offset } => {
                    println!("{} no matched msg. status={:?}, offset={}", mq, status, offset)
                }
                MessagePullEvent::Finished { mq, status, offset } => {
                    println!("{} print msg finished. status={:?}, offset={}", mq, status, offset)
                }
                MessagePullEvent::PullError { error } => eprintln!("Pull message error: {}", error),
                MessagePullEvent::Separator => println!("--------------------------------------------------------"),
                _ => {}
            }
            Ok(())
        })
        .await
    }
}
