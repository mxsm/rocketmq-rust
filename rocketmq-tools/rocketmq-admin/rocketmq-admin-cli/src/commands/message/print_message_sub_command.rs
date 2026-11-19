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

use clap::ArgAction;
use clap::Parser;
use rocketmq_error::Result as CanonicalResult;
use rocketmq_model::common::message::message_ext::MessageExt;

use crate::commands::CommandExecute;
use rocketmq_admin_core::client_adapter::services::message::MessagePullEvent;
use rocketmq_admin_core::client_adapter::services::message::MessageService;
use rocketmq_admin_core::client_adapter::services::message::PrintMessagesRequest;

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
        value_parser = super::parse_print_timestamp,
        required = false,
        help = "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    begin_timestamp: Option<u64>,

    #[arg(
        short = 'e',
        long = "endTimestamp",
        value_parser = super::parse_print_timestamp,
        required = false,
        help = "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    end_timestamp: Option<u64>,

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

fn print_message(msgs: &[MessageExt], _charset_name: &str, print_body: bool) {
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
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> CanonicalResult<()> {
        let request = PrintMessagesRequest::try_new(
            self.topic.clone(),
            self.sub_expression.clone(),
            self.begin_timestamp,
            self.end_timestamp,
            self.lmq_parent_topic.clone(),
        )?;
        let charset_name = self.charset_name.trim().to_string();
        let print_body = self.print_body;

        MessageService::print_messages_by_request_with_credentials(request, credentials, client_runtime, |event| {
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

#[cfg(test)]
mod tests {
    use chrono::{Local, TimeZone};

    use super::*;

    #[test]
    fn parses_print_time_bounds_and_rejects_invalid_dates() {
        let parse = |end| PrintMessageSubCommand::try_parse_from(["printMsg", "-t", "test", "-b", "0", "-e", end]);
        let command = parse("2025-01-02#03:04:05:123").unwrap();
        let expected = Local
            .with_ymd_and_hms(2025, 1, 2, 3, 4, 5)
            .single()
            .unwrap()
            .timestamp_millis()
            + 123;
        assert_eq!(command.begin_timestamp, Some(0));
        assert_eq!(command.end_timestamp, Some(expected as u64));
        assert!(parse("invalid").is_err());
    }
}
