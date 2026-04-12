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

use clap::Parser;
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::UtilAll::parse_date;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::message::MessagePullEvent;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_admin_core::core::message::PrintMessagesByQueueRequest;

#[derive(Debug, Clone, Parser)]
pub struct PrintMsgByQueueSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(short = 'a', long = "brokerName", required = true, help = "Broker name")]
    broker_name: String,

    #[arg(short = 'i', long = "queueId", required = true, help = "Queue id")]
    queue_id: i32,

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
        short = 'p',
        long = "print",
        required = false,
        default_value = "false",
        help = "Print msg. eg: true | false(default)"
    )]
    print_msg: bool,

    #[arg(
        short = 'd',
        long = "printBody",
        required = false,
        default_value = "false",
        help = "Print body. eg: true | false(default)"
    )]
    print_body: bool,

    #[arg(
        short = 'f',
        long = "calculate",
        required = false,
        default_value = "false",
        help = "Calculate by tag. eg: true | false(default)"
    )]
    calculate_by_tag: bool,
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

impl CommandExecute for PrintMsgByQueueSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let begin_timestamp = self.begin_timestamp.as_deref().map(str::trim).map(timestamp_format);
        let end_timestamp = self.end_timestamp.as_deref().map(str::trim).map(timestamp_format);
        let request = PrintMessagesByQueueRequest::try_new(
            self.topic.clone(),
            self.broker_name.clone(),
            self.queue_id,
            self.sub_expression.clone(),
            begin_timestamp,
            end_timestamp,
            self.print_msg,
            self.calculate_by_tag,
        )?;
        let charset_name = self.charset_name.trim().to_string();
        let print_body = self.print_body;

        MessageService::print_messages_by_queue_by_request_with_rpc_hook(request, rpc_hook, |event| {
            match event {
                MessagePullEvent::Messages { messages } => print_messages(&messages, &charset_name, print_body),
                MessagePullEvent::PullError { error } => eprintln!("Pull message error: {}", error),
                MessagePullEvent::TagCounts(tag_counts) => print_calculate_by_tag(tag_counts),
                _ => {}
            }
            Ok(())
        })
        .await
    }
}

fn print_calculate_by_tag(tag_counts: Vec<(String, i64)>) {
    for (tag, count) in tag_counts {
        println!("Tag: {:<30} Count: {}", tag, count);
    }
}

fn print_messages(msgs: &[rocketmq_rust::ArcMut<MessageExt>], _charset_name: &str, print_body: bool) {
    for msg in msgs {
        let body_str = if print_body {
            if let Some(body) = msg.body() {
                String::from_utf8(body.to_vec()).unwrap_or_else(|_| "BINARY".to_string())
            } else {
                "EMPTY".to_string()
            }
        } else {
            "NOT PRINT BODY".to_string()
        };
        let msg_ref: &MessageExt = msg;
        println!("MSGID: {} {} BODY: {}", msg.msg_id(), msg_ref, body_str);
    }
}
