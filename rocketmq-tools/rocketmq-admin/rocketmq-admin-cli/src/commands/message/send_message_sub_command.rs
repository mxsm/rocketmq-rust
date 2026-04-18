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
use rocketmq_admin_core::core::producer::ProducerService;
use rocketmq_admin_core::core::producer::SendMessageRequest;
use rocketmq_admin_core::core::producer::SendMessageResult;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct SendMessageSubCommand {
    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(
        short = 'p',
        long = "body",
        required = true,
        help = "UTF-8 string format of the message body"
    )]
    body: String,

    #[arg(short = 'k', long = "key", required = false, help = "Message keys")]
    keys: Option<String>,

    #[arg(short = 'c', long = "tags", required = false, help = "Message tags")]
    tags: Option<String>,

    #[arg(
        short = 'b',
        long = "broker",
        required = false,
        help = "Send message to target broker"
    )]
    broker_name: Option<String>,

    #[arg(short = 'i', long = "qid", required = false, help = "Send message to target queue")]
    queue_id: Option<i32>,

    #[arg(
        short = 'm',
        long = "msgTraceEnable",
        required = false,
        default_value = "false",
        help = "Message Trace Enable, Default: false"
    )]
    msg_trace_enable: bool,
}

impl SendMessageSubCommand {
    fn request(&self) -> RocketMQResult<SendMessageRequest> {
        SendMessageRequest::try_new(
            self.topic.clone(),
            self.body.clone(),
            self.keys.clone(),
            self.tags.clone(),
            self.broker_name.clone(),
            self.queue_id,
            self.msg_trace_enable,
        )
    }

    fn print_result(result: &SendMessageResult) {
        println!(
            "{:<32}  {:<4}  {:<20}    #MsgId",
            "#Broker Name", "#QID", "#Send Result"
        );
        println!(
            "{:<32}  {:<4}  {:<20}    {}",
            result.row.broker_name, result.row.queue_id, result.row.send_status, result.row.msg_id
        );
    }
}

impl CommandExecute for SendMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.queue_id.is_some() && self.broker_name.is_none() {
            println!("Broker name must be set if the queue is chosen!");
            return Ok(());
        }

        let result = ProducerService::send_message_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_message_sub_command_builds_core_request() {
        let command = SendMessageSubCommand::try_parse_from([
            "sendMessage",
            "-t",
            " TopicA ",
            "-p",
            " body ",
            "-k",
            " key ",
            "-c",
            " tag ",
            "-b",
            " broker-a ",
            "-i",
            "1",
            "-m",
        ])
        .unwrap();

        let request = command.request().unwrap();

        assert_eq!(request.topic().as_str(), "TopicA");
        assert_eq!(request.body(), "body");
        assert_eq!(request.keys(), Some("key"));
        assert_eq!(request.tags(), Some("tag"));
        assert_eq!(request.broker_name().unwrap().as_str(), "broker-a");
        assert_eq!(request.queue_id(), Some(1));
        assert!(request.msg_trace_enable());
    }
}
