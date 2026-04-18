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
use rocketmq_admin_core::core::message::DecodeMessageIdOutcome;
use rocketmq_admin_core::core::message::DecodeMessageIdRequest;
use rocketmq_admin_core::core::message::DecodeMessageIdResult;
use rocketmq_admin_core::core::message::MessageService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct DecodeMessageIdSubCommand {
    #[arg(
        short = 'i',
        long = "messageId",
        required = true,
        num_args = 1..,
        help = "Unique message ID"
    )]
    message_id: Vec<String>,
}

impl DecodeMessageIdSubCommand {
    fn request(&self) -> RocketMQResult<DecodeMessageIdRequest> {
        DecodeMessageIdRequest::try_new(self.message_id.clone())
    }

    fn print_result(result: &DecodeMessageIdResult) {
        for entry in &result.entries {
            match &entry.outcome {
                DecodeMessageIdOutcome::Decoded {
                    broker_ip,
                    broker_port,
                    commit_log_offset,
                    offset_hex,
                } => {
                    println!("MessageId: {}", entry.message_id);
                    println!();
                    println!("Decoded Information:");
                    println!("  Broker IP: {}", broker_ip);
                    println!("  Broker Port: {}", broker_port);
                    println!("  Commit Log Offset: {}", commit_log_offset);
                    println!("  Offset Hex: {}", offset_hex);
                    println!();
                }
                DecodeMessageIdOutcome::Invalid { error } => {
                    eprintln!("Invalid message ID: {}. {}", entry.message_id, error);
                }
            }
        }
    }
}

impl CommandExecute for DecodeMessageIdSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = MessageService::decode_message_ids(&self.request()?);
        Self::print_result(&result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_message_id_sub_command_builds_core_request() {
        let command = DecodeMessageIdSubCommand::try_parse_from([
            "decodeMessageId",
            "-i",
            " 7F0000010007D8260BF075769D36C348 ",
            " ",
        ])
        .unwrap();

        let request = command.request().unwrap();

        assert_eq!(request.message_ids(), ["7F0000010007D8260BF075769D36C348"]);
    }
}
