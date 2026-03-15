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
use rocketmq_common::MessageDecoder::decode_message_id;
use rocketmq_common::MessageDecoder::validate_message_id;
use rocketmq_error::RocketMQError;
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

impl CommandExecute for DecodeMessageIdSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        for msg_id in &self.message_id {
            let msg_id = msg_id.trim();

            if msg_id.is_empty() {
                continue;
            }

            if let Err(e) = validate_message_id(msg_id) {
                eprintln!("Invalid message ID: {}. {}", msg_id, e);
                continue;
            }

            match decode_message_id(msg_id) {
                Ok(message_id) => {
                    let ip = message_id.address.ip();
                    let port = message_id.address.port();
                    let offset = message_id.offset;

                    println!("MessageId: {}", msg_id);
                    println!();
                    println!("Decoded Information:");
                    println!("  Broker IP: {}", ip);
                    println!("  Broker Port: {}", port);
                    println!("  Commit Log Offset: {}", offset);
                    println!("  Offset Hex: {:#018X}", offset);
                    println!();
                }
                Err(e) => {
                    return Err(RocketMQError::Internal(format!(
                        "DecodeMessageIdSubCommand command failed: failed to decode message ID '{}': {}",
                        msg_id, e
                    )));
                }
            }
        }

        Ok(())
    }
}
