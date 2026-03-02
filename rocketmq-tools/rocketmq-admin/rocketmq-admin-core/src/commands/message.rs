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

pub mod check_msg_send_rt_sub_command;
pub mod decode_message_id_sub_command;
pub mod dump_compaction_log_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::message::check_msg_send_rt_sub_command::CheckMsgSendRTSubCommand;
use crate::commands::message::decode_message_id_sub_command::DecodeMessageIdSubCommand;
use crate::commands::message::dump_compaction_log_sub_command::DumpCompactionLogSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum MessageCommands {
    #[command(
        name = "checkMsgSendRT",
        about = "Check message send response time.",
        long_about = None,
    )]
    CheckMsgSendRT(CheckMsgSendRTSubCommand),

    #[command(
        name = "decodeMessageId",
        about = "Decode unique message ID.",
        long_about = None,
    )]
    DecodeMessageId(DecodeMessageIdSubCommand),

    #[command(
        name = "dumpCompactionLog",
        about = "Parse compaction log to message.",
        long_about = None,
    )]
    DumpCompactionLog(DumpCompactionLogSubCommand),
}

impl CommandExecute for MessageCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            MessageCommands::CheckMsgSendRT(value) => value.execute(rpc_hook).await,
            MessageCommands::DecodeMessageId(value) => value.execute(rpc_hook).await,
            MessageCommands::DumpCompactionLog(value) => value.execute(rpc_hook).await,
        }
    }
}
