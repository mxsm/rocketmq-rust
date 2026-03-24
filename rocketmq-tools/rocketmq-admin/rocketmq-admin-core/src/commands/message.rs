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
pub mod consume_message_sub_command;
pub mod decode_message_id_sub_command;
pub mod dump_compaction_log_sub_command;
pub mod print_message_sub_command;
pub mod print_msg_by_queue_sub_command;
pub mod query_msg_by_id_sub_command;
pub mod query_msg_by_key_sub_command;
pub mod query_msg_by_offset_sub_command;
pub mod query_msg_by_unique_key_sub_command;
pub mod query_msg_trace_by_id_sub_command;
pub mod send_message_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::message::check_msg_send_rt_sub_command::CheckMsgSendRTSubCommand;
use crate::commands::message::consume_message_sub_command::ConsumeMessageSubCommand;
use crate::commands::message::decode_message_id_sub_command::DecodeMessageIdSubCommand;
use crate::commands::message::dump_compaction_log_sub_command::DumpCompactionLogSubCommand;
use crate::commands::message::print_message_sub_command::PrintMessageSubCommand;
use crate::commands::message::print_msg_by_queue_sub_command::PrintMsgByQueueSubCommand;
use crate::commands::message::query_msg_by_id_sub_command::QueryMsgByIdSubCommand;
use crate::commands::message::query_msg_by_key_sub_command::QueryMsgByKeySubCommand;
use crate::commands::message::query_msg_by_offset_sub_command::QueryMsgByOffsetSubCommand;
use crate::commands::message::query_msg_by_unique_key_sub_command::QueryMsgByUniqueKeySubCommand;
use crate::commands::message::query_msg_trace_by_id_sub_command::QueryMsgTraceByIdSubCommand;
use crate::commands::message::send_message_sub_command::SendMessageSubCommand;
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
        name = "consumeMessage",
        about = "Consume message.",
        long_about = None,
    )]
    ConsumeMessage(ConsumeMessageSubCommand),

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

    #[command(
        name = "printMessage",
        about = "Print Message Detail.",
        long_about = None,
    )]
    PrintMessage(PrintMessageSubCommand),

    #[command(
        name = "printMsgByQueue",
        about = "Print Message Detail by queueId.",
        long_about = None,
    )]
    PrintMsgByQueue(PrintMsgByQueueSubCommand),

    #[command(
        name = "queryMsgById",
        about = "Query message by message ID.",
        long_about = None,
    )]
    QueryMsgById(QueryMsgByIdSubCommand),
    #[command(
        name = "queryMsgByKey",
        about = "Query Message by Key.",
        long_about = None,
    )]
    QueryMsgByKey(QueryMsgByKeySubCommand),

    #[command(
        name = "queryMsgByOffset",
        about = "Query Message by offset.",
        long_about = None,
    )]
    QueryMsgByOffset(QueryMsgByOffsetSubCommand),

    #[command(
        name = "queryMsgByUniqueKey",
        about = "Query Message by Unique key.",
        long_about = None,
    )]
    QueryMsgByUniqueKey(QueryMsgByUniqueKeySubCommand),

    #[command(
        name = "queryMsgTraceById",
        about = "Query message trace by message ID.",
        long_about = None,
    )]
    QueryMsgTraceById(QueryMsgTraceByIdSubCommand),

    #[command(
        name = "sendMessage",
        about = "Send a message.",
        long_about = None,
    )]
    SendMessage(SendMessageSubCommand),
}

impl CommandExecute for MessageCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            MessageCommands::CheckMsgSendRT(value) => value.execute(rpc_hook).await,
            MessageCommands::ConsumeMessage(value) => value.execute(rpc_hook).await,
            MessageCommands::DecodeMessageId(value) => value.execute(rpc_hook).await,
            MessageCommands::DumpCompactionLog(value) => value.execute(rpc_hook).await,
            MessageCommands::PrintMessage(value) => value.execute(rpc_hook).await,
            MessageCommands::PrintMsgByQueue(value) => value.execute(rpc_hook).await,
            MessageCommands::QueryMsgById(value) => value.execute(rpc_hook).await,
            MessageCommands::QueryMsgByKey(value) => value.execute(rpc_hook).await,
            MessageCommands::QueryMsgByOffset(value) => value.execute(rpc_hook).await,
            MessageCommands::QueryMsgByUniqueKey(value) => value.execute(rpc_hook).await,
            MessageCommands::QueryMsgTraceById(value) => value.execute(rpc_hook).await,
            MessageCommands::SendMessage(value) => value.execute(rpc_hook).await,
        }
    }
}
