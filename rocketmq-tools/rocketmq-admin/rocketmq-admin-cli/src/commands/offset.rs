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

mod clone_group_offset_sub_command;
mod get_consumer_status_sub_command;
mod reset_offset_by_time_old_sub_command;
mod reset_offset_by_time_sub_command;
mod skip_accumulated_message_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::offset::clone_group_offset_sub_command::CloneGroupOffsetSubCommand;
use crate::commands::offset::get_consumer_status_sub_command::GetConsumerStatusSubCommand;
use crate::commands::offset::reset_offset_by_time_old_sub_command::ResetOffsetByTimeOldSubCommand;
use crate::commands::offset::reset_offset_by_time_sub_command::ResetOffsetByTimeSubCommand;
use crate::commands::offset::skip_accumulated_message_sub_command::SkipAccumulatedMessageSubCommand;

#[derive(Subcommand)]
pub enum OffsetCommands {
    #[command(
        name = "cloneGroupOffset",
        about = "Clone offset from other group.",
        long_about = None,
    )]
    CloneGroupOffset(CloneGroupOffsetSubCommand),

    #[command(
        name = "getConsumerStatus",
        about = "Get consumer status from client.",
        long_about = None
    )]
    GetConsumerStatus(GetConsumerStatusSubCommand),

    #[command(
        name = "resetOffsetByTime",
        about = "Reset consumer group offsets to a specific timestamp without requiring client restart.",
        long_about = r#"Reset consumer group offsets to a specific timestamp.

Resets all queues for the topic.  Active consumers are notified by the broker
and apply the new offsets immediately - no restart required.

If the consumer group is offline the command falls back to the legacy reset
method, which requires a consumer restart to take effect.

TIMESTAMP FORMATS:
  now                           - current system time (skip all backlog)
  <millis>                      - milliseconds since epoch  (e.g. 1708330800000)
  yyyy-MM-dd#HH:mm:ss:SSS       - formatted datetime        (e.g. 2024-02-19#10:00:00:000)

EXAMPLES:
  # Replay from a specific date
  resetOffsetByTime -g orderGroup -t orderTopic -s "2024-02-19#10:00:00:000"

  # Skip all accumulated backlog
  resetOffsetByTime -g orderGroup -t orderTopic -s now

  # Reset using epoch milliseconds
  resetOffsetByTime -g orderGroup -t orderTopic -s 1708330800000"#
    )]
    ResetOffsetByTime(ResetOffsetByTimeSubCommand),

    #[command(
        name = "resetOffsetByTimeOld",
        about = "Reset consumer offset by timestamp(execute this command required client restart).",
        long_about = None,
    )]
    ResetOffsetByTimeOld(ResetOffsetByTimeOldSubCommand),

    #[command(
        name = "skipAccumulatedMessage",
        about = "Skip all messages that are accumulated (not consumed) currently.",
        long_about = None
    )]
    SkipAccumulatedMessage(SkipAccumulatedMessageSubCommand),
}

impl CommandExecute for OffsetCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            OffsetCommands::CloneGroupOffset(cmd) => cmd.execute(rpc_hook).await,
            OffsetCommands::GetConsumerStatus(cmd) => cmd.execute(rpc_hook).await,
            OffsetCommands::ResetOffsetByTime(cmd) => cmd.execute(rpc_hook).await,
            OffsetCommands::ResetOffsetByTimeOld(cmd) => cmd.execute(rpc_hook).await,
            OffsetCommands::SkipAccumulatedMessage(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
