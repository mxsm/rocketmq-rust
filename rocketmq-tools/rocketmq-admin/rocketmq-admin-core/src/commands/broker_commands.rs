// Copyright 2026 The RocketMQ Rust Authors
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

mod clean_expired_cq_sub_command;
mod clean_unused_topic_sub_command;
mod delete_expired_commit_log_sub_command;
mod get_broker_config_sub_command;
mod reset_master_flush_offset_sub_command;
mod send_msg_status_sub_command;
mod switch_timer_engine_sub_command;
mod update_cold_data_flow_ctr_group_config_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::broker_commands::clean_expired_cq_sub_command::CleanExpiredCQSubCommand;
use crate::commands::broker_commands::clean_unused_topic_sub_command::CleanUnusedTopicSubCommand;
use crate::commands::broker_commands::delete_expired_commit_log_sub_command::DeleteExpiredCommitLogSubCommand;
use crate::commands::broker_commands::get_broker_config_sub_command::GetBrokerConfigSubCommand;
use crate::commands::broker_commands::reset_master_flush_offset_sub_command::ResetMasterFlushOffsetSubCommand;
use crate::commands::broker_commands::send_msg_status_sub_command::SendMsgStatusSubCommand;
use crate::commands::broker_commands::switch_timer_engine_sub_command::SwitchTimerEngineSubCommand;
use crate::commands::broker_commands::update_cold_data_flow_ctr_group_config_sub_command::UpdateColdDataFlowCtrGroupConfigSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum BrokerCommands {
    #[command(
        name = "cleanExpiredCQ",
        about = "Clean expired ConsumeQueue on broker.",
        long_about = None,
    )]
    CleanExpiredCQ(CleanExpiredCQSubCommand),

    #[command(
        name = "cleanUnusedTopic",
        about = "Clean unused topic on broker.",
        long_about = None,
    )]
    CleanUnusedTopic(CleanUnusedTopicSubCommand),

    #[command(
        name = "deleteExpiredCommitLog",
        about = "Delete expired CommitLog files.",
        long_about = None,
    )]
    DeleteExpiredCommitLog(DeleteExpiredCommitLogSubCommand),

    #[command(
        name = "getBrokerConfig",
        about = "Get broker config by cluster or special broker.",
        long_about = None,
    )]
    GetBrokerConfig(GetBrokerConfigSubCommand),

    #[command(
        name = "resetMasterFlushOffset",
        about = "Reset master flush offset in slave.",
        long_about = None,
    )]
    ResetMasterFlushOffset(ResetMasterFlushOffsetSubCommand),

    #[command(
        name = "sendMsgStatus",
        about = "Send msg to broker.",
        long_about = None,
    )]
    SendMsgStatus(SendMsgStatusSubCommand),

    #[command(
        name = "switchTimerEngine",
        about = "Switch the engine of timer message in broker.",
        long_about = None,
    )]
    SwitchTimerEngine(SwitchTimerEngineSubCommand),

    #[command(
        name = "updateColdDataFlowCtrGroupConfig",
        about = "Add or update cold data flow ctr group config.",
        long_about = None,
    )]
    UpdateColdDataFlowCtrGroupConfig(UpdateColdDataFlowCtrGroupConfigSubCommand),
}

impl CommandExecute for BrokerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            BrokerCommands::CleanExpiredCQ(value) => value.execute(rpc_hook).await,
            BrokerCommands::CleanUnusedTopic(value) => value.execute(rpc_hook).await,
            BrokerCommands::DeleteExpiredCommitLog(value) => value.execute(rpc_hook).await,
            BrokerCommands::GetBrokerConfig(cmd) => cmd.execute(rpc_hook).await,
            BrokerCommands::ResetMasterFlushOffset(value) => value.execute(rpc_hook).await,
            BrokerCommands::SendMsgStatus(value) => value.execute(rpc_hook).await,
            BrokerCommands::SwitchTimerEngine(value) => value.execute(rpc_hook).await,
            BrokerCommands::UpdateColdDataFlowCtrGroupConfig(value) => value.execute(rpc_hook).await,
        }
    }
}
