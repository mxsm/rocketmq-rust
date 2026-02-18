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

mod broker_consume_stats_sub_command;
mod broker_status_sub_command;
mod clean_expired_cq_sub_command;
mod clean_unused_topic_sub_command;
mod delete_expired_commit_log_sub_command;
mod get_broker_config_sub_command;
mod get_broker_epoch_sub_command;
mod get_cold_data_flow_ctr_info_sub_command;
mod remove_cold_data_flow_ctr_group_config_sub_command;
mod reset_master_flush_offset_sub_command;
mod send_msg_status_sub_command;
mod switch_timer_engine_sub_command;
mod update_broker_config_sub_command;
mod update_cold_data_flow_ctr_group_config_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::broker_commands::broker_consume_stats_sub_command::BrokerConsumeStatsSubCommand;
use crate::commands::broker_commands::broker_status_sub_command::BrokerStatusSubCommand;
use crate::commands::broker_commands::clean_expired_cq_sub_command::CleanExpiredCQSubCommand;
use crate::commands::broker_commands::clean_unused_topic_sub_command::CleanUnusedTopicSubCommand;
use crate::commands::broker_commands::delete_expired_commit_log_sub_command::DeleteExpiredCommitLogSubCommand;
use crate::commands::broker_commands::get_broker_config_sub_command::GetBrokerConfigSubCommand;
use crate::commands::broker_commands::get_broker_epoch_sub_command::GetBrokerEpochSubCommand;
use crate::commands::broker_commands::get_cold_data_flow_ctr_info_sub_command::GetColdDataFlowCtrInfoSubCommand;
use crate::commands::broker_commands::remove_cold_data_flow_ctr_group_config_sub_command::RemoveColdDataFlowCtrGroupConfigSubCommand;
use crate::commands::broker_commands::reset_master_flush_offset_sub_command::ResetMasterFlushOffsetSubCommand;
use crate::commands::broker_commands::send_msg_status_sub_command::SendMsgStatusSubCommand;
use crate::commands::broker_commands::switch_timer_engine_sub_command::SwitchTimerEngineSubCommand;
use crate::commands::broker_commands::update_broker_config_sub_command::UpdateBrokerConfigSubCommand;
use crate::commands::broker_commands::update_cold_data_flow_ctr_group_config_sub_command::UpdateColdDataFlowCtrGroupConfigSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum BrokerCommands {
    #[command(
        name = "brokerConsumeStats",
        about = "Fetch broker consume stats data.",
        long_about = None,
    )]
    BrokerConsumeStats(BrokerConsumeStatsSubCommand),

    #[command(
        name = "brokerStatus",
        about = "Fetch broker runtime status data.",
        long_about = None,
    )]
    BrokerStatus(BrokerStatusSubCommand),

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
        name = "getBrokerEpoch",
        about = "Fetch broker epoch entries.",
        long_about = None,
    )]
    GetBrokerEpoch(GetBrokerEpochSubCommand),

    #[command(
        name = "getColdDataFlowCtrInfo",
        about = "Get cold data flow ctr info.",
        long_about = None,
    )]
    GetColdDataFlowCtrInfo(GetColdDataFlowCtrInfoSubCommand),

    #[command(
        name = "removeColdDataFlowCtrGroupConfig",
        about = "Remove consumer from cold ctr config.",
        long_about = None,
    )]
    RemoveColdDataFlowCtrGroupConfig(RemoveColdDataFlowCtrGroupConfigSubCommand),

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

    #[command(
        name = "updateBrokerConfig",
        about = "Update broker config by special broker or all brokers in cluster.",
        long_about = None,
    )]
    UpdateBrokerConfigSubCommand(UpdateBrokerConfigSubCommand),
}

impl CommandExecute for BrokerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            BrokerCommands::BrokerConsumeStats(value) => value.execute(rpc_hook).await,
            BrokerCommands::BrokerStatus(cmd) => cmd.execute(rpc_hook).await,
            BrokerCommands::CleanExpiredCQ(value) => value.execute(rpc_hook).await,
            BrokerCommands::CleanUnusedTopic(value) => value.execute(rpc_hook).await,
            BrokerCommands::DeleteExpiredCommitLog(value) => value.execute(rpc_hook).await,
            BrokerCommands::GetBrokerConfig(cmd) => cmd.execute(rpc_hook).await,
            BrokerCommands::GetBrokerEpoch(cmd) => cmd.execute(rpc_hook).await,
            BrokerCommands::GetColdDataFlowCtrInfo(value) => value.execute(rpc_hook).await,
            BrokerCommands::RemoveColdDataFlowCtrGroupConfig(value) => value.execute(rpc_hook).await,
            BrokerCommands::ResetMasterFlushOffset(value) => value.execute(rpc_hook).await,
            BrokerCommands::SendMsgStatus(value) => value.execute(rpc_hook).await,
            BrokerCommands::SwitchTimerEngine(value) => value.execute(rpc_hook).await,
            BrokerCommands::UpdateColdDataFlowCtrGroupConfig(value) => value.execute(rpc_hook).await,
            BrokerCommands::UpdateBrokerConfigSubCommand(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
