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

mod consumer_status_sub_command;
mod consumer_sub_command;
mod delete_subscription_group_sub_command;
mod set_consume_mode_sub_command;
mod start_monitoring_sub_command;
mod update_sub_group_list_sub_command;
mod update_sub_group_sub_command;
use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::consumer_commands::consumer_status_sub_command::ConsumerStatusSubCommand;
use crate::commands::consumer_commands::consumer_sub_command::ConsumerSubCommand;
use crate::commands::consumer_commands::delete_subscription_group_sub_command::DeleteSubscriptionGroupSubCommand;
use crate::commands::consumer_commands::set_consume_mode_sub_command::SetConsumeModeSubCommand;
use crate::commands::consumer_commands::start_monitoring_sub_command::StartMonitoringSubCommand;
use crate::commands::consumer_commands::update_sub_group_list_sub_command::UpdateSubGroupListSubCommand;
use crate::commands::consumer_commands::update_sub_group_sub_command::UpdateSubGroupSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ConsumerCommands {
    #[command(
        name = "consumerStatus",
        about = "Query and display consumer's internal data structures, including subscription information, queue allocation, consumption mode, and runtime information.",
        long_about = None,
    )]
    ConsumerStatus(ConsumerStatusSubCommand),

    #[command(
        name = "consumer",
        about = "Query consumer's connection, status, etc.",
        long_about = None,
    )]
    Consumer(ConsumerSubCommand),

    #[command(
        name = "deleteSubGroup",
        about = "Delete subscription group",
        long_about = r#"Delete subscription group from broker."#
    )]
    DeleteSubscriptionGroup(DeleteSubscriptionGroupSubCommand),

    #[command(
        name = "setConsumeMode",
        about = "Set consume message mode. pull/pop etc.",
        long_about = None,
    )]
    SetConsumeMode(SetConsumeModeSubCommand),

    #[command(
        name = "startMonitoring",
        about = "Start Monitoring.",
        long_about = None,
    )]
    StartMonitoring(StartMonitoringSubCommand),

    #[command(
        name = "updateSubGroupList",
        about = "Create or update subscription groups in batch.",
        long_about = None,
    )]
    UpdateSubGroupList(UpdateSubGroupListSubCommand),

    #[command(
        name = "updateSubGroup",
        about = "Update or create subscription group.",
        long_about = None,
    )]
    UpdateSubGroup(UpdateSubGroupSubCommand),
}

impl CommandExecute for ConsumerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ConsumerCommands::ConsumerStatus(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::Consumer(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::DeleteSubscriptionGroup(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::SetConsumeMode(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::StartMonitoring(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::UpdateSubGroupList(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::UpdateSubGroup(value) => value.execute(rpc_hook).await,
        }
    }
}
