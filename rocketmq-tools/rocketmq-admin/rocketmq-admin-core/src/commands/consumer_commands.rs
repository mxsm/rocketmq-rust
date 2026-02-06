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
mod update_sub_group_list_sub_command;
mod update_sub_group_sub_command;
use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ConsumerCommands {
    #[command(
        name = "consumerStatus",
        about = "Query and display consumer's internal data structures, including subscription information, queue allocation, consumption mode, and runtime information.",
        long_about = None,
    )]
    ConsumerStatusSubCommand(consumer_status_sub_command::ConsumerStatusSubCommand),
    #[command(
        name = "consumer",
        about = "Query consumer's connection, status, etc.",
        long_about = None,
    )]
    ConsumerSubCommand(consumer_sub_command::ConsumerSubCommand),
    #[command(
        name = "deleteSubGroup",
        about = "Delete subscription group",
        long_about = r#"Delete subscription group from broker."#
    )]
    DeleteSubGroup(delete_subscription_group_sub_command::DeleteSubscriptionGroupSubCommand),
    #[command(
        name = "updateSubGroup",
        about = "Update or create subscription group.",
        long_about = None,
    )]
    UpdateSubGroupSubCommand(update_sub_group_sub_command::UpdateSubGroupSubCommand),
    #[command(
        name = "updateSubGroupList",
        about = "Create or update subscription groups in batch.",
        long_about = None,
    )]
    UpdateSubGroupList(update_sub_group_list_sub_command::UpdateSubGroupListSubCommand),
}

impl CommandExecute for ConsumerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ConsumerCommands::ConsumerStatusSubCommand(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::ConsumerSubCommand(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::DeleteSubGroup(cmd) => cmd.execute(rpc_hook).await,
            ConsumerCommands::UpdateSubGroupSubCommand(value) => value.execute(rpc_hook).await,
            ConsumerCommands::UpdateSubGroupList(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
