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

mod copy_acl_sub_command;
mod update_acl_sub_command;
mod update_user_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::auth_commands::update_acl_sub_command::UpdateAclSubCommand;
use crate::commands::auth_commands::update_user_sub_command::UpdateUserSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum AuthCommands {
    #[command(
        name = "copyAcl",
        about = "Copy acl to cluster",
        long_about = None,
    )]
    CopyAclSubCommand(copy_acl_sub_command::CopyAclSubCommand),

    #[command(
        name = "updateAcl",
        about = "Update Access Control List (ACL)",
        long_about = None,
    )]
    UpdateAclSubCommand(UpdateAclSubCommand),

    #[command(
        name = "updateUser",
        about = "Update user to cluster.",
        long_about = None,
    )]
    UpdateUserSubCommand(UpdateUserSubCommand),
}

impl CommandExecute for AuthCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            AuthCommands::CopyAclSubCommand(value) => value.execute(rpc_hook).await,
            AuthCommands::UpdateAclSubCommand(value) => value.execute(rpc_hook).await,
            AuthCommands::UpdateUserSubCommand(value) => value.execute(rpc_hook).await,
        }
    }
}
