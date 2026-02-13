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
mod copy_users_sub_command;
mod create_acl_sub_command;
mod create_user_sub_command;
mod delete_acl_sub_command;
mod delete_user_sub_command;
mod get_acl_sub_command;
mod get_user_sub_command;
mod list_acl_sub_command;
mod list_users_sub_command;
mod update_acl_sub_command;
mod update_user_sub_command;

use std::sync::Arc;

use crate::commands::CommandExecute;
use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

#[derive(Subcommand)]
pub enum AuthCommands {
    #[command(
        name = "copyAcl",
        about = "Copy acl to cluster",
        long_about = None,
    )]
    CopyAcl(copy_acl_sub_command::CopyAclSubCommand),

    #[command(
        name = "copyUser",
        about = "Copy user to cluster",
        long_about = None,
    )]
    CopyUsers(copy_users_sub_command::CopyUsersSubCommand),

    #[command(
        name = "createAcl",
        about = "Create acl to cluster",
        long_about = None,
    )]
    CreateAcl(create_acl_sub_command::CreateAclSubCommand),

    #[command(
        name = "createUser",
        about = "Create user to cluster.",
        long_about = None,
    )]
    CreateUser(create_user_sub_command::CreateUserSubCommand),

    #[command(
        name = "deleteAcl",
        about = "Delete acl from cluster.",
        long_about = None,
    )]
    DeleteAcl(delete_acl_sub_command::DeleteAclSubCommand),

    #[command(
        name = "deleteUser",
        about = "Delete user from cluster.",
        long_about = None,
    )]
    DeleteUser(delete_user_sub_command::DeleteUserSubCommand),

    #[command(
        name = "getUser",
        about = "Get user from cluster.",
        long_about = None,
    )]
    GetUser(get_user_sub_command::GetUserSubCommand),

    #[command(
        name = "getAcl",
        about = "Get acl from cluster.",
        long_about = None,
    )]
    GetAcl(get_acl_sub_command::GetAclSubCommand),

    #[command(
        name = "listUsers",
        about = "List users from cluster.",
        long_about = None,
    )]
    ListUsers(list_users_sub_command::ListUsersSubCommand),

    #[command(
        name = "listAcl",
        about = "List acl from cluster",
        long_about = None,
    )]
    ListAcl(list_acl_sub_command::ListAclSubCommand),

    #[command(
        name = "updateAcl",
        about = "Update Access Control List (ACL)",
        long_about = None,
    )]
    UpdateAcl(update_acl_sub_command::UpdateAclSubCommand),

    #[command(
        name = "updateUser",
        about = "Update user to cluster.",
        long_about = None,
    )]
    UpdateUser(update_user_sub_command::UpdateUserSubCommand),
}

impl CommandExecute for AuthCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            AuthCommands::CopyAcl(value) => value.execute(rpc_hook).await,
            AuthCommands::CopyUsers(value) => value.execute(rpc_hook).await,
            AuthCommands::CreateAcl(value) => value.execute(rpc_hook).await,
            AuthCommands::CreateUser(value) => value.execute(rpc_hook).await,
            AuthCommands::DeleteAcl(value) => value.execute(rpc_hook).await,
            AuthCommands::DeleteUser(value) => value.execute(rpc_hook).await,
            AuthCommands::GetUser(value) => value.execute(rpc_hook).await,
            AuthCommands::GetAcl(value) => value.execute(rpc_hook).await,
            AuthCommands::ListAcl(value) => value.execute(rpc_hook).await,
            AuthCommands::ListUsers(value) => value.execute(rpc_hook).await,
            AuthCommands::UpdateAcl(value) => value.execute(rpc_hook).await,
            AuthCommands::UpdateUser(value) => value.execute(rpc_hook).await,
        }
    }
}
