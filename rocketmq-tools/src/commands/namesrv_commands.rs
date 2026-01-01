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

mod add_write_perm_sub_command;
mod delete_kv_config_command;
mod get_namesrv_config_command;
mod update_kv_config_command;
mod update_namesrv_config;
mod wipe_write_perm_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::namesrv_commands::add_write_perm_sub_command::AddWritePermSubCommand;
use crate::commands::namesrv_commands::delete_kv_config_command::DeleteKvConfigCommand;
use crate::commands::namesrv_commands::get_namesrv_config_command::GetNamesrvConfigCommand;
use crate::commands::namesrv_commands::update_kv_config_command::UpdateKvConfigCommand;
use crate::commands::namesrv_commands::update_namesrv_config::UpdateNamesrvConfig;
use crate::commands::namesrv_commands::wipe_write_perm_sub_command::WipeWritePermSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum NameServerCommands {
    #[command(
        name = "addWritePerm",
        about = "Add write perm of broker in all name server you defined in the -n param.",
        long_about = None,
    )]
    AddWritePermSubCommand(AddWritePermSubCommand),

    #[command(
        name = "deleteKvConfig",
        about = "Delete KV config.",
        long_about = None,
    )]
    DeleteKvConfig(DeleteKvConfigCommand),

    #[command(
        name = "getNamesrvConfig",
        about = "Get configs of name server.",
        long_about = None,
    )]
    GetNamesrvConfig(GetNamesrvConfigCommand),

    #[command(
        name = "updateKvConfig",
        about = "Create or update KV config.",
        long_about = None,
    )]
    UpdateKvConfigCommand(UpdateKvConfigCommand),

    #[command(
        name = "updateNamesrvConfig",
        about = "Update configs of name server.",
        long_about = None,
    )]
    UpdateNamesrvConfig(UpdateNamesrvConfig),

    #[command(
        name = "wipeWritePerm",
        about = "Wipe write perm of broker in all name server you defined in the -n param.",
        long_about = None,
    )]
    WipeWritePermSubCommand(WipeWritePermSubCommand),
}

impl CommandExecute for NameServerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            NameServerCommands::AddWritePermSubCommand(value) => value.execute(rpc_hook).await,
            NameServerCommands::DeleteKvConfig(value) => value.execute(rpc_hook).await,
            NameServerCommands::GetNamesrvConfig(value) => value.execute(rpc_hook).await,
            NameServerCommands::UpdateKvConfigCommand(value) => value.execute(rpc_hook).await,
            NameServerCommands::UpdateNamesrvConfig(value) => value.execute(rpc_hook).await,
            NameServerCommands::WipeWritePermSubCommand(value) => value.execute(rpc_hook).await,
        }
    }
}
