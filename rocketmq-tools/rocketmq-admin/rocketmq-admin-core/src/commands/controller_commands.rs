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

mod clean_broker_metadata_sub_command;
mod get_controller_config_sub_command;
mod get_controller_metadata_sub_command;
mod update_controller_config_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::controller_commands::clean_broker_metadata_sub_command::CleanBrokerMetadataSubCommand;
use crate::commands::controller_commands::get_controller_config_sub_command::GetControllerConfigSubCommand;
use crate::commands::controller_commands::get_controller_metadata_sub_command::GetControllerMetadataSubCommand;
use crate::commands::controller_commands::update_controller_config_sub_command::UpdateControllerConfigSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ControllerCommands {
    #[command(
        name = "cleanBrokerMetadata",
        about = "Clean metadata of broker on controller.",
        long_about = None,
    )]
    CleanBrokerMetadata(CleanBrokerMetadataSubCommand),

    #[command(
        name = "getControllerConfig",
        about = "Get configuration of controller(s)",
        long_about = None,
    )]
    GetControllerConfig(GetControllerConfigSubCommand),

    #[command(
        name = "getControllerMetadata",
        about = "Get meta data of controller",
        long_about = None,
    )]
    GetControllerMetadata(GetControllerMetadataSubCommand),

    #[command(
        name = "updateControllerConfig",
        about = "update controller config",
        long_about = None,
    )]
    UpdateControllerConfig(UpdateControllerConfigSubCommand),
}

impl CommandExecute for ControllerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ControllerCommands::CleanBrokerMetadata(cmd) => cmd.execute(rpc_hook).await,
            ControllerCommands::GetControllerConfig(value) => value.execute(rpc_hook).await,
            ControllerCommands::GetControllerMetadata(value) => value.execute(rpc_hook).await,
            ControllerCommands::UpdateControllerConfig(value) => value.execute(rpc_hook).await,
        }
    }
}
