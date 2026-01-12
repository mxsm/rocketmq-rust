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

mod clean_broker_metadata_command;
mod get_controller_config_sub_command;
mod get_controller_metadata_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ControllerCommands {
    #[command(
        name = "cleanBrokerMetadata",
        about = "Clean metadata of broker on controller.",
        long_about = None,
    )]
    CleanBrokerMetadata(clean_broker_metadata_command::CleanBrokerMetadataCommand),

    #[command(
        name = "getControllerConfig",
        about = "Get configuration of controller(s)",
        long_about = None,
    )]
    GetControllerConfigSubCommand(get_controller_config_sub_command::GetControllerConfigSubCommand),

    #[command(
        name = "getControllerMetadata",
        about = "Get meta data of controller",
        long_about = None,
    )]
    GetControllerMetadataSubCommand(get_controller_metadata_sub_command::GetControllerMetadataSubCommand),
}

impl CommandExecute for ControllerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ControllerCommands::CleanBrokerMetadata(cmd) => cmd.execute(rpc_hook).await,
            ControllerCommands::GetControllerConfigSubCommand(value) => value.execute(rpc_hook).await,
            ControllerCommands::GetControllerMetadataSubCommand(value) => value.execute(rpc_hook).await,
        }
    }
}
