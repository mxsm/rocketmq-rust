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

mod get_controller_metadata_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::controller_commands::get_controller_metadata_sub_command::GetControllerMetadataSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ControllerCommands {
    #[command(
        name = "getControllerMetadata",
        about = "Get meta data of controller",
        long_about = None,
    )]
    GetControllerMetadataSubCommand(GetControllerMetadataSubCommand),
}

impl CommandExecute for ControllerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ControllerCommands::GetControllerMetadataSubCommand(value) => value.execute(rpc_hook).await,
        }
    }
}
