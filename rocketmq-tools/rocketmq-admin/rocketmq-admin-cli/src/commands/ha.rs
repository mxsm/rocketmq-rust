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

mod get_sync_state_set_sub_command;
mod ha_status_sub_command;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;

use crate::commands::CommandExecute;
use crate::commands::ha::get_sync_state_set_sub_command::GetSyncStateSetSubCommand;
use crate::commands::ha::ha_status_sub_command::HAStatusSubCommand;

#[derive(Subcommand)]
pub enum HACommands {
    #[command(
        name = "getSyncStateSet",
        about = "Fetch syncStateSet for target brokers.",
        long_about = None,
    )]
    GetSyncStateSet(GetSyncStateSetSubCommand),

    #[command(
        name = "haStatus",
        about = "Fetch ha runtime status data.",
        long_about = None,
    )]
    HaStatus(HAStatusSubCommand),
}

impl CommandExecute for HACommands {
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
    ) -> RocketMQResult<()> {
        match self {
            HACommands::GetSyncStateSet(value) => value.execute(credentials).await,
            HACommands::HaStatus(value) => value.execute(credentials).await,
        }
    }
}
