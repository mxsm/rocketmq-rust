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

mod export_configs_sub_command;
mod export_metadata_in_rocks_db_sub_command;
mod export_metadata_sub_command;
mod export_pop_record_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::export::export_configs_sub_command::ExportConfigsSubCommand;
use crate::commands::export::export_metadata_in_rocks_db_sub_command::ExportMetadataInRocksDBSubCommand;
use crate::commands::export::export_metadata_sub_command::ExportMetadataSubCommand;
use crate::commands::export::export_pop_record_sub_command::ExportPopRecordSubCommand;

#[derive(Subcommand)]
pub enum ExportCommands {
    #[command(
        name = "exportConfigs",
        about = "Export configs.",
        long_about = None,
    )]
    ExportConfigs(ExportConfigsSubCommand),

    #[command(
        name = "exportMetadataInRocksDB",
        about = "Export RocksDB kv config (topics/subscriptionGroups). Recommend to use [mqadmin rocksDBConfigToJson]",
        long_about = None,
    )]
    ExportMetadataInRocksDB(ExportMetadataInRocksDBSubCommand),

    #[command(
        name = "exportMetadata",
        about = "Export metadata.",
        long_about = None,
    )]
    ExportMetadata(ExportMetadataSubCommand),

    #[command(
        name = "exportPopRecord",
        about = "Export pop consumer records.",
        long_about = None,
    )]
    ExportPopRecord(ExportPopRecordSubCommand),
}

impl CommandExecute for ExportCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ExportCommands::ExportConfigs(cmd) => cmd.execute(rpc_hook).await,
            ExportCommands::ExportMetadataInRocksDB(cmd) => cmd.execute(rpc_hook).await,
            ExportCommands::ExportMetadata(cmd) => cmd.execute(rpc_hook).await,
            ExportCommands::ExportPopRecord(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
