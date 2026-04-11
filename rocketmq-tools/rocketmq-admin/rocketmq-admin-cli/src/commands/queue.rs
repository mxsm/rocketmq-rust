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

mod check_rocksdb_cq_write_progress_sub_command;
mod query_consume_queue_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::queue::check_rocksdb_cq_write_progress_sub_command::CheckRocksdbCqWriteProgressSubCommand;
use crate::commands::queue::query_consume_queue_sub_command::QueryCqSubCommand;

#[derive(Subcommand)]
pub enum QueueCommands {
    #[command(
        name = "checkRocksdbCqWriteProgress",
        about = "check if rocksdb cq is same as file cq.",
        long_about = None,
    )]
    CheckRocksdbCqWriteProgress(CheckRocksdbCqWriteProgressSubCommand),

    #[command(
        name = "queryCq",
        about = "Query cq command.",
        long_about = None,
    )]
    QueryCq(QueryCqSubCommand),
}

impl CommandExecute for QueueCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            QueueCommands::CheckRocksdbCqWriteProgress(cmd) => cmd.execute(rpc_hook).await,
            QueueCommands::QueryCq(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
