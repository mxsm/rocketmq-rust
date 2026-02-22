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

mod producer_connection_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::connection_commands::producer_connection_sub_command::ProducerConnectionSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum ConnectionCommands {
    #[command(name = "producerConnection", about = "Query producer's connection")]
    ProducerConnection(ProducerConnectionSubCommand),
}

impl CommandExecute for ConnectionCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ConnectionCommands::ProducerConnection(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
