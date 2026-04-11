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

mod consumer_connection_sub_command;
mod producer_connection_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::connection::consumer_connection_sub_command::ConsumerConnectionSubCommand;
use crate::commands::connection::producer_connection_sub_command::ProducerConnectionSubCommand;

#[derive(Subcommand)]
pub enum ConnectionCommands {
    #[command(
        name = "consumerConnection",
        about = "Query consumer's socket connection, client version and subscription"
    )]
    ConsumerConnection(ConsumerConnectionSubCommand),

    #[command(name = "producerConnection", about = "Query producer's connection")]
    ProducerConnection(ProducerConnectionSubCommand),
}

impl CommandExecute for ConnectionCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            ConnectionCommands::ConsumerConnection(cmd) => cmd.execute(rpc_hook).await,
            ConnectionCommands::ProducerConnection(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
