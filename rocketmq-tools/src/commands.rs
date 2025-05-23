/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
mod namesrv_commands;
mod topic_commands;

use std::sync::Arc;

use clap::Parser;
use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

/// A trait that defines the execution behavior for commands.
///
/// This trait is designed to be implemented by various command types
/// that require execution logic. The `execute` method provides the
/// functionality to execute a command with a given RPC hook.
pub trait CommandExecute {
    /// Executes the command.
    ///
    /// # Parameters
    /// - `rpcHook`: An `Arc` containing a reference to a type that implements the `RPCHook` trait.
    ///   This hook is used to customize the behavior of remote procedure calls during command
    ///   execution.
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()>;
}

#[derive(Debug, Parser, Clone)]
pub struct CommonArgs {
    /// The name server address list
    #[arg(
        short = 'n',
        long = "namesrvAddr",
        required = false,
        default_value = None,
        help = "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'"
    )]
    pub namesrv_addr: Option<String>,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(subcommand)]
    #[command(about = "Name server commands")]
    #[command(name = "nameserver")]
    NameServer(namesrv_commands::NameServerCommands),

    #[command(subcommand)]
    #[command(about = "Topic commands")]
    Topic(topic_commands::TopicCommands),

    #[command(about = "Category commands show")]
    Show(ClassificationTablePrint),
}

impl CommandExecute for Commands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            Commands::NameServer(value) => value.execute(rpc_hook).await,
            Commands::Topic(value) => value.execute(rpc_hook).await,
            Commands::Show(value) => value.execute(rpc_hook).await,
        }
    }
}

// ================for commands table print================
#[derive(Tabled, Clone)]
struct Command {
    #[tabled(rename = "Category")]
    category: &'static str,

    #[tabled(rename = "Command")]
    command: &'static str,

    #[tabled(rename = "Remark")]
    remark: &'static str,
}

#[derive(Parser)]
pub(crate) struct ClassificationTablePrint;

impl CommandExecute for ClassificationTablePrint {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let commands: Vec<Command> = vec![
            Command {
                category: "Topic",
                command: "allocateMQ",
                remark: "Allocate MQ.",
            },
            Command {
                category: "NameServer",
                command: "getNamesrvConfig",
                remark: "Get configs of name server.",
            },
        ];
        let mut table = Table::new(commands);
        table.with(Style::extended());
        print!("{table}");
        Ok(())
    }
}
