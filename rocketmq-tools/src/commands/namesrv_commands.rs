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
mod get_namesrv_config_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::namesrv_commands::get_namesrv_config_command::GetNamesrvConfigCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum NameServerCommands {
    #[command(
        name = "getNamesrvConfig",
        about = "Get configs of name server.",
        long_about = None,
    )]
    GetNamesrvConfig(GetNamesrvConfigCommand),
}

impl CommandExecute for NameServerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            NameServerCommands::GetNamesrvConfig(value) => value.execute(rpc_hook).await,
        }
    }
}
