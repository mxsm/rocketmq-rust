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

mod clean_unused_topic_command;
mod get_broker_config_sub_command;
mod switch_timer_engine_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::broker_commands::clean_unused_topic_command::CleanUnusedTopicCommand;
use crate::commands::broker_commands::switch_timer_engine_sub_command::SwitchTimerEngineSubCommand;
use crate::commands::CommandExecute;

#[derive(Subcommand)]
pub enum BrokerCommands {
    #[command(
        name = "cleanUnusedTopic",
        about = "Clean unused topic on broker.",
        long_about = None,
    )]
    CleanUnusedTopic(CleanUnusedTopicCommand),

    #[command(
        name = "switchTimerEngine",
        about = "Switch the engine of timer message in broker.",
        long_about = None,
    )]
    SwitchTimerEngine(SwitchTimerEngineSubCommand),

    #[command(
        name = "getBrokerConfig",
        about = "Get broker config by cluster or special broker.",
        long_about = None,
    )]
    GetBrokerConfigSubCommand(get_broker_config_sub_command::GetBrokerConfigSubCommand),
}

impl CommandExecute for BrokerCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            BrokerCommands::CleanUnusedTopic(value) => value.execute(rpc_hook).await,
            BrokerCommands::SwitchTimerEngine(value) => value.execute(rpc_hook).await,
            BrokerCommands::GetBrokerConfigSubCommand(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
