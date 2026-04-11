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

mod get_broker_lite_info_sub_command;
mod get_lite_client_info_sub_command;
mod get_lite_group_info_sub_command;
mod get_lite_topic_info_sub_command;
mod get_parent_topic_info_sub_command;
mod trigger_lite_dispatch_sub_command;

use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::lite::get_broker_lite_info_sub_command::GetBrokerLiteInfoSubCommand;
use crate::commands::lite::get_lite_client_info_sub_command::GetLiteClientInfoSubCommand;
use crate::commands::lite::get_lite_group_info_sub_command::GetLiteGroupInfoSubCommand;
use crate::commands::lite::get_lite_topic_info_sub_command::GetLiteTopicInfoSubCommand;
use crate::commands::lite::get_parent_topic_info_sub_command::GetParentTopicInfoSubCommand;
use crate::commands::lite::trigger_lite_dispatch_sub_command::TriggerLiteDispatchSubCommand;

#[derive(Subcommand)]
pub enum LiteCommands {
    #[command(
        name = "getBrokerLiteInfo",
        about = "Get broker lite info.",
        long_about = None,
    )]
    GetBrokerLiteInfo(GetBrokerLiteInfoSubCommand),

    #[command(
        name = "getLiteClientInfo",
        about = "Get lite client info.",
        long_about = None,
    )]
    GetLiteClientInfo(GetLiteClientInfoSubCommand),

    #[command(
        name = "getLiteGroupInfo",
        about = "Get lite group info.",
        long_about = None,
    )]
    GetLiteGroupInfo(GetLiteGroupInfoSubCommand),

    #[command(
        name = "getLiteTopicInfo",
        about = "Get lite topic info.",
        long_about = None,
    )]
    GetLiteTopicInfo(GetLiteTopicInfoSubCommand),

    #[command(
        name = "getParentTopicInfo",
        about = "Get parent topic info.",
        long_about = None,
    )]
    GetParentTopicInfo(GetParentTopicInfoSubCommand),

    #[command(
        name = "triggerLiteDispatch",
        about = "Trigger Lite Dispatch.",
        long_about = None,
    )]
    TriggerLiteDispatch(TriggerLiteDispatchSubCommand),
}

impl CommandExecute for LiteCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            LiteCommands::GetBrokerLiteInfo(cmd) => cmd.execute(rpc_hook).await,
            LiteCommands::GetLiteClientInfo(cmd) => cmd.execute(rpc_hook).await,
            LiteCommands::GetLiteGroupInfo(cmd) => cmd.execute(rpc_hook).await,
            LiteCommands::GetLiteTopicInfo(cmd) => cmd.execute(rpc_hook).await,
            LiteCommands::GetParentTopicInfo(cmd) => cmd.execute(rpc_hook).await,
            LiteCommands::TriggerLiteDispatch(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
