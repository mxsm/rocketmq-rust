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

mod allocate_mq_sub_command;
mod delete_topic_sub_command;
mod remapping_static_topic_sub_command;
mod topic_cluster_sub_command;
mod topic_list_sub_command;
mod topic_route_sub_command;
mod topic_status_sub_command;
mod update_order_conf_sub_command;
mod update_static_topic_sub_command;
mod update_topic_list_sub_command;
mod update_topic_perm_sub_command;
mod update_topic_sub_command;
use std::sync::Arc;

use clap::Subcommand;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::topic_commands::allocate_mq_sub_command::AllocateMQSubCommand;
use crate::commands::topic_commands::delete_topic_sub_command::DeleteTopicSubCommand;
use crate::commands::topic_commands::remapping_static_topic_sub_command::RemappingStaticTopicSubCommand;
use crate::commands::topic_commands::topic_cluster_sub_command::TopicClusterSubCommand;
use crate::commands::topic_commands::topic_list_sub_command::TopicListSubCommand;
use crate::commands::topic_commands::topic_route_sub_command::TopicRouteSubCommand;
use crate::commands::topic_commands::topic_status_sub_command::TopicStatusSubCommand;
use crate::commands::topic_commands::update_order_conf_sub_command::UpdateOrderConfSubCommand;
use crate::commands::topic_commands::update_static_topic_sub_command::UpdateStaticTopicSubCommand;
use crate::commands::topic_commands::update_topic_list_sub_command::UpdateTopicListSubCommand;
use crate::commands::topic_commands::update_topic_perm_sub_command::UpdateTopicPermSubCommand;
use crate::commands::topic_commands::update_topic_sub_command::UpdateTopicSubCommand;
use crate::commands::CommandExecute;
const NAMESPACE_ORDER_TOPIC_CONFIG: &str = "ORDER_TOPIC_CONFIG";
#[derive(Subcommand)]
pub enum TopicCommands {
    #[command(
        name = "allocateMQ",
        about = "Allocate memory space for each topic",
        long_about = r#"Allocate memory space for each topic, which is used to allocate the memory
space of the topic when the topic is created. The default value is 1. If you want to allocate
more memory space, you can use this command to allocate it."#
    )]
    AllocateMQ(AllocateMQSubCommand),

    #[command(
        name = "deleteTopic",
        about = "Delete topic",
        long_about = r#"Delete topic from broker and NameServer."#
    )]
    DeleteTopic(DeleteTopicSubCommand),

    #[command(
        name = "remappingStaticTopic",
        about = "Remapping static topic",
        long_about = r#"Remapping static topic to different brokers or clusters."#
    )]
    RemappingStaticTopic(RemappingStaticTopicSubCommand),

    #[command(
        name = "topicClusterList",
        about = "Get cluster info for topic",
        long_about = r#"Get cluster info for a given topic. This command queries which clusters contain the specified topic."#
    )]
    TopicCluster(TopicClusterSubCommand),

    #[command(
        name = "topicList",
        about = "Get topic list",
        long_about = r#"Fetch all topic list from name server."#
    )]
    TopicList(TopicListSubCommand),

    #[command(
        name = "topicRoute",
        about = "Examine topic route info",
        long_about = r#"Examine topic route info."#
    )]
    TopicRoute(TopicRouteSubCommand),

    #[command(
        name = "topicStatus",
        about = "Examine topic status info",
        long_about = r#"Examine topic status info."#
    )]
    TopicStatus(TopicStatusSubCommand),

    #[command(
        name = "updateOrderConf",
        about = "Create or update order conf",
        long_about = r#"Create, update, or delete order topic configuration."#
    )]
    UpdateOrderConf(UpdateOrderConfSubCommand),

    #[command(
        name = "updateStaticTopic",
        about = "Update static topic",
        long_about = r#"Update or create static topic with queue mapping."#
    )]
    UpdateStaticTopic(UpdateStaticTopicSubCommand),

    #[command(
        name = "updateTopicList",
        about = "Update topic list",
        long_about = "Update topic list with specified configuration."
    )]
    UpdateTopicList(UpdateTopicListSubCommand),

    #[command(
        name = "updateTopicPerm",
        about = "Update topic perm.",
        long_about = r#"Update topic perm."#
    )]
    UpdateTopicPerm(UpdateTopicPermSubCommand),

    #[command(
        name = "updateTopic",
        about = "Update or create topic",
        long_about = r#"Update or create topic with specified configuration."#
    )]
    UpdateTopic(UpdateTopicSubCommand),
}

impl CommandExecute for TopicCommands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            TopicCommands::AllocateMQ(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::DeleteTopic(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::RemappingStaticTopic(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::TopicCluster(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::TopicList(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::TopicRoute(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::TopicStatus(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::UpdateOrderConf(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::UpdateStaticTopic(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::UpdateTopicList(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::UpdateTopicPerm(cmd) => cmd.execute(rpc_hook).await,
            TopicCommands::UpdateTopic(cmd) => cmd.execute(rpc_hook).await,
        }
    }
}
