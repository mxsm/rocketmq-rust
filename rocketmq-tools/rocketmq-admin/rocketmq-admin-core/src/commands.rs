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

pub mod command_util;

mod auth_commands;
mod broker_commands;
mod cluster_commands;
mod consumer_commands;
mod controller_commands;
mod namesrv_commands;
mod target;
mod topic_commands;

use std::sync::Arc;

use clap::Parser;
use clap::Subcommand;
use rocketmq_remoting::runtime::RPCHook;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use crate::core::RocketMQResult;

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

    /// Skip confirmation prompts (automatically answer 'yes')
    #[arg(
        short = 'y',
        long = "yes",
        help = "Skip confirmation prompts for dangerous operations"
    )]
    pub skip_confirm: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(subcommand)]
    #[command(about = "Auth commands")]
    #[command(name = "auth")]
    Auth(auth_commands::AuthCommands),

    #[command(subcommand)]
    #[command(about = "Broker commands")]
    #[command(name = "broker")]
    Broker(broker_commands::BrokerCommands),

    #[command(subcommand)]
    #[command(about = "Cluster commands")]
    #[command(name = "cluster")]
    Cluster(cluster_commands::ClusterCommands),

    #[command(subcommand)]
    #[command(about = "Consumer commands")]
    #[command(name = "consumer")]
    Consumer(consumer_commands::ConsumerCommands),

    #[command(subcommand)]
    #[command(about = "Controller commands")]
    #[command(name = "controller")]
    Controller(controller_commands::ControllerCommands),

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
            Commands::Auth(value) => value.execute(rpc_hook).await,
            Commands::Broker(value) => value.execute(rpc_hook).await,
            Commands::Cluster(value) => value.execute(rpc_hook).await,
            Commands::Consumer(value) => value.execute(rpc_hook).await,
            Commands::Controller(value) => value.execute(rpc_hook).await,
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
                category: "Auth",
                command: "copyAcl",
                remark: "Copy acl to cluster.",
            },
            Command {
                category: "Auth",
                command: "copyUser",
                remark: "Copy user to cluster.",
            },
            Command {
                category: "Auth",
                command: "createAcl",
                remark: "Create acl to cluster.",
            },
            Command {
                category: "Auth",
                command: "createUser",
                remark: "Create user to cluster.",
            },
            Command {
                category: "Auth",
                command: "deleteAcl",
                remark: "Delete acl from cluster.",
            },
            Command {
                category: "Auth",
                command: "deleteUser",
                remark: "Delete user from cluster.",
            },
            Command {
                category: "Auth",
                command: "listAcl",
                remark: "List acl from cluster.",
            },
            Command {
                category: "Auth",
                command: "updateAcl",
                remark: "Update ACL.",
            },
            Command {
                category: "Broker",
                command: "brokerConsumeStats",
                remark: "Fetch broker consume stats data.",
            },
            Command {
                category: "Broker",
                command: "brokerStatus",
                remark: "Fetch broker runtime status data.",
            },
            Command {
                category: "Broker",
                command: "cleanExpiredCQ",
                remark: "Clean expired ConsumeQueue on broker.",
            },
            Command {
                category: "Broker",
                command: "cleanUnusedTopic",
                remark: "Clean unused topic on broker.",
            },
            Command {
                category: "Broker",
                command: "deleteExpiredCommitLog",
                remark: "Delete expired CommitLog files.",
            },
            Command {
                category: "Broker",
                command: "getBrokerConfig",
                remark: "Get broker config by cluster or special broker.",
            },
            Command {
                category: "Broker",
                command: "getBrokerEpoch",
                remark: "Fetch broker epoch entries.",
            },
            Command {
                category: "Broker",
                command: "getColdDataFlowCtrInfo",
                remark: "Get cold data flow ctr info.",
            },
            Command {
                category: "Broker",
                command: "removeColdDataFlowCtrGroupConfig",
                remark: "Remove consumer from cold ctr config.",
            },
            Command {
                category: "Broker",
                command: "resetMasterFlushOffset",
                remark: "Reset master flush offset in slave.",
            },
            Command {
                category: "Broker",
                command: "sendMsgStatus",
                remark: "Send msg to broker.",
            },
            Command {
                category: "Broker",
                command: "switchTimerEngine",
                remark: "Switch the engine of timer message in broker.",
            },
            Command {
                category: "Broker",
                command: "updateBrokerConfig",
                remark: "Update broker config by broker or cluster.",
            },
            Command {
                category: "Broker",
                command: "setCommitLogReadAheadMode",
                remark: "Set read ahead mode for all commitlog files.",
            },
            Command {
                category: "Cluster",
                command: "clusterRT",
                remark: "List All clusters Message Send RT.",
            },
            Command {
                category: "Cluster",
                command: "clusterList",
                remark: "List cluster infos.",
            },
            Command {
                category: "Consumer",
                command: "consumerStatus",
                remark: "Query and display consumer's internal data structures.",
            },
            Command {
                category: "Consumer",
                command: "consumer",
                remark: "Query consumer's connection, status, etc.",
            },
            Command {
                category: "Consumer",
                command: "deleteSubGroup",
                remark: "Delete subscription group from broker.",
            },
            Command {
                category: "Consumer",
                command: "updateSubGroup",
                remark: "Update consumer sub group.",
            },
            Command {
                category: "Consumer",
                command: "updateSubGroupList",
                remark: "Create or update subscription groups in batch.",
            },
            Command {
                category: "Consumer",
                command: "startMonitoring",
                remark: "Start Monitoring.",
            },
            Command {
                category: "Controller",
                command: "cleanBrokerMetadata",
                remark: "Clean metadata of broker on controller.",
            },
            Command {
                category: "Controller",
                command: "getControllerConfig",
                remark: "Get configuration of controller(s).",
            },
            Command {
                category: "Controller",
                command: "getControllerMetaData",
                remark: "Get meta data of controller.",
            },
            Command {
                category: "Topic",
                command: "allocateMQ",
                remark: "Allocate MQ.",
            },
            Command {
                category: "Topic",
                command: "deleteTopic",
                remark: "Delete topic.",
            },
            Command {
                category: "Topic",
                command: "remappingStaticTopic",
                remark: "Remapping static topic.",
            },
            Command {
                category: "Topic",
                command: "topicClusterList",
                remark: "Get cluster info for topic.",
            },
            Command {
                category: "Topic",
                command: "topicList",
                remark: "Get topic list.",
            },
            Command {
                category: "Topic",
                command: "topicRoute",
                remark: "Examine topic route info.",
            },
            Command {
                category: "Topic",
                command: "topicStatus",
                remark: "Examine topic status info.",
            },
            Command {
                category: "Topic",
                command: "updateOrderConf",
                remark: "Create or update order conf.",
            },
            Command {
                category: "Topic",
                command: "updateStaticTopic",
                remark: "Update or create static topic.",
            },
            Command {
                category: "Topic",
                command: "updateTopicPerm",
                remark: "Update topic perm.",
            },
            Command {
                category: "Topic",
                command: "updateTopic",
                remark: "Update or create topic.",
            },
            Command {
                category: "NameServer",
                command: "addWritePerm",
                remark: "Add write perm of broker in all name server.",
            },
            Command {
                category: "NameServer",
                command: "deleteKvConfig",
                remark: "Delete KV config.",
            },
            Command {
                category: "NameServer",
                command: "getNamesrvConfig",
                remark: "Get configs of name server.",
            },
            Command {
                category: "NameServer",
                command: "updateKvConfig",
                remark: "Create or update KV config.",
            },
            Command {
                category: "NameServer",
                command: "updateNamesrvConfig",
                remark: "Update configs of name server.",
            },
            Command {
                category: "NameServer",
                command: "wipeWritePerm",
                remark: "Wipe write perm of broker in all name server.",
            },
        ];
        let mut table = Table::new(commands);
        table.with(Style::extended());
        print!("{table}");
        Ok(())
    }
}
