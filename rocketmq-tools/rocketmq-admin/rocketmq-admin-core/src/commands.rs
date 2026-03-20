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

mod auth;
mod broker;
mod cluster;
mod connection;
mod consumer;
mod controller;
mod export;
mod ha;
mod lite;
mod message;
mod namesrv;
mod offset;
mod producer;
mod queue;
mod stats;
mod target;
mod topic;

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
    Auth(auth::AuthCommands),

    #[command(subcommand)]
    #[command(about = "Broker commands")]
    #[command(name = "broker")]
    Broker(broker::BrokerCommands),

    #[command(subcommand)]
    #[command(about = "Cluster commands")]
    #[command(name = "cluster")]
    Cluster(cluster::ClusterCommands),

    #[command(subcommand)]
    #[command(about = "Connection commands")]
    #[command(name = "connection")]
    Connection(connection::ConnectionCommands),

    #[command(subcommand)]
    #[command(about = "Consumer commands")]
    #[command(name = "consumer")]
    Consumer(consumer::ConsumerCommands),

    #[command(subcommand)]
    #[command(about = "Controller commands")]
    #[command(name = "controller")]
    Controller(controller::ControllerCommands),

    #[command(subcommand)]
    #[command(about = "Export commands")]
    #[command(name = "export")]
    Export(export::ExportCommands),

    #[command(subcommand)]
    #[command(about = "HA commands")]
    #[command(name = "ha")]
    HA(ha::HACommands),

    #[command(subcommand)]
    #[command(about = "Lite commands")]
    #[command(name = "lite")]
    Lite(lite::LiteCommands),

    #[command(subcommand)]
    #[command(about = "Message commands")]
    #[command(name = "message")]
    Message(message::MessageCommands),

    #[command(subcommand)]
    #[command(about = "Name server commands")]
    #[command(name = "nameserver")]
    NameServer(namesrv::NameServerCommands),

    #[command(subcommand)]
    #[command(about = "Offset commands")]
    #[command(name = "offset")]
    Offset(offset::OffsetCommands),

    #[command(subcommand)]
    #[command(about = "Producer commands")]
    #[command(name = "producer")]
    Producer(producer::ProducerCommands),

    #[command(subcommand)]
    #[command(about = "Queue commands")]
    #[command(name = "queue")]
    Queue(queue::QueueCommands),

    #[command(subcommand)]
    #[command(about = "Stats commands")]
    #[command(name = "stats")]
    Stats(stats::StatsCommands),

    #[command(subcommand)]
    #[command(about = "Topic commands")]
    Topic(topic::TopicCommands),

    #[command(about = "Category commands show")]
    Show(ClassificationTablePrint),
}

impl CommandExecute for Commands {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        match self {
            Commands::Auth(value) => value.execute(rpc_hook).await,
            Commands::Broker(value) => value.execute(rpc_hook).await,
            Commands::Cluster(value) => value.execute(rpc_hook).await,
            Commands::Connection(value) => value.execute(rpc_hook).await,
            Commands::Consumer(value) => value.execute(rpc_hook).await,
            Commands::Controller(value) => value.execute(rpc_hook).await,
            Commands::Export(value) => value.execute(rpc_hook).await,
            Commands::HA(value) => value.execute(rpc_hook).await,
            Commands::Lite(value) => value.execute(rpc_hook).await,
            Commands::Message(value) => value.execute(rpc_hook).await,
            Commands::NameServer(value) => value.execute(rpc_hook).await,
            Commands::Offset(value) => value.execute(rpc_hook).await,
            Commands::Producer(value) => value.execute(rpc_hook).await,
            Commands::Queue(value) => value.execute(rpc_hook).await,
            Commands::Stats(value) => value.execute(rpc_hook).await,
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
                category: "Connection",
                command: "consumerConnection",
                remark: "Query consumer's socket connection, client version and subscription.",
            },
            Command {
                category: "Connection",
                command: "producerConnection",
                remark: "Query producer's socket connection and client version.",
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
                command: "getConsumerConfig",
                remark: "Get consumer config by subscription group name.",
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
                category: "Export",
                command: "exportConfigs",
                remark: "Export configs",
            },
            Command {
                category: "Export",
                command: "exportMetadata",
                remark: "Export metadata.",
            },
            Command {
                category: "Export",
                command: "exportMetadataInRocksDB",
                remark: "Export RocksDB kv config (topics/subscriptionGroups). Recommend to use [mqadmin \
                         rocksDBConfigToJson]",
            },
            Command {
                category: "Export",
                command: "exportPopRecord",
                remark: "Export pop consumer records.",
            },
            Command {
                category: "HA",
                command: "getSyncStateSet",
                remark: "Fetch sync state set for target brokers.",
            },
            Command {
                category: "HA",
                command: "haStatus",
                remark: "Fetch ha runtime status data.",
            },
            Command {
                category: "Lite",
                command: "getBrokerLiteInfo",
                remark: "Get broker lite info.",
            },
            Command {
                category: "Lite",
                command: "getLiteClientInfo",
                remark: "Get lite client info.",
            },
            Command {
                category: "Lite",
                command: "getLiteGroupInfo",
                remark: "Get lite group info.",
            },
            Command {
                category: "Lite",
                command: "getLiteTopicInfo",
                remark: "Get lite topic info.",
            },
            Command {
                category: "Lite",
                command: "getParentTopicInfo",
                remark: "Get parent topic info.",
            },
            Command {
                category: "Lite",
                command: "triggerLiteDispatch",
                remark: "Trigger Lite Dispatch.",
            },
            Command {
                category: "Message",
                command: "checkMsgSendRT",
                remark: "Check message send response time.",
            },
            Command {
                category: "Message",
                command: "decodeMessageId",
                remark: "Decode unique message ID.",
            },
            Command {
                category: "Message",
                command: "dumpCompactionLog",
                remark: "Parse compaction log to message.",
            },
            Command {
                category: "Message",
                command: "printMessage",
                remark: "Print Message Detail.",
            },
            Command {
                category: "Message",
                command: "printMsgByQueue",
                remark: "Print Message Detail by queueId.",
            },
            Command {
                category: "Message",
                command: "queryMsgById",
                remark: "Query message by message ID.",
            },
            Command {
                category: "Message",
                command: "queryMsgByKey",
                remark: "Query Message by Key.",
            },
            Command {
                category: "Message",
                command: "queryMsgByOffset",
                remark: "Query Message by offset.",
            },
            Command {
                category: "Message",
                command: "sendMessage",
                remark: "Send a message.",
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
            Command {
                category: "Offset",
                command: "cloneGroupOffset",
                remark: "Clone offset from other group.",
            },
            Command {
                category: "Offset",
                command: "getConsumerStatus",
                remark: "Get consumer status from client.",
            },
            Command {
                category: "Offset",
                command: "resetOffsetByTime",
                remark: "Reset consumer group offsets to a specific timestamp (no restart required).",
            },
            Command {
                category: "Offset",
                command: "resetOffsetByTimeOld",
                remark: "Reset consumer offset by timestamp (execute this command required client restart).",
            },
            Command {
                category: "Offset",
                command: "skipAccumulatedMessage",
                remark: "Skip all messages that are accumulated (not consumed) currently.",
            },
            Command {
                category: "Producer",
                command: "producer",
                remark: "Query producer's instances, connection, status, etc.",
            },
            Command {
                category: "Queue",
                command: "checkRocksdbCqWriteProgress",
                remark: "Check if rocksdb cq is same as file cq.",
            },
            Command {
                category: "Queue",
                command: "queryCq",
                remark: "Query cq command.",
            },
            Command {
                category: "Stats",
                command: "statsAll",
                remark: "Topic and Consumer tps stats.",
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
        ];
        let mut table = Table::new(commands);
        table.with(Style::extended());
        print!("{table}");
        Ok(())
    }
}
