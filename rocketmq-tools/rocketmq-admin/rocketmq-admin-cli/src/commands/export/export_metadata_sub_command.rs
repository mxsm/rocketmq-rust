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

use std::sync::Arc;

use clap::Parser;
use rocketmq_admin_core::core::export_data::ExportMetadataRequest;
use rocketmq_admin_core::core::export_data::ExportMetadataResult;
use rocketmq_admin_core::core::export_data::ExportMetadataScope;
use rocketmq_admin_core::core::export_data::ExportService;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

const DEFAULT_FILE_PATH: &str = "/tmp/rocketmq/export";

#[derive(Debug, Clone, Parser)]
pub struct ExportMetadataSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "choose a cluster to export"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "choose a broker to export"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'f',
        long = "filePath",
        required = false,
        default_value = DEFAULT_FILE_PATH,
        help = "export metadata.json path | default /tmp/rocketmq/export"
    )]
    file_path: String,

    #[arg(
        short = 't',
        long = "topic",
        required = false,
        default_value = "false",
        help = "only export topic metadata"
    )]
    topic: bool,

    #[arg(
        short = 'g',
        long = "subscriptionGroup",
        required = false,
        default_value = "false",
        help = "only export subscriptionGroup metadata"
    )]
    subscription_group: bool,

    #[arg(
        short = 's',
        long = "specialTopic",
        required = false,
        default_value = "false",
        help = "need retryTopic and dlqTopic"
    )]
    special_topic: bool,
}

impl ExportMetadataSubCommand {
    fn request(&self) -> RocketMQResult<ExportMetadataRequest> {
        ExportMetadataRequest::try_new(
            self.cluster_name.clone(),
            self.broker_addr.clone(),
            self.topic,
            self.subscription_group,
            self.special_topic,
        )
        .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn write_result(result: &ExportMetadataResult, file_path: &str) -> RocketMQResult<()> {
        let (export_path, json_content) = match result {
            ExportMetadataResult::BrokerTopic { wrapper } => (
                format!("{}/topic.json", file_path),
                serde_json::to_string_pretty(wrapper)
                    .map_err(|e| RocketMQError::Internal(format!("Failed to serialize topic config: {}", e)))?,
            ),
            ExportMetadataResult::BrokerSubscriptionGroup { wrapper } => (
                format!("{}/subscriptionGroup.json", file_path),
                serde_json::to_string_pretty(wrapper).map_err(|e| {
                    RocketMQError::Internal(format!("Failed to serialize subscription group config: {}", e))
                })?,
            ),
            ExportMetadataResult::Cluster {
                scope,
                topic_config_table,
                subscription_group_table,
                export_time_millis,
            } => {
                let mut output = serde_json::Map::new();
                let export_path = match scope {
                    ExportMetadataScope::Topic => {
                        output.insert(
                            "topicConfigTable".to_string(),
                            serde_json::to_value(topic_config_table).map_err(|e| {
                                RocketMQError::Internal(format!("Failed to serialize topic config: {}", e))
                            })?,
                        );
                        format!("{}/topic.json", file_path)
                    }
                    ExportMetadataScope::SubscriptionGroup => {
                        output.insert(
                            "subscriptionGroupTable".to_string(),
                            serde_json::to_value(subscription_group_table).map_err(|e| {
                                RocketMQError::Internal(format!("Failed to serialize subscription group config: {}", e))
                            })?,
                        );
                        format!("{}/subscriptionGroup.json", file_path)
                    }
                    ExportMetadataScope::All => {
                        output.insert(
                            "topicConfigTable".to_string(),
                            serde_json::to_value(topic_config_table).map_err(|e| {
                                RocketMQError::Internal(format!("Failed to serialize topic config: {}", e))
                            })?,
                        );
                        output.insert(
                            "subscriptionGroupTable".to_string(),
                            serde_json::to_value(subscription_group_table).map_err(|e| {
                                RocketMQError::Internal(format!("Failed to serialize subscription group config: {}", e))
                            })?,
                        );
                        format!("{}/metadata.json", file_path)
                    }
                };

                output.insert(
                    "exportTime".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(*export_time_millis)),
                );

                let json_content = serde_json::to_string_pretty(&output)
                    .map_err(|e| RocketMQError::Internal(format!("Failed to serialize export result: {}", e)))?;
                (export_path, json_content)
            }
        };

        rocketmq_common::FileUtils::string_to_file(&json_content, &export_path)?;
        println!("export {} success", export_path);

        Ok(())
    }
}

impl CommandExecute for ExportMetadataSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let file_path = self.file_path.trim();
        let result = ExportService::export_metadata_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;

        Self::write_result(&result, file_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_admin_core::core::export_data::ExportMetadataTarget;

    #[test]
    fn export_metadata_sub_command_builds_cluster_request() {
        let cmd = ExportMetadataSubCommand::try_parse_from([
            "exportMetadata",
            "-c",
            " DefaultCluster ",
            "-n",
            " 127.0.0.1:9876 ",
            "-s",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(
            request.target(),
            &ExportMetadataTarget::Cluster("DefaultCluster".into())
        );
        assert_eq!(request.scope(), ExportMetadataScope::All);
        assert!(request.special_topic());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn export_metadata_sub_command_builds_broker_topic_request() {
        let cmd =
            ExportMetadataSubCommand::try_parse_from(["exportMetadata", "-b", " 127.0.0.1:10911 ", "-t"]).unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(
            request.target(),
            &ExportMetadataTarget::Broker("127.0.0.1:10911".into())
        );
        assert_eq!(request.scope(), ExportMetadataScope::Topic);
    }
}
