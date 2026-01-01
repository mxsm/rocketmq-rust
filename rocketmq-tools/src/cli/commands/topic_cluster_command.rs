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

//! Topic cluster list command - CLI layer
//!
//! This is the thin CLI wrapper around core business logic

use clap::Parser;
use rocketmq_remoting::runtime::RPCHook;

use crate::cli::formatters::OutputFormat;
use crate::cli::validators;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicService;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;

#[derive(Debug, Clone, Parser)]
pub struct TopicClusterSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(
        short = 'f',
        long = "format",
        default_value = "table",
        help = "Output format: table, json, yaml"
    )]
    format: String,
}

impl CommandExecute for TopicClusterSubCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // 1. Validate input
        validators::validate_topic_name(&self.topic)?;

        // 2. Build admin client with RAII guard (auto cleanup)
        let mut builder = AdminBuilder::new();

        if let Some(addr) = &self.common_args.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }

        let mut admin = builder.build_with_guard().await?;

        // 3. Call core business logic (admin auto-shuts down on drop)
        let cluster_list = TopicService::get_topic_cluster_list(&mut admin, &self.topic)
            .await
            .map_err(|e| {
                eprintln!("Failed to get cluster list for topic '{}': {e}", self.topic);
                e
            })?;

        // 4. Format and display result

        let format = OutputFormat::from(self.format.as_str());
        match format {
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(&cluster_list.clusters)?;
                println!("{json}");
            }
            OutputFormat::Yaml => {
                let yaml = serde_yaml::to_string(&cluster_list.clusters).map_err(|e| {
                    RocketMQError::Serialization(rocketmq_error::SerializationError::encode_failed(
                        "YAML",
                        e.to_string(),
                    ))
                })?;
                println!("{yaml}");
            }
            OutputFormat::Table => {
                println!("Clusters for topic '{}':", self.topic);
                cluster_list.clusters.iter().for_each(|cluster| {
                    println!("  - {cluster}");
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_parsing() {
        let cmd =
            TopicClusterSubCommand::try_parse_from(["topicClusterList", "-t", "TestTopic", "-n", "127.0.0.1:9876"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "TestTopic");
    }

    #[test]
    fn test_command_with_format() {
        let cmd = TopicClusterSubCommand::try_parse_from(["topicClusterList", "-t", "TestTopic", "-f", "json"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.format, "json");
    }
}
