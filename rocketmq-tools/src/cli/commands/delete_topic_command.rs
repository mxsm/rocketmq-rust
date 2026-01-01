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

//! Delete topic command - CLI layer
//!
//! Thin wrapper around core business logic for deleting topics

use clap::Parser;
use rocketmq_remoting::runtime::RPCHook;

use crate::cli::validators;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicService;
use crate::core::RocketMQResult;
use crate::ui::output;
use crate::ui::progress;
use crate::ui::prompt;

#[derive(Debug, Clone, Parser)]
pub struct DeleteTopicCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "Topic name to delete")]
    topic: String,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = true,
        help = "Cluster name where the topic exists"
    )]
    cluster_name: String,
}

impl CommandExecute for DeleteTopicCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // 1. Validate input
        validators::validate_topic_name(&self.topic)?;

        // 2. Confirm dangerous operation (unless --yes flag is set)
        if !self.common_args.skip_confirm {
            let target = format!("topic '{}' from cluster '{}'", self.topic, self.cluster_name);
            if !prompt::confirm_dangerous_operation("delete", &target) {
                output::print_warning("Operation cancelled by user");
                return Ok(());
            }
        }

        // 3. Build admin client with RAII guard (auto cleanup)
        output::print_operation_start(&format!("Deleting topic '{}'", self.topic));
        let spinner = progress::create_spinner("Connecting to NameServer...");

        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common_args.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        spinner.finish_and_clear();

        // 4. Call core business logic (admin auto-shuts down on drop)
        let spinner = progress::create_spinner("Deleting topic...");
        match TopicService::delete_topic(&mut admin, &self.topic, &self.cluster_name).await {
            Ok(_) => {
                progress::finish_progress_success(&spinner, "Topic deleted");
                output::print_success(&format!(
                    "Topic '{}' deleted successfully from cluster '{}'",
                    self.topic, self.cluster_name
                ));
                Ok(())
            }
            Err(e) => {
                progress::finish_progress_error(&spinner, "Deletion failed");
                output::print_error(&format!(
                    "Failed to delete topic '{}' from cluster '{}': {}",
                    self.topic, self.cluster_name, e
                ));
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_parsing() {
        let cmd = DeleteTopicCommand::try_parse_from([
            "deleteTopic",
            "-t",
            "TestTopic",
            "-c",
            "DefaultCluster",
            "-n",
            "127.0.0.1:9876",
        ]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.cluster_name, "DefaultCluster");
        assert!(!cmd.common_args.skip_confirm); // Default is false
    }

    #[test]
    fn test_command_requires_topic() {
        let cmd = DeleteTopicCommand::try_parse_from(["deleteTopic", "-c", "DefaultCluster"]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_command_requires_cluster() {
        let cmd = DeleteTopicCommand::try_parse_from(["deleteTopic", "-t", "TestTopic"]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_command_with_yes_flag() {
        let cmd = DeleteTopicCommand::try_parse_from(["deleteTopic", "-t", "TestTopic", "-c", "DefaultCluster", "-y"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert!(cmd.common_args.skip_confirm);
    }

    #[test]
    fn test_command_with_long_yes_flag() {
        let cmd =
            DeleteTopicCommand::try_parse_from(["deleteTopic", "-t", "TestTopic", "-c", "DefaultCluster", "--yes"]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert!(cmd.common_args.skip_confirm);
    }

    #[test]
    fn test_command_field_values() {
        let cmd = DeleteTopicCommand::try_parse_from([
            "deleteTopic",
            "-t",
            "MyTestTopic",
            "-c",
            "MyCluster",
            "-n",
            "192.168.1.1:9876",
        ]);
        assert!(cmd.is_ok());
        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "MyTestTopic");
        assert_eq!(cmd.cluster_name, "MyCluster");
        assert_eq!(cmd.common_args.namesrv_addr, Some("192.168.1.1:9876".to_string()));
        assert!(!cmd.common_args.skip_confirm);
    }
}
