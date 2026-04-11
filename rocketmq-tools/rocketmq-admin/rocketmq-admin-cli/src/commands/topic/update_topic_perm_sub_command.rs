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
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_admin_core::core::topic::UpdateTopicPermRequest;
use rocketmq_admin_core::core::topic::UpdateTopicPermResult;

#[derive(Debug, Parser, Clone)]
pub struct UpdateTopicPermSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "create topic to which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,

    #[arg(
        short = 'p',
        long = "perm",
        required = true,
        help = "set topic's permission(2|4|6), intro[2:W; 4:R; 6:RW]"
    )]
    perm: String,
}

impl UpdateTopicPermSubCommand {
    fn request(&self) -> RocketMQResult<UpdateTopicPermRequest> {
        let target = if let Some(broker_addr) = &self.broker_addr {
            TopicTarget::Broker(broker_addr.trim().into())
        } else if let Some(cluster_name) = &self.cluster_name {
            TopicTarget::Cluster(cluster_name.trim().into())
        } else {
            return Err(RocketMQError::IllegalArgument(
                "UpdateTopicPermSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ));
        };
        let perm = self.perm.parse::<i32>().map_err(|e| {
            RocketMQError::IllegalArgument(format!(
                "UpdateTopicPermSubCommand: Invalid perm value '{}': {}",
                self.perm, e
            ))
        })?;

        Ok(UpdateTopicPermRequest::try_new(self.topic.clone(), target, perm)?
            .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(result: UpdateTopicPermResult) {
        match result.target {
            TopicTarget::Broker(broker_addr) => {
                println!("update topic perm to {} in {} success.", result.perm, broker_addr)
            }
            TopicTarget::Cluster(cluster_name) => {
                println!(
                    "update topic perm to {} in cluster {} success.",
                    result.perm, cluster_name
                )
            }
        }
    }
}

impl CommandExecute for UpdateTopicPermSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let validation_result = TopicValidator::validate_topic(&self.topic);
        if !validation_result.valid() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateTopicPermSubCommand: Invalid topic name: {}",
                validation_result.remark().as_str()
            )));
        }

        let result = TopicService::update_topic_perm_by_request(self.request()?).await?;
        Self::print_result(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::CommonArgs;

    #[test]
    fn test_update_topic_perm_sub_command_parse() {
        // Test parsing with broker address
        let args =
            UpdateTopicPermSubCommand::try_parse_from(["test", "-b", "127.0.0.1:10911", "-t", "TestTopic", "-p", "6"])
                .unwrap();

        assert_eq!(args.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(args.cluster_name, None);
        assert_eq!(args.topic, "TestTopic");
        assert_eq!(args.perm, "6");
    }

    #[test]
    fn test_update_topic_perm_sub_command_parse_with_cluster() {
        // Test parsing with cluster name
        let args =
            UpdateTopicPermSubCommand::try_parse_from(["test", "-c", "DefaultCluster", "-t", "TestTopic", "-p", "4"])
                .unwrap();

        assert_eq!(args.broker_addr, None);
        assert_eq!(args.cluster_name, Some("DefaultCluster".to_string()));
        assert_eq!(args.topic, "TestTopic");
        assert_eq!(args.perm, "4");
    }

    #[tokio::test]
    async fn test_execute_fails_without_broker_or_cluster() {
        let command = UpdateTopicPermSubCommand {
            common_args: CommonArgs {
                namesrv_addr: None,
                skip_confirm: false,
            },
            broker_addr: None,
            cluster_name: None,
            topic: "TestTopic".to_string(),
            perm: "6".to_string(),
        };

        let result = command.execute(None).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Either brokerAddr (-b) or clusterName (-c) must be provided")
        );
    }

    #[tokio::test]
    async fn test_execute_fails_with_invalid_topic() {
        let command = UpdateTopicPermSubCommand {
            common_args: CommonArgs {
                namesrv_addr: None,
                skip_confirm: false,
            },
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            topic: "".to_string(), // Invalid topic name
            perm: "6".to_string(),
        };

        let result = command.execute(None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid topic name"));
    }

    #[test]
    fn test_command_structure() {
        // Verify the command structure
        let command = UpdateTopicPermSubCommand {
            common_args: CommonArgs {
                namesrv_addr: Some("127.0.0.1:9876".to_string()),
                skip_confirm: false,
            },
            broker_addr: Some("127.0.0.1:10911".to_string()),
            cluster_name: None,
            topic: "TestTopic".to_string(),
            perm: "6".to_string(),
        };

        assert_eq!(command.topic, "TestTopic");
        assert_eq!(command.perm, "6");
        assert_eq!(command.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(command.cluster_name, None);
    }
}
