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

use clap::Parser;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::topic::TopicValidator;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::client_adapter::services::topic::DeleteTopicRequest;
use rocketmq_admin_core::client_adapter::services::topic::DeleteTopicResult;
use rocketmq_admin_core::client_adapter::services::topic::TopicService;

#[derive(Debug, Clone, Parser)]
pub struct DeleteTopicSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,
}
impl DeleteTopicSubCommand {
    fn request(&self) -> RocketMQResult<DeleteTopicRequest> {
        Ok(
            DeleteTopicRequest::try_new(self.topic.clone(), self.cluster_name.clone())?
                .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()),
        )
    }

    fn print_result(result: DeleteTopicResult) -> RocketMQResult<()> {
        for broker_addr in &result.broker_addrs {
            println!("delete topic {} from broker {} success", result.topic, broker_addr);
        }
        for failure in &result.failures {
            eprintln!(
                "delete topic {} from broker {} failed [{}]: {}",
                result.topic, failure.broker_addr, failure.error_code, failure.error
            );
        }
        if result.name_server_deleted {
            println!("delete topic {} from NameServer success", result.topic);
        } else if !result.failures.is_empty() {
            eprintln!("NameServer deletion skipped because one or more broker mutations failed");
        }

        if let Some(failure) = result.failures.first() {
            if failure.error_code == "BROKER_PERMISSION_DENIED" {
                return Err(RocketMQError::BrokerPermissionDenied {
                    operation: format!("delete topic {}: {}", result.topic, failure.error),
                });
            }
            return Err(RocketMQError::broker_operation_failed(
                "DELETE_TOPIC_IN_BROKER_LIST",
                -1,
                format!(
                    "failed to delete topic {} from {} broker(s); first failure at {}: {}",
                    result.topic,
                    result.failures.len(),
                    failure.broker_addr,
                    failure.error
                ),
            ));
        }

        Ok(())
    }
}
impl CommandExecute for DeleteTopicSubCommand {
    async fn execute(
        &self,
        credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "DeleteTopicSubCommand: clusterName (-c) must be provided".into(),
            ));
        }
        let validation_result = TopicValidator::validate_topic(&self.topic);
        if !validation_result.valid() {
            return Err(RocketMQError::IllegalArgument(format!(
                "DeleteTopicSubCommand: Invalid topic name: {}",
                validation_result.remark().as_str()
            )));
        }
        let result =
            TopicService::delete_topic_by_request_with_credentials(self.request()?, credentials, client_runtime)
                .await?;
        Self::print_result(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_topic_sub_command_parse() {
        let cmd = DeleteTopicSubCommand::try_parse_from([
            "deleteTopic",
            "-t",
            "TestTopic",
            "-c",
            "DefaultCluster",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.cluster_name, Some("DefaultCluster".to_string()));
        assert_eq!(cmd.common_args.namesrv_addr, Some("127.0.0.1:9876".to_string()));
    }

    #[test]
    fn delete_topic_partial_permission_failure_returns_permission_error() {
        let result = DeleteTopicResult {
            topic: "TestTopic".into(),
            cluster_name: "DefaultCluster".into(),
            broker_addrs: vec!["broker-a:10911".into()],
            failures: vec![
                rocketmq_admin_core::client_adapter::services::topic::TopicOperationFailure {
                    broker_addr: "broker-b:10911".into(),
                    error_code: "BROKER_PERMISSION_DENIED".to_string(),
                    error: "permission denied".to_string(),
                },
            ],
            name_server_deleted: false,
        };

        let error = DeleteTopicSubCommand::print_result(result).unwrap_err();
        assert_eq!(error.spec().code.as_str(), "BROKER_PERMISSION_DENIED");
    }
}
