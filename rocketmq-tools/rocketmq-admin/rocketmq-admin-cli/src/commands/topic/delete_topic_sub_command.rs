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
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::DeleteTopicRequest;
use rocketmq_admin_core::core::topic::DeleteTopicResult;
use rocketmq_admin_core::core::topic::TopicService;

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

    fn print_result(result: DeleteTopicResult) {
        println!(
            "delete topic {} from cluster {} success",
            result.topic, result.cluster_name
        );
        println!("delete topic {} from NameServer success", result.topic);
    }
}
impl CommandExecute for DeleteTopicSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
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
        let result = TopicService::delete_topic_by_request(self.request()?).await?;
        Self::print_result(result);
        Ok(())
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
}
