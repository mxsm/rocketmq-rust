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
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::TopicClusterList;
use rocketmq_admin_core::core::topic::TopicClusterQueryRequest;
use rocketmq_admin_core::core::topic::TopicService;

#[derive(Debug, Clone, Parser)]
pub struct TopicClusterSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,
}

impl TopicClusterSubCommand {
    fn request(&self) -> RocketMQResult<TopicClusterQueryRequest> {
        Ok(TopicClusterQueryRequest::try_new(self.topic.clone())?
            .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    async fn get_topic_clusters(&self) -> RocketMQResult<TopicClusterList> {
        TopicService::query_topic_clusters(self.request()?).await
    }

    fn print_clusters(&self, result: &TopicClusterList) {
        for cluster in &result.clusters {
            println!("{}", cluster);
        }
    }
}
impl CommandExecute for TopicClusterSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> rocketmq_error::RocketMQResult<()> {
        let clusters = self.get_topic_clusters().await?;
        self.print_clusters(&clusters);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_cluster_sub_command_parse() {
        let args = vec!["topicClusterList", "-t", "test-topic"];

        let cmd = TopicClusterSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "test-topic");
    }

    #[test]
    fn topic_cluster_sub_command_with_namesrv() {
        let args = vec!["topicClusterList", "-t", "test-topic", "-n", "127.0.0.1:9876"];

        let cmd = TopicClusterSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.common_args.namesrv_addr, Some("127.0.0.1:9876".to_string()));
    }

    #[test]
    fn topic_cluster_sub_command_missing_topic() {
        let args = vec!["topicClusterList"];

        let cmd = TopicClusterSubCommand::try_parse_from(args);
        assert!(cmd.is_err());
    }
}
