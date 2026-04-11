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

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::TopicListQueryRequest;
use rocketmq_admin_core::core::topic::TopicListResult;
use rocketmq_admin_core::core::topic::TopicService;

#[derive(Debug, Clone, Parser)]
pub struct TopicListSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,
}

impl TopicListSubCommand {
    fn request(&self) -> TopicListQueryRequest {
        TopicListQueryRequest::new()
            .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone())
            .with_optional_cluster_name(self.cluster_name.clone())
    }

    fn print_topics(&self, result: TopicListResult) {
        if self.cluster_name.is_some() {
            println!("#Cluster Name #Topic #Consumer Group");
            for item in result.topics {
                println!(
                    "{} {} {}",
                    item.cluster.as_ref().map(|value| value.as_str()).unwrap_or(""),
                    item.topic,
                    item.consumer_group.as_ref().map(|value| value.as_str()).unwrap_or("")
                );
            }
            return;
        }

        for item in result.topics {
            println!("{}", item.topic);
        }
    }
}

impl CommandExecute for TopicListSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let result = TopicService::query_topic_list(self.request()).await?;
        self.print_topics(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_list_sub_command_parse() {
        let cmd =
            TopicListSubCommand::try_parse_from(["topicList", "-c", "DefaultCluster", "-n", "127.0.0.1:9876"]).unwrap();

        assert_eq!(cmd.cluster_name, Some("DefaultCluster".to_string()));
        assert_eq!(cmd.common_args.namesrv_addr, Some("127.0.0.1:9876".to_string()));
    }
}
