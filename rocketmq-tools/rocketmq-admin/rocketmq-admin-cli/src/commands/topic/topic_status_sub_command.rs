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
use rocketmq_common::UtilAll::time_millis_to_human_string2;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::topic::TopicStatusQueryRequest;

#[derive(Debug, Clone, Parser)]
pub struct TopicStatusSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,
}
impl CommandExecute for TopicStatusSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = TopicStatusQueryRequest::try_new(self.topic.clone())?
            .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone())
            .with_optional_cluster_name(self.cluster_name.clone());
        let topic_status = TopicService::query_topic_status(request).await?;

        let offset_table = topic_status.get_offset_table();
        let mut mq_list: Vec<_> = offset_table.keys().cloned().collect();
        mq_list.sort();

        println!("#Broker Name #QID #Min Offset #Max Offset #Last Updated");

        for queue in &mq_list {
            let topic_offset = offset_table.get(queue);
            if let Some(offset) = topic_offset {
                if offset.get_last_update_timestamp() > 0 {
                    let human_timestamp = time_millis_to_human_string2(offset.get_last_update_timestamp());
                    println!(
                        "{}  {}  {}  {}  {}",
                        queue.broker_name(),
                        queue.queue_id(),
                        offset.get_min_offset(),
                        offset.get_max_offset(),
                        human_timestamp
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_status_sub_command_parse() {
        let cmd = TopicStatusSubCommand::try_parse_from([
            "topicStatus",
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
    fn topic_status_sub_command_missing_topic() {
        let cmd = TopicStatusSubCommand::try_parse_from(["topicStatus"]);

        assert!(cmd.is_err());
    }
}
