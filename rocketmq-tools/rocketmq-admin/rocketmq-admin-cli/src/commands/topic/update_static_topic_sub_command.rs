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
use rocketmq_admin_core::core::static_topic::StaticTopicMappingFileRequest;
use rocketmq_admin_core::core::static_topic::StaticTopicMappingPlan;
use rocketmq_admin_core::core::static_topic::StaticTopicService;
use rocketmq_admin_core::core::static_topic::UpdateStaticTopicRequest;
use rocketmq_common::FileUtils::file_to_string;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct UpdateStaticTopicSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = true,
        help = "create topic to which broker"
    )]
    broker_addr: String,

    #[arg(short = 'q', long = "totalQueueNum", required = true, help = "totalQueueNum")]
    queue_num: String,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(short = 'm', long = "mapFile", required = false, help = "The mapping data file name")]
    mapping_file: Option<String>,

    #[arg(
        short = 'f',
        long = "forceReplace",
        required = false,
        help = "Force replace the old mapping"
    )]
    force_replace: Option<String>,
}
impl UpdateStaticTopicSubCommand {
    fn update_request(&self) -> RocketMQResult<UpdateStaticTopicRequest> {
        UpdateStaticTopicRequest::try_new(
            self.topic.clone(),
            self.broker_addr.clone(),
            &self.queue_num,
            self.cluster_name.clone(),
        )
        .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn mapping_file_request(&self) -> RocketMQResult<StaticTopicMappingFileRequest> {
        StaticTopicMappingFileRequest::try_new(self.topic.clone(), self.force_replace())
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn force_replace(&self) -> bool {
        self.force_replace
            .as_ref()
            .and_then(|value| value.trim().parse::<bool>().ok())
            .unwrap_or(false)
    }

    pub async fn execute_from_file(
        &self,
        map_file_name: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let map_file_name = map_file_name.trim();
        if let Ok(map_data) = file_to_string(map_file_name) {
            if let Ok(wrapper) = serde_json::from_str::<TopicRemappingDetailWrapper>(&map_data) {
                let request = self.mapping_file_request()?;
                StaticTopicService::update_static_topic_from_mapping_by_request_with_rpc_hook(
                    request, wrapper, rpc_hook,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn write_mapping_plan(plan: &StaticTopicMappingPlan) -> RocketMQResult<()> {
        let old_mapping_data_file = TopicQueueMappingUtils::write_to_temp(&plan.old_mapping, false)?;
        println!("The old mapping data is written to file {}", old_mapping_data_file);

        let new_mapping_data_file = TopicQueueMappingUtils::write_to_temp(&plan.new_mapping, true)?;
        println!("The new mapping data is written to file {}", new_mapping_data_file);

        Ok(())
    }
}
impl CommandExecute for UpdateStaticTopicSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if let Some(f_name) = &self.mapping_file {
            return self.execute_from_file(f_name, rpc_hook).await;
        }

        let plan =
            StaticTopicService::update_static_topic_by_request_with_rpc_hook(self.update_request()?, rpc_hook).await?;
        Self::write_mapping_plan(&plan)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_static_topic_sub_command_builds_core_request() {
        let cmd = UpdateStaticTopicSubCommand::try_parse_from([
            "updateStaticTopic",
            "-t",
            " TopicA ",
            "-b",
            " broker-a,broker-b ",
            "-q",
            " 8 ",
            "-c",
            " ClusterA ",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();

        let request = cmd.update_request().unwrap();

        assert_eq!(request.topic().as_str(), "TopicA");
        assert_eq!(
            request
                .broker_names()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["broker-a", "broker-b"]
        );
        assert_eq!(
            request
                .cluster_names()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["ClusterA"]
        );
        assert_eq!(request.queue_num(), 8);
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
