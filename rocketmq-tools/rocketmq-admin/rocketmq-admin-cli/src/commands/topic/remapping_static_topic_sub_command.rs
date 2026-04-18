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
use rocketmq_admin_core::core::static_topic::RemappingStaticTopicRequest;
use rocketmq_admin_core::core::static_topic::StaticTopicMappingFileRequest;
use rocketmq_admin_core::core::static_topic::StaticTopicMappingPlan;
use rocketmq_admin_core::core::static_topic::StaticTopicService;
use rocketmq_common::FileUtils::file_to_string;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct RemappingStaticTopicSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'b',
        long = "brokers",
        required = false,
        help = "remapping static topic to brokers, comma separated"
    )]
    brokers: Option<String>,

    #[arg(
        short = 'c',
        long = "clusters",
        required = false,
        help = "remapping static topic to clusters, comma separated"
    )]
    clusters: Option<String>,

    #[arg(short = 'm', long = "mapFile", required = false, help = "The mapping data file name")]
    mapping_file: Option<String>,

    #[arg(
        short = 'f',
        long = "forceReplace",
        required = false,
        help = "Force replace the old mapping"
    )]
    force_replace: Option<bool>,
}

impl RemappingStaticTopicSubCommand {
    fn remapping_request(&self) -> RocketMQResult<RemappingStaticTopicRequest> {
        RemappingStaticTopicRequest::try_new(
            self.topic.clone(),
            self.brokers.clone(),
            self.clusters.clone(),
            self.force_replace,
        )
        .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn mapping_file_request(&self) -> RocketMQResult<StaticTopicMappingFileRequest> {
        StaticTopicMappingFileRequest::try_new(self.topic.clone(), self.force_replace.unwrap_or(false))
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    pub async fn execute_from_file(
        &self,
        map_file_name: &str,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<()> {
        let map_file_name = map_file_name.trim();
        if let Ok(map_data) = file_to_string(map_file_name) {
            if let Ok(wrapper) = serde_json::from_str::<TopicRemappingDetailWrapper>(&map_data) {
                StaticTopicService::remapping_static_topic_from_mapping_by_request_with_rpc_hook(
                    self.mapping_file_request()?,
                    wrapper,
                    rpc_hook,
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

impl CommandExecute for RemappingStaticTopicSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if let Some(f_name) = &self.mapping_file {
            return self.execute_from_file(f_name, rpc_hook).await;
        }

        let plan =
            StaticTopicService::remapping_static_topic_by_request_with_rpc_hook(self.remapping_request()?, rpc_hook)
                .await?;
        Self::write_mapping_plan(&plan)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remapping_static_topic_sub_command_parse_broker_mode() {
        let args = vec!["remapping-static-topic", "-t", "test-topic", "-b", "broker-a,broker-b"];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.brokers, Some("broker-a,broker-b".to_string()));
    }

    #[test]
    fn remapping_static_topic_sub_command_parse_cluster_mode() {
        let args = vec!["remapping-static-topic", "-t", "test-topic", "-c", "DefaultCluster"];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.clusters, Some("DefaultCluster".to_string()));
    }

    #[test]
    fn remapping_static_topic_sub_command_parse_with_file() {
        let args = vec!["remapping-static-topic", "-t", "test-topic", "-m", "/tmp/mapping.json"];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        assert_eq!(cmd.mapping_file, Some("/tmp/mapping.json".to_string()));
    }

    #[test]
    fn remapping_static_topic_sub_command_builds_core_request() {
        let args = vec![
            "remapping-static-topic",
            "-t",
            " test-topic ",
            "-b",
            " broker-a,broker-b ",
            "-f",
            "true",
            "-n",
            " 127.0.0.1:9876 ",
        ];

        let cmd = RemappingStaticTopicSubCommand::try_parse_from(args).unwrap();
        let request = cmd.remapping_request().unwrap();

        assert_eq!(request.topic().as_str(), "test-topic");
        assert_eq!(
            request
                .broker_names()
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["broker-a", "broker-b"]
        );
        assert!(request.force_replace());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
