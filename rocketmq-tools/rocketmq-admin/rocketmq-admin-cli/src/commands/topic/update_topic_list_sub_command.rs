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

use std::path::PathBuf;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_admin_core::core::topic::UpdateTopicListRequest;
use rocketmq_admin_core::core::topic::UpdateTopicListResult;

#[derive(Parser)]
pub struct UpdateTopicListSubCommand {
    /// Config file path (JSON or YAML)
    #[arg(short = 'f', long)]
    file: PathBuf,

    /// Broker address (mutually exclusive with -c)
    #[arg(short = 'b', long, conflicts_with = "cluster")]
    broker_addr: Option<String>,

    /// Cluster name (mutually exclusive with -b)
    #[arg(short = 'c', long, conflicts_with = "broker_addr")]
    cluster: Option<String>,

    /// Dry run mode (validate only, don't apply)
    #[arg(long)]
    dry_run: bool,
}

impl UpdateTopicListSubCommand {
    fn target(&self) -> RocketMQResult<TopicTarget> {
        if let Some(broker) = &self.broker_addr {
            return Ok(TopicTarget::Broker(CheetahString::from(broker.trim())));
        }
        if let Some(cluster) = &self.cluster {
            return Ok(TopicTarget::Cluster(CheetahString::from(cluster.trim())));
        }

        Err(RocketMQError::Internal(
            "a broker or cluster is required for command UpdateTopicList".to_string(),
        ))
    }

    fn request(&self, topic_configs: Vec<TopicConfig>) -> RocketMQResult<UpdateTopicListRequest> {
        UpdateTopicListRequest::try_new(self.target()?, topic_configs)
    }

    fn print_result(result: &UpdateTopicListResult) {
        for broker_addr in &result.broker_addrs {
            println!(
                "submit batch of topic config to {} success, please check the result later",
                broker_addr
            );
        }
    }
}

impl CommandExecute for UpdateTopicListSubCommand {
    async fn execute(
        &self,
        _rpc_hook: Option<std::sync::Arc<dyn rocketmq_remoting::runtime::RPCHook>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !self.file.is_file() {
            return Err(RocketMQError::Internal(
                "the file path doesn't point to a valid file".to_string(),
            ));
        }

        let mut topic_config_list_bytes = vec![];
        File::open(&self.file)
            .await
            .map_err(|e| RocketMQError::Internal(format!("open file error {}", e)))?
            .read_to_end(&mut topic_config_list_bytes)
            .await?;
        let topic_configs =
            if let Ok(topic_configs) = serde_json::from_slice::<Vec<TopicConfig>>(&topic_config_list_bytes) {
                topic_configs
            } else if let Ok(topic_configs) = serde_yaml::from_slice::<Vec<TopicConfig>>(&topic_config_list_bytes) {
                topic_configs
            } else {
                return Err(RocketMQError::Internal(
                    "the file isn't in json or yaml format".to_string(),
                ));
            };

        let result = TopicService::update_topic_config_list_by_request(self.request(topic_configs)?).await?;
        Self::print_result(&result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_topic_list_sub_command_builds_broker_request() {
        let command = UpdateTopicListSubCommand::try_parse_from([
            "updateTopicList",
            "-f",
            "topics.json",
            "-b",
            " 127.0.0.1:10911 ",
        ])
        .unwrap();

        let request = command.request(vec![TopicConfig::default()]).unwrap();

        assert_eq!(
            request.target(),
            &TopicTarget::Broker(CheetahString::from_static_str("127.0.0.1:10911"))
        );
        assert_eq!(request.topic_configs().len(), 1);
    }

    #[test]
    fn update_topic_list_sub_command_builds_cluster_request() {
        let command = UpdateTopicListSubCommand::try_parse_from([
            "updateTopicList",
            "-f",
            "topics.json",
            "-c",
            " DefaultCluster ",
        ])
        .unwrap();

        let request = command.request(vec![TopicConfig::default()]).unwrap();

        assert_eq!(
            request.target(),
            &TopicTarget::Cluster(CheetahString::from_static_str("DefaultCluster"))
        );
        assert_eq!(request.topic_configs().len(), 1);
    }

    #[test]
    fn update_topic_list_sub_command_rejects_missing_target() {
        let command = UpdateTopicListSubCommand::try_parse_from(["updateTopicList", "-f", "topics.json"]).unwrap();

        assert!(command.request(vec![TopicConfig::default()]).is_err());
    }
}
