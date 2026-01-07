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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct TopicClusterSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 't', long = "topic", required = true, help = "Topic name")]
    topic: String,
}

impl TopicClusterSubCommand {
    fn init_admin_ext(&self) -> DefaultMQAdminExt {
        let mut admin_ext = DefaultMQAdminExt::new();
        admin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());
        if let Some(addr) = &self.common_args.namesrv_addr {
            admin_ext.set_namesrv_addr(addr.trim());
        }
        admin_ext
    }

    async fn get_topic_clusters(&self, admin_ext: &mut DefaultMQAdminExt) -> RocketMQResult<HashSet<CheetahString>> {
        admin_ext
            .get_topic_cluster_list(self.topic.trim().to_string())
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "TopicClusterSubCommand: Failed to get topic cluster list: {}",
                    e
                ))
            })
    }

    fn print_clusters(&self, clusters: &HashSet<CheetahString>) {
        for cluster in clusters {
            println!("{}", cluster);
        }
    }
}
impl CommandExecute for TopicClusterSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> rocketmq_error::RocketMQResult<()> {
        let mut admin_ext = self.init_admin_ext();
        MQAdminExt::start(&mut admin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!("TopicClusterSubCommand: Failed to start MQAdminExt: {}", e))
        })?;
        let operation_result = async {
            let clusters = self.get_topic_clusters(&mut admin_ext).await?;
            self.print_clusters(&clusters);
            Ok(())
        }
        .await;
        admin_ext.shutdown().await;
        operation_result
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
