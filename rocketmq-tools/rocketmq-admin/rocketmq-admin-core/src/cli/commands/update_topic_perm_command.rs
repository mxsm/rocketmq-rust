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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::topic::TopicOperations;
use crate::core::topic::TopicTarget;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

/// Update topic permission
#[derive(Debug, Clone, Parser)]
pub struct UpdateTopicPermCommand {
    /// Topic name
    #[arg(short = 't', long = "topic", required = true)]
    pub topic: String,

    /// Permission: 2=W, 4=R, 6=RW
    #[arg(short = 'p', long = "perm", required = true)]
    pub perm: i32,

    /// Broker address (mutually exclusive with cluster)
    #[arg(short = 'b', long = "broker-addr", conflicts_with = "cluster")]
    pub broker_addr: Option<String>,

    /// Cluster name (mutually exclusive with broker_addr)
    #[arg(short = 'c', long = "cluster", conflicts_with = "broker_addr")]
    pub cluster: Option<String>,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for UpdateTopicPermCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if !matches!(self.perm, 2 | 4 | 6) {
            return Err(ToolsError::InvalidPermission {
                value: self.perm,
                allowed: vec![2, 4, 6],
            }
            .into());
        }

        let target = match (&self.broker_addr, &self.cluster) {
            (Some(broker), None) => TopicTarget::Broker(CheetahString::from(broker.clone())),
            (None, Some(cluster)) => TopicTarget::Cluster(CheetahString::from(cluster.clone())),
            (None, None) => {
                return Err(ToolsError::ValidationFailed {
                    message: "Either broker_addr or cluster must be specified".to_string(),
                }
                .into())
            }
            (Some(_), Some(_)) => {
                unreachable!("clap should prevent this due to conflicts_with")
            }
        };

        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        TopicOperations::update_topic_perm(&mut admin, CheetahString::from(self.topic.clone()), self.perm, target)
            .await?;

        let perm_str = match self.perm {
            2 => "W (Write only)",
            4 => "R (Read only)",
            6 => "RW (Read and Write)",
            _ => unreachable!(),
        };

        match (&self.broker_addr, &self.cluster) {
            (Some(broker), None) => {
                println!(
                    "Successfully updated topic '{}' permission to {} on broker: {}",
                    self.topic, perm_str, broker
                );
            }
            (None, Some(cluster)) => {
                println!(
                    "Successfully updated topic '{}' permission to {} on cluster: {}",
                    self.topic, perm_str, cluster
                );
            }
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_topic_perm_with_broker() {
        let cmd = UpdateTopicPermCommand::try_parse_from([
            "update_topic_perm",
            "-t",
            "TestTopic",
            "-p",
            "6",
            "-n",
            "127.0.0.1:9876",
            "-b",
            "127.0.0.1:10911",
        ])
        .unwrap();

        assert_eq!(cmd.topic, "TestTopic");
        assert_eq!(cmd.perm, 6);
        assert!(cmd.broker_addr.is_some());
        assert!(cmd.cluster.is_none());
    }

    #[test]
    fn test_update_topic_perm_with_cluster() {
        let cmd = UpdateTopicPermCommand::try_parse_from([
            "update_topic_perm",
            "-t",
            "TestTopic",
            "-p",
            "2",
            "-n",
            "127.0.0.1:9876",
            "-c",
            "DefaultCluster",
        ])
        .unwrap();

        assert_eq!(cmd.perm, 2);
        assert!(cmd.broker_addr.is_none());
        assert!(cmd.cluster.is_some());
    }
}
