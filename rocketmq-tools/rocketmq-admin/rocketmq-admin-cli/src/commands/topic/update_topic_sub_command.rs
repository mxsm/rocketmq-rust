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
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::topic::TopicService;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_admin_core::core::topic::UpdateTopicRequest;
use rocketmq_admin_core::core::topic::UpdateTopicResult;

#[derive(Debug, Clone, Parser)]
pub struct UpdateTopicSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "create topic to which broker"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "create topic to which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(short = 't', long = "topic", required = true, help = "topic name")]
    topic: String,

    #[arg(
        short = 'r',
        long = "readQueueNums",
        required = false,
        default_value = "8",
        value_parser = clap::value_parser!(u32).range(1..=65535),
        help = "set read queue nums"
    )]
    read_queue_nums: u32,

    #[arg(
        short = 'w',
        long = "writeQueueNums",
        required = false,
        default_value = "8",
        value_parser = clap::value_parser!(u32).range(1..=65535),
        help = "set write queue nums"
    )]
    write_queue_nums: u32,

    #[arg(
        short = 'p',
        long = "perm",
        required = false,
        value_parser = clap::builder::PossibleValuesParser::new(["2", "4", "6"]),
        help = "set topic's permission(2|4|6), intro[2:W 4:R; 6:RW]"
    )]
    perm: Option<String>,

    #[arg(
        short = 'o',
        long = "order",
        required = false,
        help = "set topic's order(true|false)"
    )]
    order: Option<bool>,

    #[arg(short = 'u', long = "unit", required = false, help = "is unit topic (true|false)")]
    unit: Option<bool>,

    #[arg(
        short = 's',
        long = "hasUnitSub",
        required = false,
        help = "has unit sub (true|false)"
    )]
    has_unit_sub: Option<bool>,
}

impl UpdateTopicSubCommand {
    fn request(&self) -> RocketMQResult<UpdateTopicRequest> {
        let target = if let Some(broker_addr) = &self.broker_addr {
            TopicTarget::Broker(broker_addr.trim().into())
        } else if let Some(cluster_name) = &self.cluster_name {
            TopicTarget::Cluster(cluster_name.trim().into())
        } else {
            return Err(RocketMQError::IllegalArgument(
                "UpdateTopicSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ));
        };

        let perm = self
            .perm
            .as_ref()
            .map(|perm| perm.parse::<u32>().expect("clap validated perm to be 2, 4, or 6"));

        Ok(UpdateTopicRequest::try_new(
            self.topic.clone(),
            target,
            self.read_queue_nums,
            self.write_queue_nums,
            perm,
            self.order,
            self.unit,
            self.has_unit_sub,
        )?
        .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(result: UpdateTopicResult) {
        match &result.target {
            TopicTarget::Broker(broker_addr) => println!("create topic to {} success.", broker_addr),
            TopicTarget::Cluster(cluster_name) => println!("create topic to cluster {} success.", cluster_name),
        }
        if result.order_warning {
            println!(
                "Warning: Order message topic created, but order configuration (queue allocation) is not yet \
                 implemented."
            );
        }
        println!("{:?}", result.config);
    }
}

impl CommandExecute for UpdateTopicSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let validation_result = TopicValidator::validate_topic(&self.topic);
        if !validation_result.valid() {
            return Err(RocketMQError::IllegalArgument(format!(
                "UpdateTopicSubCommand: Invalid topic name: {}",
                validation_result.remark().as_str()
            )));
        }

        let result = TopicService::create_or_update_topic_by_request(self.request()?).await?;
        Self::print_result(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_topic_sub_command_parse_broker_mode() {
        let args = vec![
            "update-topic",
            "-t",
            "test-topic",
            "-b",
            "127.0.0.1:10911",
            "-r",
            "8",
            "-w",
            "8",
            "-p",
            "6",
        ];

        let cmd = UpdateTopicSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.topic, "test-topic");
        assert_eq!(cmd.broker_addr, Some("127.0.0.1:10911".to_string()));
        assert_eq!(cmd.read_queue_nums, 8);
        assert_eq!(cmd.write_queue_nums, 8);
        assert_eq!(cmd.perm, Some("6".to_string()));
    }

    #[test]
    fn cluster_mode() {
        let args = vec!["update-topic", "-t", "test-topic", "-c", "DefaultCluster"];

        let cmd = UpdateTopicSubCommand::try_parse_from(args);
        assert!(cmd.is_ok());

        let cmd = cmd.unwrap();
        assert_eq!(cmd.cluster_name, Some("DefaultCluster".to_string()));
        assert!(cmd.broker_addr.is_none());
    }

    #[test]
    fn conflicting_args() {
        let args = vec![
            "update-topic",
            "-t",
            "test-topic",
            "-b",
            "127.0.0.1:10911",
            "-c",
            "DefaultCluster",
        ];

        let cmd = UpdateTopicSubCommand::try_parse_from(args);
        assert!(cmd.is_err());
    }
}
