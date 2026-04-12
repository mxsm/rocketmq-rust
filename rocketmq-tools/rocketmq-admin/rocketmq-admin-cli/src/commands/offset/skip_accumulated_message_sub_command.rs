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
use rocketmq_admin_core::core::offset::OffsetService;
use rocketmq_admin_core::core::offset::SkipAccumulatedMessageRequest;
use rocketmq_admin_core::core::offset::SkipAccumulatedMessageResult;

#[derive(Debug, Clone, Parser)]
pub struct SkipAccumulatedMessageSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'g', long = "group", required = true, help = "set the consumer group")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 'f',
        long = "force",
        required = false,
        help = "set the force rollback by timestamp switch[true|false]"
    )]
    force: Option<bool>,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "Cluster name or lmq parent topic, lmq is used to find the route."
    )]
    cluster: Option<String>,
}

impl SkipAccumulatedMessageSubCommand {
    fn request(&self) -> RocketMQResult<SkipAccumulatedMessageRequest> {
        SkipAccumulatedMessageRequest::try_new(
            self.group.clone(),
            self.topic.clone(),
            self.cluster.clone(),
            self.force,
            self.common_args.namesrv_addr.clone(),
        )
    }
}

impl CommandExecute for SkipAccumulatedMessageSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            OffsetService::skip_accumulated_message_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        print_skip_accumulated_message_result(result);
        Ok(())
    }
}

fn print_skip_accumulated_message_result(result: SkipAccumulatedMessageResult) {
    match result {
        SkipAccumulatedMessageResult::Current(offset_table) => {
            println!("{:<40}  {:<40}  {:<40}", "#brokerName", "#queueId", "#offset");
            for (mq, offset) in offset_table {
                println!("{:<40}  {:<40}  {:<40}", mq.broker_name(), mq.queue_id(), offset);
            }
        }
        SkipAccumulatedMessageResult::Legacy(rollback_stats) => {
            println!(
                "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
                "#brokerName", "#queueId", "#brokerOffset", "#consumerOffset", "#timestampOffset", "#rollbackOffset"
            );

            for stat in rollback_stats {
                println!(
                    "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
                    stat.broker_name,
                    stat.queue_id,
                    stat.broker_offset,
                    stat.consumer_offset,
                    stat.timestamp_offset,
                    stat.rollback_offset
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_skip_accumulated_message_request() {
        let cmd = SkipAccumulatedMessageSubCommand::try_parse_from([
            "skipAccumulatedMessage",
            "-g",
            " TestGroup ",
            "-t",
            " TestTopic ",
            "-c",
            " DefaultCluster ",
            "-f",
            "false",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();
        let request = cmd.request().unwrap();

        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.cluster().unwrap().as_str(), "DefaultCluster");
        assert!(!request.force());
    }
}
