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
use rocketmq_common::UtilAll::YYYY_MM_DD_HH_MM_SS_SSS;
use rocketmq_common::UtilAll::parse_date;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::admin::rollback_stats::RollbackStats;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::offset::OffsetService;
use rocketmq_admin_core::core::offset::ResetOffsetByTimeOldRequest;

#[derive(Debug, Clone, Parser)]
pub struct ResetOffsetByTimeOldSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'g', long = "group", required = true, help = "set the consumer group")]
    group: String,

    #[arg(short = 't', long = "topic", required = true, help = "set the topic")]
    topic: String,

    #[arg(
        short = 's',
        long = "timestamp",
        required = true,
        help = "set the timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]"
    )]
    timestamp: String,

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

impl ResetOffsetByTimeOldSubCommand {
    fn request(&self) -> RocketMQResult<Option<ResetOffsetByTimeOldRequest>> {
        let timestamp_str = self.timestamp.trim();
        let timestamp = match timestamp_str.parse::<u64>() {
            Ok(timestamp) => timestamp,
            Err(_) => {
                if let Some(date) = parse_date(timestamp_str, YYYY_MM_DD_HH_MM_SS_SSS) {
                    let millis = date.and_utc().timestamp_millis();
                    if millis < 0 {
                        println!("specified timestamp invalid.");
                        return Ok(None);
                    }
                    millis as u64
                } else {
                    println!("specified timestamp invalid.");
                    return Ok(None);
                }
            }
        };

        Ok(Some(ResetOffsetByTimeOldRequest::try_new(
            self.group.clone(),
            self.topic.clone(),
            timestamp,
            self.force,
            self.cluster.clone(),
            self.common_args.namesrv_addr.clone(),
        )?))
    }

    fn print_result(
        request: &ResetOffsetByTimeOldRequest,
        timestamp_str: &str,
        rollback_stats_list: Vec<RollbackStats>,
    ) {
        println!(
            "reset consumer offset by specified consumerGroup[{}], topic[{}], force[{}], timestamp(string)[{}], \
             timestamp(long)[{}]",
            request.group(),
            request.topic(),
            request.force(),
            timestamp_str,
            request.timestamp()
        );
        println!(
            "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
            "#brokerName", "#queueId", "#brokerOffset", "#consumerOffset", "#timestampOffset", "#resetOffset"
        );

        for rollback_stats in rollback_stats_list {
            let broker_name = rollback_stats.broker_name.to_string();
            let broker_name = if broker_name.chars().count() > 32 {
                broker_name.chars().take(32).collect::<String>()
            } else {
                broker_name
            };

            println!(
                "{:<20}  {:<20}  {:<20}  {:<20}  {:<20}  {:<20}",
                broker_name,
                rollback_stats.queue_id,
                rollback_stats.broker_offset,
                rollback_stats.consumer_offset,
                rollback_stats.timestamp_offset,
                rollback_stats.rollback_offset
            );
        }
    }
}

impl CommandExecute for ResetOffsetByTimeOldSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let Some(request) = self.request()? else {
            return Ok(());
        };

        let rollback_stats =
            OffsetService::reset_offset_by_time_old_by_request_with_rpc_hook(request.clone(), rpc_hook).await?;
        Self::print_result(&request, self.timestamp.trim(), rollback_stats);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn parses_reset_offset_by_time_old_request() {
        let cmd = ResetOffsetByTimeOldSubCommand::try_parse_from([
            "resetOffsetByTimeOld",
            "-g",
            " TestGroup ",
            "-t",
            " TestTopic ",
            "-s",
            "1234",
            "-f",
            "false",
            "-c",
            " DefaultCluster ",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();
        let request = cmd.request().unwrap().unwrap();

        assert_eq!(request.group().as_str(), "TestGroup");
        assert_eq!(request.topic().as_str(), "TestTopic");
        assert_eq!(request.cluster().unwrap().as_str(), "DefaultCluster");
        assert_eq!(request.timestamp(), 1234);
        assert!(!request.force());
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
