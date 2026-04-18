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

use chrono::FixedOffset;
use chrono::Utc;
use clap::Parser;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtRequest;
use rocketmq_admin_core::core::cluster::ClusterSendMessageRtResult;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::cluster::ClusterService;

fn get_cur_time() -> String {
    let offset = FixedOffset::east_opt(8 * 3600).unwrap();
    let now = Utc::now().with_timezone(&offset);
    now.format("%Y-%m-%d %H:%M:%S").to_string()
}

#[derive(Debug, Clone, Parser)]
pub struct ClusterSendMsgRTSubCommand {
    #[arg(
        short = 'a',
        long = "amount",
        required = false,
        default_value = "100",
        help = "message amount | default 100"
    )]
    amount: u64,

    #[arg(
        short = 's',
        long = "size",
        required = false,
        default_value = "128",
        help = "message size | default 128 Byte"
    )]
    size: u64,

    #[arg(
        short = 'c',
        long = "cluster",
        required = false,
        help = "cluster name | default display all cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'p',
        long = "printLog",
        required = false,
        default_value = "false",
        help = "print as tlog | default false"
    )]
    print_as_tlog: bool,

    #[arg(
        short = 'm',
        long = "machineRoom",
        required = false,
        default_value = "noname",
        help = "machine room name | default noname"
    )]
    machine_room: String,

    #[arg(
        short = 'i',
        long = "interval",
        required = false,
        default_value = "10",
        help = "print interval | default 10 seconds"
    )]
    interval: u64,
}

impl ClusterSendMsgRTSubCommand {
    fn request(&self) -> RocketMQResult<ClusterSendMessageRtRequest> {
        ClusterSendMessageRtRequest::try_new(self.amount, self.size, self.cluster_name.clone())
    }
}

impl CommandExecute for ClusterSendMsgRTSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if !self.print_as_tlog {
            println!(
                "{:<24}  {:<24}  {:<4}  {:<8}  {:<8}",
                "#Cluster Name", "#Broker Name", "#RT", "#successCount", "#failCount"
            );
        }

        loop {
            let result =
                ClusterService::send_message_rt_by_request_with_rpc_hook(self.request()?, rpc_hook.clone()).await?;
            self.print_result(&result);
            tokio::time::sleep(tokio::time::Duration::from_secs(self.interval)).await;
        }
    }
}

impl ClusterSendMsgRTSubCommand {
    fn print_result(&self, result: &ClusterSendMessageRtResult) {
        Self::print_missing_clusters(result);
        for row in &result.rows {
            if !self.print_as_tlog {
                println!(
                    "{:<24}  {:<24}  {:<8}  {:<16}  {:<16}",
                    row.cluster_name,
                    row.broker_name,
                    format!("{:.2}", row.rt),
                    row.success_count,
                    row.fail_count
                );
            } else {
                println!(
                    "{}|{}|{}|{}|{}",
                    get_cur_time(),
                    self.machine_room,
                    row.cluster_name,
                    row.broker_name,
                    row.rt.round() as u64
                );
            }
        }
    }

    fn print_missing_clusters(result: &ClusterSendMessageRtResult) {
        for cluster_name in &result.missing_clusters {
            println!("cluster [{}] not exist", cluster_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_send_msg_rt_sub_command_parse_request() {
        let cmd = ClusterSendMsgRTSubCommand::try_parse_from(["clusterRT", "-c", " DefaultCluster "]).unwrap();

        assert_eq!(
            cmd.request().unwrap().cluster_name().map(|name| name.as_str()),
            Some("DefaultCluster")
        );
    }
}
