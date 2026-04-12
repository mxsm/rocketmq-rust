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
use rocketmq_admin_core::core::cluster::ClusterListMode;
use rocketmq_admin_core::core::cluster::ClusterListQueryRequest;
use rocketmq_admin_core::core::cluster::ClusterListQueryResult;
use rocketmq_admin_core::core::cluster::ClusterService;

#[derive(Debug, Clone, Parser)]
pub struct ClusterListSubCommand {
    #[arg(
        short = 'm',
        long = "moreStats",
        required = false,
        default_value = "false",
        help = "Print more stats"
    )]
    more_stats: bool,

    #[arg(
        short = 'i',
        long = "interval",
        required = false,
        help = "specify intervals numbers, it is in seconds"
    )]
    interval: Option<u64>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "which cluster")]
    cluster_name: Option<String>,
}

impl CommandExecute for ClusterListSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let print_interval = self.interval.map(|i| i * 1000);
        let mut iteration = 0u64;
        loop {
            if iteration > 0 {
                if let Some(interval_ms) = print_interval {
                    tokio::time::sleep(tokio::time::Duration::from_millis(interval_ms)).await;
                }
            }
            iteration += 1;

            let result =
                ClusterService::query_cluster_list_by_request_with_rpc_hook(self.request(), rpc_hook.clone()).await?;
            Self::print_result(&result);

            if print_interval.is_none() {
                break;
            }
        }
        Ok(())
    }
}

impl ClusterListSubCommand {
    fn request(&self) -> ClusterListQueryRequest {
        ClusterListQueryRequest::new(self.more_stats, self.cluster_name.clone())
    }

    fn print_result(result: &ClusterListQueryResult) {
        match result.mode {
            ClusterListMode::Base => Self::print_cluster_base_info(result),
            ClusterListMode::MoreStats => Self::print_cluster_more_stats(result),
        }
    }

    fn print_cluster_more_stats(result: &ClusterListQueryResult) {
        println!(
            "{:<16}  {:<32} {:>14} {:>14} {:>14} {:>14}",
            "#Cluster Name", "#Broker Name", "#InTotalYest", "#OutTotalYest", "#InTotalToday", "#OutTotalToday"
        );

        for row in &result.more_stats_rows {
            println!(
                "{:<16}  {:<32} {:>14} {:>14} {:>14} {:>14}",
                row.cluster_name,
                row.broker_name,
                row.in_total_yesterday,
                row.out_total_yesterday,
                row.in_total_today,
                row.out_total_today
            );
        }
    }

    fn print_cluster_base_info(result: &ClusterListQueryResult) {
        println!(
            "{:<22}  {:<22}  {:<4}  {:<22} {:<16}  {:>16}  {:>30}  {:<22}  {:>11}  {:<12}  {:<8}  {:>10}",
            "#Cluster Name",
            "#Broker Name",
            "#BID",
            "#Addr",
            "#Version",
            "#InTPS(LOAD)",
            "#OutTPS(LOAD)",
            "#Timer(Progress)",
            "#PCWait(ms)",
            "#Hour",
            "#SPACE",
            "#ACTIVATED"
        );

        for row in &result.base_rows {
            println!(
                "{:<22}  {:<22}  {:<4}  {:<22} {:<16}  {:>16}  {:>30}  {:<22}  {:>11}  {:<12}  {:<8}  {:>10}",
                row.cluster_name,
                row.broker_name,
                row.broker_id,
                row.broker_addr,
                row.version,
                row.in_tps,
                row.out_tps,
                row.timer_progress,
                row.page_cache_lock_time_millis,
                row.hour,
                row.space,
                row.broker_active
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_list_sub_command_parse_base_request() {
        let cmd = ClusterListSubCommand::try_parse_from(["clusterList", "-c", " DefaultCluster "]).unwrap();

        let request = cmd.request();
        assert_eq!(request.mode(), ClusterListMode::Base);
        assert_eq!(request.cluster_name().map(|name| name.as_str()), Some("DefaultCluster"));
    }

    #[test]
    fn cluster_list_sub_command_parse_more_stats_request() {
        let cmd = ClusterListSubCommand::try_parse_from(["clusterList", "-m"]).unwrap();

        let request = cmd.request();
        assert_eq!(request.mode(), ClusterListMode::MoreStats);
        assert_eq!(request.cluster_name(), None);
    }
}
