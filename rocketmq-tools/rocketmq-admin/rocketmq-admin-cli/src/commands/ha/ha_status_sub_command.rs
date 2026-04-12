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
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::ha::HaService;
use rocketmq_admin_core::core::ha::HaStatusQueryRequest;
use rocketmq_admin_core::core::ha::HaStatusQueryResult;

#[derive(Debug, Clone, Parser)]
pub struct HAStatusSubCommand {
    #[arg(
        short = 'b',
        long = "brokerAddr",
        value_name = "HOST:PORT",
        required = false,
        help = "which broker to fetch"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'c',
        long = "clusterName",
        value_name = "CLUSTER",
        required = false,
        help = "which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'i',
        long = "interval",
        value_name = "SECONDS",
        required = false,
        help = "the interval(second) of get info"
    )]
    interval: Option<u64>,
}

impl CommandExecute for HAStatusSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if let Some(interval) = self.interval {
            let flush_second = if interval > 0 { interval } else { 3 };
            loop {
                let result =
                    HaService::query_ha_status_by_request_with_rpc_hook(self.request()?, rpc_hook.clone()).await?;
                Self::print_status_result(&result);
                tokio::time::sleep(tokio::time::Duration::from_secs(flush_second)).await;
            }
        } else {
            let result = HaService::query_ha_status_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
            Self::print_status_result(&result);
        }

        Ok(())
    }
}

impl HAStatusSubCommand {
    fn request(&self) -> RocketMQResult<HaStatusQueryRequest> {
        HaStatusQueryRequest::try_new(self.broker_addr.clone(), self.cluster_name.clone())
    }

    fn print_status_result(result: &HaStatusQueryResult) {
        for entry in &result.entries {
            if entry.runtime_info.master {
                Self::print_master_status(entry.broker_addr.as_str(), &entry.runtime_info);
            } else {
                Self::print_slave_status(&entry.runtime_info);
            }
        }
    }

    fn print_master_status(broker_addr: &str, info: &HARuntimeInfo) {
        let slave_num = info.ha_connection_info.len();

        println!("\n#MasterAddr             {}", broker_addr);
        println!("#MasterCommitLogMaxOffset        {}", info.master_commit_log_max_offset);
        println!("#SlaveNum                        {}", slave_num);
        println!("#InSyncSlaveNum                  {}", info.in_sync_slave_nums);

        println!(
            "\n{:<36} {:<20} {:<17} {:<22} {:<17} #TransferFromWhere",
            "#SlaveAddr", "#SlaveAckOffset", "#Diff", "#TransferSpeed(KB/s)", "#Status"
        );

        for conn in &info.ha_connection_info {
            let status = if conn.in_sync { "OK" } else { "Fall Behind" };
            let transfer_speed = conn.transferred_byte_in_second as f64 / 1024.0;

            println!(
                "{:<36} {:<20} {:<17} {:<22.2} {:<17} {}",
                conn.addr, conn.slave_ack_offset, conn.diff, transfer_speed, status, conn.transfer_from_where,
            );
        }
    }

    fn print_slave_status(info: &HARuntimeInfo) {
        let client = &info.ha_client_runtime_info;
        let transfer_speed = client.transferred_byte_in_second as f64 / 1024.0;

        println!("\n#MasterAddr             {}", client.master_addr);
        println!("#CommitLogMaxOffset     {}", client.max_offset);
        println!("#TransferSpeed(KB/s)    {:.2}", transfer_speed);
        println!(
            "#LastReadTime           {}",
            time_millis_to_human_string2(client.last_read_timestamp as i64)
        );
        println!(
            "#LastWriteTime          {}",
            time_millis_to_human_string2(client.last_write_timestamp as i64)
        );
        println!("#MasterFlushOffset      {}", client.master_flush_offset);
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_admin_core::core::ha::HaStatusTarget;

    use super::*;

    #[test]
    fn ha_status_sub_command_parse_broker_target() {
        let cmd = HAStatusSubCommand::try_parse_from(["haStatus", "-b", " 127.0.0.1:10911 "]).unwrap();

        assert_eq!(
            cmd.request().unwrap().target(),
            &HaStatusTarget::BrokerAddr(CheetahString::from("127.0.0.1:10911"))
        );
    }

    #[test]
    fn ha_status_sub_command_parse_cluster_target() {
        let cmd = HAStatusSubCommand::try_parse_from(["haStatus", "-c", " DefaultCluster "]).unwrap();

        assert_eq!(
            cmd.request().unwrap().target(),
            &HaStatusTarget::ClusterName(CheetahString::from("DefaultCluster"))
        );
    }
}
