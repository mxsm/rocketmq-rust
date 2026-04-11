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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_common::UtilAll::time_millis_to_human_string2;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::command_util::CommandUtil;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

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
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("HAStatusSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            if let Some(interval) = self.interval {
                let flush_second = if interval > 0 { interval } else { 3 };
                loop {
                    self.inner_exec(&default_mqadmin_ext).await?;
                    tokio::time::sleep(tokio::time::Duration::from_secs(flush_second)).await;
                }
            } else {
                self.inner_exec(&default_mqadmin_ext).await?;
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

impl HAStatusSubCommand {
    async fn inner_exec(&self, default_mqadmin_ext: &DefaultMQAdminExt) -> RocketMQResult<()> {
        if let Some(ref broker_addr) = self.broker_addr {
            Self::print_status(broker_addr.trim(), default_mqadmin_ext).await?;
        } else if let Some(ref cluster_name) = self.cluster_name {
            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                RocketMQError::Internal(format!("HAStatusSubCommand: Failed to get cluster info: {}", e))
            })?;
            let master_addrs = CommandUtil::fetch_master_addr_by_cluster_name(&cluster_info, cluster_name.trim())?;
            for addr in master_addrs {
                Self::print_status(&addr, default_mqadmin_ext).await?;
            }
        } else {
            println!("Error: either -b (brokerAddr) or -c (clusterName) must be specified");
        }

        Ok(())
    }

    async fn print_status(broker_addr: &str, default_mqadmin_ext: &DefaultMQAdminExt) -> RocketMQResult<()> {
        let ha_runtime_info: HARuntimeInfo = default_mqadmin_ext
            .get_broker_ha_status(CheetahString::from(broker_addr))
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "HAStatusSubCommand: Failed to get broker HA status from {}: {}",
                    broker_addr, e
                ))
            })?;

        if ha_runtime_info.master {
            Self::print_master_status(broker_addr, &ha_runtime_info);
        } else {
            Self::print_slave_status(&ha_runtime_info);
        }

        Ok(())
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
