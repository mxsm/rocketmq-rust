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
use clap::ArgGroup;
use clap::Parser;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(
    ArgGroup::new("target")
        .required(true)
        .args(&["broker_name", "cluster_name"])
))]
pub struct GetBrokerEpochSubCommand {
    #[arg(short = 'b', long = "brokerName", help = "which broker to get epoch")]
    broker_name: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "which cluster to get epoch")]
    cluster_name: Option<String>,

    #[arg(
        short = 'i',
        long = "interval",
        required = false,
        help = "the interval(second) of get info"
    )]
    interval: Option<u64>,
}

impl CommandExecute for GetBrokerEpochSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
            RocketMQError::Internal(format!("GetBrokerEpochSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        let operation_result = if let Some(interval) = self.interval {
            let flush_second = if interval > 0 { interval } else { 3 };
            loop {
                let result = self.inner_exec(&default_mqadmin_ext).await;
                if let Err(ref e) = result {
                    eprintln!("GetBrokerEpochSubCommand: error: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(flush_second)).await;
            }
        } else {
            self.inner_exec(&default_mqadmin_ext).await
        };

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}

impl GetBrokerEpochSubCommand {
    async fn inner_exec(&self, default_mqadmin_ext: &DefaultMQAdminExt) -> RocketMQResult<()> {
        if let Some(ref broker_name) = self.broker_name {
            let broker_name = broker_name.trim();
            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetBrokerEpochSubCommand: Failed to examine broker cluster info: {}",
                    e
                ))
            })?;
            let brokers = CommandUtil::fetch_master_and_slave_addr_by_broker_name(&cluster_info, broker_name)?;
            self.print_data(
                &brokers.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                default_mqadmin_ext,
            )
            .await?;
        } else if let Some(ref cluster_name) = self.cluster_name {
            let cluster_name = cluster_name.trim();
            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetBrokerEpochSubCommand: Failed to examine broker cluster info: {}",
                    e
                ))
            })?;
            let brokers = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?;
            self.print_data(
                &brokers.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                default_mqadmin_ext,
            )
            .await?;
        }
        Ok(())
    }

    async fn print_data(&self, brokers: &[&str], default_mqadmin_ext: &DefaultMQAdminExt) -> RocketMQResult<()> {
        for broker_addr in brokers {
            let mut epoch_cache = default_mqadmin_ext
                .get_broker_epoch_cache(CheetahString::from(*broker_addr))
                .await
                .map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetBrokerEpochSubCommand: Failed to get broker epoch cache from {}: {}",
                        broker_addr, e
                    ))
                })?;

            println!(
                "\n#clusterName\t{}\n#brokerName\t{}\n#brokerAddr\t{}\n#brokerId\t{}",
                epoch_cache.get_cluster_name(),
                epoch_cache.get_broker_name(),
                broker_addr,
                epoch_cache.get_broker_id()
            );

            let max_offset = epoch_cache.get_max_offset();
            let epoch_list = epoch_cache.get_epoch_list_mut();
            let len = epoch_list.len();
            for (i, epoch_entry) in epoch_list.iter_mut().enumerate() {
                if i == len - 1 {
                    epoch_entry.set_end_offset(max_offset as i64);
                }
                println!("#Epoch: {}", epoch_entry);
            }
        }
        Ok(())
    }
}
