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
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .args(&["broker_addr", "cluster_name"])
))]
pub struct BrokerStatusSubCommand {
    #[arg(short = 'b', long = "brokerAddr", help = "Broker address")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", help = "which cluster")]
    cluster_name: Option<String>,
}

impl BrokerStatusSubCommand {
    async fn print_broker_runtime_stats(
        default_mqadmin_ext: &DefaultMQAdminExt,
        broker_addr: &str,
        print_broker: bool,
    ) -> RocketMQResult<()> {
        let kv_table = default_mqadmin_ext
            .fetch_broker_runtime_stats(CheetahString::from(broker_addr))
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "BrokerStatusSubCommand: Failed to fetch broker runtime stats from {}: {}",
                    broker_addr, e
                ))
            })?;

        let mut sorted_entries: Vec<_> = kv_table.table.iter().collect();
        sorted_entries.sort_by(|a, b| a.0.cmp(b.0));

        for (key, value) in &sorted_entries {
            if print_broker {
                println!("{:<24} {:<32}: {}", broker_addr, key, value);
            } else {
                println!("{:<32}: {}", key, value);
            }
        }

        Ok(())
    }
}

impl CommandExecute for BrokerStatusSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(get_current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("BrokerStatusSubCommand: Failed to start MQAdminExt: {}", e))
            })?;

            if let Some(broker_addr) = &self.broker_addr {
                let broker_addr = broker_addr.trim();
                Self::print_broker_runtime_stats(&default_mqadmin_ext, broker_addr, false).await?;
            } else if let Some(cluster_name) = &self.cluster_name {
                let cluster_name = cluster_name.trim();
                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                    RocketMQError::Internal(format!(
                        "BrokerStatusSubCommand: Failed to examine broker cluster info: {}",
                        e
                    ))
                })?;

                let master_set = CommandUtil::fetch_master_and_slave_addr_by_cluster_name(&cluster_info, cluster_name)?;

                for broker_addr in &master_set {
                    match Self::print_broker_runtime_stats(&default_mqadmin_ext, broker_addr.as_str(), true).await {
                        Ok(()) => {}
                        Err(e) => {
                            eprintln!(
                                "BrokerStatusSubCommand: Failed to fetch runtime stats from {}: {}",
                                broker_addr, e
                            );
                        }
                    }
                }
            }

            Ok(())
        }
        .await;

        MQAdminExt::shutdown(&mut default_mqadmin_ext).await;
        operation_result
    }
}
