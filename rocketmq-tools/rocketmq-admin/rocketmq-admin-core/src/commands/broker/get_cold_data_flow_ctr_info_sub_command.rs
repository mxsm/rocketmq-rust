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
use rocketmq_common::utils::util_all::time_millis_to_human_string2;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use serde_json::Value;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::command_util::CommandUtil;
use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct GetColdDataFlowCtrInfoSubCommand {
    #[arg(short = 'b', long = "brokerAddr", required = false, help = "get from which broker")]
    broker_addr: Option<String>,

    #[arg(short = 'c', long = "clusterName", required = false, help = "get from which cluster")]
    cluster_name: Option<String>,
}

impl GetColdDataFlowCtrInfoSubCommand {
    async fn get_and_print(
        &self,
        default_mqadmin_ext: &DefaultMQAdminExt,
        print_prefix: &str,
        addr: &str,
    ) -> RocketMQResult<()> {
        print!(" {}", print_prefix);

        let rst_str = default_mqadmin_ext
            .get_cold_data_flow_ctr_info(CheetahString::from(addr))
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetColdDataFlowCtrInfoSubCommand: Failed to get cold data flow ctr info from broker {}: {}",
                    addr, e
                ))
            })?;

        if rst_str.is_empty() {
            println!("Broker[{}] has no cold ctr table !", addr);
            return Ok(());
        }

        let mut json_value: Value = serde_json::from_str(rst_str.as_str()).map_err(|e| {
            RocketMQError::Internal(format!(
                "GetColdDataFlowCtrInfoSubCommand: Failed to parse JSON response from broker {}: {}",
                addr, e
            ))
        })?;

        if let Some(runtime_table) = json_value.get_mut("runtimeTable") {
            if let Some(table_obj) = runtime_table.as_object_mut() {
                for (_key, entry) in table_obj.iter_mut() {
                    if let Some(entry_obj) = entry.as_object_mut() {
                        if let Some(last_cold_read_time) = entry_obj.remove("lastColdReadTimeMills") {
                            let millis = match &last_cold_read_time {
                                Value::Number(n) => n.as_i64().unwrap_or(0),
                                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                                _ => 0,
                            };
                            entry_obj.insert(
                                "lastColdReadTimeFormat".to_string(),
                                Value::String(time_millis_to_human_string2(millis)),
                            );
                        }

                        if let Some(create_time) = entry_obj.remove("createTimeMills") {
                            let millis = match &create_time {
                                Value::Number(n) => n.as_i64().unwrap_or(0),
                                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                                _ => 0,
                            };
                            entry_obj.insert(
                                "createTimeFormat".to_string(),
                                Value::String(time_millis_to_human_string2(millis)),
                            );
                        }
                    }
                }
            }
        }

        let format_str = serde_json::to_string_pretty(&json_value).map_err(|e| {
            RocketMQError::Internal(format!(
                "GetColdDataFlowCtrInfoSubCommand: Failed to format JSON: {}",
                e
            ))
        })?;
        println!("{}", format_str);

        Ok(())
    }
}

impl CommandExecute for GetColdDataFlowCtrInfoSubCommand {
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
                RocketMQError::Internal(format!(
                    "GetColdDataFlowCtrInfoSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            if let Some(broker_addr) = &self.broker_addr {
                let broker_addr = broker_addr.trim();
                self.get_and_print(
                    &default_mqadmin_ext,
                    &format!("============{}============\n", broker_addr),
                    broker_addr,
                )
                .await?;
            } else if let Some(cluster_name) = &self.cluster_name {
                let cluster_name = cluster_name.trim();
                let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                    RocketMQError::Internal(format!(
                        "GetColdDataFlowCtrInfoSubCommand: Failed to examine broker cluster info: {}",
                        e
                    ))
                })?;

                let master_and_slave_map =
                    CommandUtil::fetch_master_and_slave_distinguish(&cluster_info, cluster_name)?;

                let mut sorted_masters: Vec<_> = master_and_slave_map.keys().collect();
                sorted_masters.sort();

                for master_addr in &sorted_masters {
                    let slave_addrs = &master_and_slave_map[*master_addr];
                    self.get_and_print(
                        &default_mqadmin_ext,
                        &format!("============Master: {}============\n", master_addr),
                        master_addr.as_str(),
                    )
                    .await?;

                    let mut sorted_slaves: Vec<_> = slave_addrs.iter().collect();
                    sorted_slaves.sort();

                    for slave_addr in &sorted_slaves {
                        self.get_and_print(
                            &default_mqadmin_ext,
                            &format!(
                                "============My Master: {}=====Slave: {}============\n",
                                master_addr, slave_addr
                            ),
                            slave_addr.as_str(),
                        )
                        .await?;
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
