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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckStatus;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const THIRTY_DAYS_MILLIS: i64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Parser)]
pub struct CheckRocksdbCqWriteProgressSubCommand {
    #[arg(short = 'c', long = "cluster", required = true, help = "Cluster name")]
    cluster_name: String,

    #[arg(short = 'n', long = "nameserverAddr", required = true, help = "nameserver address")]
    namesrv_addr: String,

    #[arg(short = 't', long = "topic", help = "topic name")]
    topic: Option<String>,

    #[arg(long = "checkFrom", help = "check from time")]
    check_from: Option<i64>,
}

impl CommandExecute for CheckRocksdbCqWriteProgressSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };

        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());
        default_mqadmin_ext.set_namesrv_addr(self.namesrv_addr.trim());

        let cluster_name = self.cluster_name.trim().to_string();
        let topic = self.topic.as_deref().map(|t| t.trim().to_string()).unwrap_or_default();
        let check_store_time = self
            .check_from
            .unwrap_or_else(|| current_millis() as i64 - THIRTY_DAYS_MILLIS);

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "CheckRocksdbCqWriteProgressSubCommand: Failed to start MQAdminExt: {}",
                    e
                ))
            })?;

            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                RocketMQError::Internal(format!(
                    "CheckRocksdbCqWriteProgressSubCommand: Failed to examine broker cluster info: {}",
                    e
                ))
            })?;

            let cluster_addr_table = cluster_info
                .cluster_addr_table
                .as_ref()
                .ok_or_else(|| RocketMQError::Internal("clusterAddrTable is empty".into()))?;

            if cluster_addr_table.get(cluster_name.as_str()).is_none() {
                println!("clusterAddrTable is empty");
                return Ok(());
            }

            let broker_addr_table = cluster_info
                .broker_addr_table
                .as_ref()
                .ok_or_else(|| RocketMQError::Internal("brokerAddrTable is empty".into()))?;

            for (broker_name, broker_data) in broker_addr_table {
                let broker_addr = match broker_data.broker_addrs().get(&0u64) {
                    Some(addr) => addr.clone(),
                    None => continue,
                };

                match default_mqadmin_ext
                    .check_rocksdb_cq_write_progress(broker_addr, CheetahString::from(topic.as_str()), check_store_time)
                    .await
                {
                    Ok(result) => {
                        let check_status = result.get_check_status();
                        let check_result = result.check_result.as_deref().unwrap_or("");
                        if check_status == CheckStatus::CheckError {
                            println!(
                                "{} check error, please check log... errInfo: {}",
                                broker_name, check_result
                            );
                        } else {
                            println!(
                                "{} check doing, please wait and get the result from log...",
                                broker_name
                            );
                        }
                    }
                    Err(e) => {
                        println!("{} check error: {}", broker_name, e);
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
