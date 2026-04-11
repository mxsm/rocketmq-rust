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
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

const EXPORT_POP_RECORD_TIMEOUT_MILLIS: u64 = 30000;

#[derive(Debug, Clone, Parser)]
pub struct ExportPopRecordSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = false,
        conflicts_with = "broker_addr",
        help = "choose one cluster to export"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerAddr",
        required = false,
        conflicts_with = "cluster_name",
        help = "choose one broker to export"
    )]
    broker_addr: Option<String>,

    #[arg(
        short = 'd',
        long = "dryRun",
        required = false,
        default_value = "false",
        help = "no actual changes will be made"
    )]
    dry_run: bool,
}

impl ExportPopRecordSubCommand {
    async fn export(admin_ext: &DefaultMQAdminExt, broker_addr: &str, broker_name: &str, dry_run: bool) {
        if !dry_run {
            match admin_ext
                .export_pop_records(CheetahString::from(broker_addr), EXPORT_POP_RECORD_TIMEOUT_MILLIS)
                .await
            {
                Ok(()) => {
                    println!(
                        "Export broker records, brokerName={}, brokerAddr={}, dryRun={}",
                        broker_name, broker_addr, dry_run
                    );
                }
                Err(e) => {
                    eprintln!(
                        "Export broker records error, brokerName={}, brokerAddr={}, dryRun={}\n{}",
                        broker_name, broker_addr, dry_run, e
                    );
                }
            }
        } else {
            println!(
                "Export broker records, brokerName={}, brokerAddr={}, dryRun={}",
                broker_name, broker_addr, dry_run
            );
        }
    }
}

impl CommandExecute for ExportPopRecordSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.broker_addr.is_none() && self.cluster_name.is_none() {
            return Err(RocketMQError::IllegalArgument(
                "ExportPopRecordSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ));
        }

        let mut admin_ext = if let Some(rpc_hook) = rpc_hook {
            DefaultMQAdminExt::with_rpc_hook(rpc_hook)
        } else {
            DefaultMQAdminExt::new()
        };
        admin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        if let Some(addr) = &self.common_args.namesrv_addr {
            admin_ext.set_namesrv_addr(addr.trim());
        }

        admin_ext.start().await.map_err(|e| {
            RocketMQError::Internal(format!("ExportPopRecordSubCommand: Failed to start MQAdminExt: {}", e))
        })?;

        let result = if let Some(broker_addr) = &self.broker_addr {
            let broker_addr = broker_addr.trim();
            let broker_name = match admin_ext.get_broker_config(CheetahString::from(broker_addr)).await {
                Ok(properties) => properties
                    .get(&CheetahString::from("brokerName"))
                    .cloned()
                    .unwrap_or_else(|| CheetahString::from("")),
                Err(_) => CheetahString::from(""),
            };
            Self::export(&admin_ext, broker_addr, broker_name.as_str(), self.dry_run).await;
            Ok(())
        } else if let Some(cluster_name) = &self.cluster_name {
            let cluster_name = cluster_name.trim();
            match admin_ext.examine_broker_cluster_info().await {
                Ok(cluster_info) => {
                    if let Some(cluster_addr_table) = &cluster_info.cluster_addr_table {
                        if let Some(broker_name_set) = cluster_addr_table.get(&CheetahString::from(cluster_name)) {
                            if let Some(broker_addr_table) = &cluster_info.broker_addr_table {
                                for broker_name in broker_name_set {
                                    if let Some(broker_data) = broker_addr_table.get(broker_name) {
                                        for broker_addr in broker_data.broker_addrs().values() {
                                            Self::export(
                                                &admin_ext,
                                                broker_addr.as_str(),
                                                broker_name.as_str(),
                                                self.dry_run,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Err(RocketMQError::IllegalArgument(
                "ExportPopRecordSubCommand: Either brokerAddr (-b) or clusterName (-c) must be provided".into(),
            ))
        };

        admin_ext.shutdown().await;
        result
    }
}
