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
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::command_util::CommandUtil;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;

#[derive(Debug, Clone, Parser)]
pub struct GetSyncStateSetSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        value_name = "HOST:PORT",
        required = true,
        help = "the address of controller"
    )]
    controller_address: String,

    #[arg(
        short = 'c',
        long = "clusterName",
        value_name = "CLUSTER",
        required = false,
        help = "which cluster"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'b',
        long = "brokerName",
        value_name = "BROKER",
        required = false,
        help = "which broker to fetch"
    )]
    broker_name: Option<String>,

    #[arg(
        short = 'i',
        long = "interval",
        value_name = "SECONDS",
        required = false,
        help = "the interval(second) of get info"
    )]
    interval: Option<u64>,
}

impl CommandExecute for GetSyncStateSetSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut default_mqadmin_ext = DefaultMQAdminExt::new();
        default_mqadmin_ext
            .client_config_mut()
            .set_instance_name(current_millis().to_string().into());

        let operation_result = async {
            MQAdminExt::start(&mut default_mqadmin_ext).await.map_err(|e| {
                RocketMQError::Internal(format!("GetSyncStateSetSubCommand: Failed to start MQAdminExt: {}", e))
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

impl GetSyncStateSetSubCommand {
    async fn inner_exec(&self, default_mqadmin_ext: &DefaultMQAdminExt) -> RocketMQResult<()> {
        let controller_address: CheetahString = self
            .controller_address
            .trim()
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .into();

        if let Some(ref broker_name) = self.broker_name {
            let brokers = vec![CheetahString::from(broker_name.trim())];
            Self::print_data(&controller_address, brokers, default_mqadmin_ext).await?;
        } else if let Some(ref cluster_name) = self.cluster_name {
            let cluster_info = default_mqadmin_ext.examine_broker_cluster_info().await.map_err(|e| {
                RocketMQError::Internal(format!("GetSyncStateSetSubCommand: Failed to get cluster info: {}", e))
            })?;
            let broker_names = CommandUtil::fetch_broker_name_by_cluster_name(&cluster_info, cluster_name.trim())?;
            let brokers: Vec<CheetahString> = broker_names.into_iter().map(CheetahString::from_string).collect();
            Self::print_data(&controller_address, brokers, default_mqadmin_ext).await?;
        } else {
            println!("Error: either -b (brokerName) or -c (clusterName) must be specified");
        }

        Ok(())
    }

    async fn print_data(
        controller_address: &CheetahString,
        brokers: Vec<CheetahString>,
        default_mqadmin_ext: &DefaultMQAdminExt,
    ) -> RocketMQResult<()> {
        if brokers.is_empty() {
            return Ok(());
        }

        let broker_replicas_info: BrokerReplicasInfo = default_mqadmin_ext
            .get_in_sync_state_data(controller_address.clone(), brokers)
            .await
            .map_err(|e| {
                RocketMQError::Internal(format!(
                    "GetSyncStateSetSubCommand: Failed to get in sync state data: {}",
                    e
                ))
            })?;

        let replicas_info_table = broker_replicas_info.get_replicas_info_table();
        for (broker_name, replicas_info) in replicas_info_table {
            let in_sync_replicas = replicas_info.get_in_sync_replicas();
            let not_in_sync_replicas = replicas_info.get_not_in_sync_replicas();

            println!(
                "\n#brokerName\t{}\n#MasterBrokerId\t{}\n#MasterAddr\t{}\n#MasterEpoch\t{}\n#SyncStateSetEpoch\t{}\n#\
                 SyncStateSetNums\t{}",
                broker_name,
                replicas_info.get_master_broker_id(),
                replicas_info.get_master_address(),
                replicas_info.get_master_epoch(),
                replicas_info.get_sync_state_set_epoch(),
                in_sync_replicas.len(),
            );

            for member in in_sync_replicas {
                println!("\nInSyncReplica:\t{}", member);
            }

            for member in not_in_sync_replicas {
                println!("\nNotInSyncReplica:\t{}", member);
            }
        }

        Ok(())
    }
}
