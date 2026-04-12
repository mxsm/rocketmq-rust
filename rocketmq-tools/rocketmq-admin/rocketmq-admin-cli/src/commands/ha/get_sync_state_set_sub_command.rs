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
use rocketmq_remoting::protocol::body::broker_replicas_info::BrokerReplicasInfo;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::ha::HaService;
use rocketmq_admin_core::core::ha::SyncStateSetQueryRequest;
use rocketmq_admin_core::core::ha::SyncStateSetQueryResult;

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
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if let Some(interval) = self.interval {
            let flush_second = if interval > 0 { interval } else { 3 };
            loop {
                let result =
                    HaService::query_sync_state_set_by_request_with_rpc_hook(self.request()?, rpc_hook.clone()).await?;
                Self::print_result(&result);
                tokio::time::sleep(tokio::time::Duration::from_secs(flush_second)).await;
            }
        } else {
            let result = HaService::query_sync_state_set_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
            Self::print_result(&result);
        }

        Ok(())
    }
}

impl GetSyncStateSetSubCommand {
    fn request(&self) -> RocketMQResult<SyncStateSetQueryRequest> {
        SyncStateSetQueryRequest::try_new(
            self.controller_address.clone(),
            self.broker_name.clone(),
            self.cluster_name.clone(),
        )
    }

    fn print_result(result: &SyncStateSetQueryResult) {
        if let Some(broker_replicas_info) = &result.broker_replicas_info {
            Self::print_data(broker_replicas_info);
        }
    }

    fn print_data(broker_replicas_info: &BrokerReplicasInfo) {
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
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_admin_core::core::ha::SyncStateSetTarget;

    use super::*;

    #[test]
    fn get_sync_state_set_sub_command_parse_broker_target() {
        let cmd = GetSyncStateSetSubCommand::try_parse_from([
            "getSyncStateSet",
            "-a",
            " 127.0.0.1:9878 ",
            "-b",
            " broker-a ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(request.controller_address().as_str(), "127.0.0.1:9878");
        assert_eq!(
            request.target(),
            &SyncStateSetTarget::BrokerName(CheetahString::from("broker-a"))
        );
    }

    #[test]
    fn get_sync_state_set_sub_command_parse_cluster_target() {
        let cmd = GetSyncStateSetSubCommand::try_parse_from([
            "getSyncStateSet",
            "-a",
            " 127.0.0.1:9878;127.0.0.2:9878 ",
            "-c",
            " DefaultCluster ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(request.controller_address().as_str(), "127.0.0.1:9878");
        assert_eq!(
            request.target(),
            &SyncStateSetTarget::ClusterName(CheetahString::from("DefaultCluster"))
        );
    }
}
