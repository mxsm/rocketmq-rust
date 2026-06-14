// Copyright 2026 The RocketMQ Rust Authors
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
use rocketmq_admin_core::core::controller::ControllerElectMasterRequest;
use rocketmq_admin_core::core::controller::ControllerElectMasterResult;
use rocketmq_admin_core::core::controller::ControllerService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct ElectMasterSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'a',
        long = "controllerAddress",
        required = true,
        help = "The address of controller"
    )]
    controller_address: String,

    #[arg(
        short = 'b',
        long = "brokerId",
        required = true,
        allow_hyphen_values = true,
        help = "The id of the broker which requires to become master"
    )]
    broker_id: i64,

    #[arg(
        long = "brokerName",
        visible_alias = "bn",
        required = true,
        help = "The broker name of the replicas that require to be manipulated"
    )]
    broker_name: String,

    #[arg(
        short = 'c',
        long = "clusterName",
        required = true,
        help = "The clusterName of broker"
    )]
    cluster_name: String,
}

impl CommandExecute for ElectMasterSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ControllerService::elect_master_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

impl ElectMasterSubCommand {
    fn request(&self) -> RocketMQResult<ControllerElectMasterRequest> {
        ControllerElectMasterRequest::try_new(
            self.controller_address.clone(),
            self.cluster_name.clone(),
            self.broker_name.clone(),
            self.broker_id,
        )
        .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(result: &ControllerElectMasterResult) {
        let meta_data = &result.response_header;
        let broker_member_group = &result.broker_member_group;

        println!("\n#ClusterName\t{}", broker_member_group.cluster);
        println!("#BrokerName\t{}", broker_member_group.broker_name);
        println!(
            "#BrokerMasterAddr\t{}",
            meta_data
                .master_address
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default()
        );
        println!("#MasterEpoch\t{}", meta_data.master_epoch.unwrap_or_default());
        println!(
            "#SyncStateSetEpoch\t{}",
            meta_data.sync_state_set_epoch.unwrap_or_default()
        );

        for (broker_id, broker_addr) in &broker_member_group.broker_addrs {
            println!("\t#Broker\t{}\t{}", broker_id, broker_addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elect_master_sub_command_builds_core_request() {
        let cmd = ElectMasterSubCommand::try_parse_from([
            "electMaster",
            "-a",
            " 127.0.0.1:9878 ",
            "-b",
            "1",
            "--brokerName",
            " broker-a ",
            "-c",
            " DefaultCluster ",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.controller_addr().as_str(), "127.0.0.1:9878");
        assert_eq!(request.broker_id(), 1);
        assert_eq!(request.broker_name().as_str(), "broker-a");
        assert_eq!(request.cluster_name().as_str(), "DefaultCluster");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }

    #[test]
    fn elect_master_sub_command_rejects_negative_broker_id() {
        let cmd = ElectMasterSubCommand::try_parse_from([
            "electMaster",
            "-a",
            "127.0.0.1:9878",
            "-b",
            "-1",
            "--brokerName",
            "broker-a",
            "-c",
            "DefaultCluster",
        ])
        .unwrap();

        assert!(cmd.request().is_err());
    }
}
