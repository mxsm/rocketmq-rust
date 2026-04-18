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
use rocketmq_admin_core::core::controller::ControllerMetadataCleanRequest;
use rocketmq_admin_core::core::controller::ControllerService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;

#[derive(Debug, Clone, Parser)]
pub struct CleanBrokerMetadataSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        required = true,
        help = "The address of controller"
    )]
    controller_address: String,

    #[arg(
        short = 'b',
        long = "brokerControllerIdsToClean",
        required = false,
        help = "The brokerController id list which requires to clean metadata. eg: 1;2;3, means that clean broker-1, \
                broker-2 and broker-3"
    )]
    broker_controller_ids_to_clean: Option<String>,

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
        required = false,
        help = "The clusterName of broker"
    )]
    cluster_name: Option<String>,

    #[arg(
        short = 'l',
        long = "cleanLivingBroker",
        required = false,
        help = "Whether clean up living brokers, default value is false"
    )]
    clean_living_broker: bool,
}

impl CommandExecute for CleanBrokerMetadataSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = self.request()?;
        let broker_name = request.broker_name().to_string();
        ControllerService::clean_controller_metadata_by_request_with_rpc_hook(request, rpc_hook).await?;
        println!("clear broker {} metadata from controller success!", broker_name);
        Ok(())
    }
}

impl CleanBrokerMetadataSubCommand {
    fn request(&self) -> RocketMQResult<ControllerMetadataCleanRequest> {
        ControllerMetadataCleanRequest::try_new(
            self.controller_address.clone(),
            self.broker_name.clone(),
            self.broker_controller_ids_to_clean.clone(),
            self.cluster_name.clone(),
            self.clean_living_broker,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_broker_metadata_sub_command_builds_core_request() {
        let cmd = CleanBrokerMetadataSubCommand::try_parse_from([
            "cleanBrokerMetadata",
            "-a",
            " 127.0.0.1:9878 ",
            "--brokerName",
            " broker-a ",
            "-c",
            " cluster-a ",
            "-b",
            " 1 ; ; 2 ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();
        assert_eq!(request.controller_addr().as_str(), "127.0.0.1:9878");
        assert_eq!(request.broker_name().as_str(), "broker-a");
        assert_eq!(
            request.cluster_name().map(|cluster| cluster.as_str()),
            Some("cluster-a")
        );
        assert_eq!(
            request.broker_controller_ids_to_clean().map(|ids| ids.as_str()),
            Some("1;2")
        );
        assert!(!request.clean_living_broker());
    }
}
