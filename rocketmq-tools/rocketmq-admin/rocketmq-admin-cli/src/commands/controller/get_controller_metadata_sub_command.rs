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
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use rocketmq_admin_core::core::controller::ControllerMetadataQueryRequest;
use rocketmq_admin_core::core::controller::ControllerMetadataQueryResult;
use rocketmq_admin_core::core::controller::ControllerService;

#[derive(Debug, Clone, Parser)]
pub struct GetControllerMetadataSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        value_name = "HOST:PORT",
        required = true,
        help = "Address of the controller to query"
    )]
    controller_address: String,
}

impl CommandExecute for GetControllerMetadataSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            ControllerService::query_controller_metadata_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

impl GetControllerMetadataSubCommand {
    fn request(&self) -> RocketMQResult<ControllerMetadataQueryRequest> {
        ControllerMetadataQueryRequest::try_new(self.controller_address.clone())
    }

    fn print_result(result: &ControllerMetadataQueryResult) {
        let meta_data = &result.meta_data;

        let group = meta_data
            .group
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or(String::from("<NONE>"));
        println!("ControllerGroup\t{}", group);

        let controller_leader_id = meta_data
            .controller_leader_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or(String::from("<NONE>"));
        println!("ControllerLeaderId\t{}", controller_leader_id);

        let controller_leader_address = meta_data
            .controller_leader_address
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or(String::from("<NONE>"));
        println!("ControllerLeaderAddress\t{}", controller_leader_address);

        let is_leader = meta_data.is_leader.unwrap_or(false).to_string();
        println!("IsLeader\t{}", is_leader);

        if let Some(peers) = &meta_data.peers {
            peers.split(";").for_each(|peer| println!("#Peer:\t{}", peer));
        } else {
            println!("No peers found");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_controller_metadata_sub_command_parse_request() {
        let cmd = GetControllerMetadataSubCommand::try_parse_from(["getControllerMetadata", "-a", " 127.0.0.1:9878 "])
            .unwrap();

        assert_eq!(cmd.request().unwrap().controller_addr().as_str(), "127.0.0.1:9878");
    }
}
