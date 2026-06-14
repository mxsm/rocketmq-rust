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
use rocketmq_admin_core::core::container::ContainerAddBrokerRequest;
use rocketmq_admin_core::core::container::ContainerService;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct AddBrokerSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(
        short = 'c',
        long = "brokerContainerAddr",
        required = true,
        help = "Broker container address"
    )]
    broker_container_addr: String,

    #[arg(short = 'b', long = "brokerConfigPath", required = true, help = "Broker config path")]
    broker_config_path: String,
}

impl CommandExecute for AddBrokerSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = ContainerService::add_broker_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        println!("add broker to {} success", result.broker_container_addr);
        Ok(())
    }
}

impl AddBrokerSubCommand {
    fn request(&self) -> RocketMQResult<ContainerAddBrokerRequest> {
        ContainerAddBrokerRequest::try_new(self.broker_container_addr.clone(), self.broker_config_path.clone())
            .map(|request| request.with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_broker_sub_command_builds_core_request() {
        let cmd = AddBrokerSubCommand::try_parse_from([
            "addBroker",
            "-c",
            " 127.0.0.1:10911 ",
            "-b",
            " /tmp/broker.conf ",
            "-n",
            " 127.0.0.1:9876 ",
        ])
        .unwrap();

        let request = cmd.request().unwrap();

        assert_eq!(request.broker_container_addr().as_str(), "127.0.0.1:10911");
        assert_eq!(request.broker_config_path().as_str(), "/tmp/broker.conf");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
