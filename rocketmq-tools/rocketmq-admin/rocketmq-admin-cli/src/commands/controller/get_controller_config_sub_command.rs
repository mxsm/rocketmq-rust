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
use rocketmq_admin_core::core::controller::ControllerConfigQueryRequest;
use rocketmq_admin_core::core::controller::ControllerConfigQueryResult;
use rocketmq_admin_core::core::controller::ControllerService;

#[derive(Debug, Clone, Parser)]
pub struct GetControllerConfigSubCommand {
    #[arg(
        short = 'a',
        long = "controllerAddress",
        value_name = "HOST:PORT[;HOST:PORT...]",
        required = true,
        help = "Controller address list, eg: '192.168.0.1:9878;192.168.0.2:9878'"
    )]
    controller_address: String,
}

impl CommandExecute for GetControllerConfigSubCommand {
    async fn execute(&self, rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result =
            ControllerService::query_controller_config_by_request_with_rpc_hook(self.request()?, rpc_hook).await?;
        Self::print_result(&result);
        Ok(())
    }
}

impl GetControllerConfigSubCommand {
    fn request(&self) -> RocketMQResult<ControllerConfigQueryRequest> {
        ControllerConfigQueryRequest::try_new(self.controller_address.clone())
    }

    fn print_result(result: &ControllerConfigQueryResult) {
        for (controller_addr, config) in &result.controller_configs {
            println!("============{}============", controller_addr);
            for (key, value) in config {
                println!("{:<50}=  {}", key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_controller_config_sub_command_parse_request() {
        let cmd = GetControllerConfigSubCommand::try_parse_from([
            "getControllerConfig",
            "-a",
            " 127.0.0.1:9878 ;127.0.0.2:9878 ",
        ])
        .unwrap();

        assert_eq!(
            cmd.request()
                .unwrap()
                .controller_servers()
                .iter()
                .map(|addr| addr.as_str())
                .collect::<Vec<_>>(),
            vec!["127.0.0.1:9878", "127.0.0.2:9878"]
        );
    }
}
