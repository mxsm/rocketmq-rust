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
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::namesrv::NameServerService;
use rocketmq_admin_core::core::namesrv::WritePermRequest;
use rocketmq_admin_core::core::namesrv::WritePermResult;

#[derive(Debug, Clone, Parser)]
pub struct WipeWritePermSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'b', long = "brokerName", required = true, help = "broker name")]
    broker_name: String,
}

impl WipeWritePermSubCommand {
    fn request(&self) -> RocketMQResult<WritePermRequest> {
        Ok(WritePermRequest::try_new(self.broker_name.clone())?
            .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()))
    }

    fn print_result(result: WritePermResult) {
        for entry in result.entries {
            if let Some(affected_count) = entry.affected_count {
                println!(
                    "wipe write perm of broker[{}] in name server[{}] OK, {}",
                    result.broker_name, entry.namesrv_addr, affected_count
                );
            } else {
                println!(
                    "wipe write perm of broker[{}] in name server[{}] Failed",
                    result.broker_name, entry.namesrv_addr
                );
                if let Some(error) = entry.error {
                    println!("{error}");
                }
            }
        }
    }
}

impl CommandExecute for WipeWritePermSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let result = NameServerService::wipe_write_perm_by_request(self.request()?).await?;
        Self::print_result(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wipe_write_perm_sub_command_parse() {
        let cmd = WipeWritePermSubCommand::try_parse_from(["wipeWritePerm", "-b", "broker-a", "-n", "127.0.0.1:9876"])
            .unwrap();

        assert_eq!(cmd.request().unwrap().broker_name().as_str(), "broker-a");
    }
}
