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

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_remoting::runtime::RPCHook;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use crate::core::admin::AdminBuilder;
use crate::core::namesrv::NameServerService;
use crate::core::RocketMQResult;

/// Add write permission for broker
#[derive(Debug, Clone, Parser)]
pub struct AddWritePermCommand {
    /// Broker name
    #[arg(short = 'b', long = "broker-name", required = true)]
    pub broker_name: String,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for AddWritePermCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        let namesrv_addr = self.common.namesrv_addr.clone().unwrap_or_default();
        let affected_count = NameServerService::add_write_perm_of_broker(
            &mut admin,
            CheetahString::from(namesrv_addr),
            CheetahString::from(self.broker_name.clone()),
        )
        .await?;

        println!(
            "Successfully added write permission for broker '{}' (affected: {} brokers)",
            self.broker_name, affected_count
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_write_perm_command() {
        let cmd =
            AddWritePermCommand::try_parse_from(["add_write_perm", "-b", "broker-a", "-n", "127.0.0.1:9876"]).unwrap();

        assert_eq!(cmd.broker_name, "broker-a");
    }
}
