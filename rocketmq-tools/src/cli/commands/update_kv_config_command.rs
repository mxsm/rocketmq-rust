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

/// Update KV configuration in NameServer
#[derive(Debug, Clone, Parser)]
pub struct UpdateKvConfigCommand {
    /// Configuration namespace
    #[arg(short = 's', long = "namespace", required = true)]
    pub namespace: String,

    /// Configuration key
    #[arg(short = 'k', long = "key", required = true)]
    pub key: String,

    /// Configuration value
    #[arg(short = 'v', long = "value", required = true)]
    pub value: String,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for UpdateKvConfigCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;

        NameServerService::create_or_update_kv_config(
            &mut admin,
            CheetahString::from(self.namespace.clone()),
            CheetahString::from(self.key.clone()),
            CheetahString::from(self.value.clone()),
        )
        .await?;

        println!(
            "Successfully updated KV config: namespace={}, key={}, value={}",
            self.namespace, self.key, self.value
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_kv_config_command() {
        let cmd = UpdateKvConfigCommand::try_parse_from([
            "update_kv_config",
            "-s",
            "ORDER_TOPIC",
            "-k",
            "TestTopic",
            "-v",
            "broker-a,broker-b",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.namespace, "ORDER_TOPIC");
        assert_eq!(cmd.key, "TestTopic");
        assert_eq!(cmd.value, "broker-a,broker-b");
    }
}
