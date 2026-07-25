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

use clap::Parser;
use rocketmq_error::RocketMQResult;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::client_adapter::services::namesrv::KvConfigUpdateRequest;
use rocketmq_admin_core::client_adapter::services::namesrv::NameServerService;

#[derive(Debug, Clone, Parser)]
pub struct UpdateKvConfigSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 's', long = "namespace", required = true, help = "set the namespace")]
    namespace: String,

    #[arg(short = 'k', long = "key", required = true, help = "set the key name")]
    key: String,

    #[arg(short = 'v', long = "value", required = true, help = "set the key value")]
    value: String,
}

impl UpdateKvConfigSubCommand {
    fn request(&self) -> RocketMQResult<KvConfigUpdateRequest> {
        Ok(
            KvConfigUpdateRequest::try_new(self.namespace.clone(), self.key.clone(), self.value.clone())?
                .with_optional_namesrv_addr(self.common_args.namesrv_addr.clone()),
        )
    }
}

impl CommandExecute for UpdateKvConfigSubCommand {
    async fn execute(
        &self,
        _credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        _client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
        NameServerService::update_kv_config_by_request(self.request()?).await?;
        println!("update kv config in namespace success.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_kv_config_sub_command_parse() {
        let cmd = UpdateKvConfigSubCommand::try_parse_from([
            "updateKvConfig",
            "-s",
            "namespace",
            "-k",
            "key",
            "-v",
            "value",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.request().unwrap().namespace().as_str(), "namespace");
    }
}
