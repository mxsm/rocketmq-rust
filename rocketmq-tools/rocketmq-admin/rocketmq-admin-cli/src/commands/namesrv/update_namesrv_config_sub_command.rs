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
use rocketmq_admin_core::client_adapter::services::namesrv::NameServerService;
use rocketmq_admin_core::client_adapter::services::namesrv::NamesrvConfigUpdateRequest;
use rocketmq_admin_core::client_adapter::services::namesrv::NamesrvConfigUpdateResult;

#[derive(Debug, Clone, Parser)]
pub struct UpdateNamesrvConfigSubCommand {
    #[command(flatten)]
    common_args: CommonArgs,

    #[arg(short = 'k', long = "key", required = true, help = "config key")]
    key: String,

    #[arg(short = 'v', long = "value", required = true, help = "config value")]
    value: String,
}

impl UpdateNamesrvConfigSubCommand {
    fn request(&self) -> RocketMQResult<NamesrvConfigUpdateRequest> {
        NamesrvConfigUpdateRequest::try_new(
            self.key.clone(),
            self.value.clone(),
            self.common_args.namesrv_addr.clone(),
        )
    }

    fn print_result(result: NamesrvConfigUpdateResult) {
        let server_list = result
            .namesrv_addrs
            .unwrap_or_default()
            .iter()
            .map(|addr| addr.as_str())
            .collect::<Vec<_>>()
            .join(";");
        for (key, value) in result.properties {
            println!(
                "update name server config success!{}\n{} : {}\n",
                server_list, key, value
            );
        }
    }
}

impl CommandExecute for UpdateNamesrvConfigSubCommand {
    async fn execute(
        &self,
        _credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        _client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
        let result = NameServerService::update_namesrv_config_by_request(self.request()?).await?;
        Self::print_result(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cheetah_string::CheetahString;

    #[test]
    fn update_namesrv_config_sub_command_parse() {
        let cmd = UpdateNamesrvConfigSubCommand::try_parse_from([
            "updateNamesrvConfig",
            "-k",
            "deleteWhen",
            "-v",
            "04",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(
            cmd.request()
                .unwrap()
                .properties()
                .get(&CheetahString::from("deleteWhen"))
                .unwrap(),
            "04"
        );
    }
}
