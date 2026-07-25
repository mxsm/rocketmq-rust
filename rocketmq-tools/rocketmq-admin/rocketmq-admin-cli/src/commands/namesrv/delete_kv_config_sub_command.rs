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
use rocketmq_admin_core::client_adapter::services::namesrv::KvConfigDeleteRequest;
use rocketmq_admin_core::client_adapter::services::namesrv::NameServerService;

#[derive(Debug, Clone, Parser)]
pub struct DeleteKvConfigSubCommand {
    #[arg(short = 's', long = "namespace", required = true)]
    namespace: String,

    #[arg(short = 'k', long = "key", required = true)]
    key: String,
}

impl DeleteKvConfigSubCommand {
    fn request(&self) -> RocketMQResult<KvConfigDeleteRequest> {
        KvConfigDeleteRequest::try_new(self.namespace.clone(), self.key.clone())
    }
}

impl CommandExecute for DeleteKvConfigSubCommand {
    async fn execute(
        &self,
        _credentials: Option<rocketmq_admin_core::core::security::AdminCredentials>,
        _client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
    ) -> RocketMQResult<()> {
        NameServerService::delete_kv_config_by_request(self.request()?).await?;
        println!("delete kv config from namespace success.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_kv_config_sub_command_parse() {
        let cmd = DeleteKvConfigSubCommand::try_parse_from(["deleteKvConfig", "-s", "namespace", "-k", "key"]).unwrap();

        assert_eq!(cmd.request().unwrap().namespace().as_str(), "namespace");
    }
}
