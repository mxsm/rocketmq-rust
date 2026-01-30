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
use crate::ui::output;
use crate::ui::progress;
use crate::ui::prompt;

/// Delete KV configuration from NameServer
#[derive(Debug, Clone, Parser)]
pub struct DeleteKvConfigCommand {
    /// Configuration namespace
    #[arg(short = 's', long = "namespace", required = true)]
    pub namespace: String,

    /// Configuration key
    #[arg(short = 'k', long = "key", required = true)]
    pub key: String,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for DeleteKvConfigCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // 1. Confirm dangerous operation (unless --yes flag is set)
        if !self.common.skip_confirm {
            let target = format!("KV config (namespace='{}', key='{}')", self.namespace, self.key);
            if !prompt::confirm_dangerous_operation("delete", &target) {
                output::print_warning("Operation cancelled by user");
                return Ok(());
            }
        }

        // 2. Build admin client
        output::print_operation_start(&format!("Deleting KV config: {}={}", self.namespace, self.key));
        let spinner = progress::create_spinner("Connecting to NameServer...");

        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;
        spinner.finish_and_clear();

        // 3. Delete KV config
        let spinner = progress::create_spinner("Deleting configuration...");
        match NameServerService::delete_kv_config(
            &mut admin,
            CheetahString::from(self.namespace.clone()),
            CheetahString::from(self.key.clone()),
        )
        .await
        {
            Ok(_) => {
                progress::finish_progress_success(&spinner, "Configuration deleted");
                output::print_success("KV configuration deleted successfully");
                output::print_key_value("Namespace", &self.namespace);
                output::print_key_value("Key", &self.key);
                Ok(())
            }
            Err(e) => {
                progress::finish_progress_error(&spinner, "Deletion failed");
                output::print_error(&format!(
                    "Failed to delete KV config (namespace='{}', key='{}'): {}",
                    self.namespace, self.key, e
                ));
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delete_kv_config_command() {
        let cmd = DeleteKvConfigCommand::try_parse_from([
            "delete_kv_config",
            "-s",
            "ORDER_TOPIC",
            "-k",
            "TestTopic",
            "-n",
            "127.0.0.1:9876",
        ])
        .unwrap();

        assert_eq!(cmd.namespace, "ORDER_TOPIC");
        assert_eq!(cmd.key, "TestTopic");
        assert!(!cmd.common.skip_confirm);
    }

    #[test]
    fn test_command_with_yes_flag() {
        let cmd =
            DeleteKvConfigCommand::try_parse_from(["delete_kv_config", "-s", "ORDER_TOPIC", "-k", "TestTopic", "-y"])
                .unwrap();

        assert_eq!(cmd.namespace, "ORDER_TOPIC");
        assert_eq!(cmd.key, "TestTopic");
        assert!(cmd.common.skip_confirm);
    }

    #[test]
    fn test_command_requires_namespace() {
        let cmd = DeleteKvConfigCommand::try_parse_from(["delete_kv_config", "-k", "TestTopic"]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_command_requires_key() {
        let cmd = DeleteKvConfigCommand::try_parse_from(["delete_kv_config", "-s", "ORDER_TOPIC"]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_command_with_long_flags() {
        let cmd = DeleteKvConfigCommand::try_parse_from([
            "delete_kv_config",
            "--namespace",
            "TEST_NS",
            "--key",
            "test_key",
            "--yes",
        ])
        .unwrap();

        assert_eq!(cmd.namespace, "TEST_NS");
        assert_eq!(cmd.key, "test_key");
        assert!(cmd.common.skip_confirm);
    }

    #[test]
    fn test_command_with_various_namespaces() {
        let namespaces = vec!["ORDER_TOPIC", "RETRY_TOPIC", "DLQ_TOPIC"];

        for ns in namespaces {
            let cmd = DeleteKvConfigCommand::try_parse_from(["delete_kv_config", "-s", ns, "-k", "test_key"]).unwrap();

            assert_eq!(cmd.namespace, ns);
        }
    }
}
