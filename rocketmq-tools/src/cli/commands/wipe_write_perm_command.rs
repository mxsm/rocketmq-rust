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

/// Wipe write permission for broker
#[derive(Debug, Clone, Parser)]
pub struct WipeWritePermCommand {
    /// Broker name
    #[arg(short = 'b', long = "broker-name", required = true)]
    pub broker_name: String,

    #[command(flatten)]
    pub common: CommonArgs,
}

impl CommandExecute for WipeWritePermCommand {
    async fn execute(&self, _rpc_hook: Option<std::sync::Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        // 1. Confirm dangerous operation (unless --yes flag is set)
        if !self.common.skip_confirm {
            let target = format!("write permission for broker '{}'", self.broker_name);
            if !prompt::confirm_dangerous_operation("wipe", &target) {
                output::print_warning("Operation cancelled by user");
                return Ok(());
            }
        }

        // 2. Build admin client
        output::print_operation_start(&format!("Wiping write permission for broker '{}'", self.broker_name));
        let spinner = progress::create_spinner("Connecting to NameServer...");

        let mut builder = AdminBuilder::new();
        if let Some(addr) = &self.common.namesrv_addr {
            builder = builder.namesrv_addr(addr.trim());
        }
        let mut admin = builder.build_with_guard().await?;
        spinner.finish_and_clear();

        // 3. Wipe write permission
        let spinner = progress::create_spinner("Wiping write permission...");
        let namesrv_addr = self.common.namesrv_addr.clone().unwrap_or_default();

        match NameServerService::wipe_write_perm_of_broker(
            &mut admin,
            CheetahString::from(namesrv_addr),
            CheetahString::from(self.broker_name.clone()),
        )
        .await
        {
            Ok(affected_count) => {
                progress::finish_progress_success(&spinner, "Permission wiped");
                output::print_success(&format!(
                    "Successfully wiped write permission for broker '{}'",
                    self.broker_name
                ));
                output::print_info(&format!("Affected brokers: {}", affected_count));
                Ok(())
            }
            Err(e) => {
                progress::finish_progress_error(&spinner, "Operation failed");
                output::print_error(&format!(
                    "Failed to wipe write permission for broker '{}': {}",
                    self.broker_name, e
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
    fn test_wipe_write_perm_command() {
        let cmd = WipeWritePermCommand::try_parse_from(["wipe_write_perm", "-b", "broker-a", "-n", "127.0.0.1:9876"])
            .unwrap();

        assert_eq!(cmd.broker_name, "broker-a");
        assert!(!cmd.common.skip_confirm);
    }

    #[test]
    fn test_command_with_yes_flag() {
        let cmd =
            WipeWritePermCommand::try_parse_from(["wipe_write_perm", "-b", "broker-a", "-n", "127.0.0.1:9876", "-y"])
                .unwrap();

        assert_eq!(cmd.broker_name, "broker-a");
        assert!(cmd.common.skip_confirm);
    }

    #[test]
    fn test_command_requires_broker_name() {
        let cmd = WipeWritePermCommand::try_parse_from(["wipe_write_perm", "-n", "127.0.0.1:9876"]);
        assert!(cmd.is_err());
    }

    #[test]
    fn test_command_with_different_broker_names() {
        let test_cases = vec!["broker-a", "broker-master", "my-broker-01"];

        for broker_name in test_cases {
            let cmd =
                WipeWritePermCommand::try_parse_from(["wipe_write_perm", "-b", broker_name, "-n", "127.0.0.1:9876"])
                    .unwrap();

            assert_eq!(cmd.broker_name, broker_name);
        }
    }

    #[test]
    fn test_command_with_long_flags() {
        let cmd =
            WipeWritePermCommand::try_parse_from(["wipe_write_perm", "--broker-name", "test-broker", "--yes"]).unwrap();

        assert_eq!(cmd.broker_name, "test-broker");
        assert!(cmd.common.skip_confirm);
    }
}
