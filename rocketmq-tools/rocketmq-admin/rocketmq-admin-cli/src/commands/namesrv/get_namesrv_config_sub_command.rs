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

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use tabled::Table;
use tabled::Tabled;
use tabled::settings::Alignment;
use tabled::settings::Modify;
use tabled::settings::Style;
use tabled::settings::object::Rows;

use crate::commands::CommandExecute;
use crate::commands::CommonArgs;
use rocketmq_admin_core::core::namesrv::NameServerService;
use rocketmq_admin_core::core::namesrv::NamesrvConfigQueryRequest;

#[derive(Debug, Clone, Parser)]
pub struct GetNamesrvConfigSubCommand {
    #[command(flatten)]
    common: CommonArgs,
}

impl GetNamesrvConfigSubCommand {
    fn request(&self) -> RocketMQResult<NamesrvConfigQueryRequest> {
        NamesrvConfigQueryRequest::try_new(self.common.namesrv_addr.clone())
    }
}

impl CommandExecute for GetNamesrvConfigSubCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        let request = match self.request() {
            Ok(request) => request,
            Err(_) => {
                eprintln!("Please set the namesrvAddr parameter");
                return Ok(());
            }
        };
        if self.common.namesrv_addr.is_none() {
            eprintln!("Please set the namesrvAddr parameter");
            return Ok(());
        }

        let result = NameServerService::query_namesrv_config(request).await?;
        display_configs_with_table(&result.configs);
        Ok(())
    }
}

/// Configuration entry for table display
#[derive(Debug, Clone, Tabled)]
struct ConfigEntry {
    #[tabled(rename = "Configuration Key")]
    key: CheetahString,
    #[tabled(rename = "Value")]
    value: CheetahString,
}

/// Display configurations using tabled for formatted output
fn display_configs_with_table(configs: &HashMap<CheetahString, HashMap<CheetahString, CheetahString>>) {
    for (server_addr, properties) in configs {
        println!("============Name server: {server_addr}============",);

        // Convert properties to ConfigEntry vector
        let mut config_entries: Vec<ConfigEntry> = properties
            .iter()
            .map(|(key, value)| ConfigEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();

        // Sort by key for consistent output
        config_entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Create and display table
        if !config_entries.is_empty() {
            let table = Table::new(&config_entries)
                .with(Style::modern())
                .with(Modify::new(Rows::new(1..)).with(Alignment::left()))
                .to_string();

            println!("{table}");
        } else {
            println!("No configuration found for this server.");
        }
        println!(); // Add blank line between servers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_namesrv_config_sub_command_parse() {
        let cmd =
            GetNamesrvConfigSubCommand::try_parse_from(["getNamesrvConfig", "-n", "127.0.0.1:9876;127.0.0.2:9876"])
                .unwrap();

        assert_eq!(
            cmd.request().unwrap().namesrv_addrs(),
            vec![
                CheetahString::from("127.0.0.1:9876"),
                CheetahString::from("127.0.0.2:9876")
            ]
        );
    }
}
