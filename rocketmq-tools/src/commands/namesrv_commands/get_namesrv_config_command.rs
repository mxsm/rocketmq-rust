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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::runtime::RPCHook;
use tabled::settings::object::Rows;
use tabled::settings::Alignment;
use tabled::settings::Modify;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::commands::CommandExecute;
use crate::commands::CommonArgs;

#[derive(Debug, Clone, Parser)]
pub struct GetNamesrvConfigCommand {
    #[command(flatten)]
    common: CommonArgs,
}

impl GetNamesrvConfigCommand {
    fn parse_server_list(&self) -> Option<Vec<CheetahString>> {
        self.common.namesrv_addr.as_ref().and_then(|servers| {
            if servers.trim().is_empty() {
                None
            } else {
                let server_array = servers
                    .trim()
                    .split(';')
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.trim().to_string().into())
                    .collect::<Vec<CheetahString>>();

                if server_array.is_empty() {
                    None
                } else {
                    Some(server_array)
                }
            }
        })
    }
}

impl CommandExecute for GetNamesrvConfigCommand {
    async fn execute(&self, _rpc_hook: Option<Arc<dyn RPCHook>>) -> RocketMQResult<()> {
        if self.common.namesrv_addr.is_none() {
            eprintln!("Please set the namesrvAddr parameter");
            return Ok(());
        }
        let mut admin = DefaultMQAdminExt::new();
        admin.client_config_mut().instance_name = get_current_millis().to_string().into();

        let server_list = self.parse_server_list();
        if let Some(server_list) = server_list {
            admin.start().await?;
            let configs = admin.get_name_server_config(server_list).await?;
            display_configs_with_table(&configs);
            admin.shutdown().await;
            return Ok(());
        } else {
            eprintln!("Please set the namesrvAddr parameter");
        }

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
