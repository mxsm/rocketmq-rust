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

//! Get NameServer configuration command

use clap::Args;
use serde_json;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

use crate::cli::validators;
use crate::core::admin::AdminBuilder;
use crate::core::namesrv::NameServerService;
use crate::core::RocketMQResult;

/// Get NameServer configuration
#[derive(Debug, Args, Clone)]
pub struct GetNamesrvConfigCommand {
    /// NameServer address
    #[arg(short = 'n', long = "namesrvAddr", value_parser = validators::validate_namesrv_addr)]
    pub namesrv_addr: String,

    /// Output format: table, json, yaml
    #[arg(short = 'f', long = "format", default_value = "table")]
    pub format: String,
}

impl GetNamesrvConfigCommand {
    /// Execute the command
    pub async fn execute(&self) -> RocketMQResult<()> {
        // Validate inputs
        self.validate()?;

        // Create admin client with RAII guard
        let mut admin = AdminBuilder::new()
            .namesrv_addr(&self.namesrv_addr)
            .build_with_guard()
            .await?;

        // Parse nameserver addresses
        use cheetah_string::CheetahString;
        let addrs: Vec<CheetahString> = self
            .namesrv_addr
            .split(';')
            .map(|s| CheetahString::from(s.trim()))
            .collect();

        // Execute core operation
        let config_map = NameServerService::get_namesrv_config(&mut admin, addrs).await?;

        // Flatten the map for display (assuming only one nameserver or merging all configs)
        let config: std::collections::HashMap<String, String> = config_map
            .into_iter()
            .flat_map(|(_, configs)| configs.into_iter().map(|(k, v)| (k.to_string(), v.to_string())))
            .collect();

        // Format and display output
        self.print_config(&config)?;

        Ok(())
    }

    /// Validate command parameters
    fn validate(&self) -> RocketMQResult<()> {
        // Validate format
        if !["table", "json", "yaml"].contains(&self.format.as_str()) {
            return Err(crate::core::ToolsError::validation_error(
                "format",
                "Output format must be 'table', 'json', or 'yaml'",
            )
            .into());
        }

        Ok(())
    }

    /// Print configuration based on format
    fn print_config(&self, config: &std::collections::HashMap<String, String>) -> RocketMQResult<()> {
        match self.format.as_str() {
            "json" => {
                let json = serde_json::to_string_pretty(&config)
                    .map_err(|e| crate::core::ToolsError::internal(format!("Failed to serialize to JSON: {e}")))?;
                println!("{json}");
            }
            "yaml" => {
                let yaml = serde_yaml::to_string(&config)
                    .map_err(|e| crate::core::ToolsError::internal(format!("Failed to serialize to YAML: {e}")))?;
                println!("{yaml}");
            }
            "table" => {
                self.print_table(config);
            }
            _ => {
                return Err(crate::core::ToolsError::validation_error("format", "Invalid format specified").into());
            }
        }

        Ok(())
    }

    /// Print configuration as a table
    fn print_table(&self, config: &std::collections::HashMap<String, String>) {
        println!("\nNameServer Configuration");
        println!("Address: {}", self.namesrv_addr);
        println!();

        // Convert to table format
        let rows: Vec<ConfigEntry> = config
            .iter()
            .map(|(key, value)| ConfigEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();

        if rows.is_empty() {
            println!("No configuration entries found");
            return;
        }

        let mut table = Table::new(&rows);
        table.with(Style::rounded());

        println!("{table}");
        println!("\nTotal: {} configuration entries", rows.len());
    }
}

#[derive(Tabled)]
struct ConfigEntry {
    #[tabled(rename = "Key")]
    key: String,
    #[tabled(rename = "Value")]
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get_namesrv_config() {
        let cmd = GetNamesrvConfigCommand {
            namesrv_addr: "127.0.0.1:9876".to_string(),
            format: "table".to_string(),
        };

        assert_eq!(cmd.namesrv_addr, "127.0.0.1:9876");
        assert_eq!(cmd.format, "table");
    }

    #[test]
    fn test_validate_format() {
        let cmd = GetNamesrvConfigCommand {
            namesrv_addr: "127.0.0.1:9876".to_string(),
            format: "invalid".to_string(),
        };

        assert!(cmd.validate().is_err());
    }

    #[test]
    fn test_validate_valid_formats() {
        for format in ["table", "json", "yaml"] {
            let cmd = GetNamesrvConfigCommand {
                namesrv_addr: "127.0.0.1:9876".to_string(),
                format: format.to_string(),
            };

            assert!(cmd.validate().is_ok());
        }
    }
}
