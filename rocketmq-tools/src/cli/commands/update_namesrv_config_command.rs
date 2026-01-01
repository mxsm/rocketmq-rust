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

//! Update NameServer configuration command

use clap::Args;

use crate::cli::validators;
use crate::core::admin::AdminBuilder;
use crate::core::namesrv::NameServerService;
use crate::core::RocketMQResult;

/// Update NameServer configuration
#[derive(Debug, Args, Clone)]
pub struct UpdateNamesrvConfigCommand {
    /// Configuration key to update
    #[arg(short = 'k', long = "key")]
    pub key: String,

    /// Configuration value to set
    #[arg(short = 'v', long = "value")]
    pub value: String,

    /// NameServer address
    #[arg(short = 'n', long = "namesrvAddr", value_parser = validators::validate_namesrv_addr)]
    pub namesrv_addr: String,
}

impl UpdateNamesrvConfigCommand {
    /// Execute the command
    pub async fn execute(&self) -> RocketMQResult<()> {
        // Validate inputs
        self.validate()?;

        // Create admin client with RAII guard
        let mut admin = AdminBuilder::new()
            .namesrv_addr(&self.namesrv_addr)
            .build_with_guard()
            .await?;

        // Build properties map
        use std::collections::HashMap;

        use cheetah_string::CheetahString;
        let mut properties = HashMap::new();
        properties.insert(
            CheetahString::from(self.key.clone()),
            CheetahString::from(self.value.clone()),
        );

        // Parse nameserver addresses (optional for update)
        let addrs: Vec<CheetahString> = self
            .namesrv_addr
            .split(';')
            .map(|s| CheetahString::from(s.trim()))
            .collect();

        // Execute core operation
        NameServerService::update_namesrv_config(&mut admin, properties, Some(addrs)).await?;

        // Success message
        self.print_success();

        Ok(())
    }

    /// Validate command parameters
    fn validate(&self) -> RocketMQResult<()> {
        // Validate key
        if self.key.trim().is_empty() {
            return Err(crate::core::ToolsError::validation_error("key", "Configuration key cannot be empty").into());
        }

        // Validate value
        if self.value.trim().is_empty() {
            return Err(
                crate::core::ToolsError::validation_error("value", "Configuration value cannot be empty").into(),
            );
        }

        Ok(())
    }

    /// Print success message
    fn print_success(&self) {
        println!("NameServer configuration updated successfully");
        println!("\nConfiguration:");
        println!("  Address: {}", self.namesrv_addr);
        println!("  Key:     {}", self.key);
        println!("  Value:   {}", self.value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_update_namesrv_config() {
        let cmd = UpdateNamesrvConfigCommand {
            key: "testKey".to_string(),
            value: "testValue".to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert_eq!(cmd.key, "testKey");
        assert_eq!(cmd.value, "testValue");
        assert_eq!(cmd.namesrv_addr, "127.0.0.1:9876");
    }

    #[test]
    fn test_validate_empty_key() {
        let cmd = UpdateNamesrvConfigCommand {
            key: "".to_string(),
            value: "testValue".to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert!(cmd.validate().is_err());
    }

    #[test]
    fn test_validate_empty_value() {
        let cmd = UpdateNamesrvConfigCommand {
            key: "testKey".to_string(),
            value: "".to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert!(cmd.validate().is_err());
    }

    #[test]
    fn test_validate_success() {
        let cmd = UpdateNamesrvConfigCommand {
            key: "testKey".to_string(),
            value: "testValue".to_string(),
            namesrv_addr: "127.0.0.1:9876".to_string(),
        };

        assert!(cmd.validate().is_ok());
    }
}
