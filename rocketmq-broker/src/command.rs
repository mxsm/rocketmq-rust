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
use std::env;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use thiserror::Error;
use tracing::info;

/// Broker command line argument parsing errors
#[derive(Error, Debug)]
pub enum BrokerArgsError {
    #[error("Config file does not exist: {0}")]
    ConfigFileNotFound(PathBuf),

    #[error("Config file is not a file: {0}")]
    ConfigFileNotFile(PathBuf),

    #[error(
        "Invalid name server address format: {0}. Expected format: '192.168.0.1:9876' or \
         '192.168.0.1:9876;192.168.0.2:9876'"
    )]
    InvalidNamesrvAddr(String),

    #[error(
        "ROCKETMQ_HOME environment variable is not set. Please set it to match the location of the RocketMQ \
         installation"
    )]
    RocketmqHomeNotSet,

    #[error("Failed to parse address {0}: {1}")]
    AddressParseError(String, String),

    #[error("Failed to load configuration file: {0}")]
    ConfigLoadError(String),
}

/// RocketMQ Broker command line arguments
///
/// Supports the same command line options as the Java implementation:
/// - `-c, --configFile`: Broker config properties file
/// - `-p, --printConfigItem`: Print all config items and exit
/// - `-m, --printImportantConfig`: Print important config items and exit
/// - `-n, --namesrvAddr`: Name server address list (optional)
#[derive(Parser, Debug)]
#[command(
    author = "mxsm",
    version = "0.2.0",
    about = "RocketMQ Broker Server (Rust)",
    long_about = "Apache RocketMQ Broker Server implemented in Rust\n\
                  For more information: https://github.com/mxsm/rocketmq-rust"
)]
pub struct Args {
    /// Broker config properties file path
    ///
    /// If not specified, will try to load from:
    ///
    /// 1. $ROCKETMQ_HOME/conf/broker.toml (if ROCKETMQ_HOME is set)
    ///
    /// 2. Default configuration values
    #[arg(short = 'c', long = "configFile", value_name = "FILE")]
    pub config_file: Option<PathBuf>,

    /// Print all configuration items and exit
    ///
    /// Prints all broker configuration properties including:
    ///
    /// - Broker configuration
    ///
    /// - Netty server configuration
    ///
    /// - Netty client configuration
    ///
    /// - Message store configuration
    ///
    /// - Authentication configuration
    #[arg(short = 'p', long = "printConfigItem", conflicts_with = "print_important_config")]
    pub print_config_item: bool,

    /// Print important configuration items and exit
    ///
    /// Prints only the important configuration items that are most
    ///
    /// commonly used for broker setup and troubleshooting.
    #[arg(short = 'm', long = "printImportantConfig", conflicts_with = "print_config_item")]
    pub print_important_config: bool,

    /// Name server address list
    ///
    /// Format: '192.168.0.1:9876' or '192.168.0.1:9876;192.168.0.2:9876'
    ///
    /// Multiple addresses should be separated by semicolon (;)
    ///
    /// Can also be set via NAMESRV_ADDR environment variable
    #[arg(short = 'n', long = "namesrvAddr", value_name = "ADDR")]
    pub namesrv_addr: Option<String>,
}

impl Args {
    /// Validate command line arguments
    ///
    /// Performs comprehensive validation:
    /// - Check if config file exists and is a regular file
    /// - Validate name server address format
    /// - Verify ROCKETMQ_HOME environment variable if needed
    ///
    /// # Returns
    ///
    /// `Ok(())` if validation succeeds, otherwise returns `BrokerArgsError`
    pub fn validate(&self) -> Result<(), BrokerArgsError> {
        // Validate config file if provided
        if let Some(ref config_file) = self.config_file {
            if !config_file.exists() {
                return Err(BrokerArgsError::ConfigFileNotFound(config_file.clone()));
            }
            if !config_file.is_file() {
                return Err(BrokerArgsError::ConfigFileNotFile(config_file.clone()));
            }
            info!("Using configuration file: {}", config_file.display());
        }

        // Validate name server address format if provided
        if let Some(ref namesrv_addr) = self.namesrv_addr {
            self.validate_namesrv_addr(namesrv_addr)?;
        }

        Ok(())
    }

    /// Validate name server address format
    ///
    /// Supports formats:
    /// - Single address: `192.168.0.1:9876`
    /// - Multiple addresses: `192.168.0.1:9876;192.168.0.2:9876`
    ///
    /// # Arguments
    ///
    /// * `namesrv_addr` - Name server address string
    ///
    /// # Returns
    ///
    /// `Ok(())` if all addresses are valid, otherwise returns error
    fn validate_namesrv_addr(&self, namesrv_addr: &str) -> Result<(), BrokerArgsError> {
        if namesrv_addr.is_empty() {
            return Ok(());
        }

        // Split by semicolon for multiple addresses
        let addr_list: Vec<&str> = namesrv_addr.split(';').collect();

        for addr in addr_list {
            let trimmed = addr.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Try to parse as SocketAddr to validate format
            if let Err(e) = trimmed.parse::<SocketAddr>() {
                // If direct parsing fails, it might be a hostname:port format
                // Try to validate by splitting into host and port
                if !self.is_valid_host_port(trimmed) {
                    return Err(BrokerArgsError::AddressParseError(trimmed.to_string(), e.to_string()));
                }
            }
        }

        info!("Name server address validated: {}", namesrv_addr);
        Ok(())
    }

    /// Check if a string is valid host:port format
    fn is_valid_host_port(&self, addr: &str) -> bool {
        let parts: Vec<&str> = addr.split(':').collect();
        if parts.len() != 2 {
            return false;
        }

        // Validate port is a number
        parts[1].parse::<u16>().is_ok()
    }

    /// Check if should exit after printing config
    ///
    /// Returns `true` if either `-p` or `-m` flag is set,
    /// matching Java's behavior of exiting after printing configuration
    pub fn should_exit_after_print(&self) -> bool {
        self.print_config_item || self.print_important_config
    }

    /// Get effective name server address
    ///
    /// Priority:
    /// 1. Command line argument `-n`
    /// 2. Environment variable `NAMESRV_ADDR`
    /// 3. Default value `127.0.0.1:9876`
    pub fn get_namesrv_addr(&self) -> String {
        if let Some(ref addr) = self.namesrv_addr {
            return addr.clone();
        }

        if let Ok(addr) = env::var("NAMESRV_ADDR") {
            if !addr.is_empty() {
                return addr;
            }
        }

        "127.0.0.1:9876".to_string()
    }

    /// Get configuration file path
    ///
    /// Returns the config file path with fallback logic:
    /// 1. Explicit `-c` argument
    /// 2. `$ROCKETMQ_HOME/conf/broker.toml`
    /// 3. None (use default configuration)
    pub fn get_config_file(&self) -> Option<PathBuf> {
        if let Some(ref config_file) = self.config_file {
            return Some(config_file.clone());
        }

        // Try ROCKETMQ_HOME/conf/broker.toml
        if let Ok(rocketmq_home) = env::var("ROCKETMQ_HOME") {
            let config_path = PathBuf::from(&rocketmq_home).join("conf").join("broker.toml");
            if config_path.exists() && config_path.is_file() {
                info!(
                    "Using default config file from ROCKETMQ_HOME: {}",
                    config_path.display()
                );
                return Some(config_path);
            }
        }

        None
    }

    /// Set system properties from configuration
    ///
    /// Matches Java's `properties2SystemEnv` behavior:
    /// - `rmqAddressServerDomain` → `rocketmq.namesrv.domain`
    /// - `rmqAddressServerSubGroup` → `rocketmq.namesrv.domain.subgroup`
    pub fn apply_system_properties(properties: &HashMap<String, String>) {
        const DEFAULT_DOMAIN: &str = "jmenv.tbsite.net";
        const DEFAULT_SUBGROUP: &str = "nsaddr";

        if let Some(domain) = properties.get("rmqAddressServerDomain") {
            env::set_var("rocketmq.namesrv.domain", domain);
        } else {
            env::set_var("rocketmq.namesrv.domain", DEFAULT_DOMAIN);
        }

        if let Some(subgroup) = properties.get("rmqAddressServerSubGroup") {
            env::set_var("rocketmq.namesrv.domain.subgroup", subgroup);
        } else {
            env::set_var("rocketmq.namesrv.domain.subgroup", DEFAULT_SUBGROUP);
        }
    }
}

impl fmt::Display for Args {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Broker Arguments:")?;
        writeln!(
            f,
            "  Config File: {}",
            self.config_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "None".to_string())
        )?;
        writeln!(f, "  Print Config Item: {}", self.print_config_item)?;
        writeln!(f, "  Print Important Config: {}", self.print_important_config)?;
        write!(
            f,
            "  Name Server Address: {}",
            self.namesrv_addr.as_deref().unwrap_or("Default")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_namesrv_addr_single() {
        let args = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: Some("127.0.0.1:9876".to_string()),
        };

        assert!(args.validate_namesrv_addr("127.0.0.1:9876").is_ok());
    }

    #[test]
    fn test_validate_namesrv_addr_multiple() {
        let args = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: Some("192.168.0.1:9876;192.168.0.2:9876".to_string()),
        };

        assert!(args.validate_namesrv_addr("192.168.0.1:9876;192.168.0.2:9876").is_ok());
    }

    #[test]
    fn test_validate_namesrv_addr_invalid() {
        let args = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: Some("invalid".to_string()),
        };

        assert!(args.validate_namesrv_addr("invalid").is_err());
    }

    #[test]
    fn test_should_exit_after_print() {
        let args_print = Args {
            config_file: None,
            print_config_item: true,
            print_important_config: false,
            namesrv_addr: None,
        };
        assert!(args_print.should_exit_after_print());

        let args_important = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: true,
            namesrv_addr: None,
        };
        assert!(args_important.should_exit_after_print());

        let args_none = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
        };
        assert!(!args_none.should_exit_after_print());
    }

    #[test]
    fn test_get_namesrv_addr_default() {
        let args = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
        };

        // Should return default if no env var set
        assert_eq!(args.get_namesrv_addr(), "127.0.0.1:9876");
    }

    #[test]
    fn test_get_namesrv_addr_from_args() {
        let args = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: Some("192.168.1.1:9876".to_string()),
        };

        assert_eq!(args.get_namesrv_addr(), "192.168.1.1:9876");
    }

    #[test]
    fn test_is_valid_host_port() {
        let args = Args {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
        };

        assert!(args.is_valid_host_port("localhost:9876"));
        assert!(args.is_valid_host_port("192.168.0.1:9876"));
        assert!(!args.is_valid_host_port("invalid"));
        assert!(!args.is_valid_host_port("192.168.0.1"));
        assert!(!args.is_valid_host_port("192.168.0.1:invalid"));
    }
}
