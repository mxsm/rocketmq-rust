/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Command Line Interface Module for RocketMQ Controller
//!
//! This module provides CLI argument parsing and configuration loading for the RocketMQ Controller.
//! Uses the `config` crate (via `parse_config_file`) to support multiple configuration formats
//! including TOML, JSON, YAML, etc.
//!
//! # Examples
//!
//! ```bash
//! # Start with config file
//! rocketmq-controller --config-file ./conf/controller.toml
//!
//! # Print configuration and exit
//! rocketmq-controller --print-config-item
//!
//! # Show help
//! rocketmq-controller --help
//! ```

use std::path::PathBuf;

use clap::Parser;
use rocketmq_common::utils::parse_config_file::parse_config_file;
use rocketmq_error::RocketMQResult;
use tracing::info;

use crate::config::ControllerConfig;

/// RocketMQ Controller Command Line Arguments
///
/// Parses command-line arguments for the RocketMQ Controller using clap.
/// Supports loading configuration from files (TOML/JSON/YAML) and printing config items.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "mqcontroller",
    author = "mxsm",
    version = env!("CARGO_PKG_VERSION"),
    about = "RocketMQ Controller Server (Rust)",
    long_about = "RocketMQ Controller Server implemented in Rust.\n\
                  Responsible for broker metadata management and leader election."
)]
pub struct ControllerCli {
    /// Controller configuration file path (TOML, JSON, YAML, etc.)
    ///
    /// # Examples
    ///
    /// ```bash
    /// --config-file /opt/rocketmq/conf/controller.toml
    /// -c ./controller.toml
    /// ```
    #[arg(
        short = 'c',
        long = "config-file",
        value_name = "FILE",
        help = "Controller config file (TOML/JSON/YAML)"
    )]
    pub config_file: Option<PathBuf>,

    /// Print all configuration items and exit
    ///
    /// When enabled, prints all controller configuration properties
    /// to the console and exits without starting the controller.
    #[arg(short = 'p', long = "print-config-item", help = "Print all config items")]
    pub print_config_item: bool,

    /// Enable test/development mode with fake identifiers
    ///
    /// When enabled, uses test-mode configurations for development purposes.
    #[arg(short = 'm', long = "print-important-config", help = "Print important config items")]
    pub print_important_config: bool,
}

impl ControllerCli {
    /// Parse command line arguments
    ///
    /// Wrapper around clap's parse() for easier testing and mocking.
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Validate parsed arguments
    ///
    /// Checks for:
    /// - Config file exists if specified
    /// - Config file is readable
    ///
    /// # Errors
    ///
    /// Returns error if validation fails
    pub fn validate(&self) -> RocketMQResult<()> {
        // Validate config file if specified
        if let Some(ref path) = self.config_file {
            if !path.exists() {
                return Err(rocketmq_error::RocketMQError::from(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Config file not found: {}", path.display()),
                )));
            }

            if !path.is_file() {
                return Err(rocketmq_error::RocketMQError::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Config path is not a file: {}", path.display()),
                )));
            }
        }

        Ok(())
    }

    /// Load configuration from file
    ///
    /// Uses `parse_config_file` from rocketmq-common which supports multiple formats:
    /// - TOML (.toml)
    /// - JSON (.json)
    /// - YAML (.yaml, .yml)
    /// - And other formats supported by the `config` crate
    ///
    /// If no config file is specified, returns the default configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Default configuration to use if no file is specified
    ///
    /// # Returns
    ///
    /// Loaded configuration
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Config file cannot be read
    /// - Config file format is invalid
    pub fn load_config(&self, config: ControllerConfig) -> RocketMQResult<ControllerConfig> {
        // Load from config file if specified
        if let Some(ref config_path) = self.config_file {
            info!("Loading configuration from file: {}", config_path.display());

            // Use parse_config_file utility from rocketmq-common
            let loaded_config = parse_config_file(config_path.clone())?;

            println!("Loaded configuration from file: {}", config_path.display());

            Ok(loaded_config)
        } else {
            Ok(config)
        }
    }

    /// Print configuration to console
    ///
    /// Prints configuration in human-readable format with sections:
    /// - Controller Configuration
    /// - Node Configuration
    /// - Raft Configuration
    /// - Storage Configuration
    pub fn print_config(config: &ControllerConfig) {
        println!("\n========== Controller Configuration ==========");
        println!("RocketMQ Home:           {}", config.rocketmq_home);
        println!("Config Store Path:       {:?}", config.config_store_path);
        println!("Controller Type:         {}", config.controller_type);
        println!("Scan Interval:           {} ms", config.scan_not_active_broker_interval);
        println!("Thread Pool Nums:        {}", config.controller_thread_pool_nums);
        println!(
            "Request Queue Capacity:  {}",
            config.controller_request_thread_pool_queue_capacity
        );

        println!("\n========== Node Configuration ==========");
        println!("Node ID:                 {}", config.node_id);
        println!("Listen Address:          {}", config.listen_addr);

        println!("\n========== Raft Configuration ==========");
        println!("Election Timeout:        {} ms", config.election_timeout_ms);
        println!("Heartbeat Interval:      {} ms", config.heartbeat_interval_ms);
        println!("Raft Peers:              {} peers", config.raft_peers.len());
        for peer in &config.raft_peers {
            println!("  - Node {}: {}", peer.id, peer.addr);
        }

        println!("\n========== Storage Configuration ==========");
        println!("Storage Path:            {}", config.storage_path);
        println!("Storage Backend:         {:?}", config.storage_backend);
        println!("Mapped File Size:        {} bytes", config.mapped_file_size);
        println!();
    }
}

/// Parse command line arguments and load configuration
///
/// High-level helper function that combines parsing, validation, and config loading.
/// This is the recommended entry point for applications.
///
/// # Returns
///
/// Tuple of (CLI arguments, loaded configuration)
///
/// # Errors
///
/// Returns error if:
/// - Argument parsing fails (handled by clap, exits process)
/// - Validation fails
/// - Configuration loading fails
///
/// # Examples
///
/// ```rust,no_run
/// use rocketmq_controller::cli::parse_command_line;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let (cli, config) = parse_command_line()?;
///
///     if cli.print_config_item {
///         rocketmq_controller::cli::ControllerCli::print_config(&config);
///         std::process::exit(0);
///     }
///
///     // Start controller with config...
///     Ok(())
/// }
/// ```
pub fn parse_command_line() -> RocketMQResult<(ControllerCli, ControllerConfig)> {
    // Parse arguments (exits on --help or --version)
    let cli = ControllerCli::parse_args();

    // Validate arguments
    cli.validate()?;

    // Load default configuration
    let default_config = ControllerConfig::default();

    // Apply config file if specified
    let config = cli.load_config(default_config)?;

    Ok((cli, config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parse_no_args() {
        let cli = ControllerCli::parse_from(vec!["mqcontroller"]);
        assert!(cli.config_file.is_none());
        assert!(!cli.print_config_item);
        assert!(!cli.print_important_config);
    }

    #[test]
    fn test_cli_parse_with_config() {
        let cli = ControllerCli::parse_from(vec!["mqcontroller", "-c", "config.toml"]);
        assert!(cli.config_file.is_some());
        assert_eq!(cli.config_file.unwrap().to_str().unwrap(), "config.toml");
    }

    #[test]
    fn test_cli_parse_print_config() {
        let cli = ControllerCli::parse_from(vec!["mqcontroller", "--print-config-item"]);
        assert!(cli.print_config_item);
    }

    #[test]
    fn test_validate_nonexistent_file() {
        let cli = ControllerCli {
            config_file: Some(PathBuf::from("/nonexistent/file.toml")),
            print_config_item: false,
            print_important_config: false,
        };
        assert!(cli.validate().is_err());
    }

    #[test]
    fn test_validate_no_config_file() {
        let cli = ControllerCli {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
        };
        assert!(cli.validate().is_ok());
    }

    #[test]
    fn test_load_config_no_file() {
        let cli = ControllerCli {
            config_file: None,
            print_config_item: false,
            print_important_config: false,
        };
        let default_config = ControllerConfig::default();
        let result = cli.load_config(default_config.clone());
        assert!(result.is_ok());
        // Should return default config
        assert_eq!(result.unwrap().node_id, default_config.node_id);
    }
}
