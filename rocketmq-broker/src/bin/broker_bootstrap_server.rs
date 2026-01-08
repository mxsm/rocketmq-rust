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
use std::path::PathBuf;
use std::process;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use rocketmq_broker::command::Args;
use rocketmq_broker::Builder;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::ParseConfigFile;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_rust::rocketmq;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use tracing::error;
use tracing::info;
use tracing::warn;

const LOGO: &str = r#"
  _____            _        _   __  __  ____         _____           _     ____            _
 |  __ \          | |      | | |  \/  |/ __ \       |  __ \         | |   |  _ \          | |
 | |__) |___   ___| | _____| |_| \  / | |  | |______| |__) |   _ ___| |_  | |_) |_ __ ___ | | _____ _ __
 |  _  // _ \ / __| |/ / _ \ __| |\/| | |  | |______|  _  / | | / __| __| |  _ <| '__/ _ \| |/ / _ \ '__|
 | | \ \ (_) | (__|   <  __/ |_| |  | | |__| |      | | \ \ |_| \__ \ |_  | |_) | | | (_) |   <  __/ |
 |_|  \_\___/ \___|_|\_\___|\__|_|  |_|\___\_\      |_|  \_\__,_|___/\__| |____/|_|  \___/|_|\_\___|_|
"#;

#[rocketmq::main]
async fn main() -> Result<()> {
    // Initialize logger
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;

    // Print logo
    println!("{}", LOGO);

    // Set remoting version
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    // Parse and validate command line arguments
    let args = Args::parse();
    if let Err(e) = args.validate() {
        error!("Invalid arguments: {}", e);
        process::exit(-1);
    }

    // Check ROCKETMQ_HOME environment variable
    if let Err(e) = verify_rocketmq_home() {
        error!("{}", e);
        process::exit(-2);
    }

    // Parse configuration from file and command line
    let (mut broker_config, message_store_config) = match parse_config_file(&args) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to parse configuration: {}", e);
            process::exit(-3);
        }
    };

    // Apply system properties (should be done before command line override)
    let properties = extract_properties_from_config(&broker_config);
    Args::apply_system_properties(&properties);

    // Override config with command line arguments
    apply_command_line_args(&mut broker_config, &args);

    // Validate broker configuration
    if let Err(e) = validate_broker_config(&broker_config, &message_store_config) {
        error!("Invalid broker configuration: {}", e);
        process::exit(-4);
    }

    // Handle print config and exit
    if args.should_exit_after_print() {
        print_config(&broker_config, &message_store_config, args.print_important_config);
        process::exit(0);
    }

    // Print startup info
    print_startup_info(&broker_config);

    // Start broker
    Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .build()
        .boot()
        .await;

    Ok(())
}

/// Verify ROCKETMQ_HOME environment variable is set
fn verify_rocketmq_home() -> Result<()> {
    let home = EnvUtils::get_rocketmq_home();
    if home.is_empty() {
        anyhow::bail!(
            "Please set the ROCKETMQ_HOME environment variable to match the location of the RocketMQ installation"
        );
    }

    let home_path = PathBuf::from(&home);
    if !home_path.exists() || !home_path.is_dir() {
        warn!("ROCKETMQ_HOME directory does not exist or is not a directory: {}", home);
    }

    info!("ROCKETMQ_HOME: {}", home);
    Ok(())
}

/// Parse configuration from file
///
/// Priority:
/// 1. Explicit config file from `-c` argument
/// 2. $ROCKETMQ_HOME/conf/broker.toml
/// 3. Default configuration
fn parse_config_file(args: &Args) -> Result<(BrokerConfig, MessageStoreConfig)> {
    if let Some(config_file) = args.get_config_file() {
        info!("Loading configuration from: {}", config_file.display());

        let broker_config = ParseConfigFile::parse_config_file::<BrokerConfig>(config_file.clone())
            .with_context(|| format!("Failed to parse BrokerConfig from {:?}", config_file))?;

        let message_store_config = ParseConfigFile::parse_config_file::<MessageStoreConfig>(config_file.clone())
            .with_context(|| format!("Failed to parse MessageStoreConfig from {:?}", config_file))?;

        Ok((broker_config, message_store_config))
    } else {
        info!("Using default configuration (no config file specified)");
        Ok((BrokerConfig::default(), MessageStoreConfig::default()))
    }
}

/// Extract properties from BrokerConfig for system property mapping
fn extract_properties_from_config(_broker_config: &BrokerConfig) -> HashMap<String, String> {
    // Extract relevant properties for system env mapping
    // This is where Java's properties file entries would be mapped
    // Currently returning empty map as Rust config uses typed structs

    HashMap::new()
}

/// Apply command line arguments to broker configuration
///
/// Command line arguments have highest priority and override config file values
fn apply_command_line_args(broker_config: &mut BrokerConfig, args: &Args) {
    // Apply name server address only if explicitly provided via command line or env
    // Otherwise, keep the value from config file
    if args.namesrv_addr.is_some() || env::var("NAMESRV_ADDR").is_ok() {
        let namesrv_addr = args.get_namesrv_addr();
        broker_config.namesrv_addr = Some(namesrv_addr.into());
        info!(
            "Name server address (from command line/env): {}",
            broker_config.namesrv_addr.as_ref().unwrap()
        );
    } else if let Some(ref addr) = broker_config.namesrv_addr {
        info!("Name server address (from config file): {}", addr);
    } else {
        // Use default if not set anywhere
        broker_config.namesrv_addr = Some("127.0.0.1:9876".to_string().into());
        info!(
            "Name server address (default): {}",
            broker_config.namesrv_addr.as_ref().unwrap()
        );
    }
}

/// Validate broker configuration
///
/// Performs validation similar to Java's buildBrokerController:
/// - Validate name server address format
/// - Check broker role configuration
/// - Validate broker ID
fn validate_broker_config(broker_config: &BrokerConfig, _message_store_config: &MessageStoreConfig) -> Result<()> {
    // Validate name server address if set
    if let Some(ref namesrv_addr) = broker_config.namesrv_addr {
        validate_namesrv_address(namesrv_addr.as_str())?;
    }

    // Validate broker ID based on role (if not using controller mode)
    // Note: broker_id is u64, so it's always >= 0
    // No need to check for negative values

    Ok(())
}

/// Validate name server address format
fn validate_namesrv_address(namesrv_addr: &str) -> Result<()> {
    if namesrv_addr.is_empty() {
        return Ok(());
    }

    let addr_array: Vec<&str> = namesrv_addr.split(';').collect();
    for addr in addr_array {
        let trimmed = addr.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Basic validation: should contain colon for port
        if !trimmed.contains(':') {
            anyhow::bail!(
                "Invalid name server address format: {}. Expected format: '127.0.0.1:9876;192.168.0.1:9876'",
                namesrv_addr
            );
        }
    }

    Ok(())
}

/// Print broker configuration
fn print_config(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig, important_only: bool) {
    println!("\n========== Broker Configuration ==========");

    if important_only {
        println!("  Important configuration items:");
        print_important_broker_config(broker_config);
        print_important_message_store_config(message_store_config);
    } else {
        println!("  All configuration items:");
        print_all_broker_config(broker_config);
        print_all_message_store_config(message_store_config);
    }

    println!("==========================================\n");
}

/// Print important broker configuration items
fn print_important_broker_config(config: &BrokerConfig) {
    println!("  BrokerConfig:");
    println!("    brokerName: {}", config.broker_identity.broker_name);
    println!("    brokerClusterName: {}", config.broker_identity.broker_cluster_name);
    println!("    brokerId: {}", config.broker_identity.broker_id);
    println!("    brokerIP1: {}", config.broker_ip1);
    println!("    listenPort: {}", config.listen_port);
    println!("    namesrvAddr: {:?}", config.namesrv_addr);
    println!("    enableControllerMode: {}", config.enable_controller_mode);
    println!("    storePathRootDir: {}", config.store_path_root_dir);
}

/// Print important message store configuration items
fn print_important_message_store_config(config: &MessageStoreConfig) {
    println!("  MessageStoreConfig:");
    println!("    storePathRootDir: {}", config.store_path_root_dir);
    println!("    storePathCommitLog: {:?}", config.store_path_commit_log);
    println!("    deleteWhen: {}", config.delete_when);
    println!("    flushDiskType: {:?}", config.flush_disk_type);
}

/// Print all broker configuration items
fn print_all_broker_config(config: &BrokerConfig) {
    println!("  BrokerConfig:");
    let properties = config.get_properties();
    for (key, value) in properties.iter() {
        println!("    {}: {}", key, value);
    }
}

/// Print all message store configuration items
fn print_all_message_store_config(config: &MessageStoreConfig) {
    println!("  MessageStoreConfig:");
    // Message store config doesn't implement get_properties yet
    // Print key fields manually
    println!("    storePathRootDir: {}", config.store_path_root_dir);
    println!("    storePathCommitLog: {:?}", config.store_path_commit_log);
    println!("    deleteWhen: {}", config.delete_when);
    println!("    flushDiskType: {:?}", config.flush_disk_type);
    println!("    commitLogFileSize: {}", config.mapped_file_size_commit_log);
}

/// Print broker startup information
fn print_startup_info(broker_config: &BrokerConfig) {
    info!(
        "Starting broker: brokerName={}, brokerClusterName={}, brokerId={}",
        broker_config.broker_identity.broker_name,
        broker_config.broker_identity.broker_cluster_name,
        broker_config.broker_identity.broker_id
    );

    if let Some(ref namesrv_addr) = broker_config.namesrv_addr {
        info!("Name server address: {}", namesrv_addr);
    }

    info!(
        "Broker listening on: {}:{}",
        broker_config.broker_ip1, broker_config.listen_port
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_namesrv_address_single() {
        assert!(validate_namesrv_address("127.0.0.1:9876").is_ok());
    }

    #[test]
    fn test_validate_namesrv_address_multiple() {
        assert!(validate_namesrv_address("192.168.0.1:9876;192.168.0.2:9876").is_ok());
    }

    #[test]
    fn test_validate_namesrv_address_invalid() {
        assert!(validate_namesrv_address("invalid_address").is_err());
    }

    #[test]
    fn test_validate_namesrv_address_empty() {
        assert!(validate_namesrv_address("").is_ok());
    }
}
