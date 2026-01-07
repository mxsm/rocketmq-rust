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

use std::path::PathBuf;
use std::process;

use anyhow::bail;
use clap::Parser;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::ParseConfigFile;
use rocketmq_error::Result;
use rocketmq_namesrv::bootstrap::Builder;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_rust::rocketmq;
use tracing::error;
use tracing::info;

const LOGO: &str = r#"
      _____            _        _   __  __  ____         _____           _     _   _                         _____
     |  __ \          | |      | | |  \/  |/ __ \       |  __ \         | |   | \ | |                       / ____|
     | |__) |___   ___| | _____| |_| \  / | |  | |______| |__) |   _ ___| |_  |  \| | __ _ _ __ ___   ___  | (___   ___ _ ____   _____ _ __
     |  _  // _ \ / __| |/ / _ \ __| |\/| | |  | |______|  _  / | | / __| __| | . ` |/ _` | '_ ` _ \ / _ \  \___ \ / _ \ '__\ \ / / _ \ '__|
     | | \ \ (_) | (__|   <  __/ |_| |  | | |__| |      | | \ \ |_| \__ \ |_  | |\  | (_| | | | | | |  __/  ____) |  __/ |   \ V /  __/ |
     |_|  \_\___/ \___|_|\_\___|\__|_|  |_|\___\_\      |_|  \_\__,_|___/\__| |_| \_|\__,_|_| |_| |_|\___| |_____/ \___|_|    \_/ \___|_|
    "#;

#[rocketmq::main]
async fn main() -> Result<()> {
    // Parse command line arguments first
    let args = Args::parse();

    // Initialize the logger
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;

    println!("{}", LOGO);
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    // Parse and merge configurations
    match parse_and_merge_config(&args) {
        Ok((namesrv_config, server_config)) => {
            // Handle print config item mode
            if args.print_config_item {
                print_config_and_exit(&namesrv_config, &server_config);
            }

            // Validate ROCKETMQ_HOME is set
            if namesrv_config.rocketmq_home.is_empty() {
                error!(
                    "Please set the ROCKETMQ_HOME variable in your environment to match the location of the RocketMQ \
                     installation"
                );
                process::exit(-2);
            }

            info!("===== RocketMQ Name Server(Rust) Configuration =====");
            info!("RocketMQ Home: {}", namesrv_config.rocketmq_home);
            info!(
                "Listen Address: {}:{}",
                server_config.bind_address, server_config.listen_port
            );
            info!("KV Config Path: {}", namesrv_config.kv_config_path);
            info!("Config Store Path: {}", namesrv_config.config_store_path);
            info!("Use RouteInfoManager V2: {}", namesrv_config.use_route_info_manager_v2);
            info!("===============================================");

            // Start the name server
            Builder::new()
                .set_name_server_config(namesrv_config)
                .set_server_config(server_config)
                .build()
                .boot()
                .await?;

            Ok(())
        }
        Err(e) => {
            error!("Failed to parse configuration: {}", e);
            process::exit(-1);
        }
    }
}

/// Parse configuration file and merge with command line arguments
/// Command line arguments take precedence over config file settings
fn parse_and_merge_config(args: &Args) -> Result<(NamesrvConfig, ServerConfig)> {
    let home = EnvUtils::get_rocketmq_home();
    info!("RocketMQ Home: {}", home);

    let mut namesrv_config = if let Some(config_file) = args.config_file.clone() {
        if !config_file.exists() || !config_file.is_file() {
            bail!("Config file does not exist or is not a file: {:?}", config_file);
        }
        info!("Loading config from file: {:?}", config_file);
        ParseConfigFile::parse_config_file::<NamesrvConfig>(config_file)?
    } else {
        info!("No config file specified, using default configuration");
        NamesrvConfig::default()
    };

    // Apply command line overrides (command line takes precedence)
    if let Some(ref home_override) = args.rocketmq_home {
        namesrv_config.rocketmq_home = home_override.clone();
    }

    if let Some(ref kv_path) = args.kv_config_path {
        namesrv_config.kv_config_path = kv_path.to_string_lossy().to_string();
    }

    let server_config = ServerConfig {
        listen_port: args.listen_port.unwrap_or(9876),
        bind_address: args.bind_address.clone().unwrap_or_else(|| "0.0.0.0".to_string()),
    };

    Ok((namesrv_config, server_config))
}

/// Print all configuration items and exit
fn print_config_and_exit(namesrv_config: &NamesrvConfig, server_config: &ServerConfig) {
    println!("\n========== Name Server Configuration ==========");
    println!("rocketmqHome = {}", namesrv_config.rocketmq_home);
    println!("kvConfigPath = {}", namesrv_config.kv_config_path);
    println!("configStorePath = {}", namesrv_config.config_store_path);
    println!("productEnvName = {}", namesrv_config.product_env_name);
    println!("clusterTest = {}", namesrv_config.cluster_test);
    println!("orderMessageEnable = {}", namesrv_config.order_message_enable);
    println!(
        "returnOrderTopicConfigToBroker = {}",
        namesrv_config.return_order_topic_config_to_broker
    );
    println!(
        "clientRequestThreadPoolNums = {}",
        namesrv_config.client_request_thread_pool_nums
    );
    println!("defaultThreadPoolNums = {}", namesrv_config.default_thread_pool_nums);
    println!(
        "clientRequestThreadPoolQueueCapacity = {}",
        namesrv_config.client_request_thread_pool_queue_capacity
    );
    println!(
        "defaultThreadPoolQueueCapacity = {}",
        namesrv_config.default_thread_pool_queue_capacity
    );
    println!(
        "scanNotActiveBrokerInterval = {}",
        namesrv_config.scan_not_active_broker_interval
    );
    println!(
        "unRegisterBrokerQueueCapacity = {}",
        namesrv_config.unregister_broker_queue_capacity
    );
    println!("supportActingMaster = {}", namesrv_config.support_acting_master);
    println!("enableAllTopicList = {}", namesrv_config.enable_all_topic_list);
    println!("enableTopicList = {}", namesrv_config.enable_topic_list);
    println!(
        "notifyMinBrokerIdChanged = {}",
        namesrv_config.notify_min_broker_id_changed
    );
    println!(
        "enableControllerInNamesrv = {}",
        namesrv_config.enable_controller_in_namesrv
    );
    println!("needWaitForService = {}", namesrv_config.need_wait_for_service);
    println!("waitSecondsForService = {}", namesrv_config.wait_seconds_for_service);
    println!(
        "deleteTopicWithBrokerRegistration = {}",
        namesrv_config.delete_topic_with_broker_registration
    );
    println!("useRouteInfoManagerV2 = {}", namesrv_config.use_route_info_manager_v2);

    println!("\n========== Server Configuration ==========");
    println!("listenPort = {}", server_config.listen_port);
    println!("bindAddress = {}", server_config.bind_address);

    println!("\n===========================================\n");
    process::exit(0);
}

/// Command line arguments structure
#[derive(Parser, Debug)]
#[command(
    name = "mqnamesrv",
    author = "Apache RocketMQ",
    version = "0.1.0",
    about = "RocketMQ Name Server (Rust Implementation)",
    long_about = "Apache RocketMQ Name Server - Rust implementation providing lightweight service discovery and \
                  routing"
)]
struct Args {
    /// Name server config properties file
    #[arg(
        short = 'c',
        long = "configFile",
        value_name = "FILE",
        help = "Name server config properties file"
    )]
    config_file: Option<PathBuf>,

    /// Print all config items and exit
    #[arg(short = 'p', long = "printConfigItem", help = "Print all config items and exit")]
    print_config_item: bool,

    /// Name server listen port
    /// Command line override for listen port (default: 9876)
    #[arg(
        long = "listenPort",
        value_name = "PORT",
        help = "Name server listen port (default: 9876)"
    )]
    listen_port: Option<u32>,

    /// Name server bind address
    /// Command line override for bind address (default: 0.0.0.0)
    #[arg(
        long = "bindAddress",
        value_name = "ADDRESS",
        help = "Name server bind address (default: 0.0.0.0)"
    )]
    bind_address: Option<String>,

    /// RocketMQ home directory
    /// Command line override for ROCKETMQ_HOME
    #[arg(long = "rocketmqHome", value_name = "PATH", help = "RocketMQ home directory")]
    rocketmq_home: Option<String>,

    /// KV config path
    /// Command line override for kvConfigPath
    #[arg(long = "kvConfigPath", value_name = "PATH", help = "KV config file path")]
    kv_config_path: Option<PathBuf>,
}
