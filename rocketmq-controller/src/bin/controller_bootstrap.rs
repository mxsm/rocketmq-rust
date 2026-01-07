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

use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_controller::parse_command_line;
use rocketmq_controller::ControllerManager;
use rocketmq_error::Result;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_rust::ArcMut;
use tracing::info;

/// RocketMQ Controller Bootstrap
///
/// This is the main entry point for starting the RocketMQ Controller.
/// It handles:
/// - Command-line argument parsing
/// - Configuration loading
/// - Controller initialization and startup
///
/// # Examples
///
/// ```bash
/// # Start with default configuration
/// rocketmq-controller-rust
///
/// # Start with configuration file
/// rocketmq-controller-rust --config-file /opt/rocketmq/conf/controller.toml
///
/// # Print configuration and exit
/// rocketmq-controller-rust --print-config-item
/// ```
#[rocketmq_rust::main]
pub async fn main() -> Result<()> {
    // Initialize logger
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;

    // Print banner
    const LOGO: &str = r#"
        ______           _        _  ___  ________       ______          _     _____             _             _ _
        | ___ \         | |      | | |  \/  |  _  |      | ___ \        | |   /  __ \           | |           | | |
        | |_/ /___   ___| | _____| |_| .  . | | | |______| |_/ /   _ ___| |_  | /  \/ ___  _ __ | |_ _ __ ___ | | | ___ _ __
        |    // _ \ / __| |/ / _ \ __| |\/| | | | |______|    / | | / __| __| | |    / _ \| '_ \| __| '__/ _ \| | |/ _ \ '__|
        | |\ \ (_) | (__|   <  __/ |_| |  | \ \/' /      | |\ \ |_| \__ \ |_  | \__/\ (_) | | | | |_| | | (_) | | |  __/ |
        \_| \_\___/ \___|_|\_\___|\__\_|  |_/\_/\_\      \_| \_\__,_|___/\__|  \____/\___/|_| |_|\__|_|  \___/|_|_|\___|_|
    "#;
    println!("{}", LOGO);

    // Set remoting version
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    // Parse command line and load configuration
    info!("Parsing command line arguments...");
    let (_cli, config) = parse_command_line()?;

    info!("RocketMQ Controller configuration loaded successfully");
    info!("Node ID: {}, Listen Address: {}", config.node_id, config.listen_addr);

    let rocketmq_home = &config.rocketmq_home;
    if rocketmq_home.is_empty() {
        eprintln!("Please set the ROCKETMQ_HOME environment variable!");
        eprintln!("   Example: export ROCKETMQ_HOME=/opt/rocketmq");
        std::process::exit(-1);
    }

    info!("ROCKETMQ_HOME: {}", rocketmq_home);

    // Create controller manager
    info!("Creating Controller Manager...");
    let controller_manager = ControllerManager::new(config).await?;
    let controller_manager = ArcMut::new(controller_manager);
    // Initialize controller
    info!("Initializing Controller...");

    let init_result = ControllerManager::initialize(ArcMut::clone(&controller_manager)).await?;
    if !init_result {
        eprintln!("Controller initialization failed!");
        controller_manager.shutdown().await?;
        std::process::exit(-3);
    }

    ControllerManager::start(ArcMut::clone(&controller_manager)).await?;

    info!("Controller started successfully!");
    info!("  Node ID: {}", controller_manager.controller_config().node_id);
    info!("  Listen:  {}", controller_manager.controller_config().listen_addr);
    info!("Controller is running. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Received shutdown signal, shutting down controller...");
        }
        Err(err) => {
            eprintln!("Failed to listen for shutdown signal: {}", err);
        }
    }

    // Graceful shutdown
    if let Err(e) = controller_manager.shutdown().await {
        eprintln!("Error during shutdown: {}", e);
        std::process::exit(1);
    }

    info!("Controller shutdown completed.");
    Ok(())
}
