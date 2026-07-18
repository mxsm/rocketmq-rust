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

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_controller::parse_command_line;
use rocketmq_controller::typ::Node;
use rocketmq_controller::ControllerCli;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerManager;
use rocketmq_error::ControllerError;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_runtime::wait_for_signal_result;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceContext;
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
const ENTRYPOINT_MAX_BLOCKING_THREADS: usize = 64;
const LOGO: &str = r#"
        ______           _        _  ___  ________       ______          _     _____             _             _ _
        | ___ \         | |      | | |  \/  |  _  |      | ___ \        | |   /  __ \           | |           | | |
        | |_/ /___   ___| | _____| |_| .  . | | | |______| |_/ /   _ ___| |_  | /  \/ ___  _ __ | |_ _ __ ___ | | | ___ _ __
        |    // _ \ / __| |/ / _ \ __| |\/| | | | |______|    / | | / __| __| | |    / _ \| '_ \| __| '__/ _ \| | |/ _ \ '__|
        | |\ \ (_) | (__|   <  __/ |_| |  | \ \/' /      | |\ \ |_| \__ \ |_  | \__/\ (_) | | | | |_| | | (_) | | |  __/ |
        \_| \_\___/ \___|_|\_\___|\__\_|  |_/\_/\_\      \_| \_\__,_|___/\__|  \____/\___/|_| |_|\__|_|  \___/|_|_|\___|_|
    "#;

pub fn main() -> Result<()> {
    let owner = RuntimeOwner::new(controller_runtime_config())
        .map_err(|error| ControllerError::Internal(format!("failed to build controller runtime: {error}")))?;
    let service_context = owner.context().service_context("rocketmq-controller-runtime");

    let run_result = owner.block_on(run(service_context));
    let shutdown_result = owner
        .shutdown_runtime_blocking()
        .map_err(|error| ControllerError::Internal(format!("failed to shutdown controller runtime: {error}")));

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(report)) => {
            if !report.is_healthy() {
                tracing::warn!(
                    report = %report.to_json(),
                    "controller runtime shutdown report is unhealthy"
                );
            }
            Ok(())
        }
    }
}

fn controller_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::controller_default();
    config.max_blocking_threads = ENTRYPOINT_MAX_BLOCKING_THREADS;
    config
}

async fn run(service_context: ServiceContext) -> Result<()> {
    // Set remoting version
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    // Parse command line and load configuration
    let (cli, config) = parse_command_line()?;

    if cli.print_config_item {
        ControllerCli::print_config(&config);
        return Ok(());
    }

    let rocketmq_home = &config.rocketmq_home;
    if rocketmq_home.is_empty() {
        bail!("Please set the ROCKETMQ_HOME environment variable. Example: export ROCKETMQ_HOME=/opt/rocketmq");
    }

    let bootstrap_config = build_controller_telemetry_bootstrap_config(&config);
    let telemetry_guard = rocketmq_observability::logging::install_global(&bootstrap_config)
        .context("failed to initialize controller telemetry bootstrap")?;
    log_telemetry_bootstrap(&bootstrap_config, telemetry_guard.subscriber_install_status());

    println!("{}", LOGO);

    info!("RocketMQ Controller configuration loaded successfully");
    info!("Node ID: {}, Listen Address: {}", config.node_id, config.listen_addr);
    info!("ROCKETMQ_HOME: {}", rocketmq_home);

    let controller_result = run_controller(config, service_context).await;
    let shutdown_result = telemetry_guard
        .shutdown()
        .into_result()
        .context("failed to shutdown controller telemetry bootstrap");

    match (controller_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(_report)) => Ok(()),
    }
}

async fn run_controller(config: ControllerConfig, service_context: ServiceContext) -> Result<()> {
    // Create controller manager
    info!("Creating Controller Manager...");
    let controller_manager = ControllerManager::new_with_service_context(config, service_context).await?;
    let controller_manager = ArcMut::new(controller_manager);
    // Initialize controller
    info!("Initializing Controller...");

    let init_result = ControllerManager::initialize(ArcMut::clone(&controller_manager)).await?;
    if !init_result {
        controller_manager.shutdown().await?;
        bail!("Controller initialization failed");
    }

    ControllerManager::start(ArcMut::clone(&controller_manager)).await?;
    initialize_single_node_cluster_if_configured(&controller_manager).await?;

    info!("Controller started successfully!");
    info!("  Node ID: {}", controller_manager.controller_config().node_id);
    info!("  Listen:  {}", controller_manager.controller_config().listen_addr);
    info!("Controller is running. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    match wait_for_signal_result().await {
        Ok(()) => {
            info!("Received shutdown signal, shutting down controller...");
        }
        Err(err) => {
            tracing::warn!(error = %err, "failed to listen for shutdown signal");
        }
    }

    // Graceful shutdown
    controller_manager.shutdown().await?;

    info!("Controller shutdown completed.");
    Ok(())
}

fn build_controller_telemetry_bootstrap_config(
    controller_config: &ControllerConfig,
) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut observability = rocketmq_observability::ObservabilityConfig {
        service_name: "rocketmq-controller".to_string(),
        service_namespace: "rocketmq".to_string(),
        node_type: "controller".to_string(),
        node_id: controller_config.node_id.to_string(),
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;

    let mut logging = rocketmq_observability::LoggingConfig::default();
    logging.file.directory = controller_log_directory(controller_config);
    logging.file.file_name_prefix = "rocketmq-controller".to_string();

    rocketmq_observability::TelemetryBootstrapConfig { observability, logging }
}

fn controller_log_directory(controller_config: &ControllerConfig) -> String {
    if controller_config.rocketmq_home.trim().is_empty() {
        return "logs".to_string();
    }
    std::path::PathBuf::from(controller_config.rocketmq_home.as_str())
        .join("logs")
        .to_string_lossy()
        .into_owned()
}

fn log_telemetry_bootstrap(
    config: &rocketmq_observability::TelemetryBootstrapConfig,
    subscriber_install_status: rocketmq_observability::SubscriberInstallStatus,
) {
    info!(
        metrics_exporter = ?config.observability.metrics.exporter,
        trace_exporter = ?config.observability.traces.exporter,
        log_exporter = ?config.observability.logs.exporter,
        subscriber_installed = subscriber_install_status.installed,
        file_log_enabled = config.logging.file.enabled,
        "controller telemetry bootstrap initialized"
    );
}

async fn initialize_single_node_cluster_if_configured(controller_manager: &ArcMut<ControllerManager>) -> Result<()> {
    let config = controller_manager.controller_config();
    if config.raft_peers.len() != 1 {
        return Ok(());
    }

    let peer = config.raft_peers[0].clone();
    if peer.id != config.node_id {
        return Ok(());
    }
    if controller_manager.controller().has_committed_log()? {
        info!("Single-node controller cluster already has committed logs; skip bootstrap");
        return Ok(());
    }

    let mut nodes = BTreeMap::new();
    nodes.insert(
        peer.id,
        Node {
            node_id: peer.id,
            rpc_addr: peer.addr.to_string(),
        },
    );
    info!("Bootstrapping single-node controller cluster with node {}", peer.id);
    controller_manager.controller().initialize_cluster(nodes).await?;

    for _ in 0..30 {
        if controller_manager.is_leader() {
            info!("Single-node controller cluster elected local node as leader");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(ControllerError::Internal("single-node controller did not become leader after bootstrap".to_string()).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_telemetry_bootstrap_uses_required_logging_defaults() {
        let mut controller_config = ControllerConfig::default();
        controller_config.rocketmq_home = "target/controller-telemetry-bootstrap".to_string();
        controller_config.node_id = 7;

        let config = build_controller_telemetry_bootstrap_config(&controller_config);

        assert_eq!(config.observability.service_name, "rocketmq-controller");
        assert_eq!(config.observability.node_id, "7");
        assert_eq!(
            config.observability.subscriber_install_policy,
            rocketmq_observability::SubscriberInstallPolicy::Required
        );
        assert!(!config.observability.enabled);
        assert!(config.logging.enabled);
        assert!(config.logging.console.enabled);
        assert!(!config.logging.file.enabled);
        assert_eq!(config.logging.file.file_name_prefix, "rocketmq-controller");

        let expected_log_dir = std::path::PathBuf::from("target/controller-telemetry-bootstrap").join("logs");
        assert_eq!(
            std::path::PathBuf::from(config.logging.file.directory.as_str()),
            expected_log_dir
        );
    }
}
