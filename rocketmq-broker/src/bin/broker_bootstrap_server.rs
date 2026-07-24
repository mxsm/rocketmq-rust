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
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use rocketmq_broker::build_broker_telemetry_bootstrap_config;
use rocketmq_broker::command::Args;
use rocketmq_broker::Builder;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
#[cfg(test)]
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::ParseConfigFile;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ShutdownReason;
use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_security_api::ValidatedSecurityBootstrap;
#[cfg(feature = "tieredstore")]
use rocketmq_store::base::store_enum::StoreType;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
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

const ENTRYPOINT_MAX_BLOCKING_THREADS: usize = 64;

fn main() -> Result<()> {
    let owner = RuntimeOwner::new(broker_runtime_config()).context("failed to build broker runtime")?;
    let service_context = owner.context().service_context("rocketmq-broker-runtime");
    let lifecycle = ServiceLifecycle::from_env("rocketmq-broker").context("invalid broker lifecycle configuration")?;

    let run_result = owner.block_on(run(service_context, lifecycle.clone()));
    if run_result.is_err() {
        lifecycle.mark_failed();
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_result = owner
        .shutdown_runtime_blocking_until(shutdown_request.deadline)
        .context("failed to shutdown broker runtime");

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(report)) => {
            if !report.is_healthy() {
                lifecycle.mark_failed();
                tracing::warn!(
                    report = %report.to_json(),
                    "broker runtime shutdown report is unhealthy"
                );
                anyhow::bail!("broker runtime shutdown report is unhealthy");
            }
            Ok(())
        }
    }
}

fn broker_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::broker_default();
    config.max_blocking_threads = ENTRYPOINT_MAX_BLOCKING_THREADS;
    config
}

async fn run(service_context: ServiceContext, lifecycle: ServiceLifecycle) -> Result<()> {
    // Set remoting version
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    // Parse and validate command line arguments
    let args = Args::parse();
    args.validate().context("invalid broker arguments")?;

    // Parse configuration from file and command line
    let (mut broker_config, message_store_config, logging_overrides) =
        parse_config_file(&args).context("failed to parse broker configuration")?;

    // Apply system properties (should be done before command line override)
    let properties = extract_properties_from_config(&broker_config);
    Args::apply_system_properties(&properties);

    // Override config with command line arguments
    apply_command_line_args(&mut broker_config, &args);

    // Validate broker configuration
    validate_broker_config(&broker_config, &message_store_config).context("invalid broker configuration")?;

    verify_rocketmq_home()?;

    // Handle print config and exit without creating telemetry or service listeners.
    if args.should_exit_after_print() {
        print_config(&broker_config, &message_store_config, args.print_important_config);
        return Ok(());
    }

    let security_config =
        SecurityBootstrapConfig::from_env().context("failed to load broker security bootstrap configuration")?;
    let validated_security = validate_broker_security(
        &security_config,
        &broker_config,
        &message_store_config,
        lifecycle.config().probe_bind_addr,
    )
    .context("broker security bootstrap failed before listener bind")?;

    let environment_filter = rocketmq_observability::read_rust_log().context("failed to read RUST_LOG")?;
    let resolved_filter = resolve_startup_log_filter(&args, &logging_overrides, environment_filter.as_deref())
        .context("failed to resolve broker log filter")?;
    let mut bootstrap_config = build_broker_telemetry_bootstrap_config(&broker_config);
    bootstrap_config.logging.reload = logging_overrides.logging.reload;
    let telemetry_guard =
        rocketmq_observability::install_global_with_filter(&bootstrap_config, resolved_filter.clone())
            .context("failed to initialize broker telemetry bootstrap")?;
    log_telemetry_bootstrap(
        &bootstrap_config,
        &resolved_filter,
        telemetry_guard.subscriber_install_status(),
    );
    log_security_bootstrap(validated_security);

    // Print logo
    println!("{}", LOGO);

    // Print startup info
    print_startup_info(&broker_config, &message_store_config);
    if let Err(error) = lifecycle.start(&service_context).await {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        if let Err(shutdown_error) = telemetry_guard
            .shutdown_with_timeout(request.deadline.remaining())
            .into_result()
        {
            tracing::warn!(error = %shutdown_error, "broker telemetry cleanup after lifecycle startup failure was unhealthy");
        }
        return Err(error).context("failed to start broker lifecycle boundary");
    }

    // Start broker
    Builder::new()
        .set_broker_config(broker_config)
        .set_message_store_config(message_store_config)
        .set_service_context(service_context)
        .set_telemetry_runtime_guard(telemetry_guard)
        .build()
        .boot_with_lifecycle(lifecycle)
        .await
        .context("broker lifecycle failed")?;

    Ok(())
}

fn validate_broker_security(
    security_config: &SecurityBootstrapConfig,
    broker_config: &BrokerConfig,
    message_store_config: &MessageStoreConfig,
    probe_bind_addr: Option<SocketAddr>,
) -> Result<ValidatedSecurityBootstrap> {
    let bind_ip = broker_config
        .broker_server_config
        .bind_address
        .parse::<IpAddr>()
        .context("broker bindAddress must be an IP address")?;
    let listen_port = u16::try_from(broker_config.broker_server_config.listen_port)
        .context("broker listenPort must fit a TCP port")?;
    let fast_listen_port = listen_port
        .checked_sub(2)
        .context("broker listenPort must leave room for the fast remoting listener")?;
    let mut listeners = vec![
        SocketAddr::new(bind_ip, listen_port),
        SocketAddr::new(bind_ip, fast_listen_port),
    ];
    if !message_store_config.duplication_enable {
        let ha_listen_port =
            u16::try_from(message_store_config.ha_listen_port).context("broker haListenPort must fit a TCP port")?;
        listeners.push(SocketAddr::new(message_store_config.ha_listen_address, ha_listen_port));
    }
    if broker_config.metrics_exporter_type == rocketmq_common::common::metrics::MetricsExporterType::Prom {
        let metrics_ip = broker_config
            .metrics_prom_exporter_host
            .parse::<IpAddr>()
            .context("broker metricsPromExporterHost must be an IP address")?;
        listeners.push(SocketAddr::new(metrics_ip, broker_config.metrics_prom_exporter_port));
    }
    if let Some(probe_bind_addr) = probe_bind_addr {
        listeners.push(probe_bind_addr);
    }
    security_config.validate(&listeners).map_err(anyhow::Error::from)
}

fn log_security_bootstrap(validated: ValidatedSecurityBootstrap) {
    match validated.profile() {
        SecurityBootstrapProfile::DevelopmentInsecureLoopback => warn!(
            profile = validated.profile().as_str(),
            listener_count = validated.listener_count(),
            "broker development-insecure security profile is active; every listener is restricted to loopback"
        ),
        SecurityBootstrapProfile::SecureEnforced => info!(
            profile = validated.profile().as_str(),
            listener_count = validated.listener_count(),
            "broker secure bootstrap completed before listener bind"
        ),
    }
}

fn log_telemetry_bootstrap(
    config: &rocketmq_observability::TelemetryBootstrapConfig,
    resolved_filter: &rocketmq_observability::ResolvedLogFilter,
    subscriber_install_status: rocketmq_observability::SubscriberInstallStatus,
) {
    info!(
        service = "rocketmq-broker",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        metrics_exporter = ?config.observability.metrics.exporter,
        trace_exporter = ?config.observability.traces.exporter,
        log_exporter = ?config.observability.logs.exporter,
        subscriber_installed = subscriber_install_status.installed,
        reload_enabled = config.logging.reload.enabled,
        file_log_enabled = config.logging.file.enabled,
        "broker telemetry bootstrap initialized"
    );
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
fn parse_config_file(
    args: &Args,
) -> Result<(
    BrokerConfig,
    MessageStoreConfig,
    rocketmq_observability::LoggingOverrides,
)> {
    if let Some(config_file) = args.get_config_file() {
        info!("Loading configuration from: {}", config_file.display());

        let mut broker_config = ParseConfigFile::parse_config_file::<BrokerConfig>(config_file.clone())
            .with_context(|| format!("Failed to parse BrokerConfig from {:?}", config_file))?;
        apply_tls_properties_from_file(&mut broker_config, config_file.clone())?;

        let message_store_config = ParseConfigFile::parse_config_file::<MessageStoreConfig>(config_file.clone())
            .with_context(|| format!("Failed to parse MessageStoreConfig from {:?}", config_file))?;
        let logging_overrides =
            ParseConfigFile::parse_config_file::<rocketmq_observability::LoggingOverrides>(config_file.clone())
                .with_context(|| format!("Failed to parse logging configuration from {:?}", config_file))?;

        Ok((broker_config, message_store_config, logging_overrides))
    } else {
        info!("Using default configuration (no config file specified)");
        Ok((
            BrokerConfig::default(),
            MessageStoreConfig::default(),
            rocketmq_observability::LoggingOverrides::default(),
        ))
    }
}

fn resolve_startup_log_filter(
    args: &Args,
    overrides: &rocketmq_observability::LoggingOverrides,
    environment_filter: Option<&str>,
) -> Result<rocketmq_observability::ResolvedLogFilter, rocketmq_observability::ObservabilityError> {
    rocketmq_observability::LogFilterResolver::resolve(rocketmq_observability::LogFilterInputs {
        runtime: None,
        cli: args.log_filter.as_deref(),
        environment: environment_filter,
        config: overrides.logging.filter.as_deref(),
        legacy_config: overrides.log_filter.as_deref(),
    })
}

fn apply_tls_properties_from_file(broker_config: &mut BrokerConfig, config_file: PathBuf) -> Result<()> {
    let content = std::fs::read_to_string(&config_file)
        .with_context(|| format!("Failed to read TLS properties from {:?}", config_file))?;
    broker_config
        .broker_server_config
        .tls_config
        .apply_java_properties_str(&content);
    Ok(())
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
fn validate_broker_config(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) -> Result<()> {
    // Validate name server address if set
    if let Some(ref namesrv_addr) = broker_config.namesrv_addr {
        validate_namesrv_address(namesrv_addr.as_str())?;
    }

    validate_tieredstore_config(message_store_config)?;

    // Validate broker ID based on role (if not using controller mode)
    // Note: broker_id is u64, so it's always >= 0
    // No need to check for negative values

    Ok(())
}

#[cfg(feature = "tieredstore")]
fn validate_tieredstore_config(message_store_config: &MessageStoreConfig) -> Result<()> {
    let Some(tiered_store_config) = message_store_config.tiered_store_config.as_ref() else {
        return Ok(());
    };
    if !tiered_store_config.storage_level.enabled() {
        return Ok(());
    }
    if message_store_config.store_type != StoreType::LocalFile {
        anyhow::bail!(
            "tieredstore currently requires storeType=LocalFile, actual storeType={}",
            message_store_config.store_type.get_store_type()
        );
    }
    Ok(())
}

#[cfg(not(feature = "tieredstore"))]
fn validate_tieredstore_config(_message_store_config: &MessageStoreConfig) -> Result<()> {
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
    #[cfg(feature = "tieredstore")]
    print_important_tieredstore_config(config);
}

#[cfg(feature = "tieredstore")]
fn print_important_tieredstore_config(config: &MessageStoreConfig) {
    let Some(tiered_store_config) = config.tiered_store_config.as_ref() else {
        println!("    tieredStoreEnable: false");
        return;
    };

    println!("    tieredStoreEnable: {}", tiered_store_config.storage_level.enabled());
    println!("    tieredStorageLevel: {:?}", tiered_store_config.storage_level);
    println!("    tieredBackendProvider: {}", tiered_store_config.backend_provider);
    println!(
        "    tieredStorePathRootDir: {}",
        tiered_store_config.store_path_root_dir.display()
    );
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
    #[cfg(feature = "tieredstore")]
    print_all_tieredstore_config(config);
}

#[cfg(feature = "tieredstore")]
fn print_all_tieredstore_config(config: &MessageStoreConfig) {
    let Some(tiered_store_config) = config.tiered_store_config.as_ref() else {
        println!("    tieredStoreConfig: None");
        return;
    };

    println!("    tieredStoreConfig:");
    println!("      storageLevel: {:?}", tiered_store_config.storage_level);
    println!("      backendProvider: {}", tiered_store_config.backend_provider);
    println!("      metadataProvider: {}", tiered_store_config.metadata_provider);
    println!(
        "      storePathRootDir: {}",
        tiered_store_config.store_path_root_dir.display()
    );
    println!(
        "      commitLogSegmentSize: {}",
        tiered_store_config.commit_log_segment_size
    );
    println!(
        "      consumeQueueSegmentSize: {}",
        tiered_store_config.consume_queue_segment_size
    );
    println!(
        "      indexFileMaxHashSlotNum: {}",
        tiered_store_config.index_file_max_hash_slot_num
    );
    println!(
        "      indexFileMaxIndexNum: {}",
        tiered_store_config.index_file_max_index_num
    );
    println!("      messageIndexEnable: {}", tiered_store_config.message_index_enable);
    println!("      deleteFileEnable: {}", tiered_store_config.delete_file_enable);
    println!("      groupCommit: {}", tiered_store_config.group_commit);
    println!("      maxPendingTasks: {}", tiered_store_config.max_pending_tasks);
    println!(
        "      readAheadCacheEnable: {}",
        tiered_store_config.read_ahead_cache_enable
    );
    println!("      crcCheckEnable: {}", tiered_store_config.crc_check_enable);
}

/// Print broker startup information
fn print_startup_info(broker_config: &BrokerConfig, message_store_config: &MessageStoreConfig) {
    #[cfg(not(feature = "tieredstore"))]
    let _ = message_store_config;

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

    #[cfg(feature = "tieredstore")]
    print_tieredstore_startup_info(message_store_config);
}

#[cfg(feature = "tieredstore")]
fn print_tieredstore_startup_info(message_store_config: &MessageStoreConfig) {
    match message_store_config.tiered_store_config.as_ref() {
        Some(config) if config.storage_level.enabled() => {
            info!(
                "Tieredstore enabled: storageLevel={:?}, backendProvider={}, storePathRootDir={}",
                config.storage_level,
                config.backend_provider,
                config.store_path_root_dir.display()
            );
        }
        Some(config) => {
            info!("Tieredstore disabled by storageLevel={:?}", config.storage_level);
        }
        None => {
            info!("Tieredstore disabled: tieredStoreConfig is not configured");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn security_bootstrap_precedes_broker_listener_bind() {
        let security = SecurityBootstrapConfig::new(SecurityBootstrapProfile::DevelopmentInsecureLoopback);
        let mut broker = BrokerConfig {
            broker_server_config: ServerConfig {
                bind_address: "127.0.0.1".to_string(),
                listen_port: 10911,
                ..ServerConfig::default()
            },
            ..BrokerConfig::default()
        };
        let mut store = MessageStoreConfig {
            ha_listen_address: IpAddr::from([127, 0, 0, 1]),
            ..MessageStoreConfig::default()
        };

        validate_broker_security(
            &security,
            &broker,
            &store,
            Some(SocketAddr::from(([127, 0, 0, 1], 8088))),
        )
        .expect("loopback-only Broker bootstrap should pass");

        store.ha_listen_address = IpAddr::from([0, 0, 0, 0]);
        assert!(validate_broker_security(&security, &broker, &store, None).is_err());
        store.ha_listen_address = IpAddr::from([127, 0, 0, 1]);

        broker.metrics_exporter_type = rocketmq_common::common::metrics::MetricsExporterType::Prom;
        broker.metrics_prom_exporter_host = "0.0.0.0".into();
        assert!(validate_broker_security(&security, &broker, &store, None).is_err());
        broker.metrics_exporter_type = rocketmq_common::common::metrics::MetricsExporterType::Disable;

        broker.broker_server_config.bind_address = "0.0.0.0".to_string();
        assert!(validate_broker_security(&security, &broker, &store, None).is_err());
    }

    #[cfg(feature = "tieredstore")]
    fn tieredstore_message_store_config(store_type: StoreType) -> MessageStoreConfig {
        let mut config: MessageStoreConfig = serde_json::from_value(serde_json::json!({
            "storePathRootDir": "target/tieredstore-broker-config-test",
            "tieredStoreConfig": {
                "storageLevel": "force",
                "backendProvider": "memory",
                "metadataProvider": "json",
                "storePathRootDir": "target/tieredstore-broker-config-test/tiered"
            }
        }))
        .expect("deserialize tieredstore message store config");
        config.store_type = store_type;
        config
    }

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

    #[cfg(feature = "tieredstore")]
    #[test]
    fn validate_broker_config_accepts_tieredstore_with_local_file_store() {
        let broker_config = BrokerConfig::default();
        let message_store_config = tieredstore_message_store_config(StoreType::LocalFile);

        assert!(validate_broker_config(&broker_config, &message_store_config).is_ok());
    }

    #[cfg(feature = "tieredstore")]
    #[test]
    fn validate_broker_config_rejects_tieredstore_with_rocksdb_store() {
        let broker_config = BrokerConfig::default();
        let message_store_config = tieredstore_message_store_config(StoreType::RocksDB);

        let error = validate_broker_config(&broker_config, &message_store_config)
            .expect_err("tieredstore should reject non-local-file message stores");
        assert!(error.to_string().contains("storeType=LocalFile"));
    }

    #[cfg(not(feature = "tieredstore"))]
    #[test]
    fn validate_broker_config_keeps_default_path_without_tieredstore_feature() {
        let broker_config = BrokerConfig::default();
        let message_store_config = MessageStoreConfig::default();

        assert!(validate_broker_config(&broker_config, &message_store_config).is_ok());
    }

    #[test]
    fn parse_config_file_reads_camel_case_broker_and_store_paths() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_file = temp_dir.path().join("broker.toml");
        let store_root = temp_dir.path().join("store");
        let commit_log = store_root.join("commitlog");
        let store_root = store_root.to_string_lossy().replace('\\', "/");
        let commit_log = commit_log.to_string_lossy().replace('\\', "/");
        let content = format!(
            r#"namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
listenPort = 11911
storePathRootDir = "{}"
storePathCommitLog = "{}"
enableControllerMode = false
tls.enable = true
tls.server.mode = "enforcing"
tls.server.certPath = "/certs/server.pem"

[brokerServerConfig]
listenPort = 11911
bindAddress = "0.0.0.0"

[brokerIdentity]
brokerName = "rust-local-broker"
brokerClusterName = "DefaultCluster"
brokerId = 0
"#,
            store_root, commit_log
        );
        std::fs::write(&config_file, content).expect("write broker config");

        let args = Args {
            config_file: Some(config_file),
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
            log_filter: None,
        };

        let (broker_config, message_store_config, _logging_overrides) =
            parse_config_file(&args).expect("parse broker config");

        assert_eq!(broker_config.broker_identity.broker_name.as_str(), "rust-local-broker");
        assert_eq!(broker_config.listen_port, 11911);
        assert_eq!(broker_config.broker_server_config.listen_port, 11911);
        assert!(broker_config.broker_server_config.tls_config.enable);
        assert_eq!(
            broker_config.broker_server_config.tls_config.server.mode,
            rocketmq_common::common::tls_config::TlsMode::Enforcing
        );
        assert_eq!(
            broker_config
                .broker_server_config
                .tls_config
                .server
                .cert_path
                .as_deref(),
            Some("/certs/server.pem")
        );
        assert_eq!(broker_config.store_path_root_dir.as_str(), store_root);
        assert_eq!(message_store_config.store_path_root_dir.as_str(), store_root);
        assert_eq!(
            message_store_config
                .store_path_commit_log
                .as_ref()
                .map(|path| path.as_str()),
            Some(commit_log.as_str())
        );
    }
}
