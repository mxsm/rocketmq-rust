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

#![recursion_limit = "512"]

use std::future::Future;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use rocketmq_broker::build_broker_telemetry_bootstrap_config;
use rocketmq_broker::command::Args;
use rocketmq_broker::command::ConfigFileFormat;
use rocketmq_broker::config::broker_config::BrokerConfig;
use rocketmq_broker::config::java_properties::JavaBrokerProperties;
use rocketmq_broker::config::raw::RawBrokerConfig;
use rocketmq_broker::config::validated::ValidatedBrokerConfig;
use rocketmq_broker::Builder;
use rocketmq_model::common::mq_version::CURRENT_VERSION;
use rocketmq_model::utils::env_utils::EnvUtils;
use rocketmq_protocol::protocol::remoting_command_facade::initialize_remoting_defaults;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ShutdownReason;
use rocketmq_security_api::SecurityBootstrap;
use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapOutcome;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_store::MessageStoreConfig;
#[cfg(test)]
use rocketmq_transport::api::ServerConfig;
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

fn print_release_version_if_requested(component: &str) -> bool {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    let version = std::ffi::OsStr::new("--version");
    let verbose = std::ffi::OsStr::new("--verbose");
    let requested = (arguments.len() == 1 && arguments[0].as_os_str() == version)
        || (arguments.len() == 2 && arguments[0].as_os_str() == version && arguments[1].as_os_str() == verbose);
    if !requested {
        return false;
    }
    println!("{component}");
    println!("version={}", env!("CARGO_PKG_VERSION"));
    if arguments.len() == 2 {
        println!(
            "artifact_id={}",
            option_env!("ROCKETMQ_RELEASE_ARTIFACT_ID").unwrap_or("development")
        );
        println!(
            "requested_features={}",
            option_env!("ROCKETMQ_RELEASE_REQUESTED_FEATURES").unwrap_or("default")
        );
        println!(
            "effective_features={}",
            option_env!("ROCKETMQ_RELEASE_EFFECTIVE_FEATURES").unwrap_or("default")
        );
    }
    true
}

fn main() -> Result<()> {
    if print_release_version_if_requested("rocketmq-broker-rust") {
        return Ok(());
    }
    let owner = RuntimeOwner::new(broker_runtime_config()).context("failed to build broker runtime")?;
    let service_context = owner.root_context().component("rocketmq-broker-runtime");
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
    RuntimeConfig::broker_default()
}

fn run(
    service_context: ChildServiceContext,
    lifecycle: ServiceLifecycle,
) -> Pin<Box<impl Future<Output = Result<()>>>> {
    Box::pin(run_inner(service_context, lifecycle))
}

async fn run_inner(service_context: ChildServiceContext, lifecycle: ServiceLifecycle) -> Result<()> {
    initialize_remoting_defaults(CURRENT_VERSION as i32)
        .context("failed to initialize the immutable broker remoting defaults")?;

    // Parse and validate command line arguments
    let args = Args::parse();
    args.validate().context("invalid broker arguments")?;

    // Parse configuration from file and command line
    let mut raw_config = parse_config_file(&args).context("failed to parse broker configuration")?;

    // Override config with command line arguments
    apply_command_line_args(&mut raw_config, &args);

    let validated_config = ValidatedBrokerConfig::try_from(raw_config).context("invalid broker configuration")?;
    let broker_config = validated_config.broker();
    let message_store_config = validated_config.store();
    let logging_overrides = validated_config.logging();

    verify_rocketmq_home()?;

    // Handle print config and exit without creating telemetry or service listeners.
    if args.should_exit_after_print() {
        print_config(broker_config, message_store_config, args.print_important_config);
        return Ok(());
    }

    let mut bootstrap_config = build_broker_telemetry_bootstrap_config(broker_config);
    bootstrap_config.logging.reload = logging_overrides.logging.reload;
    let telemetry_resolution = rocketmq_observability::resolve_telemetry_from_env(
        "rocketmq-broker",
        bootstrap_config,
        validated_config.observability(),
        rocketmq_observability::TelemetryEnvironmentSpec {
            trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
        },
    )
    .context("failed to resolve Broker telemetry configuration")?;
    let process_telemetry = telemetry_resolution.process;
    let bootstrap_config = telemetry_resolution.bootstrap;

    let security_bootstrap =
        SecurityBootstrapConfig::from_env().context("failed to load broker security bootstrap configuration")?;
    let validated_security = validate_broker_security(
        &security_bootstrap,
        broker_config,
        message_store_config,
        &bootstrap_config.observability,
        lifecycle.config().probe_bind_addr,
    )
    .context("broker security bootstrap failed before listener bind")?;

    let environment_filter = rocketmq_observability::read_rust_log().context("failed to read RUST_LOG")?;
    let resolved_filter = resolve_startup_log_filter(&args, logging_overrides, environment_filter.as_deref())
        .context("failed to resolve broker log filter")?;
    let telemetry_guard = rocketmq_observability::install_global_with_filter_and_service_context(
        &bootstrap_config,
        resolved_filter.clone(),
        &service_context,
    )
    .await
    .context("failed to initialize broker telemetry bootstrap")?;
    register_broker_release_identity(&process_telemetry, &telemetry_guard.handle())?;
    log_telemetry_bootstrap(
        &bootstrap_config,
        &resolved_filter,
        telemetry_guard.subscriber_install_status(),
    );
    log_security_bootstrap(validated_security);

    // Print logo
    println!("{}", LOGO);

    // Print startup info
    print_startup_info(broker_config, message_store_config);
    if let Err(error) = lifecycle.start(&service_context).await {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        if let Err(shutdown_error) = telemetry_guard
            .shutdown_with_service_context(&service_context, request.deadline.remaining())
            .await
            .into_result()
        {
            tracing::warn!(error = %shutdown_error, "broker telemetry cleanup after lifecycle startup failure was unhealthy");
        }
        return Err(error).context("failed to start broker lifecycle boundary");
    }
    if let Err(error) = rocketmq_observability::start_runtime_diagnostics_endpoint_from_env_with_telemetry(
        &service_context,
        RuntimeComponent::Broker,
        &telemetry_guard.handle(),
    )
    .await
    {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        if let Err(shutdown_error) = telemetry_guard
            .shutdown_with_service_context(&service_context, request.deadline.remaining())
            .await
            .into_result()
        {
            tracing::warn!(error = %shutdown_error, "broker telemetry cleanup after diagnostics startup failure was unhealthy");
        }
        return Err(error).context("failed to start protected broker runtime diagnostics");
    }

    // Start broker
    Builder::new(service_context, telemetry_guard)
        .with_validated_config(validated_config)
        .require_release_identity_registration(process_telemetry.metrics_enabled())
        .build()
        .boot_with_lifecycle(lifecycle)
        .await
        .context("broker lifecycle failed")?;

    Ok(())
}

fn validate_broker_security(
    security_bootstrap: &SecurityBootstrap,
    broker_config: &BrokerConfig,
    message_store_config: &MessageStoreConfig,
    observability_config: &rocketmq_observability::ObservabilityConfig,
    probe_bind_addr: Option<SocketAddr>,
) -> Result<SecurityBootstrapOutcome> {
    if !security_bootstrap.is_enabled() {
        return security_bootstrap.validate(&[]).map_err(anyhow::Error::from);
    }
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
    if observability_config.metrics.enabled
        && observability_config.metrics.exporter == rocketmq_observability::MetricsExporter::Prometheus
    {
        let metrics_addr = format!(
            "{}:{}",
            observability_config.prometheus.host, observability_config.prometheus.port
        )
        .parse::<SocketAddr>()
        .context("broker Prometheus listener must be an IP socket address")?;
        listeners.push(metrics_addr);
    }
    if let Some(probe_bind_addr) = probe_bind_addr {
        listeners.push(probe_bind_addr);
    }
    security_bootstrap.validate(&listeners).map_err(anyhow::Error::from)
}

fn register_broker_release_identity(
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
    telemetry_handle: &rocketmq_observability::TelemetryHandle,
) -> Result<()> {
    if !process_telemetry.metrics_enabled() {
        return Ok(());
    }

    #[cfg(feature = "otel-metrics")]
    {
        telemetry_handle
            .register_release_identity(process_telemetry.release_identity().clone())
            .context("failed to register Broker release identity before readiness")?;
        if !telemetry_handle.release_identity_registered() {
            anyhow::bail!("Broker release identity was not registered before readiness");
        }
        Ok(())
    }

    #[cfg(not(feature = "otel-metrics"))]
    {
        let _ = telemetry_handle;
        anyhow::bail!("Broker metrics were enabled without the `otel-metrics` Cargo feature");
    }
}

fn log_security_bootstrap(outcome: SecurityBootstrapOutcome) {
    match outcome {
        SecurityBootstrapOutcome::Disabled => {
            warn!("broker security bootstrap is disabled because no security profile is configured")
        }
        SecurityBootstrapOutcome::Validated(validated) => match validated.profile() {
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
        },
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
fn parse_config_file(args: &Args) -> Result<RawBrokerConfig> {
    if let Some(config_file) = args.get_config_file() {
        info!("Loading configuration from: {}", config_file.display());
        match resolve_config_format(args, &config_file)? {
            ConfigFileFormat::Toml => RawBrokerConfig::load(&config_file)
                .with_context(|| format!("Failed to parse canonical broker configuration from {:?}", config_file)),
            ConfigFileFormat::Properties => {
                let input = std::fs::read_to_string(&config_file)
                    .with_context(|| format!("Failed to read Java broker properties from {:?}", config_file))?;
                let conversion = JavaBrokerProperties::parse(&input)
                    .with_context(|| format!("Failed to convert Java broker properties from {:?}", config_file))?;
                let report_path = args
                    .conversion_report
                    .clone()
                    .unwrap_or_else(|| config_file.with_extension("conversion.json"));
                let report = conversion
                    .report_json()
                    .context("Failed to serialize Java broker properties conversion report")?;
                std::fs::write(&report_path, report)
                    .with_context(|| format!("Failed to write Java conversion report to {:?}", report_path))?;
                Ok(conversion.into_config())
            }
        }
    } else {
        info!("Using default configuration (no config file specified)");
        Ok(RawBrokerConfig::default())
    }
}

fn resolve_config_format(args: &Args, path: &std::path::Path) -> Result<ConfigFileFormat> {
    if let Some(format) = args.config_format {
        return Ok(format);
    }
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some(extension) if extension.eq_ignore_ascii_case("toml") => Ok(ConfigFileFormat::Toml),
        Some(extension) if extension.eq_ignore_ascii_case("conf") || extension.eq_ignore_ascii_case("properties") => {
            Ok(ConfigFileFormat::Properties)
        }
        _ => anyhow::bail!(
            "configuration format is ambiguous for {}; use --config-format toml|properties",
            path.display()
        ),
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

/// Apply command line arguments to broker configuration
///
/// Command line arguments have highest priority and override config file values
fn apply_command_line_args(raw_config: &mut RawBrokerConfig, args: &Args) {
    // Apply name server address only if explicitly provided via command line or env
    // Otherwise, keep the value from config file
    if args.namesrv_addr.is_some() || std::env::var("NAMESRV_ADDR").is_ok() {
        let namesrv_addr = args.get_namesrv_addr();
        raw_config.set_name_server_addresses(namesrv_addr);
        info!(
            "Name server address (from command line/env): {}",
            raw_config.broker().namesrv_addr.as_ref().unwrap()
        );
    } else if let Some(ref addr) = raw_config.broker().namesrv_addr {
        info!("Name server address (from config file): {}", addr);
    } else {
        // Use default if not set anywhere
        raw_config.set_name_server_addresses("127.0.0.1:9876");
        info!(
            "Name server address (default): {}",
            raw_config.broker().namesrv_addr.as_ref().unwrap()
        );
    }
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
    println!("    flushCommitLogLeastPages: {}", config.flush_commit_log_least_pages);
    println!(
        "    flushConsumeQueueLeastPages: {}",
        config.flush_consume_queue_least_pages
    );
    println!("    slaveTimeout: {}", config.slave_timeout);
    println!("    minInSyncReplicas: {}", config.min_in_sync_replicas);
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
    let mut properties = config.get_properties().into_iter().collect::<Vec<_>>();
    properties.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    for (key, value) in properties {
        println!("    {}: {}", key, value);
    }
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

    info!("Broker listening on: {}", broker_config.get_broker_addr());

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
    fn disabled_security_bootstrap_allows_default_broker_listeners() {
        let broker = BrokerConfig::default();
        let observability = build_broker_telemetry_bootstrap_config(&broker).observability;
        let outcome = validate_broker_security(
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            &broker,
            &MessageStoreConfig::default(),
            &observability,
            None,
        )
        .expect("disabled security bootstrap should not restrict Broker listeners");

        assert_eq!(outcome, rocketmq_security_api::SecurityBootstrapOutcome::Disabled);
    }

    #[test]
    fn security_bootstrap_precedes_broker_listener_bind() {
        let security = rocketmq_security_api::SecurityBootstrap::Enabled(SecurityBootstrapConfig::new(
            SecurityBootstrapProfile::DevelopmentInsecureLoopback,
        ));
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
        let observability = build_broker_telemetry_bootstrap_config(&broker).observability;

        validate_broker_security(
            &security,
            &broker,
            &store,
            &observability,
            Some(SocketAddr::from(([127, 0, 0, 1], 8088))),
        )
        .expect("loopback-only Broker bootstrap should pass");

        store.ha_listen_address = IpAddr::from([0, 0, 0, 0]);
        assert!(validate_broker_security(&security, &broker, &store, &observability, None).is_err());
        store.ha_listen_address = IpAddr::from([127, 0, 0, 1]);

        let prometheus_overrides = rocketmq_observability::ObservabilityOverrides {
            metrics: rocketmq_observability::MetricsOverrides {
                exporter: Some(rocketmq_observability::MetricsExporter::Prometheus),
                ..Default::default()
            },
            prometheus: rocketmq_observability::PrometheusOverrides {
                host: Some("0.0.0.0".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let prometheus_observability =
            rocketmq_broker::build_broker_telemetry_bootstrap_config_with_overrides(&broker, &prometheus_overrides)
                .observability;
        assert!(validate_broker_security(&security, &broker, &store, &prometheus_observability, None).is_err());

        broker.broker_server_config.bind_address = "0.0.0.0".to_string();
        assert!(validate_broker_security(&security, &broker, &store, &observability, None).is_err());
    }

    #[test]
    fn broker_release_identity_config_is_applied_before_bootstrap() {
        let environment = rocketmq_observability::TelemetryEnvironmentValues {
            release_commit: Some("0123456789abcdef0123456789abcdef01234567".into()),
            release_nonce: Some("rollout-07".into()),
            metrics_enabled: Some("true".into()),
            metrics_exporter: Some("prometheus".into()),
            metrics_bind_addr: Some("127.0.0.1:5557".into()),
            metrics_path: Some("/metrics".into()),
            ..Default::default()
        };
        let resolution = rocketmq_observability::resolve_telemetry_values(
            "rocketmq-broker",
            build_broker_telemetry_bootstrap_config(&BrokerConfig::default()),
            &rocketmq_observability::ObservabilityOverrides::default(),
            &environment,
            rocketmq_observability::TelemetryEnvironmentSpec {
                trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
            },
        )
        .expect("valid Broker process telemetry");
        let process_telemetry = resolution.process;
        let bootstrap_config = resolution.bootstrap;

        assert_eq!(process_telemetry.release_identity().service(), "rocketmq-broker");
        assert!(bootstrap_config.observability.enabled);
        assert!(bootstrap_config.observability.metrics.enabled);
        assert_eq!(
            bootstrap_config.observability.metrics.exporter,
            rocketmq_observability::MetricsExporter::Prometheus
        );
        assert_eq!(bootstrap_config.observability.prometheus.host, "127.0.0.1");
        assert_eq!(bootstrap_config.observability.prometheus.port, 5557);
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
            r#"[broker]
namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
listenPort = 11911
storePathRootDir = "{}"
enableControllerMode = false

[broker.brokerServerConfig]
bindAddress = "0.0.0.0"

[broker.brokerServerConfig.tlsConfig]
enable = true
testModeEnable = true

[broker.brokerServerConfig.tlsConfig.server]
mode = "enforcing"
certPath = "/certs/server.pem"

[broker.brokerIdentity]
brokerName = "rust-local-broker"
brokerClusterName = "DefaultCluster"
brokerId = 0

[store]
storePathRootDir = "{}"
storePathCommitLog = "{}"
"#,
            store_root, store_root, commit_log
        );
        std::fs::write(&config_file, content).expect("write broker config");

        let args = Args {
            config_file: Some(config_file),
            config_format: None,
            conversion_report: None,
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
            log_filter: None,
        };

        let raw_config = parse_config_file(&args).expect("parse broker config");
        let validated_config =
            ValidatedBrokerConfig::try_from(raw_config).expect("canonical broker config should validate");
        let broker_config = validated_config.broker();
        let message_store_config = validated_config.store();

        assert_eq!(broker_config.broker_identity.broker_name.as_str(), "rust-local-broker");
        assert_eq!(broker_config.listen_port, 11911);
        assert_eq!(broker_config.broker_server_config.listen_port, 11911);
        assert!(broker_config.broker_server_config.tls_config.enable);
        assert_eq!(
            broker_config.broker_server_config.tls_config.server.mode,
            rocketmq_transport::api::TlsMode::Enforcing
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

    #[test]
    fn parse_config_file_converts_java_properties_and_writes_one_report() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_file = temp_dir.path().join("broker.conf");
        let report_file = temp_dir.path().join("conversion.json");
        std::fs::write(
            &config_file,
            "brokerName=converted-broker\nstoreType=DEFAULTROCKSDB\nmaxMessageSize=0\n",
        )
        .expect("write Java properties");
        let args = Args {
            config_file: Some(config_file),
            config_format: None,
            conversion_report: Some(report_file.clone()),
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
            log_filter: None,
        };

        let raw = parse_config_file(&args).expect("convert Java properties");
        assert_eq!(raw.broker().broker_identity.broker_name, "converted-broker");
        assert_eq!(raw.store().store_type, rocketmq_store::StoreType::RocksDB);
        assert_eq!(raw.store().max_message_size, 0);
        let report = std::fs::read_to_string(report_file).expect("read conversion report");
        assert!(report.contains("brokerName"));
        assert!(report.contains("store.storeType"));
    }

    #[test]
    fn conversion_report_failure_prevents_configuration_startup() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_file = temp_dir.path().join("broker.properties");
        let blocked_report = temp_dir.path().join("report-directory");
        std::fs::write(&config_file, "brokerName=blocked-report\n").expect("write Java properties");
        std::fs::create_dir(&blocked_report).expect("create blocking report directory");
        let args = Args {
            config_file: Some(config_file),
            config_format: Some(ConfigFileFormat::Properties),
            conversion_report: Some(blocked_report),
            print_config_item: false,
            print_important_config: false,
            namesrv_addr: None,
            log_filter: None,
        };

        assert!(parse_config_file(&args).is_err());
    }
}
