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

use std::net::IpAddr;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use config::Config;
use rocketmq_common::common::controller::controller_config::RaftPeer;
use rocketmq_common::common::controller::controller_config::StorageBackendType;
use rocketmq_common::common::metrics::MetricsExporterType;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use rocketmq_common::common::server::config::ServerConfig;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::ParseConfigFile;
use rocketmq_controller::resolve_controller_raft_bind_addr;
use rocketmq_controller::ControllerCli;
use rocketmq_controller::ControllerConfig;
use rocketmq_namesrv::bootstrap::Builder;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownReason;
use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapProfile;
use rocketmq_security_api::ValidatedSecurityBootstrap;
use serde::Deserialize;
use tracing::info;

const LOGO: &str = r#"
      _____            _        _   __  __  ____         _____           _     _   _                         _____
     |  __ \          | |      | | |  \/  |/ __ \       |  __ \         | |   | \ | |                       / ____|
     | |__) |___   ___| | _____| |_| \  / | |  | |______| |__) |   _ ___| |_  |  \| | __ _ _ __ ___   ___  | (___   ___ _ ____   _____ _ __
     |  _  // _ \ / __| |/ / _ \ __| |\/| | |  | |______|  _  / | | / __| __| | . ` |/ _` | '_ ` _ \ / _ \  \___ \ / _ \ '__\ \ / / _ \ '__|
     | | \ \ (_) | (__|   <  __/ |_| |  | | |__| |      | | \ \ |_| \__ \ |_  | |\  | (_| | | | | | |  __/  ____) |  __/ |   \ V /  __/ |
     |_|  \_\___/ \___|_|\_\___|\__|_|  |_|\___\_\      |_|  \_\__,_|___/\__| |_| \_|\__,_|_| |_| |_|\___| |_____/ \___|_|    \_/ \___|_|
    "#;

const ENTRYPOINT_MAX_BLOCKING_THREADS: usize = 64;

fn main() -> Result<()> {
    let owner = RuntimeOwner::new(namesrv_runtime_config()).context("failed to build namesrv runtime")?;
    let service_context = owner.context().service_context("rocketmq-namesrv-runtime");
    let lifecycle =
        ServiceLifecycle::from_env("rocketmq-namesrv").context("invalid NameServer lifecycle configuration")?;

    let run_result = owner.block_on(run(service_context, lifecycle.clone()));
    if run_result.is_err() {
        lifecycle.mark_failed();
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_result = owner
        .shutdown_runtime_blocking_until(shutdown_request.deadline)
        .context("failed to shutdown namesrv runtime");

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(report)) => {
            if !report.is_healthy() {
                lifecycle.mark_failed();
                tracing::warn!(
                    report = %report.to_json(),
                    "namesrv runtime shutdown report is unhealthy"
                );
                bail!("NameServer runtime shutdown report is unhealthy");
            }
            Ok(())
        }
    }
}

fn namesrv_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::namesrv_default();
    config.max_blocking_threads = ENTRYPOINT_MAX_BLOCKING_THREADS;
    config
}

async fn run(service_context: ServiceContext, lifecycle: ServiceLifecycle) -> Result<()> {
    // Parse command line arguments first
    let args = Args::parse();

    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );

    // Parse and merge configurations
    let (namesrv_config, server_config, controller_config, logging_overrides) =
        parse_and_merge_config(&args).context("failed to parse namesrv configuration")?;

    // Handle print config item mode
    if args.print_config_item {
        print_config(&namesrv_config, &server_config, controller_config.as_ref());
        return Ok(());
    }

    // Validate ROCKETMQ_HOME is set
    if namesrv_config.rocketmq_home.is_empty() {
        bail!(
            "Please set the ROCKETMQ_HOME variable in your environment to match the location of the RocketMQ \
             installation"
        );
    }

    let security_config =
        SecurityBootstrapConfig::from_env().context("failed to load NameServer security bootstrap configuration")?;
    let controller_raft_bind_addr = controller_config
        .as_ref()
        .map(|config| resolve_controller_raft_bind_addr(config.local_raft_addr()))
        .transpose()
        .context("failed to resolve embedded Controller Raft listener address")?;
    let validated_security = validate_namesrv_security(
        &security_config,
        &server_config,
        controller_config.as_ref(),
        controller_raft_bind_addr,
        lifecycle.config().probe_bind_addr,
    )
    .context("NameServer security bootstrap failed before listener bind")?;

    let environment_filter = rocketmq_observability::read_rust_log().context("failed to read RUST_LOG")?;
    let resolved_filter = resolve_startup_log_filter(&args, &logging_overrides, environment_filter.as_deref())
        .context("failed to resolve namesrv log filter")?;
    let mut bootstrap_config = build_namesrv_telemetry_bootstrap_config(&namesrv_config);
    bootstrap_config.logging.reload = logging_overrides.logging.reload;
    let telemetry_guard =
        rocketmq_observability::install_global_with_filter(&bootstrap_config, resolved_filter.clone())
            .context("failed to initialize namesrv telemetry bootstrap")?;
    log_telemetry_bootstrap(
        &bootstrap_config,
        &resolved_filter,
        telemetry_guard.subscriber_install_status(),
    );
    log_security_bootstrap(validated_security);

    if let Err(error) = lifecycle.start(&service_context).await {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        if let Err(shutdown_error) = telemetry_guard
            .shutdown_with_timeout(request.deadline.remaining())
            .into_result()
        {
            tracing::warn!(error = %shutdown_error, "namesrv telemetry cleanup after lifecycle startup failure was unhealthy");
        }
        return Err(error).context("failed to start NameServer lifecycle boundary");
    }

    println!("{}", LOGO);

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
    let boot_result = Builder::new()
        .set_name_server_config(namesrv_config)
        .set_server_config(server_config)
        .set_controller_config_opt(controller_config)
        .set_service_context(service_context)
        .build()
        .boot_with_lifecycle(lifecycle.clone())
        .await
        .map_err(anyhow::Error::from);
    if boot_result.is_err() {
        lifecycle.mark_failed();
        lifecycle.request_shutdown(ShutdownReason::Internal);
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let telemetry_report = telemetry_guard.shutdown_with_timeout(shutdown_request.deadline.remaining());
    let shutdown_result = telemetry_report
        .into_result()
        .context("failed to shutdown namesrv telemetry bootstrap");

    match (boot_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(_report), Err(error)) => Err(error),
        (Ok(report), Ok(_telemetry_report)) if !report.is_healthy() => {
            bail!("NameServer shutdown did not complete within the shared lifecycle deadline")
        }
        (Ok(_report), Ok(_telemetry_report)) if lifecycle.state() == ServiceLifecycleState::Failed => {
            bail!("NameServer lifecycle failed while observing or completing shutdown")
        }
        (Ok(_report), Ok(_telemetry_report)) => Ok(()),
    }
}

fn validate_namesrv_security(
    security_config: &SecurityBootstrapConfig,
    server_config: &ServerConfig,
    controller_config: Option<&ControllerConfig>,
    controller_raft_bind_addr: Option<SocketAddr>,
    probe_bind_addr: Option<SocketAddr>,
) -> Result<ValidatedSecurityBootstrap> {
    let bind_ip = server_config
        .bind_address
        .parse::<IpAddr>()
        .context("NameServer bindAddress must be an IP address")?;
    let listen_port = u16::try_from(server_config.listen_port).context("NameServer listenPort must fit a TCP port")?;
    let mut listeners = vec![SocketAddr::new(bind_ip, listen_port)];
    if let Some(controller_config) = controller_config {
        listeners.push(controller_config.listen_addr);
        listeners
            .push(controller_raft_bind_addr.context("embedded Controller Raft listener address must be resolved")?);
    }
    if let Some(probe_bind_addr) = probe_bind_addr {
        listeners.push(probe_bind_addr);
    }
    security_config.validate(&listeners).map_err(anyhow::Error::from)
}

fn log_security_bootstrap(validated: ValidatedSecurityBootstrap) {
    match validated.profile() {
        SecurityBootstrapProfile::DevelopmentInsecureLoopback => tracing::warn!(
            profile = validated.profile().as_str(),
            listener_count = validated.listener_count(),
            "NameServer development-insecure security profile is active; every listener is restricted to loopback"
        ),
        SecurityBootstrapProfile::SecureEnforced => info!(
            profile = validated.profile().as_str(),
            listener_count = validated.listener_count(),
            "NameServer secure bootstrap completed before listener bind"
        ),
    }
}

fn build_namesrv_telemetry_bootstrap_config(
    namesrv_config: &NamesrvConfig,
) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut observability = rocketmq_observability::ObservabilityConfig {
        service_name: "rocketmq-namesrv".to_string(),
        service_namespace: "rocketmq".to_string(),
        node_type: "namesrv".to_string(),
        node_id: format!("{}:{}", "namesrv", namesrv_config.rocketmq_home),
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;

    let mut logging = rocketmq_observability::LoggingConfig::default();
    logging.file.directory = service_log_directory(namesrv_config.rocketmq_home.as_str());
    logging.file.file_name_prefix = "rocketmq-namesrv".to_string();

    rocketmq_observability::TelemetryBootstrapConfig { observability, logging }
}

fn service_log_directory(rocketmq_home: &str) -> String {
    if rocketmq_home.trim().is_empty() {
        return "logs".to_string();
    }
    PathBuf::from(rocketmq_home).join("logs").to_string_lossy().into_owned()
}

fn log_telemetry_bootstrap(
    config: &rocketmq_observability::TelemetryBootstrapConfig,
    resolved_filter: &rocketmq_observability::ResolvedLogFilter,
    subscriber_install_status: rocketmq_observability::SubscriberInstallStatus,
) {
    info!(
        service = "rocketmq-namesrv",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        metrics_exporter = ?config.observability.metrics.exporter,
        trace_exporter = ?config.observability.traces.exporter,
        log_exporter = ?config.observability.logs.exporter,
        subscriber_installed = subscriber_install_status.installed,
        reload_enabled = config.logging.reload.enabled,
        file_log_enabled = config.logging.file.enabled,
        "namesrv telemetry bootstrap initialized"
    );
}

/// Parse configuration file and merge with command line arguments
/// Command line arguments take precedence over config file settings
fn parse_and_merge_config(
    args: &Args,
) -> Result<(
    NamesrvConfig,
    ServerConfig,
    Option<ControllerConfig>,
    rocketmq_observability::LoggingOverrides,
)> {
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

    let mut server_config = ServerConfig {
        listen_port: args.listen_port.unwrap_or(9876),
        bind_address: args.bind_address.clone().unwrap_or_else(|| "0.0.0.0".to_string()),
        ..ServerConfig::default()
    };
    if let Some(config_file) = args.config_file.clone() {
        apply_tls_properties_from_file(&mut server_config, config_file)?;
    }

    let controller_config = if namesrv_config.enable_controller_in_namesrv {
        Some(load_controller_config(args.config_file.clone(), &namesrv_config)?)
    } else {
        None
    };

    let logging_overrides = match args.config_file.clone() {
        Some(config_file) => {
            ParseConfigFile::parse_config_file::<rocketmq_observability::LoggingOverrides>(config_file)
                .context("failed to parse namesrv logging configuration")?
        }
        None => rocketmq_observability::LoggingOverrides::default(),
    };

    Ok((namesrv_config, server_config, controller_config, logging_overrides))
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

fn apply_tls_properties_from_file(server_config: &mut ServerConfig, config_file: PathBuf) -> Result<()> {
    let content = std::fs::read_to_string(&config_file)
        .with_context(|| format!("Failed to read TLS properties from {:?}", config_file))?;
    server_config.tls_config.apply_java_properties_str(&content);
    Ok(())
}

/// Print all configuration items
fn print_config(
    namesrv_config: &NamesrvConfig,
    server_config: &ServerConfig,
    controller_config: Option<&ControllerConfig>,
) {
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

    if let Some(controller_config) = controller_config {
        ControllerCli::print_config(controller_config);
    }

    println!("\n===========================================\n");
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControllerConfigOverrides {
    rocketmq_home: Option<String>,
    config_store_path: Option<PathBuf>,
    controller_type: Option<String>,
    scan_not_active_broker_interval: Option<u64>,
    controller_thread_pool_nums: Option<usize>,
    controller_request_thread_pool_queue_capacity: Option<usize>,
    mapped_file_size: Option<usize>,
    controller_store_path: Option<String>,
    elect_master_max_retry_count: Option<u32>,
    enable_elect_unclean_master: Option<bool>,
    is_process_read_event: Option<bool>,
    notify_broker_role_changed: Option<bool>,
    scan_inactive_master_interval: Option<u64>,
    metrics_exporter_type: Option<String>,
    metrics_grpc_exporter_target: Option<String>,
    metrics_grpc_exporter_header: Option<String>,
    metric_grpc_exporter_time_out_in_mills: Option<u64>,
    metric_grpc_exporter_interval_in_mills: Option<u64>,
    metric_logging_exporter_interval_in_mills: Option<u64>,
    metrics_prom_exporter_port: Option<u16>,
    metrics_prom_exporter_host: Option<String>,
    metrics_label: Option<String>,
    metrics_in_delta: Option<bool>,
    config_black_list: Option<String>,
    node_id: Option<u64>,
    listen_addr: Option<SocketAddr>,
    raft_peers: Option<String>,
    controller_peers: Option<String>,
    election_timeout_ms: Option<u64>,
    heartbeat_interval_ms: Option<u64>,
    storage_path: Option<String>,
    storage_backend: Option<String>,
    enable_elect_unclean_master_local: Option<bool>,
}

fn load_controller_config(config_file: Option<PathBuf>, namesrv_config: &NamesrvConfig) -> Result<ControllerConfig> {
    let mut controller_config = ControllerConfig::default().with_rocketmq_home(namesrv_config.rocketmq_home.clone());

    if let Some(config_file) = config_file {
        let cfg = Config::builder()
            .add_source(config::File::from(config_file.as_path()))
            .build()?;
        let overrides = cfg.try_deserialize::<ControllerConfigOverrides>()?;
        apply_controller_config_overrides(&mut controller_config, overrides)?;
    }

    Ok(controller_config)
}

fn apply_controller_config_overrides(
    controller_config: &mut ControllerConfig,
    overrides: ControllerConfigOverrides,
) -> Result<()> {
    if let Some(rocketmq_home) = overrides.rocketmq_home {
        controller_config.rocketmq_home = rocketmq_home;
    }
    if let Some(config_store_path) = overrides.config_store_path {
        controller_config.config_store_path = config_store_path;
    }
    if let Some(controller_type) = overrides.controller_type {
        controller_config.controller_type = controller_type;
    }
    if let Some(scan_not_active_broker_interval) = overrides.scan_not_active_broker_interval {
        controller_config.scan_not_active_broker_interval = scan_not_active_broker_interval;
    }
    if let Some(controller_thread_pool_nums) = overrides.controller_thread_pool_nums {
        controller_config.controller_thread_pool_nums = controller_thread_pool_nums;
    }
    if let Some(controller_request_thread_pool_queue_capacity) = overrides.controller_request_thread_pool_queue_capacity
    {
        controller_config.controller_request_thread_pool_queue_capacity = controller_request_thread_pool_queue_capacity;
    }
    if let Some(mapped_file_size) = overrides.mapped_file_size {
        controller_config.mapped_file_size = mapped_file_size;
    }
    if let Some(controller_store_path) = overrides.controller_store_path {
        controller_config.controller_store_path = controller_store_path;
    }
    if let Some(elect_master_max_retry_count) = overrides.elect_master_max_retry_count {
        controller_config.elect_master_max_retry_count = elect_master_max_retry_count;
    }
    if let Some(enable_elect_unclean_master) = overrides.enable_elect_unclean_master {
        controller_config.enable_elect_unclean_master = enable_elect_unclean_master;
    }
    if let Some(is_process_read_event) = overrides.is_process_read_event {
        controller_config.is_process_read_event = is_process_read_event;
    }
    if let Some(notify_broker_role_changed) = overrides.notify_broker_role_changed {
        controller_config.notify_broker_role_changed = notify_broker_role_changed;
    }
    if let Some(scan_inactive_master_interval) = overrides.scan_inactive_master_interval {
        controller_config.scan_inactive_master_interval = scan_inactive_master_interval;
    }
    if let Some(metrics_exporter_type) = overrides.metrics_exporter_type {
        controller_config.metrics_exporter_type = metrics_exporter_type
            .parse::<MetricsExporterType>()
            .map_err(|_| anyhow::anyhow!("invalid metricsExporterType: {}", metrics_exporter_type))?;
    }
    if let Some(metrics_grpc_exporter_target) = overrides.metrics_grpc_exporter_target {
        controller_config.metrics_grpc_exporter_target = metrics_grpc_exporter_target;
    }
    if let Some(metrics_grpc_exporter_header) = overrides.metrics_grpc_exporter_header {
        controller_config.metrics_grpc_exporter_header = metrics_grpc_exporter_header;
    }
    if let Some(metric_grpc_exporter_time_out_in_mills) = overrides.metric_grpc_exporter_time_out_in_mills {
        controller_config.metric_grpc_exporter_time_out_in_mills = metric_grpc_exporter_time_out_in_mills;
    }
    if let Some(metric_grpc_exporter_interval_in_mills) = overrides.metric_grpc_exporter_interval_in_mills {
        controller_config.metric_grpc_exporter_interval_in_mills = metric_grpc_exporter_interval_in_mills;
    }
    if let Some(metric_logging_exporter_interval_in_mills) = overrides.metric_logging_exporter_interval_in_mills {
        controller_config.metric_logging_exporter_interval_in_mills = metric_logging_exporter_interval_in_mills;
    }
    if let Some(metrics_prom_exporter_port) = overrides.metrics_prom_exporter_port {
        controller_config.metrics_prom_exporter_port = metrics_prom_exporter_port;
    }
    if let Some(metrics_prom_exporter_host) = overrides.metrics_prom_exporter_host {
        controller_config.metrics_prom_exporter_host = metrics_prom_exporter_host;
    }
    if let Some(metrics_label) = overrides.metrics_label {
        controller_config.metrics_label = metrics_label;
    }
    if let Some(metrics_in_delta) = overrides.metrics_in_delta {
        controller_config.metrics_in_delta = metrics_in_delta;
    }
    if let Some(config_black_list) = overrides.config_black_list {
        controller_config.config_black_list = config_black_list;
    }
    if let Some(node_id) = overrides.node_id {
        controller_config.node_id = node_id;
    }
    if let Some(listen_addr) = overrides.listen_addr {
        controller_config.listen_addr = listen_addr;
    }
    if let Some(raft_peers) = overrides.raft_peers {
        controller_config.raft_peers = parse_raft_peers(&raft_peers)?;
    }
    if let Some(controller_peers) = overrides.controller_peers {
        controller_config.controller_peers = parse_raft_peers(&controller_peers)?;
    }
    if let Some(election_timeout_ms) = overrides.election_timeout_ms {
        controller_config.election_timeout_ms = election_timeout_ms;
    }
    if let Some(heartbeat_interval_ms) = overrides.heartbeat_interval_ms {
        controller_config.heartbeat_interval_ms = heartbeat_interval_ms;
    }
    if let Some(storage_path) = overrides.storage_path {
        controller_config.storage_path = storage_path;
    }
    if let Some(storage_backend) = overrides.storage_backend {
        controller_config.storage_backend = match storage_backend.to_ascii_lowercase().as_str() {
            "rocks_db" | "rocksdb" => StorageBackendType::RocksDB,
            "file" => StorageBackendType::File,
            "memory" => StorageBackendType::Memory,
            _ => bail!("invalid storageBackend: {}", storage_backend),
        };
    }
    if let Some(enable_elect_unclean_master_local) = overrides.enable_elect_unclean_master_local {
        controller_config.enable_elect_unclean_master_local = enable_elect_unclean_master_local;
    }

    Ok(())
}

fn parse_raft_peers(value: &str) -> Result<Vec<RaftPeer>> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    value
        .split(';')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (id, addr) = entry
                .split_once('-')
                .ok_or_else(|| anyhow::anyhow!("invalid raft peer entry: {}", entry))?;
            Ok(RaftPeer {
                id: id.parse()?,
                addr: addr.parse()?,
            })
        })
        .collect()
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

    /// Override the process log filter for this startup.
    #[arg(long = "log-filter", value_name = "DIRECTIVE")]
    log_filter: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn security_bootstrap_precedes_namesrv_listener_bind() {
        let security = SecurityBootstrapConfig::new(SecurityBootstrapProfile::DevelopmentInsecureLoopback);
        let mut server = ServerConfig {
            bind_address: "127.0.0.1".to_string(),
            listen_port: 9876,
            ..ServerConfig::default()
        };

        validate_namesrv_security(
            &security,
            &server,
            None,
            None,
            Some(SocketAddr::from(([127, 0, 0, 1], 8088))),
        )
        .expect("loopback-only NameServer bootstrap should pass");

        server.bind_address = "0.0.0.0".to_string();
        assert!(validate_namesrv_security(&security, &server, None, None, None).is_err());
    }

    #[test]
    fn namesrv_cli_parses_log_filter_override() {
        let args = Args::try_parse_from(["mqnamesrv", "--log-filter", "info,rocketmq_namesrv=debug"])
            .expect("log filter should parse");

        assert_eq!(args.log_filter.as_deref(), Some("info,rocketmq_namesrv=debug"));
    }

    #[test]
    fn namesrv_telemetry_bootstrap_uses_required_logging_defaults() {
        let namesrv_config = NamesrvConfig {
            rocketmq_home: "target/namesrv-telemetry-bootstrap".to_string(),
            ..NamesrvConfig::default()
        };

        let config = build_namesrv_telemetry_bootstrap_config(&namesrv_config);

        assert_eq!(config.observability.service_name, "rocketmq-namesrv");
        assert_eq!(
            config.observability.subscriber_install_policy,
            rocketmq_observability::SubscriberInstallPolicy::Required
        );
        assert!(!config.observability.enabled);
        assert!(config.logging.enabled);
        assert!(config.logging.console.enabled);
        assert!(!config.logging.file.enabled);
        assert_eq!(config.logging.file.file_name_prefix, "rocketmq-namesrv");

        let expected_log_dir = PathBuf::from("target/namesrv-telemetry-bootstrap").join("logs");
        assert_eq!(PathBuf::from(config.logging.file.directory.as_str()), expected_log_dir);
    }
}
