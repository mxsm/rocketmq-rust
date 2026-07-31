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

#![recursion_limit = "256"]

use std::env;
use std::path::PathBuf;

use rocketmq_error::RocketMQError;
#[cfg(test)]
use rocketmq_proxy::GrpcConfig;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyError;
use rocketmq_proxy::ProxyMode;
use rocketmq_proxy::ProxyResult;
use rocketmq_proxy::ProxyRuntime;
#[cfg(test)]
use rocketmq_proxy::RemotingConfig;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeComponent;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReason;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::SecurityBootstrap;
use rocketmq_security_api::SecurityBootstrapConfig;
use rocketmq_security_api::SecurityBootstrapOutcome;
use rocketmq_security_api::SecurityBootstrapProfile;
use tracing::info;

fn main() -> ProxyResult<()> {
    let owner = RuntimeOwner::new(proxy_runtime_config()).map_err(proxy_runtime_error("build proxy runtime"))?;
    let service_context = owner.root_context().component("proxy");
    let lifecycle = ServiceLifecycle::from_env("rocketmq-proxy").map_err(|error| ProxyError::Transport {
        message: format!("invalid Proxy lifecycle configuration: {error}"),
    })?;

    let run_result = owner.block_on(run(service_context, lifecycle.clone()));
    if run_result.is_err() {
        lifecycle.mark_failed();
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_result = owner
        .shutdown_runtime_blocking_until(shutdown_request.deadline)
        .map_err(proxy_runtime_error("shutdown proxy runtime"));

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(report)) => {
            if !report.is_healthy() {
                lifecycle.mark_failed();
                tracing::warn!(
                    report = %report.to_json(),
                    "proxy runtime shutdown report is unhealthy"
                );
                return Err(ProxyError::Transport {
                    message: "Proxy runtime shutdown report is unhealthy".to_string(),
                });
            }
            Ok(())
        }
    }
}

fn proxy_runtime_config() -> RuntimeConfig {
    RuntimeConfig::proxy_default()
}

fn proxy_runtime_error(action: &'static str) -> impl FnOnce(rocketmq_runtime::RuntimeError) -> ProxyError {
    move |error| ProxyError::Transport {
        message: format!("failed to {action}: {error}"),
    }
}

async fn run(service_context: ChildServiceContext, lifecycle: ServiceLifecycle) -> ProxyResult<()> {
    let args = Args::parse()?;
    let mut config = match args.config_file {
        Some(ref path) => ProxyConfig::load_from_file(path)?,
        None => ProxyConfig::default(),
    };
    let logging_overrides = load_logging_overrides(args.config_file.as_deref())?;
    apply_overrides(&mut config, &args)?;

    if args.print_config {
        print_config(&config);
        return Ok(());
    }

    let process_telemetry =
        rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig::from_process_env("rocketmq-proxy")
            .map_err(|error| ProxyError::Transport {
            message: format!("invalid Proxy process telemetry configuration: {error}"),
        })?;
    let security_bootstrap = SecurityBootstrapConfig::from_env().map_err(proxy_security_error)?;
    let validated_security = validate_proxy_security(
        &security_bootstrap,
        &config,
        process_telemetry.prometheus_listener_addr(),
        lifecycle.config().probe_bind_addr,
    )?;

    let environment_filter = rocketmq_observability::read_rust_log().map_err(|error| ProxyError::Transport {
        message: format!("failed to read RUST_LOG: {error}"),
    })?;
    let resolved_filter = resolve_startup_log_filter(&args, &logging_overrides, environment_filter.as_deref())
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to resolve proxy log filter: {error}"),
        })?;
    let mut bootstrap_config = build_proxy_telemetry_bootstrap_config(&config, &process_telemetry);
    bootstrap_config.logging.reload = logging_overrides.logging.reload;
    rocketmq_observability::apply_standard_otlp_environment(&mut bootstrap_config).map_err(|error| {
        ProxyError::Transport {
            message: format!("failed to apply standard OTLP environment to proxy telemetry: {error}"),
        }
    })?;
    let telemetry_guard =
        rocketmq_observability::install_global_with_filter(&bootstrap_config, resolved_filter.clone()).map_err(
            |error| ProxyError::Transport {
                message: format!("failed to initialize proxy telemetry bootstrap: {error}"),
            },
        )?;
    register_proxy_release_identity(&telemetry_guard, &process_telemetry)?;
    log_telemetry_bootstrap(
        &bootstrap_config,
        &resolved_filter,
        telemetry_guard.subscriber_install_status(),
    );
    log_security_bootstrap(validated_security);

    if let Err(error) = lifecycle.start(&service_context).await {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        let primary_error = ProxyError::Transport {
            message: format!("failed to start Proxy lifecycle boundary: {error}"),
        };
        return complete_proxy_process_shutdown(
            Err(primary_error),
            telemetry_guard,
            &service_context,
            request.deadline,
        )
        .await;
    }
    if let Err(error) =
        rocketmq_observability::start_runtime_diagnostics_endpoint_from_env(&service_context, RuntimeComponent::Proxy)
            .await
    {
        lifecycle.mark_failed();
        let request = lifecycle.request_shutdown(ShutdownReason::Internal);
        if let Err(shutdown_error) = telemetry_guard
            .shutdown_with_timeout(request.deadline.remaining())
            .into_result()
        {
            tracing::warn!(error = %shutdown_error, "proxy telemetry cleanup after diagnostics startup failure was unhealthy");
        }
        return Err(ProxyError::Transport {
            message: format!("failed to start protected Proxy runtime diagnostics: {error}"),
        });
    }

    info!(
        "Starting RocketMQ proxy: mode={:?}, grpc={}, remotingEnabled={}, remoting={}",
        config.mode, config.grpc.listen_addr, config.remoting.enabled, config.remoting.listen_addr
    );
    let primary_result = match ProxyRuntime::builder(config, service_context.clone(), telemetry_guard.handle()).build()
    {
        Ok(proxy_runtime) => proxy_runtime.serve_with_lifecycle(lifecycle.clone()).await,
        Err(error) => Err(error),
    };
    let primary_result = match primary_result {
        Ok(()) if lifecycle.state() == ServiceLifecycleState::Failed => Err(ProxyError::Transport {
            message: "Proxy lifecycle failed while observing or completing shutdown".to_string(),
        }),
        result => result,
    };
    if primary_result.is_err() {
        lifecycle.mark_failed();
        lifecycle.request_shutdown(ShutdownReason::Internal);
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_result = complete_proxy_process_shutdown(
        primary_result,
        telemetry_guard,
        &service_context,
        shutdown_request.deadline,
    )
    .await;
    if shutdown_result.is_ok() {
        lifecycle.mark_stopped();
    }
    shutdown_result
}

async fn complete_proxy_process_shutdown(
    primary_result: ProxyResult<()>,
    telemetry_guard: rocketmq_observability::TelemetryRuntimeGuard,
    service_context: &ChildServiceContext,
    deadline: ShutdownDeadline,
) -> ProxyResult<()> {
    let primary_result = finish_proxy_process_shutdown(primary_result, service_context.task_group(), deadline).await;
    let telemetry_result = telemetry_guard
        .shutdown_with_service_context(service_context, deadline.remaining())
        .await
        .into_result()
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to shutdown proxy telemetry bootstrap: {error}"),
        });

    match (primary_result, telemetry_result) {
        (Err(primary_error), Err(telemetry_error)) => Err(ProxyError::Transport {
            message: format!(
                "Proxy startup or serving failed: {primary_error}; telemetry shutdown also failed: {telemetry_error}"
            ),
        }),
        (Err(error), Ok(_report)) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(_report)) => Ok(()),
    }
}

async fn finish_proxy_process_shutdown(
    primary_result: ProxyResult<()>,
    service_tasks: &TaskGroup,
    deadline: ShutdownDeadline,
) -> ProxyResult<()> {
    let service_report = service_tasks.shutdown_until(deadline).await;
    match (primary_result, service_report.is_healthy()) {
        (Ok(()), true) => Ok(()),
        (Err(primary_error), true) => Err(primary_error),
        (Ok(()), false) => Err(ProxyError::Transport {
            message: format!(
                "Proxy service task shutdown was unhealthy: {}",
                service_report.to_json()
            ),
        }),
        (Err(primary_error), false) => Err(ProxyError::Transport {
            message: format!(
                "Proxy startup or serving failed: {primary_error}; Proxy service task shutdown was unhealthy: {}",
                service_report.to_json()
            ),
        }),
    }
}

fn proxy_security_error(error: rocketmq_security_api::SecurityBootstrapError) -> ProxyError {
    ProxyError::Transport {
        message: format!("Proxy security bootstrap failed before listener bind: {error}"),
    }
}

fn validate_proxy_security(
    security_bootstrap: &SecurityBootstrap,
    config: &ProxyConfig,
    prometheus_bind_addr: Option<std::net::SocketAddr>,
    probe_bind_addr: Option<std::net::SocketAddr>,
) -> ProxyResult<SecurityBootstrapOutcome> {
    if !security_bootstrap.is_enabled() {
        return security_bootstrap.validate(&[]).map_err(proxy_security_error);
    }
    let mut listeners = vec![config.grpc.socket_addr()?];
    if config.remoting.enabled {
        listeners.push(config.remoting.socket_addr()?);
    }
    if let Some(prometheus_bind_addr) = prometheus_bind_addr {
        listeners.push(prometheus_bind_addr);
    }
    if let Some(probe_bind_addr) = probe_bind_addr {
        listeners.push(probe_bind_addr);
    }
    security_bootstrap.validate(&listeners).map_err(proxy_security_error)
}

fn log_security_bootstrap(outcome: SecurityBootstrapOutcome) {
    match outcome {
        SecurityBootstrapOutcome::Disabled => {
            tracing::warn!("Proxy security bootstrap is disabled because no security profile is configured")
        }
        SecurityBootstrapOutcome::Validated(validated) => match validated.profile() {
            SecurityBootstrapProfile::DevelopmentInsecureLoopback => tracing::warn!(
                profile = validated.profile().as_str(),
                listener_count = validated.listener_count(),
                "Proxy development-insecure security profile is active; every listener is restricted to loopback"
            ),
            SecurityBootstrapProfile::SecureEnforced => info!(
                profile = validated.profile().as_str(),
                listener_count = validated.listener_count(),
                "Proxy secure bootstrap completed before listener bind"
            ),
        },
    }
}

fn build_proxy_telemetry_bootstrap_config(
    _config: &ProxyConfig,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut observability = rocketmq_observability::ObservabilityConfig {
        service_name: "rocketmq-proxy".to_string(),
        service_namespace: "rocketmq".to_string(),
        node_type: "proxy".to_string(),
        node_id: "proxy".to_string(),
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;
    process_telemetry.apply_to(&mut observability);

    let mut logging = rocketmq_observability::LoggingConfig::default();
    logging.file.file_name_prefix = "rocketmq-proxy".to_string();

    rocketmq_observability::TelemetryBootstrapConfig { observability, logging }
}

fn register_proxy_release_identity(
    telemetry_guard: &rocketmq_observability::TelemetryRuntimeGuard,
    process_telemetry: &rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig,
) -> ProxyResult<()> {
    if !process_telemetry.metrics_enabled() {
        return Ok(());
    }

    #[cfg(feature = "observability")]
    {
        let telemetry = telemetry_guard.handle();
        telemetry
            .register_release_identity(process_telemetry.release_identity().clone())
            .map_err(|error| ProxyError::Transport {
                message: format!("failed to register Proxy release identity before readiness: {error}"),
            })?;
        if !telemetry.release_identity_registered() {
            return Err(ProxyError::Transport {
                message: "Proxy release identity was not registered before readiness".to_string(),
            });
        }
        Ok(())
    }

    #[cfg(not(feature = "observability"))]
    {
        let _ = telemetry_guard;
        Err(ProxyError::Transport {
            message: "Proxy metrics require the `observability` Cargo feature".to_string(),
        })
    }
}

fn log_telemetry_bootstrap(
    config: &rocketmq_observability::TelemetryBootstrapConfig,
    resolved_filter: &rocketmq_observability::ResolvedLogFilter,
    subscriber_install_status: rocketmq_observability::SubscriberInstallStatus,
) {
    info!(
        service = "rocketmq-proxy",
        effective_filter = resolved_filter.filter(),
        filter_source = %resolved_filter.source(),
        metrics_exporter = ?config.observability.metrics.exporter,
        trace_exporter = ?config.observability.traces.exporter,
        log_exporter = ?config.observability.logs.exporter,
        subscriber_installed = subscriber_install_status.installed,
        reload_enabled = config.logging.reload.enabled,
        file_log_enabled = config.logging.file.enabled,
        "proxy telemetry bootstrap initialized"
    );
}

fn load_logging_overrides(path: Option<&std::path::Path>) -> ProxyResult<rocketmq_observability::LoggingOverrides> {
    let Some(path) = path else {
        return Ok(rocketmq_observability::LoggingOverrides::default());
    };
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("proxy config");
    let config = config::Config::builder()
        .add_source(config::File::from(path))
        .build()
        .map_err(|error| RocketMQError::ConfigParseFailed {
            key: "proxy.logging",
            reason: format!("failed to build logging config {file_name}: {error}"),
        })?;
    config.try_deserialize().map_err(|error| {
        RocketMQError::ConfigParseFailed {
            key: "proxy.logging",
            reason: format!("failed to deserialize logging config {file_name}: {error}"),
        }
        .into()
    })
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

struct Args {
    config_file: Option<PathBuf>,
    mode: Option<ProxyMode>,
    grpc_listen_addr: Option<String>,
    remoting_listen_addr: Option<String>,
    enable_remoting: bool,
    namesrv_addr: Option<String>,
    print_config: bool,
    log_filter: Option<String>,
}

impl Args {
    fn parse() -> ProxyResult<Self> {
        Self::parse_from(env::args())
    }

    fn parse_from<I, S>(args: I) -> ProxyResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut args = args.into_iter().map(Into::into).skip(1);
        let mut parsed = Args {
            config_file: None,
            mode: None,
            grpc_listen_addr: None,
            remoting_listen_addr: None,
            enable_remoting: false,
            namesrv_addr: None,
            print_config: false,
            log_filter: None,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-c" | "--config" => parsed.config_file = Some(PathBuf::from(next_value(&mut args, arg.as_str())?)),
                "--mode" => parsed.mode = Some(parse_mode(next_value(&mut args, arg.as_str())?.as_str())?),
                "--grpcListenAddr" => parsed.grpc_listen_addr = Some(next_value(&mut args, arg.as_str())?),
                "--remotingListenAddr" => parsed.remoting_listen_addr = Some(next_value(&mut args, arg.as_str())?),
                "--enableRemoting" => parsed.enable_remoting = true,
                "--namesrvAddr" | "-n" => parsed.namesrv_addr = Some(next_value(&mut args, arg.as_str())?),
                "--printConfig" => parsed.print_config = true,
                "--log-filter" => parsed.log_filter = Some(next_value(&mut args, arg.as_str())?),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => {
                    return Err(ProxyError::from(RocketMQError::illegal_argument(format!(
                        "unknown proxy argument '{arg}'. Use --help for usage."
                    ))));
                }
            }
        }

        Ok(parsed)
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> ProxyResult<String> {
    args.next()
        .ok_or_else(|| RocketMQError::illegal_argument(format!("missing value for {name}")))
        .map_err(Into::into)
}

fn parse_mode(value: &str) -> ProxyResult<ProxyMode> {
    match value {
        #[cfg(feature = "cluster-mode")]
        "cluster" | "Cluster" => Ok(ProxyMode::Cluster),
        #[cfg(not(feature = "cluster-mode"))]
        "cluster" | "Cluster" => Err(ProxyError::not_implemented(
            "Cluster mode is unavailable because the 'cluster-mode' feature is disabled",
        )),
        #[cfg(feature = "local-mode")]
        "local" | "Local" => Ok(ProxyMode::Local),
        #[cfg(not(feature = "local-mode"))]
        "local" | "Local" => Err(ProxyError::not_implemented(
            "Local mode is unavailable because the 'local-mode' feature is disabled",
        )),
        _ => Err(ProxyError::from(RocketMQError::illegal_argument(format!(
            "invalid proxy mode '{value}', expected cluster or local"
        )))),
    }
}

fn apply_overrides(config: &mut ProxyConfig, args: &Args) -> ProxyResult<()> {
    if let Some(mode) = args.mode {
        config.mode = mode;
    }
    if let Some(addr) = &args.grpc_listen_addr {
        config.grpc.listen_addr = addr.clone();
    }
    if let Some(addr) = &args.remoting_listen_addr {
        config.remoting.listen_addr = addr.clone();
    }
    if args.enable_remoting {
        config.remoting.enabled = true;
    }
    if let Some(addr) = &args.namesrv_addr {
        #[cfg(feature = "cluster-mode")]
        {
            config.cluster.namesrv_addr = Some(addr.clone());
        }
        #[cfg(not(feature = "cluster-mode"))]
        {
            let _ = addr;
            return Err(ProxyError::not_implemented(
                "--namesrvAddr requires the 'cluster-mode' feature",
            ));
        }
    }

    config.grpc.socket_addr()?;
    if config.remoting.enabled {
        config.remoting.socket_addr()?;
    }
    Ok(())
}

fn print_usage() {
    println!(
        "rocketmq-proxy-rust [-c <proxy.toml>] [--mode cluster|local] [--grpcListenAddr <host:port>] \
         [--enableRemoting] [--remotingListenAddr <host:port>] [--namesrvAddr <host:port>] [--log-filter <directive>] \
         [--printConfig]"
    );
}

fn print_config(config: &ProxyConfig) {
    println!("mode = {:?}", config.mode);
    println!("grpc.listenAddr = {}", config.grpc.listen_addr);
    println!("remoting.enabled = {}", config.remoting.enabled);
    println!("remoting.listenAddr = {}", config.remoting.listen_addr);
    #[cfg(feature = "cluster-mode")]
    println!("cluster.namesrvAddr = {:?}", config.cluster.namesrv_addr);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_security_bootstrap_allows_default_proxy_listeners() {
        let outcome = validate_proxy_security(
            &rocketmq_security_api::SecurityBootstrap::Disabled,
            &ProxyConfig::default(),
            None,
            None,
        )
        .expect("disabled security bootstrap should not restrict Proxy listeners");

        assert_eq!(outcome, rocketmq_security_api::SecurityBootstrapOutcome::Disabled);
    }

    #[test]
    fn security_bootstrap_precedes_proxy_listener_bind() {
        let security = rocketmq_security_api::SecurityBootstrap::Enabled(SecurityBootstrapConfig::new(
            SecurityBootstrapProfile::DevelopmentInsecureLoopback,
        ));
        let mut config = ProxyConfig {
            grpc: GrpcConfig {
                listen_addr: "127.0.0.1:8081".to_string(),
                ..GrpcConfig::default()
            },
            remoting: RemotingConfig {
                enabled: true,
                listen_addr: "127.0.0.1:8080".to_string(),
            },
            ..ProxyConfig::default()
        };

        validate_proxy_security(
            &security,
            &config,
            None,
            Some(std::net::SocketAddr::from(([127, 0, 0, 1], 8088))),
        )
        .expect("loopback-only Proxy bootstrap should pass");

        config.grpc.listen_addr = "0.0.0.0:8081".to_string();
        assert!(validate_proxy_security(&security, &config, None, None).is_err());
    }

    #[test]
    fn development_security_rejects_public_prometheus_listener() {
        let security = rocketmq_security_api::SecurityBootstrap::Enabled(SecurityBootstrapConfig::new(
            SecurityBootstrapProfile::DevelopmentInsecureLoopback,
        ));
        let config = ProxyConfig {
            grpc: GrpcConfig {
                listen_addr: "127.0.0.1:8081".to_string(),
                ..GrpcConfig::default()
            },
            ..ProxyConfig::default()
        };

        assert!(validate_proxy_security(
            &security,
            &config,
            Some(std::net::SocketAddr::from(([0, 0, 0, 0], 5557))),
            None,
        )
        .is_err());
    }

    #[test]
    fn proxy_cli_parses_log_filter_override() {
        let args = Args::parse_from(["rocketmq-proxy-rust", "--log-filter", "info,rocketmq_proxy=debug"])
            .expect("log filter should parse");

        assert_eq!(args.log_filter.as_deref(), Some("info,rocketmq_proxy=debug"));
    }

    #[test]
    fn proxy_telemetry_bootstrap_uses_required_logging_defaults() {
        let process_telemetry =
            rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig::try_from_values(
                "rocketmq-proxy",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("local Proxy process telemetry");
        let config = build_proxy_telemetry_bootstrap_config(&ProxyConfig::default(), &process_telemetry);

        assert_eq!(config.observability.service_name, "rocketmq-proxy");
        assert_eq!(
            config.observability.subscriber_install_policy,
            rocketmq_observability::SubscriberInstallPolicy::Required
        );
        assert!(!config.observability.enabled);
        assert!(config.logging.enabled);
        assert!(config.logging.console.enabled);
        assert!(!config.logging.file.enabled);
        assert_eq!(config.logging.file.file_name_prefix, "rocketmq-proxy");
    }

    #[test]
    fn proxy_bootstrap_accepts_standard_otlp_environment_values() {
        let process_telemetry =
            rocketmq_observability::metrics::release_identity::ProcessTelemetryConfig::try_from_values(
                "rocketmq-proxy",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("local Proxy process telemetry");
        let mut config = build_proxy_telemetry_bootstrap_config(&ProxyConfig::default(), &process_telemetry);

        rocketmq_observability::apply_standard_otlp_environment_values(
            &mut config,
            Some(std::ffi::OsStr::new("http://collector:4317")),
            Some(std::ffi::OsStr::new("grpc")),
        )
        .expect("valid standard OTLP environment should apply");

        assert!(config.observability.enabled);
        assert_eq!(config.observability.service_name, "rocketmq-proxy");
        assert_eq!(
            config.observability.metrics.exporter,
            rocketmq_observability::MetricsExporter::OtlpGrpc
        );
        assert_eq!(
            config.observability.traces.exporter,
            rocketmq_observability::TraceExporter::OtlpGrpc
        );
        assert_eq!(
            config.observability.logs.exporter,
            rocketmq_observability::LogsExporter::OtlpGrpc
        );
    }
}
